use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::time::Instant;
use broker_core::delivery::{AckOutcome, AckRejection};
use broker_core::types::Offset;
use crate::BoxError;
use crate::client::Client;
use crate::protocol::{self, Header, Op, Status};
use super::children::decode_receipt;
use super::ledger::{Key, Ledger, Outcome, Scan, classify};
use super::process::{Broker, Process, publish, segment_bytes};
use super::{DATA_QUOTA, FETCH_BYTES, MAX_ATTEMPTS, Report, TIMEOUT, g, p, require, validate_rounds};

async fn append_one(address: SocketAddr, ledger: &mut Ledger, key: Key) -> Result<Offset, BoxError> {
    ledger.intent(key)?;
    let result = match Client::connect(address, TIMEOUT).await {
        Ok(mut client) => client.append(p(key.partition()), &key.payload()).await,
        Err(error) => Err(error),
    };
    ledger.resolve(key, classify(&result))?;
    Ok(result?)
}

async fn producer_wave(address: SocketAddr, ledger: &mut Ledger, sequence: u64) -> Result<(), BoxError> {
    let mut tasks = JoinSet::new();
    for producer in 0..8 {
        let keys = [Key { producer, sequence, attempt: 0 },
            Key { producer, sequence: sequence + 1, attempt: 0 }];
        for key in keys { ledger.intent(key)?; }
        tasks.spawn(async move {
            let mut connection: Option<Client> = None;
            let mut replies = Vec::with_capacity(keys.len());
            for key in keys {
                if connection.is_none() {
                    match Client::connect(address, TIMEOUT).await {
                        Ok(client) => connection = Some(client),
                        Err(error) => { replies.push((key, Err(error))); continue; }
                    }
                }
                let result = connection.as_mut().expect("connected producer")
                    .append(p(key.partition()), &key.payload()).await;
                if result.is_err() { drop(connection.take()); }
                replies.push((key, result));
            }
            replies
        });
    }
    let mut failure = None;
    let mut ledger_failed = false;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(replies) => for (key, result) in replies {
                if !ledger_failed {
                    if let Err(error) = ledger.resolve(key, classify(&result)) {
                        ledger_failed = true;
                        failure.get_or_insert_with(|| error.to_string());
                        tasks.abort_all();
                    }
                }
                if let Err(error) = result { failure.get_or_insert_with(|| error.to_string()); }
            },
            Err(error) => { failure.get_or_insert_with(|| error.to_string()); },
        }
    }
    if let Some(error) = failure { return Err(io::Error::other(format!("producer wave failed: {error}")).into()); }
    Ok(())
}

async fn drain_group(address: SocketAddr, ledger: &mut Ledger, group: u8, scan: &mut Scan)
    -> Result<[u64; 4], BoxError>
{
    let mut end = [0; 4];
    let mut client = Client::connect(address, TIMEOUT).await?;
    for partition in 0..4 {
        let mut empty = false;
        for _ in 0..MAX_ATTEMPTS {
            let batch = client.fetch(p(partition), g(group), 4, FETCH_BYTES).await?;
            if batch.records().len() == 0 { empty = true; break; }
            let mut tokens = Vec::with_capacity(4);
            for record in batch.records() {
                let key = ledger.observe(record.token, record.payload)?;
                let position = (partition, record.token.offset.get());
                require(scan.insert(position, key).is_none(), "same offset repeated within a bounded drain pass")?;
                tokens.push(record.token);
            }
            drop(batch);
            // Completion above the head must not retire the gap. Finish the head last.
            for token in tokens.into_iter().rev() {
                let reply = client.acknowledge(token).await?;
                require(reply.outcome == AckOutcome::Applied, "expected live acknowledgement; inspect lease/timeouts")?;
                ledger.note(&format!("ACK\tg={group}\tp={partition}\to={}\tvolatile={}\tpersisted={}",
                    token.offset.get(), reply.progress.volatile_next.get(), reply.progress.persisted_next.get()))?;
            }
        }
        require(empty, "drain exceeded the bounded checker operation count")?;
        let progress = client.progress(p(partition), g(group)).await?;
        end[partition as usize] = progress.volatile_next.get();
    }
    ledger.coverage(group, end, scan)?;
    Ok(end)
}

async fn checkpoint_group(address: SocketAddr, ledger: &mut Ledger, group: u8, end: [u64; 4])
    -> Result<(), BoxError>
{
    let mut client = Client::connect(address, TIMEOUT).await?;
    for partition in 0..4 {
        let reply = client.checkpoint(p(partition), g(group)).await?;
        let next = end[partition as usize];
        require(reply.progress.volatile_next.get() == next && reply.progress.persisted_next.get() == next,
            "checkpoint reply disagrees with checked group coverage")?;
        ledger.checkpoint(group, partition, next)?;
    }
    Ok(())
}

async fn reclaim(address: SocketAddr, root: &Path, ledger: &mut Ledger, end: [u64; 4])
    -> Result<(), BoxError>
{
    let mut client = Client::connect(address, TIMEOUT).await?;
    for partition in 0..4 {
        let before = segment_bytes(root, partition)?;
        require(before <= DATA_QUOTA, "quiescent record files exceed normal quota")?;
        let reply = client.retain(p(partition)).await?;
        let next = end[partition as usize];
        require(reply.before.get() == ledger.floor()[partition as usize] && reply.floor.get() == next,
            "retention did not stop at the checked, persisted frontier")?;
        let after = segment_bytes(root, partition)?;
        let added = if reply.rotated { 32 } else { 0 };
        let accounted = before.checked_add(added).and_then(|n| n.checked_sub(reply.removed_bytes));
        require(after == 32 && accounted == Some(after), "retained file-length accounting disagrees")?;
        require(reply.removed_segments != 0, "expected covered segments to be reclaimed")?;
        ledger.retained(partition, next)?;
        ledger.note(&format!("DISK\tp={partition}\tbefore={before}\tafter={after}\tremoved={}", reply.removed_bytes))?;
    }
    ledger.sync()?;
    Ok(())
}

async fn lose_consumer(executable: &Path, root: &Path, address: SocketAddr, ledger: &mut Ledger)
    -> Result<broker_core::delivery::LeaseToken, BoxError>
{
    let path = root.to_str().ok_or_else(|| io::Error::other("run path must be UTF-8"))?;
    let endpoint = address.to_string();
    let mut victim = Process::spawn(executable, &["consumer-child", path, &endpoint], &root.join("consumer.log"))?;
    let receipt = victim.ready(&root.join("victim.ready")).await?;
    let (token, payload) = decode_receipt(&receipt)?;
    ledger.observe(token, &payload)?;
    ledger.sync()?;
    victim.kill().await?;
    ledger.note("KILL\tconsumer; delivered receipt observed, no acknowledgement")?;
    Ok(token)
}

async fn interrupt_append(broker: &mut Broker, root: &Path, ledger: &mut Ledger) -> Result<Key, BoxError> {
    let key = Key { producer: 0, sequence: 2, attempt: 0 };
    ledger.intent(key)?;
    ledger.sync()?;
    publish(&root.join("gate.arm"), b"arm\n")?;
    let deadline = Instant::now() + TIMEOUT;
    let mut stream = protocol::within(deadline, TcpStream::connect(broker.addresses.producer)).await?;
    let payload = key.payload();
    let mut header = Header::request(Op::Append, p(0), g(0));
    header.id = 1;
    header.body = payload.len() as u32;
    protocol::write_bytes(&mut stream, &header.encode(), deadline).await?;
    protocol::write_bytes(&mut stream, &payload, deadline).await?;
    let marker = broker.process.ready(&root.join("gate.entered")).await?;
    require(marker == b"before-record-sync\n", "wrong synchronization rendezvous")?;
    // No append response has been read. The gate proves reaching the targeted owner boundary.
    ledger.resolve(key, Outcome::Ambiguous)?;
    ledger.sync()?;
    broker.process.kill().await?;
    drop(stream);
    ledger.note("KILL\tbroker; checker and external ledger remain alive")?;
    Ok(key)
}

async fn refusal_probe(address: SocketAddr, ledger: &mut Ledger, key: Key) -> Result<(), BoxError> {
    ledger.intent(key)?;
    let result: Result<(), BoxError> = async {
        let deadline = Instant::now() + TIMEOUT;
        let mut stream = protocol::within(deadline, TcpStream::connect(address)).await?;
        let mut header = Header::request(Op::Append, p(key.partition()), g(0));
        header.id = 1;
        header.body = key.payload().len() as u32;
        // Full disk is decided from this header. Do not send an unread body before refusal/close.
        protocol::write_bytes(&mut stream, &header.encode(), deadline).await?;
        let reply = protocol::read_header(&mut stream, deadline).await?
            .ok_or_else(|| io::Error::other("missing explicit disk refusal"))?;
        protocol::validate_response(header, reply)?;
        require(reply.status == Status::DiskFull, "expected explicit DiskFull response")?;
        Ok(())
    }.await;
    ledger.resolve(key, if result.is_ok() { Outcome::Rejected } else { Outcome::Ambiguous })?;
    result
}

async fn fill_to_refusal(address: SocketAddr, root: &Path, ledger: &mut Ledger) -> Result<Key, BoxError> {
    for sequence in 100..132 {
        let key = Key { producer: 0, sequence, attempt: 0 };
        // No other appends are running here. Charge one 56-byte record plus a possible 32-byte header.
        let used = segment_bytes(root, 0)?;
        require(used <= DATA_QUOTA, "record-file charge exceeds quota before admission")?;
        if used + 88 > DATA_QUOTA {
            require(used == 600, "small-budget fixture has unexpected segment geometry")?;
            refusal_probe(address, ledger, key).await?;
            ledger.note("PRESSURE\tDiskFull observed from header; no automatic retry")?;
            return Ok(key);
        }
        append_one(address, ledger, key).await?;
    }
    Err(io::Error::other("small quota did not produce a refusal within 32 attempts").into())
}

async fn pressure_phase(addresses: super::process::Addresses, root: &Path, ledger: &mut Ledger,
    scans: &mut [Scan; 2]) -> Result<Key, BoxError>
{
    let refused = fill_to_refusal(addresses.producer, root, ledger).await?;
    let end = drain_group(addresses.consumer, ledger, 0, &mut scans[0]).await?;
    checkpoint_group(addresses.consumer, ledger, 0, end).await?;
    let mut client = Client::connect(addresses.consumer, TIMEOUT).await?;
    let lag = client.progress(p(0), g(1)).await?;
    require(lag.persisted_next.get() == 0, "coverage phase accidentally checkpointed the pinned group")?;
    let before = segment_bytes(root, 0)?;
    let retained = client.retain(p(0)).await?;
    require(retained.floor.get() == 0 && retained.removed_bytes == 0
        && segment_bytes(root, 0)? == before, "stalled group did not pin required records")?;
    ledger.note("PRESSURE\tack/checkpoint/control progressed; pinned group blocked deletion")?;
    drop(client);
    let other = drain_group(addresses.consumer, ledger, 1, &mut scans[1]).await?;
    require(end == other, "groups covered different record intervals")?;
    checkpoint_group(addresses.consumer, ledger, 1, end).await?;
    reclaim(addresses.consumer, root, ledger, end).await?;
    Ok(refused)
}

async fn steady_round(addresses: super::process::Addresses, root: &Path, ledger: &mut Ledger, round: u32)
    -> Result<(), BoxError>
{
    producer_wave(addresses.producer, ledger, 1000 + u64::from(round) * 2).await?;
    let mut scans = [Scan::new(), Scan::new()];
    let end = drain_group(addresses.consumer, ledger, 0, &mut scans[0]).await?;
    let other = drain_group(addresses.consumer, ledger, 1, &mut scans[1]).await?;
    require(end == other, "steady groups covered different intervals")?;
    for group in 0..2 { checkpoint_group(addresses.consumer, ledger, group, end).await?; }
    reclaim(addresses.consumer, root, ledger, end).await?;
    ledger.note(&format!("ROUND\t{round}\tend={end:?}"))?;
    Ok(())
}

async fn check_retained_restart(address: SocketAddr, root: &Path, ledger: &mut Ledger) -> Result<(), BoxError> {
    let floor = ledger.floor();
    let mut client = Client::connect(address, TIMEOUT).await?;
    for group in 0..2 {
        for partition in 0..4 {
            let progress = client.progress(p(partition), g(group)).await?;
            require(progress.persisted_next.get() == floor[partition as usize]
                && progress.volatile_next.get() == floor[partition as usize], "restart lost retained progress")?;
            let batch = client.fetch(p(partition), g(group), 4, FETCH_BYTES).await?;
            require(batch.records().len() == 0, "restart resurrected legitimately deleted records")?;
            require(segment_bytes(root, partition)? == 32, "empty retained segment changed size")?;
        }
        ledger.coverage(group, floor, &Scan::new())?;
    }
    ledger.note("RESTART\tretained floors and checkpointed empty windows agree")?;
    Ok(())
}

pub async fn run(executable: &Path, root: &Path, rounds: u32) -> Result<Report, BoxError> {
    validate_rounds(rounds)?;
    let executable = fs::canonicalize(executable)?;
    // Refuse reuse; never remove an existing run or combine ledgers from different instances.
    fs::create_dir(root)?;
    let root = fs::canonicalize(root)?;
    let mut ledger = Ledger::create(&root.join("ledger.tsv"))?;
    let mut broker = Broker::start(&executable, &root, 1).await?;
    ledger.begin_epoch(broker.addresses.epoch)?;
    producer_wave(broker.addresses.producer, &mut ledger, 0).await?;
    drain_group(broker.addresses.consumer, &mut ledger, 0, &mut Scan::new()).await?;
    let obsolete = lose_consumer(&executable, &root, broker.addresses.consumer, &mut ledger).await?;
    let ambiguous = interrupt_append(&mut broker, &root, &mut ledger).await?;
    drop(broker);

    let mut broker = Broker::start(&executable, &root, 2).await?;
    ledger.begin_epoch(broker.addresses.epoch)?;
    let mut client = Client::connect(broker.addresses.consumer, TIMEOUT).await?;
    let fenced = client.acknowledge(obsolete).await?;
    require(fenced.outcome == AckOutcome::Rejected(AckRejection::WrongEpoch), "obsolete token was not fenced")?;
    require(fenced.progress.volatile_next.get() == 0 && fenced.progress.persisted_next.get() == 0,
        "obsolete acknowledgement advanced the restarted group")?;
    ledger.note("FENCE\told consumer epoch rejected without progress")?;
    drop(client);
    append_one(broker.addresses.producer, &mut ledger, Key { attempt: 1, ..ambiguous }).await?;
    let mut scans = [Scan::new(), Scan::new()];
    let end = drain_group(broker.addresses.consumer, &mut ledger, 0, &mut scans[0]).await?;
    let other = drain_group(broker.addresses.consumer, &mut ledger, 1, &mut scans[1]).await?;
    require(end == other, "recovery coverage differs between groups")?;
    ledger.note("RECOVERY-COVERAGE\tall confirmed positions and payloads present; retention still disabled")?;
    ledger.sync()?;

    let refused = pressure_phase(broker.addresses, &root, &mut ledger, &mut scans).await?;
    let expected = ledger.floor()[0];
    let retried = append_one(broker.addresses.producer, &mut ledger, Key { attempt: 1, ..refused }).await?;
    require(retried.get() == expected, "post-reclamation append reused or skipped an offset")?;
    for round in 0..rounds { steady_round(broker.addresses, &root, &mut ledger, round).await?; }
    broker.process.stop(broker.addresses.control).await?;
    drop(broker);

    let mut broker = Broker::start(&executable, &root, 3).await?;
    ledger.begin_epoch(broker.addresses.epoch)?;
    check_retained_restart(broker.addresses.consumer, &root, &mut ledger).await?;
    broker.process.stop(broker.addresses.control).await?;
    let mut report = ledger.report(rounds);
    report.broker_kills = 1;
    report.consumer_kills = 1;
    report.obsolete_token_rejected = true;
    require(report.ambiguous == 1 && report.rejected == 1 && report.repeated_deliveries >= 17,
        "the directed ambiguity/refusal/redelivery events were not observed")?;
    ledger.note(&format!("PASS\t{report:?}"))?;
    ledger.sync()?;
    fs::write(root.join("report.txt"), format!("{report:#?}\n"))?;
    Ok(report)
}
