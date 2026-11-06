# Chapter 19 — End-to-end coverage and overload demonstration

## Part 3 — Coverage, overload, and safe reclamation

> **Learning objectives**
> Drive eight producers without sharing mutable ledger state across tasks; validate consumer coverage before checkpointing; preserve ambiguity through a real process kill; and demonstrate that disk pressure ends only after safe persisted progress enables reclamation.

This is the third part of the same **5.0-hour chapter**. Prerequisites are [Part 1](https://25d-228.github.io/rust-learning/chapter-19.html) and [Part 2](https://25d-228.github.io/rust-learning/chapter-19-part-02.html). [Part 4](https://25d-228.github.io/rust-learning/chapter-19-part-04.html) supplies the command-line entry point, tests, and complete run commands.

## Make every append attempt explicit

Create `workload/scenario.rs`. `append_one` records intent before opening the connection, classifies the result, and returns the original error to the scenario. It does not retry. A caller choosing to retry must construct a new `Key` with a changed attempt number.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

The result is borrowed for classification, then consumed by `?`. This avoids cloning an `io::Error` merely to count it. `BoxError` can carry either transport, storage, or checker failures; it is not used to erase the append outcome stored in the ledger.

The ledger owns the expected payload identity before the network call. A received offset confirms only that attempt. A failure after acceptance can still leave a record, which is why unexpected transport errors fail the normal wave and are not automatically recorded as rejected.

## Join eight producer tasks while the parent owns the ledger

Each wave has eight producers, two per partition. Each producer sends two inputs sequentially on its own connection, while different producer tasks run concurrently. There are at most eight such tasks and 16 planned inputs per wave. The parent registers all intentions, moves small copied keys into tasks, then consumes their results.

Add the complete wave. No mutex protects the ledger because no producer task mutates it. `JoinSet` returns task completions in completion order, which is not append order; the returned partition/offset bindings remain authoritative. See [`JoinSet`](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html).

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

`Option<Client>` encodes whether a usable connection remains. An error drops the socket before a later, different input may connect. The same input is never silently resent. Reconnecting for a new key is not retrying the failed key.

A transport error is recorded but does not make the parent stop joining the other tasks. A ledger error is more serious: further observations cannot be trusted, so it aborts remaining tasks and still drains the join set. The `ledger_failed` flag prevents repeatedly trying writes to a failed transcript. Any unresolved intentions or failed tasks prevent the scenario from reporting success. This is bounded workload cleanup, not a second broker supervisor.

The task-return vector contains at most two results; all payloads are fixed 24-byte stack arrays whose borrows end with the individual append future. There is no shared mutable payload across producers and no unbounded sender queue in front of them. Offered traffic is closed-loop: each producer waits for its own response before sending its next record. It is not a fixed offered-rate benchmark.

## Observe records before acknowledging them

A group drain repeatedly fetches at most four records with an encoded-byte limit of 224, validates each payload through the external ledger, and stores the positions in a scan map. It copies only the small lease tokens before dropping the fetched batch. Those tokens can then be acknowledged without retaining the response buffer.

Add the complete function. The inverse acknowledgement order is deliberate: completing the last fetched record first must not advance through the missing head.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

For offsets `[0, 1, 2, 3]`, acknowledging `[3, 2, 1]` leaves contiguous next offset 0. Acknowledging 0 retires all four and moves it to 4. The individual `ACK` transcript records include both volatile and persisted frontiers, so diagnosis can distinguish window retirement from checkpoint publication.

Within this bounded drain, a repeated offset is treated as a scenario failure. That is **not** a general prohibition on at-least-once delivery. Here, the script acknowledges each live attempt immediately, with a 30-second lease, and has no intentionally abandoned fetch during the drain. A repeat usually means an expired lease, a missing acknowledgement, or wrong scan reuse. Historical repeats across the deliberate broker restart are counted by `Ledger::observe` and are expected.

An empty fetch is not by itself the coverage proof. The driver reads contiguous progress and passes its scan to `coverage`, which checks gaps and every confirmed binding. A missing record beyond a falsely short end still fails. A bounded iteration count also prevents an accidental infinite fetch loop.

## Keep checkpoint evidence separate from accepted completion

Append the checkpoint helper. Its caller has already verified coverage for this group. The reply must report both volatile and persisted next equal to the checked end; only then does the external ledger advance its checkpoint evidence.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

There are eight group/partition progress states, not one global checkpoint. The helper issues one request at a time and does not claim an atomic cross-group or cross-partition transaction. A failure stops the scenario; earlier successful checkpoint publications can remain valid.

## Reclaim only after both groups are covered

Append `reclaim`. It records file lengths while producers are quiet, sends the real retention operation, checks the returned floor, and inspects the remaining segment length. Then it asks the ledger to authorize the new floor using the two independently checked checkpoint frontiers.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

Let $D_{\mathrm{before}}$ and $D_{\mathrm{after}}$ be observed segment-file bytes, $B$ the returned removed-byte count, and $h$ the bytes added for a replacement active segment. In this format $h$ is either 0 or 32. The equality checked is:

$$
D_{\mathrm{after}}=D_{\mathrm{before}}+h-B.
$$

When all nine records are covered, the old files contain 600 bytes. Retention creates a 32-byte empty replacement and removes the 600 old bytes, leaving $600+32-600=32$ bytes. Simply subtracting the before/after lengths would report 568 bytes and miss the replacement header. `checked_add` and `checked_sub` avoid allowing a corrupt response to wrap the arithmetic.

The fixture expects an empty active segment after every reclaim because both groups completed the entire available interval. Partial-retention behavior remains covered by Chapter 14. This check is not a general rule that a broker should always delete all data.

## Prove consumer loss and preserve its stale token

Add the victim helper. The child publishes its receipt only after a validated fetch; the parent validates those same payload bytes against its independent expectations, synchronizes its transcript, then kills and reaps the victim. The returned token remains owned by the parent for the restart fence check.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

Only one record is fetched by the victim. Its group remains unacknowledged and uncheckpointed. When the broker later restarts, the new window begins at zero and this record can be delivered again. The transcript retains both observations without declaring another stored record.

## Interrupt an append without fabricating a failure reply

Add the raw append helper. It intentionally uses the already explained protocol framing rather than `Client::append`, because the parent must withhold response reading and wait for a different observable event: the pre-synchronization marker.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

The trace is precise:

```text
parent: INTENT -> sync external ledger -> publish gate.arm -> send header/body
owner:  write record bytes -> enter GateSync -> publish gate.entered -> wait
parent: observe gate.entered -> record Ambiguous -> sync ledger -> kill child -> reap
parent: recover same broker directory using a new child; retain the original ledger
```

Successful socket writes do not turn the outcome into a confirmed append. Even seeing a complete record during recovery does not retroactively supply a producer success response. The later explicit application retry uses `attempt: 1`; the original remains ambiguous in the final report.

No consumer progress was checkpointed before this kill, so both groups restart from zero. The 16 records confirmed before the crash must appear in the new coverage scan, independently of whether the additional ambiguous record appears.

## Force an explicit disk refusal without an unread request body

The server can refuse append admission after reading only its fixed header. Sending a payload that the server will not read and then closing can introduce TCP reset behavior that obscures the refusal response. RFC 9293 discusses close with pending received data and reset signaling in [Section 3.6.1](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.6.1). That is a transport ambiguity, not evidence that the broker accepted a record.

For the deterministic refusal check, first fill the quiescent partition to its known capacity, then send **only** a header declaring a 24-byte body. Admission must return `DiskFull` before trying to read that body. If the implementation erroneously admits it, the exchange fails rather than pretending the correct refusal occurred. This specialized probe is not a new normal producer handshake.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

The probe checks the response against the exact request ID, operation, and scope before checking its status. Only a valid `DiskFull` response resolves the planned input as rejected. A missing response makes the run fail and conservatively leaves an ambiguous classification.

Add the bounded fill loop. It observes quiescent file lengths and predicts whether another conservative 88-byte reservation fits; the final header probe still tests the real broker admission decision. The expected 600-byte geometry is derived in Part 2, not read from an internal quota counter.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

This loop does not keep retrying a rejected request. It returns that input's identity to the caller, which retries it once **after** verified reclamation, with a new attempt number. The retry must receive the old exclusive end as its offset; deletion cannot reset the offset space to zero.

## Let one group make progress while the other pins storage

Append the pressure phase. Group 0 drains the newly appended records and checkpoints. Group 1 remains persisted at zero, even though its earlier volatile completion may be ahead. The first retention request must remove nothing. Only after group 1 covers the remaining records and checkpoints can reclamation proceed.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

The scan maps still contain the first post-restart coverage, because nothing has yet been deleted. Extending them with pressure records checks the whole retained interval rather than only the tail. Group 0's checkpoint proves urgent control operations can make progress under **disk admission pressure**. It does not prove control progress through indefinitely stuck filesystem I/O, an exhausted connection pool, or every form of payload saturation; Chapters 15 and 17 provide the corresponding focused cases.

The first retention request must leave both the floor and observed file lengths unchanged. This catches using volatile completion, only the fast group's progress, or a convenience memory flag as deletion authority.

## Repeat bounded traffic after reclamation

Add the steady-round helper and the final retained-restart check. Each steady round sends 16 new records, covers them for both groups, checkpoints, and reclaims. Fresh scan maps now start at the ledger's advanced floors. This gives a finite sustained sequence without allowing the external history or offered work to grow indefinitely.

```rust title="crates/broker-app/src/workload/scenario.rs"
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

```

The last restart takes place **after** legitimate deletion and healthy shutdown. It must restore both persisted/volatile frontiers to the retained floors and return no records. Requiring old deleted payloads here would be a false loss alarm; accepting a regressed frontier or resurrected prefix would be a real failure.

The group scans in this check are empty because every still-required interval is empty. The ledger retains historical coverage and checkpoint evidence below the floor, while current-epoch delivery evidence was cleared. This is why retention authority and recovery coverage must be modeled separately.

## Read the complete orchestration from top to bottom

Finish `scenario.rs` with the full `run` function. It creates a new directory, owns the ledger for the whole scenario, and gives each child a guard. The blocks are the three phases: crash coverage, overload followed by bounded reclamation rounds, and a final retained restart.

```rust title="crates/broker-app/src/workload/scenario.rs"
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
```

The first `drop(broker)` follows a successful kill/reap; it does not request a second termination. The second follows healthy shutdown/reap. If any earlier step returns an error, the corresponding guard still owns its child and performs fallback cleanup. The root directory and transcript remain for diagnosis.

Struct update syntax such as `Key { attempt: 1, ..ambiguous }` copies the other fields from a `Copy` key. It is an explicit new append attempt for the same logical input, not a magic deduplication request. The broker has no idempotency table for these workload bytes.

The `PASS` transcript line is written only after both healthy exits, the retained restart check, and the required directed events. The human-readable report is also written only then. A ledger/report write failure still returns an error; the command-line binary does not print a success message. An earlier transcript can contain useful successful events without proving that the whole scenario finished.

The next part makes the driver executable and checks the checker itself, including mutations that would otherwise let missing records pass.
