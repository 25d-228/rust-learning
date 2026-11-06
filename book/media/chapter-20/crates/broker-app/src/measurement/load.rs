use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout_at};
use broker_core::delivery::{AckOutcome, LeaseToken};
use broker_core::delivery::progress::ProgressAck;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::segment::ReadLimits;
use broker_core::worker::{AsyncTicket, BoundedClient, RequestError};
use crate::BoxError;
use crate::metrics::Histogram;
use crate::server::Clients;
use super::{Config, FETCH_RECORDS, Mode, OP_TIMEOUT, PRODUCERS, g, require};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Identity { pub producer: usize, pub sequence: usize }
pub(super) fn fill(bytes: &mut [u8], id: Identity) {
    assert!(bytes.len() >= 24);
    bytes[..8].copy_from_slice(b"MEASURE1");
    bytes[8..16].copy_from_slice(&(id.producer as u64).to_be_bytes());
    bytes[16..24].copy_from_slice(&(id.sequence as u64).to_be_bytes());
    for (index, byte) in bytes.iter_mut().enumerate().skip(24) {
        *byte = (index as u64).wrapping_mul(17).wrapping_add(id.producer as u64)
            .wrapping_add((id.sequence as u64).wrapping_mul(31)) as u8;
    }
}
pub(super) fn matches_payload(bytes: &[u8], expected: Identity, size: usize) -> bool {
    if bytes.len() != size || size < 24 || size > 4096 { return false; }
    let mut reference = [0; 4096]; fill(&mut reference[..size], expected);
    bytes == &reference[..size]
}

// Which scheduled offer is still eligible? No queue and no catch-up burst.
pub(super) fn latest_offer(next: usize, count: usize, elapsed_us: u64, period_us: u64)
    -> (Option<usize>, usize)
{
    assert!(period_us != 0 && next <= count);
    if next == count { return (None, 0); }
    let latest = usize::try_from(elapsed_us / period_us).unwrap_or(usize::MAX).min(count);
    if latest == count { return (None, count - next); }
    let chosen = next.max(latest);
    (Some(chosen), chosen - next)
}

#[derive(Clone, Default)]
pub(super) struct ProducerStats {
    pub started: u64, pub confirmed: u64, pub refused: u64, pub timed_out: u64,
    pub ambiguous: u64, pub missed: u64,
    pub admission: Histogram, pub successful: Histogram, pub unsuccessful: Histogram,
    pub submit_to_reply: Histogram, pub scheduled_to_finish: Histogram,
    // Finite categories, not strings or per-request labels.
    pub reasons: [u64; 6], // deadline, disk, capacity, closed, invalid, owner-refused
}
impl ProducerStats {
    pub fn merge(&mut self, other: &Self) {
        self.started += other.started; self.confirmed += other.confirmed;
        self.refused += other.refused; self.timed_out += other.timed_out;
        self.ambiguous += other.ambiguous; self.missed += other.missed;
        for (left, right) in self.reasons.iter_mut().zip(other.reasons) { *left += right; }
        self.admission.merge(&other.admission); self.successful.merge(&other.successful);
        self.unsuccessful.merge(&other.unsuccessful); self.submit_to_reply.merge(&other.submit_to_reply);
        self.scheduled_to_finish.merge(&other.scheduled_to_finish);
    }
    fn refusal(&mut self, error: AdmissionError) {
        self.refused += 1;
        let index = match error {
            AdmissionError::Deadline => { self.timed_out += 1; 0 },
            AdmissionError::DiskFull => 1,
            AdmissionError::Full | AdmissionError::MemoryFull | AdmissionError::WaitersFull => 2,
            AdmissionError::Closed => 3,
            _ => 4,
        };
        self.reasons[index] += 1;
    }
}
struct ProducerResult { producer: usize, stats: ProducerStats, records: Vec<(u64, Identity)> }
pub(super) struct Appended {
    pub stats: ProducerStats,
    pub expected: Arc<[Vec<Identity>; 4]>,
    pub elapsed: Duration,
}
async fn producer(client: BoundedClient, producer: usize, config: Config, origin: Instant)
    -> ProducerResult
{
    let mut stats = ProducerStats::default();
    let mut records = Vec::with_capacity(config.offers_per_producer);
    let mut next = 0;
    sleep_until(origin).await;
    while next < config.offers_per_producer {
        let scheduled = if config.mode == Mode::Paced {
            sleep_until(origin + Duration::from_micros(config.period_us * next as u64)).await;
            let elapsed = Instant::now().duration_since(origin).as_micros().min(u64::MAX as u128) as u64;
            let (chosen, missed) = latest_offer(next, config.offers_per_producer, elapsed, config.period_us);
            stats.missed += missed as u64;
            let Some(chosen) = chosen else { break; };
            next = chosen;
            Some(origin + Duration::from_micros(config.period_us * next as u64))
        } else { None };
        let id = Identity { producer, sequence: next }; next += 1;
        stats.started += 1;
        let started = StdInstant::now();
        let admitted = client.prepare_append(config.payload_bytes, Instant::now() + Duration::from_secs(1)).await;
        stats.admission.record(StdInstant::now().checked_duration_since(started));
        let mut confirmed = None;
        match admitted {
            Err(error) => stats.refusal(error),
            Ok(mut prepared) => {
                fill(prepared.payload_mut(), id);
                let sent = StdInstant::now();
                match prepared.submit() {
                    Err(error) => stats.refusal(error),
                    Ok(ticket) => {
                        let result = timeout_at(Instant::now() + OP_TIMEOUT, ticket).await;
                        stats.submit_to_reply.record(StdInstant::now().checked_duration_since(sent));
                        match result {
                            Ok(Ok(Ok(offset))) => { stats.confirmed += 1; confirmed = Some(offset.get()); },
                            Ok(Ok(Err(RequestError::Refused(_)))) => {
                                stats.refused += 1; stats.reasons[5] += 1;
                            }
                            Err(_) => { stats.timed_out += 1; stats.ambiguous += 1; },
                            _ => stats.ambiguous += 1,
                        }
                    }
                }
            }
        }
        let elapsed = StdInstant::now().checked_duration_since(started);
        if let Some(offset) = confirmed {
            records.push((offset, id)); stats.successful.record(elapsed);
        } else { stats.unsuccessful.record(elapsed); }
        if let Some(scheduled) = scheduled {
            stats.scheduled_to_finish.record(Instant::now().checked_duration_since(scheduled));
        }
    }
    ProducerResult { producer, stats, records }
}

pub(super) async fn append(clients: &Clients, config: Config) -> Result<Appended, BoxError> {
    let origin = Instant::now() + Duration::from_millis(20);
    let mut tasks = JoinSet::new();
    for producer_id in 0..PRODUCERS {
        tasks.spawn(producer(clients[producer_id % 4].clone(), producer_id, config, origin));
    }
    let mut results = Vec::with_capacity(PRODUCERS);
    let mut first_error = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(result) => results.push(result),
            Err(error) => { if first_error.is_none() { first_error = Some(error); } tasks.abort_all(); }
        }
    }
    // Every task has been joined, including cancellations after a peer panic.
    if let Some(error) = first_error { return Err(error.into()); }
    if config.mode == Mode::Paced {
        sleep_until(origin + Duration::from_micros(config.period_us * config.offers_per_producer as u64)).await;
    }
    let elapsed = Instant::now().duration_since(origin);
    let mut stats = ProducerStats::default();
    let mut slots: [Vec<Option<Identity>>; 4] = std::array::from_fn(|_|
        vec![None; 2 * config.offers_per_producer]);
    for result in results {
        stats.merge(&result.stats);
        for (offset, id) in result.records {
            let at = usize::try_from(offset)?;
            let slot = slots[result.producer % 4].get_mut(at)
                .ok_or_else(|| std::io::Error::other("confirmed offset exceeds the bounded experiment"))?;
            require(slot.replace(id).is_none(), "two confirmations named one position")?;
        }
    }
    require(stats.started + stats.missed == (PRODUCERS * config.offers_per_producer) as u64,
        "offered requests disappeared from counters")?;
    require(stats.confirmed + stats.refused + stats.ambiguous == stats.started,
        "started requests disappeared from outcomes")?;
    // An ambiguous write prevents a comparable run. Keep counters; no blind retry or invented identity.
    let mut expected: [Vec<Identity>; 4] = std::array::from_fn(|_| Vec::new());
    if stats.ambiguous == 0 {
        for (partition, positions) in slots.into_iter().enumerate() {
            let end = positions.iter().rposition(Option::is_some).map_or(0, |index| index + 1);
            for slot in positions.into_iter().take(end) {
                expected[partition].push(slot.ok_or_else(|| std::io::Error::other("gap in confirmed offsets"))?);
            }
        }
    }
    Ok(Appended { stats, expected: Arc::new(expected), elapsed })
}

pub(super) fn admission(error: AdmissionError) -> std::io::Error {
    std::io::Error::other(format!("measurement admission: {error:?}"))
}

pub(super) async fn receive<T>(ticket: AsyncTicket<T>) -> Result<T, BoxError> {
    Ok(timeout_at(Instant::now() + OP_TIMEOUT, ticket).await???)
}
#[derive(Clone, Default)]
pub(super) struct ConsumerStats {
    pub useful: u64, pub duplicates: u64, pub fetched: u64, pub scanned: u64,
    pub cycles: Histogram, pub acknowledgements: Histogram,
    pub first_token: Option<LeaseToken>,
}
impl ConsumerStats {
    fn merge(&mut self, other: &Self) {
        self.useful += other.useful; self.duplicates += other.duplicates;
        self.fetched += other.fetched; self.scanned += other.scanned;
        self.cycles.merge(&other.cycles); self.acknowledgements.merge(&other.acknowledgements);
        if self.first_token.is_none() { self.first_token = other.first_token; }
    }
}
pub(super) struct Consumed { pub stats: ConsumerStats, pub elapsed: Duration }
async fn consume_one(client: BoundedClient, partition: usize, group: usize,
    expected: Arc<[Vec<Identity>; 4]>, config: Config) -> Result<ConsumerStats, BoxError>
{
    let mut stats = ConsumerStats::default();
    let mut seen = vec![false; expected[partition].len()];
    let limits = ReadLimits::new(FETCH_RECORDS, config.fetch_bytes())?;
    // In this no-failure experiment a complete pass must make progress. Retries remain bounded.
    for _ in 0..(2 * seen.len() + 1) {
        if stats.useful as usize == seen.len() { break; }
        let start = StdInstant::now();
        let prepared = client.prepare_lease(g(group), limits, Instant::now() + OP_TIMEOUT).await.map_err(admission)?;
        let batch = receive(prepared.submit().map_err(admission)?).await?;
        require(batch.records().len() != 0, "false-empty delivery before confirmed coverage")?;
        stats.scanned += batch.scanned_records as u64;
        for record in batch.records() {
            let offset = usize::try_from(record.token.offset.get())?;
            let id = expected[partition].get(offset).ok_or_else(||
                std::io::Error::other("consumer observed an unconfirmed offset"))?;
            require(matches_payload(record.payload(), *id, config.payload_bytes), "payload differs from confirmation")?;
            stats.fetched += 1;
            if seen[offset] { stats.duplicates += 1; } else { seen[offset] = true; stats.useful += 1; }
            if stats.first_token.is_none() { stats.first_token = Some(record.token); }
            let ack_start = StdInstant::now();
            let ack = receive(client.try_ack_progress(g(group), record.token).map_err(admission)?).await?;
            stats.acknowledgements.record(StdInstant::now().checked_duration_since(ack_start));
            match ack {
                ProgressAck::Accepted(accepted) => require(accepted.ack.outcome == AckOutcome::Applied,
                    "unexpected accepted acknowledgement")?,
                ProgressAck::NotApplied { ack, .. } => require(matches!(ack.outcome,
                    AckOutcome::Duplicate | AckOutcome::AlreadyRetired), "acknowledgement rejected")?,
            }
        }
        drop(batch); // The pooled allocation remains owned until this point.
        stats.cycles.record(StdInstant::now().checked_duration_since(start));
    }
    require(stats.useful as usize == seen.len(), "finite consumer pass did not cover all confirmations")?;
    let progress = receive(client.try_progress(g(group)).map_err(admission)?).await?;
    require(progress.volatile_next.get() == seen.len() as u64 && progress.persisted_next.get() == 0,
        "timed consumers must complete without silently checkpointing")?;
    Ok(stats)
}
pub(super) async fn consume(clients: &Clients, expected: Arc<[Vec<Identity>; 4]>, config: Config)
    -> Result<Consumed, BoxError>
{
    let start = StdInstant::now();
    let mut tasks = JoinSet::new();
    for partition in 0..4 { for group in 0..2 {
        tasks.spawn(consume_one(clients[partition].clone(), partition, group, Arc::clone(&expected), config));
    } }
    let mut stats = ConsumerStats::default();
    let mut first_error: Option<BoxError> = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(result)) => stats.merge(&result),
            result => {
                if first_error.is_none() {
                    first_error = Some(match result { Ok(Err(error)) => error, Err(error) => error.into(),
                        Ok(Ok(_)) => unreachable!() });
                }
                tasks.abort_all();
            }
        }
    }
    if let Some(error) = first_error { return Err(error); }
    Ok(Consumed { stats, elapsed: StdInstant::now().checked_duration_since(start)
        .ok_or_else(|| std::io::Error::other("consumer clock reversed"))? })
}
