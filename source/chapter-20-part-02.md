# Chapter 20 — Resource measurement and one justified optimization

## Part 2 — Finite workloads and the index-density candidate

> **Learning objectives**
> Build a finite closed-loop and paced generator, preserve every offered outcome, check confirmed bytes for both groups, and derive the scan/memory tradeoff of a denser sparse index.

Continue from [Part 1](https://25d-228.github.io/rust-learning/chapter-20.html). The chapter's total estimate remains **4.5 hours across four parts**. This part creates the public measurement configuration and the asynchronous load module. The synchronous lifecycle and report writer follow in [Part 3](https://25d-228.github.io/rust-learning/chapter-20-part-03.html).

## Change one variable at a time

Use three profiles. `single` commits one record at a time. `batched` allows two records per batch. `dense` retains the same batching policy and changes only the sparse-index stride. All use the same real file/directory synchronization and the same confirmed-append contract.

| Setting | `single` | `batched` | `dense` |
| --- | ---: | ---: | ---: |
| Maximum records per group commit | 1 | 2 | 2 |
| Maximum group-commit delay | 2 ms | 2 ms | 2 ms |
| Maximum pending encoded bytes | 2 MiB | 2 MiB | 2 MiB |
| Index entry every this many records | 64 | 64 | 8 |
| Segment size | 8 MiB | 8 MiB | 8 MiB |
| Producer tasks / partitions | 8 / 4 | 8 / 4 | 8 / 4 |

There are two producers per partition. A maximum batch of two is reachable without increasing concurrency. A deadline can still commit a singleton; “batch size two” is a ceiling, not an observation that every sync covered two records. Measure actual groups. The index-density candidate is deliberately a small, reversible change that uses the already implemented index mechanism. Production defaults remain unchanged until a measured decision justifies changing them.

Each partition gets four append buffers, two response buffers holding 32 encoded records each, a 64-slot lease window, a 60-second lease, explicit-only checkpoints, and independent ordinary/urgent lane capacities. A 64 MiB normal disk quota per partition keeps the finite experiment away from its intentional overload limit; Chapter 19 tests that limit separately. The maximum profile still fits at most 16 segments per partition. No limit is removed to obtain a larger number.

Replace the small library root, then create `measurement.rs`. The public configuration validates inputs before creating any run directory. `Config` is `Copy`: its fields are small values, not owned sockets, buffers, or handles.

```rust title="crates/broker-app/src/lib.rs"
pub mod client;
pub mod local;
pub mod measurement;
pub mod metrics;
pub mod protocol;
pub mod server;
pub mod supervisor;
pub mod workload;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
```

```rust title="crates/broker-app/src/measurement.rs"
use std::io;
use std::path::Path;
use std::time::Duration;
use broker_core::budget::{DiskQuota, MemoryConfig};
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::types::GroupId;
use broker_core::worker::BatchConfig;
use crate::BoxError;
use crate::local::OwnerConfig;

mod experiment;
mod load;
mod observed;
mod report;
#[cfg(test)]
mod tests;
pub use experiment::{Summary, execute};

pub const PRODUCERS: usize = 8;
pub const FETCH_RECORDS: usize = 32;
pub const SEGMENT_BYTES: u64 = 8 * 1024 * 1024;
pub const OP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile { Single, Batched, Dense }
impl Profile {
    pub fn parse(value: &str) -> io::Result<Self> {
        match value { "single" => Ok(Self::Single), "batched" => Ok(Self::Batched),
            "dense" => Ok(Self::Dense), _ => Err(invalid("profile must be single, batched, or dense")) }
    }
    pub fn name(self) -> &'static str {
        match self { Self::Single => "single", Self::Batched => "batched", Self::Dense => "dense" }
    }
    pub fn stride(self) -> u64 { if self == Self::Dense { 8 } else { 64 } }
    pub fn batch_records(self) -> usize { if self == Self::Single { 1 } else { 2 } }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode { Closed, Paced }
impl Mode {
    pub fn parse(value: &str) -> io::Result<Self> {
        match value { "closed" => Ok(Self::Closed), "paced" => Ok(Self::Paced),
            _ => Err(invalid("mode must be closed or paced")) }
    }
    pub fn name(self) -> &'static str { if self == Self::Closed { "closed" } else { "paced" } }
}
```

`fetch_bytes` counts 32-byte record headers plus payloads. It does not count TCP token/envelope overhead because this experiment does not use TCP. Scratch remains the fixed owner allocation from Chapter 15. The pools can therefore be much smaller than the broker's general-purpose defaults without changing their ownership rules.

`phase` writes a temporary label then renames it within the new experiment directory. The Python sampler uses it as a best-effort observation tag. It is **not** synchronized authority, a recovery marker, or proof of an ordering boundary. A missed tag change is acceptable; missing a confirmed record is not.

```rust title="crates/broker-app/src/measurement.rs"
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub profile: Profile, pub mode: Mode, pub offers_per_producer: usize,
    pub payload_bytes: usize, pub period_us: u64,
}
impl Config {
    pub fn validate(self) -> io::Result<()> {
        require((1..=4096).contains(&self.offers_per_producer), "offers must be in 1..=4096")?;
        require((24..=4096).contains(&self.payload_bytes), "payload bytes must be in 24..=4096")?;
        require(match self.mode { Mode::Closed => self.period_us == 0,
            Mode::Paced => (1000..=10_000).contains(&self.period_us) },
            "closed mode requires period 0; paced mode requires 1000..=10000 microseconds")
    }
    pub fn owners(self) -> Result<OwnerConfig, BoxError> {
        self.validate()?;
        Ok(OwnerConfig {
            batch: BatchConfig::new(self.profile.batch_records(), 2 * 1024 * 1024,
                Duration::from_millis(2))?,
            lanes: [8, 8, 16], window: 64, lease_ms: 60_000,
            checkpoint: CheckpointPolicy::explicit_only(),
            memory: MemoryConfig::new(self.payload_bytes, 4, self.fetch_bytes(), 2, 8)?,
        })
    }
    pub fn fetch_bytes(self) -> usize { FETCH_RECORDS * (32 + self.payload_bytes) }
    pub fn quota(self) -> Result<DiskQuota, BoxError> {
        Ok(DiskQuota::new(64 * 1024 * 1024, 2 * 1024 * 1024)?)
    }
}
fn invalid(message: &str) -> io::Error { io::Error::new(io::ErrorKind::InvalidInput, message) }
fn require(condition: bool, message: &str) -> io::Result<()> {
    if condition { Ok(()) } else { Err(io::Error::other(message.to_owned())) }
}
fn g(index: usize) -> GroupId { GroupId::new(index as u8).expect("fixed group") }
fn phase(root: &Path, name: &str) -> io::Result<()> {
    // An observation label only, not a durable marker or a proof of event order.
    std::fs::write(root.join("phase.tmp"), name)?;
    std::fs::rename(root.join("phase.tmp"), root.join("phase.txt"))
}
```

## Predict the sparse-index work before measuring time

Chapter 15's `Segment::copy_record` begins at the predecessor index entry, validates forward, and copies the target into a reserved response buffer. A leased fetch can select retries and new offsets, so its bounded path calls this routine for each selected record. For contiguous offsets it currently restarts that predecessor scan for every target. The older ordinary multi-record `read_into` routine has a different scan pattern; do not attribute this formula to every read API.

With stride $s$, a full block of $s$ successive target records causes $1+2+\cdots+s=s(s+1)/2$ validated record visits. For $n=qs+r$ records in **one segment**, where $q$ is the complete-block count and $0\le r<s$, the total is:

$$
S(n,s)=q\frac{s(s+1)}2+\frac{r(r+1)}2.
$$

The partial block contributes $1+\cdots+r$. Zero records require zero visits. Rotation resets the sparse index at the next segment's base, so sum this formula separately for each segment. `geometry` in Part 3 performs that split. Use `u128` for the derived multiplication; its domain is wider than the experiment needs and avoids a misleading overflow in a cost calculation.

```rust title="crates/broker-app/src/measurement.rs"
// Cost of independent predecessor scans for n contiguous records in ONE segment.
pub fn expected_scans(records: u64, stride: u64) -> u128 {
    assert!((1..=4096).contains(&stride));
    let (records, stride) = (u128::from(records), u128::from(stride));
    let (blocks, tail) = (records / stride, records % stride);
    blocks * stride * (stride + 1) / 2 + tail * (tail + 1) / 2
}
```

For one partition with 2,048 records and a 1,024-byte payload, the record length is $32+1024=1056$ bytes. Including the segment header, $32+2048\cdot1056=2{,}162{,}720$ bytes fits one 8 MiB segment.

| Quantity for one group and partition | Stride 64 | Stride 8 |
| --- | ---: | ---: |
| Sparse index entries | 32 | 256 |
| Validated record visits | 66,560 | 9,216 |
| Visits per returned record | 32.5 | 4.5 |

Across four partitions and two groups, the predicted timed visits are 532,480 and 73,728. The index has 896 additional logical entries across four partitions. If the measured `size_of::<IndexEntry>()` is 16, those extra logical entries represent 14,336 inline bytes. This is **not** the extra `Vec` capacity, allocator overhead, or RSS: index vectors may reserve more entries than their length.

A roughly 7.22-fold reduction in these record visits is not a predicted 7.22-fold throughput improvement. Payload checking, token handling, acknowledgements, scheduling, synchronization, and filesystem behavior also consume time. A denser index can increase allocation and recovery work. The experiment will report both scan counts and elapsed results. Keep or revert the candidate based on those observations, not this arithmetic alone.

## Name inputs without allocating a payload per observation

Create `measurement/load.rs`. An input identity is `(producer, sequence)`. The payload begins with an eight-byte magic and two big-endian `u64` values, followed by deterministic bytes. Every requested byte is initialized directly in the admitted pool buffer. The checker regenerates at most 4,096 bytes on its stack and compares the entire payload, not just a prefix.

Expected identities bind to returned offsets after producers finish. That lets producer interleavings vary without weakening coverage. The experiment creates a new log and accepts no ambiguous appends for comparison, so confirmed offsets must form a contiguous interval beginning at zero in each partition.

```rust title="crates/broker-app/src/measurement/load.rs"
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
```

## Pacing must not hide a queue in the generator

Closed-loop load starts another request only after the previous outcome. It measures achieved work at fixed concurrency, but naturally slows its offered rate when the broker slows. The paced mode uses a fixed grid and exposes missed offers rather than claiming to maintain an impossible offered rate with an unbounded queue.

Let $t_0$ be the common origin, $p$ the per-producer period, and $j$ an offer index. Its nominal time is $t_j=t_0+jp$. After waking, compute the latest eligible index from elapsed time, bounded by the finite offer count. Skip older unstarted slots and count them. Start at most the latest eligible slot; never burst through the missed prefix. The helper is called only after waiting until the next candidate's time.

For period 1,000 microseconds and five offers, suppose offer 0 finished and the task wakes at 3,400 microseconds. It starts offer 3, counts offers 1 and 2 as missed, and next considers offer 4. At 5,000 microseconds the offer window has ended; remaining slots are missed, not queued. A task can complete an already started request after the window ends; measured duration includes that drain.

Tokio's timer documentation states millisecond granularity rather than high-resolution timing guarantees. This runner therefore accepts periods of 1–10 milliseconds and records scheduler lateness; it does not promise exact submillisecond arrivals. See [`sleep_until`](https://docs.rs/tokio/latest/tokio/time/fn.sleep_until.html).

```rust title="crates/broker-app/src/measurement/load.rs"
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
```

The scheduled rate is $8\cdot10^6/p$ offers per second when $p$ is expressed in microseconds, but all eight streams initially share an origin. This is an aligned finite periodic workload, not Poisson arrivals or a smooth production distribution. A 2,000-microsecond period schedules 4,000 offers per second; started and confirmed rates can be lower. Skipped offers do not receive fabricated latency samples. Their count must accompany the started-request distributions.

The finite categories below separate admission deadlines, disk, capacity, closure, invalid input, and an explicit owner refusal. Arithmetic on these counters is bounded by the validated maximum run size. Histogram arithmetic is independently checked.

```rust title="crates/broker-app/src/measurement/load.rs"
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
```

## A producer measures the request it actually owns

The following complete `producer` function holds at most one prepared append or submitted ticket. `prepare_append` reserves capacity before payload filling. Its one-second admission deadline is separate from the 30-second post-submission observation deadline. A cancellation before submission returns reservations; a timeout after handoff does not undo the owner operation.

The timer for successful/unsuccessful latency starts before admission. `submit_to_reply` starts immediately before handoff and can include a tiny amount of handoff work. Only a confirmed reply adds an expected `(offset, identity)` pair. `RequestError::Refused` is decisive nonacceptance. Owner failure, owner stop, dropped reply, and post-submit timeout remain ambiguous. The test does not reinterpret them as fast failures merely to improve throughput.

```rust title="crates/broker-app/src/measurement/load.rs"
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
```

The result vector is preallocated to the finite number of offers and contains only small identities and offsets, not copied payloads. `next` increments for a started offer even when that attempt is refused. In paced mode, `scheduled_to_finish` uses the original eligible grid time, so scheduler delay is not silently removed. In closed mode that population is empty and later prints `NA`.

This is not a coordinated-omission correction algorithm. It is an explicit finite workload with no generator backlog, an outcome for every planned slot, and separate lateness observations for started requests. A comparison that ignores `missed` would still be misleading.

## Join every task before trusting its totals

`append` starts all tasks against one origin, joins them all, and aborts the remaining tasks after a peer panic. Aborting is followed by draining the `JoinSet`; the function does not drop live task handles and call cleanup complete. In paced mode it also waits through the planned offer horizon, so a fast finish of the last started request does not shorten the denominator by the final period.

The bounded slot vectors detect offset collisions and gaps. An ambiguous run still returns its counters so `attempts.tsv` can explain failure, but its expected-record vectors are not invented from an uncertain scan. Part 3 refuses to compare that run.

```rust title="crates/broker-app/src/measurement/load.rs"
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
```

These fixed slot vectors are checker memory. They are allocated after the append clock stops, then remain present during timed consumption. Their cost appears in process RSS and consumer locality, not in the broker pool capacity table. Keep offers and payload sizes equal between baseline and candidate when interpreting memory results.

`AdmissionError` is a typed admission result rather than a standard error implementation. Convert it explicitly at this harness boundary. `receive` unwraps timeout, one-shot completion, and owner result in order; the three `?` operators correspond to those three failure layers, each propagated through `BoxError`. The helper never converts a timeout into a successful owner result.

```rust title="crates/broker-app/src/measurement/load.rs"
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
```

## Count useful delivery while retaining the response owner

For each group/partition pair, `consume_one` creates a fresh seen-vector whose length comes from producer confirmations. It asks the real bounded lease API for at most 32 records and checks every returned payload at its confirmed offset. It acknowledges each token before dropping the response batch. The response buffer therefore remains charged across those awaits, exercising the lifetime boundary rather than copying payloads into an unaccounted vector.

The loop is finite: at most twice the confirmation count plus one fetch cycles. Final coverage must include every confirmation, and the outer lifecycle rejects any duplicate delivery in this no-failure experiment. A false-empty fetch before coverage is an error. Completion is volatile throughout the timed pass; `persisted_next` must remain zero because checkpoint cost is not silently hidden in this consumer metric.

Duplicate/retired replies are recognizable protocol outcomes, but any repeated delivery makes this no-failure measurement invalid in Part 3. A paused debugger or a sufficiently slow machine can exceed the 60-second lease; diagnose that rather than silently accepting a different workload.

```rust title="crates/broker-app/src/measurement/load.rs"
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
```

The outer `consume` function runs eight consumers, one for each group/partition pair. Its elapsed time includes task startup, payload verification, acknowledgement traffic, and joins. That is useful checked engine throughput, not raw disk-read bandwidth. Any error aborts and joins its peers before returning. Fresh invocation after restart creates entirely new seen-vectors.

```rust title="crates/broker-app/src/measurement/load.rs"
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
```

After both groups finish, expected useful work is twice confirmed appends even though storage contains only one copy. The `scanned` counter includes predecessor validation and returned records; `fetched` counts returned records. Their ratio measures amplification for this exact bounded read path. It does not measure physical device reads, because the operating system may satisfy repeated reads from cache.

