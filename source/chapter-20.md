# Chapter 20 — Resource measurement and one justified optimization

## Part 1 — Bounded distributions and explicit observation boundaries

> **Learning objectives**
> Measure completed work without hiding refusals; bound the measurement machinery itself; distinguish capacity, residency, and disk observations; and connect a proposed optimization to a cost model before deciding whether to retain it.

**Implementation time: 4.5 hours across all four parts**, not per page: 2.0 for instrumentation and measurement analysis, 1.5 for the focused index-density experiment, and 1.0 for regression checks and comparable repeats. Reading is excluded. Continue with [Part 2 — Finite workloads and the index-density candidate](https://25d-228.github.io/rust-learning/chapter-20-part-02.html), [Part 3 — Recovery, resource sampling, and comparable runs](https://25d-228.github.io/rust-learning/chapter-20-part-03.html), and [Part 4 — Regression checks and the retain-or-revert decision](https://25d-228.github.io/rust-learning/chapter-20-part-04.html).

**Prerequisite code:** the complete workspace through [Chapter 19](https://25d-228.github.io/rust-learning/chapter-19.html). Reuse its `LocalBroker`, real `FileSync`, bounded clients, asynchronous tickets, lease tokens, explicit checkpoints, retention, and recovery. Keep Chapter 18's failure schedules and Chapter 19's TCP/process-kill scenario as correctness regressions. Do not time their deliberate pauses or their per-record synchronized transcript as broker throughput.

The **15-file overlay** in `book/media/chapter-20/` replaces the small `broker-app` library root, the core buffer-pool module, the synchronization interface, and the owner module. It adds the bounded histogram, six measurement modules, a binary, integration tests, and two Python scripts. Apply it after overlays 1–19 to a separate learner workspace. All new files are supplied in full across filename-labelled fragments. For the existing pool and worker files, replace only the explicitly identified types and complete functions below; keep the other prerequisite code. The overlay also contains their complete resulting files. There is no new dependency, manifest setting, persistent encoding, or network protocol.

Assemble all four parts before compiling. `metrics.rs` starts here and receives its complete test module in Part 4; repeated blocks for a new filename concatenate in reading order. The code measures the real engine in one process, not the TCP server: four owner threads, eight producer tasks, two consumer groups, real files, and real synchronization. The generator, checker, and broker share that process. Report its RSS as **measurement-process RSS**, never as isolated broker memory.

## Choose the numerator before starting the clock

A producer can finish a request without creating a confirmed record. A consumer can receive the same offset twice without making twice as much useful progress. A small successful-latency distribution can also conceal many refusals. We therefore keep identities and outcomes separate from durations.

For one run let $O$ be scheduled offers, $S$ started attempts, $M$ skipped offers, $C$ confirmed appends, $R$ explicit refusals, and $A$ ambiguous outcomes. The accounting identities are:

$$
O=S+M,\qquad S=C+R+A.
$$

A timeout before submission is a refusal. A timeout after submission is ambiguous: the owner may still synchronize the record. `timed_out` is a subset of those outcomes, not a third term to add again. An ambiguous run preserves its attempt counters but is not admitted to the comparable-results table. There is no silent retry.

Append throughput is $C/T_a$, where $T_a$ is the append phase's elapsed seconds. Useful consumer throughput is $U/T_c$, where $U$ counts distinct checked **group/partition/offset** observations and $T_c$ is the consumer phase duration. With two complete groups and no intentional retries, $U=2C$. This is neither bytes per second nor the count of unique stored records. For example, 256 confirmations produce 512 useful group-records, not 512 distinct append positions.

The two phases are deliberately separate. No consumer competes with append traffic during the append phase; no producer runs during timed consumption. This makes batching and read amplification easier to attribute, but does not characterize a mixed live workload, server network overhead, or arbitrary production capacity. Chapter 19 continues to test the interacting end-to-end semantics.

The runner does not impose a throughput target. Its fixed maximum of 4,096 offers per producer bounds expected-record storage. Eight producers therefore plan at most 32,768 inputs. A closed-loop producer owns at most one outstanding request. A paced producer also owns at most one, and **drops obsolete schedule slots instead of allocating a backlog**.

## A timing boundary is part of the result

Use `Instant` for elapsed durations, not wall-clock timestamps. It is opaque and intended for monotonic comparisons; neither its resolution nor its treatment of suspended time is a cross-platform promise. `checked_duration_since` lets this experiment mark an impossible ordering invalid rather than quietly treating it as zero. See the [Rust `Instant` documentation](https://doc.rust-lang.org/std/time/struct.Instant.html).

| Distribution | Starts | Ends | Important exclusion or inclusion |
| --- | --- | --- | --- |
| Append admission | Immediately before `prepare_append` | Admission succeeds or fails | Includes waiting for the combined slot/buffer/disk predicate |
| Successful or unsuccessful append | Before admission | Final observed outcome | Includes admission and payload initialization; failures have their own distribution |
| Submit to reply | Immediately before `submit` | Ticket or timeout resolves | Includes owner queueing, batching, synchronization, and waking; not a pure service time |
| Scheduled to finish | Original eligible paced slot | Outcome observed | Includes scheduler lateness for started offers; skipped offers remain separate counts |
| Owner activity | After mailbox receive returns | Activity returns | Excludes queue/idle wait; may include nested file synchronization |
| Data synchronization | Before the real `FileSync.sync` call | That call returns | Includes both successful and failed calls, with a separate failure count |
| Fetch/acknowledgement cycle | Before acquiring a response | Every returned record checked and acknowledged; batch dropped | Includes checking bytes and individual acknowledgements |
| Recovery | Before `recovery::recover` | Validated session returned | Includes scans, synchronization and epoch publication; excludes pool, owner and listener construction |

Owner service is an **activity distribution**, not the service time of each producer request. The append that triggers a count-bound batch pays the commit cost in its activity sample; earlier appends may only enqueue pending replies. A time-triggered commit belongs to `owner_deadline`. A control request can also trigger work. Do not subtract aggregate p99 service from aggregate p99 response latency and call the difference “queue p99”: the samples are neither paired nor identically partitioned.

Nested durations overlap. Data synchronization is inside some owner activities, which are inside producer response time. Adding all three double-counts elapsed work. Instrumentation itself has a cost: clocks, histogram updates, and a short observer mutex. That cost remains in comparisons and must be considered when interpreting small differences.

## Bound the distribution without pretending every percentile is exact

A vector of every request latency grows with the run. Instead, keep 257 counters. Values are integer microseconds after rounding each positive duration **up**. Zero remains zero. The first 16 buckets represent exact values 0–15. Above that, use eight equal-width buckets per power-of-two interval; a final bucket catches all values at least $2^{34}$ microseconds.

For a finite value $u\ge16$, choose exponent $e=\lfloor\log_2 u\rfloor$, width $w=2^{e-3}$, and subdivision $j=\lfloor(u-2^e)/w\rfloor$. Here $4\le e\le33$ and $0\le j<8$. Its index and inclusive bounds are:

$$
i=16+8(e-4)+j,\qquad [\ell,h]=[2^e+jw,\;2^e+(j+1)w-1].
$$

For 100 microseconds, $e=6$, $w=8$, and $j=4$. Bucket 36 represents 96–103 microseconds. Reporting 100 as an exact p99 from that bucket would invent information. The finite bucket's integer width is at most one eighth of its lower power-of-two boundary; rounding each duration contributes less than one additional microsecond of upward error.

Create `crates/broker-app/src/metrics.rs`. `leading_zeros` computes the exponent without floating-point logarithms. `bounds` is the inverse map used by output and tests.

```rust title="crates/broker-app/src/metrics.rs"
use std::io::{self, Write};
use std::time::Duration;

pub const BINS: usize = 257;

// All integer bounds are microseconds, after rounding each duration upward.
pub fn bounds(index: usize) -> (u64, Option<u64>) {
    assert!(index < BINS);
    if index < 16 { return (index as u64, Some(index as u64)); }
    if index == BINS - 1 { return (1u64 << 34, None); }
    let exponent = 4 + (index - 16) / 8;
    let width = 1u64 << (exponent - 3);
    let low = (1u64 << exponent) + ((index - 16) % 8) as u64 * width;
    (low, Some(low + width - 1))
}
fn bucket(value: u64) -> usize {
    if value < 16 { return value as usize; }
    if value >= 1u64 << 34 { return BINS - 1; }
    let exponent = 63 - value.leading_zeros() as usize;
    let width = 1u64 << (exponent - 3);
    16 + (exponent - 4) * 8 + ((value - (1u64 << exponent)) / width) as usize
}
```

The bins alone use $257\cdot8=2056$ bytes. The complete struct also holds count, sum, maximum, a validity flag, and any alignment padding. `size_of::<Histogram>()` reports that inline representation at runtime. It does not recursively include heap allocations; a `Vec`'s inline size is not the size of its backing buffer. The experiment records rather than assumes layout. See [`size_of`](https://doc.rust-lang.org/std/mem/fn.size_of.html).

`total_us` uses `u128`; counts use `u64`. Checked arithmetic turns overflow into an invalid result instead of a plausible wrapped count. Once invalid, a histogram is never reported as a usable distribution. The report writer rejects it. A `None` duration is similarly invalid.

```rust title="crates/broker-app/src/metrics.rs"
#[derive(Debug, Clone)]
pub struct Histogram {
    bins: [u64; BINS],
    pub count: u64,
    pub total_us: u128,
    pub max_us: u64,
    pub valid: bool,
}
impl Default for Histogram {
    fn default() -> Self {
        Self { bins: [0; BINS], count: 0, total_us: 0, max_us: 0, valid: true }
    }
}
impl Histogram {
    pub fn record(&mut self, elapsed: Option<Duration>) {
        let Some(elapsed) = elapsed else { self.valid = false; return; };
        let nanos = elapsed.as_nanos();
        let micros = nanos / 1000 + u128::from(nanos % 1000 != 0);
        match u64::try_from(micros) {
            Ok(value) => self.record_us(value),
            Err(_) => self.valid = false,
        }
    }
    pub fn record_us(&mut self, value: u64) {
        let index = bucket(value);
        let next = (self.count.checked_add(1), self.bins[index].checked_add(1),
            self.total_us.checked_add(u128::from(value)));
        let (Some(count), Some(bin), Some(total)) = next else { self.valid = false; return; };
        self.count = count; self.bins[index] = bin; self.total_us = total;
        self.max_us = self.max_us.max(value);
    }
```

Merge **counts**, not percentile answers. A producer with one sample at 100 microseconds and another with 99 samples at 1 microsecond produce a pooled nearest-rank p99 of 1, not the average of their individual p99 values.

For $n$ observations and requested fraction $a/b$, select rank $k=\lceil na/b\rceil$. Walk cumulative bucket counts until they reach $k$, then return that bucket's interval. Five samples `[1, 2, 3, 16, 100]` have median `[3,3]` and p95/p99 `[96,103]`. With so few samples, the high quantiles select the same observation; extra decimal places would not create confidence.

Continue the `impl Histogram` block. A partial merge that detects overflow can leave some bins changed, but its `valid=false` prevents any comparison or percentile output from using that state. No recovery from overflow is attempted.

```rust title="crates/broker-app/src/metrics.rs"
    pub fn merge(&mut self, other: &Self) {
        self.valid &= other.valid;
        let (Some(count), Some(total)) = (self.count.checked_add(other.count),
            self.total_us.checked_add(other.total_us)) else { self.valid = false; return; };
        for (left, right) in self.bins.iter_mut().zip(other.bins.iter()) {
            let Some(value) = left.checked_add(*right) else { self.valid = false; return; };
            *left = value;
        }
        self.count = count; self.total_us = total; self.max_us = self.max_us.max(other.max_us);
    }
    pub fn quantile(&self, numerator: u64, denominator: u64) -> Option<(u64, Option<u64>)> {
        if !self.valid || self.count == 0 || denominator == 0
            || numerator == 0 || numerator > denominator { return None; }
        let rank = (u128::from(self.count) * u128::from(numerator))
            .div_ceil(u128::from(denominator));
        let mut cumulative = 0u128;
        for (index, count) in self.bins.iter().enumerate() {
            cumulative += u128::from(*count);
            if cumulative >= rank { return Some(bounds(index)); }
        }
        None
    }
```

Write both a compact summary and nonempty raw bins. An empty population is `NA`, not zero latency. The overflow bucket prints `inf` as its unknown upper bound, while `max_us` still records the exact largest rounded observation. This is bounded state even when output labels are formatted later; there is no per-request string, metric label, or logging queue.

```rust title="crates/broker-app/src/metrics.rs"
    pub fn write(&self, name: &str, summary: &mut impl Write, bins: &mut impl Write) -> io::Result<()> {
        write!(summary, "{name}\t{}\t{}\t{}\t{}", self.valid, self.count, self.total_us, self.max_us)?;
        for numerator in [50, 95, 99] {
            match self.quantile(numerator, 100) {
                Some((low, high)) => write!(summary, "\t{low}\t{}",
                    high.map(|value| value.to_string()).unwrap_or_else(|| "inf".to_owned()))?,
                None => write!(summary, "\tNA\tNA")?,
            }
        }
        writeln!(summary)?;
        for (index, count) in self.bins.iter().enumerate().filter(|(_, count)| **count != 0) {
            let (low, high) = bounds(index);
            writeln!(bins, "{name}\t{low}\t{}\t{count}",
                high.map(|value| value.to_string()).unwrap_or_else(|| "inf".to_owned()))?;
        }
        Ok(())
    }
}
```

## Count capacity at the transition, not at a polling interval

Chapter 15's pools charge the vector's allocation capacity, not the requested payload length. A response can borrow that allocation across suspension. Sampling `checked_out_bytes` every 200 milliseconds can miss a reservation that lasts 100 microseconds. Record the high-water value while the existing pool lock protects checkout.

Let $B(t)$ be checked-out capacity at a transition and $H$ the previous high-water value. Successful checkout performs $H\leftarrow\max(H,B(t))$. Return decreases $B$ but never $H$. This is exact for this pool's transitions during this resource generation. It is not an RSS measurement and not necessarily a simultaneous peak across different pools.

In `crates/broker-core/src/budget/memory.rs`, replace `PoolState` and `PoolInner` with these complete definitions. Keep the existing imports, configuration, allocation helper, and `BufferPool` struct.

```rust title="crates/broker-core/src/budget/memory.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolState {
    pub allocated_bytes: usize,
    pub checked_out_bytes: usize,
    pub peak_checked_out_bytes: usize,
    pub free_buffers: usize,
    pub block_bytes: usize,
}
struct PoolInner {
    free: Vec<Vec<u8>>,
    allocated: usize,
    checked_out: usize,
    peak: usize,
    block: usize,
}
```

Replace the complete `impl BufferPool` block. The only behavioral additions are initializing `peak`, returning it in `state`, and updating it after successful checkout. Failure before removing a free buffer does not change a peak. A zero-length checkout still owns a complete allocation and is charged accordingly.

Keep `PoolBuffer`, its return-on-drop implementation, and the waiter gate unchanged. Pool return already uses the same lock; no extra lock or atomic is needed on the hot path. `Mutex` guards release the lock when dropped, and poisoning is advisory rather than a recovery guarantee; see the [Rust mutex documentation](https://doc.rust-lang.org/std/sync/struct.Mutex.html). This change does not reinterpret the existing pool's failure policy.

```rust title="crates/broker-core/src/budget/memory.rs"
impl BufferPool {
    pub(crate) fn new(size: usize, count: usize, ceiling: usize, changed: Arc<Notify>)
        -> Result<Self, StorageError>
    {
        let mut free = Vec::new();
        free.try_reserve_exact(count).map_err(|source| StorageError::Allocation {
            operation: "preallocate free-buffer entries", source,
        })?;
        let mut allocated = 0usize;
        for _ in 0..count {
            let bytes = allocate_bytes(size)?;
            allocated = allocated.checked_add(bytes.capacity())
                .filter(|n| *n <= ceiling)
                .ok_or(StorageError::Config("allocator-reported capacities exceed the pool ceiling"))?;
            free.push(bytes);
        }
        Ok(Self { inner: Arc::new(Mutex::new(PoolInner {
            free, allocated, checked_out: 0, peak: 0, block: size,
        })), changed })
    }
    pub(crate) fn state(&self) -> PoolState {
        let p = self.inner.lock().expect("buffer-pool mutex poisoned");
        PoolState { allocated_bytes: p.allocated, checked_out_bytes: p.checked_out,
            peak_checked_out_bytes: p.peak,
            free_buffers: p.free.len(), block_bytes: p.block }
    }
    pub(crate) fn take(&self, len: usize) -> Result<PoolBuffer, AdmissionError> {
        let mut p = self.inner.lock().expect("buffer-pool mutex poisoned");
        if len > p.block { return Err(AdmissionError::TooLarge); }
        let bytes = p.free.pop().ok_or(AdmissionError::MemoryFull)?;
        p.checked_out += bytes.capacity();
        p.peak = p.peak.max(p.checked_out);
        drop(p);
        let mut value = PoolBuffer { pool: self.clone(), bytes: Some(bytes), used: len };
        // A caller never sees an earlier user's contents, even on a partially filled request.
        value.as_mut_slice().fill(0);
        Ok(value)
    }
}
```

Suppose a pool has two 64-byte-capacity buffers. Taking one with used length 1 gives `(checked_out, peak)=(64,64)`. Taking the second with used length 0 gives `(128,128)`. Dropping both gives `(0,128)`, not `(0,0)`. The actual test uses reported capacity rather than assuming the allocator returned exactly the request.

Four separate pool peaks can occur at different times. Their sum is an upper bound on simultaneous checked-out capacity, not an observed global maximum. Preallocated free buffers still consume allocation capacity. Fixed scratch, index vectors, thread stacks, task state, expected-record vectors, allocator overhead, and residency are separate categories.

## Observe the owner without moving its success boundary

The synchronization interface is already the seam for controlled failures. Add optional **observation**, not a replacement synchronization policy. Existing `SyncIo` implementers keep default no-op observation and do not pay service-clock reads. `FileSync` still performs real I/O. Replace this complete small file.

```rust title="crates/broker-core/src/storage/sync.rs"
use std::fs::File;
use std::io;
use std::path::Path;
use std::time::Duration;

use crate::io::StorageFile;
use crate::types::Offset;
use super::metadata::PublicationStep;

#[derive(Debug, Clone, Copy)]
pub enum OwnerActivity { Append, Control, Deadline, Finish }

// Each successful method must complete the named synchronization operation.
pub trait SyncIo {
    // Measurement only: observers must be bounded, nonpanicking, and perform no I/O.
    fn observes_service(&self) -> bool { false }
    fn service_observed(&mut self, _kind: OwnerActivity,
        _elapsed: Option<Duration>, _failed: bool) {}
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()>;

    fn sync_metadata(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    fn sync_checkpoint(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    fn sync_retention(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    // Successful implementations must unlink the named, closed segment.
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    // Test observation point AFTER each actual publication operation.
    fn publication_step(&mut self, _target: &Path, _step: PublicationStep) -> io::Result<()> {
        Ok(())
    }

    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }
}

pub struct FileSync;

impl SyncIo for FileSync {
    fn sync(&mut self, _base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }
}
```

In `crates/broker-core/src/worker.rs`, replace only `Worker::run` with this complete function. Its prerequisite imports already include `Instant`; the enum is imported locally. The mailbox receive happens **before** the service start. The closure around the deadline branch makes `?` return into `result`, so an ordinary error is observed before it propagates out of `run`.

`then(Instant::now)` is lazy: when observation is disabled, it does not call the clock. The implementation does not catch panics merely to produce a latency sample; an owner panic still follows Chapter 17's supervised failure path. Observation must not become a new source of I/O, unbounded allocation, or panics.

```rust title="crates/broker-core/src/worker.rs"
    fn run(&mut self) -> Result<WorkerState, Arc<StorageError>> {
        loop {
            let progress_deadline = match &self.delivery {
                DeliveryMode::Durable { progress, .. } => progress.deadline(),
                _ => None,
            };
            let deadline = self.deadline.into_iter().chain(progress_deadline).min();
            let event = self.mailbox.recv(deadline); // Queue/idle wait is outside service.
            let started = self.sync.observes_service().then(Instant::now);
            use crate::storage::sync::OwnerActivity;
            let kind = match &event {
                Event::Data(_) => OwnerActivity::Append,
                Event::Control(_) => OwnerActivity::Control,
                Event::Deadline => OwnerActivity::Deadline,
                Event::Closed => OwnerActivity::Finish,
            };
            let result: Result<Option<WorkerState>, Arc<StorageError>> = match event {
                Event::Data(command) => self.publish(command).map(|()| None),
                Event::Control(command) => self.control(command).map(|()| None),
                Event::Deadline => (|| {
                    if self.deadline.is_some_and(|end| end <= Instant::now()) { self.commit()?; }
                    self.checkpoint_due(Instant::now())?;
                    Ok(None)
                })(),
                Event::Closed => self.finish().map(Some),
            };
            if let Some(started) = started {
                self.sync.service_observed(kind,
                    Instant::now().checked_duration_since(started), result.is_err());
            }
            if let Some(state) = result? { return Ok(state); }
        }
    }
```

## Keep the observer bounded and return the original I/O result

Create `crates/broker-app/src/measurement/observed.rs`. There are ten named categories per owner, not one category per request. Four owners therefore retain forty histograms regardless of run length. `Observer` is an `Arc` so the harness can read the final counters after the worker is joined. Its mutex protects only these fixed counters. In the timed phase there is one writer per observer; we do not repeatedly copy histograms from another task.

A poisoned observation mutex marks the data invalid while preserving normal owner execution. `snapshot` copies the bounded result, and the report rejects invalid data. This is a measurement policy, not a general claim that poisoned application state is safe.

```rust title="crates/broker-app/src/measurement/observed.rs"
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use broker_core::io::StorageFile;
use broker_core::storage::sync::{FileSync, OwnerActivity, SyncIo};
use broker_core::types::Offset;
use crate::metrics::Histogram;

pub(super) const NAMES: [&str; 10] = ["owner_append", "owner_control", "owner_deadline", "owner_finish",
    "sync_data", "sync_metadata", "sync_checkpoint", "sync_retention", "sync_directory", "unlink"];
#[derive(Clone)]
pub(super) struct OwnerMetrics {
    pub hist: [Histogram; 10],
    pub failures: [u64; 10],
    pub valid: bool,
}
impl Default for OwnerMetrics {
    fn default() -> Self {
        Self { hist: std::array::from_fn(|_| Histogram::default()), failures: [0; 10], valid: true }
    }
}
#[derive(Clone, Default)]
pub(super) struct Observer(Arc<Mutex<OwnerMetrics>>);
impl Observer {
    pub fn record(&self, index: usize, elapsed: Option<Duration>, failed: bool) {
        let mut data = match self.0.lock() {
            Ok(data) => data,
            Err(error) => { let mut data = error.into_inner(); data.valid = false; data },
        };
        data.hist[index].record(elapsed);
        if failed {
            match data.failures[index].checked_add(1) {
                Some(value) => data.failures[index] = value,
                None => data.valid = false,
            }
        }
    }
    pub fn snapshot(&self) -> OwnerMetrics {
        match self.0.lock() {
            Ok(data) => data.clone(),
            Err(error) => { let mut data = error.into_inner().clone(); data.valid = false; data },
        }
    }
}
```

The wrapper delegates every synchronization and unlink to `FileSync`. It records elapsed duration and whether the **original call** failed, then returns that same result. No failed sync is converted into success. The normal publication observation hook remains the default no-op; publication itself still executes in the production metadata code.

`observe` accepts `FnOnce`: each real I/O action runs exactly once. Its closure borrows the file or path for that call. The closure does not escape the stack frame or acquire a payload owner.

```rust title="crates/broker-app/src/measurement/observed.rs"
pub(super) struct ObservedSync(pub Observer);
impl ObservedSync {
    fn observe(&self, index: usize, action: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let start = Instant::now();
        let result = action();
        let elapsed = Instant::now().checked_duration_since(start);
        self.0.record(index, elapsed, result.is_err());
        result // The original result, never an instrumentation success substituted for it.
    }
}
impl SyncIo for ObservedSync {
    fn observes_service(&self) -> bool { true }
    fn service_observed(&mut self, kind: OwnerActivity, elapsed: Option<Duration>, failed: bool) {
        let index = match kind { OwnerActivity::Append => 0, OwnerActivity::Control => 1,
            OwnerActivity::Deadline => 2, OwnerActivity::Finish => 3 };
        self.0.record(index, elapsed, failed);
    }
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(4, || FileSync.sync(base, file))
    }
    fn sync_metadata(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(5, || FileSync.sync_metadata(file))
    }
    fn sync_checkpoint(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(6, || FileSync.sync_checkpoint(file))
    }
    fn sync_retention(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(7, || FileSync.sync_retention(file))
    }
    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        self.observe(8, || FileSync.sync_directory(path))
    }
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        self.observe(9, || FileSync.remove_segment(path))
    }
}
```

The sum of `sync_data` calls need not equal `sync_groups`. One group can synchronize several dirty segment files, especially across rotation. Count one `sync_group` only when the owner advances the synchronized boundary for that batch. Neither count includes an external consumer effect.

The observer's file-duration sample ends before its own counter update. The enclosing owner sample includes the inner observer's bookkeeping, but excludes its own final callback. These are useful, explicit boundaries—not perfectly overhead-free clocks. Keep them identical between profiles.

