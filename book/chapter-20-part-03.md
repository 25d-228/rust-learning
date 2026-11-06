# Chapter 20 — Resource measurement and one justified optimization

## Part 3 — Recovery, resource sampling, and comparable runs

> **Learning objectives**
> Separate timed work from setup and correctness verification, retain resources through failure cleanup, sample the actual process without claiming exact residency peaks, and preserve comparable result files.

Continue from [Part 2](https://25d-228.github.io/rust-learning/chapter-20-part-02.html). The chapter's estimate remains **4.5 hours total**. This part supplies the complete lifecycle, report writer, command, and native Python runner. [Part 4](https://25d-228.github.io/rust-learning/chapter-20-part-04.html) contains tests, exact commands, and the acceptance decision.

## Warm code, not an imaginary cold disk

Each executable run creates two independent broker directories. The first is a 16-offer-per-producer closed-loop warmup. It completes the same coverage, recovery, checkpoint, and retention cycle, then releases its owners and clients. The measured case receives fresh logs, pools, observers, and counters.

This warms code paths, the runtime, and some system caches. It is not a cold-cache benchmark, a steady-state reuse of one log, or a guarantee that all pages are resident. Do not purge global caches or change synchronization semantics to make repeats look cleaner. Record actual machine load and use comparable profiles in alternating order.

Create `measurement/experiment.rs`. `Summary` carries the small comparison fields. `Details` owns the bounded counters and states until report writing. `geometry` divides records at the real whole-record segment boundary, with 32 bytes reserved for each segment header. A header or nonfitting record is never split just to fit the mathematical model.

```rust title="crates/broker-app/src/measurement/experiment.rs"
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use broker_core::budget::DiskState;
use broker_core::delivery::{AckOutcome, AckRejection};
use broker_core::delivery::clock::MonotonicClock;
use broker_core::delivery::progress::ProgressAck;
use broker_core::storage::recovery;
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::FileSync;
use broker_core::worker::MemoryState;
use tokio::runtime::Runtime;
use crate::BoxError;
use crate::local::LocalBroker;
use super::{Config, Mode, SEGMENT_BYTES, expected_scans, g, phase, require};
use super::load::{self, Appended, Consumed, admission, receive};
use super::observed::{ObservedSync, Observer, OwnerMetrics};
use super::report;

#[derive(Debug, Clone)]
pub struct Summary {
    pub confirmed: u64, pub refused: u64, pub missed: u64,
    pub useful: u64, pub scanned: u64, pub sync_groups: u64,
    pub append_seconds: f64, pub consumer_seconds: f64, pub recovery_seconds: f64,
    pub index_entries: u64,
    pub retained: [u64; 4],
}
pub(super) struct Details {
    pub appended: Appended, pub consumed: Consumed,
    pub owners: [OwnerMetrics; 4], pub memory: [MemoryState; 4],
    pub disk_before: [DiskState; 4], pub disk_after: [DiskState; 4],
    pub recovery: Duration, pub summary: Summary,
}
fn duration(start: Instant) -> Result<Duration, BoxError> {
    Instant::now().checked_duration_since(start)
        .ok_or_else(|| std::io::Error::other("measurement clock reversed").into())
}
pub(super) fn geometry(records: usize, payload: usize, stride: u64) -> (u128, u64) {
    let per_segment = (SEGMENT_BYTES as usize - 32) / (32 + payload);
    let mut remaining = records;
    let (mut scans, mut entries) = (0, 0);
    while remaining != 0 {
        let count = remaining.min(per_segment) as u64;
        scans += expected_scans(count, stride);
        entries += count.div_ceil(stride);
        remaining -= count as usize;
    }
    (scans, entries)
}
```

## Finish timed work, then release its resource generation

`case` is synchronous and receives a runtime owned by its caller. Blocking owner joins happen outside `runtime.block_on`, so they do not prevent the same current-thread runtime from polling an async task required by the test. The broker's owner threads themselves perform blocking filesystem I/O as in the earlier chapters.

The following is the first fragment of the complete `case` function; continue it with the next two fragments. The inner closure captures work errors rather than using `?` to bypass explicit joining. Read memory and disk states after the asynchronous tasks have joined. Then close/join the broker and drop the client array before starting a replacement generation. `LocalBroker` also has a blocking fallback destructor, but normal and ordinary error paths here join explicitly.

The plain `broker.join()` flushes appends; it deliberately does **not** request Chapter 17's final consumer checkpoints. This leaves validated volatile completion ahead of persisted progress, which is necessary for the subsequent fresh recovery-coverage pass.

```rust title="crates/broker-app/src/measurement/experiment.rs"
fn case(root: &Path, outer: &Path, config: Config, runtime: &Runtime, label: &str)
    -> Result<Summary, BoxError>
{
    fs::create_dir(root)?;
    phase(outer, &format!("{label}:initialize"))?;
    let data = root.join("broker");
    let session = recovery::initialize_budgeted(&data,
        StorageConfig::new(SEGMENT_BYTES, config.profile.stride(), 16)?, config.quota()?)?;
    let original_epoch = session.epoch();
    let observers: [Observer; 4] = std::array::from_fn(|_| Observer::default());
    let broker = LocalBroker::from_session(session, config.owners()?, Arc::new(MonotonicClock::new()),
        |partition| ObservedSync(observers[usize::from(partition.get())].clone()))?;
    let clients = broker.clients();
    // Capture the work result, then explicitly join the owner threads on BOTH success and error.
    let work = (|| -> Result<(Appended, Consumed, u64), BoxError> {
        phase(outer, &format!("{label}:append"))?;
        let appended = runtime.block_on(load::append(&clients, config))?;
        report::write_attempts(root, config, &appended)?;
        require(appended.stats.ambiguous == 0, "ambiguous append: counters retained; do not compare this run")?;
        phase(outer, &format!("{label}:consume"))?;
        let consumed = runtime.block_on(load::consume(&clients, Arc::clone(&appended.expected), config))?;
        let groups = runtime.block_on(async {
            let mut groups = 0u64;
            for (index, client) in clients.iter().enumerate() {
                let state = receive(client.try_snapshot().map_err(admission)?).await?;
                require(state.visible_end.get() == appended.expected[index].len() as u64
                    && state.pending_records == 0, "snapshot disagrees with confirmations")?;
                groups += state.sync_groups;
            }
            Ok::<_, BoxError>(groups)
        })?;
        Ok((appended, consumed, groups))
    })();
    let memory: [MemoryState; 4] = std::array::from_fn(|index| clients[index].memory_state());
    let disk_before: [DiskState; 4] = std::array::from_fn(|index| clients[index].disk_state());
    let joined = broker.join(); // Plain close flushes appends but deliberately does not checkpoint.
    drop(clients); // Release old resource-generation handles before reopening the directory.
    let owners: [OwnerMetrics; 4] = std::array::from_fn(|index| observers[index].snapshot());
    drop(observers);
    let (appended, consumed, sync_groups) = match work {
        Ok(value) => { joined?; value },
        Err(error) => { if let Err(join_error) = joined { eprintln!("cleanup also failed: {join_error}"); }
            return Err(error); }
    };
    require(consumed.stats.duplicates == 0, "unexpected retry in the no-failure measurement")?;
    let (mut predicted_scans, mut index_entries) = (0u128, 0u64);
    for identities in appended.expected.iter() {
        let (scans, entries) = geometry(identities.len(), config.payload_bytes, config.profile.stride());
        predicted_scans += 2 * scans; index_entries += entries;
    }
    require(u128::from(consumed.stats.scanned) == predicted_scans, "observed scans differ from segment geometry")?;
    require(consumed.stats.useful == 2 * appended.stats.confirmed, "both groups must cover every confirmation")?;
    for state in &memory {
        require(state.append.checked_out_bytes == 0 && state.response.checked_out_bytes == 0,
            "a timed payload owner remains alive")?;
    }
```

The predicted scan count is checked against actual counters, not substituted for them. Both groups must cover every confirmation, and all timed payload owners must have returned their buffers. These are conditions for a valid measurement, not performance assertions.

Recovery timing begins with the populated log still intact. It includes the real startup validation, record scanning, file/directory synchronization, and epoch publication. It stops before pool allocation and owner startup. Timing recovery after retention had already deleted everything would answer an easier, different question.

```rust title="crates/broker-app/src/measurement/experiment.rs"
    phase(outer, &format!("{label}:recover"))?;
    let start = Instant::now();
    let session = recovery::recover(&data)?;
    let recovery = duration(start)?;
    require(session.epoch() > original_epoch && session.repairs().is_empty(), "ordinary recovery changed the fixture")?;
    let broker = LocalBroker::from_session(session, config.owners()?, Arc::new(MonotonicClock::new()), |_| FileSync)?;
    let clients = broker.clients();
    let verify = runtime.block_on(async {
        phase(outer, &format!("{label}:verify-recovery"))?;
        if let Some(token) = consumed.stats.first_token {
            let result = receive(clients[usize::from(token.partition.get())]
                .try_ack_progress(token.group, token).map_err(admission)?).await?;
            require(matches!(result, ProgressAck::NotApplied { ack, .. }
                if ack.outcome == AckOutcome::Rejected(AckRejection::WrongEpoch)),
                "old epoch was not fenced")?;
        }
        // A fresh seen-set is created inside consume; pre-restart observations cannot cover this pass.
        let recovered = load::consume(&clients, Arc::clone(&appended.expected), config).await?;
        require(recovered.stats.useful == consumed.stats.useful && recovered.stats.duplicates == 0,
            "restart coverage differs from confirmed work")?;
        phase(outer, &format!("{label}:checkpoint-retain"))?;
        let mut retained = [0u64; 4];
        for (partition, client) in clients.iter().enumerate() {
            let end = appended.expected[partition].len() as u64;
            for group in 0..2 {
                let checkpoint = receive(client.try_checkpoint(g(group)).map_err(admission)?).await?;
                require(checkpoint.progress.persisted_next.get() == end, "checkpoint failed to cover checked work")?;
            }
            let result = receive(client.try_retention().map_err(admission)?).await?;
            require(result.floor.get() == end && result.disk.file_bytes == 32,
                "whole-log reclamation must leave a single empty active header")?;
            retained[partition] = result.floor.get();
        }
        Ok::<_, BoxError>(retained)
    });
    let disk_after: [DiskState; 4] = std::array::from_fn(|index| clients[index].disk_state());
    let joined = broker.join(); drop(clients);
    let retained = match verify {
        Ok(value) => { joined?; value },
        Err(error) => { if let Err(join_error) = joined { eprintln!("cleanup also failed: {join_error}"); }
            return Err(error); }
    };
    // Reopen once more to validate the published floor and the now-retained topology.
    let retained_session = recovery::recover(&data)?;
    let (_, logs) = retained_session.into_parts();
    for (partition, log) in logs.iter().enumerate() {
        require(log.retained_floor().get() == retained[partition]
            && log.written_end().get() == retained[partition], "retained restart resurrected records")?;
    }
    drop(logs);
    require(!appended.elapsed.is_zero() && !consumed.elapsed.is_zero() && !recovery.is_zero(),
        "zero duration is not a throughput measurement")?;
```

An old token must be rejected specifically for `WrongEpoch`. Fresh consumers then check all confirmed bytes again with empty seen-vectors. These checks happen outside the reported timed consumer phase. Only after fresh coverage does the harness checkpoint both groups and request retention. The retained log must end at the original exclusive end and contain one empty 32-byte active segment per partition.

A second reopen validates that the published floor survives and obsolete records do not reappear. Recovery here is ordinary restart, not process kill or power failure. Chapter 19 still supplies process-kill coverage. Histograms from the first generation remain separate from verification work; pool state and owner counters in this report do not include later recovered-generation allocations.

Finish `case`, then add the public wrapper. A new run directory is required even when an earlier run failed. A failure writes a diagnostic only inside the directory created by this call; it never clears or repairs a user's existing path.

```rust title="crates/broker-app/src/measurement/experiment.rs"
    let summary = Summary {
        confirmed: appended.stats.confirmed, refused: appended.stats.refused, missed: appended.stats.missed,
        useful: consumed.stats.useful, scanned: consumed.stats.scanned, sync_groups,
        append_seconds: appended.elapsed.as_secs_f64(), consumer_seconds: consumed.elapsed.as_secs_f64(),
        recovery_seconds: recovery.as_secs_f64(), index_entries, retained,
    };
    let details = Details { appended, consumed, owners, memory, disk_before, disk_after, recovery,
        summary: summary.clone() };
    report::write(root, config, &details)?;
    Ok(summary)
}

pub fn execute(root: &Path, config: Config) -> Result<Summary, BoxError> {
    config.validate()?; // Invalid arguments cannot create or overwrite a run.
    fs::create_dir(root)?;
    let result = (|| {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let warmup = Config { mode: Mode::Closed, offers_per_producer: 16, period_us: 0, ..config };
        case(&root.join("warmup"), root, warmup, &runtime, "warmup")?;
        let summary = case(&root.join("measured"), root, config, &runtime, "measured")?;
        phase(root, "complete")?;
        Ok::<_, BoxError>(summary)
    })();
    if let Err(error) = &result {
        // The supplied run directory was newly created by this call; unrelated directories are untouched.
        let _ = fs::write(root.join("failure.txt"), format!("{error}\n"));
        let _ = phase(root, "failed");
    }
    result
}
```

## A result file must state what was counted

Create `measurement/report.rs`. `attempts.tsv` is written before the ambiguity check. It remains useful when a post-submit timeout prevents a comparable run. The successful `summary.tsv` is written last, only after all correctness and resource checks. This is experiment output, not synchronized broker authority or a crash-replay ledger. The parent also requires a zero process exit status.

The histogram helper rejects invalid clocks/counters before writing results. Empty valid populations remain `NA`. No latency distribution can hide a failed observer by silently dropping it.

```rust title="crates/broker-app/src/measurement/report.rs"
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::mem::size_of;
use std::path::Path;
use crate::BoxError;
use crate::metrics::Histogram;
use super::{Config, PRODUCERS, SEGMENT_BYTES, require};
use super::experiment::Details;
use super::load::Appended;
use super::observed::NAMES;

pub(super) fn write_attempts(root: &Path, config: Config, data: &Appended) -> io::Result<()> {
    let mut out = BufWriter::new(File::create(root.join("attempts.tsv"))?);
    for (key, value) in [
        ("offered", (PRODUCERS * config.offers_per_producer) as u64),
        ("started", data.stats.started), ("confirmed", data.stats.confirmed),
        ("refused", data.stats.refused), ("ambiguous", data.stats.ambiguous),
        ("timed_out", data.stats.timed_out), ("missed", data.stats.missed),
    ] { writeln!(out, "{key}\t{value}")?; }
    for (name, count) in ["admission_deadline", "disk", "capacity", "closed", "invalid", "owner_refused"]
        .iter().zip(data.stats.reasons) { writeln!(out, "refused_{name}\t{count}")?; }
    out.flush()
}
fn histogram(name: &str, value: &Histogram, out: &mut impl Write, bins: &mut impl Write)
    -> Result<(), BoxError>
{
    require(value.valid, "invalid histogram: clock or counter overflow; do not compare")?;
    value.write(name, out, bins)?; Ok(())
}
```

The writer emits seven request/cycle distributions plus ten categories for each owner. Some categories, such as checkpoint synchronization in the first generation, are legitimately empty. `owner-failures.tsv` gives failed-call counts beside those distributions.

`memory.tsv` records each pool's actual allocated vector capacity, exact checked-out high water, and final checked-out capacity. Scratch is a persistent allocation, not a checkout, so its peak column is `NA`. Summing per-pool peaks does not establish simultaneous peak usage. `disk.tsv` uses the broker's logical data-file accounting and quiescent reservation checks, not filesystem block allocation.

```rust title="crates/broker-app/src/measurement/report.rs"
pub(super) fn write(root: &Path, config: Config, data: &Details) -> Result<(), BoxError> {
    let mut distributions = BufWriter::new(File::create(root.join("histograms.tsv"))?);
    let mut bins = BufWriter::new(File::create(root.join("bins.tsv"))?);
    writeln!(distributions, "metric\tvalid\tcount\ttotal_us\tmax_us\tp50_low\tp50_high\tp95_low\tp95_high\tp99_low\tp99_high")?;
    writeln!(bins, "metric\tlow_us\thigh_us\tcount")?;
    let a = &data.appended.stats; let c = &data.consumed.stats;
    for (name, h) in [("append_admission_all", &a.admission), ("append_success", &a.successful),
        ("append_unsuccessful", &a.unsuccessful), ("append_submit_to_reply", &a.submit_to_reply),
        ("append_scheduled_to_finish", &a.scheduled_to_finish), ("fetch_ack_cycle", &c.cycles),
        ("acknowledgement", &c.acknowledgements)] {
        histogram(name, h, &mut distributions, &mut bins)?;
    }
    let mut failures = BufWriter::new(File::create(root.join("owner-failures.tsv"))?);
    writeln!(failures, "partition\tmetric\tfailed_calls")?;
    for (partition, owner) in data.owners.iter().enumerate() {
        require(owner.valid, "owner observer was poisoned or overflowed")?;
        for (index, name) in NAMES.iter().enumerate() {
            histogram(&format!("p{partition}_{name}"), &owner.hist[index], &mut distributions, &mut bins)?;
            writeln!(failures, "{partition}\t{name}\t{}", owner.failures[index])?;
        }
    }
    distributions.flush()?; bins.flush()?; failures.flush()?;
    let mut memory = BufWriter::new(File::create(root.join("memory.tsv"))?);
    writeln!(memory, "partition\tpool\tallocated_capacity\tpeak_checked_out_capacity\tfinal_checked_out_capacity")?;
    let mut allocated = 0usize;
    let mut sum_peaks = 0usize;
    for (partition, state) in data.memory.iter().enumerate() {
        for (name, pool) in [("append", state.append), ("response", state.response)] {
            require(pool.peak_checked_out_bytes <= pool.allocated_bytes, "peak exceeds pool allocations")?;
            writeln!(memory, "{partition}\t{name}\t{}\t{}\t{}", pool.allocated_bytes,
                pool.peak_checked_out_bytes, pool.checked_out_bytes)?;
            allocated += pool.allocated_bytes; sum_peaks += pool.peak_checked_out_bytes;
        }
        allocated += state.scratch_bytes;
        writeln!(memory, "{partition}\tscratch\t{}\tNA\tNA", state.scratch_bytes)?;
    }
    memory.flush()?;
    let mut disk = BufWriter::new(File::create(root.join("disk.tsv"))?);
    writeln!(disk, "partition\tbefore_file_lengths\tafter_file_lengths\tnormal_quota\tmaintenance_allowance")?;
    for partition in 0..4 {
        let (before, after) = (data.disk_before[partition], data.disk_after[partition]);
        require(!before.stopped && !after.stopped && before.reserved_bytes == 0 && after.reserved_bytes == 0,
            "disk accounting is not quiescent")?;
        writeln!(disk, "{partition}\t{}\t{}\t{}\t{}", before.file_bytes, after.file_bytes,
            before.quota.data_bytes(), before.quota.reserve_bytes())?;
    }
    disk.flush()?;
```

The summary keeps scope, profile, payload size, pacing, warmup, concurrency, sync groups, scan counts, recovery, and retained offsets beside rates. It also records runtime `size_of` values rather than assuming a compiler's layout. Derived index-entry count describes logical entries; it is not a measurement of vector capacity.

`line!` is a local declarative macro: each invocation expands to the same checked `writeln!` statement. It removes repetitive output syntax without hiding an algorithm, allocation policy, or error branch. It writes only after timing has stopped.

```rust title="crates/broker-app/src/measurement/report.rs"
    // Write the successful summary last. Failure before here cannot masquerade as a complete sample.
    let mut summary = BufWriter::new(File::create(root.join("summary.tsv"))?);
    macro_rules! line { ($key:expr, $value:expr) => { writeln!(summary, "{}\t{}", $key, $value)? }; }
    line!("scope", "in_process_bounded_engine_two_phase");
    line!("profile", config.profile.name()); line!("mode", config.mode.name());
    line!("producers", PRODUCERS); line!("offers_per_producer", config.offers_per_producer);
    line!("payload_bytes", config.payload_bytes); line!("period_us", config.period_us);
    line!("batch_records", config.profile.batch_records()); line!("batch_delay_us", 2000);
    line!("index_stride", config.profile.stride()); line!("segment_bytes", SEGMENT_BYTES);
    line!("warmup_offers_per_producer", 16); line!("instrumented_owner", true);
    line!("offered", PRODUCERS * config.offers_per_producer); line!("started", a.started);
    line!("confirmed", a.confirmed); line!("refused", a.refused); line!("missed", a.missed);
    line!("timed_out", a.timed_out); line!("ambiguous", a.ambiguous);
    line!("append_seconds", data.summary.append_seconds);
    line!("confirmed_per_second", a.confirmed as f64 / data.summary.append_seconds);
    line!("consumer_seconds", data.summary.consumer_seconds);
    line!("useful_group_records", c.useful); line!("duplicate_deliveries", c.duplicates);
    line!("useful_group_records_per_second", c.useful as f64 / data.summary.consumer_seconds);
    line!("scanned_records", c.scanned); line!("fetched_records", c.fetched);
    line!("sync_groups", data.summary.sync_groups);
    line!("recovery_seconds", data.recovery.as_secs_f64());
    line!("index_entries_derived", data.summary.index_entries);
    line!("index_entry_size", size_of::<broker_core::storage::index::IndexEntry>());
    line!("pool_plus_scratch_capacity", allocated);
    line!("sum_pool_peaks_not_simultaneous", sum_peaks);
    line!("histogram_size", size_of::<Histogram>());
    line!("retained_p0", data.summary.retained[0]); line!("retained_p1", data.summary.retained[1]);
    line!("retained_p2", data.summary.retained[2]); line!("retained_p3", data.summary.retained[3]);
    line!("valid", true);
    summary.flush()?;
    Ok(())
}
```

## The executable owns one runtime and one new output tree

Create the complete binary below. It validates profile and mode before `execute` creates a directory. There are no network listeners, hidden retries, pre-existing data resets, or implicit builds. A nonzero exit means the run must not be used as a successful comparison, even if some intermediate files exist.

```rust title="crates/broker-app/src/bin/broker-measure.rs"
use std::path::Path;
use broker_app::measurement::{self, Config, Mode, Profile};
use broker_app::BoxError;

fn main() -> Result<(), BoxError> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7 {
        return Err("usage: broker-measure NEW_RUN_DIR single|batched|dense closed|paced OFFERS PAYLOAD_BYTES PERIOD_US".into());
    }
    let config = Config { profile: Profile::parse(&args[2])?, mode: Mode::parse(&args[3])?,
        offers_per_producer: args[4].parse()?, payload_bytes: args[5].parse()?, period_us: args[6].parse()? };
    let summary = measurement::execute(Path::new(&args[1]), config)?;
    println!("complete: confirmed={} useful_group_records={} scanned={} sync_groups={}",
        summary.confirmed, summary.useful, summary.scanned, summary.sync_groups);
    println!("results: {}/measured (engine-only, not TCP throughput)", args[1]);
    Ok(())
}
```

## Sample residency and disk allocation without confusing their meanings

The Python driver launches the already-built executable and samples that exact child PID. Its RSS includes broker threads, producer/consumer tasks, expected-record vectors, histograms, runtime state, and stack pages. It is not isolated broker RSS. The sampled peak is a **lower bound on the true peak**: a short spike can occur between samples. It also spans warmup and later verification, unlike the first-generation pool table. The phase column helps locate observations but is not an atomic snapshot with the sample.

For files, sum `st_size` for logical lengths and, where supported, `st_blocks*512` for reported allocated bytes. `st_blksize` is not the allocation unit to multiply by. This filesystem allocation report is not device write amplification or exclusive physical usage under compression, sharing, or snapshots. See Python's [`stat_result` fields](https://docs.python.org/3/library/os.html#os.stat_result) and Apple's archived [`stat` interface](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/stat.2.html).

Only regular files under `measured/broker` are counted. Symlinks are skipped, and files disappearing during retention are counted as races. Directory metadata and the experiment's logs/ledger are outside this total. A tree walk is not an atomic snapshot, so a sample overlapping rotation can miss a file or combine different instants. The broker's own quiescent data-file count and the filesystem sample answer related but different questions.

Create `scripts/measure.py`. It uses only Python's standard library. `ps -o rss= -p PID` is intentionally isolated in a helper; consult `man ps` on the actual Mac for the platform's RSS definition and units. A missing/unsupported reading stays unknown rather than becoming zero.

```python title="scripts/measure.py"
#!/usr/bin/env python3
"""Run a fixed native measurement matrix; never build, publish, or overwrite a run."""
from __future__ import annotations
import argparse
import csv
import json
import math
import os
from pathlib import Path
import platform
import shutil
import stat
import statistics
import subprocess
import time

PROFILES = ("single", "batched", "dense")


def text_command(args: list[str]) -> str | None:
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=3, check=False)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def rss_kib(pid: int) -> int | None:
    value = text_command(["ps", "-o", "rss=", "-p", str(pid)])
    return int(value) if value and value.strip().isdigit() else None


def disk_sample(root: Path) -> tuple[int, int | None, int]:
    logical, allocated, raced = 0, 0, 0
    supported = True
    # The measurement creates this tree itself. Do not follow symlinks out of it.
    for directory, _, files in os.walk(root, followlinks=False):
        for name in files:
            try:
                info = (Path(directory) / name).lstat()
            except FileNotFoundError:
                raced += 1
                continue
            if not stat.S_ISREG(info.st_mode):
                continue
            logical += info.st_size
            blocks = getattr(info, "st_blocks", None)
            if blocks is None:
                supported = False
            else:
                allocated += blocks * 512
    return logical, allocated if supported else None, raced
```

Parse the successful summary defensively. Reject duplicate keys, incomplete scope, nonfinite/nonpositive elapsed times, unexplained outcomes, and an ambiguous append population. A file that happens to contain `confirmed` is not sufficient. Positive time with zero confirmations is a valid zero-throughput result; it must not be confused with zero elapsed time.

```python title="scripts/measure.py"
def parse_summary(path: Path) -> dict[str, str]:
    rows: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) != 2 or parts[0] in rows:
            raise ValueError(f"invalid or duplicate summary row in {path}")
        rows[parts[0]] = parts[1]
    if rows.get("valid") != "true" or rows.get("scope") != "in_process_bounded_engine_two_phase":
        raise ValueError(f"not a complete engine sample: {path}")
    for name in ("append_seconds", "consumer_seconds", "recovery_seconds"):
        value = float(rows[name])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"invalid {name} in {path}")
    offered, started, confirmed, refused, ambiguous, missed = (
        int(rows[key]) for key in ("offered", "started", "confirmed", "refused", "ambiguous", "missed"))
    if min(offered, started, confirmed, refused, ambiguous, missed) < 0:
        raise ValueError("negative request counter")
    if started + missed != offered or confirmed + refused + ambiguous != started or ambiguous:
        raise ValueError("unaccounted or ambiguous requests; do not compare")
    if int(rows["useful_group_records"]) != 2 * confirmed:
        raise ValueError("both groups did not cover all confirmed work")
    return rows
```

## A timeout is followed by process ownership cleanup

The child writes stdout and stderr directly to a file. A pipe whose contents are never drained could fill and stall the child, corrupting the experiment. The runner polls only its own child, kills it on its outer observation timeout, and always waits to reap it. A kill request is not proof that exit is already complete; kernel I/O can still delay the subsequent wait. See Python's [subprocess process-wait documentation](https://docs.python.org/3/library/subprocess.html#subprocess.Popen.wait).

The timeout bounds when the harness requests termination, not an absolute bound on a kernel-blocked process. Resource polling uses sleeps only to limit observation overhead. It establishes no broker event order and replaces none of the earlier deterministic gates.

A `--no-sampling` run preserves the workload and Rust instrumentation but omits `ps` and filesystem walks. Compare it separately to assess external polling overhead. It is not a completely uninstrumented engine.

```python title="scripts/measure.py"
def measured_child(command: list[str], run: Path, timeout: float, interval: float,
                   sample: bool) -> dict[str, int | None]:
    # The Rust program owns creation of run/. Its parent directory belongs to this driver.
    if run.exists():
        raise FileExistsError(run)
    log_path = run.parent / (run.name + ".log")
    samples_path = run.parent / (run.name + "-resources.tsv")
    peaks: dict[str, int | None] = {"rss_kib_sampled": None, "logical_bytes_sampled": None,
                                  "allocated_bytes_sampled": None}
    start = time.monotonic()
    with log_path.open("x", encoding="utf-8") as log, samples_path.open("x", newline="", encoding="utf-8") as output:
        writer = csv.writer(output, delimiter="\t")
        writer.writerow(["seconds", "phase", "rss_kib", "logical_bytes", "allocated_bytes", "raced_files"])
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        try:
            while child.poll() is None:
                if time.monotonic() - start >= timeout:
                    child.kill()
                    child.wait()  # A kill request does not replace reaping; kernel I/O can still delay exit.
                    raise TimeoutError(f"measurement timed out; inspect {log_path}")
                if sample:
                    try:
                        phase = (run / "phase.txt").read_text(encoding="utf-8").strip()
                    except FileNotFoundError:
                        phase = "starting"
                    rss = rss_kib(child.pid)
                    logical, allocated, raced = disk_sample(run / "measured" / "broker")
                    writer.writerow([f"{time.monotonic() - start:.6f}", phase,
                                     "NA" if rss is None else rss, logical,
                                     "NA" if allocated is None else allocated, raced])
                    for key, value in zip(peaks, (rss, logical, allocated)):
                        if value is not None:
                            old = peaks[key]
                            peaks[key] = value if old is None else max(old, value)
                # This controls observation overhead; it is not evidence of broker event ordering.
                time.sleep(interval)
            code = child.wait()
            if code != 0:
                raise RuntimeError(f"measurement exited {code}; inspect {log_path}")
        finally:
            if child.poll() is None:
                child.kill()
            child.wait()
    return peaks
```

## Preserve populations instead of averaging away their differences

For every profile, keep each run's actual confirmation, refusal, and missed-offer counts. The comparison file reports median, minimum, and maximum throughput and recovery duration. It does not produce a significance claim or select a winner. The dense/batched ratio is a ratio of medians, not a median paired speedup. Inspect corresponding runs as well as that convenient summary.

Latency quantiles remain in each run's histogram files. Do not average their p99 values. To pool a distribution, add matching raw bin counts and recompute the rank, keeping workloads and semantics comparable. In paced mode, a candidate can change how many offers start; a lower p99 from a much smaller successful population can be selection rather than improvement.

```python title="scripts/measure.py"
def summarize(rows: list[dict[str, str | int | None]]) -> dict:
    report: dict = {"scope": "engine-only; no TCP", "profiles": {},
                    "decision": "not selected automatically; evaluate paired runs and correctness"}
    fields = ("confirmed_per_second", "useful_group_records_per_second", "recovery_seconds")
    for profile in PROFILES:
        values = [row for row in rows if row["profile"] == profile]
        if not values:
            continue
        aggregate = {}
        for field in fields:
            samples = [float(row[field]) for row in values]
            if not all(math.isfinite(value) for value in samples):
                raise ValueError("nonfinite throughput or recovery value")
            aggregate[field] = {"median": statistics.median(samples), "min": min(samples), "max": max(samples)}
        aggregate["confirmed_counts"] = [int(row["confirmed"]) for row in values]
        aggregate["missed_counts"] = [int(row["missed"]) for row in values]
        aggregate["refused_counts"] = [int(row["refused"]) for row in values]
        aggregate["scan_amplification"] = [float(row["scanned_records"]) / int(row["fetched_records"])
            if int(row["fetched_records"]) else None for row in values]
        report["profiles"][profile] = aggregate
    if "dense" in report["profiles"] and "batched" in report["profiles"]:
        dense = report["profiles"]["dense"]["useful_group_records_per_second"]["median"]
        baseline = report["profiles"]["batched"]["useful_group_records_per_second"]["median"]
        report["dense_consumer_ratio_of_medians"] = dense / baseline if baseline else None
    return report
```

The command-line entry point records the actual platform and toolchain reported on PATH, copies an existing `Cargo.lock`, and alternates profile order between repeats. It does not claim that the compiler on PATH built an old executable; build immediately before running. No lockfile is fabricated. It never publishes, installs software, or silently resumes an old directory.

Run names are fresh children of the new matrix directory. The Rust executable creates each child; the Python parent owns sibling logs and resource samples. `results.tsv` is rewritten only within this new output tree as completed cases arrive. The maximum repeat count, offer count, and timeout make the matrix finite. This remains a local experiment, not an unattended benchmarking service.

```python title="scripts/measure.py"
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="new output directory, normally under runs/")
    parser.add_argument("--executable", type=Path, default=Path("target/release/broker-measure"))
    parser.add_argument("--offers", type=int, default=1024)
    parser.add_argument("--payload", type=int, default=1024)
    parser.add_argument("--mode", choices=("closed", "paced"), default="closed")
    parser.add_argument("--period-us", type=int, default=2000, help="paced mode only; closed records zero")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--no-sampling", action="store_true")
    args = parser.parse_args()
    if not (1 <= args.offers <= 4096 and 24 <= args.payload <= 4096 and 1 <= args.repeats <= 5
            and 1000 <= args.period_us <= 10_000 and 1 <= args.timeout <= 900 and 0.05 <= args.interval <= 2):
        parser.error("offers 1..4096, payload 24..4096, repeats 1..5, period 1000..10000us, timeout 1..900s, interval .05..2s")
    executable = args.executable.resolve(strict=True)
    args.root.parent.mkdir(parents=True, exist_ok=True)
    args.root.mkdir()  # Never overwrite, clear, or resume an existing run implicitly.
    manifest = {"platform": platform.platform(), "machine": platform.machine(),
                "sw_vers": text_command(["sw_vers"]), "rustc_on_path": text_command(["rustc", "--version", "--verbose"]),
                "cargo_on_path": text_command(["cargo", "--version"]), "python": platform.python_version(),
                "executable": str(executable), "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "rustflags": os.environ.get("RUSTFLAGS", ""), "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "build_note": "Build this executable with this toolchain before running; version-on-PATH is not embedded build provenance."}
    (args.root / "environment.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if Path("Cargo.lock").is_file():
        shutil.copyfile("Cargo.lock", args.root / "Cargo.lock")
    rows = []
    for repeat in range(args.repeats):
        order = PROFILES if repeat % 2 == 0 else tuple(reversed(PROFILES))
        for profile in order:
            run = args.root / f"{repeat + 1:02d}-{profile}"
            command = [str(executable), str(run), profile, args.mode, str(args.offers), str(args.payload),
                       str(args.period_us if args.mode == "paced" else 0)]
            peaks = measured_child(command, run, args.timeout, args.interval, not args.no_sampling)
            row = parse_summary(run / "measured" / "summary.tsv")
            row.update(peaks); row["repeat"] = repeat + 1; rows.append(row)
            # This summary is rewritten only inside the newly created, owned output directory.
            with (args.root / "results.tsv").open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=list(rows[0]), delimiter="\t")
                writer.writeheader(); writer.writerows(rows)
            print(f"completed {run}: confirmed={row['confirmed']} refused={row['refused']} missed={row['missed']}", flush=True)
    (args.root / "comparison.json").write_text(json.dumps(summarize(rows), indent=2, allow_nan=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
```

The driver samples the whole measurement process through warmup and verification, but its logical/allocated file columns refer only to `measured/broker`. Thus early samples can legitimately have positive RSS and zero measured disk bytes. Do not compare that RSS peak directly to the measured generation's pool peak as though they were simultaneous measurements of the same object.

For deeper allocation diagnosis, use the Mac's available profiling tools in a separate run. Apple's [Allocations guidance](https://developer.apple.com/library/archive/technotes/tn2434/_index.html) explains examining allocations over a selected time range. Treat that as a diagnostic workflow, not a requirement for a particular Xcode release or evidence of a measurement already made. Profiling changes overhead; do not mix a profiled sample into the baseline throughput table without labelling it.

