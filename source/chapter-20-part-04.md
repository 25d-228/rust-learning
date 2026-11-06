# Chapter 20 — Resource measurement and one justified optimization

## Part 4 — Regression checks and the retain-or-revert decision

> **Learning objectives**
> Make measurement defects fail visibly, run comparable native experiments, distinguish calculated fixtures from observations, and retain an optimization only while preserving the broker's correctness and resource contracts.

Continue from [Part 3](https://25d-228.github.io/rust-learning/chapter-20-part-03.html). This completes the final chapter's **4.5-hour implementation plan**. All expected counts below are derived fixtures, not recorded performance results. Throughput, latency, RSS, disk allocation, and recovery seconds must come from the actual run on the actual machine.

## Test the histogram as an arithmetic data structure

Append this complete test module to `metrics.rs`. Boundary routing checks both ends of every finite bucket and their adjacency. The rounding test includes zero, one nanosecond, and the two sides of a microsecond boundary. The percentile test checks intervals, not a fabricated exact value. The merge test would fail if the implementation averaged p99s.

Overflow and reversed-clock tests require invalidation. These tests do not assert any wall-clock latency and are therefore useful even on a loaded development machine.

```rust title="crates/broker-app/src/metrics.rs"
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_finite_buckets_are_adjacent_and_route_both_boundaries() {
        for index in 0..BINS - 1 {
            let (low, high) = bounds(index);
            assert_eq!(bucket(low), index); assert_eq!(bucket(high.unwrap()), index);
            assert_eq!(high.unwrap() + 1, bounds(index + 1).0);
        }
    }
    #[test]
    fn submicrosecond_observations_round_up_but_zero_is_zero() {
        let mut h = Histogram::default();
        for n in [0, 1, 999, 1000, 1001] { h.record(Some(Duration::from_nanos(n))); }
        assert_eq!((h.bins[0], h.bins[1], h.bins[2]), (1, 3, 1));
        assert_eq!((h.count, h.total_us, h.max_us), (5, 5, 2));
    }
    #[test]
    fn nearest_rank_returns_an_interval_not_a_false_exact_sample() {
        let mut h = Histogram::default(); for value in [1, 2, 3, 16, 100] { h.record_us(value); }
        assert_eq!(h.quantile(50, 100), Some((3, Some(3))));
        assert_eq!(h.quantile(95, 100), Some((96, Some(103))));
        assert_eq!(h.quantile(99, 100), Some((96, Some(103))));
    }
    #[test]
    fn merge_pools_counts_instead_of_averaging_percentiles() {
        let mut a = Histogram::default(); a.record_us(100);
        let mut b = Histogram::default(); for _ in 0..99 { b.record_us(1); }
        a.merge(&b); assert_eq!(a.quantile(99, 100), Some((1, Some(1))));
        assert_eq!((a.count, a.total_us), (100, 199));
    }
    #[test]
    fn empty_and_invalid_quantile_arguments_are_not_zero_latency() {
        let mut h = Histogram::default(); assert_eq!(h.quantile(99, 100), None);
        h.record_us(7);
        for (n, d) in [(0, 1), (1, 0), (2, 1)] { assert_eq!(h.quantile(n, d), None); }
    }
    #[test]
    fn overflow_bucket_keeps_exact_observed_maximum() {
        let mut h = Histogram::default(); h.record_us(u64::MAX);
        assert_eq!(h.quantile(99, 100), Some((1u64 << 34, None)));
        assert_eq!(h.max_us, u64::MAX);
    }
    #[test]
    fn counter_overflow_invalidates_without_wrapping() {
        let mut h = Histogram::default(); h.count = u64::MAX; h.record_us(1);
        assert!(!h.valid); assert_eq!(h.count, u64::MAX); assert_eq!(h.bins[1], 0);
    }
    #[test]
    fn a_reversed_clock_sample_invalidates_the_distribution() {
        let mut h = Histogram::default(); h.record(None);
        assert!(!h.valid); assert_eq!(h.count, 0); assert_eq!(h.quantile(99, 100), None);
    }
}
```

## Make a brief capacity reservation visible after it has gone away

Add this one complete test inside the existing `budget::memory::tests` module. Keep its existing `use super::*` and all earlier tests. The two checkouts have used lengths 1 and 0, but together retain every buffer allocation. After both drops, current checkout returns to zero while the high water remains. A later smaller checkout must not lower it.

```rust title="crates/broker-core/src/budget/memory.rs"
    #[test]
    fn peak_counts_capacity_and_survives_return() {
        let pool = BufferPool::new(64, 2, 256, Arc::new(Notify::new())).unwrap();
        let a = pool.take(1).unwrap();
        let first = pool.state().checked_out_bytes;
        assert_eq!(pool.state().peak_checked_out_bytes, first);
        let b = pool.take(0).unwrap();
        let peak = pool.state().allocated_bytes;
        assert_eq!(pool.state().peak_checked_out_bytes, peak);
        drop(a); drop(b);
        assert_eq!(pool.state().checked_out_bytes, 0);
        assert_eq!(pool.state().peak_checked_out_bytes, peak);
        let c = pool.take(2).unwrap();
        assert_eq!(pool.state().peak_checked_out_bytes, peak);
        drop(c);
    }
```

## Test profiles, pacing, scan geometry, and observation failures

Create `measurement/tests.rs`. Its first checks ensure the finite configuration and profile differences are exactly the intended ones. The payload checks alter an identity, a size, and a tail byte. A checker that validates only the prefix would miss that final corruption.

The pacing fixtures do not wait on wall-clock timers. They pass explicit elapsed values to `latest_offer`, so `[0,1000,4500,9000]` microseconds with eight slots and a 1,000-microsecond period deterministically yields three started and five missed offers. The helper's caller still owns real timer behavior; these tests establish arithmetic, not operating-system scheduling precision.

```rust title="crates/broker-app/src/measurement/tests.rs"
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use broker_core::io::FaultFile;
use broker_core::storage::sync::{OwnerActivity, SyncIo};
use broker_core::types::Offset;
use super::*;
use super::load::{Identity, fill, latest_offer, matches_payload};
use super::observed::{ObservedSync, Observer};

fn config() -> Config {
    Config { profile: Profile::Batched, mode: Mode::Closed,
        offers_per_producer: 32, payload_bytes: 128, period_us: 0 }
}
#[test]
fn configurations_bound_counts_payloads_and_pacing() {
    assert!(config().validate().is_ok());
    for offers in [0, 4097] { assert!(Config { offers_per_producer: offers, ..config() }.validate().is_err()); }
    for payload in [0, 23, 4097] { assert!(Config { payload_bytes: payload, ..config() }.validate().is_err()); }
    assert!(Config { period_us: 1000, ..config() }.validate().is_err());
    assert!(Config { mode: Mode::Paced, period_us: 1000, ..config() }.validate().is_ok());
    assert!(Config { mode: Mode::Paced, period_us: 999, ..config() }.validate().is_err());
}
#[test]
fn profiles_change_only_batch_count_or_index_stride() {
    assert_eq!(Profile::Single.batch_records(), 1);
    assert_eq!(Profile::Batched.batch_records(), 2);
    assert_eq!(Profile::Dense.batch_records(), 2);
    assert_eq!(Profile::Single.stride(), 64);
    assert_eq!(Profile::Batched.stride(), 64);
    assert_eq!(Profile::Dense.stride(), 8);
    assert!(Profile::parse("unbounded").is_err());
    assert!(Mode::parse("open").is_err());
}
#[test]
fn payload_identity_initializes_every_requested_byte() {
    let id = Identity { producer: 7, sequence: 4095 };
    for size in [24, 25, 128, 4096] {
        let mut bytes = vec![0xff; size]; fill(&mut bytes, id);
        assert!(matches_payload(&bytes, id, size));
        assert_eq!(&bytes[..8], b"MEASURE1");
        assert_eq!(u64::from_be_bytes(bytes[8..16].try_into().unwrap()), 7);
        assert_eq!(u64::from_be_bytes(bytes[16..24].try_into().unwrap()), 4095);
    }
}
#[test]
fn payload_checker_detects_identity_tail_and_size_changes() {
    let id = Identity { producer: 0, sequence: 1 };
    let mut bytes = [0; 128]; fill(&mut bytes, id);
    assert!(!matches_payload(&bytes, Identity { producer: 0, sequence: 2 }, 128));
    assert!(!matches_payload(&bytes, id, 127));
    bytes[127] ^= 1; assert!(!matches_payload(&bytes, id, 128));
}
#[test]
fn rate_grid_starts_at_zero_and_never_queues_the_missed_prefix() {
    assert_eq!(latest_offer(0, 5, 0, 1000), (Some(0), 0));
    assert_eq!(latest_offer(1, 5, 3400, 1000), (Some(3), 2));
    assert_eq!(latest_offer(4, 5, 4500, 1000), (Some(4), 0));
}
#[test]
fn overdue_end_and_exhausted_grid_account_for_every_remaining_offer() {
    assert_eq!(latest_offer(2, 5, 5000, 1000), (None, 3));
    assert_eq!(latest_offer(2, 5, u64::MAX, 1000), (None, 3));
    assert_eq!(latest_offer(5, 5, 0, 1000), (None, 0));
}
#[test]
fn mixed_rate_trace_has_no_invisible_requests() {
    let (mut next, mut started, mut missed) = (0, 0, 0);
    for now in [0, 1000, 4500, 9000] {
        let (chosen, skipped) = latest_offer(next, 8, now, 1000); missed += skipped;
        match chosen { Some(index) => { started += 1; next = index + 1; }, None => break }
    }
    assert_eq!((started, missed), (3, 5));
}
```

The scan test compares the closed form with a direct sum over offsets, then checks rotation and the dense candidate. These are operation-count assertions, not performance thresholds. The observer test delegates to a real file wrapped by the existing failing-sync adapter and requires the original error to survive. A service sample with reversed time invalidates its own histogram, rather than contaminating another activity category.

```rust title="crates/broker-app/src/measurement/tests.rs"
#[test]
fn scan_formula_matches_an_independent_offset_loop() {
    for stride in [1, 8, 64, 4096] {
        for records in [0, 1, 7, 8, 9, 63, 64, 65, 1000, 8192] {
            let direct: u128 = (0..records).map(|offset| u128::from(offset % stride + 1)).sum();
            assert_eq!(expected_scans(records, stride), direct);
        }
    }
}
#[test]
fn segment_geometry_resets_the_index_at_rotation() {
    let per_segment = (SEGMENT_BYTES as usize - 32) / (4096 + 32);
    let records = per_segment + 65;
    let (scans, entries) = experiment::geometry(records, 4096, 64);
    assert_eq!(scans, expected_scans(per_segment as u64, 64) + expected_scans(65, 64));
    assert_eq!(entries, (per_segment as u64).div_ceil(64) + 2);
    assert_eq!(experiment::geometry(0, 4096, 64), (0, 0));
}
#[test]
fn dense_candidate_reduces_predicted_scan_work_but_adds_entries() {
    let sparse = experiment::geometry(2048, 1024, 64);
    let dense = experiment::geometry(2048, 1024, 8);
    assert_eq!(sparse, (66_560, 32)); assert_eq!(dense, (9_216, 256));
    // These are operation counts, not a throughput or latency speedup assertion.
}
#[test]
fn observer_preserves_real_sync_failure_and_counts_it() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!("measure-sync-{}-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)));
    let file = fs::OpenOptions::new().write(true).read(true).create_new(true).open(&path).unwrap();
    let mut file = FaultFile::new(file, 64).unwrap(); file.fail_syncs();
    let observer = Observer::default(); let mut io = ObservedSync(observer.clone());
    let result = io.sync(Offset::new(0), &mut file);
    drop(file); fs::remove_file(path).unwrap();
    assert!(result.is_err());
    let snapshot = observer.snapshot();
    assert_eq!((snapshot.hist[4].count, snapshot.failures[4]), (1, 1));
}
#[test]
fn owner_service_categories_and_clock_failures_remain_distinct() {
    let observer = Observer::default(); let mut io = ObservedSync(observer.clone());
    io.service_observed(OwnerActivity::Append, Some(Duration::from_micros(10)), false);
    io.service_observed(OwnerActivity::Deadline, Some(Duration::from_micros(20)), true);
    io.service_observed(OwnerActivity::Control, None, false);
    let snapshot = observer.snapshot();
    assert_eq!(snapshot.hist[0].count, 1); assert_eq!(snapshot.failures[0], 0);
    assert_eq!(snapshot.hist[2].count, 1); assert_eq!(snapshot.failures[2], 1);
    assert!(!snapshot.hist[1].valid); assert_eq!(snapshot.hist[1].count, 0);
}
```

## Run the complete candidate through recovery and retention

Create `crates/broker-app/tests/measurement.rs`. These are ordinary synchronous integration tests because `execute` owns its runtime. Each profile performs its own warmup, measured append/consumer phases, populated recovery, stale-epoch rejection, fresh coverage, checkpoint/retention, and retained-state reopen.

With 32 offers per producer, 256 confirmations are expected, 64 per partition. Two groups produce 512 useful observations. For stride 64, each group/partition scans $64\cdot65/2=2080$ records, or 16,640 total. Stride 8 gives eight complete blocks, $8\cdot(8\cdot9/2)=288$ per group/partition, or 2,304 total. `single` requires 256 sync groups. The other profiles can produce between 128 and 256 depending on deadline/interleaving; no test assumes a scheduler will always fill the batch.

Paced execution permits either started outcomes or explicit misses. It does not demand an exact number of confirmations from a noisy scheduler. Existing-directory and invalid-input tests protect unrelated files. The integration tests intentionally make **no comparison of elapsed time**.

```rust title="crates/broker-app/tests/measurement.rs"
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use broker_app::measurement::{self, Config, Mode, Profile};

fn root(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!("broker-measure-{label}-{}-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)))
}
fn config(profile: Profile) -> Config {
    Config { profile, mode: Mode::Closed, offers_per_producer: 32, payload_bytes: 128, period_us: 0 }
}
#[test]
fn all_profiles_cover_the_same_confirmations_and_retained_restart() {
    for profile in [Profile::Single, Profile::Batched, Profile::Dense] {
        let path = root(profile.name());
        let summary = measurement::execute(&path, config(profile))
            .unwrap_or_else(|error| panic!("inspect {}: {error}", path.display()));
        assert_eq!((summary.confirmed, summary.refused, summary.missed, summary.useful), (256, 0, 0, 512));
        assert_eq!(summary.retained, [64; 4]);
        assert_eq!(summary.scanned, if profile == Profile::Dense { 2304 } else { 16640 });
        if profile == Profile::Single { assert_eq!(summary.sync_groups, 256); }
        else { assert!((128..=256).contains(&summary.sync_groups)); }
        assert!(fs::read_to_string(path.join("measured/summary.tsv")).unwrap().contains("valid\ttrue\n"));
        // No assertion compares elapsed time: noisy CI is not a performance oracle.
        fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn paced_offers_are_either_started_or_explicitly_missed() {
    let path = root("paced");
    let config = Config { mode: Mode::Paced, offers_per_producer: 8, period_us: 1000,
        ..config(Profile::Batched) };
    let summary = measurement::execute(&path, config).unwrap();
    assert_eq!(summary.confirmed + summary.refused + summary.missed, 64);
    assert_eq!(summary.useful, 2 * summary.confirmed);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn existing_run_preserves_unrelated_files() {
    let path = root("existing"); fs::create_dir(&path).unwrap();
    fs::write(path.join("keep"), b"unrelated").unwrap();
    assert!(measurement::execute(&path, config(Profile::Batched)).is_err());
    assert_eq!(fs::read(path.join("keep")).unwrap(), b"unrelated");
    assert!(!path.join("warmup").exists()); fs::remove_dir_all(path).unwrap();
}
#[test]
fn invalid_input_fails_before_creating_a_directory() {
    let path = root("invalid");
    assert!(measurement::execute(&path, Config { offers_per_producer: 0, ..config(Profile::Batched) }).is_err());
    assert!(!path.exists());
    let output = Command::new(env!("CARGO_BIN_EXE_broker-measure"))
        .arg(&path).args(["unknown", "closed", "32", "128", "0"]).output().unwrap();
    assert!(!output.status.success()); assert!(!path.exists());
}
```

## Test the Python runner without pretending it ran the Rust broker

Create `scripts/test_measure.py`. These tests check result parsing, symlink-safe file accounting, process ownership, failed exit handling, and timeout cleanup. Their child programs are small Python fixtures, not the broker. Passing them establishes that narrow harness behavior only; it is not a substitute for the Rust integration tests or the native measurement.

```python title="scripts/test_measure.py"
"""Checks for the measurement driver only; they do not execute the Rust broker."""
from pathlib import Path
import sys
import tempfile
import unittest
import measure


class MeasurementDriverTests(unittest.TestCase):
    def test_disk_lengths_ignore_symlinks(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name); (root / "a").write_bytes(b"abc")
            (root / "link").symlink_to(root / "a")
            logical, allocated, raced = measure.disk_sample(root)
            self.assertEqual((logical, raced), (3, 0))
            self.assertTrue(allocated is None or allocated >= 0)

    def test_missing_tree_is_empty_not_a_fake_allocation(self):
        with tempfile.TemporaryDirectory() as name:
            self.assertEqual(measure.disk_sample(Path(name) / "absent"), (0, 0, 0))

    def fixture(self):
        return {"valid": "true", "scope": "in_process_bounded_engine_two_phase", "offered": "8", "started": "6",
                "confirmed": "5", "refused": "1", "ambiguous": "0", "missed": "2", "useful_group_records": "10",
                "append_seconds": "1", "consumer_seconds": "2", "recovery_seconds": "0.1"}

    def parse(self, values):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "summary.tsv"
            path.write_text("".join(f"{key}\t{value}\n" for key, value in values.items()))
            return measure.parse_summary(path)

    def test_summary_keeps_refusals_and_missed_offers(self):
        values = self.parse(self.fixture())
        self.assertEqual((values["refused"], values["missed"]), ("1", "2"))

    def test_ambiguous_or_missing_outcomes_refuse_comparison(self):
        for updates in ({"ambiguous": "1"}, {"missed": "0"}, {"useful_group_records": "9"}):
            values = self.fixture(); values.update(updates)
            with self.assertRaises(ValueError):
                self.parse(values)

    def test_nonfinite_or_zero_time_is_not_a_throughput_measurement(self):
        for value in ("nan", "inf", "0", "-1"):
            values = self.fixture(); values["append_seconds"] = value
            with self.assertRaises(ValueError):
                self.parse(values)

    def test_duplicate_summary_key_is_rejected(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "summary.tsv"; path.write_text("valid\ttrue\nvalid\ttrue\n")
            with self.assertRaises(ValueError):
                measure.parse_summary(path)

    def test_successful_child_is_reaped_and_unsampled_values_are_unknown(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / "child"
            peaks = measure.measured_child([sys.executable, "-c", "print('fixture only')"], root, 3, .01, False)
            self.assertTrue(all(value is None for value in peaks.values()))
            self.assertIn("fixture only", (root.parent / "child.log").read_text())

    def test_failed_child_and_existing_run_are_not_success(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / "child"
            with self.assertRaises(RuntimeError):
                measure.measured_child([sys.executable, "-c", "raise SystemExit(7)"], root, 3, .01, False)
            root.mkdir(); (root / "keep").write_text("unrelated")
            with self.assertRaises(FileExistsError):
                measure.measured_child([sys.executable, "-c", "pass"], root, 3, .01, False)
            self.assertEqual((root / "keep").read_text(), "unrelated")

    def test_timeout_kills_and_reaps_a_fixture_child(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / "slow"
            with self.assertRaises(TimeoutError):
                measure.measured_child([sys.executable, "-c", "import time; time.sleep(30)"], root, .08, .01, False)


if __name__ == "__main__":
    unittest.main()
```

## Build and run the checks before collecting a comparison

Use the separate learner workspace reconstructed through Chapter 20. There are no new dependency requirements. Keep the Rust feature baseline explained in Chapter 7; compatible newer releases are acceptable. Confirm that the terminal and Rust host are native for the M4 machine rather than unintentionally running translated x86_64 tools. Do not invent a macOS version from the hardware name.

Run formatting, focused checks, and the full required targets:

```sh
uname -m
rustc --version --verbose
cargo --version
cargo fmt --all
cargo fmt --all -- --check
cargo test -p broker-app --lib metrics::tests
cargo test -p broker-app --lib measurement::tests
cargo test -p broker-core --lib budget::memory::tests
cargo test -p broker-app --test measurement
python3 -m unittest discover -s scripts -p test_measure.py -v
cargo test --workspace --all-targets
```

Formatting may change the compact reference layout; review the diff and retain semantic code unchanged. The new supplied tests are 8 histogram tests, 12 measurement-unit tests, 1 core pool test, and 4 integration tests: **25 Rust tests**, bringing the cumulative supplied suite to **481**. The Python script supplies **9 separate tests**. These counts describe the supplied code, not an assertion that all Rust targets have already compiled or passed on this machine.

Then rerun the real TCP/process interruption scenario under a fresh directory. This preserves the earlier correctness boundary while instrumentation and measurement-specific settings are introduced:

```sh
mkdir -p runs
cargo run -p broker-app --bin broker-load -- run runs/final-coverage 4
```

Expected Chapter 19 invariants still include 86 observed unique positions, one ambiguous and one rejected attempt, 17 repeated deliveries, and retained floors `[26, 20, 20, 20]`; confirmations may be 85 or 86 depending on the permitted ambiguous original. The measurement profile does not replace those independent crash/overload checks.

If the command refuses an existing path, choose a new name. Do not delete previous results merely to make a script convenient. Inspect a failed run before retrying; there is no blind retry loop.

## Collect a small release-mode baseline first

Build the executable you will measure. Cargo's `release` profile changes optimization and debugging defaults; it does not mean every setting is automatically optimal, and workspace/profile environment overrides matter. Keep compiler, features, and build flags fixed between profiles. See the [Cargo profile reference](https://doc.rust-lang.org/cargo/reference/profiles.html).

```sh
cargo build --release -p broker-app --bin broker-measure
./target/release/broker-measure runs/measure-smoke batched closed 32 128 0
```

With no unexpected refusals, the calculated console properties are:

```text
confirmed=256 useful_group_records=512 scanned=16640
sync_groups is an observed integer from 128 through 256
```

That block describes properties, not a literal transcript: the binary prints all counters on one line, followed by its result-directory line. It deliberately does not print an invented sample rate. In `runs/measure-smoke/measured/`, expect:

```text
attempts.tsv        every offered/started/confirmed/refused/ambiguous/missed count
histograms.tsv      p50/p95/p99 intervals, counts, rounded sums and maxima
bins.tsv            nonempty raw buckets for independent recomputation
owner-failures.tsv  failed calls per owner/activity category
memory.tsv          allocated capacity, exact per-pool checkout peaks, final ownership
disk.tsv            quiescent logical data-file accounting before and after retention
summary.tsv         successful complete sample, configuration and actual elapsed rates
broker/             retained broker state, not the expected-record oracle
```

`warmup/` has the same result structure but must not be pooled into measured results. A failure may leave only partial files and `failure.txt`. A successful summary is meaningful only with a successful exit and all preceding checks.

## Compare batching and the dense-index candidate independently

Run the finite closed-loop matrix on the same native machine, with other heavy work reduced and no debugger pause. The default workload has 1,024 offers per producer and 1,024-byte payloads. Each profile receives fresh directories and counters; repeat order alternates. These are commands to execute, not precomputed results:

```sh
python3 scripts/measure.py runs/measure-closed --offers 1024 --payload 1024 --repeats 3
python3 scripts/measure.py runs/measure-closed-nosampling --offers 1024 --payload 1024 --repeats 3 --no-sampling
```

The first comparison is `single` against `batched`: inspect actual `sync_groups`, data-sync calls, confirmed throughput, successful p95/p99 intervals, and unsuccessful outcomes. Fewer sync groups can improve throughput while increasing some waiting latencies. Never replace real synchronization with a no-op to produce the “batched” result.

The second comparison is `batched` against `dense`: same batching, payloads, concurrency, successful-work population, and synchronization; only index density changes. Confirm the scan counts derived in Part 2, inspect useful consumer throughput and fetch/ack-cycle latency, and compare index entries, recovery seconds, and resource observations. Record scans per fetched record rather than saying “disk reads” when no device I/O was measured.

Then use the paced generator to expose unmet offered load separately:

```sh
python3 scripts/measure.py runs/measure-paced --offers 1024 --payload 1024 --mode paced --period-us 2000 --repeats 3
```

The scheduled horizon is $1024\cdot2000$ microseconds, or 2.048 seconds per producer. Eight aligned producers schedule 8,192 offers, nominally 4,000 per second. The actual append interval includes any final outstanding request drain. Report `missed`, refusals, and timeouts beside the successful rate. Do not call 4,000 the achieved rate unless observations justify it.

Three repeats provide a small variability check, not a statistical theorem. When runs are too short or noisy to distinguish the candidate, increase offers up to the existing 4,096 cap or use five repeats under new directory names. Do not increase the estimate merely because data are noisy, and do not tune indefinitely to a single favorable result. A negative or inconclusive result is a valid engineering outcome.

## Decide using the same contract, including the costs

First state the observation that motivated the change. For example, the measured bounded read path might show high `scanned_records/fetched_records` while consumer time dominates the phase being optimized. The formula explains why stride matters; the timings establish whether that work matters on this machine. If acknowledgement or scheduling cost dominates instead, a denser index may reduce scans without a useful throughput improvement.

Compute relative useful-throughput change as $(Q_d-Q_b)/Q_b$, where $Q_d$ and $Q_b$ are comparable dense and batched rates, and retain the actual rates alongside the ratio. Define the acceptable latency, recovery, and memory tradeoff before selecting a winning run. Compare p99 **intervals**; overlapping buckets do not resolve an arbitrarily small improvement. Check sample counts and misses before interpreting a shifted tail.

Keep the candidate only when the measured benefit is relevant and reproducible enough for the intended workload, with acceptable resource/recovery costs and unchanged correctness checks. Otherwise explicitly revert the chosen runtime configuration to `batched`; the experiment profiles remain useful test fixtures. No production default is silently changed by this chapter. A concise local note under the new run directory should name the two configurations, actual sample counts, observed rates/quantile intervals, resource/recovery costs, and the retain-or-revert decision. It needs no publication ceremony or checksum workflow.

Changing the index stride is not an excuse to skip payload CRC validation, move producer success before synchronization, advance persisted progress from volatile completion, or retain an unbounded read cache. Those are different semantics, not optimizations under this comparison. A more invasive sequential-cursor optimization would require its own proof about retries, noncontiguous selections, corruption, and rotation; this chapter does not smuggle one in as an unexplained shortcut.

## Make two measurement bugs fail visibly

First, in `Histogram::record`, temporarily replace the complete rounding expression

```rust
let micros = nanos / 1000 + u128::from(nanos % 1000 != 0);
```

with `let micros = nanos / 1000;`. Run:

```sh
cargo test -p broker-app --lib metrics::tests::submicrosecond_observations_round_up_but_zero_is_zero -- --exact
```

The expected failure is the `(1,3,1)` bin assertion: positive submicrosecond observations were rounded down into zero. Restore the exact complete expression above and rerun. This exercise changes precision, not the underlying operation durations.

Second, temporarily replace `p.peak = p.peak.max(p.checked_out);` in `BufferPool::take` with `p.peak = p.checked_out;`. Run:

```sh
cargo test -p broker-core --lib budget::memory::tests::peak_counts_capacity_and_survives_return -- --exact
```

The later smaller checkout incorrectly lowers the high water. Restore the `max` expression and rerun. Keep both repairs in place before the full workspace suite or measurement matrix. These deliberately broken variants are not alternate implementations to retain.

## Diagnose the first invalid boundary

| Observation | Check first | Do not conclude |
| --- | --- | --- |
| `ambiguous append` and no successful summary | Attempt counters, owner error, ticket timeout, storage error | That the record was definitely rejected or absent |
| Paced p99 improves while misses increase sharply | Started and successful populations, scheduled-to-finish bins | That all offered users received lower latency |
| `scanned` differs from geometry | Actual segment rotation, selected retries, payload size, read path | That a calculated count may replace the observed one |
| Dense scan count falls but throughput does not | Ack-cycle costs, owner activity, checker CPU, variability | That the implementation must have failed |
| Pool peaks stay bounded but RSS rises | Checker vectors, stacks, index capacity, allocator/residency, phase | That the pool accounting is automatically wrong or RSS is a strict capacity bound |
| Sampled disk bytes jump or briefly dip | Rotation/retention phase and raced-file count | That a non-atomic walk proves data loss |
| Final checkout is nonzero | Response batch and prepared append owners, remaining task handles | That dropping only the client released all buffers |
| Recovery appears extremely fast | Whether the timed log was populated, run path, retained floor | That an empty-log restart characterizes populated recovery |
| A sample has zero or nonfinite duration | Clock ordering and malformed summary | That infinite throughput is a real result |
| Python exits after killing a timed-out child | Child log and recorded phase; actual reaping | That forced termination was graceful or that pending appends rolled back |

The measurement runner is intentionally bounded enough to diagnose. It neither retries a failed comparison nor deletes its evidence. Rerun under a new name only after understanding which boundary failed.

## What is complete at the end of the course

The implementation now joins language-level ownership to system-level ownership: borrowed record views, owned permits, bounded response buffers, partition state machines, durable append publication, lease attempts, checkpoint frontiers, retained-floor authority, and supervised resource release. The same limits survive real transport, cancellation, restart, and pressure checks. The measurement layer makes their resource and latency costs visible without weakening those contracts.

The course covers one process, one topic, four fixed partitions, and two groups. It does not claim replication, Kafka compatibility, exactly-once external effects, comprehensive power-loss testing, or a benchmark target. A demonstrated optimization consists of the actual before/after runs and an explicit decision, including a justified rejection when the candidate is not useful. The final workspace should pass its deterministic checks, complete the end-to-end interruption/overload demonstration, and produce finite measurement files on the native machine. Those are executable engineering claims to verify, not accomplishments inferred from the amount of source code written.

