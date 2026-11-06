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
