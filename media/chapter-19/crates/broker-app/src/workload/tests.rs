use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use broker_core::delivery::LeaseToken;
use broker_core::types::{Epoch, Offset};
use crate::client::ClientError;
use crate::protocol::Status;
use super::children::{decode_receipt, encode_receipt};
use super::ledger::{Key, Ledger, Outcome, Scan, classify};
use super::process::Addresses;
use super::{g, p, validate_rounds};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("broker-ledger-{}-{time}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for Directory {
    fn drop(&mut self) { if !std::thread::panicking() { let _ = fs::remove_dir_all(&self.0); } }
}
fn fixture() -> (Directory, Ledger) {
    let directory = Directory::new();
    let mut ledger = Ledger::create(&directory.0.join("ledger.tsv")).unwrap();
    ledger.begin_epoch(1).unwrap();
    (directory, ledger)
}
fn key(sequence: u64) -> Key { Key { producer: 0, sequence, attempt: 0 } }
fn token(offset: u64, group: u8, epoch: u64) -> LeaseToken {
    LeaseToken { epoch: Epoch::new(epoch).unwrap(), partition: p(0), group: g(group),
        offset: Offset::new(offset), generation: 1, deadline_ms: 30_000 }
}
fn add(ledger: &mut Ledger, key: Key, outcome: Outcome) {
    ledger.intent(key).unwrap(); ledger.resolve(key, outcome).unwrap();
}
fn observe(ledger: &mut Ledger, group: u8, epoch: u64, offset: u64, key: Key) -> Scan {
    ledger.observe(token(offset, group, epoch), &key.payload()).unwrap();
    Scan::from([((0, offset), key)])
}

#[test]
fn payload_has_independent_golden_bytes() {
    let value = Key { producer: 5, sequence: 0x0102030405060708, attempt: 2 };
    let expected = [76, 79, 65, 68, 48, 48, 48, 49, 0, 0, 0, 5, 0, 0, 0, 2, 1, 2, 3, 4, 5, 6, 7, 8];
    assert_eq!(value.payload(), expected);
    assert_eq!(Key::decode(&expected).unwrap(), value);
    assert_eq!(value.partition(), 1);
    assert_ne!(value.payload(), Key { attempt: 3, ..value }.payload());
}
#[test]
fn payload_rejects_short_long_and_wrong_magic() {
    let bytes = key(0).payload();
    for cut in 0..bytes.len() { assert!(Key::decode(&bytes[..cut]).is_err()); }
    let mut bad = bytes; bad[0] ^= 1;
    assert!(Key::decode(&bad).is_err());
    let mut long = bytes.to_vec(); long.push(0);
    assert!(Key::decode(&long).is_err());
}
#[test]
fn classification_never_calls_transport_uncertainty_rejection() {
    assert_eq!(classify(&Ok(Offset::new(7))), Outcome::Confirmed(7));
    for status in [Status::Invalid, Status::Busy, Status::DiskFull, Status::Unavailable,
        Status::AdmissionDeadline, Status::Refused] {
        assert_eq!(classify(&Err(ClientError::Remote(status))), Outcome::Rejected);
    }
    assert_eq!(classify(&Err(ClientError::Remote(Status::Uncertain))), Outcome::Ambiguous);
    for kind in [io::ErrorKind::TimedOut, io::ErrorKind::ConnectionReset, io::ErrorKind::UnexpectedEof] {
        assert_eq!(classify(&Err(ClientError::Io(io::Error::new(kind, "fixture")))), Outcome::Ambiguous);
    }
}
#[test]
fn intent_and_resolution_cannot_be_reused() {
    let (_directory, mut ledger) = fixture();
    ledger.intent(key(0)).unwrap();
    assert!(ledger.intent(key(0)).is_err());
    assert!(ledger.resolve(key(0), Outcome::Pending).is_err());
    ledger.resolve(key(0), Outcome::Ambiguous).unwrap();
    assert!(ledger.resolve(key(0), Outcome::Confirmed(0)).is_err());
}
#[test]
fn two_inputs_cannot_claim_one_position() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    let other = Key { producer: 4, sequence: 0, attempt: 0 };
    ledger.intent(other).unwrap();
    assert!(ledger.resolve(other, Outcome::Confirmed(0)).is_err());
}
#[test]
fn unknown_input_cannot_become_an_expectation() {
    let (_directory, mut ledger) = fixture();
    assert!(ledger.observe(token(0, 0, 1), &key(0).payload()).is_err());
}
#[test]
fn rejected_input_cannot_be_observed() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Rejected);
    assert!(ledger.observe(token(0, 0, 1), &key(0).payload()).is_err());
}
#[test]
fn confirmation_binds_both_offset_and_partition() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    assert!(ledger.observe(token(1, 0, 1), &key(0).payload()).is_err());
    let wrong = LeaseToken { partition: p(1), ..token(0, 0, 1) };
    assert!(ledger.observe(wrong, &key(0).payload()).is_err());
}
#[test]
fn ambiguity_allows_absence_or_one_matching_record() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Ambiguous);
    ledger.coverage(0, [0; 4], &Scan::new()).unwrap();
    let scan = observe(&mut ledger, 0, 1, 0, key(0));
    ledger.coverage(0, [1, 0, 0, 0], &scan).unwrap();
    let report = ledger.report(1);
    assert_eq!((report.ambiguous, report.observed_ambiguous, report.confirmed), (1, 1, 0));
}
#[test]
fn one_attempt_cannot_appear_at_two_offsets() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Ambiguous);
    observe(&mut ledger, 0, 1, 0, key(0));
    assert!(ledger.observe(token(1, 0, 1), &key(0).payload()).is_err());
}
#[test]
fn old_delivery_does_not_satisfy_recovery_coverage() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    let old = observe(&mut ledger, 0, 1, 0, key(0));
    ledger.coverage(0, [1, 0, 0, 0], &old).unwrap();
    ledger.begin_epoch(2).unwrap();
    assert!(ledger.coverage(0, [1, 0, 0, 0], &old).is_err());
    assert!(ledger.observe(token(0, 0, 1), &key(0).payload()).is_err());
    let fresh = observe(&mut ledger, 0, 2, 0, key(0));
    ledger.coverage(0, [1, 0, 0, 0], &fresh).unwrap();
}
#[test]
fn missing_confirmed_record_is_not_hidden_by_an_empty_end() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    assert!(ledger.coverage(0, [0; 4], &Scan::new()).is_err());
}
#[test]
fn groups_cannot_share_delivery_evidence() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    let scan = observe(&mut ledger, 0, 1, 0, key(0));
    assert!(ledger.coverage(1, [1, 0, 0, 0], &scan).is_err());
}
#[test]
fn interval_gaps_and_extra_offsets_are_rejected() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    add(&mut ledger, key(1), Outcome::Confirmed(2));
    let mut scan = observe(&mut ledger, 0, 1, 0, key(0));
    scan.extend(observe(&mut ledger, 0, 1, 2, key(1)));
    assert!(ledger.coverage(0, [3, 0, 0, 0], &scan).is_err());
    assert!(ledger.coverage(0, [1, 0, 0, 0], &scan).is_err());
}
#[test]
fn checkpoint_cannot_outrun_checked_coverage() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    assert!(ledger.checkpoint(0, 0, 1).is_err());
}
#[test]
fn deletion_needs_both_checkpointed_coverage_frontiers() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    for group in 0..2 {
        let scan = observe(&mut ledger, group, 1, 0, key(0));
        ledger.coverage(group, [1, 0, 0, 0], &scan).unwrap();
    }
    ledger.checkpoint(0, 0, 1).unwrap();
    assert!(ledger.retained(0, 1).is_err());
    ledger.checkpoint(1, 0, 1).unwrap();
    ledger.retained(0, 1).unwrap();
    ledger.begin_epoch(2).unwrap();
    for group in 0..2 { ledger.coverage(group, [1, 0, 0, 0], &Scan::new()).unwrap(); }
}
#[test]
fn delivery_repetition_is_distinct_from_duplicate_logical_input() {
    let (_directory, mut ledger) = fixture();
    let retried = Key { attempt: 1, ..key(0) };
    add(&mut ledger, key(0), Outcome::Ambiguous);
    add(&mut ledger, retried, Outcome::Confirmed(1));
    observe(&mut ledger, 0, 1, 0, key(0));
    observe(&mut ledger, 0, 1, 0, key(0));
    observe(&mut ledger, 0, 1, 1, retried);
    let report = ledger.report(1);
    assert_eq!(report.unique_records, 2);
    assert_eq!(report.repeated_deliveries, 1);
    assert_eq!(report.duplicate_logical_inputs, 1);
}
#[test]
fn unresolved_append_prevents_coverage_success() {
    let (_directory, mut ledger) = fixture();
    ledger.intent(key(0)).unwrap();
    assert!(ledger.coverage(0, [0; 4], &Scan::new()).is_err());
}
#[test]
fn victim_receipt_round_trips_and_rejects_wrong_scope() {
    let original = token(3, 1, 2);
    let bytes = encode_receipt(original, &key(0).payload()).unwrap();
    assert_eq!(&bytes[..8], b"VICTIM01");
    let (read, payload) = decode_receipt(&bytes).unwrap();
    assert_eq!(read, original);
    assert_eq!(payload, key(0).payload());
    for cut in 0..64 { assert!(decode_receipt(&bytes[..cut]).is_err()); }
    assert!(encode_receipt(token(0, 0, 1), &key(0).payload()).is_err());
}
#[test]
fn rendezvous_requires_loopback_bound_ports_and_nonzero_epoch() {
    let value = Addresses::parse(b"127.0.0.1:1234 127.0.0.1:1235 127.0.0.1:1236 2\n").unwrap();
    assert_eq!(value.epoch, 2);
    for bad in ["127.0.0.1:0 127.0.0.1:2 127.0.0.1:3 1",
        "192.0.2.1:1 127.0.0.1:2 127.0.0.1:3 1",
        "127.0.0.1:1 127.0.0.1:2 127.0.0.1:3 0", "missing fields"] {
        assert!(Addresses::parse(bad.as_bytes()).is_err());
    }
}
#[test]
fn bounded_rounds_and_monotonic_epochs_are_checked() {
    assert!(validate_rounds(0).is_err());
    assert!(validate_rounds(33).is_err());
    assert!(validate_rounds(1).is_ok());
    assert!(validate_rounds(32).is_ok());
    let (_directory, mut ledger) = fixture();
    assert!(ledger.begin_epoch(1).is_err());
    assert!(ledger.begin_epoch(0).is_err());
}
