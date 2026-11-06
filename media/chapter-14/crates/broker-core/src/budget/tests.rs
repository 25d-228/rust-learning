use super::*;
use std::sync::{Arc, Barrier};
use std::thread;

fn budget(data: u64, live: u64) -> DiskBudget {
    DiskBudget::new(DiskQuota::new(data, 416).unwrap(), live).unwrap()
}

#[test]
fn quota_checks_minimum_headroom_and_total_overflow() {
    assert!(DiskQuota::new(95, 416).is_err());
    assert!(DiskQuota::new(96, 415).is_err());
    assert!(DiskQuota::new(u64::MAX, 416).is_err());
    assert_eq!(4 * DiskQuota::default().total_bytes(), 512 * 1024 * 1024);
}

#[test]
fn admission_counts_live_and_reserved_bytes_together() {
    let b = budget(166, 32);
    let first = b.reserve_append(35).unwrap();
    let second = b.reserve_append(35).unwrap();
    assert_eq!(b.state().reserved_bytes, 134);
    assert!(matches!(b.reserve_append(35), Err(DiskRefusal::Full)));
    assert!(b.state().charged_upper_bound().unwrap() <= b.state().quota.total_bytes());
    drop(first); drop(second);
    assert_eq!(b.state().reserved_bytes, 0);
}

#[test]
fn settlement_converts_only_actual_growth_to_live_bytes() {
    let b = budget(200, 32);
    b.reserve_append(35).unwrap().settle(35);
    assert_eq!((b.state().file_bytes, b.state().reserved_bytes), (67, 0));
    b.reserve_append(35).unwrap().settle(67);
    assert_eq!((b.state().file_bytes, b.state().reserved_bytes), (134, 0));
}

#[test]
fn ordinary_unwinding_returns_an_unsettled_reservation() {
    let b = budget(200, 32); let inner = b.clone();
    let result = std::panic::catch_unwind(move || {
        let _permit = inner.reserve_append(35).unwrap();
        panic!("before handoff");
    });
    assert!(result.is_err());
    assert_eq!(b.state().reserved_bytes, 0);
    assert!(b.reserve_append(35).is_ok());
}

#[test]
fn maintenance_can_use_one_header_but_not_metadata_capacity() {
    let b = budget(134, 134);
    assert!(matches!(b.reserve_append(32), Err(DiskRefusal::Full)));
    b.reserve_rotation(true).unwrap().settle(32);
    assert_eq!(b.state().file_bytes, 166);
    assert!(matches!(b.reserve_rotation(true), Err(DiskRefusal::Full)));
    b.reclaimed(134);
    assert!(b.reserve_append(35).is_ok());
}

#[test]
fn recovered_maintenance_header_is_charged_before_admission() {
    let b = budget(134, 166);
    assert!(matches!(b.reserve_append(32), Err(DiskRefusal::Full)));
    assert!(DiskBudget::new(DiskQuota::new(134, 416).unwrap(), 167).is_err());
}

#[test]
fn stop_closes_new_admission_but_existing_permits_still_settle_or_drop() {
    let b = budget(200, 32);
    let written = b.reserve_append(35).unwrap();
    let rejected = b.reserve_append(35).unwrap();
    b.stop();
    assert!(matches!(b.reserve_append(35), Err(DiskRefusal::Stopped)));
    written.settle(35); drop(rejected);
    assert_eq!((b.state().file_bytes, b.state().reserved_bytes), (67, 0));
    assert!(b.state().stopped);
    assert_eq!(b.state().charged_upper_bound(), None);
}

#[test]
fn permits_are_scoped_to_one_ledger_not_equal_numeric_quotas() {
    let a = budget(200, 32); let b = budget(200, 32);
    let permit = a.reserve_append(35).unwrap();
    assert!(permit.belongs_to(&a)); assert!(!permit.belongs_to(&b));
}

#[test]
fn racing_reservations_cannot_oversubscribe_the_remaining_bytes() {
    let b = budget(233, 32); // Exactly three 67-byte reservations.
    let gate = Arc::new(Barrier::new(12));
    let mut handles = Vec::new();
    for _ in 0..12 {
        let b = b.clone(); let gate = Arc::clone(&gate);
        handles.push(thread::spawn(move || { gate.wait(); b.reserve_append(35) }));
    }
    // Successful permits remain owned by the JoinHandle result, then by this vector.
    let permits: Vec<_> = handles.into_iter().filter_map(|h| h.join().unwrap().ok()).collect();
    assert_eq!(permits.len(), 3);
    assert_eq!(b.state().reserved_bytes, 201);
    drop(permits); assert_eq!(b.state().reserved_bytes, 0);
}
