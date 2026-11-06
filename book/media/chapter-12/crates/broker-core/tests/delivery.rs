mod common;

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use broker_core::delivery::{DeliveryBatch, DeliveryConfig, DeliveryStop, GroupState};
use broker_core::delivery::window::{CompletionKind, MAX_WINDOW, Slot, Window, WindowError};
use broker_core::storage::log::segment_name;
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::{Epoch, GroupId, Offset};
use broker_core::worker::{BatchConfig, Client, RequestError, Ticket, WorkerHandle};
use common::TestDir;

fn offset(value: u64) -> Offset { Offset::new(value) }
fn group(value: u8) -> GroupId { GroupId::new(value).unwrap() }
fn limits(records: usize, bytes: usize) -> ReadLimits { ReadLimits::new(records, bytes).unwrap() }
fn receive<T>(ticket: Ticket<T>) -> Result<T, RequestError> {
    ticket.recv_timeout(Duration::from_secs(5)).expect("owner reply watchdog")
}
fn state(client: &Client, id: u8) -> GroupState {
    receive(client.try_group_state(group(id)).unwrap()).unwrap()
}
fn deliver(client: &Client, id: u8, records: usize, bytes: usize) -> DeliveryBatch {
    receive(client.try_deliver(group(id), limits(records, bytes)).unwrap()).unwrap()
}
fn offsets(batch: &DeliveryBatch) -> Vec<u64> {
    batch.records.iter().map(|record| record.offset().get()).collect()
}
fn batch(count: usize) -> BatchConfig {
    BatchConfig::new(count, 4096, Duration::from_secs(60)).unwrap()
}

// The child path is absent, as required by explicit initialization.
fn fixture(payloads: &[&[u8]], width: usize, segment_bytes: u64)
    -> (TestDir, PathBuf, WorkerHandle, Client)
{
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("broker");
    let (epoch, mut logs) = recovery::initialize(&root,
        StorageConfig::new(segment_bytes, 2, 16).unwrap()).unwrap().into_parts();
    let log = logs.remove(0);
    drop(logs);
    let owner = WorkerHandle::start_with_delivery(log, FileSync, batch(1), 16, 16,
        DeliveryConfig::new(epoch, width).unwrap()).unwrap();
    let client = owner.client();
    for (index, payload) in payloads.iter().enumerate() {
        assert_eq!(receive(client.try_publish(payload.to_vec()).unwrap()).unwrap(), offset(index as u64));
    }
    (directory, root, owner, client)
}

fn same_window(window: &Window, completed: u64, issued: u64, done: &BTreeSet<u64>) {
    let state = window.state();
    assert_eq!((state.completed_end.get(), state.issued_end.get()), (completed, issued));
    assert_eq!(state.occupied, usize::try_from(issued - completed).unwrap());
    assert!(state.occupied <= state.capacity);
    for value in completed..issued {
        let expected = if done.contains(&value) { Slot::Completed } else { Slot::Outstanding };
        assert_eq!(window.status(offset(value)), Some(expected));
    }
    assert_eq!(window.status(offset(issued)), None);
    if completed != 0 { assert_eq!(window.status(offset(completed - 1)), None); }
}

#[test]
fn capacity_policy_is_checked_before_allocation() {
    assert!(matches!(Window::new(offset(0), 0), Err(WindowError::Capacity(0))));
    assert!(matches!(Window::new(offset(0), MAX_WINDOW + 1), Err(WindowError::Capacity(_))));
    assert_eq!(Window::new(offset(0), MAX_WINDOW).unwrap().state().capacity, MAX_WINDOW);
    assert!(DeliveryConfig::new(Epoch::new(1).unwrap(), 0).is_err());
}

#[test]
fn admission_can_fill_exactly_and_does_not_retire_anything() {
    let mut window = Window::new(offset(10), 4).unwrap();
    window.admit(4, offset(14)).unwrap();
    same_window(&window, 10, 14, &BTreeSet::new());
    assert_eq!(window.state().free(), 0);
    assert_eq!(window.read_upper(offset(100)).unwrap(), offset(14));
}

#[test]
fn failed_admissions_do_not_change_slots_or_frontiers() {
    let mut window = Window::new(offset(7), 3).unwrap();
    window.admit(1, offset(12)).unwrap();
    let before = window.state();
    assert_eq!(window.admit(3, offset(12)), Err(WindowError::Full { requested: 3, free: 2 }));
    assert!(matches!(window.admit(2, offset(9)), Err(WindowError::BeyondVisible { .. })));
    assert!(matches!(window.admit(0, offset(7)), Err(WindowError::RegressedVisible { .. })));
    assert_eq!(window.state(), before);
    same_window(&window, 7, 8, &BTreeSet::new());
}

#[test]
fn completion_order_two_zero_three_one_retires_only_a_prefix() {
    let mut window = Window::new(offset(0), 4).unwrap();
    window.admit(4, offset(4)).unwrap();
    for (value, expected_end, retired) in [(2, 0, 0), (0, 1, 1), (3, 1, 0), (1, 4, 3)] {
        let answer = window.complete(offset(value)).unwrap();
        assert_eq!(answer.kind, CompletionKind::Applied);
        assert_eq!(answer.retired, retired);
        assert_eq!(answer.state.completed_end, offset(expected_end));
        assert_eq!(answer.state.occupied, (4 - expected_end) as usize);
    }
}

#[test]
fn a_stalled_head_keeps_completed_followers_occupied() {
    let mut window = Window::new(offset(0), 4).unwrap();
    window.admit(4, offset(20)).unwrap();
    for value in 1..4 { assert_eq!(window.complete(offset(value)).unwrap().retired, 0); }
    assert_eq!(window.state().free(), 0);
    assert_eq!(window.admit(1, offset(20)), Err(WindowError::Full { requested: 1, free: 0 }));
    assert_eq!(window.complete(offset(0)).unwrap().retired, 4);
    window.admit(4, offset(20)).unwrap();
    same_window(&window, 4, 8, &BTreeSet::new());
}

#[test]
fn duplicate_and_retired_completions_do_not_double_count() {
    let mut window = Window::new(offset(0), 3).unwrap();
    window.admit(3, offset(3)).unwrap();
    window.complete(offset(1)).unwrap();
    let before = window.state();
    let duplicate = window.complete(offset(1)).unwrap();
    assert_eq!((duplicate.kind, duplicate.retired), (CompletionKind::Duplicate, 0));
    assert_eq!(window.state(), before);
    assert_eq!(window.complete(offset(0)).unwrap().retired, 2);
    let retired = window.complete(offset(1)).unwrap();
    assert_eq!((retired.kind, retired.retired), (CompletionKind::AlreadyRetired, 0));
    assert_eq!(retired.state.completed_end, offset(2));
}

#[test]
fn nonzero_start_is_an_assumed_prefix_not_delivery_history() {
    let mut window = Window::new(offset(100), 2).unwrap();
    assert_eq!(window.complete(offset(17)).unwrap().kind, CompletionKind::AlreadyRetired);
    assert!(matches!(window.complete(offset(100)), Err(WindowError::NotIssued { .. })));
    window.admit(2, offset(102)).unwrap();
    window.complete(offset(101)).unwrap();
    assert_eq!(window.complete(offset(100)).unwrap().retired, 2);
    assert_eq!(window.state().completed_end, offset(102));
}

#[test]
fn last_representable_exclusive_end_is_valid_but_does_not_wrap() {
    let mut window = Window::new(offset(u64::MAX - 1), 2).unwrap();
    assert_eq!(window.admit(2, offset(u64::MAX)), Err(WindowError::OffsetExhausted));
    assert_eq!(window.state().occupied, 0);
    window.admit(1, offset(u64::MAX)).unwrap();
    assert_eq!(window.complete(offset(u64::MAX - 1)).unwrap().state.completed_end, offset(u64::MAX));
    assert_eq!(window.admit(1, offset(u64::MAX)), Err(WindowError::OffsetExhausted));
    window.admit(0, offset(u64::MAX)).unwrap();
    assert_eq!(window.read_upper(offset(u64::MAX)).unwrap(), offset(u64::MAX));
}

#[test]
fn width_one_reuses_its_only_slot() {
    let mut window = Window::new(offset(0), 1).unwrap();
    for value in 0..100 {
        window.admit(1, offset(value + 1)).unwrap();
        assert_eq!(window.status(offset(value)), Some(Slot::Outstanding));
        assert_eq!(window.complete(offset(value)).unwrap().retired, 1);
        assert_eq!(window.state().free(), 1);
    }
}

#[test]
fn read_upper_intersects_visible_work_with_free_slots() {
    let mut window = Window::new(offset(5), 4).unwrap();
    assert_eq!(window.read_upper(offset(5)).unwrap(), offset(5));
    assert_eq!(window.read_upper(offset(7)).unwrap(), offset(7));
    assert_eq!(window.read_upper(offset(100)).unwrap(), offset(9));
    window.admit(2, offset(100)).unwrap();
    assert_eq!(window.read_upper(offset(100)).unwrap(), offset(9));
    assert!(matches!(window.read_upper(offset(6)), Err(WindowError::RegressedVisible { .. })));
}

#[test]
fn wrapped_slots_preserve_remaining_outstanding_positions() {
    let mut window = Window::new(offset(0), 4).unwrap();
    window.admit(4, offset(9)).unwrap();
    window.complete(offset(2)).unwrap();
    window.complete(offset(0)).unwrap();
    window.admit(1, offset(9)).unwrap(); // offset 4 reuses physical slot 0.
    window.complete(offset(3)).unwrap();
    window.complete(offset(1)).unwrap(); // retires 1, 2, 3 but not 4.
    assert_eq!(window.status(offset(4)), Some(Slot::Outstanding));
    assert_eq!((window.state().completed_end.get(), window.state().occupied), (4, 1));
    window.admit(3, offset(9)).unwrap();
    for value in [7, 5, 6, 4] { window.complete(offset(value)).unwrap(); }
    assert_eq!((window.state().completed_end.get(), window.state().free()), (8, 4));
}

#[test]
fn all_twenty_four_orders_agree_with_a_set_reference() {
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    let order = [a, b, c, d];
                    if order.iter().copied().collect::<BTreeSet<_>>().len() != 4 { continue; }
                    let mut window = Window::new(offset(0), 4).unwrap();
                    window.admit(4, offset(4)).unwrap();
                    let mut done = BTreeSet::new();
                    let mut completed = 0;
                    for value in order {
                        done.insert(value);
                        while done.remove(&completed) { completed += 1; }
                        window.complete(offset(value)).unwrap();
                        same_window(&window, completed, 4, &done);
                    }
                }
            }
        }
    }
}

#[test]
fn mixed_admission_and_completion_match_a_set_across_many_wraps() {
    for width in [1, 2, 3, 4, 7, 17] {
        let mut window = Window::new(offset(11), width).unwrap();
        let (mut completed, mut issued) = (11_u64, 11_u64);
        let mut done = BTreeSet::new();
        let mut seed = 9_u64;
        for step in 0..1200 {
            // Intentional modular arithmetic for a repeatable TEST selector only.
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let free = width - usize::try_from(issued - completed).unwrap();
            let count = (seed as usize) % (free + 1);
            window.admit(count, offset(100_000)).unwrap();
            issued += count as u64;
            if issued != completed {
                let value = if step % 5 == 0 { completed }
                    else { completed + (seed >> 17) % (issued - completed) };
                done.insert(value);
                while done.remove(&completed) { completed += 1; }
                window.complete(offset(value)).unwrap();
            }
            same_window(&window, completed, issued, &done);
            let before = window.state();
            assert!(matches!(window.complete(offset(issued)), Err(WindowError::NotIssued { .. })));
            assert_eq!(window.state(), before);
        }
        assert!(completed > 11 + 4 * width as u64);
    }
}

#[test]
fn empty_visible_suffix_is_success_without_issuing_slots() {
    let (_directory, _root, owner, client) = fixture(&[], 4, 4096);
    let answer = deliver(&client, 0, 8, 1000);
    assert!(answer.records.is_empty());
    assert_eq!(answer.stop, DeliveryStop::VisibleEnd);
    assert_eq!((answer.state.window.occupied, answer.scanned_records), (0, 0));
    owner.join().unwrap();
}

#[test]
fn two_groups_share_bytes_but_have_independent_windows() {
    let (_directory, _root, owner, client) = fixture(&[b"cat", b"dog", b"owl", b"fox"], 4, 102);
    let first = deliver(&client, 0, 8, 1000);
    let second = deliver(&client, 1, 2, 1000);
    assert_eq!(offsets(&first), vec![0, 1, 2, 3]);
    assert_eq!(offsets(&second), vec![0, 1]);
    assert_eq!(first.records[0].payload(), second.records[0].payload());
    for value in [2, 0, 3, 1] {
        receive(client.try_complete(group(0), offset(value)).unwrap()).unwrap();
    }
    assert_eq!(state(&client, 0).window.completed_end, offset(4));
    assert_eq!((state(&client, 1).window.completed_end.get(), state(&client, 1).window.occupied), (0, 2));
    owner.join().unwrap();
}

#[test]
fn full_window_stops_reads_even_when_followers_are_complete() {
    let (_directory, _root, owner, client) = fixture(&[b"a", b"b", b"c", b"d"], 2, 4096);
    let first = deliver(&client, 0, 8, 1000);
    assert_eq!(offsets(&first), vec![0, 1]);
    assert_eq!(first.stop, DeliveryStop::WindowFull);
    receive(client.try_complete(group(0), offset(1)).unwrap()).unwrap();
    let blocked = deliver(&client, 0, 8, 1000);
    assert_eq!((blocked.records.len(), blocked.scanned_records, blocked.encoded_bytes), (0, 0, 0));
    assert_eq!(blocked.stop, DeliveryStop::WindowFull);
    receive(client.try_complete(group(0), offset(0)).unwrap()).unwrap();
    assert_eq!(offsets(&deliver(&client, 0, 8, 1000)), vec![2, 3]);
    owner.join().unwrap();
}

#[test]
fn record_limit_does_not_reserve_the_unread_suffix() {
    let (_directory, _root, owner, client) = fixture(&[b"a", b"b", b"c", b"d"], 4, 4096);
    let answer = deliver(&client, 0, 2, 1000);
    assert_eq!(offsets(&answer), vec![0, 1]);
    assert_eq!(answer.stop, DeliveryStop::RecordLimit);
    assert_eq!(answer.state.window.free(), 2);
    assert_eq!(offsets(&deliver(&client, 0, 2, 1000)), vec![2, 3]);
    owner.join().unwrap();
}

#[test]
fn byte_limit_advances_only_by_records_actually_copied() {
    let (_directory, _root, owner, client) = fixture(&[b"cat", b"dog", b"owl"], 4, 4096);
    let answer = deliver(&client, 0, 4, 70);
    assert_eq!(offsets(&answer), vec![0, 1]);
    assert_eq!(answer.encoded_bytes, 70);
    assert_eq!(answer.stop, DeliveryStop::ByteLimit { required: 35 });
    assert_eq!(answer.state.window.issued_end, offset(2));
    assert_eq!(offsets(&deliver(&client, 0, 4, 35)), vec![2]);
    owner.join().unwrap();
}

#[test]
fn a_nonfitting_first_record_is_not_consumed_even_for_empty_payloads() {
    let (_directory, _root, owner, client) = fixture(&[b"", b"cat"], 4, 4096);
    let blocked = deliver(&client, 0, 4, 31);
    assert!(blocked.records.is_empty());
    assert_eq!(blocked.stop, DeliveryStop::ByteLimit { required: 32 });
    assert_eq!(blocked.state.window.issued_end, offset(0));
    let empty = deliver(&client, 0, 4, 32);
    assert_eq!(offsets(&empty), vec![0]);
    assert!(empty.records[0].payload().is_empty());
    assert_eq!(empty.stop, DeliveryStop::ByteLimit { required: 35 });
    assert_eq!(offsets(&deliver(&client, 0, 4, 35)), vec![1]);
    owner.join().unwrap();
}

#[test]
fn written_but_unsynchronized_records_do_not_enter_a_window() {
    let directory = TestDir::new().unwrap();
    let (epoch, mut logs) = recovery::initialize(&directory.path().join("broker"),
        StorageConfig::new(4096, 2, 8).unwrap()).unwrap().into_parts();
    let owner = WorkerHandle::start_with_delivery(logs.remove(0), FileSync, batch(4), 8, 8,
        DeliveryConfig::new(epoch, 4).unwrap()).unwrap();
    drop(logs);
    let client = owner.client();
    let publish = client.try_publish(b"cat".to_vec()).unwrap();
    let mut observed = false;
    for _ in 0..3 {
        let snapshot = receive(client.try_snapshot().unwrap()).unwrap();
        if snapshot.written_end == offset(1) {
            assert_eq!(snapshot.visible_end, offset(0));
            observed = true;
            break;
        }
    }
    assert!(observed, "alternating owner lanes must process the queued publish");
    let hidden = deliver(&client, 0, 4, 1000);
    assert_eq!(hidden.stop, DeliveryStop::VisibleEnd);
    assert_eq!(hidden.state.window.issued_end, offset(0));
    receive(client.try_flush().unwrap()).unwrap();
    assert_eq!(receive(publish).unwrap(), offset(0));
    assert_eq!(offsets(&deliver(&client, 0, 4, 1000)), vec![0]);
    owner.join().unwrap();
}

#[test]
fn abandoning_a_delivery_reply_does_not_rewind_issuance() {
    let (_directory, _root, owner, client) = fixture(&[b"a", b"b", b"c"], 2, 4096);
    let ticket = client.try_deliver(group(0), limits(2, 1000)).unwrap();
    drop(ticket);
    // Same control lane, same submitting thread: the snapshot follows the delivery.
    let observed = state(&client, 0);
    assert_eq!((observed.window.issued_end.get(), observed.window.occupied), (2, 2));
    assert_eq!(deliver(&client, 0, 2, 1000).stop, DeliveryStop::WindowFull);
    owner.join().unwrap();
}

#[test]
fn invalid_and_duplicate_completions_leave_the_owner_usable() {
    let (_directory, _root, owner, client) = fixture(&[b"a", b"b"], 2, 4096);
    deliver(&client, 0, 2, 1000);
    let before = state(&client, 0);
    assert!(matches!(receive(client.try_complete(group(0), offset(2)).unwrap()),
        Err(RequestError::Delivery(WindowError::NotIssued { .. }))));
    assert_eq!(state(&client, 0), before);
    receive(client.try_complete(group(0), offset(1)).unwrap()).unwrap();
    let duplicate = receive(client.try_complete(group(0), offset(1)).unwrap()).unwrap();
    assert_eq!(duplicate.completion.kind, CompletionKind::Duplicate);
    assert_eq!(duplicate.state.window.completed_end, offset(0));
    receive(client.try_complete(group(0), offset(0)).unwrap()).unwrap();
    let retired = receive(client.try_complete(group(0), offset(1)).unwrap()).unwrap();
    assert_eq!(retired.completion.kind, CompletionKind::AlreadyRetired);
    assert_eq!(receive(client.try_publish(b"c".to_vec()).unwrap()).unwrap(), offset(2));
    owner.join().unwrap();
}

#[test]
fn diagnostic_fetch_does_not_issue_or_complete_group_work() {
    let (_directory, _root, owner, client) = fixture(&[b"cat", b"dog"], 4, 4096);
    let before = state(&client, 0);
    let raw = receive(client.try_fetch(offset(0), limits(8, 1000)).unwrap()).unwrap();
    assert_eq!(raw.records.len(), 2);
    assert_eq!(state(&client, 0), before);
    assert_eq!(state(&client, 1).window.issued_end, offset(0));
    assert_eq!(offsets(&deliver(&client, 0, 8, 1000)), vec![0, 1]);
    owner.join().unwrap();
}

#[test]
fn completion_changes_memory_not_segment_or_metadata_bytes() {
    let (_directory, root, owner, client) = fixture(&[b"cat", b"dog"], 4, 4096);
    let path = root.join("partition-0").join(segment_name(offset(0)));
    let segment_before = fs::read(&path).unwrap();
    let metadata_path = root.join(broker_core::storage::metadata::META_NAME);
    let metadata_before = fs::read(&metadata_path).unwrap();
    deliver(&client, 0, 4, 1000);
    for value in [1, 0] { receive(client.try_complete(group(0), offset(value)).unwrap()).unwrap(); }
    owner.join().unwrap();
    assert_eq!(fs::read(path).unwrap(), segment_before);
    assert_eq!(fs::read(metadata_path).unwrap(), metadata_before);
}

#[test]
fn restart_resets_volatile_windows_and_keeps_the_new_epoch() {
    let (_directory, root, owner, client) = fixture(&[b"cat", b"dog"], 4, 4096);
    deliver(&client, 0, 4, 1000);
    for value in [1, 0] { receive(client.try_complete(group(0), offset(value)).unwrap()).unwrap(); }
    assert_eq!(state(&client, 0).window.completed_end, offset(2));
    owner.join().unwrap();
    drop(client);
    let (epoch, mut logs) = recovery::recover(&root).unwrap().into_parts();
    assert_eq!(epoch.get(), 2);
    let owner = WorkerHandle::start_with_delivery(logs.remove(0), FileSync, batch(1), 8, 8,
        DeliveryConfig::new(epoch, 4).unwrap()).unwrap();
    drop(logs);
    let client = owner.client();
    assert_eq!(state(&client, 0).window.completed_end, offset(0));
    let repeated = deliver(&client, 0, 4, 1000);
    assert_eq!(repeated.state.epoch, epoch);
    assert_eq!(offsets(&repeated), vec![0, 1]);
    owner.join().unwrap();
}

#[test]
fn storage_only_start_refuses_delivery_without_stopping() {
    let directory = TestDir::new().unwrap();
    let (_epoch, mut logs) = recovery::initialize(&directory.path().join("broker"),
        StorageConfig::new(4096, 2, 8).unwrap()).unwrap().into_parts();
    let owner = WorkerHandle::start(logs.remove(0), FileSync, batch(1), 8, 8).unwrap();
    drop(logs);
    let client = owner.client();
    assert!(matches!(receive(client.try_group_state(group(0)).unwrap()), Err(RequestError::DeliveryDisabled)));
    assert!(matches!(receive(client.try_deliver(group(0), limits(4, 1000)).unwrap()), Err(RequestError::DeliveryDisabled)));
    assert!(matches!(receive(client.try_complete(group(0), offset(0)).unwrap()), Err(RequestError::DeliveryDisabled)));
    assert_eq!(receive(client.try_publish(b"cat".to_vec()).unwrap()).unwrap(), offset(0));
    owner.join().unwrap();
}

#[test]
fn all_four_partition_owners_keep_distinct_group_identity() {
    let directory = TestDir::new().unwrap();
    let (epoch, logs) = recovery::initialize(&directory.path().join("broker"),
        StorageConfig::new(4096, 2, 8).unwrap()).unwrap().into_parts();
    let mut owners = Vec::new();
    for log in logs {
        owners.push(WorkerHandle::start_with_delivery(log, FileSync, batch(1), 8, 8,
            DeliveryConfig::new(epoch, 2).unwrap()).unwrap());
    }
    for (index, owner) in owners.iter().enumerate() {
        let client = owner.client();
        receive(client.try_publish(vec![index as u8]).unwrap()).unwrap();
        for id in 0..2 {
            let answer = deliver(&client, id, 4, 1000);
            assert_eq!(answer.state.epoch, epoch);
            assert_eq!(answer.state.partition.get(), index as u16);
            assert_eq!(answer.state.group, group(id));
            assert_eq!(answer.records[0].payload(), &[index as u8]);
            assert_eq!(answer.state.window.completed_end, offset(0));
        }
    }
    for owner in owners { owner.join().unwrap(); }
}

#[test]
fn delivery_crosses_segments_and_wraps_without_rewriting_storage() {
    let (_directory, root, owner, client) = fixture(&[b"cat"; 8], 3, 102);
    let mut seen = Vec::new();
    loop {
        let answer = deliver(&client, 0, 8, 1000);
        let values = offsets(&answer);
        if values.is_empty() { break; }
        seen.extend_from_slice(&values);
        for value in values.into_iter().rev() {
            receive(client.try_complete(group(0), offset(value)).unwrap()).unwrap();
        }
    }
    assert_eq!(seen, (0..8).collect::<Vec<_>>());
    assert_eq!(state(&client, 0).window.completed_end, offset(8));
    owner.join().unwrap();
    let files: Vec<_> = fs::read_dir(root.join("partition-0")).unwrap().collect();
    assert_eq!(files.len(), 4); // Two 35-byte records + a 32-byte segment header = 102.
}


// Chapter 12 additions. Keep all Chapter 11 tests above this line.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use broker_core::delivery::{Ack, AckOutcome, AckRejection, LeaseBatch, LeaseConfig, LeaseError,
    LeaseStop, LeaseToken};
use broker_core::delivery::clock::{Clock, ClockError, ManualClock};
use broker_core::types::PartitionId;

fn leased_fixture(payloads: &[&[u8]], width: usize, clock: Arc<dyn Clock>)
    -> (TestDir, PathBuf, WorkerHandle, Client)
{
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("broker");
    let (epoch, mut logs) = recovery::initialize(&root,
        StorageConfig::new(102, 2, 16).unwrap()).unwrap().into_parts();
    let log = logs.remove(0);
    drop(logs);
    let owner = WorkerHandle::start_with_leases(log, FileSync, batch(1), 16, 16,
        LeaseConfig::new(epoch, width, 10).unwrap(), clock).unwrap();
    let client = owner.client();
    for payload in payloads {
        receive(client.try_publish(payload.to_vec()).unwrap()).unwrap();
    }
    (directory, root, owner, client)
}
fn leased(client: &Client, id: u8, records: usize, bytes: usize) -> LeaseBatch {
    receive(client.try_lease(group(id), limits(records, bytes)).unwrap()).unwrap()
}
fn acknowledged(client: &Client, id: u8, token: LeaseToken) -> Ack {
    receive(client.try_ack(group(id), token).unwrap()).unwrap()
}
fn lease_offsets(batch: &LeaseBatch) -> Vec<u64> {
    batch.records.iter().map(|item| item.token.offset.get()).collect()
}

#[test]
fn lease_manual_clock_is_checked_and_shared() {
    let clock = Arc::new(ManualClock::new(u64::MAX - 1));
    let other = clock.clone();
    assert_eq!(other.advance(1).unwrap(), u64::MAX);
    assert_eq!(clock.now().unwrap(), u64::MAX);
    assert_eq!(clock.advance(1), Err(ClockError::Exhausted));
    assert_eq!(clock.now().unwrap(), u64::MAX);
}

#[test]
fn lease_owner_retries_at_equality_then_rejects_the_previous_generation() {
    let clock = Arc::new(ManualClock::new(100));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat"], 1, clock.clone());
    let first = leased(&client, 0, 1, 35).records.remove(0);
    assert_eq!(first.token.deadline_ms, 110);
    clock.advance(9).unwrap();
    assert!(leased(&client, 0, 1, 35).records.is_empty());
    clock.advance(1).unwrap();
    let before = state(&client, 0);
    assert_eq!(acknowledged(&client, 0, first.token).outcome, AckOutcome::Rejected(AckRejection::Expired));
    let second = leased(&client, 0, 1, 35).records.remove(0);
    assert_eq!(second.record.payload(), first.record.payload());
    assert_eq!(second.token.generation, 2);
    assert_eq!(state(&client, 0), before);
    assert_eq!(acknowledged(&client, 0, first.token).outcome,
        AckOutcome::Rejected(AckRejection::StaleGeneration));
    assert_eq!(acknowledged(&client, 0, second.token).retired, 1);
    owner.join().unwrap();
}

#[test]
fn lease_abandoned_reply_recovers_by_expiration_not_rollback() {
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat"], 1, clock.clone());
    drop(client.try_lease(group(0), limits(1, 35)).unwrap());
    // Same control lane: this snapshot is processed after the abandoned delivery.
    assert_eq!(state(&client, 0).window.occupied, 1);
    assert!(leased(&client, 0, 1, 35).records.is_empty());
    clock.advance(10).unwrap();
    let retried = leased(&client, 0, 1, 35);
    assert_eq!(retried.records[0].token.generation, 2);
    assert_eq!(retried.state.window.issued_end, offset(1));
    owner.join().unwrap();
}

#[test]
fn lease_byte_refusal_does_not_consume_new_or_retry_attempts() {
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat", b"dog"], 2, clock.clone());
    let empty = leased(&client, 0, 2, 34);
    assert_eq!(empty.stop, LeaseStop::ByteLimit { required: 35 });
    assert_eq!(empty.state.window.occupied, 0);
    let first = leased(&client, 0, 2, 35);
    assert_eq!(lease_offsets(&first), vec![0]);
    assert_eq!(first.stop, LeaseStop::ByteLimit { required: 35 });
    clock.advance(10).unwrap();
    let before = state(&client, 0);
    assert!(leased(&client, 0, 2, 34).records.is_empty());
    assert_eq!(state(&client, 0), before);
    let retried = leased(&client, 0, 2, 35);
    assert_eq!(lease_offsets(&retried), vec![0]);
    assert_eq!(retried.records[0].token.generation, 2);
    assert_eq!(retried.state.window.issued_end, offset(1));
    let new = leased(&client, 0, 1, 35);
    assert_eq!((new.records[0].token.offset.get(), new.records[0].token.generation), (1, 1));
    owner.join().unwrap();
}

#[test]
fn lease_out_of_order_acknowledgements_do_not_release_a_gap() {
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat", b"dog", b"owl"], 3, clock.clone());
    let original = leased(&client, 0, 3, 105);
    assert_eq!(acknowledged(&client, 0, original.records[2].token).retired, 0);
    assert_eq!(acknowledged(&client, 0, original.records[1].token).retired, 0);
    clock.advance(10).unwrap();
    let retry = leased(&client, 0, 3, 105);
    assert_eq!(lease_offsets(&retry), vec![0]);
    assert_eq!(retry.state.window.occupied, 3);
    let accepted = acknowledged(&client, 0, retry.records[0].token);
    assert_eq!((accepted.retired, accepted.state.window.completed_end.get()), (3, 3));
    assert_eq!(acknowledged(&client, 0, original.records[2].token).outcome, AckOutcome::AlreadyRetired);
    owner.join().unwrap();
}

#[test]
fn lease_group_partition_and_deadline_fields_are_not_advisory() {
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat"], 1, clock);
    let a = leased(&client, 0, 1, 35).records[0].token;
    let b = leased(&client, 1, 1, 35).records[0].token;
    let before = state(&client, 1);
    assert_eq!(acknowledged(&client, 1, a).outcome, AckOutcome::Rejected(AckRejection::WrongGroup));
    assert_eq!(acknowledged(&client, 1, LeaseToken { partition: PartitionId::new(1).unwrap(), ..b }).outcome,
        AckOutcome::Rejected(AckRejection::WrongPartition));
    assert_eq!(acknowledged(&client, 1, LeaseToken { deadline_ms: 999, ..b }).outcome,
        AckOutcome::Rejected(AckRejection::WrongDeadline));
    assert_eq!(state(&client, 1), before);
    assert_eq!(acknowledged(&client, 0, a).retired, 1);
    assert_eq!(state(&client, 1), before);
    owner.join().unwrap();
}

#[test]
fn lease_owners_refuse_the_legacy_tokenless_path() {
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat"], 1, clock);
    let token = leased(&client, 0, 1, 35).records[0].token;
    let before = state(&client, 0);
    assert!(matches!(receive(client.try_complete(group(0), offset(0)).unwrap()),
        Err(RequestError::DeliveryDisabled)));
    assert!(matches!(receive(client.try_deliver(group(0), limits(1, 35)).unwrap()),
        Err(RequestError::DeliveryDisabled)));
    assert_eq!(state(&client, 0), before);
    assert_eq!(acknowledged(&client, 0, token).outcome, AckOutcome::Applied);
    owner.join().unwrap();
}

#[test]
fn lease_commands_are_not_enabled_on_a_trusted_owner() {
    let (_directory, _root, owner, client) = fixture(&[b"cat"], 1, 102);
    assert!(matches!(receive(client.try_lease(group(0), limits(1, 35)).unwrap()),
        Err(RequestError::DeliveryDisabled)));
    owner.join().unwrap();
}

#[test]
fn lease_diagnostic_reads_do_not_mint_attempts() {
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat"], 1, clock);
    let raw = receive(client.try_fetch(offset(0), limits(1, 35)).unwrap()).unwrap();
    assert_eq!(raw.records.len(), 1);
    assert_eq!(state(&client, 0).window.occupied, 0);
    let first = leased(&client, 0, 1, 35);
    assert_eq!(first.records[0].token.generation, 1);
    owner.join().unwrap();
}

struct ScriptClock {
    samples: Mutex<VecDeque<Result<u64, ClockError>>>,
}
impl ScriptClock {
    fn new(samples: impl IntoIterator<Item = Result<u64, ClockError>>) -> Self {
        Self { samples: Mutex::new(samples.into_iter().collect()) }
    }
}
impl Clock for ScriptClock {
    fn now(&self) -> Result<u64, ClockError> {
        self.samples.lock().unwrap().pop_front().expect("scripted clock observation")
    }
}

#[test]
fn lease_deadline_begins_after_record_copying() {
    let clock = Arc::new(ScriptClock::new([Ok(1), Ok(9)]));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat"], 1, clock);
    let first = leased(&client, 0, 1, 35);
    assert_eq!(first.records[0].token.deadline_ms, 19);
    owner.join().unwrap();
}

#[test]
fn lease_clock_regression_refuses_without_installing_or_rewinding() {
    // First read succeeds but its installation-time sample regresses. The next request recovers.
    let clock = Arc::new(ScriptClock::new([Ok(5), Ok(4), Ok(6), Ok(6), Ok(6)]));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat"], 1, clock);
    assert!(matches!(receive(client.try_lease(group(0), limits(1, 35)).unwrap()),
        Err(RequestError::Lease(LeaseError::Clock(ClockError::Regressed)))));
    assert_eq!(state(&client, 0).window.occupied, 0);
    let token = leased(&client, 0, 1, 35).records[0].token;
    assert_eq!((token.generation, token.deadline_ms), (1, 16));
    assert_eq!(acknowledged(&client, 0, token).outcome, AckOutcome::Applied);
    owner.join().unwrap();
}

#[test]
fn lease_deadline_overflow_keeps_owner_usable_for_diagnostics() {
    let clock = Arc::new(ManualClock::new(u64::MAX - 5));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat"], 1, clock);
    assert!(matches!(receive(client.try_lease(group(0), limits(1, 35)).unwrap()),
        Err(RequestError::Lease(LeaseError::DeadlineExhausted))));
    assert_eq!(state(&client, 0).window.occupied, 0);
    assert_eq!(receive(client.try_snapshot().unwrap()).unwrap().visible_end, offset(1));
    owner.join().unwrap();
}

#[test]
fn lease_second_record_corruption_does_not_install_a_successful_prefix() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, root, owner, client) = leased_fixture(&[b"cat", b"dog"], 2, clock);
    // Both 35-byte records fit after the 32-byte segment header: corrupt payload byte 99.
    let path = root.join("partition-0").join(segment_name(offset(0)));
    let mut file = fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(99)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(99)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(matches!(receive(client.try_lease(group(0), limits(2, 70)).unwrap()),
        Err(RequestError::Failed(_))));
    assert!(owner.join().is_err());
    // This owner is terminal; no partial LeaseBatch or token was returned.
}

#[test]
fn lease_restart_fences_even_a_token_with_matching_offset_generation_and_deadline() {
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, root, owner, client) = leased_fixture(&[b"cat"], 1, clock);
    let old = leased(&client, 0, 1, 35).records[0].token;
    owner.join().unwrap();
    drop(client);
    let (epoch, mut logs) = recovery::recover(&root).unwrap().into_parts();
    let owner = WorkerHandle::start_with_leases(logs.remove(0), FileSync, batch(1), 8, 8,
        LeaseConfig::new(epoch, 1, 10).unwrap(), Arc::new(ManualClock::new(0))).unwrap();
    drop(logs);
    let client = owner.client();
    let new = leased(&client, 0, 1, 35).records[0].token;
    assert_eq!((new.offset, new.generation, new.deadline_ms), (old.offset, old.generation, old.deadline_ms));
    assert_ne!(new.epoch, old.epoch);
    let before = state(&client, 0);
    assert_eq!(acknowledged(&client, 0, old).outcome, AckOutcome::Rejected(AckRejection::WrongEpoch));
    assert_eq!(state(&client, 0), before);
    assert_eq!(acknowledged(&client, 0, new).retired, 1);
    owner.join().unwrap();
}

#[test]
fn lease_empty_payloads_still_charge_the_record_header() {
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, _root, owner, client) = leased_fixture(&[b"", b""], 2, clock.clone());
    assert!(leased(&client, 0, 2, 31).records.is_empty());
    let first = leased(&client, 0, 2, 32);
    assert_eq!(first.encoded_bytes, 32);
    clock.advance(10).unwrap();
    let retry = leased(&client, 0, 2, 32);
    assert_eq!(retry.encoded_bytes, 32);
    assert_eq!(retry.records[0].token.generation, 2);
    owner.join().unwrap();
}

#[test]
fn lease_retry_new_fairness_is_preserved_across_owner_requests() {
    let clock = Arc::new(ManualClock::new(0));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat", b"dog", b"owl", b"fox"], 4, clock.clone());
    assert_eq!(lease_offsets(&leased(&client, 0, 2, 70)), vec![0, 1]);
    let mut seen = Vec::new();
    for _ in 0..4 {
        clock.advance(10).unwrap();
        let next = leased(&client, 0, 1, 35);
        seen.push(next.records[0].token.offset.get());
    }
    assert_eq!(seen, vec![0, 2, 1, 3]);
    assert_eq!(state(&client, 0).window.occupied, 4);
    owner.join().unwrap();
}

#[test]
fn lease_acknowledgement_uses_processing_time_not_enqueue_time() {
    // Two observations issue the token; the next observation processes its acknowledgement.
    let clock = Arc::new(ScriptClock::new([Ok(0), Ok(0), Ok(10)]));
    let (_directory, _root, owner, client) = leased_fixture(&[b"cat"], 1, clock);
    let token = leased(&client, 0, 1, 35).records[0].token;
    assert_eq!(acknowledged(&client, 0, token).outcome, AckOutcome::Rejected(AckRejection::Expired));
    assert_eq!(state(&client, 0).window.completed_end, offset(0));
    owner.join().unwrap();
}
