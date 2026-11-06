# Chapter 11 — Bounded delivery and out-of-order completion
## Part 3 — Completion orders, bounded reads, and restart checks

This part completes [Part 1](https://25d-228.github.io/rust-learning/chapter-11.html) and [Part 2](https://25d-228.github.io/rust-learning/chapter-11-part-02.html). The chapter's **5.0-hour implementation estimate** includes these tests and the demonstration. The examples below describe expected behavior; execute the commands on the native target to observe actual results.

The new target supplies **28 tests**. Together with the prerequisite chapters, the workspace contains **216 required supplied tests**. That is a source count, not a statement that a particular machine has run them successfully. Keep earlier examples and test targets unchanged.

## Use two kinds of checks for two kinds of mistakes

A pure state-machine test needs no files, clocks, threads, or sleeps. It can explore many completion orders and inspect every active slot. An owner integration test checks a different boundary: startup identity, synchronization visibility, request ordering, read limits, ownership transfer, and restart. A ring reference cannot establish those properties by itself.

Create `crates/broker-core/tests/delivery.rs`. Keep Chapter 7's complete `tests/common/mod.rs`; its `TestDir` owns a fresh temporary directory and removes that directory when dropped. The helpers below unwrap expected fixture success so a test stops at the failed boundary. Production client code must instead distinguish admission failure, owner refusal, storage failure, and disconnection.

The five-second receiver timeout is a test watchdog, not a broker deadline and not a proof of speed. There are no sleeps used as evidence that a command ran.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

`fixture` creates an absent child directory through the real initialization protocol, consumes the returned session, and moves partition 0 into a delivery-enabled owner. The other three logs are dropped, but their directories remain as part of the initialized broker. Partition 0's log retains its own share of the directory guard.

Each fixture publish uses a one-record synchronization group and waits for its success reply. Consequently delivery tests begin with known visible records, not merely an assumed ordering between mailbox lanes. Returning the `TestDir` first lets tests bind it before the owner, so ordinary scope cleanup joins the later owner before removing the earlier directory.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Compare meaning, not another copy of the ring formula

The reference uses `BTreeSet<u64>` to hold completed offsets above the current prefix. It never computes a wrapped position. Completion inserts an offset into the set; repeatedly removing the current prefix end advances that reference end. Rust's [`BTreeSet` documentation](https://doc.rust-lang.org/std/collections/struct.BTreeSet.html) describes its unique-element set operations. This set is a small test oracle, not the broker's bounded representation.

`same_window` compares the two ends, occupied count, every active state, and both adjacent out-of-range observations. Comparing only the final completed end could miss clearing an outstanding slot during an intermediate wrap.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Construction and refusals must preserve the state

These first tests cover the width policy, exact capacity, and three distinct admission refusals. The failed requests must leave both counters and every active state unchanged. In particular, a zero-count admission with a regressed visible end is still inconsistent; it is not silently accepted merely because no new slots were requested.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Make the gap do observable work

The required 2, 0, 3, 1 trace checks each intermediate end and each retirement count. The stalled-head case then completes every follower while leaving zero unfinished at the front. It must remain full despite three successful completions. Only completing the head releases four slots.

This is the direct counterexample to freeing capacity on every completion. Such an implementation could keep issuing beyond a permanently stalled head and would no longer have a fixed-width representation of the unfinished prefix.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

A repeated completion while its slot remains in the window is different from one below the retired prefix. Both are no-ops. The nonzero-start test also demonstrates the limitation of `AlreadyRetired`: offset 17 is below an assumed start of 100, but this newly constructed object has no history proving an earlier delivery of 17.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Exercise the smallest ring and the largest offset

Width one exposes off-by-one assumptions that a larger ring can conceal. The maximum-offset case exposes arithmetic that a normal zero-based fixture never reaches. The last representable exclusive end is allowed; attempting to issue another record must return `OffsetExhausted`, not reuse zero.

These are logical counter tests. They do not allocate a log with an astronomically large number of records.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

The next pair checks the upper-bound calculation and the physical wrap trace from Part 1. Offset 4 reuses slot 0 while offsets 1, 2, and 3 remain elsewhere. Retiring 1 through 3 must leave 4 outstanding. Then another fill crosses the boundary again.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Enumerate every four-record completion order

There are `4 * 3 * 2 * 1 = 24` permutations. The four small loops enumerate 256 candidate tuples and retain only those with four distinct values. This intentionally simple enumeration is independent of a permutation library and small enough that its wasted candidates are irrelevant here.

After every completion, not just at the end of an order, compare against the set reference. A wrong implementation that eventually reaches four but temporarily skips an outstanding offset must fail.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Mix refills and completions across many wraps

The larger reference test runs 1,200 transitions for each of six widths: 1, 2, 3, 4, 7, and 17. It alternates bounded admissions with selected active completions, sometimes completing the head to guarantee progress. The selector uses explicitly wrapping integer arithmetic to generate a reproducible test sequence; wrapping is **not** used for broker offsets.

The reference's issued and completed counters are updated by their logical definitions, with no call to `Window::state()` to decide what those counters ought to be. The window state is observed only for comparison. After each transition, completing the exclusive issued end must fail without altering the snapshot. The final progress assertion ensures the test really crossed multiple rings instead of remaining stuck at its first gap.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

This is differential testing over a fixed deterministic workload, not a proof over all executions. The exhaustive four-position case complements it by covering all orders in one small state space. Neither check models external consumer side effects.

## Connect independent windows to one shared log

The first owner test treats an empty visible suffix as successful zero work. The second reads the same stored bytes into two independent groups and completes only group 0. Group 1 must remain at its own start with its own occupied count. The small 102-byte segment cap deliberately makes those four 35-byte records cross two segment files.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

The next checks distinguish window capacity from a request's record cap. A full window produces a no-read response: zero returned bytes and zero scanned records. A two-record request against a width-four window leaves the other two slots available instead of reserving them in advance.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Charge encoded bytes, including empty-payload headers

Three `cat` records need 105 encoded bytes. A 70-byte request returns exactly two and stops before the third. An empty payload still has a 32-byte record header, so a 31-byte request cannot consume even that record. The following 32-byte request consumes only the empty record, not the `cat` after it.

These tests check that a nonfitting record remains at the next issued offset. Merely checking the length of the returned vector would miss advancing over an unread record.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Observe writing separately from synchronized visibility

For this test only, use an append batch of four records with a 60-second maximum delay. Submit one record, then obtain snapshots until the owner has actually processed it. The mailbox's alternating lane service ensures a queued data command progresses between repeated control requests. The snapshot, not a sleep, establishes that the record was written.

Before flushing, group delivery must return no records and issue no positions. After a flush and the producer's success reply, it can return offset zero. This fixture checks the delivery path's choice of visible rather than written end. It is not a timing benchmark: a machine paused long enough to reach the 60-second batch deadline can invalidate the intended pre-flush state. Chapter 9's synchronization-gate tests separately cover responses withheld during an actual blocked synchronization.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Abandoning the reply still occupies the window

Submit a delivery and immediately drop its ticket. Then submit a group snapshot from the same thread. Both commands enter the same FIFO control lane, so the later snapshot observes the completed delivery operation regardless of whether the receiver disappeared before or after its reply was sent. Two slots must remain occupied.

Do not complete those offsets artificially just to make the test finish. Joining an owner is allowed with outstanding consumer work; it ends the volatile state. This test is the running-session liveness gap that the next chapter's retry mechanism must close.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

Invalid and repeated completions must not stop otherwise healthy storage. The following test tries the never-issued end, a duplicate above a gap, and an already-retired position, then publishes another record. A refusal that accidentally escapes as a fatal owner error fails that last step.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Separate observation, completion, and persistence

A raw diagnostic fetch must leave both groups untouched. Conversely, completion must change group memory without changing the segment or authoritative startup metadata bytes. The file comparisons below are ordinary fixture comparisons, not a publication checksum workflow. They read the known test files and do not modify them while the owner is alive.

The metadata comparison also catches an accidental epoch rewrite on every completion. Epoch publication belongs to startup, not to this control command.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

The restart test goes further: it completes the entire two-record group prefix, gracefully joins the owner, and recovers through Chapter 10. Recovery advances the epoch to two, but the new group's window begins at zero and returns both records again. Graceful shutdown alone must not be mistaken for the consumer checkpoint that has not yet been implemented.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Preserve compatibility without inventing a session identity

Earlier tests call the storage-only `WorkerHandle::start`. All three delivery commands on that path must return `DeliveryDisabled`, while an ordinary publish remains usable. This is why the worker uses an explicit `Option<Groups>` instead of attaching default windows with a fabricated epoch.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

The four-owner test consumes every log returned by the initialized session. All four use the same published epoch, but partition and group labels remain distinct. Each partition receives one identifying payload, so accidentally attaching a group to another partition cannot pass by coincidence.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

The last test combines three-slot windows, two-record segments, reverse-order completion of each batch, and repeated refills. It expects offsets 0 through 7 exactly once within this live session. It also confirms that ordinary completion did not delete or duplicate the four existing segment files. This is not a retention test; no consumer progress has been persisted.

```rust title="crates/broker-core/tests/delivery.rs"
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
```

## Run the focused target before the cumulative suite

From the learner workspace root, after entering all seven complete files:

```sh
cargo test -p broker-core --test delivery
cargo test --workspace --all-targets
```

The focused target has 28 tests. A successful run should report 28 passed and zero failed; elapsed times and test order can differ. The cumulative command also compiles ordinary examples as test targets, but it does not execute their `main` demonstrations. Run the example below separately.

Do not create another Cargo project under `book/media/chapter-11/`. The downloadable reference is an overlay on the prerequisite workspace. A separate scratch reconstruction can apply `chapter-01` through `chapter-11` overlays in numeric order; the later copies intentionally replace only the files changed by that chapter. Preserve your own learner branch and do not overwrite unrelated work just to run a reference.

## A complete two-group demonstration with a real restart

Create `crates/broker-core/examples/delivery_probe.rs`. The `request` helper distinguishes command admission from its ticket result by resolving them in order. `E: Debug` lets it render either ordinary control admission errors or rejected publish payload wrappers. The two `?` operators after `recv_timeout` propagate the outer receive failure and then the inner `RequestError`; they are separate error layers, not special syntax for retries.

The executable requires an absent broker directory. It initializes all four partitions through the normal protocol and starts four owners with the published epoch. The visible demonstration uses partition 0; it does not create a second log for the other group.

```rust title="crates/broker-core/examples/delivery_probe.rs"
use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::time::Duration;

use broker_core::delivery::{DeliveryConfig, DeliveryStop};
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::{GroupId, Offset};
use broker_core::worker::{BatchConfig, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}

```

Startup is deliberately explicit. Six three-byte records are published one at a time, with each success received before proceeding. With a 102-byte segment cap, two such records and their segment header fit exactly, so the six records occupy three segment files.

```rust title="crates/broker-core/examples/delivery_probe.rs"
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 2 {
        return Err(io::Error::other("usage: delivery_probe ABSENT_DIRECTORY").into());
    }
    let root = Path::new(&args[1]);
    let (epoch, logs) = recovery::initialize(root, StorageConfig::new(102, 2, 16)?)?.into_parts();
    let config = BatchConfig::new(1, 1000, Duration::from_secs(1))?;
    let mut owners = Vec::new();
    for log in logs {
        owners.push(WorkerHandle::start_with_delivery(log, FileSync, config, 8, 8,
            DeliveryConfig::new(epoch, 4)?)?);
    }
    println!("epoch={} partitions={} groups=2", epoch.get(), owners.len());
    let client = owners[0].client();
    for (index, payload) in [b"cat", b"dog", b"owl", b"fox", b"eel", b"ant"].iter().enumerate() {
        assert_eq!(request(client.try_publish(payload.to_vec()))?.get(), index as u64);
    }
```

Group 0 receives four records because its window has width four. Group 1 has the same width but receives only two because its request is limited to 70 encoded bytes. The record copies are dropped before completion commands to make the separation of payload ownership and slot state concrete.

```rust title="crates/broker-core/examples/delivery_probe.rs"
    let first = request(client.try_deliver(GroupId::new(0)?, ReadLimits::new(4, 1000)?))?;
    let values: Vec<_> = first.records.iter().map(|record| record.offset().get()).collect();
    assert_eq!(values, vec![0, 1, 2, 3]);
    assert_eq!(first.stop, DeliveryStop::WindowFull);
    println!("group=0 offsets={values:?} stop={:?}", first.stop);
    let second = request(client.try_deliver(GroupId::new(1)?, ReadLimits::new(4, 70)?))?;
    let values: Vec<_> = second.records.iter().map(|record| record.offset().get()).collect();
    assert_eq!(values, vec![0, 1]);
    assert_eq!(second.stop, DeliveryStop::ByteLimit { required: 35 });
    println!("group=1 offsets={values:?} stop={:?}", second.stop);
    // Neither reply owns a window. Dropping record copies is independent of completion.
    drop(first);
    drop(second);
```

Complete group 0 in the required out-of-order sequence and print each retirement result. Its next delivery returns offsets 4 and 5. Group 1 remains stalled at its earlier prefix. All four owner handles are then explicitly joined; no thread holding storage is detached before recovery begins.

```rust title="crates/broker-core/examples/delivery_probe.rs"
    for (value, expected_end) in [(2, 0), (0, 1), (3, 1), (1, 4)] {
        let result = request(client.try_complete(GroupId::new(0)?, Offset::new(value)))?;
        assert_eq!(result.state.window.completed_end.get(), expected_end);
        println!("complete={value} retired={} completed_end={} occupied={}",
            result.completion.retired, result.state.window.completed_end.get(),
            result.state.window.occupied);
    }
    let remaining = request(client.try_deliver(GroupId::new(0)?, ReadLimits::new(4, 1000)?))?;
    let values: Vec<_> = remaining.records.iter().map(|record| record.offset().get()).collect();
    assert_eq!(values, vec![4, 5]);
    println!("group=0 offsets={values:?} stop={:?}", remaining.stop);
    let stalled = request(client.try_group_state(GroupId::new(1)?))?;
    assert_eq!((stalled.window.completed_end.get(), stalled.window.issued_end.get()), (0, 2));
    println!("group=1 completed_end={} issued_end={} occupied={}",
        stalled.window.completed_end.get(), stalled.window.issued_end.get(), stalled.window.occupied);
    drop(remaining);
    for owner in owners { owner.join()?; }
    drop(client);

```

Recovery publishes a new session epoch. Only partition 0 is needed for the replay observation, so the other recovered log owners are dropped. The new group window starts at zero: records completed before shutdown are returned again. This demonstrates volatile progress honestly rather than giving it an accidental persistence guarantee.

```rust title="crates/broker-core/examples/delivery_probe.rs"
    // No consumer checkpoint exists yet: even a graceful restart repeats completed work.
    let (new_epoch, mut logs) = recovery::recover(root)?.into_parts();
    assert_eq!(new_epoch.get(), epoch.get() + 1);
    let owner = WorkerHandle::start_with_delivery(logs.remove(0), FileSync, config, 8, 8,
        DeliveryConfig::new(new_epoch, 4)?)?;
    drop(logs);
    let repeated = request(owner.client().try_deliver(GroupId::new(0)?, ReadLimits::new(2, 70)?))?;
    let values: Vec<_> = repeated.records.iter().map(|record| record.offset().get()).collect();
    assert_eq!(values, vec![0, 1]);
    assert_eq!(repeated.state.window.completed_end.get(), 0);
    println!("recovered epoch={} group=0 offsets={values:?} completed_end={}",
        repeated.state.epoch.get(), repeated.state.window.completed_end.get());
    owner.join()?;
    Ok(())
}
```

Use a newly created temporary parent so the broker leaf is absent. The subshell keeps the temporary path and cleanup local to this demonstration:

```sh
(
    scratch="$(mktemp -d)" || exit 1
    trap 'rm -rf "$scratch"' EXIT
    cargo run -p broker-core --example delivery_probe -- "$scratch/broker"
)
```

Expected program output, excluding Cargo's build messages:

```text
epoch=1 partitions=4 groups=2
group=0 offsets=[0, 1, 2, 3] stop=WindowFull
group=1 offsets=[0, 1] stop=ByteLimit { required: 35 }
complete=2 retired=0 completed_end=0 occupied=4
complete=0 retired=1 completed_end=1 occupied=3
complete=3 retired=0 completed_end=1 occupied=3
complete=1 retired=3 completed_end=4 occupied=0
group=0 offsets=[4, 5] stop=VisibleEnd
group=1 completed_end=0 issued_end=2 occupied=2
recovered epoch=2 group=0 offsets=[0, 1] completed_end=0
```

The numeric trace follows the state-machine rules; it is not a measured throughput or durability result. A normal restart is enough to show that completion was volatile. Process interruption and filesystem synchronization failure remain covered by the prerequisite chapters' distinct fixtures.

## Diagnose the boundary that failed

| Symptom | Likely boundary to inspect | Concrete check |
| --- | --- | --- |
| Completing 2 advances the end to 3 | Maximum-seen offset mistaken for a prefix | Stop retirement at the first outstanding head |
| Completing followers frees slots while 0 is stalled | Completed slots were treated as empty | Count all positions in `[completed_end, issued_end)` |
| Offset 4 disappears after a wrap | Physical position confused with logical identity | Inspect the `head=1`, distance-3 mapping and interval check |
| A byte-limited fetch skips the next record | Issued end advanced to the planned upper bound | Admit `batch.records.len()`, not the requested count |
| Delivery returns a pending unsynchronized append | Written end passed to the reader | Pass the owner's `visible_end` |
| Group 1 advances when group 0 completes | Shared or misindexed window state | Verify `GroupId` indexing and two separately constructed windows |
| Dropped reply makes slots immediately reusable | Send failure treated as rollback | Keep issued positions until valid completion or a defined retry policy |
| Restart resumes at the old volatile completed end | Memory progress mistaken for checkpointed progress | Construct at zero until Chapter 13 loads a validated checkpoint |
| Old storage tests require an invented epoch | Compatibility mode was removed | Keep storage-only `start` and explicit `DeliveryDisabled` responses |

If a watchdog expires, first inspect the owner's error or panic and confirm all new control variants are handled by failure cleanup. Increasing the timeout cannot repair a forgotten reply. On a genuinely overloaded machine a longer diagnostic watchdog may help distinguish scheduling delay from a missing reply, but it must not change broker semantics.

## Two small predictions before changing the program

Change group 1's request from 70 to 69 bytes. Predict a single returned record, a 35-byte `required` value for the next one, and an issued end of one. The budget is for encoded records; two three-byte payloads are not a six-byte delivery.

Then use a width-three pure window: admit 0, 1, 2; complete 2; complete 0; admit 3; complete 1. Predict that the completed end becomes three, offset 3 remains outstanding, and two slots are free. Add an assertion for `status(3)` rather than checking only the end counter. These are useful local variations, not extra prerequisites or a new chapter.

## What this chapter establishes

The partition owner now issues only synchronized records into independent fixed-width group windows, copies within existing count/byte read limits, and advances completion only across a contiguous prefix. Duplicate and retired trusted completions do not double-count retirement. Rejected never-issued completions leave the owner usable. Physical slot reuse is independent of monotonically increasing offsets.

It does not yet authenticate acknowledgements, retry abandoned deliveries, persist consumer progress, authorize retention, or bound all client-held payload memory. The next chapter adds lease expiration, checked attempt generations, and complete stale-token validation while preserving this window's capacity and prefix invariants.
