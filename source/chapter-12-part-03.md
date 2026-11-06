# Chapter 12 — Lease expiration, retries, and stale-token fencing
## Part 3 — Deterministic expiry and restart checks

> **Learning objectives**
> Exercise both command orders at a deadline, force arithmetic limits, and compare ring state with an ordered-map reference. Check actual owner replies, byte-limited reads, abandoned receivers, and restart fencing without using sleeps to advance lease time.

This completes the same **4.5-hour chapter**. [Part 1](https://25d-228.github.io/rust-learning/chapter-12.html) defines attempt state and token validation; [Part 2](https://25d-228.github.io/rust-learning/chapter-12-part-02.html) integrates clocks, storage reads, and owner commands. Keep the earlier `tests/common/mod.rs`, `tests/delivery.rs`, recovery helpers, and workspace manifests.

The chapter adds **31 tests**: 14 private-state unit tests and 17 additions to the existing delivery integration target. The cumulative workspace contains **247 supplied tests** when all chapter overlays through Chapter 12 are applied. These counts describe supplied tests, not observed passes. Commands and output below are local checks to run; no Rust compilation, dependency resolution, test execution, formatting pass, or native macOS execution is claimed for this authoring environment.

## Make a boundary observable instead of guessing with sleep

A timer test that sleeps ten milliseconds does not establish whether the owner observed exactly 10, slightly before it, or much later. Here the manual clock is ordinary shared state, and an owner reply establishes which command has completed. We advance the clock between those observed operations.

`recv_timeout` in the existing helper is a hang watchdog. It does not advance the manual clock or establish the lease boundary. A missing reply is a test failure rather than an instruction to sleep longer. The scripted clock later supplies individual observations when the precise difference between selection and installation matters.

Pure window tests receive integer times directly. These methods are crate-private implementation details, not a public API allowing consumers to choose an acknowledgement time. Owner-level tests pass the same clock implementations used by the actual command path.

## Build a small inspection vocabulary

Create `crates/broker-core/src/delivery/lease/tests.rs`. This is the child module declared at the end of `lease.rs`, so `use super::*` can access its private attempt metadata. The helper named `unchanged` copies frontiers, the entire bounded attempt array, and both scheduler fields. Comparing only `completed_end` would miss an accidental generation increment or cursor movement.

The `issue` helper models a successful fully returned plan. Separate tests below call `plan` and `install` themselves when only a prefix is returned or installation must fail.

```rust title="crates/broker-core/src/delivery/lease/tests.rs"
use super::*;
use std::collections::BTreeMap;

fn off(value: u64) -> Offset { Offset::new(value) }
fn fixture(width: usize, start: u64) -> LeaseWindow {
    LeaseWindow::new(PartitionId::new(0).unwrap(), GroupId::new(0).unwrap(), off(start),
        LeaseConfig::new(Epoch::new(7).unwrap(), width, 10).unwrap())
}
fn issue(window: &mut LeaseWindow, visible: u64, count: usize, now: u64) -> Vec<LeaseToken> {
    let plan = window.plan(off(visible), count, now).unwrap();
    window.install(&plan, off(visible), now).unwrap()
}
fn unchanged(window: &LeaseWindow) -> (GroupState, Vec<Option<Attempt>>, Offset, bool) {
    (window.state(), window.attempts.to_vec(), window.retry_cursor, window.prefer_retry)
}

```

The first tests cover construction, generation-1 token contents, unchanged occupancy on retry, and the strict inequality just before a deadline. The token assertion checks every scope field rather than relying only on a generation number.

```rust title="crates/broker-core/src/delivery/lease/tests.rs"
#[test]
fn configuration_and_plan_bounds_are_checked() {
    let epoch = Epoch::new(7).unwrap();
    assert!(LeaseConfig::new(epoch, 0, 10).is_err());
    assert!(LeaseConfig::new(epoch, 4097, 10).is_err());
    assert!(matches!(LeaseConfig::new(epoch, 1, 0), Err(LeaseError::LeaseMillis(0))));
    assert!(LeaseConfig::new(epoch, 1, 60_001).is_err());
    assert!(LeaseConfig::new(epoch, 4096, 60_000).is_ok());
    let window = fixture(2, 0);
    for count in [0, 1025] {
        assert!(matches!(window.plan(off(2), count, 0), Err(LeaseError::RecordLimit(_))));
    }
}

#[test]
fn issue_then_retry_changes_attempt_not_occupancy() {
    let mut window = fixture(2, 0);
    let first = issue(&mut window, 2, 2, 100);
    assert_eq!(first[0], LeaseToken { epoch: Epoch::new(7).unwrap(),
        partition: PartitionId::new(0).unwrap(), group: GroupId::new(0).unwrap(),
        offset: off(0), generation: 1, deadline_ms: 110 });
    let before = window.state();
    assert!(issue(&mut window, 2, 2, 109).is_empty());
    let retried = issue(&mut window, 2, 2, 110);
    assert_eq!(window.state(), before);
    assert_eq!(retried.iter().map(|token| token.generation).collect::<Vec<_>>(), vec![2, 2]);
    assert_eq!(retried[0].deadline_ms, 120);
    assert_eq!(window.acknowledge(first[0], 110).outcome,
        AckOutcome::Rejected(AckRejection::StaleGeneration));
}

#[test]
fn just_before_deadline_can_complete_and_prevent_retry() {
    let mut window = fixture(1, 0);
    let token = issue(&mut window, 1, 1, 0)[0];
    assert_eq!(window.acknowledge(token, 9).outcome, AckOutcome::Applied);
    assert!(issue(&mut window, 1, 1, 10).is_empty());
    assert_eq!(window.state().window.completed_end, off(1));
}

```

## Test equality in both orders

At exactly 10, acknowledgement-first must return `Expired`. Reissue-first must make the original token `StaleGeneration`. Neither order may complete the record. Both should leave the same unfinished offset and eventually permit a new attempt with generation 2.

The next test changes each identity field independently. It includes offset and deadline as well as the more obvious epoch and generation. A token that happens to refer to a valid active ring position is not sufficient authority.

```rust title="crates/broker-core/src/delivery/lease/tests.rs"
#[test]
fn exact_deadline_is_fenced_in_both_command_orders() {
    for retry_first in [false, true] {
        let mut window = fixture(1, 0);
        let old = issue(&mut window, 1, 1, 0)[0];
        if retry_first {
            assert_eq!(issue(&mut window, 1, 1, 10)[0].generation, 2);
        }
        let before = unchanged(&window);
        let expected = if retry_first { AckRejection::StaleGeneration } else { AckRejection::Expired };
        assert_eq!(window.acknowledge(old, 10).outcome, AckOutcome::Rejected(expected));
        assert_eq!(unchanged(&window), before);
        if !retry_first { assert_eq!(issue(&mut window, 1, 1, 10)[0].generation, 2); }
        assert_eq!(window.state().window.completed_end, off(0));
    }
}

#[test]
fn each_identity_field_is_checked_without_mutating_delivery_state() {
    let mut window = fixture(2, 0);
    let token = issue(&mut window, 1, 1, 0)[0];
    let variants = [
        (LeaseToken { epoch: Epoch::new(8).unwrap(), ..token }, AckRejection::WrongEpoch),
        (LeaseToken { partition: PartitionId::new(1).unwrap(), ..token }, AckRejection::WrongPartition),
        (LeaseToken { group: GroupId::new(1).unwrap(), ..token }, AckRejection::WrongGroup),
        (LeaseToken { offset: off(1), ..token }, AckRejection::NotIssued),
        (LeaseToken { generation: 0, ..token }, AckRejection::StaleGeneration),
        (LeaseToken { deadline_ms: 11, ..token }, AckRejection::WrongDeadline),
    ];
    for (wrong, expected) in variants {
        let before = unchanged(&window);
        assert_eq!(window.acknowledge(wrong, 1).outcome, AckOutcome::Rejected(expected));
        assert_eq!(unchanged(&window), before);
    }
}

```

A completed follower retains its accepted attempt metadata while a head is missing. The following test allows its exact duplicate classification even at time 10, retries only the unfinished head, and then retires both positions together. Once retired, the old follower is classified without retaining its detailed attempt history. A wrong epoch is still rejected first.

```rust title="crates/broker-core/src/delivery/lease/tests.rs"
#[test]
fn duplicate_retired_and_wrong_epoch_are_distinct() {
    let mut window = fixture(2, 0);
    let tokens = issue(&mut window, 2, 2, 0);
    assert_eq!(window.acknowledge(tokens[1], 1).retired, 0);
    let before = unchanged(&window);
    assert_eq!(window.acknowledge(tokens[1], 10).outcome, AckOutcome::Duplicate);
    assert_eq!(unchanged(&window), before);
    let retry = issue(&mut window, 2, 2, 10);
    assert_eq!(retry.len(), 1); // Completed followers are not retried, even after their deadline.
    assert_eq!(window.acknowledge(retry[0], 11).retired, 2);
    let retired = window.acknowledge(tokens[1], 11);
    assert_eq!((retired.outcome, retired.retired), (AckOutcome::AlreadyRetired, 0));
    let wrong_epoch = LeaseToken { epoch: Epoch::new(8).unwrap(), ..tokens[1] };
    assert_eq!(window.acknowledge(wrong_epoch, 11).outcome,
        AckOutcome::Rejected(AckRejection::WrongEpoch));
}

```

## Test scheduling across calls, not only inside one batch

With one-record requests and enough time between them for earlier retries to expire again, a scan that always starts at the head would keep returning 0. The rotating-cursor test demands 0, 1, 2, 0 instead.

The second test leaves both retries and new offsets available. It requires the cross-request sequence 0, 2, 1, 3: retry, new, retry, new. Merely alternating inside each batch would fail this test because every one-record batch would restart at the same category.

```rust title="crates/broker-core/src/delivery/lease/tests.rs"
#[test]
fn retry_cursor_rotates_even_when_the_head_expires_again() {
    let mut window = fixture(3, 0);
    issue(&mut window, 3, 3, 0);
    let mut seen = Vec::new();
    for now in [10, 20, 30, 40] { seen.push(issue(&mut window, 3, 1, now)[0].offset.get()); }
    assert_eq!(seen, vec![0, 1, 2, 0]);
    assert_eq!(window.state().window.occupied, 3);
}

#[test]
fn retry_and_new_service_alternate_across_single_record_polls() {
    let mut window = fixture(4, 0);
    issue(&mut window, 4, 2, 0);
    let mut seen = Vec::new();
    for now in [10, 20, 30, 40] {
        let token = issue(&mut window, 4, 1, now)[0];
        seen.push((token.offset.get(), token.generation));
    }
    assert_eq!(seen, vec![(0, 2), (2, 1), (1, 2), (3, 1)]);
    assert_eq!(window.state().window.occupied, 4);
}

```

Planning is deliberately observable without mutation. For `[retry 0, new 2, retry 1, new 3]`, install only the first selection. Offset 2 must not become issued, offset 1 must keep generation 1, and the next successful selection must honor the preference established by the one actual retry.

```rust title="crates/broker-core/src/delivery/lease/tests.rs"
#[test]
fn planning_and_an_unreturned_suffix_do_not_advance_state() {
    let mut window = fixture(4, 0);
    issue(&mut window, 4, 2, 0);
    let before = unchanged(&window);
    let plan = window.plan(off(4), 4, 10).unwrap();
    assert_eq!(plan.iter().map(|selected| selected.offset.get()).collect::<Vec<_>>(), vec![0, 2, 1, 3]);
    assert_eq!(unchanged(&window), before);
    window.install(&plan[..1], off(4), 10).unwrap();
    assert_eq!(window.state().window.issued_end, off(2));
    assert_eq!(window.attempt(off(1)).generation, 1);
    let next = issue(&mut window, 4, 1, 10);
    assert_eq!((next[0].offset.get(), next[0].generation), (2, 1));
}

```

## Force numeric boundaries rather than waiting for them

The arithmetic tests do not attempt billions of retries. The child module can place a private attempt at `u64::MAX` to exercise the successor path. That is a controlled fixture, not a production mutation method. It also checks that an earlier valid candidate in the same plan is not partially installed when a later generation overflows.

Deadline overflow is different: the plan is valid, but installation cannot create the new deadline. Both paths must leave the complete inspected state unchanged.

```rust title="crates/broker-core/src/delivery/lease/tests.rs"
#[test]
fn deadline_overflow_does_not_partially_install() {
    let mut window = fixture(2, 0);
    let plan = window.plan(off(2), 2, u64::MAX - 5).unwrap();
    let before = unchanged(&window);
    assert_eq!(window.install(&plan, off(2), u64::MAX - 5), Err(LeaseError::DeadlineExhausted));
    assert_eq!(unchanged(&window), before);
}

#[test]
fn generation_exhaustion_does_not_wrap_or_install_other_candidates() {
    let mut window = fixture(2, 0);
    issue(&mut window, 2, 2, 0);
    let position = window.position(off(1));
    window.attempts[position].as_mut().unwrap().generation = u64::MAX;
    let before = unchanged(&window);
    assert!(matches!(window.plan(off(2), 2, 10), Err(LeaseError::GenerationExhausted(value)) if value == off(1)));
    assert_eq!(unchanged(&window), before);
}

```

Two final addressing checks cover the largest representable exclusive end and repeated modulo-position reuse. A new record can begin again at generation 1 because its offset is different. An old token cannot become current merely because it shares the same physical metadata slot with that newer record.

```rust title="crates/broker-core/src/delivery/lease/tests.rs"
#[test]
fn maximum_offsets_keep_distinct_slots_and_retire_without_wrapping() {
    let mut window = fixture(2, u64::MAX - 2);
    let first = issue(&mut window, u64::MAX, 2, 0);
    assert_ne!(window.position(first[0].offset), window.position(first[1].offset));
    let retry = issue(&mut window, u64::MAX, 2, 10);
    window.acknowledge(retry[1], 11);
    assert_eq!(window.acknowledge(retry[0], 11).retired, 2);
    assert_eq!(window.state().window.completed_end, off(u64::MAX));
    assert!(issue(&mut window, u64::MAX, 2, 11).is_empty());
}

#[test]
fn repeated_slot_reuse_does_not_make_a_retired_token_current() {
    let mut window = fixture(2, 0);
    let old = issue(&mut window, 40, 1, 0)[0];
    window.acknowledge(old, 0);
    for value in 1..40 {
        let token = issue(&mut window, 40, 1, value)[0];
        assert_eq!(token.offset.get(), value);
        assert_eq!(token.generation, 1);
        let before = unchanged(&window);
        assert_eq!(window.acknowledge(old, value).outcome, AckOutcome::AlreadyRetired);
        assert_eq!(unchanged(&window), before);
        assert_eq!(window.acknowledge(token, value).outcome, AckOutcome::Applied);
    }
}

```

## Compare the fixed ring with an ordered map

An independent `BTreeMap` stores active logical offsets with generation, deadline, and completion state. It has no ring positions and clears a completed prefix by logical key. Across four widths and 200 transitions per width, compare its state with the ring after issuing, retrying, applying completions, and submitting tokens retained from earlier attempts.

The model does not replace the implementation's scheduler: the scheduling-specific tests above supply that contract. Here it checks that whichever valid selections were installed preserve logical identity, generation succession, contiguous retirement, and bounded occupancy through repeated wraps. This distinction prevents a shared scheduler assumption from masquerading as an independent scheduling oracle.

```rust title="crates/broker-core/src/delivery/lease/tests.rs"
#[test]
fn mixed_tokens_match_an_ordered_map_through_retries_and_wraps() {
    for width in [1, 2, 3, 7] {
        let mut window = fixture(width, 0);
        let mut map: BTreeMap<u64, (u64, u64, bool)> = BTreeMap::new();
        let mut history = Vec::new();
        let mut completed = 0;
        let mut issued = 0;
        for step in 0..200u64 {
            let now = step * 3;
            let tokens = issue(&mut window, 80, 3, now);
            for token in tokens {
                let key = token.offset.get();
                if let Some(&(generation, deadline, done)) = map.get(&key) {
                    assert!(!done && deadline <= now);
                    assert_eq!(token.generation, generation + 1);
                } else {
                    assert_eq!(key, issued);
                    assert_eq!(token.generation, 1);
                    issued += 1;
                }
                map.insert(key, (token.generation, token.deadline_ms, false));
                history.push(token);
            }
            if !history.is_empty() {
                let index = if step % 3 == 0 { 0 } else { history.len() - 1 };
                let token = history[index];
                let key = token.offset.get();
                let expected = if key < completed { AckOutcome::AlreadyRetired } else {
                    let entry = map.get_mut(&key).unwrap();
                    if entry.0 != token.generation { AckOutcome::Rejected(AckRejection::StaleGeneration) }
                    else if entry.2 { AckOutcome::Duplicate }
                    else if now >= entry.1 { AckOutcome::Rejected(AckRejection::Expired) }
                    else { entry.2 = true; AckOutcome::Applied }
                };
                while map.get(&completed).is_some_and(|entry| entry.2) {
                    map.remove(&completed);
                    completed += 1;
                }
                assert_eq!(window.acknowledge(token, now).outcome, expected);
            }
            let state = window.state().window;
            assert_eq!((state.completed_end.get(), state.issued_end.get()), (completed, issued));
            assert_eq!(state.occupied, map.len());
            for (&key, &(generation, deadline_ms, done)) in &map {
                assert_eq!(window.attempt(off(key)), Attempt { generation, deadline_ms });
                assert_eq!(window.window.status(off(key)),
                    Some(if done { Slot::Completed } else { Slot::Outstanding }));
            }
        }
    }
}
```

## Extend the existing owner integration target

Keep every Chapter 11 line of `crates/broker-core/tests/delivery.rs` unchanged. Append the following listings in order. They reuse the existing `offset`, `group`, `limits`, `receive`, `state`, `batch`, `fixture`, and `TestDir` definitions. The new `use` declarations are valid at module scope even though they appear after earlier test functions; do not paste them inside a function.

The fixture explicitly initializes an absent broker directory, takes the epoch from that session, starts partition 0 in leased mode, and publishes each input record with a one-record commit policy. The other partition handles are dropped, but the live partition log retains directory ownership. Every returned record is therefore within an already synchronized visible prefix.

The storage cap of 102 bytes is purposeful. A 32-byte segment header plus two three-byte payload records uses $32 + 2(32+3)=102$ bytes. The third record rotates into another segment. Lease reads consequently exercise existing segment lookup rather than an unrelated in-memory payload map.

```rust title="crates/broker-core/tests/delivery.rs"


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

```

The manual-clock check first proves checked advancement leaves the previous value intact at overflow and is visible through a shared handle. The next test crosses from 109 to exactly 110, verifies expiry before any reissue, then verifies stale-generation rejection after reissue. It also compares record payloads: a retry changes authority, not stored content.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

Dropping a delivery ticket must not rewind issuance. The following snapshot is on the same FIFO control lane, so receiving its result establishes that the earlier abandoned delivery command was processed. At time zero the slot is still occupied and not retryable; at time ten a fresh token can recover it. No sleep is used as evidence that the owner has run.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

For byte refusal, inspect both new issuance and retry generations. A 34-byte allowance cannot fit a 35-byte record. A 35-byte allowance can return one candidate but must not consume the next. This catches the tempting bug of updating attempts while constructing a batch that later stops at a byte boundary.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

Acknowledging followers must still preserve the missing head. When the head expires, only that head is redelivered; already completed followers do not accumulate fresh attempts. A valid acknowledgement of the retry can retire all three positions at once.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

The identity checks operate on the real group selector, not just the pure state machine. Both groups intentionally issue the same offset with the same generation and deadline. That collision makes it observable whether the group field is actually checked. The partition and deadline checks then alter just one field at a time, while snapshots confirm that the selected group is unchanged.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Prove there is no legacy path around fencing

A leased owner must reject tokenless completion and the old trusted delivery operation. A trusted owner must reject the new lease operation. A diagnostic fetch can still copy a record but cannot mint a lease or occupy a group slot. These are command-level tests: they would catch a correct token implementation accidentally bypassed by integration wiring.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Control individual clock observations

`ScriptClock` returns a finite sequence of successful samples or clock errors. Unlike `ManualClock`, it can deliberately violate the nondecreasing contract to test the owner's defensive check. It is confined to this test module. Running out of scripted samples is a test bug, not a fabricated time value.

The first test supplies time 1 for selection and time 9 after the record copy. Its token must expire at 19, not 11. This verifies the placement of the second sample without pretending to measure actual disk latency.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

A regression from 5 to 4 occurs after a successful read but before attempt installation. The owner must keep zero occupied slots. A later sample of 6 is acceptable because the watermark remained at 5; the next successful attempt still begins at generation 1. The overflow test separately shows that diagnostic snapshots remain available when lease deadlines cannot be represented.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Corruption is not an empty poll

The next fixture intentionally modifies a file behind a live owner's back. Ordinary participants must not do this; it is a fault test of the existing checksum boundary. The first payload begins at byte 64, and the second begins at $32 + 35 + 32 = 99$. Flipping byte 99 corrupts the second record while leaving the first available to be copied.

The request must return a terminal storage error, not a successful one-record prefix or an empty poll. The assertions observe the failed reply and failed join; they do not inspect a dead owner's private window. The no-installation-before-all-reads property is established by the read path in Part 2 together with the pure planning/installation tests, rather than inferred from a surviving snapshot that no longer exists.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

## Make the restart collision as strong as possible

Restart under a newly persisted epoch, but reset the test clock to zero. Arrange for the newly delivered record to have the **same offset, generation, and deadline** as an old token. Rejecting the old token then depends on the epoch rather than an accidentally different clock value.

The first owner is joined and its client dropped before recovery. Recovery, not an ad hoc in-process window reset, creates the new session. The test preserves the rule that one exposed epoch never reconstructs its attempt state from scratch.

```rust title="crates/broker-core/tests/delivery.rs"
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

```

The remaining integration checks retain the 32-byte charge for empty records, verify retry/new preference across owner requests, and make acknowledgement time come from the owner's observation. The last scripted sequence issues at zero and handles the acknowledgement at ten; no client timestamp can extend the token.

```rust title="crates/broker-core/tests/delivery.rs"
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
```

## Run the targeted checks before the whole workspace

From the learner workspace root, use a compatible installed Rust toolchain with the earlier crate's required APIs. This chapter adds no toolchain requirement or dependency. The inherited edition and API requirements still apply; there is no need to match a particular newer release merely for these snippets.

```sh
cargo test -p broker-core --lib delivery::lease::tests
cargo test -p broker-core --test delivery
cargo test --workspace --all-targets
```

The first command should discover 14 lease unit tests. The delivery integration target should discover 45 tests: the prior 28 plus these 17. The full workspace should discover 247 supplied test functions across its targets on the native Unix target. Cargo can print separate result summaries for separate binaries; do not mistake one such summary for the whole workspace total. These are expected counts, not execution results from this package.

Run `cargo fmt --all` when integrating the source, then inspect the changes. The compact textbook listings are not a claim that `cargo fmt --check` has passed. A compiler error should be fixed before interpreting a missing or filtered test as an algorithm failure.

The new lease tests advance time explicitly. Earlier chapters still have their own synchronization checks and watchdogs; this chapter does not retroactively turn every earlier test into a virtual-time test.

### Assemble a separate reference workspace when diagnosing integration

The public book is not the learner's implementation tree. To compare with the cumulative reference, run the following from the extracted course-package root. It creates a new temporary workspace and copies overlays in chapter order. It does not modify an existing learner workspace or place Cargo build products in `book/media`.

```sh
REFERENCE_WORKSPACE="$(mktemp -d "${TMPDIR:-/tmp}/rust-lease-reference.XXXXXX")"
export REFERENCE_WORKSPACE
python3 - <<'PYCODE'
import os
from pathlib import Path
import shutil

package = Path.cwd()
target = Path(os.environ["REFERENCE_WORKSPACE"])
if any(target.iterdir()):
    raise SystemExit("Reference target must be empty")
for number in range(1, 13):
    overlay = package / "book" / "media" / f"chapter-{number:02d}"
    if not overlay.is_dir():
        raise SystemExit(f"Missing prerequisite overlay: {overlay}")
    shutil.copytree(overlay, target, dirs_exist_ok=True)
print(target)
PYCODE
cd "$REFERENCE_WORKSPACE"
cargo test -p broker-core --lib delivery::lease::tests
cargo test -p broker-core --test delivery
```

The reference workspace can help isolate a missed file edit; it does not replace understanding the two-phase read/install sequence. Keep the temporary directory only as long as it is useful, and remove that exact generated directory when finished.

## Demonstrate consumer loss and a new startup epoch

Create `crates/broker-core/examples/lease_probe.rs`. The example starts four owners using one manually controlled clock, publishes four records to partition 0, and lets two groups progress independently. It completes a follower, moves time to the exact deadline, retries the missing records, rejects the old attempt, and retires the prefix. It then joins the owners and recovers a new epoch.

The helper receives actual owner replies with a watchdog. It does not insert sleeps to make the scenario likely. The `describe` function projects only the stable `(offset, generation, deadline)` values used in the worked trace; it is not a serialization format for the full token.

```rust title="crates/broker-core/examples/lease_probe.rs"
use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use broker_core::delivery::{AckOutcome, AckRejection, LeaseBatch, LeaseConfig};
use broker_core::delivery::clock::ManualClock;
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::GroupId;
use broker_core::worker::{BatchConfig, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}
fn describe(batch: &LeaseBatch) -> Vec<(u64, u64, u64)> {
    batch.records.iter().map(|item| (item.token.offset.get(),
        item.token.generation, item.token.deadline_ms)).collect()
}

```

Initialize an absent directory, use the session's epoch for every owner, and publish with a one-record commit trigger. Group 0 fills its three-slot window. Group 1 receives only offset 0. Completing group 0's offset 2 leaves the head missing and retires nothing.

```rust title="crates/broker-core/examples/lease_probe.rs"
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 2 {
        return Err(io::Error::other("usage: lease_probe ABSENT_DIRECTORY").into());
    }
    let root = Path::new(&args[1]);
    let (epoch, logs) = recovery::initialize(root, StorageConfig::new(102, 2, 16)?)?.into_parts();
    let batch = BatchConfig::new(1, 1000, Duration::from_secs(1))?;
    let clock = Arc::new(ManualClock::new(0));
    let mut owners = Vec::new();
    for log in logs {
        owners.push(WorkerHandle::start_with_leases(log, FileSync, batch, 8, 8,
            LeaseConfig::new(epoch, 3, 10)?, clock.clone())?);
    }
    let client = owners[0].client();
    let group0 = GroupId::new(0)?;
    let group1 = GroupId::new(1)?;
    for payload in [b"cat", b"dog", b"owl", b"fox"] {
        request(client.try_publish(payload.to_vec()))?;
    }
    println!("epoch={} partitions={} clock=0ms", epoch.get(), owners.len());
    let first = request(client.try_lease(group0, ReadLimits::new(3, 105)?))?;
    let other = request(client.try_lease(group1, ReadLimits::new(1, 35)?))?;
    assert_eq!(describe(&first), vec![(0, 1, 10), (1, 1, 10), (2, 1, 10)]);
    println!("group=0 attempts={:?} occupied={}", describe(&first), first.state.window.occupied);
    let follower = request(client.try_ack(group0, first.records[2].token))?;
    assert_eq!((follower.retired, follower.state.window.completed_end.get()), (0, 0));
    println!("ack=2 outcome={:?} retired={}", follower.outcome, follower.retired);

```

At time ten, the old head token is expired even before retry selection. The retry batch contains offsets 0 and 1, not the completed follower 2, and occupancy remains three. Complete retry 1 before retry 0 to make the eventual three-position retirement visible.

```rust title="crates/broker-core/examples/lease_probe.rs"
    clock.advance(10)?;
    let expired = request(client.try_ack(group0, first.records[0].token))?;
    assert_eq!(expired.outcome, AckOutcome::Rejected(AckRejection::Expired));
    println!("clock=10ms old=0 outcome={:?}", expired.outcome);
    let retry = request(client.try_lease(group0, ReadLimits::new(3, 105)?))?;
    assert_eq!(describe(&retry), vec![(0, 2, 20), (1, 2, 20)]);
    assert_eq!(retry.state.window.occupied, 3);
    println!("group=0 retries={:?} occupied={}", describe(&retry), retry.state.window.occupied);
    let stale = request(client.try_ack(group0, first.records[0].token))?;
    assert_eq!(stale.outcome, AckOutcome::Rejected(AckRejection::StaleGeneration));
    println!("old=0 outcome={:?}", stale.outcome);
    for index in [1usize, 0] {
        let accepted = request(client.try_ack(group0, retry.records[index].token))?;
        println!("retry_ack={} retired={} completed_end={}", retry.records[index].token.offset.get(),
            accepted.retired, accepted.state.window.completed_end.get());
        assert_eq!(accepted.retired, if index == 1 { 0 } else { 3 });
    }
    let retired = request(client.try_ack(group0, first.records[2].token))?;
    assert_eq!(retired.outcome, AckOutcome::AlreadyRetired);
    println!("old=2 outcome={:?}", retired.outcome);
    let next = request(client.try_lease(group0, ReadLimits::new(1, 35)?))?;
    assert_eq!(describe(&next), vec![(3, 1, 20)]);
    println!("group=0 next={:?}", describe(&next));
    let stalled = request(client.try_group_state(group1))?;
    assert_eq!((stalled.window.completed_end.get(), stalled.window.issued_end.get()), (0, 1));
    println!("group=1 completed_end={} issued_end={}",
        stalled.window.completed_end.get(), stalled.window.issued_end.get());
    let old = first.records[0].token;
    drop((first, retry, other, next));
    for owner in owners { owner.join()?; }
    drop(client);

```

Recovery resets the demonstration clock to zero and repeats offset 0 under a new epoch. The old and new numeric attempt triples match, but the old epoch does not. This is an orderly restart demonstration, not a process-crash or power-loss test. Chapter 10's interruption harness remains a separate check.

```rust title="crates/broker-core/examples/lease_probe.rs"
    // Recovered progress is still zero: Chapter 13 will add a durable consumer checkpoint.
    let (epoch, mut logs) = recovery::recover(root)?.into_parts();
    let owner = WorkerHandle::start_with_leases(logs.remove(0), FileSync, batch, 8, 8,
        LeaseConfig::new(epoch, 3, 10)?, Arc::new(ManualClock::new(0)))?;
    drop(logs);
    let client = owner.client();
    let repeated = request(client.try_lease(group0, ReadLimits::new(1, 35)?))?;
    let new = repeated.records[0].token;
    assert_eq!((new.offset, new.generation, new.deadline_ms), (old.offset, old.generation, old.deadline_ms));
    assert_ne!(new.epoch, old.epoch);
    println!("recovered epoch={} attempts={:?}", epoch.get(), describe(&repeated));
    let fenced = request(client.try_ack(group0, old))?;
    assert_eq!(fenced.outcome, AckOutcome::Rejected(AckRejection::WrongEpoch));
    println!("pre_restart_token outcome={:?}", fenced.outcome);
    assert_eq!(request(client.try_ack(group0, new))?.retired, 1);
    owner.join()?;
    Ok(())
}
```

Build and run against a new temporary parent. The child broker path must be absent; initialization will not reinterpret an existing directory as a fresh broker.

```sh
cargo build -p broker-core --example lease_probe
PARENT="$(mktemp -d "${TMPDIR:-/tmp}/lease-probe.XXXXXX")"
trap 'rm -rf -- "$PARENT"' EXIT
cargo run -p broker-core --example lease_probe -- "$PARENT/broker"
```

Expected application output, excluding Cargo's own status messages:

```text
epoch=1 partitions=4 clock=0ms
group=0 attempts=[(0, 1, 10), (1, 1, 10), (2, 1, 10)] occupied=3
ack=2 outcome=Applied retired=0
clock=10ms old=0 outcome=Rejected(Expired)
group=0 retries=[(0, 2, 20), (1, 2, 20)] occupied=3
old=0 outcome=Rejected(StaleGeneration)
retry_ack=1 retired=0 completed_end=0
retry_ack=0 retired=3 completed_end=3
old=2 outcome=AlreadyRetired
group=0 next=[(3, 1, 20)]
group=1 completed_end=0 issued_end=1
recovered epoch=2 attempts=[(0, 1, 10)]
pre_restart_token outcome=Rejected(WrongEpoch)
```

There are no throughput or latency measurements in this output. The numeric deadlines come from a manual clock, not observed system scheduling. The successful completion before restart is still volatile: group 0 repeats offset 0 after recovery. Fencing prevents an old token from completing that new delivery, but does not preserve consumer progress.

## Diagnose a wrong result by its state transition

| Symptom | Likely mechanism error | First place to inspect |
| --- | --- | --- |
| The old token is accepted at exactly its deadline | Used `>` instead of `>=`, or trusted a client timestamp | Outstanding-attempt time comparison and owner sampling |
| Every single-record poll retries the head | Retry scan restarts from completed end | Cursor advancement after an actually installed retry |
| New work starves while free slots exist | Preference resets at the start of each poll | Persistent category preference in `install` |
| A 34-byte refusal consumes generation 2 | Attempt changed during planning or before the byte-limited read completed | Boundary between `plan`, copied prefix, and `install` |
| A completed follower reappears in retries | Retry selection ignores `Slot::Completed` | Eligibility predicate, not just deadline comparison |
| Wrong-group token advances the selected group | Handler uses token group as routing authority | Separate command group and token scope validation |
| A leased owner accepts `try_complete` | Old command still reaches leased window state | `DeliveryMode` match arms |
| An old token works after reconstruction | Epoch was reused or not validated | Session recovery and the first acknowledgement scope check |
| A reply timeout is “fixed” by a longer sleep | Test assumes scheduling instead of waiting for the required event | Control-lane order, received results, and watchdog failure |

A too-small byte limit can intentionally block the next selected candidate. This version does not skip an oversized selected retry to search for a smaller new record. Raise the allowance to fit a valid record or define and test a different selection policy; do not secretly increment its generation and discard the returned bytes.

## Predict two changes before running them

First change the example's window width to 1. Group 0 can issue only offset 0; there is no follower slot to complete. This is not a broken lease algorithm. Adapt the demonstration to expire and retry that one record, acknowledge it, then issue offset 1. Occupancy must remain one through retries and become zero only after its valid acknowledgement retires it.

Next keep width 3 but reduce the retry response allowance to 35 bytes. The first retry response should carry only offset 0 at generation 2. Offset 1 must retain generation 1 until a later successful response actually returns it. Completing offset 0 alone cannot retire the still-outstanding offset 1 or the completed offset 2 behind it. Predict the state as completed end 1, issued end 3, occupied 2 before making the edit.

These variations change behavior without creating another timer service or a second payload store. Retaining the invariants is the point, not increasing the number of mechanisms.

## Boundary reached

The broker can now retry lost deliveries with bounded metadata and reject stale attempts using epoch, partition, group, offset, generation, and deadline checks before accepting a new completion. Completed followers still cannot bypass a gap. A missing reply is recoverable through expiration, not by pretending issuance did not happen.

What remains deliberately absent is durable consumer progress. `AckOutcome::Applied` changes memory but publishes no checkpoint. The next chapter will distinguish accepted completion from checkpointed contiguous progress, define its disk format and publication boundary, and restore windows from that persisted position. Lease attempt state will remain volatile; old tokens will continue to be fenced by a fresh startup epoch.
