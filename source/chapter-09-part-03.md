# Chapter 9, Part 3 — Controlled failures and four-partition checks

A test that sleeps briefly and finds no reply has not established where the owner was executing. It may have been descheduled before writing anything. To test the publication boundary, stop the owner **inside the synchronization adapter**, after all group writes and before that synchronization can return. Then inspect the reply channels while the owner remains at that known boundary.

**Prerequisites:** the complete Chapter 8 workspace and both preceding parts of Chapter 9. Retain `tests/common/mod.rs` from Chapter 7. This part completes the single **6.0-hour** chapter with the integration tests and `owner_probe` executable; it does not add another implementation estimate.

> **Learning objectives**
> Create controlled interleavings around real file synchronization; distinguish empty, failed, and disconnected replies; test each batch trigger separately; observe failure through joining; and validate concurrent producers without assuming a global ordering between partitions.

Create `crates/broker-core/tests/group_commit.rs`. Every filename-labelled listing for that file concatenates in order. These tests use real temporary segment files and the real storage/owner implementations. Their bounded trace vectors and watchdogs are test infrastructure, not broker admission or durability mechanisms.

## Stop exactly at a synchronization boundary

The adapter owns one receiver for test instructions. The test controller owns the corresponding sender and a receiver that announces entry. A small mutex protects only the selected future sync ordinal and a trace of segment bases.

```rust title="crates/broker-core/tests/group_commit.rs"
mod common;

use std::io;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use broker_core::io::StorageFile;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::log::{PartitionLog, segment_name};
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{Offset, PartitionId};
use broker_core::worker::{BatchConfig, Client, RequestError, Ticket, WorkerError, WorkerHandle, WorkerState};
use common::TestDir;

const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Debug)]
enum Action { Proceed, Fail, Panic }

#[derive(Default)]
struct GateState { remaining: usize, calls: Vec<u64> }

struct Gate {
    state: Arc<Mutex<GateState>>,
    entered: mpsc::SyncSender<u64>,
    release: mpsc::Receiver<Action>,
}

struct GateControl {
    state: Arc<Mutex<GateState>>,
    entered: mpsc::Receiver<u64>,
    release: mpsc::SyncSender<Action>,
}

```

The two one-slot channels carry opposite-direction handshakes. `remaining=0` means that no sync is armed. Arming ordinal 2 makes the next sync proceed normally and holds the one after it. This is how we can test failure of a later file after an earlier file in the same group has already synchronized.

```rust title="crates/broker-core/tests/group_commit.rs"
fn gate() -> (Gate, GateControl) {
    let state = Arc::new(Mutex::new(GateState::default()));
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    (Gate { state: Arc::clone(&state), entered: entered_tx, release: release_rx },
     GateControl { state, entered: entered_rx, release: release_tx })
}

impl SyncIo for Gate {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        let hold = {
            let mut state = self.state.lock().unwrap();
            state.calls.push(base.get());
            if state.remaining == 0 { false } else {
                state.remaining -= 1;
                state.remaining == 0
            }
        };
        if hold {
            self.entered.try_send(base.get()).map_err(|e| io::Error::other(e.to_string()))?;
            match self.release.recv_timeout(WATCHDOG)
                .map_err(|e| io::Error::new(io::ErrorKind::TimedOut, e))?
            {
                Action::Proceed => {}
                Action::Fail => return Err(io::Error::other("injected synchronization failure")),
                Action::Panic => panic!("injected owner panic outside mailbox lock"),
            }
        }
        file.sync()
    }
}

```

`Gate::sync` records the file base and updates the countdown while holding the small test mutex, then drops that guard **before** announcing or waiting. The controller can therefore inspect the trace while the owner is blocked. `Proceed` invokes the actual `StorageFile::sync`; `Fail` returns an I/O error without claiming a successful sync; `Panic` exercises unwinding outside the mailbox mutex.

The five-second watchdog bounds a broken test handshake. It is not evidence that a disk operation must finish in five seconds. The adapter also does not simulate a hardware cache losing data or a directory entry disappearing on power loss.

```rust title="crates/broker-core/tests/group_commit.rs"
impl GateControl {
    fn arm(&self, ordinal: usize) {
        assert!(ordinal > 0);
        let mut state = self.state.lock().unwrap();
        state.remaining = ordinal;
        state.calls.clear();
    }
    fn wait(&self) -> u64 { self.entered.recv_timeout(WATCHDOG).expect("sync gate not reached") }
    fn release(&self, action: Action) { self.release.try_send(action).unwrap(); }
    fn calls(&self) -> Vec<u64> { self.state.lock().unwrap().calls.clone() }
    fn clear(&self) { self.state.lock().unwrap().calls.clear(); }
}

impl Drop for GateControl {
    fn drop(&mut self) { let _ = self.release.try_send(Action::Fail); }
}

```

`GateControl::drop` offers a failure instruction during ordinary cleanup. The adapter's own timeout is still necessary: local destruction order can drop an owner handle, which joins, before the controller itself is destroyed. A test failure must not rely exclusively on a destructor that has not run yet.

The remaining helpers reuse Chapter 7's `TestDir` and Chapter 8's validated limits. `start` synchronizes the initial file as part of owner construction, then clears that startup entry from the trace. Later assertions therefore count runtime synchronization calls rather than silently including an initial header operation.

```rust title="crates/broker-core/tests/group_commit.rs"
fn storage(cap: u64, count: usize) -> StorageConfig { StorageConfig::new(cap, 2, count).unwrap() }
fn batch(count: usize, bytes: usize) -> BatchConfig {
    BatchConfig::new(count, bytes, Duration::from_secs(60)).unwrap()
}
fn limits() -> ReadLimits { ReadLimits::new(1024, 4096).unwrap() }
fn start(directory: &TestDir, storage: StorageConfig, batch: BatchConfig)
    -> (WorkerHandle, Client, GateControl)
{
    let directory = Arc::new(DirectoryGuard::acquire(directory.path()).unwrap());
    let log = PartitionLog::create(directory, PartitionId::new(0).unwrap(), storage).unwrap();
    let (gate, control) = gate();
    let owner = WorkerHandle::start(log, gate, batch, 32, 8).unwrap();
    control.clear();
    let client = owner.client();
    (owner, client, control)
}
fn receive<T>(ticket: &Ticket<T>) -> Result<T, RequestError> {
    ticket.recv_timeout(WATCHDOG).expect("missing reply or disconnected sender")
}
fn no_reply<T>(ticket: &Ticket<T>) {
    assert!(matches!(ticket.try_recv(), Err(mpsc::TryRecvError::Empty)));
}
fn written(client: &Client, target: u64) -> WorkerState {
    // Only this test submits controls. Alternating lanes make at least one queued
    // data command progress between successive snapshots while both lanes are busy.
    for _ in 0..target + 2 {
        let state = receive(&client.try_snapshot().unwrap()).unwrap();
        if state.written_end.get() == target { return state; }
    }
    panic!("expected written end {target}");
}

```

[`Receiver::try_recv`](https://doc.rust-lang.org/std/sync/mpsc/struct.Receiver.html#method.try_recv) distinguishes an empty channel from a disconnected one. `no_reply` requires **Empty**, not merely “some error”: a prematurely destroyed reply sender is a failure, not proof of correct withholding. `receive` requires an explicit response within a watchdog; an inner `Err(RequestError::...)` is different from the channel itself timing out or disconnecting.

The `written` helper does not assume that a snapshot enqueued after data is a barrier. Only this test submits controls; while data remains queued, alternating service forces progress between successive snapshots. Its small bounded loop observes the requested written end without sleeps. This helper is for these fixtures, not a proposed busy-polling network API.

## Check both fetch visibility and reply withholding

The first test has two phases. With one record written but a two-record group incomplete, a real fetch must return an empty visible suffix. With the second record written and sync held, both success replies must remain absent; a queued fetch also cannot be serviced until the owner leaves the sync call.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn written_bytes_are_hidden_until_the_entire_sync_completes() {
    let directory = TestDir::new().unwrap();
    let (owner, client, gate) = start(&directory, storage(4096, 8), batch(2, 1024));
    let first = client.try_publish(b"cat".to_vec()).unwrap();
    let state = written(&client, 1);
    assert_eq!((state.assigned_end.get(), state.written_end.get(), state.visible_end.get()), (1, 1, 0));
    assert_eq!((state.pending_records, state.pending_bytes), (1, 35));
    let hidden = receive(&client.try_fetch(Offset::new(0), limits()).unwrap()).unwrap();
    assert!(hidden.records.is_empty());
    no_reply(&first);
    gate.arm(1);
    let second = client.try_publish(b"dog".to_vec()).unwrap();
    assert_eq!(gate.wait(), 0);
    let fetch = client.try_fetch(Offset::new(0), limits()).unwrap();
    no_reply(&first); no_reply(&second); no_reply(&fetch);
    gate.release(Action::Proceed);
    assert_eq!(receive(&first).unwrap().get(), 0);
    assert_eq!(receive(&second).unwrap().get(), 1);
    let visible = receive(&fetch).unwrap();
    assert_eq!(visible.records.len(), 2);
    assert_eq!(visible.records[0].payload(), b"cat");
    assert_eq!(visible.records[1].payload(), b"dog");
    assert_eq!(owner.join().unwrap().sync_groups, 1);
}

```

These are different observations. The first fetch positively exercises `read(start, visible_end, limits)` while written bytes exist. The fetch queued during the gate instead proves no response escapes through an alternative read path while the only owner is blocked. After release, it must return both payloads in offset order.

For the cross-rotation fixture, use the 102-byte cap calculated in Part 1. Five `cat` records require bases 0, 2, and 4. One logical group must cause three file synchronizations:

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn one_group_crossing_rotation_syncs_every_dirty_file() {
    let directory = TestDir::new().unwrap();
    let (owner, client, gate) = start(&directory, storage(102, 8), batch(5, 1024));
    gate.arm(1);
    let tickets: Vec<_> = (0..5).map(|_| client.try_publish(b"cat".to_vec()).unwrap()).collect();
    assert_eq!(gate.wait(), 0);
    for ticket in &tickets { no_reply(ticket); }
    gate.release(Action::Proceed);
    for (i, ticket) in tickets.iter().enumerate() { assert_eq!(receive(ticket).unwrap().get(), i as u64); }
    assert_eq!(gate.calls(), vec![0, 2, 4]);
    let state = owner.join().unwrap();
    assert_eq!((state.visible_end.get(), state.sync_groups), (5, 1));
}

```

The expected trace `[0, 2, 4]` catches an implementation that synchronizes only the latest file. It also checks that all five replies stay withheld at the first held sync. `sync_groups=1` counts the logical group, not the number of files.

Now hold the second file of a four-record group, then fail it. Include another queued publish and fetch so terminal cleanup must handle more than the pending vector.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn later_sync_failure_replies_to_no_member_of_the_group() {
    let directory = TestDir::new().unwrap();
    let (owner, client, gate) = start(&directory, storage(102, 8), batch(4, 1024));
    gate.arm(2);
    let tickets: Vec<_> = (0..4).map(|_| client.try_publish(b"cat".to_vec()).unwrap()).collect();
    assert_eq!(gate.wait(), 2); // base 0 has already synchronized; base 2 has not.
    let fetch = client.try_fetch(Offset::new(0), limits()).unwrap();
    let queued = client.try_publish(b"owl".to_vec()).unwrap();
    for ticket in &tickets { no_reply(ticket); }
    gate.release(Action::Fail);
    for ticket in &tickets { assert!(matches!(receive(ticket), Err(RequestError::Failed(_)))); }
    assert!(matches!(receive(&fetch), Err(RequestError::Failed(_))));
    assert!(matches!(receive(&queued), Err(RequestError::Failed(_))));
    match owner.join() {
        Err(WorkerError::Failed { state, cause }) => {
            assert_eq!((state.assigned_end.get(), state.written_end.get(), state.visible_end.get()), (4, 4, 0));
            assert!(std::error::Error::source(cause.as_ref()).is_some());
        }
        other => panic!("expected joined failure, got {other:?}"),
    }
    assert_eq!(gate.calls(), vec![0, 2]);
    let refusal = client.try_publish(vec![9]).unwrap_err();
    assert_eq!(refusal.reason, AdmissionError::Closed);
    assert_eq!(refusal.value, vec![9]);
}

```

At the gate, base 0 has already synchronized. The expected joined state is still `assigned=4, written=4, visible=0`. Every pending and queued request receives an explicit error; subsequent admission is closed and returns its payload. No test assumes that failure erased the four written records.

An earlier successful group must remain visible when a later group fails:

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn a_failed_later_group_does_not_retract_an_earlier_success() {
    let directory = TestDir::new().unwrap();
    let (owner, client, gate) = start(&directory, storage(4096, 8), batch(1, 1024));
    assert_eq!(receive(&client.try_publish(b"cat".to_vec()).unwrap()).unwrap().get(), 0);
    gate.arm(1);
    let second = client.try_publish(b"dog".to_vec()).unwrap();
    gate.wait(); gate.release(Action::Fail);
    assert!(matches!(receive(&second), Err(RequestError::Failed(_))));
    match owner.join() {
        Err(WorkerError::Failed { state, .. }) => {
            assert_eq!((state.written_end.get(), state.visible_end.get(), state.sync_groups), (2, 1, 1));
        }
        other => panic!("expected joined failure, got {other:?}"),
    }
}

```

The first receipt has already escaped only after its sync succeeded. The later failure reports `written=2, visible=1`, not a reset to zero and not an advance to two.

## Exercise count, byte, and idle-deadline triggers independently

The first visibility test already exercised a two-record count limit with ample byte capacity. The next tests use a high record cap so that an exact byte boundary, and then a nonfitting next record, must be the actual trigger.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn exact_byte_limit_commits_without_waiting_for_the_record_limit() {
    let directory = TestDir::new().unwrap();
    let (owner, client, gate) = start(&directory, storage(4096, 8), batch(8, 70));
    gate.arm(1);
    let a = client.try_publish(b"cat".to_vec()).unwrap();
    let b = client.try_publish(b"dog".to_vec()).unwrap();
    gate.wait(); no_reply(&a); no_reply(&b); gate.release(Action::Proceed);
    assert_eq!(receive(&a).unwrap().get(), 0);
    assert_eq!(receive(&b).unwrap().get(), 1);
    assert_eq!(owner.join().unwrap().sync_groups, 1);
}

```

At a 70-byte cap, two 35-byte records commit without a third request or an explicit flush. At a 71-byte cap, the next 36-byte record must instead flush the 70-byte group before receiving its offset:

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn a_nonfitting_next_record_commits_the_old_group_before_assignment() {
    let directory = TestDir::new().unwrap();
    let (owner, client, gate) = start(&directory, storage(4096, 8), batch(8, 71));
    gate.arm(1);
    let a = client.try_publish(b"cat".to_vec()).unwrap();
    let b = client.try_publish(b"dog".to_vec()).unwrap();
    let c = client.try_publish(b"bird".to_vec()).unwrap();
    gate.wait(); no_reply(&a); no_reply(&b); no_reply(&c); gate.release(Action::Proceed);
    assert_eq!(receive(&a).unwrap().get(), 0);
    assert_eq!(receive(&b).unwrap().get(), 1);
    let state = written(&client, 3);
    assert_eq!((state.visible_end.get(), state.pending_records, state.pending_bytes), (2, 1, 36));
    no_reply(&c);
    let final_state = owner.join().unwrap();
    assert_eq!(receive(&c).unwrap().get(), 2);
    assert_eq!((final_state.visible_end.get(), final_state.sync_groups), (3, 2));
}

```

The snapshot after the boundary verifies that the third record belongs to a new pending group. Closure later commits it. This catches an implementation that accidentally included the nonfitting record in the old group or silently discarded its reply while flushing.

The idle case must succeed without any additional command waking the owner:

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn an_idle_partial_group_commits_on_its_deadline() {
    let directory = TestDir::new().unwrap();
    let config = BatchConfig::new(8, 1024, Duration::from_millis(10)).unwrap();
    let (owner, client, _) = start(&directory, storage(4096, 8), config);
    let ticket = client.try_publish(b"cat".to_vec()).unwrap();
    // No second command, explicit flush, or close can make this reply succeed.
    assert_eq!(receive(&ticket).unwrap().get(), 0);
    assert_eq!(owner.join().unwrap().sync_groups, 1);
}

```

The ten-millisecond delay is not compared with a narrow timing tolerance. The observable requirement is that a single pending publish receives success without another record, flush, or close. Part 1 separately tests that an expired deadline takes priority even when lanes are nonempty.

## Refusal must leave a healthy owner usable

A four-byte payload cannot fit a 67-byte segment with two 32-byte headers: it needs 68 bytes. Refuse it before assignment, then show that a three-byte payload still receives offset 0. Separately reject a record that fits storage but exceeds the configured batch-byte cap.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn refusal_does_not_allocate_an_offset_or_kill_the_owner() {
    let directory = TestDir::new().unwrap();
    let (owner, client, _) = start(&directory, storage(67, 8), batch(1, 1024));
    let rejected = client.try_publish(b"bird".to_vec()).unwrap();
    assert!(matches!(receive(&rejected), Err(RequestError::Refused(_))));
    assert_eq!(receive(&client.try_publish(b"cat".to_vec()).unwrap()).unwrap().get(), 0);
    assert_eq!(owner.join().unwrap().visible_end.get(), 1);
}

#[test]
fn the_batch_byte_cap_is_checked_before_a_write() {
    let directory = TestDir::new().unwrap();
    let (owner, client, _) = start(&directory, storage(4096, 8), batch(1, 34));
    assert!(matches!(receive(&client.try_publish(b"cat".to_vec()).unwrap()), Err(RequestError::Refused(_))));
    assert_eq!(receive(&client.try_publish(b"ok".to_vec()).unwrap()).unwrap().get(), 0);
    assert_eq!(owner.join().unwrap().written_end.get(), 1);
}

```

A segment-count refusal and an invalid visible read range are also preflight failures, not reasons to terminate healthy storage. Both tests perform a later successful operation through the same owner.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn segment_quota_refusal_leaves_the_owner_healthy() {
    let directory = TestDir::new().unwrap();
    let (owner, client, _) = start(&directory, storage(67, 1), batch(1, 1024));
    receive(&client.try_publish(b"cat".to_vec()).unwrap()).unwrap();
    assert!(matches!(receive(&client.try_publish(b"dog".to_vec()).unwrap()), Err(RequestError::Refused(_))));
    let records = receive(&client.try_fetch(Offset::new(0), limits()).unwrap()).unwrap();
    assert_eq!(records.records.len(), 1);
    assert_eq!(owner.join().unwrap().written_end.get(), 1);
}

#[test]
fn bad_fetch_range_is_a_refusal_not_a_partition_failure() {
    let directory = TestDir::new().unwrap();
    let (owner, client, _) = start(&directory, storage(4096, 8), batch(1, 1024));
    let bad = client.try_fetch(Offset::new(1), limits()).unwrap();
    assert!(matches!(receive(&bad), Err(RequestError::Refused(_))));
    receive(&client.try_publish(b"cat".to_vec()).unwrap()).unwrap();
    assert_eq!(owner.join().unwrap().visible_end.get(), 1);
}

```

The flush test explicitly observes a processed record before requesting a flush. It also checks that an empty flush does not create spurious sync operations or increment the successful-group counter.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn empty_flush_does_no_io_and_nonempty_flush_commits_only_processed_data() {
    let directory = TestDir::new().unwrap();
    let (owner, client, gate) = start(&directory, storage(4096, 8), batch(8, 1024));
    assert_eq!(receive(&client.try_flush().unwrap()).unwrap().sync_groups, 0);
    assert!(gate.calls().is_empty());
    let ticket = client.try_publish(b"cat".to_vec()).unwrap();
    written(&client, 1); // Do not assume control-lane enqueue order fences data.
    assert_eq!(receive(&client.try_flush().unwrap()).unwrap().visible_end.get(), 1);
    assert_eq!(receive(&ticket).unwrap().get(), 0);
    assert_eq!(receive(&client.try_flush().unwrap()).unwrap().sync_groups, 1);
    assert_eq!(gate.calls(), vec![0]);
    owner.join().unwrap();
}

```

## Cancellation and shutdown do not erase accepted work

Drop a ticket immediately after admission, then close and join. The record must still be present in a strict reopen. This is a complete-record read-back check after graceful completion, not a crash-recovery experiment.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn dropping_a_reply_does_not_cancel_transferred_work() {
    let directory = TestDir::new().unwrap();
    let (owner, client, _) = start(&directory, storage(4096, 8), batch(8, 1024));
    drop(client.try_publish(b"cat".to_vec()).unwrap());
    assert_eq!(owner.join().unwrap().visible_end.get(), 1);
    let guard = Arc::new(DirectoryGuard::acquire(directory.path()).unwrap());
    let mut log = PartitionLog::open_strict(guard, PartitionId::new(0).unwrap(), storage(4096, 8)).unwrap();
    let records = log.read(Offset::new(0), Offset::new(1), limits()).unwrap();
    assert_eq!(records.records[0].payload(), b"cat");
}

```

`Client` contains only a mailbox handle, so retaining it after the owner joins cannot retain the log's directory lock. The reopened log obtains a new guard. This is another ownership relationship that would be obscured by sharing the whole log with clients.

The next test submits eight records into a sufficiently large lane and closes admission. Three-record groups produce two full groups and one final two-record group. Every accepted ticket must complete, and the last partial group must not wait for its 60-second batching delay.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn close_drains_all_accepted_data_and_commits_the_last_partial_group() {
    let directory = TestDir::new().unwrap();
    let (owner, client, _) = start(&directory, storage(4096, 8), batch(3, 1024));
    let tickets: Vec<_> = (0..8).map(|_| client.try_publish(b"cat".to_vec()).unwrap()).collect();
    owner.close();
    assert_eq!(client.try_publish(vec![4]).unwrap_err().reason, AdmissionError::Closed);
    let state = owner.join().unwrap();
    for (i, ticket) in tickets.iter().enumerate() { assert_eq!(receive(ticket).unwrap().get(), i as u64); }
    assert_eq!((state.visible_end.get(), state.pending_records, state.sync_groups), (8, 0, 3));
}

```

Dirty tracking also matters after success. Once the first two-record file has synchronized, rotating and committing the next two records should synchronize only the new file:

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn a_clean_old_segment_is_not_resynchronized_after_rotation() {
    let directory = TestDir::new().unwrap();
    let (owner, client, gate) = start(&directory, storage(102, 8), batch(2, 1024));
    for _ in 0..2 {
        let a = client.try_publish(b"cat".to_vec()).unwrap();
        let b = client.try_publish(b"dog".to_vec()).unwrap();
        receive(&a).unwrap(); receive(&b).unwrap();
    }
    assert_eq!(gate.calls(), vec![0, 2]);
    assert_eq!(owner.join().unwrap().sync_groups, 2);
}

```

## Stop after uncertain I/O, and do not hide a panic

The next fixture forces a real create failure during rotation by placing a directory at the next expected segment pathname. That namespace collision is deliberate fault injection in an otherwise owned temporary directory. The already-written first record is still pending; the second has been assigned but cannot complete its append.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn rotation_creation_failure_stops_and_fails_the_pending_group() {
    let directory = TestDir::new().unwrap();
    let (owner, client, _) = start(&directory, storage(67, 8), batch(4, 1024));
    let first = client.try_publish(b"cat".to_vec()).unwrap();
    written(&client, 1);
    let collision = directory.path().join("partition-0").join(segment_name(Offset::new(1)));
    std::fs::create_dir(collision).unwrap();
    let second = client.try_publish(b"dog".to_vec()).unwrap();
    assert!(matches!(receive(&first), Err(RequestError::Failed(_))));
    assert!(matches!(receive(&second), Err(RequestError::Failed(_))));
    match owner.join() {
        Err(WorkerError::Failed { state, .. }) => {
            assert_eq!((state.assigned_end.get(), state.written_end.get(), state.visible_end.get()), (2, 1, 0));
        }
        other => panic!("expected joined failure, got {other:?}"),
    }
}

```

The report `assigned=2, written=1, visible=0` demonstrates why the assigned frontier is not interchangeable with the fully written one. The owner fails both replies and stops. Chapter 7's `FaultFile` tests still cover positive short writes and a prefix left behind by a write error; this fixture specifically covers a failed rotation creation through the real partition-owner path.

A panic is not an I/O `Err`. The following test panics at a held sync after placing data and control commands behind it. All unfinished reply wrappers must deliver their fallback errors, and joining must identify the panic.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn an_owner_panic_fails_pending_and_queued_replies_and_is_joined() {
    let directory = TestDir::new().unwrap();
    let (owner, client, gate) = start(&directory, storage(4096, 8), batch(1, 1024));
    gate.arm(1);
    let pending = client.try_publish(b"cat".to_vec()).unwrap();
    gate.wait();
    let queued = client.try_publish(b"dog".to_vec()).unwrap();
    let control = client.try_snapshot().unwrap();
    gate.release(Action::Panic);
    assert!(matches!(receive(&pending), Err(RequestError::OwnerStopped)));
    assert!(matches!(receive(&queued), Err(RequestError::OwnerStopped)));
    assert!(matches!(receive(&control), Err(RequestError::OwnerStopped)));
    assert!(matches!(owner.join(), Err(WorkerError::Panicked)));
}

```

With a normal unwinding test profile, a panic hook may print the injected message when output capture is disabled; the expected test result is still success after asserting `WorkerError::Panicked`. An aborting panic profile cannot exercise this cleanup path because it terminates the process.

The next test checks the handle's ordinary-scope fallback: dropping the handle joins, rather than merely detaches, and the directory guard is available again afterward.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn dropping_the_handle_joins_instead_of_detaching() {
    let directory = TestDir::new().unwrap();
    let (owner, client, _) = start(&directory, storage(4096, 8), batch(8, 1024));
    let ticket = client.try_publish(b"cat".to_vec()).unwrap();
    drop(owner);
    assert_eq!(receive(&ticket).unwrap().get(), 0);
    assert_eq!(client.try_snapshot().unwrap_err(), AdmissionError::Closed);
    // Only clients remain; none may keep the data-directory file guard alive.
    let _new_guard = DirectoryGuard::acquire(directory.path()).unwrap();
}

```

## Initialize visibility only after synchronization

The initial synchronization operation is outside the worker thread, before any client can be returned. A failure there must produce neither a usable client nor a leaked directory guard.

```rust title="crates/broker-core/tests/group_commit.rs"
struct RejectSync;
impl SyncIo for RejectSync {
    fn sync(&mut self, _base: Offset, _file: &mut dyn StorageFile) -> io::Result<()> {
        Err(io::Error::other("initial synchronization refused"))
    }
}

#[test]
fn initial_sync_failure_returns_no_client_or_thread_handle() {
    let directory = TestDir::new().unwrap();
    let guard = Arc::new(DirectoryGuard::acquire(directory.path()).unwrap());
    let log = PartitionLog::create(guard, PartitionId::new(0).unwrap(), storage(4096, 8)).unwrap();
    assert!(matches!(WorkerHandle::start(log, RejectSync, batch(2, 1024), 4, 4), Err(WorkerError::Setup(_))));
    let _guard = DirectoryGuard::acquire(directory.path()).unwrap();
}

```

Strict reopening a valid but previously unsynchronized fixture marks every discovered file dirty. Starting an owner resynchronizes both segment files before exposing their complete prefix. This still performs no repair and makes no inference about old producer receipts.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn strict_reopening_is_resynchronized_before_becoming_visible() {
    let directory = TestDir::new().unwrap();
    let guard = Arc::new(DirectoryGuard::acquire(directory.path()).unwrap());
    let mut log = PartitionLog::create(Arc::clone(&guard), PartitionId::new(0).unwrap(), storage(102, 8)).unwrap();
    for _ in 0..3 { log.append(b"cat").unwrap(); }
    drop(log);
    let reopened = PartitionLog::open_strict(guard, PartitionId::new(0).unwrap(), storage(102, 8)).unwrap();
    let (gate, control) = gate();
    let owner = WorkerHandle::start(reopened, gate, batch(2, 1024), 4, 4).unwrap();
    assert_eq!(control.calls(), vec![0, 2]);
    let client = owner.client();
    assert_eq!(receive(&client.try_snapshot().unwrap()).unwrap().visible_end.get(), 3);
    assert_eq!(owner.join().unwrap().sync_groups, 0); // startup is not a producer group
}

```

Finish the test file with invalid batch settings and a four-owner check. Each partition has its own offset space, so each pair of publishes receives 0 and 1. Close every owner before collecting the join results.

```rust title="crates/broker-core/tests/group_commit.rs"
#[test]
fn invalid_batch_settings_are_rejected() {
    assert!(BatchConfig::new(0, 100, Duration::from_millis(1)).is_err());
    assert!(BatchConfig::new(1, 31, Duration::from_millis(1)).is_err());
    assert!(BatchConfig::new(1, 100, Duration::ZERO).is_err());
    assert!(BatchConfig::new(1, 100, Duration::from_secs(61)).is_err());
}

#[test]
fn four_partition_owners_have_independent_offset_spaces() {
    let directory = TestDir::new().unwrap();
    let guard = Arc::new(DirectoryGuard::acquire(directory.path()).unwrap());
    let mut owners = Vec::new();
    let mut tickets = Vec::new();
    for p in 0..4 {
        let log = PartitionLog::create(Arc::clone(&guard), PartitionId::new(p).unwrap(), storage(4096, 8)).unwrap();
        let owner = WorkerHandle::start(log, FileSync, batch(2, 1024), 4, 4).unwrap();
        let client = owner.client();
        tickets.push((client.try_publish(vec![p as u8]).unwrap(), client.try_publish(vec![p as u8]).unwrap()));
        owners.push(owner);
    }
    for (a, b) in tickets { assert_eq!(receive(&a).unwrap().get(), 0); assert_eq!(receive(&b).unwrap().get(), 1); }
    for owner in &owners { owner.close(); }
    for owner in owners { assert_eq!(owner.join().unwrap().visible_end.get(), 2); }
}
```

## Drive two concurrent producers per partition

Create `crates/broker-core/examples/owner_probe.rs`. The driver uses four owner threads and eight short-lived producer threads: two per partition. Every producer sends six three-byte payloads shaped as `[partition, producer, sequence]`. These are binary bytes, not UTF-8 labels.

The driver pipelines a bounded six tickets per producer. Each producer's observed offsets must increase, but offsets from the two producers may interleave in either order. A partition's combined confirmed ledger must be exactly 0 through 11.

```rust title="crates/broker-core/examples/owner_probe.rs"
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::log::PartitionLog;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::{Offset, PartitionId};
use broker_core::worker::{BatchConfig, Client, WorkerHandle};

type DemoError = Box<dyn Error + Send + Sync>;

fn producer(client: Client, partition: u16, producer: u8) -> Result<(u16, Vec<u64>), DemoError> {
    let mut tickets = Vec::new();
    for sequence in 0u8..6 {
        tickets.push(client.try_publish(vec![partition as u8, producer, sequence])
            .map_err(|e| format!("publish admission: {:?}", e.reason))?);
    }
    let mut offsets = Vec::new();
    for ticket in tickets { offsets.push(ticket.recv_timeout(Duration::from_secs(5))??.get()); }
    if !offsets.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err("one producer's accepted order was not preserved".into());
    }
    Ok((partition, offsets))
}

```

The doubled `??` is two ordinary propagations. The first unwraps the channel's `Result` from `recv_timeout`; the second unwraps the owner's `Result<Offset, RequestError>`. A transport timeout does not prove that the owner did not write the record. The driver reports failure rather than retrying an uncertain publish.

`DemoError` permits owned errors to move out of producer threads. `Send + Sync` are bounds on the error object, not a claim that the producer shares mutable log state.

The workload uses [`thread::scope`](https://doc.rust-lang.org/std/thread/fn.scope.html) for its short-lived producers. Scoped threads cannot outlive the scope. We also join them explicitly to turn panics into returned errors and collect every result before leaving.

```rust title="crates/broker-core/examples/owner_probe.rs"
fn load(clients: &[Client]) -> Result<Vec<usize>, DemoError> {
    let outcomes = thread::scope(|scope| {
        let mut threads = Vec::new();
        for (partition, client) in clients.iter().enumerate() {
            for producer_id in 0u8..2 {
                let client = client.clone();
                threads.push(scope.spawn(move || producer(client, partition as u16, producer_id)));
            }
        }
        let mut outcomes = Vec::new();
        for handle in threads {
            outcomes.push(match handle.join() {
                Ok(result) => result,
                Err(_) => Err(DemoError::from("producer thread panicked")),
            });
        }
        outcomes
    });
```

No `?` inside the join loop can leave later producer handles unexamined. Only after all producer threads have joined do we propagate their returned errors, combine their ledgers, and fetch the visible records.

For each partition the driver expects 12 records and $12(32+3)=420$ encoded-record bytes. A two-by-six Boolean array tracks the bounded producer/sequence combinations. Length and range checks precede indexing that array.

```rust title="crates/broker-core/examples/owner_probe.rs"
    let mut confirmed = vec![Vec::new(); clients.len()];
    for outcome in outcomes {
        let (partition, offsets) = outcome?;
        confirmed[usize::from(partition)].extend(offsets);
    }
    let mut counts = Vec::new();
    for (partition, client) in clients.iter().enumerate() {
        confirmed[partition].sort_unstable();
        if confirmed[partition] != (0u64..12).collect::<Vec<_>>() {
            return Err("confirmed offsets contain a gap or duplicate".into());
        }
        let ticket = client.try_fetch(Offset::new(0), ReadLimits::new(12, 420)?)
            .map_err(|e| format!("fetch admission: {e:?}"))?;
        let batch = ticket.recv_timeout(Duration::from_secs(5))??;
        if batch.records.len() != 12 || batch.encoded_bytes != 420 {
            return Err("visible record count or byte count is wrong".into());
        }
        let mut seen = [[false; 6]; 2];
        for (expected_offset, record) in batch.records.iter().enumerate() {
            let payload = record.payload();
            if record.offset().get() != expected_offset as u64 || payload.len() != 3
                || payload[0] != partition as u8 || payload[1] >= 2 || payload[2] >= 6
            {
                return Err("record identity or payload is wrong".into());
            }
            let slot = &mut seen[usize::from(payload[1])][usize::from(payload[2])];
            if *slot { return Err("duplicate producer/sequence payload".into()); }
            *slot = true;
        }
        if !seen.iter().flatten().all(|seen| *seen) { return Err("missing payload".into()); }
        counts.push(batch.records.len());
    }
    Ok(counts)
}

```

Checking only a record count could miss a duplicate payload replacing a missing one. This check verifies contiguous record offsets, partition identity, producer/sequence bounds, no repeated pair, and presence of every expected pair. It does not require a particular interleaving between the two producers.

The main program creates four logs in a fresh directory. A 172-byte segment contains four 35-byte records after its 32-byte header. Its batching policy also permits four records or 140 encoded bytes, with a five-millisecond delay. The actual number of groups can vary with scheduling; the correctness checks do not require a fixed timing pattern.

```rust title="crates/broker-core/examples/owner_probe.rs"
fn main() -> Result<(), DemoError> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: owner_probe EXISTING_EMPTY_DIRECTORY")?);
    if args.next().is_some() { return Err("expected one directory argument".into()); }
    let directory = Arc::new(DirectoryGuard::acquire(&root)?);
    let storage = StorageConfig::new(172, 2, 16)?;
    let batching = BatchConfig::new(4, 140, Duration::from_millis(5))?;
    let mut owners = Vec::new();
    let mut clients = Vec::new();
    for number in 0..4 {
        let log = PartitionLog::create(Arc::clone(&directory), PartitionId::new(number)?, storage)?;
        let owner = WorkerHandle::start(log, FileSync, batching, 32, 8)?;
        clients.push(owner.client());
        owners.push(owner);
    }
    let workload = load(&clients);
    // Close all owners before joining any. Preserve every join result even if one fails.
    for owner in &owners { owner.close(); }
    let results: Vec<_> = owners.into_iter().map(WorkerHandle::join).collect();
    let counts = workload?;
    for (partition, result) in results.into_iter().enumerate() {
        let state = result?;
        if state.visible_end.get() != 12 || state.pending_records != 0 {
            return Err("shutdown left an unexpected visible boundary".into());
        }
        println!("partition={partition} confirmed=12 visible=12 records={}", counts[partition]);
    }
    println!("48 confirmed records; four owners joined; no global partition order claimed");
    Ok(())
}
```

The workload result is saved before closing the owners. All four are closed, then every join result is collected, before any workload or join error is propagated. If initialization fails after some owners have started, the already-created handles' destructors close and join them. This is a small lifecycle boundary, not the final network supervisor.

Because segment rotation is driven by file bytes rather than group timing, each successful partition contains three 172-byte segment files. Its logical segment-file total is:

$$
3(32)+12(35)=516\text{ bytes}.
$$

Four partitions therefore contain 2064 logical segment bytes. This excludes the lock file, directory metadata, allocation blocks, indexes in memory, thread stacks, and channel buffers. Do not report it as disk usage or process memory. A faster machine or a different delay can change sync grouping without changing these record bytes.

## Run the checks in the prerequisite workspace

Use the native workspace from earlier chapters, with this chapter's nine-file overlay applied. No new Cargo dependency, manifest entry, or toolchain pin is required. Cargo discovers the new integration test and example by their conventional locations.

First run the focused tests:

```sh
cargo test -p broker-core --lib mailbox::tests
cargo test -p broker-core --test group_commit
```

The supplied checks are eight mailbox unit tests and 22 group-commit integration tests. These are expected local checks, not reported execution results. The required cumulative supplied suite now contains 157 tests, including the earlier examples. Preserve those earlier targets rather than replace them with this chapter's tests.

Run the cumulative targets and apply normal formatting to the assembled learner workspace:

```sh
cargo test --workspace --all-targets
cargo fmt --all
cargo fmt --all -- --check
```

Formatting may change line wrapping; it is not a substitute for compiling or running the tests. Run the ordinary example main separately, because compiling an example as a test target does not execute its demonstration main.

Use a newly created disposable directory. The subshell's trap removes only that freshly allocated fixture when it exits:

```sh
(
    set -eu
    demo_dir="$(mktemp -d "${TMPDIR:-/tmp}/partition-owner.XXXXXX")"
    trap 'rm -rf "$demo_dir"' EXIT
    cargo run -p broker-core --example owner_probe -- "$demo_dir"
    find "$demo_dir" -type f -name '*.seg' -print
)
```

Expected application output, apart from Cargo's own diagnostics and the subsequent file listing:

```text
partition=0 confirmed=12 visible=12 records=12
partition=1 confirmed=12 visible=12 records=12
partition=2 confirmed=12 visible=12 records=12
partition=3 confirmed=12 visible=12 records=12
48 confirmed records; four owners joined; no global partition order claimed
```

The listed segment bases in each partition should be 0, 4, and 8 in their canonical 20-digit names. `find` does not guarantee traversal order; do not turn its unsorted output into a record-order assertion. The actual owner preserves per-partition data-lane admission order and contiguous assigned offsets, not thread-number order across competing producers.

## Diagnose a failure at its boundary

| Symptom | Likely boundary violation | What to inspect |
| --- | --- | --- |
| First fetch returns `cat` before the two-record sync | Written bytes exposed too early | The `upper` passed to `PartitionLog::read` must be `visible_end`. |
| A held sync already has successful tickets | Replies precede the publication point | All success completion must follow the full `synchronize` call. |
| Cross-rotation trace contains only base 4 | Only the active file was synchronized | Mark rotated headers and appended files dirty; visit every dirty segment. |
| One idle publish never completes | No pending-group deadline, or an infinite empty wait | Set the first deadline and pass it into every `recv`. |
| Busy lanes postpone a due commit | Timer checked after dequeuing ready work | Expiry must precede both lane selections. |
| A flush unexpectedly reports fewer records | A control command overtook data | Observe the desired written end or use a specific publish receipt; flush is not a cross-lane fence. |
| Failure leaves a ticket disconnected instead of explicitly failed | Reply ownership was lost | Preserve `Reply` wrappers through queued, current, and pending states; distinguish `Empty` from `Disconnected`. |
| Shutdown waits for an otherwise idle 60-second group | Closure waits for the batching timer | Closed-and-empty should return `Closed`, which immediately commits the final group. |
| A lock remains busy after joining with only clients alive | Clients retain storage ownership | Clients should hold only the mailbox, not the log or directory guard. |

A watchdog expiration can also indicate an unusually stalled filesystem or scheduler. Preserve the failure and inspect the environment before increasing the watchdog. Never “fix” an ordering test by replacing its handshake with a longer sleep.

For a useful variation, predict the cross-rotation trace when its segment cap changes from 102 to 172 bytes while the group still contains five three-byte payloads. Four records now fit in the first file; the fifth begins at base 4, so the expected sync trace is `[0, 4]` and the visible end remains 5. Add this as an optional second fixture or restore the original after trying it. Do not change the publication order to make a test pass. A premature visible-end advance is detected by the **failed-sync joined-state** assertion; a prematurely sent success is detected by `no_reply` at the held gate. The blocked owner cannot service a snapshot while inside that sync call, so those observations are intentionally different.

## What is now implemented, and what remains separate

The partition owner now connects segmented storage, sparse reads, two bounded item lanes, count/byte/deadline batching, synchronized visibility, one-shot replies, and explicit joining. It retains the real storage engine rather than replacing it with a queue library or an embedded database.

Its limits are equally explicit. This is a trusted in-process API, not an authenticated network protocol. Item caps and pending-group byte counts do not bound caller allocations, unused vector capacity, retained tickets, or process RSS. A slow file operation blocks that partition's controls. Success is not exactly-once external processing. A dropped or timed-out ticket can leave a complete unconfirmed record. Strict reopening still refuses incomplete tails, and file synchronization is not a complete directory-publication or power-loss protocol.

**Next: Chapter 10 — Recovery, metadata publication, and restart epochs.** It will classify interrupted tails versus corruption, synchronize allowed repairs, publish metadata in a defined order, and establish a new restart epoch before serving delivery identities. Do not improvise tail truncation or reset missing metadata in this chapter's strict-open path.
