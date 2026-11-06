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
