# Chapter 17 — Cancellation boundaries and supervised shutdown

## Part 4 — Controlled shutdown and failure checks

**Prerequisites:** [Parts 1–3](https://25d-228.github.io/rust-learning/chapter-17.html), assembled on the complete Chapter 16 workspace. These tests are the **1.5-hour checking/debugging portion** of the chapter's single **4.5-hour estimate**. They add **28 lifecycle integration tests and three supervisor unit tests**. The cumulative required suite contains **410 supplied tests**, not observed passes.

The important questions are not “did the process disappear?” but “which command crossed admission, which synchronization finished, which progress became authority, which resources still have owners, and which join result has actually been collected?” We control those boundaries directly. No sleep is used as proof that another task reached a particular line.

## Build one fixture that cannot deadlock its own cleanup

Create the integration entry point. The explicit paths make these three child modules and their shared support one Cargo integration target, `lifecycle`.

```rust title="crates/broker-app/tests/lifecycle.rs"
#[path = "lifecycle/support.rs"] mod support;
#[path = "lifecycle/state.rs"] mod state;
#[path = "lifecycle/failures.rs"] mod failures;
#[path = "lifecycle/network.rs"] mod network;
```

Create `crates/broker-app/tests/lifecycle/support.rs` from the following blocks in order. All imports, helper constructors, and error unwrapping are included. The test configuration uses small pools and queues; it does not silently change the production defaults.

```rust title="crates/broker-app/tests/lifecycle/support.rs"
use std::future::{Future, poll_fn};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, AtomicUsize, Ordering}, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};
use broker_app::local::{LocalBroker, OwnerConfig};
use broker_app::server::Clients;
use broker_app::supervisor::{DrainStatus, ShutdownReport, Supervisor, SupervisorConfig};
use broker_core::budget::MemoryConfig;
use broker_core::delivery::{LeaseToken, PooledLeaseBatch};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::io::StorageFile;
use broker_core::storage::{checkpoint, recovery};
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{GroupId, Offset, PartitionId};
use broker_core::worker::{AsyncTicket, BatchConfig, BoundedClient};

pub fn p(value: u16) -> PartitionId { PartitionId::new(value).unwrap() }
pub fn g(value: u8) -> GroupId { GroupId::new(value).unwrap() }
pub fn address() -> SocketAddr { (Ipv4Addr::LOCALHOST, 0).into() }
pub fn runtime() -> Runtime { tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap() }
pub fn owners() -> OwnerConfig {
    OwnerConfig { batch: BatchConfig::new(1, 4096, Duration::from_secs(1)).unwrap(),
        lanes: [2, 2, 2], window: 8, lease_ms: 10_000,
        checkpoint: CheckpointPolicy::explicit_only(), memory: MemoryConfig::new(1024, 2, 4096, 2, 2).unwrap() }
}
pub async fn reply<T>(ticket: AsyncTicket<T>) -> T {
    timeout(Duration::from_secs(10), ticket).await.expect("owner watchdog")
        .expect("reply channel").expect("owner result")
}
pub async fn append(client: &BoundedClient, payload: &[u8]) -> Offset {
    let mut request = client.try_prepare_append(payload.len()).unwrap();
    request.payload_mut().copy_from_slice(payload);
    reply(request.submit().unwrap()).await
}
pub async fn fetch(client: &BoundedClient, group: u8, count: usize) -> PooledLeaseBatch {
    reply(client.try_prepare_lease(g(group), ReadLimits::new(count, 4096).unwrap()).unwrap()
        .submit().unwrap()).await
}
pub async fn ack(client: &BoundedClient, token: LeaseToken) {
    let result = reply(client.try_ack_progress(token.group, token).unwrap()).await;
    assert!(matches!(result, broker_core::delivery::progress::ProgressAck::Accepted(_)));
}
pub async fn entered(receiver: oneshot::Receiver<()>) {
    timeout(Duration::from_secs(10), receiver).await.expect("sync-entry watchdog").unwrap();
}
pub async fn eventually(mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(10), async {
        while !predicate() { tokio::task::yield_now().await; }
    }).await.expect("observable-state watchdog");
}
```

`reply` uses ten seconds as a watchdog so a failed test produces a useful failure instead of silently waiting forever. The synchronous I/O gate, not that elapsed time, proves that an append reached synchronization. `eventually` observes a concrete resource predicate while yielding; it is used to establish that a session has actually taken a buffer. Lease time is separately controlled by `ManualClock(0)` and is not advanced by wall-clock delays in this fixture.

To test cancellation at a genuine pending state, poll the owning future once. To test a notification rather than merely a successful subsequent recheck, supply a counting waker. The standard `Wake` trait lets the test construct a safe `Waker` from an `Arc`; there is no raw-waker unsafe implementation.

```rust title="crates/broker-app/tests/lifecycle/support.rs"
pub async fn pending_once<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("future unexpectedly completed at the controlled boundary"),
    }).await;
}
pub struct WakeCount(pub AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
}
pub fn poll_with_counter<F: Future>(future: Pin<&mut F>) -> Arc<WakeCount> {
    let counter = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&counter));
    assert!(future.poll(&mut Context::from_waker(&waker)).is_pending());
    counter
}

```

The waker counter is only an observation of wake requests. It is not a scheduler and does not establish thread completion. After the request, the test polls/awaits the future normally and checks its `Closed` result. Dropping the **owning pinned future scope**, rather than just one `Pin<&mut _>` borrow, is what tests cancellation.

Now add temporary-directory ownership and a gate that can be released on normal control flow or during unwinding. Fixture paths are unique to the current process/test counter and are the only paths this helper removes.

```rust title="crates/broker-app/tests/lifecycle/support.rs"
static NEXT: AtomicU64 = AtomicU64::new(0);
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        Self(std::env::temp_dir().join(format!("broker-lifecycle-{}-{}", std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed))))
    }
}
impl Drop for Directory { fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); } }
#[derive(Clone, Copy)]
pub enum AfterGate { Pass, Fail, Panic }
struct Gate {
    entered: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
    after: AfterGate,
}
pub struct Release(Option<mpsc::Sender<()>>);
impl Release { fn release(&mut self) { if let Some(sender) = self.0.take() { let _ = sender.send(()); } } }
impl Drop for Release { fn drop(&mut self) { self.release(); } }
```

`Gate` stores an entered notification and a blocking release receiver. `Release` is the matching owned sender. Its destructor sends the release once; this prevents an assertion failure from leaving an injected owner permanently blocked while fixture cleanup tries to join it.

The synchronization adapter removes the optional gate from its mutex **before** waiting. Otherwise a test trying to inspect or change the plan could deadlock on the same lock. Injection is enabled only for partition 0, so the other three owners can demonstrate independent cleanup. Outside the injected point it delegates to the same real file synchronization path.

```rust title="crates/broker-app/tests/lifecycle/support.rs"
#[derive(Default)]
struct Injection {
    gate: Mutex<Option<Gate>>,
    checkpoint_calls: AtomicUsize,
    fail_checkpoint_at: AtomicUsize,
}
struct TestSync { plan: Arc<Injection>, enabled: bool }
impl SyncIo for TestSync {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        let gate = if self.enabled { self.plan.gate.lock().unwrap().take() } else { None };
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            gate.release.recv().map_err(|_| io::Error::other("gate release disappeared"))?;
            match gate.after {
                AfterGate::Pass => {},
                AfterGate::Fail => return Err(io::Error::other("injected record synchronization failure")),
                AfterGate::Panic => panic!("injected owner panic"),
            }
        }
        FileSync.sync(base, file)
    }
    fn sync_checkpoint(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        if self.enabled {
            let call = self.plan.checkpoint_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.plan.fail_checkpoint_at.load(Ordering::SeqCst) {
                return Err(io::Error::other("injected final checkpoint failure"));
            }
        }
        file.sync()
    }
}

```

The checkpoint counter counts attempted checkpoint-file synchronizations after the test arms it. Failing attempt 2 means group 0 can already have published before group 1 fails. It does not mean the second record write failed, and it does not undo a prior authority replacement. This is a controlled error-return experiment, not a simulation of power-loss write ordering.

`Rig` deliberately declares the release guard **before** the supervisor. Rust drops struct fields in declaration order. If the fixture unwinds, it releases the test gate before the supervisor's blocking owner fallback can run. Its clients are dropped before the temporary directory is removed. There is no custom `Drop` on `Rig`, so individual client fields can also be explicitly moved/dropped before a restart test.

```rust title="crates/broker-app/tests/lifecycle/support.rs"
pub struct Rig {
    // Declaration order matters: release a gate before Supervisor's blocking Drop fallback.
    release: Option<Release>,
    pub supervisor: Supervisor,
    pub clients: Clients,
    pub root: Directory,
    plan: Arc<Injection>,
}
impl Rig {
    pub fn new() -> Self { Self::configured(owners(), SupervisorConfig::default()) }
    pub fn configured(config: OwnerConfig, supervisor: SupervisorConfig) -> Self {
        let root = Directory::new();
        let session = recovery::initialize(&root.0, StorageConfig::new(4 * 1024 * 1024, 2, 4).unwrap()).unwrap();
        let plan = Arc::new(Injection::default());
        let broker = LocalBroker::from_session(session, config, Arc::new(ManualClock::new(0)), |partition|
            TestSync { plan: Arc::clone(&plan), enabled: partition == p(0) }).unwrap();
        let clients = broker.clients();
        Self { release: None, supervisor: Supervisor::new(broker, supervisor).unwrap(), clients, root, plan }
    }
    pub fn gate(&mut self, after: AfterGate) -> oneshot::Receiver<()> {
        self.release();
        let (sender, receiver) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        *self.plan.gate.lock().unwrap() = Some(Gate { entered: sender, release: wait, after });
        self.release = Some(Release(Some(release)));
        receiver
    }
    pub fn release(&mut self) { self.release.take(); }
    pub fn fail_checkpoint(&self, call: usize) {
        self.plan.checkpoint_calls.store(0, Ordering::SeqCst);
        self.plan.fail_checkpoint_at.store(call, Ordering::SeqCst);
    }
    pub fn saved(&self, group: u8) -> u64 { checkpoint::read(&self.root.0, p(0), g(group)).unwrap().next.get() }
    pub fn stop_and_join(&mut self, runtime: &Runtime) -> ShutdownReport {
        self.supervisor.request_shutdown();
        let status = runtime.block_on(self.supervisor.drain_until(Instant::now() + Duration::from_secs(10))).unwrap();
        assert_eq!(status, DrainStatus::ReadyToJoin, "unfinished child: {:?}", self.supervisor.report());
        self.supervisor.join().unwrap()
    }
}
```

`stop_and_join` is for tests whose gate has already been released. Calling it while intentionally holding a gate would wait until the watchdog and then enter fallback cleanup; use the explicit observation-deadline tests for that case. The normal method verifies readiness and performs the real thread join outside `block_on`.

## State boundaries, full lanes, and final progress

Create `crates/broker-app/tests/lifecycle/state.rs`. The first checks cover validation and a broker stopped before it binds any socket. A clean `Starting` shutdown is important because startup may fail after owners exist but before serving begins.

```rust title="crates/broker-app/tests/lifecycle/state.rs"
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;
use broker_app::local::LocalBroker;
use broker_app::supervisor::{DrainStatus, Phase, SupervisorConfig};
use broker_core::mailbox::AdmissionError;
use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::segment::ReadLimits;
use broker_core::worker::OwnerExit;
use super::support::*;

#[test]
fn configuration_bounds_both_listeners_and_allows_immediate_network_stop() {
    let mut config = SupervisorConfig { network_grace: Duration::ZERO, ..SupervisorConfig::default() };
    assert!(config.validate().is_ok());
    config.capacity[0] = 0; assert!(config.validate().is_err());
    config.capacity = [64, 64]; assert!(config.validate().is_ok());
    config.capacity[1] = 65; assert!(config.validate().is_err());
    config.capacity = [1, 1]; config.network_grace = Duration::from_secs(61);
    assert!(config.validate().is_err());
}
#[test]
fn starting_can_drain_without_binding_a_listener() {
    let runtime = runtime(); let mut rig = Rig::new();
    assert_eq!(rig.supervisor.phase(), Phase::Starting);
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Stopped);
    assert_eq!(report.active_sessions, 0); assert_eq!(report.pending_owners, 0);
    assert!(report.owners.iter().all(|exit| matches!(exit,
        Some(OwnerExit::Stopped { checkpointed: true, .. }))));
    assert!(DirectoryGuard::acquire(&rig.root.0).is_ok());
}
```

The next pair separates prepared work from accepted work. A held preparation must return all of its resources after closure. Conversely, a gate holds the owner while data, query, and urgent lanes fill; shutdown still needs no spare slot. The first append has already copied into owner scratch before synchronization, so its append-pool block is available for the two queued appends. With capacity two, both query tickets and both urgent tickets fill their independent lanes.

```rust title="crates/broker-app/tests/lifecycle/state.rs"
#[test]
fn reservations_are_not_committed_work_and_cannot_submit_after_quiesce() {
    let runtime = runtime(); let mut rig = Rig::new();
    let append = rig.clients[0].try_prepare_append(3).unwrap();
    let fetch = rig.clients[0].try_prepare_lease(g(0), ReadLimits::new(1, 64).unwrap()).unwrap();
    rig.supervisor.request_shutdown();
    assert!(matches!(append.submit(), Err(AdmissionError::Closed)));
    assert!(matches!(fetch.submit(), Err(AdmissionError::Closed)));
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Stopped);
    assert_eq!(rig.clients[0].memory_state().append.checked_out_bytes, 0);
    assert_eq!(rig.clients[0].memory_state().response.checked_out_bytes, 0);
    assert_eq!(rig.clients[0].disk_state().reserved_bytes, 0);
}
#[test]
fn full_data_query_and_urgent_lanes_need_no_extra_finish_slot() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let first = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready));
    let second = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    let third = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    let queries = [rig.clients[0].try_snapshot().unwrap(), rig.clients[0].try_snapshot().unwrap()];
    let controls = [rig.clients[0].try_checkpoint(g(0)).unwrap(), rig.clients[0].try_checkpoint(g(1)).unwrap()];
    assert!(matches!(rig.clients[0].try_snapshot(), Err(AdmissionError::Full)));
    assert!(matches!(rig.clients[0].try_checkpoint(g(0)), Err(AdmissionError::Full)));
    rig.supervisor.request_shutdown();
    rig.release();
    runtime.block_on(async {
        assert_eq!(reply(first).await.get(), 0);
        assert_eq!(reply(second).await.get(), 1);
        assert_eq!(reply(third).await.get(), 2);
        for ticket in queries { reply(ticket).await; }
        for ticket in controls { reply(ticket).await; }
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
```

The append tickets must resolve to offsets 0, 1, and 2, even though shutdown began while the lanes were full. That assertion is about accepted append order, not an invented total ordering among all three lanes. The query and urgent replies are merely required to resolve under their normal semantics.

The following tests prove that closure wakes blocked capacity waiters and that cancelling a waiter does not steal buffers owned by someone else. Each waiter is first polled with a registered waker; the state count confirms it actually entered the wait path.

```rust title="crates/broker-app/tests/lifecycle/state.rs"
#[test]
fn append_admission_waiter_is_notified_and_observes_closed() {
    let runtime = runtime(); let mut rig = Rig::new();
    let client = rig.clients[0].clone();
    let held = [client.try_prepare_append(1).unwrap(), client.try_prepare_append(1).unwrap()];
    runtime.block_on(async {
        let wait = client.prepare_append(1, Instant::now() + Duration::from_secs(10));
        tokio::pin!(wait);
        let count = poll_with_counter(wait.as_mut());
        assert_eq!(client.memory_state().append_waiters, 1);
        rig.supervisor.request_shutdown();
        assert!(count.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(wait.await, Err(AdmissionError::Closed)));
    });
    drop(held); assert_eq!(client.memory_state().append_waiters, 0);
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn response_admission_waiter_is_notified_and_observes_closed() {
    let runtime = runtime(); let mut rig = Rig::new();
    let client = rig.clients[0].clone(); let limits = ReadLimits::new(1, 64).unwrap();
    let held = [client.try_prepare_lease(g(0), limits).unwrap(), client.try_prepare_lease(g(1), limits).unwrap()];
    runtime.block_on(async {
        let wait = client.prepare_lease(g(0), limits, Instant::now() + Duration::from_secs(10));
        tokio::pin!(wait);
        let count = poll_with_counter(wait.as_mut());
        assert_eq!(client.memory_state().response_waiters, 1);
        rig.supervisor.request_shutdown();
        assert!(count.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(wait.await, Err(AdmissionError::Closed)));
    });
    drop(held); assert_eq!(client.memory_state().response_waiters, 0);
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn cancelling_an_admission_future_releases_its_waiter_not_someone_elses_buffers() {
    let runtime = runtime(); let mut rig = Rig::new();
    let client = rig.clients[0].clone();
    let held = [client.try_prepare_append(1).unwrap(), client.try_prepare_append(1).unwrap()];
    runtime.block_on(async {
        {
            let wait = client.prepare_append(1, Instant::now() + Duration::from_secs(10));
            tokio::pin!(wait); pending_once(wait.as_mut()).await;
            assert_eq!(client.memory_state().append_waiters, 1);
        }
        assert_eq!(client.memory_state().append_waiters, 0);
        assert_eq!(client.memory_state().append.free_buffers, 0);
    });
    drop(held); assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
```

If one of these wake tests fails, do not add a short sleep. Inspect whether `finish` notifies the same `Notify` used by the admission facade and whether notification registration still precedes predicate inspection. A returned `Closed` after manually repolling alone would not prove the notification happened; that is why the counting waker is checked separately.

Now check what final checkpointing is allowed to save. A gap at offset 1 must prevent checkpointing completed offset 2. Two groups must restart at separate frontiers, not at the maximum of their combined work.

```rust title="crates/broker-app/tests/lifecycle/state.rs"
#[test]
fn final_checkpoint_stops_at_a_gap_and_does_not_complete_another_group() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        for payload in [b"cat", b"dog", b"owl"] { append(&rig.clients[0], payload).await; }
        let batch = fetch(&rig.clients[0], 0, 3).await;
        let tokens: Vec<_> = batch.records().map(|record| record.token).collect();
        ack(&rig.clients[0], tokens[2]).await; ack(&rig.clients[0], tokens[0]).await;
        assert_eq!(rig.saved(0), 0);
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
    assert_eq!(rig.saved(0), 1); assert_eq!(rig.saved(1), 0);
}
#[test]
fn both_groups_restart_at_their_independent_final_frontiers() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        for payload in [b"cat", b"dog"] { append(&rig.clients[0], payload).await; }
        for group in 0..2 {
            let batch = fetch(&rig.clients[0], group, 2).await;
            for token in batch.records().take(group as usize + 1).map(|record| record.token) {
                ack(&rig.clients[0], token).await;
            }
        }
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
    drop(rig.clients);
    let restarted = LocalBroker::open(&rig.root.0, false).unwrap();
    let clients = restarted.clients();
    runtime.block_on(async {
        assert_eq!(reply(clients[0].try_progress(g(0)).unwrap()).await.persisted_next.get(), 1);
        assert_eq!(reply(clients[0].try_progress(g(1)).unwrap()).await.persisted_next.get(), 2);
    });
    drop(clients); restarted.join().unwrap();
}
```

The restart case explicitly drops the old client generation before starting the new `LocalBroker`. It then reads the persisted values through the owner API, not only by inspecting a conveniently named file. The new startup epoch and empty lease windows remain the earlier recovery mechanism.

The final state checks distinguish storage ownership, payload ownership, and observation timeouts. A returned batch can outlive the file lock. A reached observation deadline cannot authorize a premature thread join.

```rust title="crates/broker-app/tests/lifecycle/state.rs"
#[test]
fn response_bytes_can_outlive_join_without_keeping_the_storage_lock() {
    let runtime = runtime(); let mut rig = Rig::new();
    let batch = runtime.block_on(async {
        append(&rig.clients[0], b"cat").await; fetch(&rig.clients[0], 0, 1).await
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
    assert_eq!(batch.records().next().unwrap().payload(), b"cat");
    assert!(rig.clients[0].memory_state().response.checked_out_bytes >= 4096);
    assert!(DirectoryGuard::acquire(&rig.root.0).is_ok());
    drop(batch);
    assert_eq!(rig.clients[0].memory_state().response.checked_out_bytes, 0);
    assert!(rig.clients[0].memory_state().response.allocated_bytes > 0);
}
#[test]
fn an_observation_timeout_is_pending_not_a_terminal_phase() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let ticket = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready)); rig.supervisor.request_shutdown();
    let outcome = runtime.block_on(rig.supervisor.drain_until(Instant::now())).unwrap();
    assert_eq!(outcome, DrainStatus::Pending); assert_eq!(rig.supervisor.phase(), Phase::Draining);
    assert_eq!(rig.supervisor.join().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
    rig.release(); runtime.block_on(reply(ticket));
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
```

The allocation assertion intentionally remains positive after the batch is dropped: the fixture still owns clients and therefore the reusable pool generation. Checked-out bytes returning to zero is not the same as allocator memory returning to zero. Likewise, an available directory lock proves exclusion ended, not that every old client has been destroyed.

## Fatal storage outcomes must still join the other owners

Create `crates/broker-app/tests/lifecycle/failures.rs`. These first tests establish a blocked owner, a failed owner whose reply was abandoned, and an unwinding owner panic. None relies on a network request to discover the failure.

```rust title="crates/broker-app/tests/lifecycle/failures.rs"
use std::future::pending;
use std::time::Duration;
use tokio::time::{Instant, timeout};
use broker_app::supervisor::{DrainStatus, Phase, StopCause};
use broker_core::mailbox::AdmissionError;
use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::recovery;
use broker_core::worker::{OwnerExit, RequestError};
use super::support::*;

#[test]
fn blocked_sync_keeps_the_lock_and_rejects_new_admission_after_quiesce() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let ticket = rig.clients[0].try_prepare_append(3).unwrap().submit().unwrap();
    runtime.block_on(entered(ready));
    rig.supervisor.request_shutdown();
    assert!(DirectoryGuard::acquire(&rig.root.0).is_err());
    assert!(recovery::recover(&rig.root.0).is_err());
    assert!(matches!(rig.clients[0].try_prepare_append(1), Err(AdmissionError::Closed)));
    assert!(matches!(rig.clients[0].try_checkpoint(g(0)), Err(AdmissionError::Closed)));
    rig.release(); runtime.block_on(reply(ticket));
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn an_owner_failure_is_detected_without_waiting_for_its_abandoned_reply() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Fail);
    drop(rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap());
    runtime.block_on(entered(ready)); rig.release();
    runtime.block_on(async {
        timeout(Duration::from_secs(10), rig.supervisor.serve_until(pending())).await.unwrap().unwrap();
    });
    assert_eq!(rig.supervisor.report().cause, Some(StopCause::OwnerExited));
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Failed);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Failed { .. })));
    assert_eq!(report.pending_owners, 0);
}
#[test]
fn owner_panic_is_fatal_but_the_other_three_owners_are_still_joined() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Panic);
    let ticket = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready)); rig.release();
    runtime.block_on(async {
        timeout(Duration::from_secs(10), rig.supervisor.serve_until(pending())).await.unwrap().unwrap();
        assert!(matches!(ticket.await.unwrap(), Err(RequestError::OwnerStopped)));
    });
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Failed);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Panicked)));
    assert!(report.owners[1..].iter().all(|notice| matches!(notice,
        Some(OwnerExit::Stopped { checkpointed: true, .. }))));
}
```

While the synchronization gate is held, the directory guard must still exclude recovery and new admission must already be closed. When the injected synchronization fails, the continuously driven supervisor sees the owner notice even though nobody awaits the abandoned reply. When the owner panics, its reply guard reports `OwnerStopped` and the final report identifies the panic; the three healthy owners must still have checkpointed exits.

An expected panic hook message does not by itself mean the test failed. Inspect the test result and the asserted notice. Conversely, compiling with abort-on-panic behavior would not satisfy an unwind-based panic test; this fixture uses the ordinary test profile and makes no claim about catching process abort.

The next tests check two non-rollback cases: a final checkpoint can fail after earlier appends succeeded, and a second group checkpoint can fail after the first was already published. The trigger remains `Requested` when a later storage failure occurs during a user-requested shutdown.

```rust title="crates/broker-app/tests/lifecycle/failures.rs"
#[test]
fn failed_final_checkpoint_does_not_claim_persisted_progress() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        append(&rig.clients[0], b"cat").await;
        let batch = fetch(&rig.clients[0], 0, 1).await;
        ack(&rig.clients[0], batch.records().next().unwrap().token).await;
    });
    rig.fail_checkpoint(1);
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Failed); assert_eq!(rig.saved(0), 0);
    assert!(report.first_failure.unwrap().contains("checkpoint"));
}
#[test]
fn later_checkpoint_failure_does_not_roll_back_an_earlier_group() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        append(&rig.clients[0], b"cat").await;
        for group in 0..2 {
            let batch = fetch(&rig.clients[0], group, 1).await;
            ack(&rig.clients[0], batch.records().next().unwrap().token).await;
        }
    });
    rig.fail_checkpoint(2);
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Failed);
    assert_eq!(rig.saved(0), 1); assert_eq!(rig.saved(1), 0);
}
#[test]
fn requested_shutdown_does_not_hide_a_later_sync_failure() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Fail);
    let ticket = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready));
    rig.supervisor.request_shutdown(); rig.release();
    runtime.block_on(async { assert!(matches!(ticket.await.unwrap(), Err(RequestError::Failed(_)))); });
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.cause, Some(StopCause::Requested)); assert_eq!(report.phase, Phase::Failed);
}
```

In the second checkpoint test, expected authoritative next offsets are 1 for group 0 and 0 for group 1. Reporting a failed shutdown is necessary, but pretending both files rolled back would be false. The recovery rules already validate whichever authorities were actually published.

Finish the file by checking that a successful accepted append survives an abandoned reply, and that cancelling a borrowed drain call does not destroy its owner handles. The recovered `cat` record is checked by payload bytes, not by a guessed offset count alone.

```rust title="crates/broker-app/tests/lifecycle/failures.rs"
#[test]
fn abandoning_a_ticket_does_not_remove_a_successfully_drained_append() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let mut request = rig.clients[0].try_prepare_append(3).unwrap();
    request.payload_mut().copy_from_slice(b"cat");
    drop(request.submit().unwrap()); runtime.block_on(entered(ready));
    rig.supervisor.request_shutdown(); rig.release();
    let report = rig.stop_and_join(&runtime);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Stopped { state, .. }) if state.visible_end.get() == 1));
    // Drop client-owned pool generations before creating a new service generation.
    drop(rig.clients);
    let session = recovery::recover(&rig.root.0).unwrap();
    let (_, mut logs) = session.into_parts();
    let batch = logs[0].read(broker_core::types::Offset::new(0), broker_core::types::Offset::new(1),
        broker_core::storage::segment::ReadLimits::new(1, 64).unwrap()).unwrap();
    assert_eq!(batch.records[0].payload(), b"cat");
}
#[test]
fn cancelling_a_borrowed_drain_keeps_its_owner_handles_for_resumption() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let ticket = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready)); rig.supervisor.request_shutdown();
    runtime.block_on(async {
        let drain = rig.supervisor.drain_until(Instant::now() + Duration::from_secs(10));
        tokio::pin!(drain); pending_once(drain.as_mut()).await;
        // The owning pinned future ends at this scope; the Supervisor does not.
    });
    assert_eq!(rig.supervisor.phase(), Phase::Draining);
    assert_eq!(runtime.block_on(rig.supervisor.drain_until(Instant::now())).unwrap(), DrainStatus::Pending);
    rig.release(); runtime.block_on(reply(ticket));
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
```

The cancellation test ends the scope that owns the pinned drain future. Its `&mut Supervisor` borrow is released, but the supervisor's `JoinSet`, thread handles, exit receivers, and original network deadline remain stored. An immediate subsequent observation is still pending because the synchronization gate, not a scheduling delay, is holding the owner.

## Real socket boundaries and partial frames

Create `crates/broker-app/tests/lifecycle/network.rs`. The small `drive` helper runs the serving loop concurrently with an activity and sends a stop request after that activity produces its result. Each live socket test binds loopback port 0, so it does not depend on fixed development ports being free.

```rust title="crates/broker-app/tests/lifecycle/network.rs"
use std::future::{Future, pending};
use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, timeout};
use broker_app::protocol::{self, Endpoint, Header, Op, Status};
use broker_app::server::{SessionLimits, session_until};
use broker_app::supervisor::{DrainStatus, Phase, StopCause, Supervisor, SupervisorConfig};
use broker_core::worker::OwnerExit;
use super::support::*;

fn append_header() -> Header {
    let mut header = Header::request(Op::Append, p(0), g(0)); header.body = 3; header
}
async fn drive<F: Future>(supervisor: &mut Supervisor, activity: F) -> F::Output {
    let (stop, requested) = oneshot::channel();
    let serving = supervisor.serve_until(async { requested.await.map_err(io::Error::other) });
    let activity = async { let value = activity.await; let _ = stop.send(()); value };
    let (served, value) = tokio::join!(serving, activity);
    served.unwrap(); value
}

```

The first two tests use a duplex byte stream for the exact partial-header boundary. They check that stop or loss of the stop sender ends the owning session and closes the stream. No append buffer should have been reserved while only twenty header bytes exist.

```rust title="crates/broker-app/tests/lifecycle/network.rs"
#[test]
fn partial_header_is_cancelled_only_with_its_socket() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        let (mut peer, stream) = tokio::io::duplex(128);
        peer.write_all(&append_header().encode()[..20]).await.unwrap();
        let (stop, changed) = watch::channel(false);
        let session = session_until(stream, Endpoint::Producer, rig.clients.clone(), SessionLimits::default(), changed);
        tokio::pin!(session); pending_once(session.as_mut()).await;
        stop.send_replace(true);
        timeout(Duration::from_secs(1), session).await.unwrap().unwrap();
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert_eq!(rig.clients[0].memory_state().append.checked_out_bytes, 0);
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn losing_the_session_stop_sender_closes_an_idle_session() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        let (mut peer, stream) = tokio::io::duplex(128);
        let (stop, changed) = watch::channel(false); drop(stop);
        session_until(stream, Endpoint::Consumer, rig.clients.clone(), SessionLimits::default(), changed).await.unwrap();
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
```

The next test is real TCP. It sends a valid header declaring three payload bytes but sends only one byte of body. Observing checked-out append capacity proves that admission occurred. It does **not** prove submission occurred; the missing bytes prevent that. Zero network grace then cancels and joins the session, returning its unsubmitted resources while the owner exits at visible end 0.

```rust title="crates/broker-app/tests/lifecycle/network.rs"
#[test]
fn forcing_a_partial_body_returns_its_unsubmitted_buffer_and_disk_credit() {
    let runtime = runtime();
    let mut rig = Rig::configured(owners(), SupervisorConfig { network_grace: Duration::ZERO,
        ..SupervisorConfig::default() });
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let client = rig.clients[0].clone();
    let _peer = runtime.block_on(drive(&mut rig.supervisor, async {
        let mut peer = TcpStream::connect(addresses[0]).await.unwrap();
        peer.write_all(&append_header().encode()).await.unwrap(); peer.write_all(b"c").await.unwrap();
        eventually(|| client.memory_state().append.checked_out_bytes > 0).await;
        peer
    }));
    let report = rig.stop_and_join(&runtime);
    assert!(report.network_forced); assert_eq!(report.sessions[0].cancelled, 1);
    assert_eq!(report.phase, Phase::Stopped);
    assert_eq!(client.memory_state().append.checked_out_bytes, 0);
    assert_eq!(client.disk_state().reserved_bytes, 0);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Stopped { state, .. }) if state.visible_end.get() == 0));
}
```

Now send the complete body and wait for the owner synchronization gate. The append has crossed submission. Quiescence is requested while the gate is held, then the test releases it and concurrently drains the service while reading the response. The success offset is permitted only after the real synchronization path returns. A peer that cooperates can receive this response during the grace period.

```rust title="crates/broker-app/tests/lifecycle/network.rs"
#[test]
fn accepted_append_can_finish_its_response_during_the_grace_period() {
    let runtime = runtime(); let mut rig = Rig::new();
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Pass);
    let mut peer = runtime.block_on(drive(&mut rig.supervisor, async {
        let mut peer = TcpStream::connect(addresses[0]).await.unwrap();
        peer.write_all(&append_header().encode()).await.unwrap(); peer.write_all(b"cat").await.unwrap();
        entered(ready).await; peer
    }));
    rig.release();
    runtime.block_on(async {
        let (drain, response) = tokio::join!(
            rig.supervisor.drain_until(Instant::now() + Duration::from_secs(10)),
            protocol::read_header(&mut peer, Instant::now() + Duration::from_secs(10)));
        assert_eq!(drain.unwrap(), DrainStatus::ReadyToJoin);
        let response = response.unwrap().unwrap(); assert_eq!(response.status, Status::Ok);
        assert_eq!(response.args[0], 0);
    });
    let report = rig.supervisor.join().unwrap();
    assert_eq!(report.phase, Phase::Stopped); assert!(!report.network_forced);
    assert_eq!(report.sessions[0].completed, 1);
}
```

Repeat that accepted-append case with zero network grace and keep the owner gate held. The session must be cancelled and joined without producing a false success. The owner still has accepted work; the test releases the gate only after observing no active session tasks. Repeated short observations do not prove completion by elapsed time—the task-count predicate proves which joins were collected, and a larger watchdog bounds a broken test.

```rust title="crates/broker-app/tests/lifecycle/network.rs"
#[test]
fn network_deadline_does_not_cancel_or_falsely_confirm_an_owner_append() {
    let runtime = runtime();
    let mut rig = Rig::configured(owners(), SupervisorConfig { network_grace: Duration::ZERO,
        ..SupervisorConfig::default() });
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Pass);
    let mut peer = runtime.block_on(drive(&mut rig.supervisor, async {
        let mut peer = TcpStream::connect(addresses[0]).await.unwrap();
        peer.write_all(&append_header().encode()).await.unwrap(); peer.write_all(b"cat").await.unwrap();
        entered(ready).await; peer
    }));
    runtime.block_on(async {
        timeout(Duration::from_secs(10), async {
            loop {
                let result = rig.supervisor.drain_until(Instant::now() + Duration::from_millis(20)).await.unwrap();
                assert_eq!(result, DrainStatus::Pending); // The owner is still held at the gate.
                if rig.supervisor.report().active_sessions == 0 { break; }
            }
        }).await.expect("cancelled session was not joined");
        let response = protocol::read_header(&mut peer, Instant::now() + Duration::from_secs(1)).await;
        assert!(!matches!(response, Ok(Some(header)) if header.status == Status::Ok));
    });
    assert_eq!(rig.supervisor.report().active_sessions, 0);
    assert_eq!(rig.supervisor.phase(), Phase::Draining);
    rig.release(); let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Stopped); assert!(report.network_forced);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Stopped { state, .. }) if state.visible_end.get() == 1));
}
```

Possible peer outcomes are EOF, reset, or a framing/read error; none can be `Status::Ok` while synchronization is held. After release, the owner's final visible end must be 1. This is the uncertainty that a client must carry after disconnection: lack of a reply does not prove lack of an append.

The next test cancels the borrowed serving future at the same entered-sync boundary. The task count must remain one and the supervisor must remain `Serving`; cancellation of the drive call is not itself a stop request. Explicit shutdown then finishes the retained children.

```rust title="crates/broker-app/tests/lifecycle/network.rs"
#[test]
fn cancelling_the_serving_borrow_does_not_drop_its_session_set() {
    let runtime = runtime(); let mut rig = Rig::new();
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Pass);
    let _peer = runtime.block_on(async {
        let serving = rig.supervisor.serve_until(pending()); tokio::pin!(serving);
        let activity = async {
            let mut peer = TcpStream::connect(addresses[0]).await.unwrap();
            peer.write_all(&append_header().encode()).await.unwrap(); peer.write_all(b"cat").await.unwrap();
            entered(ready).await; peer
        };
        tokio::select! {
            peer = activity => peer,
            result = &mut serving => panic!("serving ended before cancellation: {result:?}"),
        }
    });
    assert_eq!(rig.supervisor.phase(), Phase::Serving);
    assert_eq!(rig.supervisor.report().active_sessions, 1);
    rig.supervisor.request_shutdown(); rig.release();
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
```

Finish with startup failure, ordinary peer failure, and signal failure. Holding the consumer port makes the second bind fail after the first bind succeeded; no temporary listener or already started owner may be forgotten. A malformed peer only increments the session-error counter. A signal-registration error, in contrast, is a failed service outcome after cleanup.

```rust title="crates/broker-app/tests/lifecycle/network.rs"
#[test]
fn bind_failure_enters_cleanup_instead_of_leaking_started_owners() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        let held = TcpListener::bind(address()).await.unwrap();
        assert!(rig.supervisor.bind(address(), held.local_addr().unwrap()).await.is_err());
    });
    assert_eq!(rig.supervisor.phase(), Phase::Draining);
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Failed); assert_eq!(report.cause, Some(StopCause::StartupFailed));
    assert_eq!(report.active_sessions, 0); assert_eq!(report.pending_owners, 0);
}
#[test]
fn malformed_peer_is_a_session_error_not_an_owner_failure() {
    let runtime = runtime(); let mut rig = Rig::new();
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    runtime.block_on(drive(&mut rig.supervisor, async {
        let mut peer = TcpStream::connect(addresses[1]).await.unwrap();
        peer.write_all(&[0; 64]).await.unwrap();
        let _ = timeout(Duration::from_secs(2), peer.read(&mut [0; 1])).await.unwrap();
    }));
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Stopped); assert_eq!(report.sessions[1].session_errors, 1);
}
#[test]
fn signal_registration_error_is_failed_only_after_children_are_cleaned_up() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        rig.supervisor.bind(address(), address()).await.unwrap();
        rig.supervisor.serve_until(async { Err(io::Error::other("signal unavailable")) }).await.unwrap();
    });
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.cause, Some(StopCause::SignalFailed)); assert_eq!(report.phase, Phase::Failed);
    assert_eq!(report.pending_owners, 0); assert_eq!(report.active_sessions, 0);
}
```

## Task panic and accounting are supervisor-unit concerns

Create `crates/broker-app/src/supervisor/tests.rs`; the parent already declares it under `#[cfg(test)]`. These tests need private access to the task-ID map to inject a panicking or permanently pending task without adding a production “panic me” network operation. The synthetic task is otherwise tracked and joined exactly like a session.

```rust title="crates/broker-app/src/supervisor/tests.rs"
use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Drop for Directory { fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); } }
fn fixture() -> (Directory, Supervisor, tokio::runtime::Runtime) {
    let directory = Directory(std::env::temp_dir().join(format!("broker-supervisor-unit-{}-{}",
        std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))));
    let broker = LocalBroker::open(&directory.0, true).unwrap();
    let config = SupervisorConfig { capacity: [1, 1], network_grace: Duration::ZERO,
        ..SupervisorConfig::default() };
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    (directory, Supervisor::new(broker, config).unwrap(), runtime)
}
fn track(supervisor: &mut Supervisor, future: impl Future<Output = io::Result<()>> + Send + 'static) {
    let task = supervisor.tasks.spawn(future);
    supervisor.scopes.insert(task.id(), 0);
    supervisor.active[0] += 1;
    supervisor.stats[0].accepted += 1;
}
fn complete(supervisor: &mut Supervisor, runtime: &tokio::runtime::Runtime) -> ShutdownReport {
    supervisor.request_shutdown();
    let result = runtime.block_on(supervisor.drain_until(Instant::now() + Duration::from_secs(10))).unwrap();
    assert_eq!(result, DrainStatus::ReadyToJoin);
    supervisor.join().unwrap()
}
```

The fixture tuple binds the temporary-directory owner before the supervisor, so ordinary local-variable reverse drop order keeps the directory until after supervisor cleanup. The runtime remains live while the explicit drain runs. These unit fixtures use production pool defaults; limit test concurrency on the 16 GB machine rather than multiplying heavy configurations without purpose.

```rust title="crates/broker-app/src/supervisor/tests.rs"
#[test]
fn a_panicking_child_is_counted_and_causes_supervised_failure() {
    let (_directory, mut supervisor, runtime) = fixture();
    runtime.block_on(async {
        let address: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 0).into();
        supervisor.bind(address, address).await.unwrap();
        track(&mut supervisor, async { panic!("injected session panic") });
        tokio::time::timeout(Duration::from_secs(10), supervisor.serve_until(std::future::pending()))
            .await.unwrap().unwrap();
    });
    let report = complete(&mut supervisor, &runtime);
    assert_eq!(report.phase, Phase::Failed); assert_eq!(report.sessions[0].panicked, 1);
    assert_eq!(report.active_sessions, 0); assert!(supervisor.scopes.is_empty());
}
#[test]
fn forced_tasks_are_joined_and_counted_exactly_once() {
    let (_directory, mut supervisor, runtime) = fixture();
    runtime.block_on(async { track(&mut supervisor, std::future::pending()); });
    let report = complete(&mut supervisor, &runtime);
    assert_eq!(report.phase, Phase::Stopped); assert!(report.network_forced);
    assert_eq!(report.sessions[0].accepted, 1); assert_eq!(report.sessions[0].cancelled, 1);
    assert_eq!(supervisor.active, [0, 0]); assert!(supervisor.scopes.is_empty());
}
#[test]
fn repeated_shutdown_cannot_extend_the_original_network_deadline() {
    let (_directory, mut supervisor, runtime) = fixture();
    supervisor.request_shutdown();
    let recorded = Instant::now() + Duration::from_secs(50);
    supervisor.network_deadline = Some(recorded);
    supervisor.request_shutdown();
    assert_eq!(supervisor.network_deadline, Some(recorded));
    assert_eq!(supervisor.cause, Some(StopCause::Requested));
    assert_eq!(complete(&mut supervisor, &runtime).phase, Phase::Stopped);
}
```

The last test sets a known stored deadline inside the private unit fixture and verifies that a repeated stop request leaves it unchanged. There are no tasks to wait for, so the test does not spend fifty seconds sleeping. It tests idempotent state, not elapsed scheduler timing.

## Run the focused and cumulative checks

From the implementation workspace, after all files are assembled:

```bash
cargo check --workspace --all-targets
cargo test -p broker-app --test lifecycle -- --test-threads=4
cargo test -p broker-app --lib supervisor::tests -- --test-threads=4
cargo test --workspace --all-targets -- --test-threads=4
cargo fmt --all -- --check
```

Expected focused results are **28 passing lifecycle tests** and **three passing supervisor tests**. The final workspace command also reruns the prerequisite storage, lease, progress, retention, memory, transport, and language tests; **410 tests are supplied across the required workspace targets**. These are expected checks, **not observed execution results**. No Rust compiler, Cargo, or rustfmt was available in the authoring environment, so compilation, dependency resolution, actual test outcomes, loopback execution, and formatting remain to be verified locally. No `Cargo.lock` has been invented. A formatter difference is not a durability failure: run the formatter, inspect its changes, and rerun the checks; do not label the original text formatting-verified.

The panic tests may print the expected panic hook while still passing. A watchdog failure is a failed check, not permission to remove the test or silently raise every timeout. First identify the missing transition. The synchronous gate's release guard exists specifically to let a failed assertion unwind without abandoning a blocked owner.

After focused tests, run the `shutdown_probe` example and the two-terminal sequence in Part 3. An ordinary example run checks real synchronization and restart at the API level; a loopback run additionally checks the transport. Neither is a power-loss experiment or a proof of arbitrary filesystem behavior.

## Diagnose a failed boundary precisely

| Observation | Inspect first | Incorrect shortcut |
| --- | --- | --- |
| Admission waiter never wakes after stop | `finish` notification, Chapter 15 registration-before-check order, actual pending waiter count | Periodic polling that masks a missed notification |
| Shutdown hangs with full queues | Closure flag and `recv`'s empty committed-queue predicate | Enqueueing one more urgent stop command |
| `ReadyToJoin` appears but terminal state is absent | Call synchronous `join` outside `block_on`; inspect per-owner join results | Reporting success from an exit notice alone |
| Repeated unfinished reports with an entered sync gate | Release the controlled gate; inspect `pending_owners` | Detaching the owner or starting another over its files |
| Progress after restart is too high | Gap retirement, accepted acknowledgement result, checkpoint source frontier | Saving highest observed completion instead of contiguous next offset |
| Progress file advanced despite failed shutdown | Which group's publication succeeded before the failure | Assuming the multi-group loop rolled back |
| Response memory remains after joins | Retained batches and client pool owners; allocated versus checked-out capacities | Calling lock release proof of zero process memory |
| Pending network work remains after grace | JoinSet results, executor progress, cooperative yield points | Treating `abort_all` as a completed join |

## A required small variation: distinguish two simultaneous reasons

Use the existing `AfterGate::Fail` case and the `requested_shutdown_does_not_hide_a_later_sync_failure` test as the starting point. Move the explicit shutdown request to **after** the failed owner's notice is observed by `serve_until`. Predict the report before changing code: the cause should become `OwnerExited`, but the final phase remains `Failed` in either ordering. The other owners must still be joined, and no client success can be invented for the failed append.

This is an edit-and-restore exercise within the allocated test/debug time; it is not counted as a thirty-second new test or a new framework. Restore the canonical test after observing the alternative ordering. Explain in your own words why the first trigger is useful operational information but cannot decide whether cleanup was successful.

## Completion criteria

You can now follow ownership from an admission waiter through a prepared body, committed queue entry, synchronized append, pending reply, and joined shutdown. You can identify which cancellation returns resources and which only abandons observation. You have an explicit clean path, a fatal path that still joins every child, and an unfinished path that retains handles rather than claiming termination.

The service still does not promise exactly-once external effects, hard real-time cancellation, cross-partition shutdown atomicity, or safe automatic replacement while old resource generations remain referenced. The next chapter composes the existing storage and lifecycle seams into deterministic multi-step failure schedules; it does not replace these focused boundary tests.

