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
