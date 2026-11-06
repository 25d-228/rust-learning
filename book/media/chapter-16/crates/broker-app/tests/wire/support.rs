use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, AtomicUsize, Ordering}, mpsc};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, oneshot};

use broker_app::protocol::{Header, Op};
use broker_app::server::Clients;
use broker_core::budget::MemoryConfig;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::storage::recovery;
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{GroupId, Offset, PartitionId};
use broker_core::worker::{AsyncTicket, BatchConfig, BoundedClient, BoundedHandle};

pub fn p(value: u16) -> PartitionId { PartitionId::new(value).unwrap() }
pub fn g(value: u8) -> GroupId { GroupId::new(value).unwrap() }
pub fn request(op: Op) -> Header { Header::request(op, p(0), g(0)) }
pub fn append_frame(bytes: &[u8]) -> Vec<u8> {
    let mut h = request(Op::Append); h.body = bytes.len() as u32;
    let mut frame = h.encode().to_vec(); frame.extend_from_slice(bytes); frame
}
pub async fn reply<T>(ticket: AsyncTicket<T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), ticket).await.expect("owner watchdog")
        .expect("reply channel").expect("owner result")
}
pub async fn append(client: &BoundedClient, bytes: &[u8]) -> Offset {
    let mut prepared = client.try_prepare_append(bytes.len()).unwrap();
    prepared.payload_mut().copy_from_slice(bytes);
    reply(prepared.submit().unwrap()).await
}

static NEXT: AtomicU64 = AtomicU64::new(0);
pub struct Fixture {
    pub clients: Clients,
    pub clock: Arc<ManualClock>,
    owners: Vec<BoundedHandle>,
    root: PathBuf,
}
impl Fixture {
    pub fn new() -> Self { Self::with_sync(FileSync) }
    pub fn with_sync<S: SyncIo + Send + 'static>(first_sync: S) -> Self {
        let root = std::env::temp_dir().join(format!("broker-wire-{}-{}", std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)));
        let (epoch, logs) = recovery::initialize(&root, StorageConfig::new(4 * 1024 * 1024, 2, 4).unwrap())
            .unwrap().into_parts();
        let clock = Arc::new(ManualClock::new(0));
        let mut owners = Vec::new();
        let mut first_sync = Some(first_sync);
        for (index, log) in logs.into_iter().enumerate() {
            let batch = BatchConfig::new(1, 4096, Duration::from_millis(10)).unwrap();
            let leases = LeaseConfig::new(epoch, 64, 10_000).unwrap();
            let memory = MemoryConfig::new(1024, 2, 4096, 2, 2).unwrap();
            let owner = if index == 0 {
                BoundedHandle::start(log, first_sync.take().unwrap(), batch, [2, 2, 4], leases,
                    clock.clone(), CheckpointPolicy::explicit_only(), memory)
            } else {
                BoundedHandle::start(log, FileSync, batch, [2, 2, 4], leases,
                    clock.clone(), CheckpointPolicy::explicit_only(), memory)
            }.unwrap();
            owners.push(owner);
        }
        let clients = Arc::new(std::array::from_fn(|i| owners[i].client()));
        Self { clients, clock, owners, root }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for owner in &self.owners { owner.close(); }
        for owner in self.owners.drain(..) { let _ = owner.join(); }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

pub struct GateSync { entered: Option<oneshot::Sender<()>>, release: mpsc::Receiver<()> }
pub struct GateRelease(Option<mpsc::Sender<()>>);
impl GateRelease {
    pub fn release(&mut self) { if let Some(sender) = self.0.take() { let _ = sender.send(()); } }
}
impl Drop for GateRelease { fn drop(&mut self) { self.release(); } }
pub fn sync_gate() -> (GateSync, oneshot::Receiver<()>, GateRelease) {
    let (sender, entered) = oneshot::channel();
    let (release, receiver) = mpsc::channel();
    (GateSync { entered: Some(sender), release: receiver }, entered, GateRelease(Some(release)))
}
impl SyncIo for GateSync {
    fn sync(&mut self, base: Offset, file: &mut dyn broker_core::io::StorageFile) -> io::Result<()> {
        if let Some(sender) = self.entered.take() {
            let _ = sender.send(());
            self.release.recv().map_err(|_| io::Error::other("sync gate closed"))?;
        }
        FileSync.sync(base, file)
    }
}

#[derive(Clone)]
pub struct Capture {
    pub output: Arc<Mutex<Vec<u8>>>,
    pub read_bytes: Arc<AtomicUsize>,
    pub blocked: Arc<Notify>,
}
pub struct ScriptIo {
    input: Vec<u8>,
    position: usize,
    cut: usize,
    max_write: usize,
    fail_write: bool,
    block_after: Option<usize>,
    written: usize,
    capture: Capture,
}
impl ScriptIo {
    pub fn new(input: Vec<u8>, cut: usize) -> (Self, Capture) {
        let capture = Capture { output: Arc::new(Mutex::new(Vec::new())),
            read_bytes: Arc::new(AtomicUsize::new(0)), blocked: Arc::new(Notify::new()) };
        (Self { input, position: 0, cut, max_write: usize::MAX, fail_write: false,
            block_after: None, written: 0, capture: capture.clone() }, capture)
    }
    pub fn short_writes(mut self) -> Self { self.max_write = 1; self }
    pub fn fail_writes(mut self) -> Self { self.fail_write = true; self }
    pub fn block_writes_after(mut self, bytes: usize) -> Self { self.block_after = Some(bytes); self }
}
impl AsyncRead for ScriptIo {
    fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, buffer: &mut ReadBuf<'_>)
        -> Poll<io::Result<()>>
    {
        let end = if self.position < self.cut { self.cut.min(self.input.len()) } else { self.input.len() };
        let count = (end - self.position).min(buffer.remaining());
        buffer.put_slice(&self.input[self.position..self.position + count]);
        self.position += count;
        self.capture.read_bytes.fetch_add(count, Ordering::Relaxed);
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for ScriptIo {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        if self.fail_write { return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "peer left"))); }
        let remaining = self.block_after.map_or(usize::MAX, |end| end.saturating_sub(self.written));
        if remaining == 0 {
            self.capture.blocked.notify_one();
            return Poll::Pending;
        }
        let count = bytes.len().min(self.max_write).min(remaining);
        self.capture.output.lock().unwrap().extend_from_slice(&bytes[..count]);
        self.written += count;
        Poll::Ready(Ok(count))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
}
pub fn captured_header(capture: &Capture) -> Header {
    let output = capture.output.lock().unwrap();
    Header::decode(output[..64].try_into().unwrap()).unwrap()
}
