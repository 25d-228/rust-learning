use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broker_core::budget::DiskQuota;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::io::StorageFile;
use broker_core::storage::{checkpoint, retention};
use broker_core::storage::log::segment_name;
use broker_core::storage::metadata::PublicationStep;
use broker_core::storage::recovery::{self, StorageSession};
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::SyncIo;
use broker_core::types::{GroupId, Offset, PartitionId};
use broker_core::worker::{BatchConfig, Client, RequestError, Ticket, WorkerHandle};
use crate::common::TestDir;

pub const WINDOW: usize = 4;
pub const LEASE_MS: u64 = 10;
pub const QUOTA: u64 = 512;
pub fn p() -> PartitionId { PartitionId::new(0).unwrap() }
pub fn g(index: usize) -> GroupId { GroupId::new(index as u8).unwrap() }
pub fn config() -> StorageConfig { StorageConfig::new(112, 2, 16).unwrap() }
pub fn segment(root: &Path, base: u64) -> PathBuf {
    root.join("partition-0").join(segment_name(Offset::new(base)))
}
pub fn setup() -> (TestDir, PathBuf, StorageSession) {
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("broker");
    let session = recovery::initialize_budgeted(&root, config(),
        DiskQuota::new(QUOTA, 416).unwrap()).unwrap();
    (directory, root, session)
}
pub fn start<S: SyncIo + Send + 'static>(session: StorageSession, io: S,
    clock: Arc<ManualClock>) -> (WorkerHandle, Client)
{
    let (epoch, mut logs) = session.into_parts();
    let log = logs.remove(0);
    drop(logs); // Partition zero's log still retains the whole-directory guard.
    let owner = WorkerHandle::start_with_progress(log, io,
        BatchConfig::new(1, 4096, Duration::from_secs(60)).unwrap(), 8, 8,
        LeaseConfig::new(epoch, WINDOW, LEASE_MS).unwrap(), clock,
        CheckpointPolicy::explicit_only()).unwrap();
    let client = owner.client();
    (owner, client)
}
pub fn receive<T>(ticket: Ticket<T>) -> Result<T, RequestError> {
    ticket.recv_timeout(Duration::from_secs(5)).expect("schedule reply watchdog")
}
pub fn get<T, E: Debug>(request: Result<Ticket<T>, E>) -> T {
    receive(request.unwrap()).unwrap()
}
pub fn payload(id: u64) -> [u8; 8] { id.to_be_bytes() }
pub fn record_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    for partition in 0..4 {
        for entry in fs::read_dir(root.join(format!("partition-{partition}"))).unwrap() {
            let path = entry.unwrap().path();
            files.insert(path.strip_prefix(root).unwrap().to_path_buf(), fs::read(path).unwrap());
        }
    }
    files
}

pub struct Trace {
    seed: u64,
    case: &'static str,
    next: usize,
    tail: VecDeque<String>,
}
impl Trace {
    pub fn new(seed: u64, case: &'static str) -> Self {
        Self { seed, case, next: 0, tail: VecDeque::new() }
    }
    pub fn step(&mut self, description: impl Into<String>) {
        if self.tail.len() == 24 { self.tail.pop_front(); }
        self.tail.push_back(format!("{:03}: {}", self.next, description.into()));
        self.next += 1;
    }
}
impl Drop for Trace {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let mut output = io::stderr().lock();
            let _ = writeln!(output, "schedule case={} seed=0x{:016x}; last {} transitions:",
                self.case, self.seed, self.tail.len());
            for line in &self.tail { let _ = writeln!(output, "  {line}"); }
        }
    }
}

pub struct Generator(pub u64);
impl Generator {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }
    pub fn pick(&mut self, bound: usize) -> usize {
        assert!(bound > 0);
        ((self.next() >> 32) % bound as u64) as usize
    }
}
pub fn selected_seeds() -> Vec<u64> {
    match std::env::var("BROKER_SCHEDULE_SEED") {
        Ok(text) => {
            let seed = if let Some(hex) = text.strip_prefix("0x") {
                u64::from_str_radix(hex, 16).expect("hexadecimal BROKER_SCHEDULE_SEED")
            } else { text.parse().expect("decimal BROKER_SCHEDULE_SEED") };
            vec![seed]
        }
        Err(std::env::VarError::NotPresent) => vec![0, 1, 7, 0x5eed, 0xdead_beef, u64::MAX],
        Err(error) => panic!("BROKER_SCHEDULE_SEED: {error}"),
    }
}

#[derive(Debug, Clone)]
pub enum Cut {
    SyncBefore(usize),
    Publication { name: String, step: PublicationStep },
    UnlinkAfter(usize),
}
#[derive(Debug)]
enum Point {
    Sync(u64),
    Publication(String, PublicationStep),
    Unlink(u64),
}
#[derive(Default)]
struct FaultState {
    cut: Option<Cut>,
    syncs: usize,
    unlinks: usize,
    hit: bool,
    events: Vec<String>,
}
#[derive(Clone, Default)]
pub struct FaultIo(Arc<Mutex<FaultState>>);
impl FaultIo {
    pub fn arm(&self, cut: Cut) {
        *self.0.lock().unwrap() = FaultState { cut: Some(cut), ..FaultState::default() };
    }
    pub fn hit(&self) -> bool { self.0.lock().unwrap().hit }
    pub fn events(&self) -> Vec<String> { self.0.lock().unwrap().events.clone() }
    fn observe(&self, point: Point) -> io::Result<()> {
        let mut state = self.0.lock().unwrap();
        match &point {
            Point::Sync(base) => { state.syncs += 1; let _ = base; }
            Point::Unlink(base) => { state.unlinks += 1; let _ = base; }
            Point::Publication(_, _) => {},
        }
        if state.events.len() < 128 { state.events.push(format!("{point:?}")); }
        let hit = match (&state.cut, &point) {
            (Some(Cut::SyncBefore(n)), Point::Sync(_)) => state.syncs == *n,
            (Some(Cut::UnlinkAfter(n)), Point::Unlink(_)) => state.unlinks == *n,
            (Some(Cut::Publication { name, step }), Point::Publication(actual, at)) =>
                name == actual && step == at,
            _ => false,
        };
        if hit {
            state.hit = true;
            state.cut = None;
            Err(io::Error::other("scheduled boundary failure"))
        } else { Ok(()) }
    }
}
impl SyncIo for FaultIo {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(Point::Sync(base.get()))?;
        file.sync()
    }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        let name = target.file_name().unwrap().to_string_lossy().into_owned();
        self.observe(Point::Publication(name, step))
    }
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        let parent = path.parent().unwrap();
        assert_eq!(parent.file_name().unwrap().to_str().unwrap(), "partition-0");
        let root = parent.parent().unwrap();
        let base: u64 = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
        let floor = retention::read(root, p()).unwrap().floor.get();
        let safe = (0..2).map(|index| checkpoint::read(root, p(), g(index)).unwrap().next.get())
            .min().unwrap();
        if !(base < floor && floor <= safe) {
            return Err(io::Error::other("attempted unlink without safe published authority"));
        }
        fs::remove_file(path)?;
        self.observe(Point::Unlink(base))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct View {
    pub epoch: u64,
    pub floor: u64,
    pub end: u64,
    pub fronts: [u64; 2],
    pub payloads: Vec<Vec<u8>>,
    pub repairs: usize,
}
pub fn recovered_view(root: &Path) -> View {
    let session = recovery::recover(root).unwrap();
    let repairs = session.repairs().len();
    let (epoch, mut logs) = session.into_parts();
    let log = &mut logs[0];
    let floor = log.retained_floor();
    let end = log.written_end();
    let batch = log.read(floor, end,
        broker_core::storage::segment::ReadLimits::new(1024, 4096).unwrap()).unwrap();
    assert_eq!(batch.next, end);
    assert_eq!(batch.records.len() as u64, end.get() - floor.get());
    let view = View { epoch: epoch.get(), floor: floor.get(), end: end.get(), repairs,
        fronts: std::array::from_fn(|index| checkpoint::read(root, p(), g(index)).unwrap().next.get()),
        payloads: batch.records.iter().map(|record| record.payload().to_vec()).collect() };
    drop(logs);
    view
}
