use std::error::Error;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use broker_core::budget::DiskQuota;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::{CheckpointPolicy, ProgressAck};
use broker_core::io::StorageFile;
use broker_core::storage::metadata::PublicationStep;
use broker_core::storage::recovery::{self, StorageSession};
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{GroupId, Offset};
use broker_core::worker::{BatchConfig, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(30))??)
}
fn start<S: SyncIo + Send + 'static>(session: StorageSession, sync: S)
    -> Result<WorkerHandle, Box<dyn Error>>
{
    let (epoch, mut logs) = session.into_parts();
    let log = logs.remove(0); drop(logs);
    Ok(WorkerHandle::start_with_progress(log, sync,
        BatchConfig::new(1, 4096, Duration::from_secs(1))?, 8, 8,
        LeaseConfig::new(epoch, 8, 10000)?, Arc::new(ManualClock::new(0)),
        CheckpointPolicy::explicit_only())?)
}
struct Gate { phase: String, removed: usize }
impl Gate {
    fn reached(&self, phase: &str) -> io::Result<()> {
        if self.phase == phase {
            println!("READY {phase}"); io::stdout().flush()?;
            // The parent kills the process. A timeout is a failed harness, not permission to continue.
            loop { std::thread::park(); }
        }
        Ok(())
    }
}
impl SyncIo for Gate {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()?;
        if base.get() == 4 { self.reached("rotate")?; }
        Ok(())
    }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        if target.file_name() == Some(OsStr::new("retention-p0.floor")) {
            self.reached(match step {
                PublicationStep::TemporaryCreated => "created",
                PublicationStep::TemporaryWritten => "written",
                PublicationStep::TemporarySynced => "synced",
                PublicationStep::Renamed => "renamed",
                PublicationStep::DirectorySynced => "directory",
            })?;
        }
        Ok(())
    }
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)?; self.removed += 1;
        self.reached(&format!("unlink-{}", self.removed))
    }
    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()?;
        if self.removed > 0 { self.reached("cleanup")?; }
        Ok(())
    }
}
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() < 3 { return Err(io::Error::other("usage: retention_crash_probe cut ROOT PHASE | verify ROOT").into()); }
    let root = Path::new(&args[2]);
    match args[1].to_str() {
        Some("cut") if args.len() == 4 => {
            let phase = args[3].to_str().ok_or_else(|| io::Error::other("phase must be UTF-8"))?;
            if !["rotate", "created", "written", "synced", "renamed", "directory", "unlink-1", "unlink-2", "cleanup"].contains(&phase) {
                return Err(io::Error::other("unknown retention gate").into());
            }
            let session = recovery::initialize_budgeted(root, StorageConfig::new(102, 2, 4)?, DiskQuota::new(236, 416)?)?;
            let owner = start(session, Gate { phase: phase.to_owned(), removed: 0 })?;
            let client = owner.client();
            for _ in 0..4 { request(client.try_publish(b"cat".to_vec()))?; }
            for group in [GroupId::new(0)?, GroupId::new(1)?] {
                let batch = request(client.try_lease(group, ReadLimits::new(4, 140)?))?;
                for record in batch.records {
                    assert!(matches!(request(client.try_ack_progress(group, record.token))?, ProgressAck::Accepted(_)));
                }
                request(client.try_checkpoint(group))?;
            }
            request(client.try_retention())?;
            return Err(io::Error::other("retention finished without entering its gate").into());
        }
        Some("verify") if args.len() == 3 => {
            let session = recovery::recover(root)?;
            let epoch = session.epoch();
            let owner = start(session, FileSync)?; let client = owner.client();
            let state = client.disk_state().unwrap();
            let value = broker_core::storage::retention::read(root, broker_core::types::PartitionId::new(0)?)?;
            println!("RECOVERED {} {} {}", epoch.get(), value.floor.get(), state.file_bytes);
            let retained = request(client.try_retention())?;
            assert_eq!((retained.floor.get(), retained.disk.file_bytes), (4, 32));
            println!("RECLAIMED 4 32"); owner.join()?;
        }
        _ => return Err(io::Error::other("invalid mode or argument count").into()),
    }
    Ok(())
}
