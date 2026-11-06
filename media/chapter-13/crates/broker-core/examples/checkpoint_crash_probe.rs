use std::error::Error;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}
fn phase(name: &str) -> Result<PublicationStep, io::Error> {
    match name {
        "created" => Ok(PublicationStep::TemporaryCreated),
        "written" => Ok(PublicationStep::TemporaryWritten),
        "synced" => Ok(PublicationStep::TemporarySynced),
        "renamed" => Ok(PublicationStep::Renamed),
        "directory" => Ok(PublicationStep::DirectorySynced),
        _ => Err(io::Error::other("unknown checkpoint phase")),
    }
}
struct Pause { phase: PublicationStep, label: String }
impl SyncIo for Pause {
    fn sync(&mut self, _: Offset, file: &mut dyn StorageFile) -> io::Result<()> { file.sync() }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        if target.file_name() == Some(OsStr::new("progress-p0-g0.ckp")) && step == self.phase {
            println!("READY {}", self.label);
            io::stdout().flush()?;
            // The parent kills this process. Spurious unparks cannot release the gate.
            loop { std::thread::park(); }
        }
        Ok(())
    }
}
fn start<S: SyncIo + Send + 'static>(session: StorageSession, sync: S) -> Result<WorkerHandle, Box<dyn Error>> {
    let (epoch, mut logs) = session.into_parts();
    let log = logs.remove(0); drop(logs);
    Ok(WorkerHandle::start_with_progress(log, sync,
        BatchConfig::new(1, 1000, Duration::from_secs(1))?, 8, 8,
        LeaseConfig::new(epoch, 4, 10_000)?, Arc::new(ManualClock::new(0)), CheckpointPolicy::explicit_only())?)
}
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() < 3 {
        return Err(io::Error::other("usage: checkpoint_crash_probe cut DIRECTORY PHASE | verify DIRECTORY").into());
    }
    let root = Path::new(&args[2]);
    let group = GroupId::new(0)?;
    if args[1] == "cut" && args.len() == 4 {
        let label = args[3].to_str().ok_or_else(|| io::Error::other("phase is not UTF-8"))?;
        let session = recovery::initialize(root, StorageConfig::new(102, 2, 16)?)?;
        let owner = start(session, Pause { phase: phase(label)?, label: label.to_owned() })?;
        let client = owner.client();
        for payload in [b"cat", b"dog", b"owl", b"fox"] { request(client.try_publish(payload.to_vec()))?; }
        let batch = request(client.try_lease(group, ReadLimits::new(4, 140)?))?;
        for record in &batch.records[..2] {
            assert!(matches!(request(client.try_ack_progress(group, record.token))?, ProgressAck::Accepted(_)));
        }
        let state = request(client.try_progress(group))?;
        assert_eq!((state.volatile_next.get(), state.persisted_next.get()), (2, 0));
        let ticket = client.try_checkpoint(group).map_err(|e| io::Error::other(format!("admission: {e:?}")))?;
        // This cannot complete: the owner stops at the selected publication phase.
        let result = ticket.recv();
        return Err(io::Error::other(format!("checkpoint escaped its gate: {result:?}")).into());
    }
    if args[1] == "verify" && args.len() == 3 {
        let owner = start(recovery::recover(root)?, FileSync)?;
        let client = owner.client();
        let state = request(client.try_progress(group))?;
        let batch = request(client.try_lease(group, ReadLimits::new(4, 140)?))?;
        let first = batch.records.first().ok_or_else(|| io::Error::other("missing repeat interval"))?.token.offset;
        assert_eq!(first, state.persisted_next);
        assert_eq!(request(client.try_progress(GroupId::new(1)?))?.persisted_next.get(), 0);
        println!("RECOVERED {} {} {}", state.epoch.get(), state.persisted_next.get(), first.get());
        owner.join()?;
        return Ok(());
    }
    Err(io::Error::other("invalid mode or arguments").into())
}
