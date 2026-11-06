use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use broker_core::budget::DiskQuota;
use broker_core::delivery::{LeaseConfig, LeaseToken};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::{CheckpointPolicy, ProgressAck};
use broker_core::mailbox::AdmissionError;
use broker_core::storage::recovery::{self, StorageSession};
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::GroupId;
use broker_core::worker::{BatchConfig, Client, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}
fn accept(client: &Client, token: LeaseToken) -> Result<(), Box<dyn Error>> {
    match request(client.try_ack_progress(token.group, token))? {
        ProgressAck::Accepted(_) => Ok(()),
        other => Err(io::Error::other(format!("unexpected acknowledgement: {other:?}")).into()),
    }
}
fn owners(session: StorageSession) -> Result<Vec<WorkerHandle>, Box<dyn Error>> {
    let (epoch, logs) = session.into_parts();
    let mut result = Vec::new();
    for log in logs {
        result.push(WorkerHandle::start_with_progress(log, FileSync,
            BatchConfig::new(1, 4096, Duration::from_secs(1))?, 8, 8,
            LeaseConfig::new(epoch, 8, 10_000)?, Arc::new(ManualClock::new(0)),
            CheckpointPolicy::explicit_only())?);
    }
    Ok(result)
}
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 2 { return Err(io::Error::other("usage: retention_probe ABSENT_DIRECTORY").into()); }
    let root = Path::new(&args[1]);
    let running = owners(recovery::initialize_budgeted(root, StorageConfig::new(102, 2, 4)?,
        DiskQuota::new(236, 416)?)?)?;
    let client = running[0].client();
    for payload in [b"cat", b"dog", b"owl", b"fox"] { request(client.try_publish(payload.to_vec()))?; }
    let refusal = client.try_publish(b"yak".to_vec()).unwrap_err();
    assert_eq!(refusal.reason, AdmissionError::DiskFull);
    let disk = client.disk_state().unwrap();
    assert_eq!((disk.file_bytes, disk.reserved_bytes, disk.quota.data_bytes()), (204, 0, 236));
    println!("full: file_bytes={} reserved_bytes={} data_limit={}",
        disk.file_bytes, disk.reserved_bytes, disk.quota.data_bytes());
    let g0 = GroupId::new(0)?; let g1 = GroupId::new(1)?;
    let first = request(client.try_lease(g0, ReadLimits::new(4, 140)?))?;
    for record in &first.records { accept(&client, record.token)?; }
    request(client.try_checkpoint(g0))?;
    let other = request(client.try_lease(g1, ReadLimits::new(4, 140)?))?;
    for record in &other.records[..2] { accept(&client, record.token)?; }
    let blocked = request(client.try_retention())?;
    assert_eq!((blocked.floor.get(), blocked.removed_segments, blocked.rotated), (0, 0, false));
    let progress = request(client.try_progress(g1))?;
    assert_eq!((progress.volatile_next.get(), progress.persisted_next.get()), (2, 0));
    println!("group1: volatile=2 persisted=0; floor=0 removed=0");
    request(client.try_checkpoint(g1))?;
    let reclaimed = request(client.try_retention())?;
    assert_eq!((reclaimed.floor.get(), reclaimed.removed_bytes, reclaimed.disk.file_bytes), (2, 102, 102));
    assert!(!reclaimed.rotated);
    println!("checkpointed: floor=2 removed_bytes=102 file_bytes=102 active_preserved=true");
    assert_eq!(first.records[0].record.payload(), b"cat");
    println!("copied response after deletion: cat");
    for record in &other.records[2..] { accept(&client, record.token)?; }
    request(client.try_checkpoint(g1))?;
    let reclaimed = request(client.try_retention())?;
    assert!(reclaimed.rotated);
    assert_eq!((reclaimed.floor.get(), reclaimed.disk.file_bytes), (4, 32));
    println!("caught up: rotated=true floor=4 file_bytes=32");
    assert_eq!(request(client.try_publish(refusal.value))?.get(), 4);
    println!("producer resumed: offset=4");
    for owner in running { owner.join()?; } drop(client);
    let session = recovery::recover(root)?;
    let (epoch, logs) = session.into_parts();
    assert_eq!(logs[0].retained_floor().get(), 4);
    assert_eq!(logs[0].written_end().get(), 5);
    assert_eq!(logs[0].disk_state().unwrap().file_bytes, 67);
    println!("restart: epoch={} floor=4 written_end=5 file_bytes=67", epoch.get());
    drop(logs);
    Ok(())
}
