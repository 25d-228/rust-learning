use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use broker_core::delivery::{AckOutcome, AckRejection, LeaseConfig, LeaseToken};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::{AckAccepted, CheckpointPolicy, ProgressAck};
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::GroupId;
use broker_core::worker::{BatchConfig, Client, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}
fn accept(client: &Client, token: LeaseToken) -> Result<AckAccepted, Box<dyn Error>> {
    match request(client.try_ack_progress(token.group, token))? {
        ProgressAck::Accepted(value) => Ok(value),
        other => Err(io::Error::other(format!("expected accepted completion, got {other:?}")).into()),
    }
}
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 2 { return Err(io::Error::other("usage: progress_probe ABSENT_DIRECTORY").into()); }
    let root = Path::new(&args[1]);
    let (epoch, logs) = recovery::initialize(root, StorageConfig::new(102, 2, 16)?)?.into_parts();
    let batch = BatchConfig::new(1, 1000, Duration::from_secs(1))?;
    let mut owners = Vec::new();
    for log in logs {
        owners.push(WorkerHandle::start_with_progress(log, FileSync, batch, 8, 8,
            LeaseConfig::new(epoch, 4, 10_000)?, Arc::new(ManualClock::new(0)), CheckpointPolicy::explicit_only())?);
    }
    println!("epoch={} partitions={} policy=explicit", epoch.get(), owners.len());
    let client = owners[0].client();
    let g0 = GroupId::new(0)?; let g1 = GroupId::new(1)?;
    for payload in [b"cat", b"dog", b"owl", b"fox"] { request(client.try_publish(payload.to_vec()))?; }
    let first = request(client.try_lease(g0, ReadLimits::new(4, 140)?))?;
    for index in [2usize, 0] {
        let a = accept(&client, first.records[index].token)?;
        println!("ack={} retired={} volatile={} persisted={}", index, a.ack.retired,
            a.progress.volatile_next.get(), a.progress.persisted_next.get());
    }
    let cp = request(client.try_checkpoint(g0))?;
    assert_eq!(cp.progress.persisted_next.get(), 1);
    println!("checkpoint group=0 persisted={} published={}", cp.progress.persisted_next.get(), cp.published);
    let a = accept(&client, first.records[1].token)?;
    assert_eq!((a.progress.volatile_next.get(), a.progress.persisted_next.get()), (3, 1));
    println!("ack=1 retired={} volatile={} persisted={}", a.ack.retired,
        a.progress.volatile_next.get(), a.progress.persisted_next.get());
    let other = request(client.try_lease(g1, ReadLimits::new(1, 35)?))?;
    let a = accept(&client, other.records[0].token)?;
    println!("group=1 volatile={} persisted={}", a.progress.volatile_next.get(), a.progress.persisted_next.get());
    let old = first.records[1].token;
    for owner in owners { owner.join()?; }
    drop(client);

    let (epoch, mut logs) = recovery::recover(root)?.into_parts();
    let owner = WorkerHandle::start_with_progress(logs.remove(0), FileSync, batch, 8, 8,
        LeaseConfig::new(epoch, 4, 10_000)?, Arc::new(ManualClock::new(0)), CheckpointPolicy::explicit_only())?;
    drop(logs);
    let client = owner.client();
    for group in [g0, g1] {
        let s = request(client.try_group_state(group))?;
        println!("recovered epoch={} group={} completed={} issued={} occupied={}", epoch.get(), group.get(),
            s.window.completed_end.get(), s.window.issued_end.get(), s.window.occupied);
    }
    let repeated = request(client.try_lease(g0, ReadLimits::new(4, 140)?))?;
    let offsets: Vec<_> = repeated.records.iter().map(|r| r.token.offset.get()).collect();
    assert_eq!(offsets, vec![1, 2, 3]);
    println!("group=0 repeated={offsets:?}");
    let new = repeated.records[0].token;
    assert_eq!((new.offset, new.generation, new.deadline_ms), (old.offset, old.generation, old.deadline_ms));
    match request(client.try_ack_progress(g0, old))? {
        ProgressAck::NotApplied { ack, .. } => {
            assert_eq!(ack.outcome, AckOutcome::Rejected(AckRejection::WrongEpoch));
            println!("old_token={:?}", ack.outcome);
        }
        other => return Err(io::Error::other(format!("old token was not fenced: {other:?}")).into()),
    }
    for record in repeated.records { accept(&client, record.token)?; }
    let cp = request(client.try_checkpoint(g0))?;
    assert_eq!(cp.progress.persisted_next.get(), 4);
    println!("checkpoint group=0 persisted={}", cp.progress.persisted_next.get());
    owner.join()?;
    Ok(())
}
