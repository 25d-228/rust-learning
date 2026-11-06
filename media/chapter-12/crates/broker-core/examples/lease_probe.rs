use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use broker_core::delivery::{AckOutcome, AckRejection, LeaseBatch, LeaseConfig};
use broker_core::delivery::clock::ManualClock;
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::GroupId;
use broker_core::worker::{BatchConfig, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}
fn describe(batch: &LeaseBatch) -> Vec<(u64, u64, u64)> {
    batch.records.iter().map(|item| (item.token.offset.get(),
        item.token.generation, item.token.deadline_ms)).collect()
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 2 {
        return Err(io::Error::other("usage: lease_probe ABSENT_DIRECTORY").into());
    }
    let root = Path::new(&args[1]);
    let (epoch, logs) = recovery::initialize(root, StorageConfig::new(102, 2, 16)?)?.into_parts();
    let batch = BatchConfig::new(1, 1000, Duration::from_secs(1))?;
    let clock = Arc::new(ManualClock::new(0));
    let mut owners = Vec::new();
    for log in logs {
        owners.push(WorkerHandle::start_with_leases(log, FileSync, batch, 8, 8,
            LeaseConfig::new(epoch, 3, 10)?, clock.clone())?);
    }
    let client = owners[0].client();
    let group0 = GroupId::new(0)?;
    let group1 = GroupId::new(1)?;
    for payload in [b"cat", b"dog", b"owl", b"fox"] {
        request(client.try_publish(payload.to_vec()))?;
    }
    println!("epoch={} partitions={} clock=0ms", epoch.get(), owners.len());
    let first = request(client.try_lease(group0, ReadLimits::new(3, 105)?))?;
    let other = request(client.try_lease(group1, ReadLimits::new(1, 35)?))?;
    assert_eq!(describe(&first), vec![(0, 1, 10), (1, 1, 10), (2, 1, 10)]);
    println!("group=0 attempts={:?} occupied={}", describe(&first), first.state.window.occupied);
    let follower = request(client.try_ack(group0, first.records[2].token))?;
    assert_eq!((follower.retired, follower.state.window.completed_end.get()), (0, 0));
    println!("ack=2 outcome={:?} retired={}", follower.outcome, follower.retired);

    clock.advance(10)?;
    let expired = request(client.try_ack(group0, first.records[0].token))?;
    assert_eq!(expired.outcome, AckOutcome::Rejected(AckRejection::Expired));
    println!("clock=10ms old=0 outcome={:?}", expired.outcome);
    let retry = request(client.try_lease(group0, ReadLimits::new(3, 105)?))?;
    assert_eq!(describe(&retry), vec![(0, 2, 20), (1, 2, 20)]);
    assert_eq!(retry.state.window.occupied, 3);
    println!("group=0 retries={:?} occupied={}", describe(&retry), retry.state.window.occupied);
    let stale = request(client.try_ack(group0, first.records[0].token))?;
    assert_eq!(stale.outcome, AckOutcome::Rejected(AckRejection::StaleGeneration));
    println!("old=0 outcome={:?}", stale.outcome);
    for index in [1usize, 0] {
        let accepted = request(client.try_ack(group0, retry.records[index].token))?;
        println!("retry_ack={} retired={} completed_end={}", retry.records[index].token.offset.get(),
            accepted.retired, accepted.state.window.completed_end.get());
        assert_eq!(accepted.retired, if index == 1 { 0 } else { 3 });
    }
    let retired = request(client.try_ack(group0, first.records[2].token))?;
    assert_eq!(retired.outcome, AckOutcome::AlreadyRetired);
    println!("old=2 outcome={:?}", retired.outcome);
    let next = request(client.try_lease(group0, ReadLimits::new(1, 35)?))?;
    assert_eq!(describe(&next), vec![(3, 1, 20)]);
    println!("group=0 next={:?}", describe(&next));
    let stalled = request(client.try_group_state(group1))?;
    assert_eq!((stalled.window.completed_end.get(), stalled.window.issued_end.get()), (0, 1));
    println!("group=1 completed_end={} issued_end={}",
        stalled.window.completed_end.get(), stalled.window.issued_end.get());
    let old = first.records[0].token;
    drop((first, retry, other, next));
    for owner in owners { owner.join()?; }
    drop(client);

    // Recovered progress is still zero: Chapter 13 will add a durable consumer checkpoint.
    let (epoch, mut logs) = recovery::recover(root)?.into_parts();
    let owner = WorkerHandle::start_with_leases(logs.remove(0), FileSync, batch, 8, 8,
        LeaseConfig::new(epoch, 3, 10)?, Arc::new(ManualClock::new(0)))?;
    drop(logs);
    let client = owner.client();
    let repeated = request(client.try_lease(group0, ReadLimits::new(1, 35)?))?;
    let new = repeated.records[0].token;
    assert_eq!((new.offset, new.generation, new.deadline_ms), (old.offset, old.generation, old.deadline_ms));
    assert_ne!(new.epoch, old.epoch);
    println!("recovered epoch={} attempts={:?}", epoch.get(), describe(&repeated));
    let fenced = request(client.try_ack(group0, old))?;
    assert_eq!(fenced.outcome, AckOutcome::Rejected(AckRejection::WrongEpoch));
    println!("pre_restart_token outcome={:?}", fenced.outcome);
    assert_eq!(request(client.try_ack(group0, new))?.retired, 1);
    owner.join()?;
    Ok(())
}
