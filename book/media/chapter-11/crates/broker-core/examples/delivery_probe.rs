use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::time::Duration;

use broker_core::delivery::{DeliveryConfig, DeliveryStop};
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::{GroupId, Offset};
use broker_core::worker::{BatchConfig, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 2 {
        return Err(io::Error::other("usage: delivery_probe ABSENT_DIRECTORY").into());
    }
    let root = Path::new(&args[1]);
    let (epoch, logs) = recovery::initialize(root, StorageConfig::new(102, 2, 16)?)?.into_parts();
    let config = BatchConfig::new(1, 1000, Duration::from_secs(1))?;
    let mut owners = Vec::new();
    for log in logs {
        owners.push(WorkerHandle::start_with_delivery(log, FileSync, config, 8, 8,
            DeliveryConfig::new(epoch, 4)?)?);
    }
    println!("epoch={} partitions={} groups=2", epoch.get(), owners.len());
    let client = owners[0].client();
    for (index, payload) in [b"cat", b"dog", b"owl", b"fox", b"eel", b"ant"].iter().enumerate() {
        assert_eq!(request(client.try_publish(payload.to_vec()))?.get(), index as u64);
    }
    let first = request(client.try_deliver(GroupId::new(0)?, ReadLimits::new(4, 1000)?))?;
    let values: Vec<_> = first.records.iter().map(|record| record.offset().get()).collect();
    assert_eq!(values, vec![0, 1, 2, 3]);
    assert_eq!(first.stop, DeliveryStop::WindowFull);
    println!("group=0 offsets={values:?} stop={:?}", first.stop);
    let second = request(client.try_deliver(GroupId::new(1)?, ReadLimits::new(4, 70)?))?;
    let values: Vec<_> = second.records.iter().map(|record| record.offset().get()).collect();
    assert_eq!(values, vec![0, 1]);
    assert_eq!(second.stop, DeliveryStop::ByteLimit { required: 35 });
    println!("group=1 offsets={values:?} stop={:?}", second.stop);
    // Neither reply owns a window. Dropping record copies is independent of completion.
    drop(first);
    drop(second);
    for (value, expected_end) in [(2, 0), (0, 1), (3, 1), (1, 4)] {
        let result = request(client.try_complete(GroupId::new(0)?, Offset::new(value)))?;
        assert_eq!(result.state.window.completed_end.get(), expected_end);
        println!("complete={value} retired={} completed_end={} occupied={}",
            result.completion.retired, result.state.window.completed_end.get(),
            result.state.window.occupied);
    }
    let remaining = request(client.try_deliver(GroupId::new(0)?, ReadLimits::new(4, 1000)?))?;
    let values: Vec<_> = remaining.records.iter().map(|record| record.offset().get()).collect();
    assert_eq!(values, vec![4, 5]);
    println!("group=0 offsets={values:?} stop={:?}", remaining.stop);
    let stalled = request(client.try_group_state(GroupId::new(1)?))?;
    assert_eq!((stalled.window.completed_end.get(), stalled.window.issued_end.get()), (0, 2));
    println!("group=1 completed_end={} issued_end={} occupied={}",
        stalled.window.completed_end.get(), stalled.window.issued_end.get(), stalled.window.occupied);
    drop(remaining);
    for owner in owners { owner.join()?; }
    drop(client);

    // No consumer checkpoint exists yet: even a graceful restart repeats completed work.
    let (new_epoch, mut logs) = recovery::recover(root)?.into_parts();
    assert_eq!(new_epoch.get(), epoch.get() + 1);
    let owner = WorkerHandle::start_with_delivery(logs.remove(0), FileSync, config, 8, 8,
        DeliveryConfig::new(new_epoch, 4)?)?;
    drop(logs);
    let repeated = request(owner.client().try_deliver(GroupId::new(0)?, ReadLimits::new(2, 70)?))?;
    let values: Vec<_> = repeated.records.iter().map(|record| record.offset().get()).collect();
    assert_eq!(values, vec![0, 1]);
    assert_eq!(repeated.state.window.completed_end.get(), 0);
    println!("recovered epoch={} group=0 offsets={values:?} completed_end={}",
        repeated.state.epoch.get(), repeated.state.window.completed_end.get());
    owner.join()?;
    Ok(())
}
