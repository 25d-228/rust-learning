use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broker_core::budget::MemoryConfig;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::GroupId;
use broker_core::worker::{AsyncTicket, BatchConfig, BoundedClient, BoundedHandle};

fn g(n: u8) -> GroupId { GroupId::new(n).unwrap() }
async fn reply<T>(ticket: AsyncTicket<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), ticket).await.expect("reply watchdog")
        .expect("owner reply channel").expect("owner operation")
}
async fn exercise(client: &BoundedClient) {
    for bytes in [b"cat", b"dog"] {
        let mut request = client.try_prepare_append(bytes.len()).unwrap();
        request.payload_mut().copy_from_slice(bytes);
        reply(request.submit().unwrap()).await;
    }
    let batch = reply(client.try_prepare_lease(g(0), ReadLimits::new(2, 70).unwrap()).unwrap()
        .submit().unwrap()).await;
    let held_query = client.try_prepare_lease(g(1), ReadLimits::new(2, 70).unwrap()).unwrap();
    let held_append0 = client.try_prepare_append(3).unwrap();
    let held_append1 = client.try_prepare_append(3).unwrap();
    let state = client.memory_state();
    assert_eq!(state.append.free_buffers, 0); assert_eq!(state.response.free_buffers, 0);
    assert!(matches!(client.try_progress(g(0)), Err(AdmissionError::Full)));
    println!("SATURATED append_buffers=2 response_buffers=2 query_slots=1 disk_reserved={}",
        client.disk_state().reserved_bytes);
    for record in batch.records() { reply(client.try_ack_progress(g(0), record.token).unwrap()).await; }
    let checkpoint = reply(client.try_checkpoint(g(0)).unwrap()).await;
    println!("CONTROL persisted_next={} payload_buffers_still_held=4", checkpoint.progress.persisted_next.get());
    drop((held_append0, held_append1, held_query));
    let other = reply(client.try_prepare_lease(g(1), ReadLimits::new(2, 70).unwrap()).unwrap()
        .submit().unwrap()).await;
    for record in other.records() { reply(client.try_ack_progress(g(1), record.token).unwrap()).await; }
    reply(client.try_checkpoint(g(1)).unwrap()).await;
    let retained = reply(client.try_retention().unwrap()).await;
    assert_eq!(batch.records().next().unwrap().payload(), b"cat");
    println!("RETAINED floor={} held_reply_first=cat", retained.floor.get());
    drop((batch, other));
    let state = client.memory_state();
    println!("RELEASED append_checked_out={} response_checked_out={} disk_reserved={}",
        state.append.checked_out_bytes, state.response.checked_out_bytes, client.disk_state().reserved_bytes);
    println!("ALLOCATED append={} response={} scratch={} (allocator-reported capacities)",
        state.append.allocated_bytes, state.response.allocated_bytes, state.scratch_bytes);
}
fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: memory_pressure_probe NEW_DIRECTORY")?);
    if args.next().is_some() { return Err("usage: memory_pressure_probe NEW_DIRECTORY".into()); }
    let (epoch, mut logs) = recovery::initialize(&root, StorageConfig::new(102, 2, 4)?)?.into_parts();
    let log = logs.remove(0); drop(logs);
    let owner = BoundedHandle::start(log, FileSync,
        BatchConfig::new(1, 4096, Duration::from_secs(1))?, [2, 1, 2],
        LeaseConfig::new(epoch, 8, 10000)?, Arc::new(ManualClock::new(0)),
        CheckpointPolicy::explicit_only(), MemoryConfig::new(64, 2, 128, 2, 2)?)?;
    let client = owner.client();
    let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
    runtime.block_on(exercise(&client));
    let state = owner.join()?;
    println!("JOINED visible_end={}", state.visible_end.get());
    Ok(())
}
