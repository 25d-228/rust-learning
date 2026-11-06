use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::log::PartitionLog;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::{Offset, PartitionId};
use broker_core::worker::{BatchConfig, Client, WorkerHandle};

type DemoError = Box<dyn Error + Send + Sync>;

fn producer(client: Client, partition: u16, producer: u8) -> Result<(u16, Vec<u64>), DemoError> {
    let mut tickets = Vec::new();
    for sequence in 0u8..6 {
        tickets.push(client.try_publish(vec![partition as u8, producer, sequence])
            .map_err(|e| format!("publish admission: {:?}", e.reason))?);
    }
    let mut offsets = Vec::new();
    for ticket in tickets { offsets.push(ticket.recv_timeout(Duration::from_secs(5))??.get()); }
    if !offsets.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err("one producer's accepted order was not preserved".into());
    }
    Ok((partition, offsets))
}

fn load(clients: &[Client]) -> Result<Vec<usize>, DemoError> {
    let outcomes = thread::scope(|scope| {
        let mut threads = Vec::new();
        for (partition, client) in clients.iter().enumerate() {
            for producer_id in 0u8..2 {
                let client = client.clone();
                threads.push(scope.spawn(move || producer(client, partition as u16, producer_id)));
            }
        }
        let mut outcomes = Vec::new();
        for handle in threads {
            outcomes.push(match handle.join() {
                Ok(result) => result,
                Err(_) => Err(DemoError::from("producer thread panicked")),
            });
        }
        outcomes
    });
    let mut confirmed = vec![Vec::new(); clients.len()];
    for outcome in outcomes {
        let (partition, offsets) = outcome?;
        confirmed[usize::from(partition)].extend(offsets);
    }
    let mut counts = Vec::new();
    for (partition, client) in clients.iter().enumerate() {
        confirmed[partition].sort_unstable();
        if confirmed[partition] != (0u64..12).collect::<Vec<_>>() {
            return Err("confirmed offsets contain a gap or duplicate".into());
        }
        let ticket = client.try_fetch(Offset::new(0), ReadLimits::new(12, 420)?)
            .map_err(|e| format!("fetch admission: {e:?}"))?;
        let batch = ticket.recv_timeout(Duration::from_secs(5))??;
        if batch.records.len() != 12 || batch.encoded_bytes != 420 {
            return Err("visible record count or byte count is wrong".into());
        }
        let mut seen = [[false; 6]; 2];
        for (expected_offset, record) in batch.records.iter().enumerate() {
            let payload = record.payload();
            if record.offset().get() != expected_offset as u64 || payload.len() != 3
                || payload[0] != partition as u8 || payload[1] >= 2 || payload[2] >= 6
            {
                return Err("record identity or payload is wrong".into());
            }
            let slot = &mut seen[usize::from(payload[1])][usize::from(payload[2])];
            if *slot { return Err("duplicate producer/sequence payload".into()); }
            *slot = true;
        }
        if !seen.iter().flatten().all(|seen| *seen) { return Err("missing payload".into()); }
        counts.push(batch.records.len());
    }
    Ok(counts)
}

fn main() -> Result<(), DemoError> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: owner_probe EXISTING_EMPTY_DIRECTORY")?);
    if args.next().is_some() { return Err("expected one directory argument".into()); }
    let directory = Arc::new(DirectoryGuard::acquire(&root)?);
    let storage = StorageConfig::new(172, 2, 16)?;
    let batching = BatchConfig::new(4, 140, Duration::from_millis(5))?;
    let mut owners = Vec::new();
    let mut clients = Vec::new();
    for number in 0..4 {
        let log = PartitionLog::create(Arc::clone(&directory), PartitionId::new(number)?, storage)?;
        let owner = WorkerHandle::start(log, FileSync, batching, 32, 8)?;
        clients.push(owner.client());
        owners.push(owner);
    }
    let workload = load(&clients);
    // Close all owners before joining any. Preserve every join result even if one fails.
    for owner in &owners { owner.close(); }
    let results: Vec<_> = owners.into_iter().map(WorkerHandle::join).collect();
    let counts = workload?;
    for (partition, result) in results.into_iter().enumerate() {
        let state = result?;
        if state.visible_end.get() != 12 || state.pending_records != 0 {
            return Err("shutdown left an unexpected visible boundary".into());
        }
        println!("partition={partition} confirmed=12 visible=12 records={}", counts[partition]);
    }
    println!("48 confirmed records; four owners joined; no global partition order claimed");
    Ok(())
}
