use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use broker_core::budget::DiskState;
use broker_core::delivery::{AckOutcome, AckRejection};
use broker_core::delivery::clock::MonotonicClock;
use broker_core::delivery::progress::ProgressAck;
use broker_core::storage::recovery;
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::FileSync;
use broker_core::worker::MemoryState;
use tokio::runtime::Runtime;
use crate::BoxError;
use crate::local::LocalBroker;
use super::{Config, Mode, SEGMENT_BYTES, expected_scans, g, phase, require};
use super::load::{self, Appended, Consumed, admission, receive};
use super::observed::{ObservedSync, Observer, OwnerMetrics};
use super::report;

#[derive(Debug, Clone)]
pub struct Summary {
    pub confirmed: u64, pub refused: u64, pub missed: u64,
    pub useful: u64, pub scanned: u64, pub sync_groups: u64,
    pub append_seconds: f64, pub consumer_seconds: f64, pub recovery_seconds: f64,
    pub index_entries: u64,
    pub retained: [u64; 4],
}
pub(super) struct Details {
    pub appended: Appended, pub consumed: Consumed,
    pub owners: [OwnerMetrics; 4], pub memory: [MemoryState; 4],
    pub disk_before: [DiskState; 4], pub disk_after: [DiskState; 4],
    pub recovery: Duration, pub summary: Summary,
}
fn duration(start: Instant) -> Result<Duration, BoxError> {
    Instant::now().checked_duration_since(start)
        .ok_or_else(|| std::io::Error::other("measurement clock reversed").into())
}
pub(super) fn geometry(records: usize, payload: usize, stride: u64) -> (u128, u64) {
    let per_segment = (SEGMENT_BYTES as usize - 32) / (32 + payload);
    let mut remaining = records;
    let (mut scans, mut entries) = (0, 0);
    while remaining != 0 {
        let count = remaining.min(per_segment) as u64;
        scans += expected_scans(count, stride);
        entries += count.div_ceil(stride);
        remaining -= count as usize;
    }
    (scans, entries)
}

fn case(root: &Path, outer: &Path, config: Config, runtime: &Runtime, label: &str)
    -> Result<Summary, BoxError>
{
    fs::create_dir(root)?;
    phase(outer, &format!("{label}:initialize"))?;
    let data = root.join("broker");
    let session = recovery::initialize_budgeted(&data,
        StorageConfig::new(SEGMENT_BYTES, config.profile.stride(), 16)?, config.quota()?)?;
    let original_epoch = session.epoch();
    let observers: [Observer; 4] = std::array::from_fn(|_| Observer::default());
    let broker = LocalBroker::from_session(session, config.owners()?, Arc::new(MonotonicClock::new()),
        |partition| ObservedSync(observers[usize::from(partition.get())].clone()))?;
    let clients = broker.clients();
    // Capture the work result, then explicitly join the owner threads on BOTH success and error.
    let work = (|| -> Result<(Appended, Consumed, u64), BoxError> {
        phase(outer, &format!("{label}:append"))?;
        let appended = runtime.block_on(load::append(&clients, config))?;
        report::write_attempts(root, config, &appended)?;
        require(appended.stats.ambiguous == 0, "ambiguous append: counters retained; do not compare this run")?;
        phase(outer, &format!("{label}:consume"))?;
        let consumed = runtime.block_on(load::consume(&clients, Arc::clone(&appended.expected), config))?;
        let groups = runtime.block_on(async {
            let mut groups = 0u64;
            for (index, client) in clients.iter().enumerate() {
                let state = receive(client.try_snapshot().map_err(admission)?).await?;
                require(state.visible_end.get() == appended.expected[index].len() as u64
                    && state.pending_records == 0, "snapshot disagrees with confirmations")?;
                groups += state.sync_groups;
            }
            Ok::<_, BoxError>(groups)
        })?;
        Ok((appended, consumed, groups))
    })();
    let memory: [MemoryState; 4] = std::array::from_fn(|index| clients[index].memory_state());
    let disk_before: [DiskState; 4] = std::array::from_fn(|index| clients[index].disk_state());
    let joined = broker.join(); // Plain close flushes appends but deliberately does not checkpoint.
    drop(clients); // Release old resource-generation handles before reopening the directory.
    let owners: [OwnerMetrics; 4] = std::array::from_fn(|index| observers[index].snapshot());
    drop(observers);
    let (appended, consumed, sync_groups) = match work {
        Ok(value) => { joined?; value },
        Err(error) => { if let Err(join_error) = joined { eprintln!("cleanup also failed: {join_error}"); }
            return Err(error); }
    };
    require(consumed.stats.duplicates == 0, "unexpected retry in the no-failure measurement")?;
    let (mut predicted_scans, mut index_entries) = (0u128, 0u64);
    for identities in appended.expected.iter() {
        let (scans, entries) = geometry(identities.len(), config.payload_bytes, config.profile.stride());
        predicted_scans += 2 * scans; index_entries += entries;
    }
    require(u128::from(consumed.stats.scanned) == predicted_scans, "observed scans differ from segment geometry")?;
    require(consumed.stats.useful == 2 * appended.stats.confirmed, "both groups must cover every confirmation")?;
    for state in &memory {
        require(state.append.checked_out_bytes == 0 && state.response.checked_out_bytes == 0,
            "a timed payload owner remains alive")?;
    }

    phase(outer, &format!("{label}:recover"))?;
    let start = Instant::now();
    let session = recovery::recover(&data)?;
    let recovery = duration(start)?;
    require(session.epoch() > original_epoch && session.repairs().is_empty(), "ordinary recovery changed the fixture")?;
    let broker = LocalBroker::from_session(session, config.owners()?, Arc::new(MonotonicClock::new()), |_| FileSync)?;
    let clients = broker.clients();
    let verify = runtime.block_on(async {
        phase(outer, &format!("{label}:verify-recovery"))?;
        if let Some(token) = consumed.stats.first_token {
            let result = receive(clients[usize::from(token.partition.get())]
                .try_ack_progress(token.group, token).map_err(admission)?).await?;
            require(matches!(result, ProgressAck::NotApplied { ack, .. }
                if ack.outcome == AckOutcome::Rejected(AckRejection::WrongEpoch)),
                "old epoch was not fenced")?;
        }
        // A fresh seen-set is created inside consume; pre-restart observations cannot cover this pass.
        let recovered = load::consume(&clients, Arc::clone(&appended.expected), config).await?;
        require(recovered.stats.useful == consumed.stats.useful && recovered.stats.duplicates == 0,
            "restart coverage differs from confirmed work")?;
        phase(outer, &format!("{label}:checkpoint-retain"))?;
        let mut retained = [0u64; 4];
        for (partition, client) in clients.iter().enumerate() {
            let end = appended.expected[partition].len() as u64;
            for group in 0..2 {
                let checkpoint = receive(client.try_checkpoint(g(group)).map_err(admission)?).await?;
                require(checkpoint.progress.persisted_next.get() == end, "checkpoint failed to cover checked work")?;
            }
            let result = receive(client.try_retention().map_err(admission)?).await?;
            require(result.floor.get() == end && result.disk.file_bytes == 32,
                "whole-log reclamation must leave a single empty active header")?;
            retained[partition] = result.floor.get();
        }
        Ok::<_, BoxError>(retained)
    });
    let disk_after: [DiskState; 4] = std::array::from_fn(|index| clients[index].disk_state());
    let joined = broker.join(); drop(clients);
    let retained = match verify {
        Ok(value) => { joined?; value },
        Err(error) => { if let Err(join_error) = joined { eprintln!("cleanup also failed: {join_error}"); }
            return Err(error); }
    };
    // Reopen once more to validate the published floor and the now-retained topology.
    let retained_session = recovery::recover(&data)?;
    let (_, logs) = retained_session.into_parts();
    for (partition, log) in logs.iter().enumerate() {
        require(log.retained_floor().get() == retained[partition]
            && log.written_end().get() == retained[partition], "retained restart resurrected records")?;
    }
    drop(logs);
    require(!appended.elapsed.is_zero() && !consumed.elapsed.is_zero() && !recovery.is_zero(),
        "zero duration is not a throughput measurement")?;
    let summary = Summary {
        confirmed: appended.stats.confirmed, refused: appended.stats.refused, missed: appended.stats.missed,
        useful: consumed.stats.useful, scanned: consumed.stats.scanned, sync_groups,
        append_seconds: appended.elapsed.as_secs_f64(), consumer_seconds: consumed.elapsed.as_secs_f64(),
        recovery_seconds: recovery.as_secs_f64(), index_entries, retained,
    };
    let details = Details { appended, consumed, owners, memory, disk_before, disk_after, recovery,
        summary: summary.clone() };
    report::write(root, config, &details)?;
    Ok(summary)
}

pub fn execute(root: &Path, config: Config) -> Result<Summary, BoxError> {
    config.validate()?; // Invalid arguments cannot create or overwrite a run.
    fs::create_dir(root)?;
    let result = (|| {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let warmup = Config { mode: Mode::Closed, offers_per_producer: 16, period_us: 0, ..config };
        case(&root.join("warmup"), root, warmup, &runtime, "warmup")?;
        let summary = case(&root.join("measured"), root, config, &runtime, "measured")?;
        phase(root, "complete")?;
        Ok::<_, BoxError>(summary)
    })();
    if let Err(error) = &result {
        // The supplied run directory was newly created by this call; unrelated directories are untouched.
        let _ = fs::write(root.join("failure.txt"), format!("{error}\n"));
        let _ = phase(root, "failed");
    }
    result
}
