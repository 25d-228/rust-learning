use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use broker_core::budget::MemoryConfig;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::{Clock, MonotonicClock};
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::storage::recovery::{self, StorageSession};
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{PARTITION_COUNT, PartitionId};
use broker_core::worker::{BatchConfig, BoundedHandle, ExitWatch, WorkerError, WorkerState};
use crate::BoxError;
use crate::server::Clients;

#[derive(Clone, Copy)]
pub struct OwnerConfig {
    pub batch: BatchConfig,
    pub lanes: [usize; 3],
    pub window: usize,
    pub lease_ms: u64,
    pub checkpoint: CheckpointPolicy,
    pub memory: MemoryConfig,
}
impl Default for OwnerConfig {
    fn default() -> Self {
        Self { batch: BatchConfig::new(64, 2 * 1024 * 1024, Duration::from_millis(5)).expect("fixed batch"),
            lanes: [8, 8, 16], window: 256, lease_ms: 30_000,
            checkpoint: CheckpointPolicy::explicit_only(), memory: MemoryConfig::default() }
    }
}

pub struct LocalBroker { owners: Vec<BoundedHandle> }
impl LocalBroker {
    // Synchronous startup: call before entering the async serving runtime.
    pub fn open(path: &Path, initialize: bool) -> Result<Self, BoxError> {
        let session = if initialize {
            recovery::initialize(path, StorageConfig::new(8 * 1024 * 1024, 64, 16)?)?
        } else { recovery::recover(path)? };
        Self::from_session(session, OwnerConfig::default(), Arc::new(MonotonicClock::new()), |_| FileSync)
    }
    // The same constructor supports real I/O and the existing controlled I/O/clock seams.
    pub fn from_session<S, F>(session: StorageSession, config: OwnerConfig,
        clock: Arc<dyn Clock>, mut sync_for: F) -> Result<Self, BoxError>
    where S: SyncIo + Send + 'static, F: FnMut(PartitionId) -> S {
        let (epoch, logs) = session.into_parts();
        if logs.len() != PARTITION_COUNT as usize
            || logs.iter().enumerate().any(|(i, log)| log.partition().get() as usize != i)
        { return Err("startup must supply four ordered partitions".into()); }
        let leases = LeaseConfig::new(epoch, config.window, config.lease_ms)?;
        let mut broker = Self { owners: Vec::with_capacity(PARTITION_COUNT as usize) };
        for log in logs {
            let sync = sync_for(log.partition());
            let owner = BoundedHandle::start(log, sync, config.batch, config.lanes, leases,
                Arc::clone(&clock), config.checkpoint, config.memory)?;
            broker.owners.push(owner);
        }
        Ok(broker)
    }
    pub fn clients(&self) -> Clients {
        Arc::new(std::array::from_fn(|index| self.owners[index].client()))
    }
    pub(crate) fn exits(&self) -> [ExitWatch; PARTITION_COUNT as usize] {
        std::array::from_fn(|index| self.owners[index].exits())
    }
    pub(crate) fn begin_shutdown(&self) -> [bool; PARTITION_COUNT as usize] {
        std::array::from_fn(|index| self.owners[index].begin_shutdown())
    }
    pub(crate) fn join_all(&mut self) -> Vec<Result<WorkerState, WorkerError>> {
        for owner in &self.owners { owner.close(); }
        self.owners.drain(..).map(BoundedHandle::join).collect()
    }
    // Legacy close: flushes appends, but does not implicitly request consumer checkpoints.
    pub fn join(mut self) -> Result<(), BoxError> {
        let results = self.join_all();
        for result in results { result?; }
        Ok(())
    }
}
impl Drop for LocalBroker {
    fn drop(&mut self) {
        for owner in &self.owners { owner.close(); }
        // Each WorkerHandle joins on Drop. This is blocking fallback, not async shutdown.
    }
}
