use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use broker_core::budget::MemoryConfig;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::MonotonicClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::storage::recovery;
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::FileSync;
use broker_core::worker::{BatchConfig, BoundedHandle};
use crate::BoxError;
use crate::server::Clients;

pub struct LocalBroker { owners: Vec<BoundedHandle> }
impl LocalBroker {
    // Synchronous startup: call before entering the async serving runtime.
    pub fn open(path: &Path, initialize: bool) -> Result<Self, BoxError> {
        let session = if initialize {
            recovery::initialize(path, StorageConfig::new(8 * 1024 * 1024, 64, 16)?)?
        } else { recovery::recover(path)? };
        let (epoch, logs) = session.into_parts();
        let mut broker = Self { owners: Vec::with_capacity(4) };
        for log in logs {
            let owner = BoundedHandle::start(log, FileSync,
                BatchConfig::new(64, 2 * 1024 * 1024, Duration::from_millis(5))?,
                [8, 8, 16], LeaseConfig::new(epoch, 256, 30_000)?,
                Arc::new(MonotonicClock::new()), CheckpointPolicy::explicit_only(), MemoryConfig::default())?;
            broker.owners.push(owner);
        }
        Ok(broker)
    }
    pub fn clients(&self) -> Clients {
        Arc::new(std::array::from_fn(|index| self.owners[index].client()))
    }
    // Blocking, and intentionally not claimed to have a finite deadline.
    pub fn join(mut self) -> Result<(), BoxError> {
        for owner in &self.owners { owner.close(); }
        let mut first = None;
        for owner in self.owners.drain(..) {
            if let Err(error) = owner.join() {
                if first.is_none() { first = Some(error); }
            }
        }
        match first { Some(error) => Err(error.into()), None => Ok(()) }
    }
}
impl Drop for LocalBroker {
    fn drop(&mut self) {
        for owner in &self.owners { owner.close(); }
        // Each prerequisite WorkerHandle also joins on Drop; never detach an owner.
    }
}
