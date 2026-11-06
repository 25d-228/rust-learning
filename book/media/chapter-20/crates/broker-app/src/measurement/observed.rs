use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use broker_core::io::StorageFile;
use broker_core::storage::sync::{FileSync, OwnerActivity, SyncIo};
use broker_core::types::Offset;
use crate::metrics::Histogram;

pub(super) const NAMES: [&str; 10] = ["owner_append", "owner_control", "owner_deadline", "owner_finish",
    "sync_data", "sync_metadata", "sync_checkpoint", "sync_retention", "sync_directory", "unlink"];
#[derive(Clone)]
pub(super) struct OwnerMetrics {
    pub hist: [Histogram; 10],
    pub failures: [u64; 10],
    pub valid: bool,
}
impl Default for OwnerMetrics {
    fn default() -> Self {
        Self { hist: std::array::from_fn(|_| Histogram::default()), failures: [0; 10], valid: true }
    }
}
#[derive(Clone, Default)]
pub(super) struct Observer(Arc<Mutex<OwnerMetrics>>);
impl Observer {
    pub fn record(&self, index: usize, elapsed: Option<Duration>, failed: bool) {
        let mut data = match self.0.lock() {
            Ok(data) => data,
            Err(error) => { let mut data = error.into_inner(); data.valid = false; data },
        };
        data.hist[index].record(elapsed);
        if failed {
            match data.failures[index].checked_add(1) {
                Some(value) => data.failures[index] = value,
                None => data.valid = false,
            }
        }
    }
    pub fn snapshot(&self) -> OwnerMetrics {
        match self.0.lock() {
            Ok(data) => data.clone(),
            Err(error) => { let mut data = error.into_inner().clone(); data.valid = false; data },
        }
    }
}
pub(super) struct ObservedSync(pub Observer);
impl ObservedSync {
    fn observe(&self, index: usize, action: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let start = Instant::now();
        let result = action();
        let elapsed = Instant::now().checked_duration_since(start);
        self.0.record(index, elapsed, result.is_err());
        result // The original result, never an instrumentation success substituted for it.
    }
}
impl SyncIo for ObservedSync {
    fn observes_service(&self) -> bool { true }
    fn service_observed(&mut self, kind: OwnerActivity, elapsed: Option<Duration>, failed: bool) {
        let index = match kind { OwnerActivity::Append => 0, OwnerActivity::Control => 1,
            OwnerActivity::Deadline => 2, OwnerActivity::Finish => 3 };
        self.0.record(index, elapsed, failed);
    }
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(4, || FileSync.sync(base, file))
    }
    fn sync_metadata(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(5, || FileSync.sync_metadata(file))
    }
    fn sync_checkpoint(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(6, || FileSync.sync_checkpoint(file))
    }
    fn sync_retention(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(7, || FileSync.sync_retention(file))
    }
    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        self.observe(8, || FileSync.sync_directory(path))
    }
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        self.observe(9, || FileSync.remove_segment(path))
    }
}
