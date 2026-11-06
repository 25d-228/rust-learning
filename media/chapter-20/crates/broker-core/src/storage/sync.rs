use std::fs::File;
use std::io;
use std::path::Path;
use std::time::Duration;

use crate::io::StorageFile;
use crate::types::Offset;
use super::metadata::PublicationStep;

#[derive(Debug, Clone, Copy)]
pub enum OwnerActivity { Append, Control, Deadline, Finish }

// Each successful method must complete the named synchronization operation.
pub trait SyncIo {
    // Measurement only: observers must be bounded, nonpanicking, and perform no I/O.
    fn observes_service(&self) -> bool { false }
    fn service_observed(&mut self, _kind: OwnerActivity,
        _elapsed: Option<Duration>, _failed: bool) {}
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()>;

    fn sync_metadata(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    fn sync_checkpoint(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    fn sync_retention(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    // Successful implementations must unlink the named, closed segment.
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    // Test observation point AFTER each actual publication operation.
    fn publication_step(&mut self, _target: &Path, _step: PublicationStep) -> io::Result<()> {
        Ok(())
    }

    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }
}

pub struct FileSync;

impl SyncIo for FileSync {
    fn sync(&mut self, _base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }
}
