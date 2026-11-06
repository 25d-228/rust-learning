use std::fs::File;
use std::io;
use std::path::Path;

use crate::io::StorageFile;
use crate::types::Offset;
use super::metadata::PublicationStep;

// Each successful method must complete the named synchronization operation.
pub trait SyncIo {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()>;

    fn sync_metadata(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    fn sync_checkpoint(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
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
