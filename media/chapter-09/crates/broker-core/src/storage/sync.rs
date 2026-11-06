use std::io;

use crate::io::StorageFile;
use crate::types::Offset;

// Implementations must either synchronize this file or report failure.
// The base identifies the file for fault injection; it is not a byte position.
pub trait SyncIo {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()>;
}

pub struct FileSync;

impl SyncIo for FileSync {
    fn sync(&mut self, _base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }
}
