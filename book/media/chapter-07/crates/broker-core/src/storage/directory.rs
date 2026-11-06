use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use crate::error::DirectoryError;

pub struct DirectoryGuard {
    root: PathBuf,
    _lock: File,
}

impl DirectoryGuard {
    pub fn acquire(path: &Path) -> Result<Self, DirectoryError> {
        let root = path.canonicalize().map_err(|source| DirectoryError::Io {
            operation: "resolve data directory", path: path.to_path_buf(), source,
        })?;
        let lock_path = root.join(".broker.lock");
        let file = OpenOptions::new().read(true).write(true)
            .create(true).truncate(false).open(&lock_path)
            .map_err(|source| DirectoryError::Io {
                operation: "open directory lock", path: lock_path.clone(), source,
            })?;
        match file.try_lock() {
            Ok(()) => Ok(Self { root, _lock: file }),
            Err(TryLockError::WouldBlock) => Err(DirectoryError::Busy(root)),
            Err(TryLockError::Error(source)) => Err(DirectoryError::Io {
                operation: "lock data directory", path: lock_path, source,
            }),
        }
    }

    pub fn root(&self) -> &Path { &self.root }
}
