use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct TestDir(PathBuf);

impl TestDir {
    pub fn new() -> io::Result<Self> {
        for _ in 0..100 {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("partition-broker-{}-{id}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "temporary directory names occupied"))
    }

    pub fn path(&self) -> &Path { &self.0 }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!("temporary directory cleanup {}: {error}", self.0.display());
        }
    }
}
