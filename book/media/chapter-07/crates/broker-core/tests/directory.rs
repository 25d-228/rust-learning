mod common;

use std::fs;
use broker_core::error::DirectoryError;
use broker_core::storage::directory::DirectoryGuard;
use common::TestDir;

#[test]
fn a_live_guard_excludes_a_separately_opened_handle() {
    let dir = TestDir::new().unwrap();
    let guard = DirectoryGuard::acquire(dir.path()).unwrap();
    assert_eq!(guard.root(), dir.path().canonicalize().unwrap().as_path());
    assert!(matches!(DirectoryGuard::acquire(dir.path()), Err(DirectoryError::Busy(_))));
    drop(guard);
}

#[test]
fn leftover_filename_is_not_ownership_and_is_never_truncated() {
    let dir = TestDir::new().unwrap();
    let lock = dir.path().join(".broker.lock");
    fs::write(&lock, b"sentinel").unwrap();
    let guard = DirectoryGuard::acquire(dir.path()).unwrap();
    drop(guard);
    assert_eq!(fs::read(&lock).unwrap(), b"sentinel");
    let guard = DirectoryGuard::acquire(dir.path()).unwrap();
    drop(guard);
    assert!(lock.exists());
}

#[test]
fn missing_root_is_not_silently_initialized() {
    let dir = TestDir::new().unwrap();
    let absent = dir.path().join("absent");
    assert!(matches!(DirectoryGuard::acquire(&absent), Err(DirectoryError::Io { .. })));
    assert!(!absent.exists());
}
