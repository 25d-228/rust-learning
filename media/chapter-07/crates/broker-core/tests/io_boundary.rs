mod common;

use std::fs;
use std::io::{Seek, SeekFrom, Write};
use broker_core::io::{FaultFile, StorageFile, create_file, open_file, read_prefix, write_fully};
use common::TestDir;

#[test]
fn short_writes_and_interruption_preserve_the_exact_suffix() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("short.log");
    let mut file = FaultFile::new(create_file(&path).unwrap(), 3).unwrap();
    file.interrupt_next_write();
    write_fully(&mut file, b"abcdefgh").unwrap();
    assert_eq!(file.file_len().unwrap(), 8);
    file.sync().unwrap();
    drop(file);
    assert_eq!(fs::read(path).unwrap(), b"abcdefgh");
}

#[test]
fn write_error_keeps_a_prefix_instead_of_rolling_back() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("partial.log");
    let mut file = FaultFile::new(create_file(&path).unwrap(), 3).unwrap();
    file.fail_writes_after(5);
    assert!(write_fully(&mut file, b"abcdefgh").is_err());
    assert_eq!(file.stream_position().unwrap(), 5);
    drop(file);
    assert_eq!(fs::read(path).unwrap(), b"abcde");
}

#[test]
fn zero_progress_is_an_error_not_an_infinite_loop() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("zero.log");
    let mut file = FaultFile::new(create_file(&path).unwrap(), 4).unwrap();
    file.zero_next_write();
    assert_eq!(write_fully(&mut file, b"x").unwrap_err().kind(), std::io::ErrorKind::WriteZero);
    assert_eq!(file.file_len().unwrap(), 0);
}

#[test]
fn completed_writes_do_not_hide_sync_failure() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("sync.log");
    let mut file = FaultFile::new(create_file(&path).unwrap(), 8).unwrap();
    write_fully(&mut file, b"written").unwrap();
    file.flush().unwrap();
    file.fail_syncs();
    assert!(file.sync().is_err());
    drop(file);
    assert_eq!(fs::read(path).unwrap(), b"written");
}

#[test]
fn prefix_read_reports_partial_eof_then_clean_eof() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("read.log");
    fs::write(&path, b"cat").unwrap();
    let mut file = open_file(&path).unwrap();
    let mut bytes = [0; 5];
    assert_eq!(read_prefix(&mut file, &mut bytes).unwrap(), 3);
    assert_eq!(&bytes[..3], b"cat");
    assert_eq!(read_prefix(&mut file, &mut bytes).unwrap(), 0);
}

#[test]
fn truncation_does_not_reposition_the_cursor() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("truncate.log");
    let mut file = create_file(&path).unwrap();
    write_fully(&mut file, b"abcdef").unwrap();
    file.truncate(2).unwrap();
    assert_eq!(file.file_len().unwrap(), 2);
    assert_eq!(file.stream_position().unwrap(), 6);
    file.seek(SeekFrom::Start(2)).unwrap();
    write_fully(&mut file, b"Z").unwrap();
    file.sync().unwrap();
    drop(file);
    assert_eq!(fs::read(path).unwrap(), b"abZ");
}

#[test]
fn create_does_not_overwrite_and_open_does_not_create() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("exclusive.log");
    assert!(open_file(&path).is_err());
    let mut file = create_file(&path).unwrap();
    write_fully(&mut file, b"keep").unwrap();
    drop(file);
    assert_eq!(create_file(&path).unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
    let file = open_file(&path).unwrap();
    assert!(FaultFile::new(file, 0).is_err());
    assert_eq!(fs::read(path).unwrap(), b"keep");
}
