mod common;

use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use broker_core::codec::{self, HEADER_LEN, MAX_PAYLOAD, Probe, SegmentHeader};
use broker_core::io::{FaultFile, create_file, open_file};
use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::error::StorageError;
use broker_core::storage::log::{PartitionLog, segment_name};
use broker_core::storage::segment::{ReadLimits, ReadStop, Segment, StorageConfig};
use broker_core::types::{Offset, PartitionId};
use common::TestDir;

type TestResult = Result<(), Box<dyn std::error::Error>>;
type Rows = Vec<(u64, Vec<u8>)>;

fn partition() -> PartitionId { PartitionId::new(0).unwrap() }
fn offset(n: u64) -> Offset { Offset::new(n) }
fn config(cap: u64, stride: u64, count: usize) -> StorageConfig {
    StorageConfig::new(cap, stride, count).unwrap()
}
fn limits(records: usize, bytes: usize) -> ReadLimits { ReadLimits::new(records, bytes).unwrap() }
fn root(directory: &TestDir) -> PathBuf { directory.path().join("partition-0") }
fn path(directory: &TestDir, base: u64) -> PathBuf { root(directory).join(segment_name(offset(base))) }
fn guard(directory: &TestDir) -> Arc<DirectoryGuard> {
    Arc::new(DirectoryGuard::acquire(directory.path()).unwrap())
}
fn fixture(log: &mut PartitionLog) {
    for (i, payload) in [b"cat".as_slice(), b"dog", b"", b"owl", b"fox", b"hen", b"bee"].iter().enumerate() {
        assert_eq!(log.append(payload).unwrap(), offset(i as u64));
    }
}
fn rows(batch: &broker_core::storage::segment::ReadBatch) -> Rows {
    batch.records.iter().map(|r| (r.offset().get(), r.payload().to_vec())).collect()
}

// Intentionally unbounded only for these tiny test fixtures: no index or Segment reader.
fn full_scan(directory: &Path) -> Rows {
    let mut paths: Vec<_> = fs::read_dir(directory).unwrap()
        .map(|e| e.unwrap().path()).collect();
    paths.sort();
    let mut records = Vec::new();
    for path in paths {
        let bytes = fs::read(path).unwrap();
        let header = match codec::decode_segment(&bytes).unwrap() {
            Probe::Complete { value, .. } => value,
            Probe::Incomplete { .. } => panic!("oracle segment header is incomplete"),
        };
        assert_eq!(header.base.get(), records.len() as u64);
        let mut position = HEADER_LEN;
        while position < bytes.len() {
            match codec::decode_record(&bytes[position..]).unwrap() {
                Probe::Complete { value, consumed } => {
                    assert_eq!(value.offset.get(), records.len() as u64);
                    records.push((value.offset.get(), value.payload.to_vec()));
                    position += consumed;
                }
                Probe::Incomplete { .. } => panic!("oracle record is incomplete"),
            }
        }
    }
    records
}

fn compare_reads(log: &mut PartitionLog, oracle: &[(u64, Vec<u8>)]) {
    for start in 0..=oracle.len() {
        for upper in start..=oracle.len() {
            for max_records in [1, 2, 5] {
                for max_bytes in [1, 31, 32, 34, 35, 66, 67, 100, 105, 300] {
                    let mut expected = Vec::new();
                    let mut bytes = 0;
                    let mut stop = ReadStop::End;
                    for (number, payload) in &oracle[start..upper] {
                        if expected.len() == max_records { stop = ReadStop::RecordLimit; break; }
                        let encoded = HEADER_LEN + payload.len();
                        if bytes + encoded > max_bytes {
                            stop = ReadStop::ByteLimit { required: encoded };
                            break;
                        }
                        bytes += encoded;
                        expected.push((*number, payload.clone()));
                    }
                    let batch = log.read(offset(start as u64), offset(upper as u64), limits(max_records, max_bytes)).unwrap();
                    assert_eq!(rows(&batch), expected, "start={start} upper={upper}");
                    assert_eq!(batch.encoded_bytes, bytes);
                    assert_eq!(batch.next.get(), (start + expected.len()) as u64);
                    assert_eq!(batch.stop, stop);
                }
            }
        }
    }
}

#[test]
fn exact_fit_then_rotation_keeps_records_whole() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config(102, 2, 8))?;
    log.append(b"cat")?;
    log.append(b"dog")?;
    assert_eq!(log.segment_infos()[0].bytes, 102);
    log.append(b"")?;
    let infos = log.segment_infos();
    assert_eq!(infos.iter().map(|i| (i.base.get(), i.end.get(), i.bytes)).collect::<Vec<_>>(),
        vec![(0, 2, 102), (2, 3, 64)]);
    assert_eq!(full_scan(&root(&directory)), vec![(0, b"cat".to_vec()), (1, b"dog".to_vec()), (2, vec![])]);
    Ok(())
}

#[test]
fn rejection_before_rotation_preserves_a_healthy_owner() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config(64, 1, 8))?;
    log.append(b"")?;
    let before = log.segment_infos();
    assert!(matches!(log.append(b"x"), Err(StorageError::RecordTooLarge { .. })));
    assert!(matches!(log.append(&vec![0; MAX_PAYLOAD + 1]), Err(StorageError::Format { .. })));
    assert_eq!(log.segment_infos(), before);
    assert_eq!(log.append(b"")?, offset(1));
    Ok(())
}

#[test]
fn segment_limit_is_not_poison_and_reads_still_work() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config(64, 1, 1))?;
    log.append(b"")?;
    assert!(matches!(log.append(b""), Err(StorageError::SegmentLimit)));
    assert_eq!(log.written_end(), offset(1));
    assert_eq!(log.read(offset(0), offset(1), limits(1, 32))?.records.len(), 1);
    Ok(())
}

#[test]
fn empty_tail_reopens_without_consuming_or_reusing_an_offset() -> TestResult {
    let directory = TestDir::new()?;
    let owner = guard(&directory);
    let mut log = PartitionLog::create(Arc::clone(&owner), partition(), config(137, 2, 8))?;
    log.rotate()?;
    assert_eq!(log.segment_infos().len(), 1);
    log.append(b"cat")?;
    log.rotate()?;
    log.rotate()?;
    assert_eq!(log.segment_infos().len(), 2);
    assert_eq!(log.segment_infos()[1].bytes, 32);
    drop(log);
    let mut reopened = PartitionLog::open_strict(owner, partition(), config(137, 2, 8))?;
    assert_eq!(reopened.written_end(), offset(1));
    assert_eq!(reopened.append(b"dog")?, offset(1));
    assert_eq!(reopened.segment_infos().len(), 2);
    Ok(())
}

#[test]
fn reading_an_old_record_cannot_redirect_the_next_append() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config(512, 2, 8))?;
    for payload in [b"cat", b"dog", b"owl"] { log.append(payload)?; }
    log.read(offset(0), offset(1), limits(1, 100))?;
    log.append(b"fox")?;
    assert_eq!(full_scan(&root(&directory)), vec![
        (0, b"cat".to_vec()), (1, b"dog".to_vec()), (2, b"owl".to_vec()), (3, b"fox".to_vec()),
    ]);
    Ok(())
}

#[test]
fn indexed_reads_match_full_scan_at_every_fixture_boundary() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config(137, 2, 8))?;
    fixture(&mut log);
    compare_reads(&mut log, &full_scan(&root(&directory)));
    Ok(())
}

#[test]
fn dropping_all_indexes_and_rebuilding_with_another_stride_preserves_reads() -> TestResult {
    let directory = TestDir::new()?;
    let owner = guard(&directory);
    let mut log = PartitionLog::create(Arc::clone(&owner), partition(), config(137, 2, 8))?;
    fixture(&mut log);
    let oracle = full_scan(&root(&directory));
    let before: Vec<_> = fs::read_dir(root(&directory))?.map(|e| {
        let path = e.unwrap().path();
        (path.clone(), fs::read(path).unwrap())
    }).collect();
    drop(log);
    let mut log = PartitionLog::open_strict(owner, partition(), config(137, 3, 8))?;
    compare_reads(&mut log, &oracle);
    for (path, bytes) in before { assert_eq!(fs::read(path)?, bytes); }
    let batch = log.read(offset(5), offset(6), limits(1, 35))?;
    assert_eq!(batch.scanned_records, 3);
    Ok(())
}

#[test]
fn sparse_predecessor_handles_before_equal_between_and_after_keys() -> TestResult {
    let directory = TestDir::new()?;
    let file = create_file(&directory.path().join("index.seg"))?;
    let mut segment = Segment::create(file, SegmentHeader { partition: partition(), base: offset(7) }, config(512, 3, 8))?;
    assert_eq!(segment.index().predecessor(offset(7)), None);
    for _ in 0..7 { segment.append(b"x")?; }
    let entries = segment.index().entries();
    assert_eq!(entries.iter().map(|e| (e.offset.get(), e.position)).collect::<Vec<_>>(),
        vec![(7, 32), (10, 131), (13, 230)]);
    for target in 0..=16 {
        let expected = entries.iter().copied().filter(|e| e.offset <= offset(target)).last();
        assert_eq!(segment.index().predecessor(offset(target)), expected);
    }
    Ok(())
}

#[test]
fn byte_budget_counts_headers_and_never_skips_a_nonfitting_record() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config(137, 2, 8))?;
    fixture(&mut log);
    let small = log.read(offset(1), offset(7), limits(3, 34))?;
    assert!(small.records.is_empty());
    assert_eq!(small.next, offset(1));
    assert_eq!(small.stop, ReadStop::ByteLimit { required: 35 });
    let batch = log.read(offset(1), offset(7), limits(3, 100))?;
    assert_eq!(rows(&batch), vec![(1, b"dog".to_vec()), (2, vec![])]);
    assert_eq!((batch.encoded_bytes, batch.scanned_records), (67, 3));
    assert_eq!(batch.next, offset(3));
    assert_eq!(batch.stop, ReadStop::ByteLimit { required: 35 });
    Ok(())
}

#[test]
fn record_limit_and_explicit_upper_bound_are_independent() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config(137, 2, 8))?;
    fixture(&mut log);
    let one = log.read(offset(2), offset(7), limits(1, 100))?;
    assert_eq!(one.encoded_bytes, 32);
    assert_eq!(one.stop, ReadStop::RecordLimit);
    let end = log.read(offset(2), offset(3), limits(1, 32))?;
    assert_eq!(end.stop, ReadStop::End);
    let empty = log.read(offset(7), offset(7), limits(1, 1))?;
    assert!(empty.records.is_empty());
    assert_eq!(empty.scanned_records, 0);
    assert!(matches!(log.read(offset(8), offset(8), limits(1, 100)), Err(StorageError::Range { .. })));
    assert!(matches!(log.read(offset(3), offset(2), limits(1, 100)), Err(StorageError::Range { .. })));
    assert_eq!(log.append(b"yak")?, offset(7));
    Ok(())
}

#[test]
fn configuration_rejects_zero_and_unbounded_requests() {
    for args in [(63, 1, 1), (67108865, 1, 1), (64, 0, 1), (64, 4097, 1), (64, 1, 0), (64, 1, 65)] {
        assert!(StorageConfig::new(args.0, args.1, args.2).is_err());
    }
    for args in [(0, 32), (1, 0), (1025, 32), (1, 4194305)] {
        assert!(ReadLimits::new(args.0, args.1).is_err());
    }
    assert!(StorageConfig::new(64, 4096, 64).is_ok());
    assert!(ReadLimits::new(1024, 4194304).is_ok());
}

#[test]
fn maximum_payload_fits_only_when_segment_header_is_also_budgeted() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config((MAX_PAYLOAD + 64) as u64, 1, 2))?;
    let payload = vec![0x5a; MAX_PAYLOAD];
    log.append(&payload)?;
    assert_eq!(log.segment_infos()[0].bytes, (MAX_PAYLOAD + 64) as u64);
    let batch = log.read(offset(0), offset(1), limits(1, MAX_PAYLOAD + 32))?;
    assert_eq!(batch.records[0].payload(), payload.as_slice());
    Ok(())
}

#[test]
fn partial_append_poison_preserves_last_complete_metadata() -> TestResult {
    let directory = TestDir::new()?;
    let path = directory.path().join("fault.seg");
    let mut file = FaultFile::new(create_file(&path)?, 3)?;
    file.fail_writes_after(37); // 32-byte segment header, then five record bytes.
    let mut segment = Segment::create(file, SegmentHeader { partition: partition(), base: offset(0) }, config(137, 2, 8))?;
    let before = segment.info();
    assert!(matches!(segment.append(b"cat"), Err(StorageError::Io { .. })));
    assert_eq!(segment.info(), before);
    assert!(matches!(segment.append(b"dog"), Err(StorageError::Poisoned)));
    assert_eq!(fs::metadata(path)?.len(), 37);
    Ok(())
}

#[test]
fn nonempty_file_is_not_overwritten_by_segment_create() -> TestResult {
    let directory = TestDir::new()?;
    let path = directory.path().join("occupied.seg");
    fs::write(&path, b"keep")?;
    let result = Segment::create(open_file(&path)?, SegmentHeader { partition: partition(), base: offset(0) }, config(137, 2, 8));
    assert!(matches!(result, Err(StorageError::Topology(_))));
    assert_eq!(fs::read(path)?, b"keep");
    Ok(())
}

#[test]
fn rotation_collision_never_replaces_the_existing_file() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config(137, 2, 8))?;
    log.append(b"cat")?;
    fs::write(path(&directory, 1), b"keep")?;
    assert!(matches!(log.rotate(), Err(StorageError::PathIo { .. })));
    assert_eq!(fs::read(path(&directory, 1))?, b"keep");
    assert!(matches!(log.append(b"dog"), Err(StorageError::Poisoned)));
    Ok(())
}

fn write_raw(directory: &TestDir, base: u64, offsets: &[u64]) {
    fs::create_dir_all(root(directory)).unwrap();
    let mut bytes = codec::encode_segment(SegmentHeader { partition: partition(), base: offset(base) }).to_vec();
    for number in offsets { bytes.extend(codec::encode_record(offset(*number), b"cat").unwrap()); }
    fs::write(path(directory, base), bytes).unwrap();
}

#[test]
fn strict_rebuild_rejects_every_incomplete_first_record_prefix_without_repair() -> TestResult {
    let directory = TestDir::new()?;
    let owner = guard(&directory);
    write_raw(&directory, 0, &[0]);
    let whole = fs::read(path(&directory, 0))?;
    for end in 33..67 {
        fs::write(path(&directory, 0), &whole[..end])?;
        assert!(matches!(PartitionLog::open_strict(Arc::clone(&owner), partition(), config(137, 2, 8)),
            Err(StorageError::Incomplete { .. })));
        assert_eq!(fs::read(path(&directory, 0))?, whole[..end]);
    }
    fs::write(path(&directory, 0), &whole[..32])?;
    assert_eq!(PartitionLog::open_strict(owner, partition(), config(137, 2, 8))?.written_end(), offset(0));
    Ok(())
}

#[test]
fn strict_rebuild_rejects_complete_header_and_payload_corruption() -> TestResult {
    let directory = TestDir::new()?;
    let owner = guard(&directory);
    write_raw(&directory, 0, &[0]);
    let whole = fs::read(path(&directory, 0))?;
    for position in [32, 60, 64] {
        let mut bad = whole.clone();
        bad[position] ^= 1;
        fs::write(path(&directory, 0), &bad)?;
        assert!(matches!(PartitionLog::open_strict(Arc::clone(&owner), partition(), config(137, 2, 8)),
            Err(StorageError::Format { .. })));
        assert_eq!(fs::read(path(&directory, 0))?, bad);
    }
    Ok(())
}

#[test]
fn indexed_read_checks_the_skipped_prefix_payload_and_stops_the_owner() -> TestResult {
    let directory = TestDir::new()?;
    let mut log = PartitionLog::create(guard(&directory), partition(), config(137, 3, 8))?;
    log.append(b"cat")?;
    log.append(b"dog")?;
    // Deliberate, sequential fault injection; no second production writer is permitted.
    let mut file = open_file(&path(&directory, 0))?;
    file.seek(SeekFrom::Start(64))?;
    file.write_all(b"X")?;
    assert!(matches!(log.read(offset(1), offset(2), limits(1, 35)), Err(StorageError::Format { .. })));
    assert!(matches!(log.append(b"owl"), Err(StorageError::Poisoned)));
    Ok(())
}

#[test]
fn checked_record_with_the_wrong_offset_is_still_invalid() -> TestResult {
    let directory = TestDir::new()?;
    let owner = guard(&directory);
    write_raw(&directory, 0, &[0, 2]);
    assert!(matches!(PartitionLog::open_strict(Arc::clone(&owner), partition(), config(137, 2, 8)),
        Err(StorageError::Offset { expected, found, .. }) if expected == offset(1) && found == offset(2)));
    let bytes = fs::read(path(&directory, 0))?;
    fs::write(path(&directory, 0), &bytes[..bytes.len() - 1])?;
    assert!(matches!(PartitionLog::open_strict(owner, partition(), config(137, 2, 8)),
        Err(StorageError::Offset { expected, found, .. }) if expected == offset(1) && found == offset(2)));
    Ok(())
}

#[test]
fn file_header_must_match_both_partition_and_filename_base() -> TestResult {
    let directory = TestDir::new()?;
    let owner = guard(&directory);
    fs::create_dir(root(&directory))?;
    for header in [
        SegmentHeader { partition: PartitionId::new(1)?, base: offset(0) },
        SegmentHeader { partition: partition(), base: offset(9) },
    ] {
        fs::write(path(&directory, 0), codec::encode_segment(header))?;
        assert!(matches!(PartitionLog::open_strict(Arc::clone(&owner), partition(), config(137, 2, 8)),
            Err(StorageError::Identity { .. })));
    }
    Ok(())
}

#[test]
fn segment_gaps_overlaps_and_nonfinal_empty_segments_fail_closed() -> TestResult {
    for (first, second_base) in [(vec![0], 2), (vec![0, 1], 1), (vec![], 1)] {
        let directory = TestDir::new()?;
        let owner = guard(&directory);
        write_raw(&directory, 0, &first);
        write_raw(&directory, second_base, &[second_base]);
        assert!(matches!(PartitionLog::open_strict(owner, partition(), config(137, 2, 8)),
            Err(StorageError::Topology(_))));
    }
    Ok(())
}

#[test]
fn empty_missing_and_unrecognized_namespaces_are_not_initialized_on_open() -> TestResult {
    let directory = TestDir::new()?;
    let owner = guard(&directory);
    assert!(matches!(PartitionLog::open_strict(Arc::clone(&owner), partition(), config(137, 2, 8)),
        Err(StorageError::PathIo { .. })));
    fs::create_dir(root(&directory))?;
    assert!(matches!(PartitionLog::open_strict(Arc::clone(&owner), partition(), config(137, 2, 8)),
        Err(StorageError::Topology(_))));
    for name in ["1.seg", "not-a-segment", "99999999999999999999.seg"] {
        let bad = root(&directory).join(name);
        fs::write(&bad, b"")?;
        assert!(matches!(PartitionLog::open_strict(Arc::clone(&owner), partition(), config(137, 2, 8)),
            Err(StorageError::Name(_))));
        fs::remove_file(bad)?;
    }
    Ok(())
}

#[test]
fn reopened_file_and_segment_counts_respect_the_new_configuration() -> TestResult {
    let directory = TestDir::new()?;
    let owner = guard(&directory);
    let mut log = PartitionLog::create(Arc::clone(&owner), partition(), config(137, 2, 8))?;
    fixture(&mut log);
    drop(log);
    assert!(matches!(PartitionLog::open_strict(Arc::clone(&owner), partition(), config(100, 2, 8)),
        Err(StorageError::Topology(_))));
    assert!(matches!(PartitionLog::open_strict(owner, partition(), config(137, 2, 2)),
        Err(StorageError::SegmentLimit)));
    Ok(())
}

#[test]
fn log_retains_directory_ownership_after_caller_drops_its_share() -> TestResult {
    let directory = TestDir::new()?;
    let owner = guard(&directory);
    let weak = Arc::downgrade(&owner);
    let log = PartitionLog::create(Arc::clone(&owner), partition(), config(137, 2, 8))?;
    drop(owner);
    assert!(weak.upgrade().is_some());
    drop(log);
    assert!(weak.upgrade().is_none());
    Ok(())
}

#[test]
fn exhausted_offset_rejects_before_writing() -> TestResult {
    let directory = TestDir::new()?;
    let path = directory.path().join("last.seg");
    let mut segment = Segment::create(create_file(&path)?,
        SegmentHeader { partition: partition(), base: offset(u64::MAX) }, config(137, 2, 8))?;
    assert!(matches!(segment.append(b""), Err(StorageError::Format { .. })));
    assert_eq!(segment.info().end, offset(u64::MAX));
    assert_eq!(fs::metadata(path)?.len(), 32);
    Ok(())
}
