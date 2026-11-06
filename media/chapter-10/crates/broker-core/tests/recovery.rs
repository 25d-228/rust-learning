mod common;

use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use broker_core::codec::{self, SegmentHeader};
use broker_core::io::StorageFile;
use broker_core::storage::log::segment_name;
use broker_core::storage::metadata::{self, META_NAME, Metadata, PublicationStep, TEMP_NAME};
use broker_core::storage::recovery::{self, RepairKind, StartupStep};
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{Epoch, Offset, PartitionId};
use broker_core::worker::{BatchConfig, RequestError, WorkerHandle};
use common::TestDir;

fn config(cap: u64) -> StorageConfig { StorageConfig::new(cap, 2, 8).unwrap() }

fn fixture(cap: u64, payloads: &[&[u8]], rotate: bool) -> (TestDir, PathBuf) {
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("broker");
    let (epoch, mut logs) = recovery::initialize(&root, config(cap)).unwrap().into_parts();
    assert_eq!(epoch.get(), 1);
    for payload in payloads { logs[0].append(payload).unwrap(); }
    if rotate { logs[0].rotate().unwrap(); }
    drop(logs); // These extra writes are deliberately NOT synchronized by this helper.
    (directory, root)
}

fn segment(root: &Path, partition: u16, base: u64) -> PathBuf {
    root.join(format!("partition-{partition}")).join(segment_name(Offset::new(base)))
}

fn suffix(path: &Path, bytes: &[u8]) {
    OpenOptions::new().append(true).open(path).unwrap().write_all(bytes).unwrap();
}

fn overwrite(path: &Path, position: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(position)).unwrap();
    file.write_all(bytes).unwrap();
}

fn shorten(path: &Path, length: u64) {
    OpenOptions::new().write(true).open(path).unwrap().set_len(length).unwrap();
}

fn cut(point: PublicationStep) -> impl FnMut(StartupStep) -> io::Result<()> {
    move |step| {
        if step == StartupStep::Metadata(point) { Err(io::Error::other("injected publication cut")) }
        else { Ok(()) }
    }
}

fn epoch(root: &Path) -> u64 { metadata::read(root).unwrap().epoch().get() }

fn reseal(raw: &mut [u8; 48]) {
    let crc = crc32fast::hash(&raw[..44]);
    raw[44..48].copy_from_slice(&crc.to_be_bytes());
}

#[test]
fn metadata_has_an_independent_golden_layout() {
    let golden = [
        0x50, 0x42, 0x4d, 0x44, 0x00, 0x01, 0x00, 0x30,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
        0x00, 0x04, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x66, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x00, 0x82, 0xb5, 0x3a, 0x19,
    ];
    let value = Metadata::new(Epoch::new(7).unwrap(), config(102));
    assert_eq!(value.encode(), golden);
    let decoded = Metadata::decode(&golden).unwrap();
    assert_eq!(decoded.epoch().get(), 7);
    assert_eq!(decoded.config().segment_bytes(), 102);
    assert_eq!(decoded.config().index_stride(), 2);
    assert_eq!(decoded.config().max_segments(), 8);
}

#[test]
fn metadata_rejects_damage_and_checksummed_invalid_fields() {
    let good = Metadata::new(Epoch::new(7).unwrap(), config(102)).encode();
    for length in 0..48 { assert!(Metadata::decode(&good[..length]).is_err()); }
    let mut extra = good.to_vec(); extra.push(0);
    assert!(Metadata::decode(&extra).is_err());
    for i in 0..48 {
        let mut bad = good; bad[i] ^= 1;
        assert!(Metadata::decode(&bad).is_err(), "changed byte {i}");
    }
    for field in 0..8 {
        let mut bad = good;
        match field {
            0 => bad[0] = b'X',
            1 => bad[5] = 2,
            2 => bad[7] = 47,
            3 => bad[8..16].fill(0),
            4 => bad[17] = 3,
            5 => bad[19] = 1,
            6 => bad[20..28].fill(0),
            7 => bad[40] = 1,
            _ => unreachable!(),
        }
        reseal(&mut bad);
        assert!(Metadata::decode(&bad).is_err(), "field case {field}");
    }
}

#[test]
fn initialization_is_explicit_and_all_four_partitions_exist() {
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("broker");
    let session = recovery::initialize(&root, config(102)).unwrap();
    assert_eq!(session.epoch().get(), 1);
    let (_, logs) = session.into_parts();
    assert_eq!(logs.len(), 4);
    assert!(logs.iter().all(|log| log.written_end() == Offset::new(0)));
    assert!(recovery::initialize(&root, config(102)).is_err());
    drop(logs);
    assert_eq!(recovery::recover(&root).unwrap().epoch().get(), 2);
    let existing = directory.path().join("already-exists");
    fs::create_dir(&existing).unwrap();
    assert!(recovery::initialize(&existing, config(102)).is_err());
    assert!(!existing.join(META_NAME).exists());
}

#[test]
fn missing_authority_never_promotes_a_candidate() {
    let (_directory, root) = fixture(102, &[], false);
    fs::rename(root.join(META_NAME), root.join(TEMP_NAME)).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert!(recovery::initialize(&root, config(102)).is_err());
    assert!(!root.join(META_NAME).exists());
    assert!(root.join(TEMP_NAME).exists());
}

#[test]
fn corrupt_authority_never_falls_back_to_a_valid_candidate() {
    let (_directory, root) = fixture(102, &[], false);
    fs::copy(root.join(META_NAME), root.join(TEMP_NAME)).unwrap();
    overwrite(&root.join(META_NAME), 0, b"X");
    let bad = fs::read(root.join(META_NAME)).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::read(root.join(META_NAME)).unwrap(), bad);
    assert!(root.join(TEMP_NAME).exists());
}

#[test]
fn complete_unconfirmed_records_survive_and_indexes_are_rebuilt() {
    let (_directory, root) = fixture(102, &[b"cat", b"dog", b"owl", b"fox", b"yak"], false);
    let session = recovery::recover(&root).unwrap();
    assert!(session.repairs().is_empty());
    let (new_epoch, mut logs) = session.into_parts();
    assert_eq!(new_epoch.get(), 2);
    assert_eq!(logs[0].written_end().get(), 5);
    let infos = logs[0].segment_infos();
    assert_eq!(infos.iter().map(|s| s.base.get()).collect::<Vec<_>>(), [0, 2, 4]);
    assert!(infos.iter().all(|s| s.index_entries == 1));
    for start in 0..5 {
        let batch = logs[0].read(Offset::new(start), Offset::new(5), ReadLimits::new(8, 1000).unwrap()).unwrap();
        assert_eq!(batch.records.len(), (5 - start) as usize);
        assert_eq!(batch.records[0].offset().get(), start);
    }
    assert_eq!(logs[0].append(b"emu").unwrap().get(), 5);
    logs[0].synchronize(&mut FileSync).unwrap();
    drop(logs);
    let (_, logs) = recovery::recover(&root).unwrap().into_parts();
    assert_eq!(logs[0].written_end().get(), 6);
}

#[test]
fn every_final_record_cut_has_the_expected_boundary() {
    let record = codec::encode_record(Offset::new(1), b"dog").unwrap();
    for cut_at in 0..=record.len() {
        let (_directory, root) = fixture(256, &[b"cat"], false);
        let file = segment(&root, 0, 0);
        suffix(&file, &record[..cut_at]);
        let session = recovery::recover(&root).unwrap();
        let partial = cut_at > 0 && cut_at < record.len();
        assert_eq!(session.repairs().len(), usize::from(partial), "cut {cut_at}");
        if partial {
            let repair = session.repairs()[0];
            assert_eq!((repair.before, repair.after), (67 + cut_at as u64, 67));
            assert_eq!(repair.kind, RepairKind::TruncateRecord);
        }
        let (_, mut logs) = session.into_parts();
        let end = if cut_at == record.len() { 2 } else { 1 };
        assert_eq!(logs[0].written_end().get(), end);
        assert_eq!(logs[0].append(b"owl").unwrap().get(), end);
        let batch = logs[0].read(Offset::new(0), Offset::new(end + 1), ReadLimits::new(8, 1000).unwrap()).unwrap();
        assert_eq!(batch.records.last().unwrap().payload(), b"owl");
        assert_eq!(fs::metadata(file).unwrap().len(), 32 + (end + 1) * 35);
    }
}

#[test]
fn a_truncated_nonfinal_file_is_not_repaired() {
    let (_directory, root) = fixture(102, &[b"cat", b"dog", b"owl"], false);
    let path = segment(&root, 0, 0); shorten(&path, 101);
    let before = fs::read(&path).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(epoch(&root), 1);
}

#[test]
fn complete_record_header_corruption_is_not_a_tail_cut() {
    let (_directory, root) = fixture(256, &[b"cat", b"dog"], false);
    let path = segment(&root, 0, 0); overwrite(&path, 67 + 16, &[0xff]);
    let before = fs::read(&path).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn complete_payload_corruption_is_not_a_tail_cut() {
    let (_directory, root) = fixture(256, &[b"cat", b"dog"], false);
    let path = segment(&root, 0, 0); overwrite(&path, 67 + 32, b"X");
    let before = fs::read(&path).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn every_partition_is_validated_before_any_repair() {
    let (_directory, root) = fixture(256, &[b"cat"], false);
    let tail = segment(&root, 0, 0);
    suffix(&tail, &codec::encode_record(Offset::new(1), b"dog").unwrap()[..34]);
    overwrite(&segment(&root, 3, 0), 0, b"X");
    let before = fs::read(&tail).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::read(&tail).unwrap(), before);
    assert_eq!(epoch(&root), 1);
}

#[test]
fn missing_middle_and_noncanonical_names_are_refused() {
    for case in 0..3 {
        let (_directory, root) = fixture(102, &[b"cat", b"dog", b"owl", b"fox", b"yak"], false);
        let middle = segment(&root, 0, 2);
        match case {
            0 => fs::remove_file(&middle).unwrap(),
            1 => fs::rename(&middle, segment(&root, 0, 3)).unwrap(),
            2 => fs::rename(&middle, middle.parent().unwrap().join("2.seg")).unwrap(),
            _ => unreachable!(),
        }
        assert!(recovery::recover(&root).is_err());
        assert_eq!(epoch(&root), 1);
    }
}

#[test]
fn unexpected_root_entries_and_missing_partitions_are_refused() {
    for case in 0..3 {
        let (_directory, root) = fixture(102, &[], false);
        match case {
            0 => fs::write(root.join("unexplained"), b"data").unwrap(),
            1 => fs::remove_dir_all(root.join("partition-3")).unwrap(),
            2 => fs::create_dir(root.join(TEMP_NAME)).unwrap(),
            _ => unreachable!(),
        }
        assert!(recovery::recover(&root).is_err());
        assert_eq!(epoch(&root), 1);
    }
}

#[cfg(unix)]
#[test]
fn a_segment_symlink_is_not_followed() {
    let (directory, root) = fixture(102, &[], false);
    let path = segment(&root, 0, 0);
    let outside = directory.path().join("outside");
    fs::rename(&path, &outside).unwrap();
    std::os::unix::fs::symlink(&outside, &path).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::metadata(outside).unwrap().len(), 32);
}

#[test]
fn an_empty_rotated_tail_is_kept_and_used() {
    let (_directory, root) = fixture(102, &[b"cat", b"dog"], true);
    let session = recovery::recover(&root).unwrap();
    assert!(session.repairs().is_empty());
    let (_, mut logs) = session.into_parts();
    assert_eq!(logs[0].segment_infos().last().unwrap().bytes, 32);
    assert_eq!(logs[0].append(b"owl").unwrap().get(), 2);
    assert_eq!(logs[0].segment_infos().len(), 2);
}

#[test]
fn every_short_rotated_header_can_finish_without_reusing_offsets() {
    for cut_at in 0..32 {
        let (_directory, root) = fixture(102, &[b"cat", b"dog"], true);
        let tail = segment(&root, 0, 2); shorten(&tail, cut_at);
        let session = recovery::recover(&root).unwrap();
        assert_eq!(session.repairs()[0].kind, RepairKind::CompleteSegmentHeader);
        assert_eq!((session.repairs()[0].before, session.repairs()[0].after), (cut_at, 32));
        let (_, mut logs) = session.into_parts();
        assert_eq!(logs[0].append(b"owl").unwrap().get(), 2);
        assert_eq!(fs::metadata(tail).unwrap().len(), 67);
    }
}

#[test]
fn an_incomplete_initial_segment_header_is_not_recreated() {
    let (_directory, root) = fixture(102, &[], false);
    let path = segment(&root, 0, 0); shorten(&path, 8);
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::metadata(path).unwrap().len(), 8);
}

#[test]
fn a_wrong_partial_rotated_header_is_not_recreated() {
    let (_directory, root) = fixture(102, &[b"cat", b"dog"], true);
    let path = segment(&root, 0, 2); shorten(&path, 4); overwrite(&path, 0, b"NOPE");
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::read(path).unwrap(), b"NOPE");
}

#[test]
fn a_checksummed_but_impossible_record_length_is_refused() {
    let (_directory, root) = fixture(102, &[b"cat"], false);
    let path = segment(&root, 0, 0);
    let oversized_here = codec::encode_record(Offset::new(1), b"four").unwrap();
    suffix(&path, &oversized_here[..32]); // 36 required bytes, only 35 could ever fit.
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::metadata(path).unwrap().len(), 99);
}

#[test]
fn a_checksummed_wrong_offset_is_refused() {
    let (_directory, root) = fixture(256, &[b"cat"], false);
    let path = segment(&root, 0, 0);
    suffix(&path, &codec::encode_record(Offset::new(9), b"dog").unwrap());
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::metadata(path).unwrap().len(), 102);
}

#[test]
fn epoch_exhaustion_precedes_all_repair() {
    let (_directory, root) = fixture(256, &[b"cat"], false);
    let path = segment(&root, 0, 0);
    suffix(&path, &codec::encode_record(Offset::new(1), b"dog").unwrap()[..34]);
    fs::write(root.join(META_NAME), Metadata::new(Epoch::new(u64::MAX).unwrap(), config(256)).encode()).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::metadata(path).unwrap().len(), 101);
    assert_eq!(epoch(&root), u64::MAX);
}

#[test]
fn first_creation_cuts_never_look_like_a_fresh_empty_directory() {
    let points = [PublicationStep::TemporaryCreated, PublicationStep::TemporaryWritten,
        PublicationStep::TemporarySynced, PublicationStep::Renamed, PublicationStep::DirectorySynced];
    for point in points {
        let directory = TestDir::new().unwrap();
        let root = directory.path().join("broker");
        assert!(recovery::initialize_with(&root, config(102), &mut FileSync, &mut cut(point)).is_err());
        assert!(recovery::initialize(&root, config(102)).is_err());
        let published = matches!(point, PublicationStep::Renamed | PublicationStep::DirectorySynced);
        if published {
            assert_eq!(epoch(&root), 1);
            assert_eq!(recovery::recover(&root).unwrap().epoch().get(), 2);
        } else {
            assert!(!root.join(META_NAME).exists());
            assert!(recovery::recover(&root).is_err());
        }
    }
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("broker");
    let mut stop = |step| if step == StartupStep::LayoutSynced {
        Err(io::Error::other("cut before first metadata"))
    } else { Ok(()) };
    assert!(recovery::initialize_with(&root, config(102), &mut FileSync, &mut stop).is_err());
    assert!(recovery::recover(&root).is_err());
}

#[test]
fn interrupted_epoch_publication_may_skip_but_never_reuses_a_served_epoch() {
    let points = [PublicationStep::TemporaryCreated, PublicationStep::TemporaryWritten,
        PublicationStep::TemporarySynced, PublicationStep::Renamed, PublicationStep::DirectorySynced];
    for point in points {
        let (_directory, root) = fixture(102, &[b"cat"], false);
        assert!(recovery::recover_with(&root, &mut FileSync, &mut cut(point)).is_err());
        let published = matches!(point, PublicationStep::Renamed | PublicationStep::DirectorySynced);
        assert_eq!(epoch(&root), if published { 2 } else { 1 });
        let session = recovery::recover(&root).unwrap();
        assert_eq!(session.epoch().get(), if published { 3 } else { 2 });
        assert!(!root.join(TEMP_NAME).exists());
        let (_, logs) = session.into_parts();
        assert_eq!(logs[0].written_end().get(), 1);
    }
}

#[test]
fn a_second_recovery_after_interrupted_truncation_is_safe() {
    for after_sync in [false, true] {
        let (_directory, root) = fixture(256, &[b"cat"], false);
        let path = segment(&root, 0, 0);
        suffix(&path, &codec::encode_record(Offset::new(1), b"dog").unwrap()[..34]);
        let mut stop = |step| {
            let selected = if after_sync { matches!(step, StartupStep::RepairSynced(_)) }
                else { matches!(step, StartupStep::RepairApplied(_)) };
            if selected { Err(io::Error::other("cut during repair")) } else { Ok(()) }
        };
        assert!(recovery::recover_with(&root, &mut FileSync, &mut stop).is_err());
        assert_eq!(fs::metadata(&path).unwrap().len(), 67);
        assert_eq!(epoch(&root), 1);
        let session = recovery::recover(&root).unwrap();
        assert_eq!(session.epoch().get(), 2);
        assert!(session.repairs().is_empty());
    }
}

struct FailingSync {
    metadata: bool,
    data: bool,
    directory: Option<PathBuf>,
}

impl SyncIo for FailingSync {
    fn sync(&mut self, _base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        if self.data { Err(io::Error::other("injected segment sync failure")) } else { file.sync() }
    }
    fn sync_metadata(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        if self.metadata { Err(io::Error::other("injected metadata sync failure")) } else { file.sync() }
    }
    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        if self.directory.as_deref() == Some(path) {
            Err(io::Error::other("injected directory sync failure"))
        } else { File::open(path)?.sync_all() }
    }
}

#[test]
fn failed_metadata_sync_does_not_publish_or_return_storage() {
    let (_directory, root) = fixture(102, &[], false);
    let mut io = FailingSync { metadata: true, data: false, directory: None };
    assert!(recovery::recover_with(&root, &mut io, &mut |_| Ok(())).is_err());
    assert_eq!(epoch(&root), 1);
    assert!(root.join(TEMP_NAME).exists());
    assert_eq!(recovery::recover(&root).unwrap().epoch().get(), 2);
}

#[test]
fn failed_directory_sync_after_rename_still_returns_no_storage() {
    let (_directory, root) = fixture(102, &[], false);
    let mut io = FailingSync { metadata: false, data: false, directory: Some(root.canonicalize().unwrap()) };
    assert!(recovery::recover_with(&root, &mut io, &mut |_| Ok(())).is_err());
    assert_eq!(epoch(&root), 2);
    assert_eq!(recovery::recover(&root).unwrap().epoch().get(), 3);
}

#[test]
fn a_rotated_name_sync_failure_withholds_the_producer_success() {
    let (_directory, root) = fixture(67, &[], false);
    let (_, mut logs) = recovery::recover(&root).unwrap().into_parts();
    let log = logs.remove(0);
    let io = FailingSync { metadata: false, data: false,
        directory: Some(root.canonicalize().unwrap().join("partition-0")) };
    let owner = WorkerHandle::start(log, io, BatchConfig::new(1, 1000, Duration::from_secs(1)).unwrap(), 4, 4).unwrap();
    let client = owner.client();
    let first = client.try_publish(b"cat".to_vec()).unwrap();
    assert_eq!(first.recv_timeout(Duration::from_secs(5)).unwrap().unwrap().get(), 0);
    let second = client.try_publish(b"dog".to_vec()).unwrap();
    assert!(matches!(second.recv_timeout(Duration::from_secs(5)).unwrap(), Err(RequestError::Failed(_))));
    assert!(owner.join().is_err());
    drop(logs);
    let (_, logs) = recovery::recover(&root).unwrap().into_parts();
    assert_eq!(logs[0].written_end().get(), 2); // Failure did not roll the write back.
}

#[test]
fn the_session_and_each_remaining_log_keep_exclusive_ownership() {
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("broker");
    let session = recovery::initialize(&root, config(102)).unwrap();
    assert!(recovery::recover(&root).is_err());
    let (_, mut logs) = session.into_parts();
    let last = logs.pop().unwrap();
    drop(logs);
    assert!(recovery::recover(&root).is_err());
    drop(last);
    assert_eq!(recovery::recover(&root).unwrap().epoch().get(), 2);
}

#[test]
fn a_valid_candidate_is_not_an_alternate_authority() {
    let (_directory, root) = fixture(102, &[], false);
    fs::write(root.join(META_NAME), Metadata::new(Epoch::new(9).unwrap(), config(102)).encode()).unwrap();
    fs::write(root.join(TEMP_NAME), Metadata::new(Epoch::new(100).unwrap(), config(102)).encode()).unwrap();
    assert_eq!(recovery::recover(&root).unwrap().epoch().get(), 10);
    assert!(!root.join(TEMP_NAME).exists());
}

#[test]
fn failed_data_sync_does_not_advance_the_epoch() {
    let (_directory, root) = fixture(102, &[b"cat"], false);
    let mut io = FailingSync { metadata: false, data: true, directory: None };
    assert!(recovery::recover_with(&root, &mut io, &mut |_| Ok(())).is_err());
    assert_eq!(epoch(&root), 1);
    assert_eq!(recovery::recover(&root).unwrap().epoch().get(), 2);
}

#[test]
fn complete_segment_headers_and_impossible_partial_prefixes_are_not_repaired() {
    for case in 0..3 {
        let (_directory, root) = fixture(256, &[b"cat"], false);
        let path = segment(&root, 0, 0);
        match case {
            0 => overwrite(&path, 0, b"X"),
            1 => suffix(&path, b"BAD"),
            2 => {
                let wrong = codec::encode_segment(SegmentHeader {
                    partition: PartitionId::new(1).unwrap(), base: Offset::new(0),
                });
                overwrite(&path, 0, &wrong);
            }
            _ => unreachable!(),
        }
        let before = fs::read(&path).unwrap();
        assert!(recovery::recover(&root).is_err());
        assert_eq!(fs::read(path).unwrap(), before);
    }
}
