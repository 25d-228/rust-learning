# Chapter 10 — Recovery, metadata publication, and restart epochs
## Part 3 — Recovery fixtures and a process interruption

This part completes [Part 1](https://25d-228.github.io/rust-learning/chapter-10.html) and [Part 2](https://25d-228.github.io/rust-learning/chapter-10-part-02.html). The chapter's **6.0-hour implementation estimate** includes these checks. The code here adds 31 Rust tests, one Rust example, and a small Python process harness. Test listings and displayed command outputs are expected behavior, not a substitute for executing them on the native target.

## Construct states whose meaning is unambiguous

A filesystem fixture and a process termination test answer different questions. A fixture writes a precise byte sequence, so a failure can be attributed to a known header, payload, or boundary. A process harness proves that independently running code can terminate at a real handoff point and restart after the operating system releases its handles. Neither is a physical power-loss experiment.

Create `crates/broker-core/tests/recovery.rs`. Keep the existing `tests/common/mod.rs` from Chapter 7; its `TestDir` creates an exclusive temporary directory and removes only that directory on destruction. The helpers below create a child broker directory, initialize it through the real protocol, and optionally append records without synchronization. Those records are deliberately complete but unconfirmed fixtures.

`overwrite`, `shorten`, and `suffix` are test-only file edits performed after the owning logs have been dropped. They are not service APIs. `cut` returns a closure with its own selected publication point. Its `move` capture copies that small enum into the closure, so the caller may invoke it later without borrowing the function's local parameter.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Test the metadata layout independently of its decoder

A round trip alone can let an encoder and decoder agree on the same wrong layout. The golden array checks exact byte positions and the independently calculated CRC. For epoch 7 with the small configuration, the checksum is `82 b5 3a 19`. The assertions also check that decoded configuration values are not swapped.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

The next test exercises truncation at every metadata byte, one-bit damage at every position, an extra trailing byte, and several invalid fields with a newly correct checksum. The last cases distinguish integrity checking from semantic compatibility. `fill(0)` mutates an already bounded slice; no resize or allocation occurs.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Initialization is not recovery with an empty default

The initializer creates all four partitions and publishes epoch 1. Reopening after dropping their owners yields epoch 2. An existing empty leaf is still not an absent leaf; rejecting it avoids ambiguity about what the caller intended to initialize.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

The two authority tests are intentionally similar but test different failure classes. Missing metadata must not promote a valid candidate. Corrupt metadata must not fall back to a valid candidate either. Preserving the candidate after refusal also makes it available for diagnosis without making it authoritative.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Keep complete data and rebuild disposable indexes

Five three-byte records at cap 102 create segment bases 0, 2, and 4. Stride 2 produces one sparse entry per file. Reading from each start offset compares the reconstructed logical coverage, and a subsequent append checks that ordinary operation resumes at offset 5. A second recovery must see six records, not reuse the last offset.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Cut a record at every byte boundary

The initial `cat` occupies bytes 32 through 66, making the next record start at 67. The encoded `dog` is 35 bytes. Test every prefix length $c$ from 0 through 35. The expected repaired size is:

$$
L_{recovered} = \begin{cases}
67, & 0 \le c < 35,\\
102, & c=35.
\end{cases}
$$

At $c=0$, there is no incomplete suffix and thus no repair report. At $1\le c<35$, exactly one record truncation is reported. At $c=35$, the complete record is retained. Appending `owl` after each case checks both the logical next offset and the physical cursor boundary; the resulting length must be the header plus the exact number of 35-byte records, with no hole.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

The next group holds the repair policy against three superficially similar failures: an incomplete immutable file, a complete corrupt record header, and a complete corrupt payload. The bytes remain unchanged after refusal. A blanket “truncate after any parse error” implementation would fail these tests.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Plan the entire broker before changing one partition

Partition 0 has a repairable active tail. Partition 3 has a complete corrupt segment header. Recovery must discover the latter before modifying the former. The unchanged epoch is an additional check that no session was published from a partial plan.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

Topology and namespace tests cover a missing middle segment, a renamed base leaving a gap, a noncanonical filename, an unknown root entry, a missing partition, and a directory masquerading as the metadata candidate. Each loop builds a fresh broker; cases cannot contaminate one another.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

The native targets for this course are Unix-style systems. `#[cfg(unix)]` includes this one symlink test there and excludes it on other targets; it is conditional compilation, not a runtime branch. The actual native Mac run should include it. The original file is moved outside the partition and a symlink is placed at its former name; recovery must refuse the link rather than follow it.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Separate an empty rotation from a missing initial header

A valid empty tail already has its complete 32-byte header. Reuse it for the next record instead of creating another file at the same base. For a short rotated header, test every prefix length from 0 to 31. Header completion restores the file to 32 bytes, and the next record still receives offset 2.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

The corresponding refusal tests ensure the exception does not become a general header-recreation feature. A short initial header contradicts published initialization. A short rotated header with a wrong known prefix contradicts the expected rotation. Neither may be silently replaced.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

A valid checksum does not make every record valid at every location. At byte 67 in a 102-byte-cap segment, a 36-byte record is impossible. Likewise, an expected offset of 1 cannot be replaced by a checksummed offset of 9. Both tests preserve their full fixture lengths after refusal.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Counter exhaustion must precede mutation

The test writes a maximum-valued authoritative epoch and a repairable data suffix. Startup must fail before truncating that suffix. This checks ordering, not only whether `checked_add` eventually returns an error. A future refactor that increments after repair would be observably different.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Stop first creation around publication

Every metadata publication boundary is a distinct case. Before rename, the initialized leaf exists but has no authority; both reuse through `initialize` and fallback through `recover` are refused. After rename, the authority is epoch 1 and a successful recovery publishes epoch 2. A separate stop after `LayoutSynced` covers interruption before even creating the candidate.

These callbacks return ordinary errors, so Rust drops their local handles. They construct the same named-file states as stops at those boundaries, but are not claimed to reproduce every effect of abrupt process death. The real process harness later addresses that separate behavior.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

Restart publication differs because a valid old authority already exists. Before replacement, the next successful startup may use the not-yet-served candidate epoch. After replacement, it must skip past the newly visible authority even though the interrupted attempt never returned a session. All cases retain the complete `cat` record.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Interrupt a repair, then recover again

Both selected interruptions leave the truncated file at 67 bytes. One is before its explicit synchronization; the other is after. A fresh recovery finds no remaining partial record, synchronizes the complete prefix through normal reopening, and publishes epoch 2. There is no second destructive adjustment and no attempt to restore the lost incomplete suffix.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Inject real-operation failures, not only phase stops

`FailingSync` overrides the new default methods when needed. `Option<PathBuf>::as_deref()` borrows its stored path as `Option<&Path>` without cloning it. Comparing that borrow selects one particular directory operation. All unselected operations call the real synchronization primitive; the adapter does not supply a fake storage engine.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

A metadata-file sync failure must leave the old epoch authoritative. A root-directory sync failure occurs after rename, so it can leave the new epoch visible while still returning no session. The next startup therefore produces different successor values in these two cases.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

## Recheck the producer boundary after adding directory sync

A 67-byte segment holds exactly one three-byte record. Recovered logs start clean, so `WorkerHandle::start` does not trigger the injected partition-directory failure. The first publish dirties only the existing file and succeeds. The second publish rotates, then fails while synchronizing the new name. Its reply is `Failed`, its owner is joined as failed, and a later recovery may retain both complete records.

The five-second receive timeout is a test watchdog, not an I/O latency promise. The assertion requires an explicit failed reply, not mere timeout or disconnection.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

A session's directory ownership is not released when only some logs are dropped. The final surviving log retains an `Arc<DirectoryGuard>` share and prevents a second recovery. This checks resource lifetime independently of record count.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

A valid candidate with an even larger epoch is still not authority. The candidate-100/current-9 test must publish 10, not 101. Separately, a failed segment sync must leave epoch 1 unchanged, because metadata publication follows synchronization rather than preceding it.

```rust title="crates/broker-core/tests/recovery.rs"
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

```

Finish the test file with complete segment-header corruption, an impossible short record prefix, and a complete but wrong partition identity. These exercise refusal paths before any permission to truncate is returned.

```rust title="crates/broker-core/tests/recovery.rs"
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
```

## Run the focused and cumulative Rust checks

After all new files and replacements are in the Chapter 9 workspace, run:

```sh
cargo test -p broker-core --test recovery
cargo test -p broker-core --test storage
cargo test -p broker-core --test group_commit
cargo test --workspace --all-targets
```

On the native Unix target, the focused target supplies **31 tests**. The required cumulative suite supplies **188 tests** across library, integration, and example targets, including the earlier Tokio examples. These counts describe supplied tests, not observed passes. Exact Cargo presentation can vary, and test output order is intentionally unspecified. Passing the new target while omitting the older storage/group-commit targets would miss regressions in the synchronization changes.

For a focused failure, Cargo accepts a name filter:

```sh
cargo test -p broker-core --test recovery every_final_record_cut -- --nocapture
cargo test -p broker-core --test recovery interrupted_epoch_publication -- --nocapture
```

A failed native directory synchronization is not a reason to remove the operation from the implementation. Inspect the error chain, selected filesystem, permissions, and whether the path is a directory. The course does not assert support for every mounted filesystem. No new compiler feature is required beyond the existing edition-2024 workspace and the standard file-lock API already used in Chapter 7; compatible maintained toolchains remain acceptable.

## Kill a real owner between a write and synchronization

Create `crates/broker-core/examples/recovery_probe.rs`. It has three modes. `init` performs explicit first creation. `hold` recovers, moves partition 0 into the real Chapter 9 owner, and submits a one-record batch. Its special synchronization adapter prints a boundary marker only after the append is complete, then parks without returning success. `inspect` performs a new full recovery and reads the resulting records.

`inspect` is not a read-only metadata peek: it publishes a new startup epoch. Repeating it changes the epoch again. Its small read limit is suitable for this fixed one-record demonstration, not a complete administrative dump of arbitrary logs.

`BeforeSync` has no timer and never reports successful synchronization. Dropping an owner handle here would block while joining its parked thread, so this mode is intended for the following terminating process harness, not for a graceful shutdown demonstration. No external service or network setup is needed.

```rust title="crates/broker-core/examples/recovery_probe.rs"
use std::error::Error;
use std::io::{self, Write};
use std::path::Path;
use std::thread;
use std::time::Duration;

use broker_core::io::StorageFile;
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::SyncIo;
use broker_core::types::{Epoch, Offset};
use broker_core::worker::{BatchConfig, WorkerHandle};

struct BeforeSync { epoch: Epoch }

impl SyncIo for BeforeSync {
    fn sync(&mut self, base: Offset, _file: &mut dyn StorageFile) -> io::Result<()> {
        // This adapter never reports a successful sync. The process harness kills us here.
        writeln!(io::stdout(), "CUT epoch={} base={} before-data-sync", self.epoch.get(), base.get())?;
        io::stdout().flush()?;
        loop { thread::park(); }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 3 {
        return Err(io::Error::other("usage: recovery_probe init|hold|inspect DIRECTORY").into());
    }
    let mode = args[1].to_str().ok_or_else(|| io::Error::other("mode must be text"))?;
    let path = Path::new(&args[2]);
    match mode {
        "init" => {
            let session = recovery::initialize(path, StorageConfig::new(102, 2, 8)?)?;
            println!("initialized epoch={}", session.epoch().get());
        }
        "hold" => {
            let (epoch, mut logs) = recovery::recover(path)?.into_parts();
            let log = logs.remove(0);
            let owner = WorkerHandle::start(log, BeforeSync { epoch },
                BatchConfig::new(1, 1000, Duration::from_secs(1))?, 4, 4)?;
            let ticket = owner.client().try_publish(b"cat".to_vec())
                .map_err(|_| io::Error::other("unexpected fixture admission failure"))?;
            let result = ticket.recv()?;
            result?;
            owner.join()?;
            return Err(io::Error::other("blocking fault gate unexpectedly returned").into());
            // `logs` keeps the other partitions' guard shares alive until process death.
        }
        "inspect" => {
            let (epoch, mut logs) = recovery::recover(path)?.into_parts();
            println!("epoch={}", epoch.get());
            for log in &mut logs {
                let end = log.written_end();
                let batch = log.read(Offset::new(0), end, ReadLimits::new(8, 1000)?)?;
                let first = batch.records.first().map(|record| {
                    record.payload().iter().map(|b| format!("{b:02x}")).collect::<String>()
                }).unwrap_or_else(|| "-".to_owned());
                println!("partition={} end={} first={first}", log.partition().get(), end.get());
            }
        }
        _ => return Err(io::Error::other("unknown mode; use init, hold, or inspect").into()),
    }
    Ok(())
}
```

The printed record payload is hex, so arbitrary bytes need not be valid UTF-8. `636174` is the byte sequence for `cat`. The other three logs remain owned by the process while partition 0 runs; their guard shares are released by process termination along with the owning thread's resources.

Create `checks/check_recovery.py`. It uses a fresh temporary parent and an absent child directory. It first initializes through the Rust binary, starts `hold` as a separate process, waits for its exact boundary marker, kills that process, and waits for its termination before starting recovery. The stdout marker is a synchronization observation, not an arbitrary sleep. A timeout is only a watchdog for a stuck test.

```python title="checks/check_recovery.py"
#!/usr/bin/env python3
"""Kill the Rust example at a completed-write / not-yet-sync boundary, then reopen."""
from __future__ import annotations

import argparse
from pathlib import Path
import select
import signal
import subprocess
import tempfile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/examples/recovery_probe"))
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit("Build first: cargo build -p broker-core --example recovery_probe")
    with tempfile.TemporaryDirectory(prefix="broker-recovery-") as temporary:
        root = Path(temporary) / "broker"  # Initialization requires this child to be absent.
        initialized = subprocess.run(
            [str(binary), "init", str(root)], check=True, capture_output=True, text=True, timeout=15
        )
        if initialized.stdout.strip() != "initialized epoch=1":
            raise RuntimeError(f"unexpected initialization: {initialized.stdout!r}")
        child = subprocess.Popen(
            [str(binary), "hold", str(root)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        try:
            assert child.stdout is not None
            readable, _, _ = select.select([child.stdout], [], [], 15)
            if not readable:
                raise RuntimeError("watchdog: child did not reach its synchronization gate")
            line = child.stdout.readline().strip()
            if line != "CUT epoch=2 base=0 before-data-sync":
                if child.poll() is None:
                    child.kill()
                _, errors = child.communicate(timeout=10)
                raise RuntimeError(f"unexpected child boundary: {line!r}; exit={child.returncode}; {errors}")
            child.kill()
            _, errors = child.communicate(timeout=10)
            if child.returncode != -signal.SIGKILL:
                raise RuntimeError(f"child was not killed as expected: {child.returncode}; {errors}")
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=10)
        recovered = subprocess.run(
            [str(binary), "inspect", str(root)], check=True, capture_output=True, text=True, timeout=15
        )
        expected = ["epoch=3", "partition=0 end=1 first=636174"]
        expected.extend(f"partition={n} end=0 first=-" for n in range(1, 4))
        if recovered.stdout.splitlines() != expected:
            raise RuntimeError(f"unexpected recovered state: {recovered.stdout!r}")
        print("process-crash check: unconfirmed cat retained; epoch 2 -> 3")
        print("This is not a power-loss test.")


if __name__ == "__main__":
    main()
```

Build the Rust example, then run the Python harness from the workspace root:

```sh
cargo build -p broker-core --example recovery_probe
python3 checks/check_recovery.py
```

Expected final harness output:

```text
process-crash check: unconfirmed cat retained; epoch 2 -> 3
This is not a power-loss test.
```

With a nondefault Cargo target directory, pass the actual built executable using `--binary /path/to/recovery_probe`. The script does not install tools, publish anything, or operate on an existing data directory. Its `finally` block kills and waits for a child still running after a failed assertion, avoiding a stranded synchronization gate.

The process sequence is explicit: initialization publishes 1; the holding process publishes 2 before its owner accepts a record; the owner finishes writing `cat` but cannot reply successfully while the adapter is parked; termination releases the directory ownership; recovery finds the complete record, synchronizes it, publishes 3, and reads it. No assertion says the original producer received success. No test deliberately removes machine power.

## Diagnose a disagreement at its first boundary

If the cut-at-every-byte test reports a repair for prefix length zero, the scanner is treating a clean end as an incomplete record. If prefix length 35 loses `dog`, the complete-record path is falling through into truncation or using an off-by-one comparison. If the post-recovery file has a hole, inspect the explicit seek after truncation and the absolute seek in append.

If metadata checks pass round trips but fail the golden array, inspect byte intervals, endian conversion, reserved bytes, and the checksum span. If a candidate is adopted after missing/corrupt authority, remove that fallback; do not weaken the test. If a failed post-rename startup reuses epoch 2 after reading authority 2, the caller is using stale in-memory metadata instead of rereading disk.

If the process harness never sees `CUT`, run the built binary's `init` on a fresh temporary child and inspect the returned error chain. The holding mode must use a recovered clean log: placing the blocking adapter in initial recovery would block before the intended append boundary. If the child exits early, its stderr and exit status are diagnostic; an empty stdout line is not the boundary marker. If the next process reports a busy directory, verify the previous process was actually waited for and no other owner is running. Never unlink `.broker.lock` as a workaround.

## One useful variation

Change the process fixture from `cat` to the empty payload and update its expected hex output. Predict the file length, recovered end, and epoch sequence before running it. The record still has its 32-byte header, so it is a real record and advances the offset. An empty payload is not an empty file and must not be confused with an empty rotated tail. Keep this as an optional variation rather than changing the reference fixture.

This completes the storage startup boundary. [The roadmap](https://25d-228.github.io/rust-learning/index.html#chapter-11-bounded-delivery-and-out-of-order-completion) continues with **Chapter 11 — Bounded delivery and out-of-order completion**, where the synchronized visible prefix becomes the input to a consumer window rather than merely a readable range of records.

