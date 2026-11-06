# Chapter 14 — Safe retention and disk admission
## Part 2 — Segment reclamation and restart reconstruction

> **Learning objectives**
> Reclaim only a checkpoint-safe closed prefix. Publish a durable floor before deleting files. Reconstruct a bounded retained interval after every interruption without reusing offsets, promoting candidates, or repairing around a retained gap.

**Implementation time:** shared with the 4.5-hour estimate in [Part 1](https://25d-228.github.io/rust-learning/chapter-14.html). This part replaces `storage/log.rs` and `storage/recovery.rs` completely. Continue with [Part 3](https://25d-228.github.io/rust-learning/chapter-14-part-03.html) before building; [Part 4](https://25d-228.github.io/rust-learning/chapter-14-part-04.html) supplies checks for the boundaries introduced here.

## Make the floor a starting condition, not an inferred observation

Previously strict reopening began at offset zero. Retained storage must instead begin at the exact floor loaded from validated metadata. It is not safe to say “the smallest filename is probably the new start”: a missing file caused by damage would then look like successful retention.

The low-level `open_strict` entry point retains its old floor-zero behavior for earlier demonstrations. Startup uses a private `open_at` that receives an authoritative floor. It verifies continuous segment bases from that floor and rebuilds sparse indexes. No public API lets a client choose a more convenient floor after encountering a missing segment.

A second change bounds the transitional state. Normal storage has at most `max_segments` files. Maintenance may temporarily have one more, whose last segment must be empty. That extra slot is paired with the 32-byte emergency header from Part 1, not an unlimited exception to either cap. Normal appends refuse while this transient count remains above the normal cap.

## Keep the log's ownership explicit

The log owns its file handles, disposable indexes, dirty flags, root directory guard, retained authority, and optional shared disk ledger. Raw storage fixtures created without startup remain unbudgeted, preserving earlier low-level examples; initialized or recovered broker logs always carry a persisted quota. The optional form is a teaching compatibility boundary, not a network-facing way to disable admission.

## Discovery, construction, and floor-aware reopening

Keep canonical filename parsing and bounded discovery. `open_at` starts `expected` at the supplied floor and requires each segment base to equal the previous segment's decoded end. The `maintenance` flag adds exactly `usize::from(true) == 1` to the discovery limit; `false` adds zero. It does not change the per-file byte cap.

```rust title="crates/broker-core/src/storage/log.rs"
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::budget::{DiskBudget, DiskPermit, DiskState};
use crate::codec::{HEADER_LEN, SegmentHeader};
use crate::io::{create_file, open_file};
use crate::types::{Epoch, GROUP_COUNT, Offset, PartitionId};
use super::checkpoint::RecoveredProgress;
use super::directory::DirectoryGuard;
use super::error::StorageError;
use super::segment::{ReadBatch, ReadLimits, ReadStop, Segment, SegmentInfo, StorageConfig};
use super::sync::SyncIo;
use super::retention::{self, Retained, Retention};

pub fn segment_name(base: Offset) -> String { format!("{:020}.seg", base.get()) }

pub(crate) fn discover(root: &Path, maximum: usize) -> Result<Vec<(Offset, PathBuf)>, StorageError> {
    let directory = fs::read_dir(root).map_err(|e| StorageError::path("list partition", root, e))?;
    let mut paths = Vec::new();
    for entry in directory {
        let entry = entry.map_err(|e| StorageError::path("read directory entry", root, e))?;
        let path = entry.path();
        let kind = entry.file_type().map_err(|e| StorageError::path("inspect entry", &path, e))?;
        if !kind.is_file() { return Err(StorageError::Name(path)); }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { return Err(StorageError::Name(path)); };
        let Some(digits) = name.strip_suffix(".seg") else { return Err(StorageError::Name(path)); };
        if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(StorageError::Name(path));
        }
        let number: u64 = digits.parse().map_err(|_| StorageError::Name(path.clone()))?;
        if paths.len() == maximum { return Err(StorageError::SegmentLimit); }
        paths.push((Offset::new(number), path));
    }
    paths.sort_by_key(|(base, _)| *base);
    if paths.is_empty() { return Err(StorageError::Topology("partition has no segment files")); }
    Ok(paths)
}

pub struct PartitionLog {
    // Declaration order closes segment handles before dropping this log's guard share.
    segments: Vec<Segment<File>>,
    dirty: Vec<bool>,
    directory_dirty: bool,
    root: PathBuf,
    partition: PartitionId,
    config: StorageConfig,
    poisoned: bool,
    recovered_progress: Option<RecoveredProgress>,
    retention: Option<Retention>,
    disk: Option<DiskBudget>,
    _directory: Arc<DirectoryGuard>,
}

impl PartitionLog {
    pub fn create(directory: Arc<DirectoryGuard>, partition: PartitionId, config: StorageConfig)
        -> Result<Self, StorageError>
    {
        let root = directory.root().join(format!("partition-{}", partition.get()));
        fs::create_dir(&root).map_err(|e| StorageError::path("create partition", &root, e))?;
        let header = SegmentHeader { partition, base: Offset::new(0) };
        let path = root.join(segment_name(header.base));
        let file = create_file(&path).map_err(|e| StorageError::path("create segment", &path, e))?;
        let segment = Segment::create(file, header, config)?;
        Ok(Self { segments: vec![segment], dirty: vec![true], directory_dirty: true, root, partition, config,
            poisoned: false, recovered_progress: None, retention: None, disk: None, _directory: directory })
    }

    pub fn open_strict(directory: Arc<DirectoryGuard>, partition: PartitionId, config: StorageConfig)
        -> Result<Self, StorageError>
    {
        Self::open_at(directory, partition, config, Offset::new(0), false)
    }

    pub(crate) fn open_at(directory: Arc<DirectoryGuard>, partition: PartitionId,
        config: StorageConfig, floor: Offset, maintenance: bool) -> Result<Self, StorageError>
    {
        let root = directory.root().join(format!("partition-{}", partition.get()));
        let paths = discover(&root, config.max_segments() + usize::from(maintenance))?;
        let mut expected = floor;
        let mut segments = Vec::new();
        let count = paths.len();
        for (i, (base, path)) in paths.into_iter().enumerate() {
            if base != expected { return Err(StorageError::Topology("segment bases leave a gap or overlap")); }
            let file = open_file(&path).map_err(|e| StorageError::path("open segment", &path, e))?;
            let segment = Segment::open_strict(file, SegmentHeader { partition, base }, config)?;
            let info = segment.info();
            if i + 1 < count && info.end == info.base {
                return Err(StorageError::Topology("only the last segment may be empty"));
            }
            if count > config.max_segments() && i + 1 == count && info.base != info.end {
                return Err(StorageError::Topology("the extra maintenance segment must be empty"));
            }
            expected = info.end;
            segments.push(segment);
        }
        let dirty = vec![true; segments.len()];
        Ok(Self { segments, dirty, directory_dirty: true, root, partition, config, poisoned: false, recovered_progress: None, retention: None, disk: None, _directory: directory })
    }

```

## Install authority only after measuring the files

`live_bytes` sums the decoded segment lengths, not payload lengths or index sizes. `install_retention` checks that the first owned segment agrees with the metadata floor and creates a fresh ledger. Recovered reservations start at zero because no old command queue survives a process restart. The existing non-cloneable progress seed is still consumed once by durable delivery setup.

```rust title="crates/broker-core/src/storage/log.rs"
    pub fn partition(&self) -> PartitionId { self.partition }
    pub fn retained_floor(&self) -> Offset { self.segments[0].info().base }
    pub fn disk_state(&self) -> Option<DiskState> { self.disk.as_ref().map(DiskBudget::state) }

    fn live_bytes(&self) -> u64 { self.segments.iter().map(|s| s.info().bytes).sum() }

    pub(crate) fn install_retention(&mut self, value: Retention) -> Result<(), StorageError> {
        if value.partition != self.partition || value.floor != self.retained_floor() {
            return Err(StorageError::Metadata("retained floor does not match the owned log"));
        }
        self.disk = Some(DiskBudget::new(value.quota, self.live_bytes())?);
        self.retention = Some(value);
        Ok(())
    }

    pub(crate) fn disk_admission(&self) -> Option<DiskBudget> { self.disk.clone() }
    pub(crate) fn stop_admission(&self) { if let Some(disk) = &self.disk { disk.stop(); } }


    pub(crate) fn install_progress(&mut self, epoch: Epoch, next: [Offset; GROUP_COUNT as usize]) {
        self.recovered_progress = Some(RecoveredProgress { epoch, next });
    }

    pub(crate) fn take_progress(&mut self, epoch: Epoch)
        -> Result<(PathBuf, [Offset; GROUP_COUNT as usize]), StorageError>
    {
        let seed = self.recovered_progress.as_ref()
            .ok_or(StorageError::Metadata("durable delivery requires an initialized/recovered log"))?;
        if seed.epoch != epoch {
            return Err(StorageError::Metadata("lease epoch differs from the startup session"));
        }
        let seed = self.recovered_progress.take().expect("validated startup seed");
        Ok((self._directory.root().to_path_buf(), seed.next))
    }

    pub fn written_end(&self) -> Offset { self.segments.last().expect("log has a segment").info().end }
    pub fn segment_infos(&self) -> Vec<SegmentInfo> { self.segments.iter().map(Segment::info).collect() }

    fn healthy(&self) -> Result<(), StorageError> {
        if self.poisoned { Err(StorageError::Poisoned) } else { Ok(()) }
    }

```

## Separate a rotation's physical work from its reservation

Public low-level `rotate` reserves normal capacity first. `rotate_file` performs the actual file creation and header write; its callers must already own capacity. The helper allows an extra count slot only for maintenance, preserves an already empty active file, and poisons the log before a fallible mutation.

The expression `option.map(...).transpose()` bridges the optional ledger and a fallible reservation. `None` becomes `Ok(None)`; `Some(Ok(permit))` becomes `Ok(Some(permit))`; `Some(Err(reason))` becomes `Err(reason)`. This preserves the unbudgeted low-level mode without ignoring reservation errors in managed storage.

```rust title="crates/broker-core/src/storage/log.rs"
    pub fn rotate(&mut self) -> Result<(), StorageError> {
        self.healthy()?;
        let tail = self.segments.last().expect("log has a segment").info();
        if tail.base == tail.end { return Ok(()); }
        if self.segments.len() >= self.config.max_segments() { return Err(StorageError::SegmentLimit); }
        let permit = self.disk.as_ref().map(|disk| disk.reserve_rotation(false))
            .transpose().map_err(|_| StorageError::DiskFull)?;
        match self.rotate_file(false) {
            Ok(()) => { if let Some(permit) = permit { permit.settle(HEADER_LEN as u64); } Ok(()) }
            Err(error) => { self.stop_admission(); Err(error) }
        }
    }

    // The caller reserves bytes before entering. Maintenance has one extra count slot.
    fn rotate_file(&mut self, maintenance: bool) -> Result<(), StorageError> {
        self.healthy()?;
        let tail = self.segments.last().expect("log has a segment").info();
        if tail.base == tail.end { return Ok(()); }
        if self.segments.len() >= self.config.max_segments() + usize::from(maintenance) {
            return Err(StorageError::SegmentLimit);
        }
        self.poisoned = true;
        let path = self.root.join(segment_name(tail.end));
        let file = create_file(&path).map_err(|e| StorageError::path("create next segment", &path, e))?;
        let segment = Segment::create(file, SegmentHeader { partition: self.partition, base: tail.end }, self.config)?;
        self.segments.push(segment);
        self.dirty.push(true);
        self.directory_dirty = true;
        self.poisoned = false;
        Ok(())
    }

```

## Refuse before mutation; settle only completed growth

`check_append` retains payload-fit, successor-overflow, and segment-count checks. `append_reserved` requires a permit belonging to this exact ledger. It captures total file bytes before mutation, rotates if required, writes the record, and settles the measured difference only after success. A healthy preflight refusal drops the still-unspent permit. A write/rotation failure stops admission and leaves recovery to inspect any prefix that reached the filesystem.

The permit is passed by value, not borrowed: the append must consume its unique settlement permission. A caller cannot reuse it for another record or retain it after settlement.

```rust title="crates/broker-core/src/storage/log.rs"
    pub(crate) fn check_append(&self, payload_len: usize) -> Result<usize, StorageError> {
        self.healthy()?;
        let size = self.config.check_record(payload_len)?;
        let tail = self.segments.last().expect("log has a segment").info();
        tail.end.next().map_err(|e| StorageError::format(tail.bytes, crate::error::FormatError::Identity(e)))?;
        if self.segments.len() > self.config.max_segments()
            || (size as u64 > self.config.segment_bytes() - tail.bytes
                && self.segments.len() == self.config.max_segments())
        {
            return Err(StorageError::SegmentLimit);
        }
        Ok(size)
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<Offset, StorageError> {
        let size = self.check_append(payload.len())?;
        let permit = self.disk.as_ref().map(|disk| disk.reserve_append(size))
            .transpose().map_err(|_| StorageError::DiskFull)?;
        self.append_reserved(payload, permit)
    }

    pub(crate) fn append_reserved(&mut self, payload: &[u8], permit: Option<DiskPermit>)
        -> Result<Offset, StorageError>
    {
        let size = self.check_append(payload.len())?;
        match (&self.disk, &permit) {
            (None, None) => {},
            (Some(disk), Some(permit)) if permit.belongs_to(disk) => {},
            _ => return Err(StorageError::Config("append is missing its own partition's disk reservation")),
        }
        let before = self.live_bytes();
        let result = (|| {
            let tail = self.segments.last().expect("log has a segment").info();
            if size as u64 > self.config.segment_bytes() - tail.bytes { self.rotate_file(false)?; }
            self.poisoned = true;
            let last = self.segments.len() - 1;
            self.dirty[last] = true;
            self.segments[last].append(payload)
        })();
        match result {
            Ok(offset) => {
                if let Some(permit) = permit { permit.settle(self.live_bytes() - before); }
                self.poisoned = false;
                Ok(offset)
            }
            Err(error) => { self.stop_admission(); Err(error) }
        }
    }

```

## Derive a prefix from persisted frontiers

The public client will not supply a floor; the owner supplies its two persisted frontiers and epoch. `retain_inner` verifies their relation to the old floor and written end. When the whole active segment is safe, it first reserves the emergency header, creates the replacement, and synchronizes both file bytes and filename.

Then `take(infos.len() - 1)` excludes the active segment. `take_while` chooses only the initial closed prefix whose ends are safe. The new floor is the base of the first kept file. The struct update `Retention { epoch, floor, ..old }` replaces the two advancing fields and keeps partition and quota unchanged. All these fields are numeric `Copy` values; it does not duplicate file ownership.

```rust title="crates/broker-core/src/storage/log.rs"
    pub(crate) fn retain<S: SyncIo + ?Sized>(&mut self, epoch: Epoch,
        persisted: [Offset; GROUP_COUNT as usize], io: &mut S) -> Result<Retained, StorageError>
    {
        let result = self.retain_inner(epoch, persisted, io);
        if result.is_err() { self.poisoned = true; self.stop_admission(); }
        result
    }

    fn retain_inner<S: SyncIo + ?Sized>(&mut self, epoch: Epoch,
        persisted: [Offset; GROUP_COUNT as usize], io: &mut S) -> Result<Retained, StorageError>
    {
        self.healthy()?;
        let old = self.retention.ok_or(StorageError::Metadata("retention needs startup authority"))?;
        let disk = self.disk.clone().ok_or(StorageError::Metadata("retention needs a disk ledger"))?;
        let safe = *persisted.iter().min().expect("two groups");
        if old.epoch > epoch || persisted.iter().any(|p| *p < old.floor || *p > self.written_end()) {
            return Err(StorageError::Topology("invalid persisted retention frontiers"));
        }
        let tail = self.segments.last().expect("log has a segment").info();
        let rotated = tail.base != tail.end && tail.end <= safe;
        if rotated {
            let permit = disk.reserve_rotation(true).map_err(|_| StorageError::DiskFull)?;
            self.rotate_file(true)?;
            permit.settle(HEADER_LEN as u64);
            // Publish the replacement active segment before authorizing deletion of its predecessor.
            self.synchronize(io)?;
        }
        let infos = self.segment_infos();
        let count = infos.iter().take(infos.len() - 1).take_while(|s| s.end <= safe).count();
        if count == 0 {
            return Ok(Retained { before: old.floor, floor: old.floor, removed_segments: 0,
                removed_bytes: 0, rotated, disk: disk.state() });
        }
        let floor = infos[count].base;
        if floor <= old.floor || floor > safe {
            return Err(StorageError::Topology("retained floor must advance within both checkpoints"));
        }
        let value = Retention { epoch, floor, ..old };
        self.poisoned = true;
        retention::publish(self._directory.root(), value, io)?;
        self.retention = Some(value);
        // No response borrows a file. Dropping these owners closes every old handle first.
        self.segments.drain(..count).for_each(drop);
        self.dirty.drain(..count).for_each(drop);
        let mut bytes = 0;
        for info in &infos[..count] {
            let path = self.root.join(segment_name(info.base));
            io.remove_segment(&path).map_err(|e| StorageError::path("unlink retired segment", &path, e))?;
            bytes += info.bytes;
        }
        io.sync_directory(&self.root)
            .map_err(|e| StorageError::path("synchronize retired filenames", &self.root, e))?;
        // Keep the old charge until ALL removals and their directory synchronization succeed.
        disk.reclaimed(bytes);
        self.poisoned = false;
        Ok(Retained { before: old.floor, floor, removed_segments: count,
            removed_bytes: bytes, rotated, disk: disk.state() })
    }

```

## Leave synchronized visibility and bounded copying intact

The synchronization and read paths remain complete here because the file is replaced as a unit. Synchronization still covers every dirty segment and newly created name. A read below the new floor returns `Range` before poisoning storage. Valid reads copy owned records and enforce the existing record-count and encoded-byte limits; they do not lend file-backed slices to callers.

Retention does not send producer replies or advance `visible_end`. Its eligible offsets came from persisted consumer progress, which was already bounded by visibility. Synchronizing during a maintenance rotation does not silently change the group-commit reply contract.

```rust title="crates/broker-core/src/storage/log.rs"
    pub fn synchronize<S: SyncIo + ?Sized>(&mut self, io: &mut S) -> Result<Offset, StorageError> {
        self.healthy()?;
        self.poisoned = true;
        for (segment, dirty) in self.segments.iter_mut().zip(self.dirty.iter_mut()) {
            if *dirty {
                segment.synchronize(io)?;
                *dirty = false;
            }
        }
        if self.directory_dirty {
            io.sync_directory(&self.root)
                .map_err(|e| StorageError::path("synchronize partition directory", &self.root, e))?;
            self.directory_dirty = false;
        }
        self.poisoned = false;
        Ok(self.written_end())
    }

    pub fn read(&mut self, start: Offset, upper: Offset, limits: ReadLimits)
        -> Result<ReadBatch, StorageError>
    {
        self.healthy()?;
        let floor = self.segments[0].info().base;
        let end = self.written_end();
        if start < floor || upper < start || upper > end {
            return Err(StorageError::Range { start, upper, floor, end });
        }
        let mut batch = ReadBatch::empty(start, limits);
        if start == upper { return Ok(batch); }
        let right = self.segments.partition_point(|segment| segment.info().base <= start);
        let first = right.checked_sub(1).ok_or(StorageError::Topology("no starting segment"))?;
        // Stop on any unexpected read failure too; no partial batch escapes through Err.
        self.poisoned = true;
        for segment in &mut self.segments[first..] {
            if batch.next == upper || batch.stop != ReadStop::End { break; }
            segment.read_into(upper, limits, &mut batch)?;
        }
        if batch.next != upper && batch.stop == ReadStop::End {
            return Err(StorageError::Topology("scan ended before requested upper bound"));
        }
        self.poisoned = false;
        Ok(batch)
    }
}
```

## Why the deletion order has two different commit points

Consider files with bases 0 and 2, each 102 bytes, and both persisted fronts at 4. Maintenance creates base 4, an empty 32-byte active file. The sequence is:

```text
old authority F=0; files 0,2                     L=204
create and sync file 4, sync partition directory L=236
publish and sync floor F=4                      L=236
close handles/index owners for files 0,2        L=236
unlink file 0, unlink file 2                    L=236 charged
sync partition directory                       L=236 charged
return deleted-byte credit                     L=32
```

The floor publication commits **logical unavailability**. Disk credit is released only after the subsequent cleanup completes. Those points need not coincide. If a deletion fails after the floor publishes, the owner stops rather than making the old records readable again or crediting uncertain space.

`drain(..count).for_each(drop)` consumes and destroys the obsolete `Segment<File>` values before unlinking their paths. The parallel dirty vector is drained over the same prefix. A previously returned `OwnedRecord` owns its own encoded-record vector, so the client does not keep a hidden file handle alive. Rust's `remove_file` documentation explicitly warns that removal need not immediately release a file when other handles exist ([Rust `remove_file`](https://doc.rust-lang.org/std/fs/fn.remove_file.html)). This implementation closes all handles it owns; it does not claim control over unrelated processes or physical allocation.

A successful floor publication followed by a missing success reply is still a published floor. Never retry on the uncertain owner in an attempt to force the old operation to “finish.” Stop the session, keep the directory, and use validated recovery.

## Recovery is a read-only plan before any destructive action

Recovery reads broker metadata, all four retained authorities, all retained segment intervals, and all eight group checkpoints before changing record files or deleting obsolete files. For a planned partition, let $F$ be its authority and $E$ its complete recovered end. Every checkpoint must satisfy

$$
F\le P_g\le E.
$$

Files whose base is below `F` are obsolete leftovers. Their contents need not be valid records anymore; the validated authority says they are outside the retained interval. Their names must still be canonical regular files, their lengths remain bounded, and they still count toward the on-disk envelope until cleanup. Missing *subsets* below the floor are expected after interrupted deletion.

The file whose base equals `F` must exist. From there, bases and record offsets must be contiguous, checksums must validate, and only the narrowly defined incomplete final tail can be repaired. A corrupted retained record is not turned into an obsolete file by choosing a higher floor. A missing file in the middle is not repaired by scanning ahead.

Before deleting leftovers, recovery synchronizes the validated floor and checkpoint authorities and their root directory. It then applies approved record repairs, removes only the obsolete paths in the plan, synchronizes affected partition directories, reopens the retained logs, and builds fresh ledgers from their actual lengths. Finally it publishes the next broker epoch before returning any usable owner.

## Keep the existing repair vocabulary and observation points

The session still returns its epoch, logs, and any explicit record repairs. `PartitionPlan` now also owns retained authority and the finite list of obsolete paths. It does not treat cleanup as a record repair or invent a transaction journal. The original `read_at` rejects a surprise short read after captured length inspection rather than silently changing its plan.

```rust title="crates/broker-core/src/storage/recovery.rs"
use std::fs;
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::budget::{DiskQuota, ROTATION_HEADROOM};
use crate::codec::{self, HEADER_LEN, Probe, SegmentHeader};
use crate::error::FormatError;
use crate::io::{StorageFile, open_file, read_prefix, write_fully};
use crate::types::{Epoch, GROUP_COUNT, GroupId, Offset, PARTITION_COUNT, PartitionId};
use super::checkpoint::{self, Checkpoint};
use super::directory::DirectoryGuard;
use super::error::StorageError;
use super::log::{PartitionLog, discover, segment_name};
use super::metadata::{self, META_NAME, Metadata, PublicationStep, TEMP_NAME};
use super::segment::StorageConfig;
use super::retention::{self, Retention};
use super::sync::{FileSync, SyncIo};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairKind {
    TruncateRecord,
    CompleteSegmentHeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Repair {
    pub partition: PartitionId,
    pub base: Offset,
    pub before: u64,
    pub after: u64,
    pub kind: RepairKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupStep {
    LayoutSynced,
    PlanValidated,
    RepairApplied(Repair),
    RepairSynced(Repair),
    PartitionsSynced,
    Metadata(PublicationStep),
    Checkpoint { partition: PartitionId, group: GroupId, step: PublicationStep },
}

pub struct StorageSession {
    epoch: Epoch,
    logs: Vec<PartitionLog>,
    repairs: Vec<Repair>,
}

impl StorageSession {
    pub fn epoch(&self) -> Epoch { self.epoch }
    pub fn repairs(&self) -> &[Repair] { &self.repairs }

    pub fn into_parts(self) -> (Epoch, Vec<PartitionLog>) {
        (self.epoch, self.logs)
    }
}

struct Scan {
    end: Offset,
    repair: Option<Repair>,
}

struct PartitionPlan {
    partition: PartitionId,
    retained: Retention,
    obsolete: Vec<PathBuf>,
    end: Offset,
    repair: Option<Repair>,
}

fn notify<H>(root: &Path, after: &mut H, step: StartupStep) -> Result<(), StorageError>
where H: FnMut(StartupStep) -> io::Result<()> {
    after(step).map_err(|e| StorageError::path("interrupted startup", root, e))
}

fn read_at<F: StorageFile>(file: &mut F, position: u64, bytes: &mut [u8])
    -> Result<(), StorageError>
{
    file.seek(SeekFrom::Start(position)).map_err(|e| StorageError::io("seek recovery", position, e))?;
    let count = read_prefix(file, bytes).map_err(|e| StorageError::io("read recovery", position, e))?;
    if count != bytes.len() {
        // Incompleteness is decided from the captured length, not a surprise short read.
        return Err(StorageError::io("file changed during recovery", position,
            io::Error::new(io::ErrorKind::UnexpectedEof, "shorter than inspected length")));
    }
    Ok(())
}

```

## Retain the narrow incomplete-record policy

These helpers are unchanged in purpose. A partial record must match every fixed prefix byte available; a complete header must validate before its payload length is trusted. Only an active final tail can be truncated. Completing a partial rotated-segment header requires both a predecessor and a matching canonical prefix. The new retention floor does not relax any of those rules for retained data.

```rust title="crates/broker-core/src/storage/recovery.rs"
fn partial_record(raw: &[u8], position: u64, expected: Offset, config: StorageConfig)
    -> Result<(), StorageError>
{
    if HEADER_LEN as u64 > config.segment_bytes() - position {
        return Err(StorageError::Topology("partial header could not fit this segment"));
    }
    let reference = codec::encode_record(expected, &[])
        .map_err(|e| StorageError::format(position, e))?;
    let fixed = raw.len().min(16);
    if raw[..fixed] != reference[..fixed] {
        return Err(StorageError::Topology("partial record has an impossible fixed prefix"));
    }
    if raw.len() > 24 && raw[24..raw.len().min(28)].iter().any(|b| *b != 0) {
        return Err(StorageError::format(position, FormatError::Reserved));
    }
    if raw.len() >= 20 {
        let length = u32::from_be_bytes(raw[16..20].try_into().expect("complete length field"));
        let length = usize::try_from(length)
            .map_err(|_| StorageError::format(position, FormatError::LengthOverflow))?;
        let total = codec::record_len(length).map_err(|e| StorageError::format(position, e))?;
        if total as u64 > config.segment_bytes() - position {
            return Err(StorageError::Topology("partial record could not fit this segment"));
        }
    }
    Ok(())
}

fn scan<F: StorageFile>(file: &mut F, expected: SegmentHeader, config: StorageConfig,
    active: bool, rotated: bool) -> Result<Scan, StorageError>
{
    let length = file.file_len().map_err(|e| StorageError::io("inspect recovery file", 0, e))?;
    if length > config.segment_bytes() {
        return Err(StorageError::Topology("existing segment exceeds configured cap"));
    }
    let mut raw = [0; HEADER_LEN];
    if length < HEADER_LEN as u64 {
        read_at(file, 0, &mut raw[..length as usize])?;
        if !active || !rotated {
            return Err(StorageError::Incomplete { position: 0,
                required: HEADER_LEN as u64, available: length });
        }
        let canonical = codec::encode_segment(expected);
        if raw[..length as usize] != canonical[..length as usize] {
            return Err(StorageError::Topology("partial rotated header differs from its canonical prefix"));
        }
        return Ok(Scan { end: expected.base, repair: Some(Repair {
            partition: expected.partition, base: expected.base, before: length,
            after: HEADER_LEN as u64, kind: RepairKind::CompleteSegmentHeader,
        }) });
    }
    read_at(file, 0, &mut raw)?;
    let found = match codec::decode_segment(&raw).map_err(|e| StorageError::format(0, e))? {
        Probe::Complete { value, .. } => value,
        Probe::Incomplete { .. } => return Err(StorageError::Topology("full segment header decoded as incomplete")),
    };
    if found != expected { return Err(StorageError::Identity { expected, found }); }
    let mut position = HEADER_LEN as u64;
    let mut end = expected.base;
    while position < length {
        let available = length - position;
        let required;
        if available < HEADER_LEN as u64 {
            read_at(file, position, &mut raw[..available as usize])?;
            partial_record(&raw[..available as usize], position, end, config)?;
            required = HEADER_LEN as u64;
        } else {
            read_at(file, position, &mut raw)?;
            let header = codec::decode_record_header(&raw)
                .map_err(|e| StorageError::format(position, e))?;
            if header.offset() != end {
                return Err(StorageError::Offset { position, expected: end, found: header.offset() });
            }
            required = header.total_len() as u64;
            if required > config.segment_bytes() - position {
                return Err(StorageError::Topology("record could not fit this segment"));
            }
            if available >= required {
                let mut bytes = vec![0; header.total_len()];
                bytes[..HEADER_LEN].copy_from_slice(&raw);
                read_at(file, position + HEADER_LEN as u64, &mut bytes[HEADER_LEN..])?;
                match codec::decode_record(&bytes).map_err(|e| StorageError::format(position, e))? {
                    Probe::Complete { .. } => {},
                    Probe::Incomplete { .. } => return Err(StorageError::Topology("filled recovery record is incomplete")),
                }
                end = end.next().map_err(|e| StorageError::format(position, FormatError::Identity(e)))?;
                position += required;
                continue;
            }
        }
        if !active {
            return Err(StorageError::Incomplete { position, required, available });
        }
        return Ok(Scan { end, repair: Some(Repair {
            partition: expected.partition, base: expected.base, before: length,
            after: position, kind: RepairKind::TruncateRecord,
        }) });
    }
    Ok(Scan { end, repair: None })
}

```

## Partition planning: split obsolete names from retained contents

Discovery permits at most the normal count plus one maintenance slot. Capture all file lengths before cleanup, including obsolete files. The floor must match an existing base exactly. `partition_point` finds the first base not less than that floor; everything before it becomes a cleanup candidate, not a scan input.

For each retained file, compare its base with the previously scanned exclusive end. If total discovery used the extra count slot, the last retained file must be empty. Adjust planned byte accounting for an approved tail repair, then require the planned total not to exceed the persisted data quota plus one emergency header. The constructor has already made that addition representable.

```rust title="crates/broker-core/src/storage/recovery.rs"
fn plan_partition(root: &Path, partition: PartitionId, config: StorageConfig, retained: Retention)
    -> Result<PartitionPlan, StorageError>
{
    let path = root.join(format!("partition-{}", partition.get()));
    let files = discover(&path, config.max_segments() + 1)?;
    let total_count = files.len();
    let mut lengths = 0_u64;
    for (_, path) in &files {
        let length = fs::metadata(path).map_err(|e| StorageError::path("inspect segment length", path, e))?.len();
        if length > config.segment_bytes() {
            return Err(StorageError::Topology("existing segment exceeds configured cap"));
        }
        lengths += length;
    }
    let first = files.partition_point(|(base, _)| *base < retained.floor);
    if first == files.len() || files[first].0 != retained.floor {
        return Err(StorageError::Topology("the authoritative retained-floor segment is missing"));
    }
    // Below the authority, absence or arbitrary deletion order is expected. Never scan these bytes.
    let obsolete = files[..first].iter().map(|(_, path)| path.clone()).collect();
    let count = files.len() - first;
    let mut end = retained.floor;
    let mut repair = None;
    for (i, (base, path)) in files.into_iter().skip(first).enumerate() {
        if base != end { return Err(StorageError::Topology("retained segment bases leave a gap or overlap")); }
        let mut file = open_file(&path).map_err(|e| StorageError::path("open recovery file", &path, e))?;
        let result = scan(&mut file, SegmentHeader { partition, base }, config, i + 1 == count, i > 0)
            .map_err(|source| StorageError::InFile { path: path.clone(), source: Box::new(source) })?;
        if i + 1 < count && result.end == base {
            return Err(StorageError::Topology("only the last segment may be empty"));
        }
        if total_count > config.max_segments() && i + 1 == count && result.end != base {
            return Err(StorageError::Topology("the extra maintenance segment must be empty"));
        }
        end = result.end;
        if let Some(value) = result.repair {
            lengths = lengths - value.before + value.after;
            repair = Some(value);
        }
    }
    if lengths > retained.quota.data_bytes() + ROTATION_HEADROOM {
        return Err(StorageError::Metadata("planned file lengths exceed the persisted disk quota"));
    }
    Ok(PartitionPlan { partition, retained, obsolete, end, repair })
}

```

## Validate the root layout before operating on it

The name allowlist now recognizes floor authorities and candidates. Unknown entries, symlinks, or missing partition directories remain errors. This is a controlled broker-owned directory, not a generic filesystem garbage collector. Never broaden the deletion loop to arbitrary files merely because they resemble temporary artifacts.

```rust title="crates/broker-core/src/storage/recovery.rs"
fn ensure_directory(path: &Path) -> Result<(), StorageError> {
    let kind = fs::symlink_metadata(path)
        .map_err(|e| StorageError::path("inspect data directory", path, e))?.file_type();
    if !kind.is_dir() { return Err(StorageError::Name(path.to_path_buf())); }
    Ok(())
}

fn validate_layout(root: &Path) -> Result<(), StorageError> {
    let mut found = [false; PARTITION_COUNT as usize];
    for entry in fs::read_dir(root).map_err(|e| StorageError::path("list broker directory", root, e))? {
        let entry = entry.map_err(|e| StorageError::path("read broker entry", root, e))?;
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else { return Err(StorageError::Name(path)); };
        let kind = entry.file_type().map_err(|e| StorageError::path("inspect broker entry", &path, e))?;
        if [".broker.lock", META_NAME, TEMP_NAME].contains(&name) || checkpoint::is_name(name) || retention::is_name(name) {
            if !kind.is_file() { return Err(StorageError::Name(path)); }
        } else if let Some(n) = (0..PARTITION_COUNT).find(|n| name == format!("partition-{n}")) {
            if !kind.is_dir() { return Err(StorageError::Name(path)); }
            found[n as usize] = true;
        } else {
            return Err(StorageError::Name(path));
        }
    }
    if found.iter().any(|present| !present) {
        return Err(StorageError::Topology("initialized broker is missing a partition directory"));
    }
    Ok(())
}

```

## Apply only an already validated record repair

`apply` still rechecks the captured length, modifies exactly the approved active tail or matching partial header, resets the cursor, and synchronizes the repaired file. The caller now reaches it only after all floor/checkpoint constraints have been checked across the entire broker. Invalid authority in partition 3 must prevent repairing partition 0.

```rust title="crates/broker-core/src/storage/recovery.rs"
fn apply<S, H>(root: &Path, repair: Repair, io: &mut S, after: &mut H)
    -> Result<(), StorageError>
where S: SyncIo + ?Sized, H: FnMut(StartupStep) -> io::Result<()> {
    let path = root.join(format!("partition-{}", repair.partition.get())).join(segment_name(repair.base));
    let mut file = open_file(&path).map_err(|e| StorageError::path("open repair target", &path, e))?;
    let length = file.file_len().map_err(|e| StorageError::path("inspect repair target", &path, e))?;
    if length != repair.before { return Err(StorageError::Topology("repair target length changed after planning")); }
    match repair.kind {
        RepairKind::TruncateRecord => file.truncate(repair.after)
            .map_err(|e| StorageError::path("truncate incomplete active record", &path, e))?,
        RepairKind::CompleteSegmentHeader => {
            file.seek(SeekFrom::Start(0)).map_err(|e| StorageError::path("seek rotated header", &path, e))?;
            let raw = codec::encode_segment(SegmentHeader { partition: repair.partition, base: repair.base });
            write_fully(&mut file, &raw).map_err(|e| StorageError::path("complete rotated header", &path, e))?;
        }
    }
    file.seek(SeekFrom::Start(repair.after)).map_err(|e| StorageError::path("reset repaired cursor", &path, e))?;
    notify(root, after, StartupStep::RepairApplied(repair))?;
    io.sync(repair.base, &mut file).map_err(|e| StorageError::path("synchronize repair", &path, e))?;
    notify(root, after, StartupStep::RepairSynced(repair))?;
    Ok(())
}

```

## First creation persists the quota and zero floor

Existing `initialize` wrappers select the default quota. `initialize_budgeted` accepts a validated quota for deterministic small fixtures. Initial broker setup requires at least two normal segment slots; raw low-level storage tests may still construct a one-slot log. The single transient maintenance slot is separate from this normal limit.

After exclusive directory creation and synchronized log layout, publish each partition's floor-zero authority and both group checkpoints. Publish `broker.meta` last. These zero values are justified by **first creation**, never by a missing file during recovery.

```rust title="crates/broker-core/src/storage/recovery.rs"
pub fn initialize(path: &Path, config: StorageConfig) -> Result<StorageSession, StorageError> {
    initialize_with(path, config, &mut FileSync, &mut |_| Ok(()))
}

pub fn initialize_with<S, H>(path: &Path, config: StorageConfig, io: &mut S, after: &mut H)
    -> Result<StorageSession, StorageError>
where S: SyncIo + ?Sized, H: FnMut(StartupStep) -> io::Result<()> {
    initialize_budgeted_with(path, config, DiskQuota::default(), io, after)
}

pub fn initialize_budgeted(path: &Path, config: StorageConfig, quota: DiskQuota)
    -> Result<StorageSession, StorageError>
{
    initialize_budgeted_with(path, config, quota, &mut FileSync, &mut |_| Ok(()))
}

pub fn initialize_budgeted_with<S, H>(path: &Path, config: StorageConfig, quota: DiskQuota,
    io: &mut S, after: &mut H) -> Result<StorageSession, StorageError>
where S: SyncIo + ?Sized, H: FnMut(StartupStep) -> io::Result<()> {
    if config.max_segments() < 2 {
        return Err(StorageError::Config("broker retention requires at least two normal segment slots"));
    }
    // Exclusive first creation, never create_dir_all or "open or create".
    fs::create_dir(path).map_err(|e| StorageError::path("create new broker directory", path, e))?;
    let directory = Arc::new(DirectoryGuard::acquire(path).map_err(StorageError::Directory)?);
    let root = directory.root();
    let mut logs = Vec::with_capacity(PARTITION_COUNT as usize);
    for n in 0..PARTITION_COUNT {
        let partition = PartitionId::new(n).expect("fixed partition range");
        let mut log = PartitionLog::create(Arc::clone(&directory), partition, config)?;
        log.synchronize(io)?;
        logs.push(log);
    }
    io.sync_directory(root).map_err(|e| StorageError::path("synchronize initial broker layout", root, e))?;
    let parent = root.parent().ok_or(StorageError::Topology("new broker has no parent directory"))?;
    io.sync_directory(parent).map_err(|e| StorageError::path("synchronize new broker name", parent, e))?;
    notify(root, after, StartupStep::LayoutSynced)?;
    let epoch = Epoch::new(1).map_err(StorageError::Epoch)?;
    // No broker.meta exists yet. These zeros belong only to exclusive first creation.
    for log in &mut logs {
        let retained = Retention { partition: log.partition(), epoch, floor: Offset::new(0), quota };
        retention::publish(root, retained, io)?;
        log.install_retention(retained)?;
        for g in 0..GROUP_COUNT {
            let group = GroupId::new(g).expect("fixed group range");
            let partition = log.partition();
            checkpoint::publish(root, Checkpoint { partition, group, epoch, next: Offset::new(0) }, io,
                &mut |step| after(StartupStep::Checkpoint { partition, group, step }))?;
        }
        log.install_progress(epoch, [Offset::new(0); GROUP_COUNT as usize]);
    }
    metadata::publish(root, Metadata::new(epoch, config), io,
        &mut |point| after(StartupStep::Metadata(point)))?;
    Ok(StorageSession { epoch, logs, repairs: Vec::new() })
}

```

## Recovery validates every authority before repairing or unlinking

Read the previous broker epoch, derive its checked successor, validate the layout, and construct all four plans. Check each group checkpoint against its own plan's actual floor and end. Only after `PlanValidated` may the implementation synchronize authority, apply repairs, or remove planned obsolete paths.

Notice that candidates are discarded only after their authorities have been validated and the retained interval successfully reopened. Neither a valid `.next` file nor a convenient first segment name supplies missing authority. New progress seeds carry the new session epoch; persisted floor/checkpoint epochs may be older.

```rust title="crates/broker-core/src/storage/recovery.rs"
pub fn recover(path: &Path) -> Result<StorageSession, StorageError> {
    recover_with(path, &mut FileSync, &mut |_| Ok(()))
}

pub fn recover_with<S, H>(path: &Path, io: &mut S, after: &mut H)
    -> Result<StorageSession, StorageError>
where S: SyncIo + ?Sized, H: FnMut(StartupStep) -> io::Result<()> {
    ensure_directory(path)?;
    let directory = Arc::new(DirectoryGuard::acquire(path).map_err(StorageError::Directory)?);
    let root = directory.root();
    let old = metadata::read(root)?;
    let epoch = old.epoch().next().map_err(StorageError::Epoch)?;
    let config = old.config();
    validate_layout(root)?;
    let mut plans = Vec::with_capacity(PARTITION_COUNT as usize);
    for n in 0..PARTITION_COUNT {
        let partition = PartitionId::new(n).expect("fixed partition range");
        let retained = retention::read(root, partition)?.validate(partition, old.epoch())?;
        plans.push(plan_partition(root, partition, config, retained)?);
    }
    let mut starts = Vec::with_capacity(PARTITION_COUNT as usize);
    for plan in &plans {
        let mut next = [Offset::new(0); GROUP_COUNT as usize];
        for g in 0..GROUP_COUNT {
            let group = GroupId::new(g).expect("fixed group range");
            next[usize::from(g)] = checkpoint::read(root, plan.partition, group)
                .and_then(|value| value.validate(plan.partition, group, old.epoch(), plan.retained.floor, plan.end))
                .map_err(|source| StorageError::InFile {
                    path: root.join(checkpoint::name(plan.partition, group)), source: Box::new(source),
                })?;
        }
        starts.push(next);
    }
    notify(root, after, StartupStep::PlanValidated)?;
    // Make every validated authority and its name durable before destructive cleanup.
    for plan in &plans {
        retention::synchronize(root, plan.partition, io)?;
        for g in 0..GROUP_COUNT {
            checkpoint::synchronize(root, plan.partition, GroupId::new(g).expect("fixed groups"), io)?;
        }
    }
    io.sync_directory(root).map_err(|e| StorageError::path("synchronize recovery authorities", root, e))?;
    let mut repairs = Vec::new();
    for plan in &plans {
        if let Some(repair) = plan.repair {
            apply(root, repair, io, after)?;
            repairs.push(repair);
        }
    }
    let mut logs = Vec::with_capacity(PARTITION_COUNT as usize);
    for (plan, next) in plans.into_iter().zip(starts) {
        for path in &plan.obsolete {
            io.remove_segment(path).map_err(|e| StorageError::path("finish retired-segment cleanup", path, e))?;
        }
        if !plan.obsolete.is_empty() {
            let path = root.join(format!("partition-{}", plan.partition.get()));
            io.sync_directory(&path).map_err(|e| StorageError::path("synchronize recovered deletions", &path, e))?;
        }
        let mut log = PartitionLog::open_at(Arc::clone(&directory), plan.partition, config,
            plan.retained.floor, true)?;
        if log.written_end() != plan.end {
            return Err(StorageError::Topology("validated end changed while reconstructing indexes"));
        }
        log.synchronize(io)?;
        for g in 0..GROUP_COUNT {
            let group = GroupId::new(g).expect("fixed group range");
            checkpoint::discard_pending(root, plan.partition, group)?;
        }
        retention::discard_pending(root, plan.partition)?;
        log.install_retention(plan.retained)?;
        log.install_progress(epoch, next);
        logs.push(log);
    }
    notify(root, after, StartupStep::PartitionsSynced)?;
    // Only a valid authoritative file permits throwing away an uncommitted candidate.
    metadata::discard_pending(root)?;
    metadata::publish(root, Metadata::new(epoch, config), io,
        &mut |point| after(StartupStep::Metadata(point)))?;
    Ok(StorageSession { epoch, logs, repairs })
}
```

## Interpret interrupted maintenance without inventing state

| Last completed action before interruption | Authority after process restart | Recovery action |
| --- | --- | --- |
| Replacement active file synchronized, old floor still authoritative | Old floor | Keep old retained records and the empty tail; a later explicit retention can finish |
| Floor candidate created, written, or synchronized | Old floor | Validate old interval; discard candidate only after authority validation |
| Floor replaced, even before a success reply | New floor | Treat lower-base files as obsolete and finish their removal |
| Some obsolete files removed | New floor | Accept absent obsolete files; remove remaining obsolete names |
| All obsolete files removed, directory synchronization not reported | New floor | Validate retained interval and synchronize reopened storage; do not lower floor |

The table describes process interruption on a running machine, not arbitrary power loss. Part 4 makes these boundaries executable. A failed initialization/recovery may have completed a durable publication or repair before returning an error; it does not return a usable partial session.

There is an important non-migration case. A Chapter 13 directory has no floor authority. Even if its first segment happens to be base zero and all checkpoints are valid, this implementation refuses it. Accepting such a directory would introduce a second authority rule, which would become ambiguous after any missing-prefix failure. An explicit, separately specified migration could be added later; it is not smuggled into ordinary startup.

## Check the fully full configuration by hand

Take a 67-byte segment cap, two normal slots, and `Q_d=134`. Each file holds one three-byte payload: `32 + 32 + 3 = 67`. With two files the data portion is exactly full. Both groups checkpoint next offset 2.

The maintenance path creates a third, empty file at base 2. The transient file sum is `134 + 32 = 166`, and the transient count is three. Both fit their *explicit* maintenance allowances. Publishing floor 2 then permits removing the first two files. The final sum is 32, the final count is one, and the next producer still receives offset 2, not zero.

If synchronization of the new header fails, startup can reopen the three-file state only when the last file is empty and the aggregate remains within the extra-header envelope. Producers remain refused until retention cleans the prefix. A third file containing another record is **not** a maintenance tail and is refused; neither quota nor count overflow is excused merely because the process crashed.

Continue to Part 3 to carry reservations through the owning command and expose retention without caller-selected offsets.

