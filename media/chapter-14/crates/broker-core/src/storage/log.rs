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
