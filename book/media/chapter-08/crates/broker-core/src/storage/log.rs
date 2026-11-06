use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::codec::SegmentHeader;
use crate::io::{create_file, open_file};
use crate::types::{Offset, PartitionId};
use super::directory::DirectoryGuard;
use super::error::StorageError;
use super::segment::{ReadBatch, ReadLimits, ReadStop, Segment, SegmentInfo, StorageConfig};

pub fn segment_name(base: Offset) -> String { format!("{:020}.seg", base.get()) }

fn discover(root: &Path, maximum: usize) -> Result<Vec<(Offset, PathBuf)>, StorageError> {
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
    root: PathBuf,
    partition: PartitionId,
    config: StorageConfig,
    poisoned: bool,
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
        Ok(Self { segments: vec![segment], root, partition, config, poisoned: false, _directory: directory })
    }

    pub fn open_strict(directory: Arc<DirectoryGuard>, partition: PartitionId, config: StorageConfig)
        -> Result<Self, StorageError>
    {
        let root = directory.root().join(format!("partition-{}", partition.get()));
        let paths = discover(&root, config.max_segments())?;
        let mut expected = Offset::new(0);
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
            expected = info.end;
            segments.push(segment);
        }
        Ok(Self { segments, root, partition, config, poisoned: false, _directory: directory })
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
        if self.segments.len() == self.config.max_segments() { return Err(StorageError::SegmentLimit); }
        self.poisoned = true;
        let path = self.root.join(segment_name(tail.end));
        let file = create_file(&path).map_err(|e| StorageError::path("create next segment", &path, e))?;
        let segment = Segment::create(file, SegmentHeader { partition: self.partition, base: tail.end }, self.config)?;
        self.segments.push(segment);
        self.poisoned = false;
        Ok(())
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<Offset, StorageError> {
        self.healthy()?;
        let size = self.config.check_record(payload.len())?;
        let tail = self.segments.last().expect("log has a segment").info();
        tail.end.next().map_err(|e| StorageError::format(tail.bytes, crate::error::FormatError::Identity(e)))?;
        if size as u64 > self.config.segment_bytes() - tail.bytes { self.rotate()?; }
        self.poisoned = true;
        let offset = self.segments.last_mut().expect("log has a segment").append(payload)?;
        self.poisoned = false;
        Ok(offset)
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
