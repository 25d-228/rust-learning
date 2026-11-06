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
