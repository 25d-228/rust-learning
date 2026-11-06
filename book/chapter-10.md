# Chapter 10 — Recovery, metadata publication, and restart epochs
## Part 1 — Validate before repairing

> **Learning objectives**
> Recover a contiguous record prefix without confusing corruption with an interrupted append. Make repair decisions before changing files, preserve complete unconfirmed records, and extend group commit to include newly created filenames.

**Implementation time:** 6.0 hours for all three parts together: 4.0 for implementation and 2.0 for recovery fixtures, integration, and diagnosis. Reading is separate. Part 1 develops the read-only scanner and synchronization changes; [Part 2 — Publish the startup epoch](https://25d-228.github.io/rust-learning/chapter-10-part-02.html) implements metadata and startup; [Part 3 — Recovery fixtures and a process interruption](https://25d-228.github.io/rust-learning/chapter-10-part-03.html) supplies the complete tests and runnable demonstration.

**Prerequisite code:** finish all three parts of [Chapter 9](https://25d-228.github.io/rust-learning/chapter-09.html). Keep its `PartitionLog`, `Segment`, sparse index, directory guard, `SyncIo`, mailbox, and partition owner. The Chapter 7 codec supplies 32-byte headers, header/payload checksum validation, and checked `Offset`/`Epoch` types. The earlier `language-lab` remains in the workspace. No new dependency or manifest change is needed.

This chapter's ten-file overlay lives under `book/media/chapter-10/`. It replaces `storage/{error,log,mod,segment,sync}.rs`, adds `storage/{metadata,recovery}.rs`, and adds `tests/recovery.rs`, `examples/recovery_probe.rs`, and `checks/check_recovery.py`. Paths below are relative to the learner workspace, not to a media directory. Repeated listings with exactly the same filename concatenate in order; `segment.rs` has one explicitly labelled insertion instead of a full repeated listing. Finish all parts before compiling: the module root intentionally names both new modules.

The lower-level constructors used by earlier tests remain available. Normal broker startup will instead call `recovery::initialize` for an absent directory or `recovery::recover` for an existing initialized directory. A Chapter 9 fixture without authoritative metadata is not automatically upgraded or reinterpreted as a fresh broker. Keep it as a fixture; use a new temporary directory for this chapter's demonstration.

## A lost reply does not erase a complete record

Suppose a partition owner has synchronized `cat` at offset 0. It then writes all of `dog` at offset 1, but the process terminates before the next synchronization and before a success reply. Just before termination its assigned and written ends can be 2 while its visible end is 1. These counters were memory, not disk records.

On restart the file may contain both complete records. Neither the record header nor its checksum says whether a producer observed a reply. The recovery policy therefore keeps both, synchronizes the recovered prefix, and establishes a new visible end of 2. Retrying the uncertain `dog` publish can create a duplicate. This is compatible with at-least-once delivery; it is not producer deduplication.

Let $W_r$ be the exclusive end reconstructed from complete contiguous records and let $V_r$ be the end the new owner exposes. Startup establishes:

$$
V_r = W_r
$$

**only after** validation, permitted repairs, required synchronizations, and publication of the new restart epoch succeed. During startup there is no client that may observe a provisional recovered end. If any phase fails, no `StorageSession` is returned.

There is also no remembered historical success watermark in the current disk format. A malicious edit or storage fault that truncates a previously synchronized last record can resemble an interrupted append. The bounded model is a process interruption on a correctly functioning local filesystem, with no concurrent noncooperating writer. Recovery rejects detectable format/topology violations; it cannot infer lost historical facts from absent bytes.

## Define the repair boundary before writing a scanner

A segment is **active** only when its canonical base-offset filename is the greatest one in that partition. Every earlier segment is immutable for recovery. Before retention is implemented, the first base must be zero, and each following base must equal the previous reconstructed end.

| Observed state | Action |
| --- | --- |
| Complete valid records, including previously unconfirmed ones | Retain every record |
| Incomplete final record in the active segment, after a valid prefix | Truncate to that record's start, subject to the header checks below |
| Valid empty active segment created by rotation | Keep it; append at its base |
| Short header of a final rotated segment, exactly matching the expected header prefix | Complete that header; do not invent any records |
| Short initial segment header or short header in an immutable segment | Refuse startup |
| Complete header with a bad checksum/version/identity, complete payload checksum failure, wrong offset, gap, overlap, or unexplained entry | Refuse startup without treating it as a repairable tail |

The short rotated-header exception is narrow. A canonical next filename identifies the exact expected base, the preceding nonempty segment must end there, and every byte already present must match `encode_segment(expected)`. A zero-byte new tail can arise between exclusive file creation and writing its header. Initial segment headers are different: authoritative metadata is not published until all four initial headers exist and synchronize. Recreating a missing initial header after that point would conceal unexplained loss.

A short **record** header cannot have its full checksum verified. We still compare its available magic/version/size/offset prefix, check any available reserved bytes, and validate the declared length once all four length bytes are present. The remaining uncertainty is stated, not hidden: a matching truncated prefix is consistent with an interrupted append, not proof that no storage corruption occurred.

## Preserve a path as well as a cause

An error such as “invalid format at byte 67” is useful inside one file, but ambiguous among many segments. Extend the storage error with `InFile { path, source }`, metadata errors, epoch errors, and the directory-ownership error. `Box<StorageError>` gives the recursive error a finite size: the outer value owns a pointer to the inner error instead of containing an infinitely nested value. `Error::source` still exposes the original cause.

Replace `src/storage/error.rs` with these complete pieces. Existing variants remain so earlier examples and tests keep their meanings.

```rust title="crates/broker-core/src/storage/error.rs"
use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::codec::SegmentHeader;
use crate::error::{DirectoryError, FormatError, IdError};
use crate::types::Offset;

#[derive(Debug)]
pub enum StorageError {
    Config(&'static str),
    Metadata(&'static str),
    InFile { path: PathBuf, source: Box<StorageError> },
    Epoch(IdError),
    Directory(DirectoryError),
    Topology(&'static str),
    Name(PathBuf),
    Identity { expected: SegmentHeader, found: SegmentHeader },
    Offset { position: u64, expected: Offset, found: Offset },
    Incomplete { position: u64, required: u64, available: u64 },
    Format { position: u64, source: FormatError },
    Io { operation: &'static str, position: u64, source: io::Error },
    PathIo { operation: &'static str, path: PathBuf, source: io::Error },
    RecordTooLarge { encoded: usize, segment_cap: u64 },
    SegmentFull,
    SegmentLimit,
    Range { start: Offset, upper: Offset, floor: Offset, end: Offset },
    Poisoned,
}

```

The new path wrapper is used only when the recovery scanner adds file context. It does not turn a checksum failure into an I/O failure. `Directory` retains the original lock error, including a busy owner; `Epoch` retains checked-counter exhaustion. The existing construction helpers remain unchanged.

```rust title="crates/broker-core/src/storage/error.rs"
impl StorageError {
    pub(crate) fn io(operation: &'static str, position: u64, source: io::Error) -> Self {
        Self::Io { operation, position, source }
    }

    pub(crate) fn path(operation: &'static str, path: &std::path::Path, source: io::Error) -> Self {
        Self::PathIo { operation, path: path.to_path_buf(), source }
    }

    pub(crate) fn format(position: u64, source: FormatError) -> Self {
        Self::Format { position, source }
    }
}

```

The outer display names the operation or location, while the cause chain holds the narrower explanation. This keeps a failed scan distinguishable from a failed truncation or publication.

```rust title="crates/broker-core/src/storage/error.rs"
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InFile { path, .. } => write!(f, "recovery scan: {}", path.display()),
            Self::Metadata(message) => write!(f, "authoritative metadata: {message}"),
            Self::Epoch(_) => write!(f, "cannot allocate a restart epoch"),
            Self::Directory(_) => write!(f, "cannot own the data directory"),
            Self::Config(message) => write!(f, "storage configuration: {message}"),
            Self::Topology(message) => write!(f, "segment topology: {message}"),
            Self::Name(path) => write!(f, "unexpected segment entry: {}", path.display()),
            Self::Identity { expected, found } => write!(f,
                "segment identity {found:?} differs from {expected:?}"),
            Self::Offset { position, expected, found } => write!(f,
                "byte {position}: expected offset {}, found {}", expected.get(), found.get()),
            Self::Incomplete { position, required, available } => write!(f,
                "byte {position}: need {required} bytes, have {available}"),
            Self::Format { position, .. } => write!(f, "invalid format at byte {position}"),
            Self::Io { operation, position, .. } => write!(f, "{operation} at byte {position}"),
            Self::PathIo { operation, path, .. } => write!(f, "{operation}: {}", path.display()),
            Self::RecordTooLarge { encoded, segment_cap } => write!(f,
                "{encoded}-byte record plus segment header exceeds cap {segment_cap}"),
            Self::SegmentFull => write!(f, "active segment has insufficient room; rotate first"),
            Self::SegmentLimit => write!(f, "segment-count limit reached"),
            Self::Range { start, upper, floor, end } => write!(f,
                "requested [{}, {}) outside [{}, {})", start.get(), upper.get(), floor.get(), end.get()),
            Self::Poisoned => write!(f, "storage owner stopped after an uncertain operation"),
        }
    }
}

```

`source.as_ref()` borrows the boxed storage error; it does not clone or unwrap it. The returned borrow is then usable as an `Error` trait object, just as in Chapter 3.

```rust title="crates/broker-core/src/storage/error.rs"
impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InFile { source, .. } => Some(source.as_ref()),
            Self::Epoch(source) => Some(source),
            Self::Directory(source) => Some(source),
            Self::Format { source, .. } => Some(source),
            Self::Io { source, .. } | Self::PathIo { source, .. } => Some(source),
            _ => None,
        }
    }
}
```

Replace the storage module root. The crate root still exports `storage`; no new top-level crate module is required.

```rust title="crates/broker-core/src/storage/mod.rs"
pub mod directory;
pub mod error;
pub mod index;
pub mod log;
pub mod metadata;
pub mod recovery;
pub mod segment;
pub mod sync;
```

## A synchronized file and a synchronized name are different work

Chapter 9 synchronizes every dirty segment before replying. A rotation also creates a directory entry. This chapter adds a separate directory synchronization after the dirty files, and initial startup synchronizes the broker root and its parent before publishing initialization metadata.

The Linux [file-synchronization manual](https://man7.org/linux/man-pages/man2/fsync.2.html#DESCRIPTION) explicitly distinguishes file synchronization from synchronizing its containing directory. The native implementation below requests both operations and propagates errors. Do not silently accept a filesystem that rejects directory synchronization. This code targets local Unix-style filesystem behavior, not arbitrary network mounts or Windows portability.

A Rust trait method may have a body that implementing types inherit or override; see [default trait implementations](https://doc.rust-lang.org/book/ch10-02-traits.html). Add defaults for metadata-file and directory synchronization. Existing Chapter 9 adapters keep their file interception and inherit real operations for the two new methods. A focused test can override either new method to inject a failure.

Replace `src/storage/sync.rs`:

```rust title="crates/broker-core/src/storage/sync.rs"
use std::fs::File;
use std::io;
use std::path::Path;

use crate::io::StorageFile;
use crate::types::Offset;

// Each successful method must complete the named synchronization operation.
pub trait SyncIo {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()>;

    fn sync_metadata(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }
}

pub struct FileSync;

impl SyncIo for FileSync {
    fn sync(&mut self, _base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }
}
```

`File::open(path)?.sync_all()` owns a temporary directory handle for one call. The `?` propagates an open failure before attempting synchronization; a sync failure propagates to the caller. Returning `Ok(())` without the required operation would violate the adapter contract. The same trusted-boundary qualification applies to all three methods.

These calls are not a blanket power-loss guarantee. Apple's archived [fsync manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html) distinguishes host flushing from stronger device-cache requests and discusses `F_FULLFSYNC`. We do not add an unexamined platform-specific claim or assume an unspecified macOS version. Test the native calls on the actual filesystem. The demonstration in Part 3 kills a process, not the machine or its power supply.

## Carry directory dirtiness through the log

Replace `src/storage/log.rs` with the following complete pieces. Most operations are the Chapter 9 implementation. The changes are: make `discover` crate-visible so the recovery planner uses exactly the same filename rules; add `directory_dirty`; set it for creation, reopening, and rotation; and clear it only after directory synchronization succeeds.

The discovery routine still bounds the number of collected files before sorting. It rejects nonregular entries and names that are not exactly twenty decimal digits followed by `.seg`. No permissive “ignore whatever is unfamiliar” path exists.

```rust title="crates/broker-core/src/storage/log.rs"
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::codec::SegmentHeader;
use crate::io::{create_file, open_file};
use crate::types::{Offset, PartitionId};
use super::directory::DirectoryGuard;
use super::error::StorageError;
use super::segment::{ReadBatch, ReadLimits, ReadStop, Segment, SegmentInfo, StorageConfig};
use super::sync::SyncIo;

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

```

The log keeps its guard share after its file-owning fields, as before. Marking a strictly reopened directory dirty makes startup reestablish its name synchronization rather than treating successful reads as a persistence barrier. Creating or strictly reopening a log still does not create broker metadata.

```rust title="crates/broker-core/src/storage/log.rs"
pub struct PartitionLog {
    // Declaration order closes segment handles before dropping this log's guard share.
    segments: Vec<Segment<File>>,
    dirty: Vec<bool>,
    directory_dirty: bool,
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
        Ok(Self { segments: vec![segment], dirty: vec![true], directory_dirty: true, root, partition, config,
            poisoned: false, _directory: directory })
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
        let dirty = vec![true; segments.len()];
        Ok(Self { segments, dirty, directory_dirty: true, root, partition, config, poisoned: false, _directory: directory })
    }

```

`rotate` does not replace an empty tail. It sets `directory_dirty` only after the new segment object has been constructed successfully. An error during creation leaves the log poisoned, and recovery later inspects the resulting file. No live retry writes through that uncertain owner.

```rust title="crates/broker-core/src/storage/log.rs"
    pub fn partition(&self) -> PartitionId { self.partition }

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
        self.dirty.push(true);
        self.directory_dirty = true;
        self.poisoned = false;
        Ok(())
    }

```

Append preflight and poison-on-uncertain-mutation remain unchanged. A new record is never intentionally split across the segment cap. That fact later lets recovery reject a declared record length that could not have fit even in a complete append.

```rust title="crates/broker-core/src/storage/log.rs"
    pub(crate) fn check_append(&self, payload_len: usize) -> Result<usize, StorageError> {
        self.healthy()?;
        let size = self.config.check_record(payload_len)?;
        let tail = self.segments.last().expect("log has a segment").info();
        tail.end.next().map_err(|e| StorageError::format(tail.bytes, crate::error::FormatError::Identity(e)))?;
        if size as u64 > self.config.segment_bytes() - tail.bytes
            && self.segments.len() == self.config.max_segments()
        {
            return Err(StorageError::SegmentLimit);
        }
        Ok(size)
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<Offset, StorageError> {
        let size = self.check_append(payload.len())?;
        let tail = self.segments.last().expect("log has a segment").info();
        if size as u64 > self.config.segment_bytes() - tail.bytes { self.rotate()?; }
        self.poisoned = true;
        let last = self.segments.len() - 1;
        self.dirty[last] = true;
        let offset = self.segments[last].append(payload)?;
        self.poisoned = false;
        Ok(offset)
    }

```

The required order is now dirty file syncs, then the containing directory if needed, then a returned end. A directory failure occurs while `poisoned` remains true. Some files may already have synchronized, but the owner still receives an error and must not advance visibility or complete that batch successfully.

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

```

The read path is unchanged. A fetched batch owns its copied records; it never borrows a file handle or the log's directory guard into another thread. Include the final method and closing brace to finish the replacement file.

```rust title="crates/broker-core/src/storage/log.rs"
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

For a fresh 67-byte-cap segment, one three-byte record exactly fills it: 32 bytes for the segment header plus 35 for the record. A second record rotates. If the new file sync succeeds but directory sync fails, the second producer gets an uncertain failure, not success. A restart may nevertheless retain that second complete record. This is the same “failure is not rollback” distinction at a newly named file boundary.

## Plan small repairs, not a second copy of the log

Create `src/storage/recovery.rs`. Its first types record only partition/base identities, before/after lengths, and the kind of repair. A plan does not retain payloads. `Repair` is `Copy` because all its fields are small owned values; it contains no file handle or borrowed byte slice.

`StorageSession` owns all four reconstructed logs and their directory-guard shares. Its fields are private, and `into_parts(self)` consumes the session to transfer the logs and epoch once. The existing lower-level APIs remain trusted fixture tools, so this is an application startup boundary rather than a claim that the type system prevents every misuse of the library.

`StartupStep` lets a test stop after a named operation. The callback may inspect the point or return an error; it never substitutes a fake scanner or metadata encoder. Metadata's own publication points are defined in Part 2.

```rust title="crates/broker-core/src/storage/recovery.rs"
use std::fs;
use std::io::{self, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::codec::{self, HEADER_LEN, Probe, SegmentHeader};
use crate::error::FormatError;
use crate::io::{StorageFile, open_file, read_prefix, write_fully};
use crate::types::{Epoch, Offset, PARTITION_COUNT, PartitionId};
use super::directory::DirectoryGuard;
use super::error::StorageError;
use super::log::{PartitionLog, discover, segment_name};
use super::metadata::{self, META_NAME, Metadata, PublicationStep, TEMP_NAME};
use super::segment::StorageConfig;
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
    end: Offset,
    repair: Option<Repair>,
}

```

The callback bound is `FnMut(StartupStep) -> io::Result<()>`: it may update an internal counter or remember the last phase, and a failure is an ordinary typed result. `notify` attaches startup's root path to that error. A failure prevents a session from escaping; it does not undo filesystem operations already completed.

```rust title="crates/broker-core/src/storage/recovery.rs"
fn notify<H>(root: &Path, after: &mut H, step: StartupStep) -> Result<(), StorageError>
where H: FnMut(StartupStep) -> io::Result<()> {
    after(step).map_err(|e| StorageError::path("interrupted startup", root, e))
}

```

## Capture the file length, then read only within it

The scanner first asks for the file's length. A deliberate short suffix is classified using that captured length. A later surprise short read is different: it means the inspected file changed, or I/O did not supply the inspected bytes. Do not turn that surprise into permission to truncate.

`read_at` seeks explicitly because each helper may change the cursor. It fills the requested slice using Chapter 7's short-read loop and checks the resulting count.

```rust title="crates/broker-core/src/storage/recovery.rs"
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

We assume the directory lock is respected by every writer and the directory tree is trusted. An advisory lock does not prevent another program from modifying files without taking it. The extra length/count checks diagnose some violations; they are not a race-proof filesystem security boundary.

## Validate what a partial header can actually tell you

The first sixteen record-header bytes have fixed expected content: magic, version, header size, and offset. Their prefix can be compared even before the full header exists. Bytes 16–19 supply the length when present. Bytes 24–27 must be zero when present. A partial checksum cannot validate the whole header; the function does not pretend otherwise.

A record must have room for at least its 32-byte header at the proposed byte position. Once its length is known, the whole encoded record must fit the remaining segment capacity. A live append would have rotated instead of violating either condition.

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

```

`reference` encodes an empty record solely to obtain its fixed first sixteen bytes. It is a small temporary vector, not a second payload copy. The expected offset is checked for a representable successor by that encoder. A partial offset prefix that differs is refused; an offset at the maximum value is not silently wrapped.

For example, after `cat` a file position is 67. At cap 102, only 35 bytes remain. A valid complete header announcing four payload bytes requires 36 bytes. Even if the file stops before that payload arrives, truncating it as an ordinary interrupted append would hide a state that this writer cannot generate. The scanner rejects it.

## Scan a segment in physical and logical order

Let $L$ be the captured file length in bytes, $q$ the next candidate record's byte position, $o$ its expected logical offset, and $r=32+n$ its encoded size for $n$ payload bytes. Initially $q=32$ and $o$ is the segment base. A valid record advances both coordinates:

$$
q_{next}=q+r, \qquad o_{next}=o+1.
$$

The available suffix is $L-q$. With a complete valid header, the body is incomplete exactly when:

$$
L-q < r.
$$

For two three-byte records, the physical positions are 32, 67, and 102 while the logical offsets are 0, 1, and 2. A 101-byte file has a complete first record and only 34 of the 35 bytes required for the second. An active file may be shortened to 67. An immutable file with the same bytes is refused, because a later segment implies that rotation already moved past it.

The full function follows. It never modifies the file, even when it returns a repair description. `active` identifies the last discovered file; `rotated` says there is an earlier segment. Only their conjunction permits completing a short segment header.

```rust title="crates/broker-core/src/storage/recovery.rs"
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

The initial-header branch reads at most 31 bytes into a fixed stack array. It compares them with the exact expected segment header and returns a header-completion plan only for the final rotated file. The ordinary path requires a complete checksummed segment header with the correct partition and base.

Inside the loop, header validation precedes payload allocation. A complete record allocates at most the codec's allowed encoded size, reads the payload, verifies its checksum, then advances `end` and `position`. The `continue` prevents a complete record from falling into the truncation decision. The temporary vector is dropped at the end of that iteration; no payload vector is stored in `Scan`.

Notice the full-checksum failure path: `decode_record(&bytes)?` returns an error directly. It never passes through `RepairKind::TruncateRecord`. A complete bad record is not made acceptable by being last in the file.

For a three-byte payload the largest transient record in this fixture is 35 bytes. For the format's maximum payload, one encoded record is $32+1{,}048{,}576=1{,}048{,}608$ bytes. This is a per-record scan allocation, not a claim about whole-process memory. Retained indexes, path strings, loaded modules, and later service buffers are separate resources.

## Validate segment topology without mutating it

The planner uses the same bounded `discover` routine as strict reopening. It walks sorted bases and carries the reconstructed exclusive end into the next expected base. Empty immutable segments are refused. Any file-specific scanner error is wrapped with its path.

```rust title="crates/broker-core/src/storage/recovery.rs"
fn plan_partition(root: &Path, partition: PartitionId, config: StorageConfig)
    -> Result<PartitionPlan, StorageError>
{
    let path = root.join(format!("partition-{}", partition.get()));
    let files = discover(&path, config.max_segments())?;
    let count = files.len();
    let mut end = Offset::new(0);
    let mut repair = None;
    for (i, (base, path)) in files.into_iter().enumerate() {
        if base != end { return Err(StorageError::Topology("segment bases leave a gap or overlap")); }
        let mut file = open_file(&path).map_err(|e| StorageError::path("open recovery file", &path, e))?;
        let result = scan(&mut file, SegmentHeader { partition, base }, config, i + 1 == count, i > 0)
            .map_err(|source| StorageError::InFile { path: path.clone(), source: Box::new(source) })?;
        if i + 1 < count && result.end == base {
            return Err(StorageError::Topology("only the last segment may be empty"));
        }
        end = result.end;
        if result.repair.is_some() { repair = result.repair; }
    }
    Ok(PartitionPlan { partition, end, repair })
}

```

Only the final segment can produce a repair, so each `PartitionPlan` needs at most one. Planning all four partitions before applying any plan prevents this avoidable sequence: truncate partition 0, discover that partition 3 is corrupt, then report a startup refusal after needlessly changing partition 0. An I/O error during the later repair phase can still leave earlier permitted repairs completed; Part 2 makes those rerunnable.

The planner deliberately does not build the sparse index. After repairs, strict reopening performs the second validating scan and reconstructs the index with the existing `Segment` implementation. This costs another linear pass over complete data but avoids maintaining two subtly different index builders. If $D$ is the total scanned data size, both passes are linear in $D$; no measured recovery time is implied. Optimization must preserve the preflight-before-mutation property.

For a segment with $N$ records and index stride $k$, the existing index retains the first record and every $k$th record thereafter, giving $\lceil N/k \rceil$ entries, or zero when the segment is empty. The five-record fixture split into bases 0, 2, and 4 with stride 2 therefore rebuilds one entry in each file. Part 3 checks indexed reads across those boundaries.

Continue with [Part 2 — Publish the startup epoch](https://25d-228.github.io/rust-learning/chapter-10-part-02.html). The remaining recovery functions validate the root namespace, apply these exact plans, synchronize the results, and publish metadata before returning owners.

