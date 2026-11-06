# Chapter 8, Part 2 — Rotation, bounded reads, and storage checks

A segment can map offsets to bytes, but a partition needs an ordered set of segments, one active tail, and a rule for crossing their boundaries. This part supplies those rules without handing storage decisions to a database library.

**Prerequisite:** [Chapter 8, Part 1](https://25d-228.github.io/rust-learning/chapter-08.html), including its complete new modules, plus Chapter 7's `tests/common/mod.rs`. Both parts form one 4.5-hour implementation milestone. The storage fixture is single-threaded and operates on complete files; it is not the recovery protocol or a durable publish service.

## Give every file a canonical base-offset name

A partition lives beneath the held data directory as `partition-0`, `partition-1`, `partition-2`, or `partition-3`. Inside it, `00000000000000000003.seg` names a segment whose base is offset 3. Twenty decimal digits cover every `u64` value. The file's header must agree with the filename and partition; the name is not proof of the contents.

Create `storage/log.rs` with its imports and discovery function:

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

```

`read_dir` yields fallible entries and does not promise an order; the standard library explicitly requires sorting when order matters. We propagate both creation and iteration errors, validate each entry, cap the collected list, and sort by the parsed numeric base. See [`read_dir`](https://doc.rust-lang.org/std/fs/fn.read_dir.html).

The suffix, digit count, ASCII digit test, and checked `u64` parse reject alternate spellings and overflow. [`DirEntry::file_type`](https://doc.rust-lang.org/std/fs/struct.DirEntry.html#method.file_type) distinguishes ordinary files from directories and symbolic links; neither belongs in this segment-only namespace. A future metadata format must extend the namespace deliberately, not make this function ignore arbitrary debris. Root `.broker.lock` is outside the partition directory and is not a segment.

The list is bounded before another path is pushed. The limit is on entries we accept, not a promise that the filesystem will grant that many open handles. An empty or missing partition is not silently initialized by `open_strict`. Initialization uses a separate constructor.

## Retain the directory guard for the whole log lifetime

A path string does not keep the operating-system lock alive. The log retains an `Arc<DirectoryGuard>` so dropping the caller's share does not release the last guard while this log still owns storage.

```rust title="crates/broker-core/src/storage/log.rs"
pub struct PartitionLog {
    // Declaration order closes segment handles before dropping this log's guard share.
    segments: Vec<Segment<File>>,
    root: PathBuf,
    partition: PartitionId,
    config: StorageConfig,
    poisoned: bool,
    _directory: Arc<DirectoryGuard>,
}

```

The `segments` field is declared before `_directory`. Rust drops struct fields in declaration order, so ordinary destruction closes those segment handles before releasing this object's guard share. See the [destructor rules](https://doc.rust-lang.org/reference/destructors.html#destructors.operation). `Arc::clone` shares the guard allocation, rather than reopening or cloning its lock-file handle; the inner guard drops after the final strong owner disappears. See [`Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html).

This is not a per-partition singleton registry. The caller must create exactly one live `PartitionLog` for a partition, and drop it before reopening that same partition. Sharing the directory guard does not prevent two objects in one process from opening the same file. The upcoming partition-owner construction will enforce that application-level topology. Do not create a second reader/writer object as a shortcut around `&mut self`.

The initial constructor uses exclusive directory creation and exclusive file creation:

```rust title="crates/broker-core/src/storage/log.rs"
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

```

`create_dir` rejects an existing partition instead of treating it as a fresh log. `create_file` retains Chapter 7's `create_new` behavior, which fails rather than replacing an existing file. That atomic nonreplacement property is specified by [`OpenOptions::create_new`](https://doc.rust-lang.org/std/fs/struct.OpenOptions.html#method.create_new).

A failure after creating the directory may leave an empty directory or a partial segment header. We deliberately do not erase it on error or retry initialization automatically. Chapter 10 will add authoritative broker metadata and a first-creation protocol. This constructor is sufficient for a new, explicitly chosen fixture directory; it is not a substitute for that later protocol.

## Reopen only a complete, contiguous partition

Append the strict opener:

```rust title="crates/broker-core/src/storage/log.rs"
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

```

Before retention exists, the required floor is offset 0. For every sorted file, its base must equal the exclusive end of the previous segment. `Segment::open_strict` verifies the header identity, every record offset, and both checksums; its derived end becomes the next expected base.

An empty segment may appear only last. For example, `[0, 3)` followed by an empty segment with base 3 is valid; the next append still receives offset 3. An empty segment before a later file is rejected. Gaps and overlaps are rejected even when both files independently contain valid records.

When this function fails after opening some files, the local vector drops their handles; no partially built log escapes. It does not delete, truncate, resynchronize, rename, or repair any file. Rebuilding indexes with a different stride is permitted because the stride is not part of the on-disk record format. Reopening with a smaller segment cap that an existing file exceeds is not permitted.

Later retention must obtain a nonzero floor from authoritative metadata before accepting removed prefix segments. Do not “prepare” for that by accepting whichever base happens to be the first file today: an accidentally deleted leading file would become invisible data loss.

## Rotate without spending an offset

The inspection methods expose copied metadata; `healthy` stops operations after uncertainty. The rotation method only creates a new file when the current tail contains records.

```rust title="crates/broker-core/src/storage/log.rs"
    pub fn written_end(&self) -> Offset { self.segments.last().expect("log has a segment").info().end }
    pub fn segment_infos(&self) -> Vec<SegmentInfo> { self.segments.iter().map(Segment::info).collect() }

    fn healthy(&self) -> Result<(), StorageError> {
        if self.poisoned { Err(StorageError::Poisoned) } else { Ok(()) }
    }

```

`written_end` is an exclusive next offset. It does not expose synchronized visibility and is meaningful for serving reads only while the object is healthy. `segment_infos` allocates a small diagnostic vector bounded by the segment-count cap; no payloads are collected.

```rust title="crates/broker-core/src/storage/log.rs"
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

```

If the tail is already empty, rotation is a no-op. Otherwise, its exclusive end becomes the next filename and segment-header base. There is no counter increment for the new file. The segment-count check occurs before mutation, so exceeding it does not poison an otherwise healthy log.

Once file creation begins, the log is poisoned until the new segment has been fully initialized and added to its vector. An existing next filename causes a nonreplacement error; it is not permission to overwrite that path or invent another offset. A partially created next header likewise stops this owner. Chapter 10, not an automatic retry loop, will decide how interrupted initialization is recognized.

Old segments remain open for reading. Only the vector's last segment is passed to `append`; immutability of earlier files is the `PartitionLog` protocol, not a filesystem permission change or a separate immutable-file type.

## Refuse impossible records before changing the file set

Append the log-level operation:

```rust title="crates/broker-core/src/storage/log.rs"
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

```

The log preflights the record's absolute size and the next offset before considering rotation. It then compares the encoded record size with the active segment's remaining space. A valid record that fits exactly stays; one that does not fit causes at most one rotation, because preflight already proved it fits an empty segment.

The inner `Segment::append` repeats defensive checks and establishes its own cursor. Any error after the log marks itself poisoned leaves it stopped. A successful append returns the assigned offset, not a publish-success reply to a producer. We have not synchronized either old or new files.

Use this seven-record fixture with cap 137 and stride 2:

| Offset | Payload | Encoded bytes | Segment base | Header position |
| ---: | --- | ---: | ---: | ---: |
| 0 | `cat` | 35 | 0 | 32 |
| 1 | `dog` | 35 | 0 | 67 |
| 2 | empty | 32 | 0 | 102 |
| 3 | `owl` | 35 | 3 | 32 |
| 4 | `fox` | 35 | 3 | 67 |
| 5 | `hen` | 35 | 3 | 102 |
| 6 | `bee` | 35 | 6 | 32 |

The first segment ends at byte 134; the second fits exactly at 137; the third ends at 67. The files occupy 338 bytes in total, excluding filesystem metadata. Their exclusive logical ends are 3, 6, and 7. Index anchors are offsets 0 and 2, then 3 and 5, then 6: five entries, not seven payload copies.

## Cross segment boundaries without exceeding either read limit

The final method selects a starting segment and lets its reader extend a single batch. Supply the method and the `impl` closing brace:

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

The requested interval is `[start, upper)`. The checks require `floor <= start <= upper <= written_end`. An empty interval is a successful empty result without touching a record. A caller may deliberately choose an upper bound below the written end; Chapter 9 will supply the synchronized visible end there. Today, merely passing a smaller number is not evidence that synchronization occurred.

The sorted segment bases are partitioned by `base <= start`. The standard [`slice::partition_point`](https://doc.rust-lang.org/std/primitive.slice.html#method.partition_point) finds the first false entry; its predecessor is the starting segment. This is the same boundary interpretation we implemented explicitly for sparse anchors in Part 1. The log uses it only after construction has established sorted, contiguous segments.

The loop holds one mutable segment borrow at a time. No future, lock guard, or file reference leaves the function. Each later segment begins at `batch.next`, so no previous prefix is rescanned across a boundary. Reaching the record or byte limit stops the entire log read, not just the current file. Finishing all available segments before the requested upper bound is an error, not a short success.

A read error drops the partly constructed owned batch and leaves the log poisoned. It does not return both an error and some records for the caller to acknowledge. Range errors are detected before that poison transition and do not stop the owner.

For the fixture, `read(1, 7, records=3, bytes=100)` checks and discards offset 0, then returns offsets 1 and 2. It owns 35 + 32 = 67 encoded bytes. Offset 3 needs 35 bytes but only 33 remain, so the result is `ByteLimit { required: 35 }`, with `next = 3` and `scanned_records = 3`. Increasing the byte limit to 105 permits three returned records and stops at the record-count limit instead. Neither decision skips a nonfitting record.

## Run the fixture without a daemon

Create the following example. Its argument names an existing directory chosen for this experiment. It creates `partition-0` exclusively and never removes or reuses an existing partition directory.

```rust title="crates/broker-core/examples/segment_probe.rs"
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::log::PartitionLog;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::types::{Offset, PartitionId};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: segment_probe EXISTING_EMPTY_DIRECTORY")?);
    if args.next().is_some() { return Err("expected one directory argument".into()); }
    let owner = Arc::new(DirectoryGuard::acquire(&root)?);
    let partition = PartitionId::new(0)?;
    let config = StorageConfig::new(137, 2, 8)?;
    let mut log = PartitionLog::create(Arc::clone(&owner), partition, config)?;
    for payload in [b"cat".as_slice(), b"dog", b"", b"owl", b"fox", b"hen", b"bee"] {
        log.append(payload)?;
    }
    println!("segments={} written_end={}", log.segment_infos().len(), log.written_end().get());
    let upper = log.written_end();
    let batch = log.read(Offset::new(1), upper, ReadLimits::new(3, 100)?)?;
    let offsets: Vec<_> = batch.records.iter().map(|r| r.offset().get()).collect();
    println!("offsets={offsets:?} bytes={} next={} stop={:?} scanned={}",
        batch.encoded_bytes, batch.next.get(), batch.stop, batch.scanned_records);
    drop(log);
    let mut rebuilt = PartitionLog::open_strict(owner, partition, StorageConfig::new(137, 3, 8)?)?;
    let batch = rebuilt.read(Offset::new(5), Offset::new(6), ReadLimits::new(1, 35)?)?;
    println!("rebuilt stride=3 offset={} scanned={}", batch.records[0].offset().get(), batch.scanned_records);
    println!("writes only; no synchronization or durable publish acknowledgement");
    Ok(())
}
```

`args_os` keeps path handling independent of UTF-8. `Arc::clone` retains the same held directory guard for both phases. The explicit `drop(log)` closes all old file owners and discards every old index before reopening. The output batch owns its bytes, so it does not borrow those closed handles.

Build and run it from the learner workspace root. The `mktemp` command creates a unique directory; the program does not create a directory at an unexamined path or clean up unrelated files.

```sh
cargo check -p broker-core --all-targets
scratch=$(mktemp -d "${TMPDIR:-/tmp}/partition-broker-segments.XXXXXX")
printf 'Fixture directory: %s\n' "$scratch"
cargo run -p broker-core --example segment_probe -- "$scratch"
```

Expected program output, apart from Cargo's build messages:

```text
segments=3 written_end=7
offsets=[1, 2] bytes=67 next=3 stop=ByteLimit { required: 35 } scanned=3
rebuilt stride=3 offset=5 scanned=3
writes only; no synchronization or durable publish acknowledgement
```

The second read asks only for offset 5. Rebuilding with stride 3 leaves one anchor per fixture segment: 0, 3, and 6. It must validate offsets 3, 4, and 5 to return 5, hence three scanned records. With stride 2 it would start directly at anchor 5 and scan one record. The files and returned payload do not change.

Keep the printed directory for inspection, or remove only this directory after confirming it is the one created for the experiment. Running the program again against it should fail at `create partition`; that refusal is intentional. Use a new temporary directory for another creation experiment.

The outputs in this chapter are expected local checks, not a recorded Rust execution. The tests below define exactly what must be checked without relying on the example's text alone.

## Compare lookup with an independent full scan

Create `tests/storage.rs`. Its `mod common` imports Chapter 7's existing `tests/common/mod.rs`, whose `TestDir` creates and cleans up only its own unique test directory. No new test dependency is needed.

```rust title="crates/broker-core/tests/storage.rs"
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

```

`Rows` is only a test alias for `(offset, owned payload)` pairs. The fixture explicitly supplies seven payloads, including an empty one. `rows` copies returned payloads for comparison; that test-only copy is not part of the storage read path.

The oracle below reads the tiny fixture files in their canonical filename order and walks every encoded record. It deliberately does not call `PartitionLog::read`, use a sparse index, or select a predecessor. It shares the already independently byte-tested codec; therefore it checks lookup, bounds, and topology behavior, not an independent implementation of CRC or byte decoding.

```rust title="crates/broker-core/tests/storage.rs"
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

```

Using `fs::read` here is acceptable only because the test fixture is explicitly small. The production segment scanner never reads a whole segment into memory. The oracle insists that each segment base and every record offset equal its independently accumulated next offset.

Now enumerate every start and exclusive upper boundary, several record counts, and byte budgets just below, at, and above meaningful sizes. The expected selection uses a simple forward slice walk, not the production search.

```rust title="crates/broker-core/tests/storage.rs"
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

```

For seven records, there are 36 valid `[start, upper)` pairs, including empty intervals. Combining three count limits and ten byte limits gives 1080 comparisons per call to `compare_reads`. Each comparison checks payloads, returned offsets, byte accounting, next offset, and the stopping reason. That number is a derived fixture count, not 1080 separately named Rust tests or an observed pass count.

The precedence matches the public contract: reaching the requested upper bound gives `End`, even when the last returned record exactly fills a limit. Otherwise, a full record count gives `RecordLimit` before inspecting another header. A byte refusal reports the next whole encoded size.

## Test rotation and cursor ownership

Append these three tests for exact fit, impossible payloads, and the segment-count boundary:

```rust title="crates/broker-core/tests/storage.rs"
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

```

The 102-byte file fits two three-byte payloads exactly: 32 + 35 + 35. The next empty record belongs wholly to a new segment. With cap 64, an empty record fits but a one-byte payload does not; rejection must happen before rotation. The final test verifies that reaching the configured segment count leaves reads available rather than falsely poisoning a healthy log.

An empty new tail and a historical read probe two different state errors:

```rust title="crates/broker-core/tests/storage.rs"
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

```

Calling `rotate` twice on the empty tail must not create duplicate-base files or advance `written_end`. Dropping and strictly reopening then appending proves the next record still gets offset 1. In the cursor test, reading only offset 0 leaves the file cursor near the beginning; the next append must still seek to the remembered end and preserve all earlier records.

## Rebuild and compare every boundary

Append the full-scan comparisons and sparse-search check:

```rust title="crates/broker-core/tests/storage.rs"
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

```

The second test saves each small file's bytes, drops the entire log, reopens with a different stride, and repeats all 1080 comparisons. Comparing saved bytes afterward verifies that strict rebuilding did not mutate the files. Nothing resembling an `.idx` file is created: dropping the owner genuinely removes all indexes, and reconstruction derives them from records.

The next test isolates predecessor behavior using a segment with base 7. It inspects only the read-only index view and compares binary lookup with a linear search over those entries.

```rust title="crates/broker-core/tests/storage.rs"
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

```

Targets before 7 must return no anchor; equality must select the equal entry; intermediate targets must select the lower entry; targets beyond 13 still select 13. The index alone does not establish whether such a target is a record in this segment. The log range and segment-end checks provide that separate constraint.

## Check both limits, not just returned vector length

Append the byte-limit and range tests:

```rust title="crates/broker-core/tests/storage.rs"
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

```

The 34-byte request cannot return `dog`'s 35-byte record and must leave `next = 1`. It may still validate the skipped `cat` prefix; “zero returned records” does not mean “zero I/O.” The 100-byte request returns `dog` and the empty record without hiding the latter's 32-byte header cost.

For the upper-bound test, `[2, 3)` ends successfully even when both count and bytes are exactly exhausted. `[7, 7)` is valid and performs no body scans. Reversed or beyond-end intervals are refused without poisoning, demonstrated by the successful next append.

Append configuration and maximum-payload checks:

```rust title="crates/broker-core/tests/storage.rs"
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

```

The configuration test exercises both ends of the explicit ranges, including zero values. The maximum-payload test budgets the segment header as well as the record header and body, then returns the full payload within the response-byte limit. It does not infer memory usage from the payload length.

## Inject a partial write and prohibit blind continuation

Append the fault test and the nonempty-file guard test:

```rust title="crates/broker-core/tests/storage.rs"
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

```

`FaultFile` permits three-byte write chunks and only 37 written bytes in total. Segment creation uses 32; append writes five bytes and then fails. The complete metadata is unchanged, but the physical file length is 37. A second append must be rejected. This is stronger than testing only a failure before the first byte, and it is not a simulation of power loss or metadata persistence.

The nonempty-file check protects the lower-level constructor even when a caller supplies an already opened file instead of using the intended exclusive-creation path. It does not truncate or replace the bytes `keep`.

Append the rotation collision check and a raw-fixture helper:

```rust title="crates/broker-core/tests/storage.rs"
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

```

The collision is deliberate fault injection into a trusted namespace. The existing next filename must survive byte-for-byte, and the log must stop. `write_raw` similarly bypasses normal append only to construct malformed on-disk cases; it remains inside the tests. A real caller must not use either pattern to modify a live log.

## Distinguish incomplete bytes, corruption, and wrong identities

Append the strict prefix and corruption checks:

```rust title="crates/broker-core/tests/storage.rs"
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

```

The valid first record ends at byte 67. Lengths 33 through 66 contain a nonempty incomplete record suffix and must all be refused without changing a byte. Exactly 32 bytes is a valid empty segment. Complete header or payload corruption must produce `Format`, never a successful truncation.

The same validation applies to records skipped between an anchor and the target:

```rust title="crates/broker-core/tests/storage.rs"
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

```

Only after the owner has created two records, the test changes the first payload's byte at position 64 using a separate handle. This is sequential fault injection, not a supported second writer. A read starting at offset 1 must still validate offset 0 because stride 3 starts it at that earlier anchor. It fails and stops the log. A read whose anchor is later would not inspect this earlier damage; the test is about the span actually visited.

Checksums and identity checks have different jobs. Append these tests:

```rust title="crates/broker-core/tests/storage.rs"
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

```

`write_raw` uses the real encoder for offsets 0 and 2, so the bad sequence has valid CRCs. Its error is the missing offset 1. Truncating that second body does not change the classification: the complete header already proves the wrong offset. The header test separately constructs a valid header for the wrong partition and a valid header for the wrong base. Neither should be accepted merely because its checksum is correct.

## Reject topology and namespace surprises

Append the file-set checks:

```rust title="crates/broker-core/tests/storage.rs"
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

```

A first segment ending at 1 cannot be followed by base 2; one ending at 2 cannot be followed by base 1. An empty nonfinal segment also fails. The next test separates a missing partition, an existing empty directory, a noncanonical name, an unrelated file, and a 20-digit number too large for `u64`. None is treated as permission to initialize or ignore evidence.

Finish the test file with configuration changes, ownership lifetime, and offset exhaustion:

```rust title="crates/broker-core/tests/storage.rs"
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
```

Reducing the cap below existing file sizes or the segment-count limit below the existing count refuses reopening; changing only stride is safe. The `Weak` check observes the guard allocation's lifetime without trying a second lock through the same handle. It complements, rather than replaces, Chapter 7's two-process exclusion check.

The final test starts a low-level empty segment at `u64::MAX`. That number can represent an end but cannot identify another record with a representable successor. Append fails before writing, leaving only the 32-byte segment header. The normal partition constructor still begins at zero.

## Run the storage checks and diagnose by boundary

Run the focused target first, then the whole supplied workspace:

```sh
cargo test -p broker-core --test storage
cargo test --workspace --all-targets
cargo fmt --all -- --check
```

The focused file supplies **25 tests**. The required cumulative source supplies **127 tests**: the previous 102 plus these 25. Test counts exclude optional exercises and do not imply that an authoring environment executed them. [`--all-targets`](https://doc.rust-lang.org/cargo/commands/cargo-test.html#target-selection) includes the earlier example test targets; run their ordinary example mains separately when checking demonstration output. A formatting check may report differences in the supplied hand-formatted listings; review and apply `cargo fmt --all` locally rather than treating formatting as a storage-correctness result.

| Symptom | Likely violated boundary | Concrete diagnosis |
| --- | --- | --- |
| File exceeds the cap by 32 bytes | Segment header omitted from admission | Check `segment_bytes - HEADER_LEN`, not only record size versus cap. |
| Duplicate-base file on repeated rotation | Empty tail treated as a record-bearing segment | Compare tail base with tail end before creating another file. |
| Earlier bytes change after a read followed by append | Cursor confused with append position | Restore the explicit `SeekFrom::Start(self.bytes)` before every append. |
| Empty result repeatedly advances `next` | Byte refusal treated as consumption | Advance `next` only when moving a full record into the batch. |
| Incorrect record after a segment boundary | Local byte positions treated as global positions | Re-enter the next segment at its own anchor; its first record begins at byte 32. |
| Reopening “fixes” damage or loses a prefix | Strict validator became an implicit recovery routine | Remove mutation from `open_strict`; report the exact error and leave files untouched. |
| Error followed by a successful retry in the same owner | Poison flag cleared on an error path | Clear only after the whole operation completes; preserve the original error cause. |

No Rust compilation, Cargo test execution, dependency resolution, formatter pass, or native macOS execution is recorded for this chapter. The commands and expected results are runnable checks to perform with the compatible toolchain established earlier. Do not substitute fixture arithmetic or a successful webpage build for these Rust checks.

## Change one parameter and predict the mechanism

Repeat the seven-record fixture using strides 1, 2, and 3. Before running, predict the number of anchors and `scanned_records` for `[5, 6)`: total anchors 7, 5, and 3; body scans 1, 1, and 3. Then inspect `segment_infos()` and the returned counter. This compares deterministic work, not throughput; measure elapsed time only with an adequately repeated workload later.

Next, change the byte budget for `read(1, 7, records=3, bytes=B)` from 100 to 102. The first returns two records and stops on bytes; the second returns offsets 1, 2, and 3 using exactly 102 bytes, then stops on count with `next = 4`. Predict the result at 101 before running. No additional code is required beyond changing the example's limit.

The required milestone is a log that rotates whole records, selects validated sparse predecessors, returns owned bounded results across files, and reconstructs disposable indexes from complete authoritative bytes. It is **not** a claim that those bytes were synchronized, that interrupted tails can be repaired, or that consumers may already observe them.

Chapter 9 — *Partition owners and synchronized group commit* — will give each partition a dedicated thread, combine requests into bounded batches, synchronize the files that hold those batches, and advance visibility only before successful publish replies. Its owner will reuse this chapter's explicit written end and read upper bound rather than letting a file cursor or an index entry define durability.

