# Chapter 8, Part 1 — Segments and sparse record lookup

A record offset is not a byte position. Offset 2 may begin at byte 102, while offset 3 begins at byte 32 of another file. Looking up offset 2 therefore requires a storage structure, not a multiplication by a fixed record size. The structure must remain useful after its in-memory index disappears, and a read must not accidentally change where the next append lands.

**Prerequisites:** Chapters 1–7, including both parts of Chapter 7 and its complete `broker-core` overlay. Keep the existing workspace manifest, the `broker-core` manifest, `codec.rs`, `io.rs`, `types.rs`, `error.rs`, `storage/directory.rs`, and `tests/common/mod.rs`. This chapter changes no dependency or compiler requirement. The language examples remain separate, runnable targets.

**Implementation time for all of Chapter 8:** 4.5 hours: 3.25 for segments, indexing, rotation, and bounded reads; 1.25 for comparisons, fault checks, and diagnosis. Both parts share that estimate. Reading is excluded. Record actual implementation time rather than extending a finished task to fit the estimate.

> **Learning objectives**
> Map logical offsets to validated record boundaries; implement sparse predecessor search; rotate before a complete record exceeds a segment cap; separate returned-record limits from returned-byte limits; keep cursor ownership explicit; and rebuild disposable indexes without mistaking validation for recovery or writing for durable publication.

This part implements a generic segment over Chapter 7's `StorageFile` interface. [Part 2 — Rotation, bounded reads, and storage checks](https://25d-228.github.io/rust-learning/chapter-08-part-02.html) connects segments into one partition log and supplies the executable and all tests. Finish both parts before running the new targets: the module root names `log.rs`, which Part 2 creates. There is still no partition worker, network server, synchronized visibility, tail repair, or retention.

## Carry the byte format forward unchanged

The prerequisite segment header is 32 bytes and identifies a partition and a base offset. A record is a 32-byte header followed by at most 1,048,576 payload bytes. Integers use the existing big-endian layout. Header integrity and semantic checks precede trusting the payload length; payload integrity is checked after reading the body. The existing codec returns `Probe::Incomplete` separately from `FormatError`.

`Offset` is the logical record identity. `u64` file positions in this chapter are byte positions, always named `position` or `bytes`. `StorageFile` supplies `Read`, `Write`, `Seek`, length inspection, truncation, and synchronization; this chapter uses no truncation or synchronization. `read_prefix` fills a requested slice until EOF or error, and `write_fully` handles positive short writes and interrupted calls. Neither provides rollback.

Create four new modules and replace only `storage/mod.rs` with the following. The existing `directory` module remains unchanged; the new `storage/error.rs` is distinct from the existing crate-level `error.rs`.

```rust title="crates/broker-core/src/storage/mod.rs"
pub mod directory;
pub mod error;
pub mod index;
pub mod log;
pub mod segment;
```

The complete seven-file overlay is under `book/media/chapter-08/`. Its files are `src/storage/{mod,error,index,segment,log}.rs`, `tests/storage.rs`, and `examples/segment_probe.rs`, all beneath `crates/broker-core/`. Matching [segment](https://25d-228.github.io/rust-learning/media/chapter-08/crates/broker-core/src/storage/segment.rs), [index](https://25d-228.github.io/rust-learning/media/chapter-08/crates/broker-core/src/storage/index.rs), and [log](https://25d-228.github.io/rust-learning/media/chapter-08/crates/broker-core/src/storage/log.rs) files are available as references, but every required new line is also included in the two chapter bodies. For a repeated filename, append its listings in order. An `impl` block may begin in one listing and end in a later one; do not add extra braces between them.

## Separate physical space from logical progress

Let $H_s=32$ be the segment-header size, $H_r=32$ the record-header size, and $p_i$ the payload length of record $i$, measured in bytes. Its encoded size is $r_i=H_r+p_i$. For a segment with base offset $b$ and $n$ complete records, its exclusive next offset is $e=b+n$, but its byte length is:

$$
L=H_s+\sum_{i=0}^{n-1}r_i.
$$

The empty segment has $n=0$, $e=b$, and $L=32$. Creating it does not allocate an offset. For `cat`, `dog`, and an empty payload, the encoded sizes are 35, 35, and 32; their positions are 32, 67, and 102, and the file ends at 134. The offset after those three records is 3, not 134.

Let $S$ be the configured segment-byte cap, including its header. A candidate record of encoded size $r$ is legal in some segment only when $H_s+r\le S$. It fits the current segment only when $L+r\le S$. We test these separately: an impossible record is rejected before rotation; a legal record that does not fit triggers rotation. Equality fits. There is no oversized-record exception.

A physical write may stop after a prefix. “Append whole records” means that records are never deliberately split across segment files and complete metadata is advanced only after the full write succeeds. It does not mean a record-sized write is an atomic filesystem transaction.

## Errors describe the failed boundary

Create `storage/error.rs` with the imports and variants below. A format error retains its byte position and codec cause; a namespace I/O error retains its path and operating-system cause. `SegmentFull` means that a legal record needs rotation. `RecordTooLarge` means that even an empty segment cannot hold it.

```rust title="crates/broker-core/src/storage/error.rs"
use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::codec::SegmentHeader;
use crate::error::FormatError;
use crate::types::Offset;

#[derive(Debug)]
pub enum StorageError {
    Config(&'static str),
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

`Incomplete` reports bytes required at one position and bytes available there, not missing bytes. `Identity` compares the segment header with the partition and filename we expected. `Offset` reports a checksummed record whose logical identity breaks continuity. A checksum alone does not establish ordering.

The small constructors below attach context without discarding the source error. `pub(crate)` exposes them to modules in this crate, not to external users of `broker-core`.

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

Supply the complete formatting and cause implementations. These errors are ordinary return values, not instructions to delete files or reset an offset.

```rust title="crates/broker-core/src/storage/error.rs"
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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

Matching variants retains structure. Printing only the outer error retains location but not its cause; a command-line diagnostic can follow `Error::source`, as in Chapter 7's process-lock diagnostic.

```rust title="crates/broker-core/src/storage/error.rs"
impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Format { source, .. } => Some(source),
            Self::Io { source, .. } | Self::PathIo { source, .. } => Some(source),
            _ => None,
        }
    }
}
```

The owner will distinguish preflight refusal from an uncertain operation:

| Outcome | On-disk changes attempted? | Continue with this owner? |
| --- | --- | --- |
| Invalid payload, invalid read range, exhausted offset, or segment-count limit | No | Yes; the request was refused. |
| Successful append | Full record written | Yes, but no durable-success claim yet. |
| Failed seek/write after mutation begins, or unexpected read failure | May be uncertain or damaged | No; retain a poisoned owner and stop serving it. |
| Strict reopen sees an incomplete or corrupt file | No repair attempted | No new owner is returned. |

Poisoning is our explicit Boolean state, not the standard mutex-poisoning mechanism. It records that continuing from cached metadata is not justified. Restart recovery will become a separate operation in Chapter 10.

## Store sparse anchors, not every payload

An index entry contains an offset and the byte position of that record's header. With stride $k$, we keep offsets $b,b+k,b+2k$ and so on, stopping before the segment end. The segment appender or full validator is the only writer of entries. External callers receive only a shared slice.

Create `storage/index.rs`:

```rust title="crates/broker-core/src/storage/index.rs"
use crate::types::Offset;
use super::error::StorageError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub offset: Offset,
    pub position: u64,
}

pub struct SparseIndex {
    base: Offset,
    stride: u64,
    entries: Vec<IndexEntry>,
}

```

`SparseIndex` has its own base because the stride restarts at each segment. It does not assume that all segment bases are multiples of the stride. The constructor permits strides from 1 through 4096. This is an implementation work bound, not a persisted format limit.

```rust title="crates/broker-core/src/storage/index.rs"
impl SparseIndex {
    pub fn new(base: Offset, stride: u64) -> Result<Self, StorageError> {
        if !(1..=4096).contains(&stride) {
            return Err(StorageError::Config("index stride must be in 1..=4096"));
        }
        Ok(Self { base, stride, entries: Vec::new() })
    }

    // Only the validated, contiguous segment scanner/appender calls this.
    pub(crate) fn record(&mut self, offset: Offset, position: u64) {
        assert!(offset >= self.base);
        if (offset.get() - self.base.get()) % self.stride == 0 {
            if let Some(previous) = self.entries.last() {
                assert!(previous.offset < offset && previous.position < position);
            }
            self.entries.push(IndexEntry { offset, position });
        }
    }

    pub fn entries(&self) -> &[IndexEntry] { &self.entries }

```

`record` is deliberately crate-visible only. Its precondition is a validated, contiguous stream in increasing byte order. The segment code will check on-disk identities before calling it. The assertions defend programmer invariants; invalid disk bytes must become `StorageError` before they reach these assertions.

The remainder expression selects every $k$th record relative to `base`, not every $k$th byte. `entries()` borrows rather than clones the vector. Callers can inspect an anchor but cannot append a fabricated one through that view.

Now implement the search, rather than delegating the central lookup algorithm to a container:

```rust title="crates/broker-core/src/storage/index.rs"
    pub fn predecessor(&self, target: Offset) -> Option<IndexEntry> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            let middle = low + (high - low) / 2;
            if self.entries[middle].offset <= target {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low.checked_sub(1).map(|i| self.entries[i])
    }
}
```

The half-open interval `[low, high)` contains undecided entries. Entries below `low` satisfy `offset <= target`; entries at or above `high` do not. A qualifying midpoint moves `low` past that entry, because we want the **last** qualifier, not any match. A nonqualifying midpoint remains a possible first greater entry, so `high` becomes `middle`.

At termination, `low` is the first entry greater than the target, or the vector length if none is greater. `checked_sub(1)` turns zero into `None`, handling both an empty index and a target before the first anchor. The midpoint calculation avoids adding two possibly large indexes. Returning an `IndexEntry` copies two small fields; it does not clone the index or borrow the segment's cursor.

For base 7, stride 3, and seven one-byte payloads, each record occupies 33 bytes. The anchors are:

| Anchor offset | Byte position | Why |
| --- | ---: | --- |
| 7 | 32 | First record, immediately after the segment header. |
| 10 | 131 | Three records after offset 7: `32 + 3 × 33`. |
| 13 | 230 | Six records after offset 7: `32 + 6 × 33`. |

For target 12, the first midpoint selects offset 10 and moves `low` to 2; the next midpoint is offset 13 and moves `high` to 2. Subtracting one selects offset 10. We then validate records 10, 11, and 12 from the file. The index did not contain record 12, but it gave a safe place to begin.

For $n$ records and positive stride $k$, the number of entries is:

$$
E=\left\lceil\frac{n}{k}\right\rceil.
$$

The empty case gives zero entries. A predecessor search takes logarithmic comparisons in $E$; reaching a valid target then skips at most $k-1$ complete records. Larger strides save index entries but increase read work. These are algorithmic counts, not measured timings.

## Make resource limits constructible only in valid states

Create `storage/segment.rs`. The first imports bring in the existing codec and file-I/O seam. `StorageConfig` has private fields so its constructor is the boundary that establishes all later subtraction and allocation assumptions.

```rust title="crates/broker-core/src/storage/segment.rs"
use std::io::SeekFrom;

use crate::codec::{
    self, HEADER_LEN, Probe, RecordHeader, SegmentHeader,
};
use crate::error::FormatError;
use crate::io::{StorageFile, read_prefix, write_fully};
use crate::types::Offset;
use super::error::StorageError;
use super::index::SparseIndex;

#[derive(Debug, Clone, Copy)]
pub struct StorageConfig {
    segment_bytes: u64,
    index_stride: u64,
    max_segments: usize,
}

```

The per-segment byte cap lies between 64 bytes and 64 MiB: the lower bound holds one segment header and one empty record. At most 64 segments are retained by this chapter's single log object. A stride is positive and at most 4096. These small, explicit ceilings bound this implementation; they are not claims about what the operating system can allocate or how large a general-purpose log should be.

```rust title="crates/broker-core/src/storage/segment.rs"
impl StorageConfig {
    pub fn new(segment_bytes: u64, index_stride: u64, max_segments: usize)
        -> Result<Self, StorageError>
    {
        if !(64..=64 * 1024 * 1024).contains(&segment_bytes) {
            return Err(StorageError::Config("segment bytes must be in 64..=67108864"));
        }
        if !(1..=4096).contains(&index_stride) {
            return Err(StorageError::Config("index stride must be in 1..=4096"));
        }
        if !(1..=64).contains(&max_segments) {
            return Err(StorageError::Config("segment count must be in 1..=64"));
        }
        Ok(Self { segment_bytes, index_stride, max_segments })
    }

    pub fn segment_bytes(self) -> u64 { self.segment_bytes }
    pub fn index_stride(self) -> u64 { self.index_stride }
    pub fn max_segments(self) -> usize { self.max_segments }

    pub(crate) fn check_record(self, payload_len: usize) -> Result<usize, StorageError> {
        let size = codec::record_len(payload_len).map_err(|e| StorageError::format(0, e))?;
        if size as u64 > self.segment_bytes - HEADER_LEN as u64 {
            return Err(StorageError::RecordTooLarge { encoded: size, segment_cap: self.segment_bytes });
        }
        Ok(size)
    }
}

```

`check_record` first applies the codec's 1 MiB payload limit. Only then does it convert the bounded encoded size to `u64`. The cast is a widening or provably fitting conversion on the target; it is not used to validate an arbitrary untrusted integer. Subtracting `HEADER_LEN` from `segment_bytes` is safe because construction established a cap of at least 64.

To permit the largest legal payload, the segment cap must cover **two** headers: 1,048,576 + 32 + 32 = 1,048,640 bytes. A smaller cap is still valid configuration, but some otherwise valid payloads will not fit it. The 137-byte fixture deliberately makes rotation frequent.

Read requests have separate limits. The byte limit counts complete **encoded record bytes**, including each record header, but not a future TCP response envelope. Zero limits are rejected rather than producing a nonprogressing loop with an unexplained empty result.

```rust title="crates/broker-core/src/storage/segment.rs"
#[derive(Debug, Clone, Copy)]
pub struct ReadLimits {
    records: usize,
    bytes: usize,
}

impl ReadLimits {
    pub fn new(records: usize, bytes: usize) -> Result<Self, StorageError> {
        if !(1..=1024).contains(&records) || !(1..=4 * 1024 * 1024).contains(&bytes) {
            return Err(StorageError::Config("read limits: 1..=1024 records, 1..=4194304 bytes"));
        }
        Ok(Self { records, bytes })
    }
}

```

A request can ask for at most 1024 records and 4 MiB of encoded record bytes. Private fields keep an invalid request from bypassing `new`. There is no network parser yet; the caller is another Rust function.

## Own the returned bytes

A read result must survive the next read, append, rotation, or eventual file deletion. We therefore return owned encoded buffers. Keeping the header in the buffer avoids a second payload copy after validating it; `payload()` presents only the payload slice.

```rust title="crates/broker-core/src/storage/segment.rs"
#[derive(Debug, PartialEq, Eq)]
pub struct OwnedRecord {
    offset: Offset,
    bytes: Vec<u8>,
}

impl OwnedRecord {
    pub fn offset(&self) -> Offset { self.offset }
    pub fn payload(&self) -> &[u8] { &self.bytes[HEADER_LEN..] }
    pub fn encoded_len(&self) -> usize { self.bytes.len() }
}

```

The buffer is private, so a caller cannot mutate the encoded offset while retaining an inconsistent `offset` field. The returned payload borrow is tied to `&self`; it cannot outlive the `OwnedRecord`. Moving the record into a batch transfers its vector allocation, not a second copy of its bytes.

A successful read also explains its stopping condition:

```rust title="crates/broker-core/src/storage/segment.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStop {
    End,
    RecordLimit,
    ByteLimit { required: usize },
}

#[derive(Debug)]
pub struct ReadBatch {
    pub records: Vec<OwnedRecord>,
    pub next: Offset,
    pub encoded_bytes: usize,
    pub scanned_records: usize,
    pub stop: ReadStop,
}

impl ReadBatch {
    pub(crate) fn empty(start: Offset, limits: ReadLimits) -> Self {
        Self {
            records: Vec::with_capacity(limits.records), next: start,
            encoded_bytes: 0, scanned_records: 0, stop: ReadStop::End,
        }
    }
}

```

`End` means the requested exclusive upper offset was reached, not that no future record can be appended. `RecordLimit` means another requested record remains but no record slot remains. `ByteLimit { required }` gives the **entire encoded size of the next record**, not the additional bytes needed. The next offset remains that record's offset, allowing a caller to retry with adequate capacity instead of silently skipping it.

`scanned_records` counts complete records whose bodies were read and validated, including discarded prefix records. A candidate whose header is inspected but whose body will not fit is not included. This counter makes the index tradeoff observable without pretending it measures latency.

`Vec::with_capacity` reserves descriptor capacity while leaving length zero. The [vector documentation](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.with_capacity) distinguishes that reservation from initialized elements. The count bound is checked before pushing; a capacity reservation alone is not a limit.

## Give the segment one cursor and one metadata owner

Append the segment metadata and the generic storage object:

```rust title="crates/broker-core/src/storage/segment.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentInfo {
    pub base: Offset,
    pub end: Offset,
    pub bytes: u64,
    pub index_entries: usize,
}

pub struct Segment<F: StorageFile> {
    file: F,
    header: SegmentHeader,
    config: StorageConfig,
    end: Offset,
    bytes: u64,
    index: SparseIndex,
    poisoned: bool,
}

```

`SegmentInfo` is a copied diagnostic snapshot. It does not grant mutation rights. `Segment<F>` owns its file, header, index, and complete-record ends. `F` can be a real `File` or the earlier `FaultFile<File>` adapter; the layout and algorithms do not change with the adapter.

`end` counts complete records. `bytes` is the byte position just after the last complete record. Neither field is inferred from the file's current cursor. That cursor may be at the end of a historical read. Every append will explicitly seek to `bytes` before writing.

A checked successor is still required even when the byte cap is small: a segment may have a base near the largest representable offset. Convert the existing typed error into a located storage error:

```rust title="crates/broker-core/src/storage/segment.rs"
fn next_offset(offset: Offset, position: u64) -> Result<Offset, StorageError> {
    offset.next().map_err(|e| StorageError::format(position, FormatError::Identity(e)))
}

```

The file API allows seeks and independent file length changes; `Seek` positions are expressed in bytes, not record identities. See [`Seek::seek`](https://doc.rust-lang.org/std/io/trait.Seek.html#tymethod.seek). Our metadata is meaningful only under the trusted single-owner protocol. Rust borrowing does not prevent an unrelated process from modifying the same path.

## Validate a fixed header before allocating a body

The next helper reads exactly one fixed record header at an explicit position. `end` is the caller's known segment byte boundary, not the cursor. A short prefix is reported before any payload allocation.

```rust title="crates/broker-core/src/storage/segment.rs"
fn read_header<F: StorageFile>(file: &mut F, position: u64, end: u64, expected: Offset)
    -> Result<([u8; HEADER_LEN], RecordHeader), StorageError>
{
    let available = end.checked_sub(position)
        .ok_or(StorageError::Topology("read position beyond segment end"))?;
    if available < HEADER_LEN as u64 {
        return Err(StorageError::Incomplete { position, required: HEADER_LEN as u64, available });
    }
    file.seek(SeekFrom::Start(position)).map_err(|e| StorageError::io("seek record", position, e))?;
    let mut raw = [0; HEADER_LEN];
    let count = read_prefix(file, &mut raw).map_err(|e| StorageError::io("read header", position, e))?;
    if count != HEADER_LEN {
        return Err(StorageError::Incomplete {
            position, required: HEADER_LEN as u64, available: count as u64,
        });
    }
    let header = codec::decode_record_header(&raw).map_err(|e| StorageError::format(position, e))?;
    if header.offset() != expected {
        return Err(StorageError::Offset { position, expected, found: header.offset() });
    }
    if header.total_len() as u64 > available {
        return Err(StorageError::Incomplete {
            position, required: header.total_len() as u64, available,
        });
    }
    Ok((raw, header))
}

```

First, `checked_sub` rejects a position beyond the boundary. A suffix shorter than 32 bytes cannot contain a complete header. Otherwise, an absolute seek establishes the cursor, `read_prefix` handles short reads, and the existing codec verifies header CRC, magic, version, reserved fields, offset validity, and payload length. Only a validated header supplies `total_len`. The helper then compares its offset with the caller's `expected` offset **before** classifying a short body. A checksummed wrong offset must not be disguised as a repairable tail merely because its payload is also incomplete.

The final length comparison is against the known remaining file bytes. A complete valid header followed by an incomplete body is still `Incomplete`; an invalid complete header is `Format`, not permission to truncate. The loop will never scan ahead for another magic sequence.

`read_body` is called immediately after `read_header`, with no intervening operation on the cursor. It allocates at most one validated encoded record:

```rust title="crates/broker-core/src/storage/segment.rs"
fn read_body<F: StorageFile>(file: &mut F, position: u64, raw: [u8; HEADER_LEN], header: RecordHeader)
    -> Result<OwnedRecord, StorageError>
{
    // read_header left this exclusively owned cursor immediately after the header.
    let mut bytes = vec![0; header.total_len()];
    bytes[..HEADER_LEN].copy_from_slice(&raw);
    let count = read_prefix(file, &mut bytes[HEADER_LEN..])
        .map_err(|e| StorageError::io("read payload", position + HEADER_LEN as u64, e))?;
    if count != header.payload_len() {
        return Err(StorageError::Incomplete {
            position, required: header.total_len() as u64,
            available: (HEADER_LEN + count) as u64,
        });
    }
    match codec::decode_record(&bytes).map_err(|e| StorageError::format(position, e))? {
        Probe::Complete { .. } => Ok(OwnedRecord { offset: header.offset(), bytes }),
        Probe::Incomplete { .. } => Err(StorageError::Topology("filled record decoded as incomplete")),
    }
}

```

The fixed header is copied into the new vector, and the payload is read directly into its final owned buffer. `decode_record` checks the payload CRC and returns a temporary borrowed view. We do not retain that view; the completed vector becomes the `OwnedRecord`. The codec rechecks the header as well. That duplicate small validation is explicit, simple, and not a claimed optimization.

The final `Incomplete` arm would indicate an internal disagreement: the buffer was allocated to the validated total length and filled completely. It is still an explicit error rather than an omitted branch. A short actual read is handled earlier, with the total bytes obtained at the record position.

This helper's cursor sequencing is a local program invariant, not a typestate guarantee. Keeping it private and calling it directly after the header reader makes that dependency visible. A future position-based I/O implementation could remove the dependency, but simply cloning a `File` does not: [`File::try_clone`](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_clone) shares underlying position changes.

## Create a new segment without overwriting old contents

The constructor accepts an already exclusively created file. Chapter 7's `create_file` uses `create_new`; the log layer in Part 2 selects the path. The segment constructor adds a second check that its input file is empty.

```rust title="crates/broker-core/src/storage/segment.rs"
impl<F: StorageFile> Segment<F> {
    pub fn create(mut file: F, header: SegmentHeader, config: StorageConfig)
        -> Result<Self, StorageError>
    {
        let length = file.file_len().map_err(|e| StorageError::io("inspect new segment", 0, e))?;
        if length != 0 { return Err(StorageError::Topology("new segment file is not empty")); }
        file.seek(SeekFrom::Start(0)).map_err(|e| StorageError::io("seek new segment", 0, e))?;
        write_fully(&mut file, &codec::encode_segment(header))
            .map_err(|e| StorageError::io("write segment header", 0, e))?;
        Ok(Self {
            file, header, config, end: header.base, bytes: HEADER_LEN as u64,
            index: SparseIndex::new(header.base, config.index_stride)?, poisoned: false,
        })
    }

```

An empty file may have a cursor elsewhere, so the constructor still seeks to zero. After writing the segment header, it initializes an empty index and sets both logical ends to `header.base`. A partial header write returns an error and drops the owned handle. It does not delete the partial file. The caller must not reinterpret its existence as successful initialization.

`SparseIndex::new` cannot reject this validated configuration, but it still returns `Result` because its public constructor independently protects its own invariants. No raw-struct serialization, implicit truncation, or synchronized durability is hidden in `create`.

## Rebuild a complete file by scanning, not by trusting an index

`open_strict` is a validator for complete files. It neither repairs a tail nor discovers whether a producer received an acknowledgement. Append it inside the same `impl` block:

```rust title="crates/broker-core/src/storage/segment.rs"
    pub fn open_strict(mut file: F, expected: SegmentHeader, config: StorageConfig)
        -> Result<Self, StorageError>
    {
        let length = file.file_len().map_err(|e| StorageError::io("inspect segment", 0, e))?;
        if length > config.segment_bytes {
            return Err(StorageError::Topology("existing segment exceeds configured cap"));
        }
        file.seek(SeekFrom::Start(0)).map_err(|e| StorageError::io("seek segment header", 0, e))?;
        let mut raw = [0; HEADER_LEN];
        let count = read_prefix(&mut file, &mut raw)
            .map_err(|e| StorageError::io("read segment header", 0, e))?;
        let header = match codec::decode_segment(&raw[..count]).map_err(|e| StorageError::format(0, e))? {
            Probe::Incomplete { required } => return Err(StorageError::Incomplete {
                position: 0, required: required as u64, available: count as u64,
            }),
            Probe::Complete { value, .. } => value,
        };
        if header != expected { return Err(StorageError::Identity { expected, found: header }); }
        let mut segment = Self {
            file, header, config, end: header.base, bytes: HEADER_LEN as u64,
            index: SparseIndex::new(header.base, config.index_stride)?, poisoned: false,
        };
        while segment.bytes < length {
            let position = segment.bytes;
            let (raw, record_header) = read_header(&mut segment.file, position, length, segment.end)?;
            let record = read_body(&mut segment.file, position, raw, record_header)?;
            segment.index.record(record.offset, position);
            segment.end = next_offset(record.offset, position)?;
            segment.bytes += record.encoded_len() as u64;
        }
        Ok(segment)
    }

```

The file length is captured under exclusive ownership, checked against the cap, and used as the scan boundary. The decoded segment header must exactly match the expected partition and filename base. A valid checksum over the wrong identity is not acceptable.

The scan begins at byte 32 with expected offset equal to the base. For each record, it validates the header, requires the expected offset, validates the body, records an anchor when appropriate, and advances both ends. The temporary record buffer is dropped at the end of that loop iteration. The loop retains indexes, not all payloads.

For a file containing offsets 0 and 2, the second header may have perfectly valid checksums. The expected offset is nevertheless 1, so reopening fails before adding an anchor for 2. For a 67-byte file containing offset 0 and `cat`, truncating to 66 bytes produces a body-shortage error; it does not produce an empty log.

The scan never calls `truncate` or `sync`. An exact 32-byte segment header is a valid empty segment here; deciding whether an empty segment is legal in the **middle of a partition** belongs to the log topology check in Part 2. A shorter header is incomplete, including on the final file. Chapter 10 will introduce the explicit recovery policy.

## Append at the remembered boundary

Supply the inspection methods and append operation:

```rust title="crates/broker-core/src/storage/segment.rs"
    pub fn info(&self) -> SegmentInfo {
        SegmentInfo {
            base: self.header.base, end: self.end, bytes: self.bytes,
            index_entries: self.index.entries().len(),
        }
    }

    pub fn index(&self) -> &SparseIndex { &self.index }

    pub fn into_file(self) -> F { self.file }

```

The borrowed index is read-only. `into_file` consumes the entire `Segment`, discarding its metadata and returning ownership of the file; it does not return a second cursor alias. It is a low-level escape for controlled adapters and later recovery, not permission to keep using the old segment object.

```rust title="crates/broker-core/src/storage/segment.rs"
    pub fn append(&mut self, payload: &[u8]) -> Result<Offset, StorageError> {
        if self.poisoned { return Err(StorageError::Poisoned); }
        let size = self.config.check_record(payload.len())?;
        let offset = self.end;
        let next = next_offset(offset, self.bytes)?;
        if size as u64 > self.config.segment_bytes - self.bytes {
            return Err(StorageError::SegmentFull);
        }
        let bytes = codec::encode_record(offset, payload).map_err(|e| StorageError::format(self.bytes, e))?;
        self.poisoned = true;
        self.file.seek(SeekFrom::Start(self.bytes))
            .map_err(|e| StorageError::io("seek append", self.bytes, e))?;
        write_fully(&mut self.file, &bytes)
            .map_err(|e| StorageError::io("append record", self.bytes, e))?;
        self.index.record(offset, self.bytes);
        self.bytes += size as u64;
        self.end = next;
        self.poisoned = false;
        Ok(offset)
    }

```

The preflight order matters. Reject a poisoned owner first. Check payload and segment-size limits, obtain a representable next offset, and verify remaining room before encoding or touching the file. A standalone segment reports `SegmentFull`; the log layer knows how to rotate and prevents this case before calling it.

Only then do we set `poisoned = true`, seek to the cached complete byte boundary, and perform the full write. The index and both ends advance afterward. Finally, clearing poison makes the object usable again. An I/O error returned by `?` bypasses that clearing assignment. A panic during subsequent metadata work also leaves the flag set if unwinding is caught elsewhere; allocation failure or process termination is not promised to unwind.

Consider an adapter allowing 37 total written bytes: 32 bytes initialize the segment, and the next append writes only five header bytes before failing. The actual file length is 37, but `bytes` remains 32, `end` remains the base, and the index remains empty. That is intentional: cached complete metadata is not an assertion that no extra bytes exist. A second append is rejected as `Poisoned`, not retried at byte 32.

There is no `sync` call in this method. Successful writing and successful synchronization are separate operations in the [`File` API](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all). Part 2's demonstration labels its results accordingly; Chapter 9 will decide visibility and successful replies.

## Scan from an anchor into a bounded result

The segment reader accepts the requested upper offset, validated limits, and a batch under construction. `batch.next` begins at the requested start and advances only when a record is actually returned. Append the final method and closing brace:

```rust title="crates/broker-core/src/storage/segment.rs"
    pub(crate) fn read_into(&mut self, upper: Offset, limits: ReadLimits, batch: &mut ReadBatch)
        -> Result<(), StorageError>
    {
        if self.poisoned { return Err(StorageError::Poisoned); }
        let anchor = self.index.predecessor(batch.next)
            .ok_or(StorageError::Topology("nonempty read has no predecessor"))?;
        let mut position = anchor.position;
        let mut expected = anchor.offset;
        while expected < self.end && batch.next < upper {
            if expected == batch.next && batch.records.len() == limits.records {
                batch.stop = ReadStop::RecordLimit;
                break;
            }
            let (raw, header) = read_header(&mut self.file, position, self.bytes, expected)?;
            if expected == batch.next && header.total_len() > limits.bytes - batch.encoded_bytes {
                batch.stop = ReadStop::ByteLimit { required: header.total_len() };
                break;
            }
            let record = read_body(&mut self.file, position, raw, header)?;
            batch.scanned_records += 1;
            let next = next_offset(expected, position)?;
            position += record.encoded_len() as u64;
            if expected == batch.next {
                batch.encoded_bytes += record.encoded_len();
                batch.next = next;
                batch.records.push(record);
            }
            expected = next;
        }
        Ok(())
    }
}
```

A predecessor points at a validated boundary but does not replace validation of records read now. The loop checks each encountered header's offset against `expected`, then reads and checks its body. Prefix records before `batch.next` are deliberately validated and dropped. Returned records are moved into the result, and only those increase `encoded_bytes` and `next`.

The record limit is checked before reading another candidate header. The byte limit can only be checked after a valid header provides its encoded length, but **before allocating or reading that candidate's payload**. The subtraction `limits.bytes - batch.encoded_bytes` is safe because every earlier push preserved the bound. The candidate is not consumed when it does not fit.

Suppose the requested start is 1, the stride is 2, and the segment starts at 0 with `cat`, `dog`, and an empty payload. The anchor is offset 0 at byte 32. Record 0 is checked and discarded; record 1 contributes 35 returned bytes; record 2 contributes 32. The result owns 67 encoded bytes, `next` is 3, and three bodies were scanned. If the next segment's first record requires 35 bytes but only 33 remain, the batch stops before its body allocation.

`ReadStop::End` also serves as the intermediate “no limit encountered” value while a batch crosses segments. Part 2's log reader continues to the next segment in that case. The public result reports `End` only after confirming that `next == upper`. A partial traversal cannot masquerade as the requested end.

An indexed read is not a full-file integrity scrub: it validates the visited span, starting at its anchor. Corruption before that anchor is not inspected by that read. Strict reopening validates all record bytes before constructing the index. Neither mechanism authenticates hostile replacement; the checksum and ownership model remain Chapter 7's trusted-local model.

## Account for returned data, scratch space, and index entries separately

Let $C$ be the requested record limit and $B$ the requested encoded-byte limit. A successful batch containing $m$ records satisfies:

$$
m\le C,\qquad \sum_{j=0}^{m-1}(32+p_j)\le B.
$$

Those two limits are independent. One empty record still consumes one slot and 32 encoded bytes. A 34-byte budget cannot return a three-byte payload, even though its payload alone is smaller than 34.

The reader may temporarily allocate a complete skipped-prefix record, even for a one-byte response budget. That scratch allocation is bounded by the codec's maximum encoded record, 1,048,608 bytes. At most one such record is alive in this serialized scan. Returned buffers, vector descriptors, index capacity, allocator overhead, and the operating system's file cache are additional categories. These limits are therefore not a whole-process resident-memory bound or recoverable-allocation design. The [vector guarantees](https://doc.rust-lang.org/std/vec/struct.Vec.html#guarantees) explain why length, capacity, and allocator size must not be conflated.

The maximum number of records in a segment, even with empty payloads, is $N_{\max}=\lfloor(S-32)/32\rfloor$. With stride $k$, its live entry count is at most $\lceil N_{\max}/k\rceil$. For the 137-byte fixture, the segment holds at most three records and needs at most two entries with stride 2. The vector may reserve spare capacity; its entries are still bounded by this derived count.

For a read spanning several segments, only the initial segment can require a skipped prefix: each later segment begins exactly at the next requested offset. Thus at most $C+k-1$ complete bodies are validated, plus at most one additional candidate header when bytes are insufficient. This is a work-count bound under the contiguous-log invariant, not a constant-time claim; payload sizes and I/O costs still matter.

Continue to [Part 2](https://25d-228.github.io/rust-learning/chapter-08-part-02.html) to implement directory discovery, rotation, the cross-segment reader, and the executable tests. The segment code alone is not yet the Chapter 8 milestone.

