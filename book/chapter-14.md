# Chapter 14 — Safe retention and disk admission
## Part 1 — Disk reservations and retained-floor metadata

> **Learning objectives**
> Derive deletion authority from both persisted consumer frontiers. Own disk reservations across queued appends. Reserve enough logical capacity to publish metadata and replace a fully consumed active file. Make restart distinguish an obsolete leftover from a missing retained record.

**Implementation time:** 4.5 hours for all four parts together: 3.0 for disk admission, floor publication, recovery, and owner integration; 1.5 for pressure, restart, and failure checks. Reading is separate. [Part 2 — Segment reclamation and restart reconstruction](https://25d-228.github.io/rust-learning/chapter-14-part-02.html), [Part 3 — Owner integration and a pressure trace](https://25d-228.github.io/rust-learning/chapter-14-part-03.html), and [Part 4 — Failure cuts and recovery checks](https://25d-228.github.io/rust-learning/chapter-14-part-04.html) complete the same implementation.

**Prerequisite code:** the complete [Chapter 13](https://25d-228.github.io/rust-learning/chapter-13.html) workspace. Keep its two independent persisted frontiers, exact checkpoint format, synchronized visibility, and startup epoch. Retention must not reinterpret an accepted acknowledgement as a durable checkpoint.

This chapter supplies a **15-file overlay** under `book/media/chapter-14/`. New files are `src/budget.rs`, `src/budget/tests.rs`, `src/storage/retention.rs`, `tests/pressure.rs`, `examples/retention_probe.rs`, and `examples/retention_crash_probe.rs` under `crates/broker-core/`, plus workspace-root `checks/check_retention_crash.py`. Complete replacements are the crate module index, storage module index/error/synchronization/log/recovery modules, mailbox, and worker. There are no manifest or dependency changes. All earlier tests remain.

Concatenate the labelled listings for each filename in order. A fragment may continue an open function or `impl`; it is not an alternative version. Finish all four parts before building. The reference overlay contains the identical complete files, not additional hidden logic.

**Directory compatibility:** initialize a new demonstration directory. An established Chapter 13 directory lacks retained-floor authority and is deliberately refused by this version. There is no migration command. Do not manufacture zero floors or delete old metadata to make startup succeed; preserve any directory whose data matters.

## Why consuming everything can still leave the disk full

A partition has two files: a closed segment containing offsets 0 and 1, and an active segment containing offsets 2 and 3. Group 0 has checkpointed next offset 4. Group 1 has completed everything in memory but checkpointed only next offset 1. A restart must deliver offset 1 to group 1 again. Deleting the first file because its records were *seen*, *leased*, or *acknowledged* would remove required restart input.

Let $F$ be the retained floor. For group $g$, let $P_g$ be its persisted next offset, $C_g$ its volatile contiguous-completion end, and $D_g$ its issued end. Let $V$ be the synchronized-visible end. Ends remain exclusive:

$$
F \le P_g \le C_g \le D_g \le V, \qquad g\in\{0,1\}.
$$

The largest consumer-safe exclusive offset is

$$
S=\min(P_0,P_1).
$$

A closed segment with interval $[b,e)$ is eligible only when $e\le S$. We remove whole files, not a byte prefix inside a file. With segment intervals `[0,2)` and `[2,4)`, persisted fronts `(4,1)` permit no deletion. Fronts `(4,2)` permit deleting the first file and setting `F=2`. Fronts `(4,4)` make every record safe, but the last file is still active.

The active file is not unlinked in place. Create an empty replacement at base 4, synchronize its header and name, then retire the older files. That replacement needs 32 bytes and one temporary segment slot. Without this small maintenance allowance, a quota-sized log whose consumers have caught up could be unable to reclaim itself.

## A logical disk budget, not a promise about free filesystem blocks

Use fixed quotas per partition. Let $Q_d$ be normal data capacity, $Q_r$ reserved maintenance capacity, $L$ the sum of charged segment-file lengths, and $A$ bytes reserved for accepted appends that have not settled. Let $H=384$ be metadata headroom and $E=32$ the emergency rotation header. While healthy, normal admission requires

$$
L+A\le Q_d.
$$

Maintenance may temporarily use the one-header allowance:

$$
L+A\le Q_d+E, \qquad L+A+H\le Q_d+Q_r.
$$

The default quota is 126 MiB of normal data plus 2 MiB of reserve per partition. Four partitions therefore have a 512 MiB logical envelope, of which 8 MiB is reserved. An idle partition does not lend its quota to a busy one. This conservative policy avoids a shared cross-owner disk allocator; it does not introduce cross-partition delivery ordering.

Why 384 bytes? Each partition has two group checkpoint authorities and their two possible candidates, one floor authority and its candidate, and a conservative charge for the broker metadata authority and candidate. These are eight 48-byte files. The broker pair is counted in *every* partition, overcharging rather than depending on a concurrent global credit. A valid quota reserves at least `384 + 32 = 416` bytes. Larger default reserve leaves room for later bounded metadata changes but is not available to producer appends.

`Metadata::len` reports a file's byte length; it is the quantity this model charges ([Rust file metadata](https://doc.rust-lang.org/std/fs/struct.Metadata.html#method.len)). This is **not physical allocated space**, directory-entry overhead, a filesystem reservation, preallocation, or protection against another process filling the device. Real writes, synchronization, and metadata replacement can still fail with available logical quota. Such failures stop the owner. The demonstrated progress property assumes the storage operations themselves can complete; later measurements must distinguish logical file bytes from actual disk allocation.

## Reserve the worst case before the command enters the mailbox

A queued append does not yet know whether an earlier command will fill the active file. Let $x$ be payload length, $r$ reserved bytes, and $\Delta L$ actual file growth. The encoded record already includes a 32-byte record header; the worst case adds a new 32-byte segment header:

$$
r=(32+x)+32, \qquad 0\le\Delta L\le r.
$$

A three-byte payload therefore reserves 67 bytes. If it fits the active file, settlement adds only 35 bytes to file lengths and releases the spare 32. If rotation is needed, it adds all 67. For a permit of size $r$, settlement performs one locked transition:

$$
(L,A)\longmapsto(L+\Delta L,A-r).
$$

Dropping an unspent permit performs `(L,A) -> (L,A-r)`. No I/O occurs in either destructor or ledger lock. A failed mailbox insertion returns the payload and drops its reservation. Dropping a *reply receiver* does not drop the queued command and must not free that command's reservation.

This is deliberately conservative. A request can be refused even though an omniscient owner could fit its record without rotation. Predicting the future active-file shape across concurrent admission would couple the mailbox to storage scheduling. We choose bounded over-reservation, immediate refusal, and explicit retry after progress instead.

## Quotas and observable state

Create `src/budget.rs`. `DiskQuota` keeps its fields private so all externally supplied quotas pass the same overflow and headroom checks. `Copy` here copies numbers, not a resource permit. The minimum data capacity is 96 bytes: an initial 32-byte file header plus the 64-byte reservation for an empty record.

`charged_upper_bound` returns an `Option`, not a falsely precise number after failure. A failed write can leave an on-disk prefix whose length has not been settled. The stopped ledger is diagnostic state; only recovery remeasures its files.

```rust title="crates/broker-core/src/budget.rs"
use std::sync::{Arc, Mutex};

use crate::codec::HEADER_LEN;
use crate::storage::error::StorageError;

// Two checkpoint authorities/candidates, one floor pair, and one broker-meta pair.
// Charging the entire broker-meta pair to EACH partition is conservative.
pub const METADATA_HEADROOM: u64 = 8 * 48;
pub const ROTATION_HEADROOM: u64 = HEADER_LEN as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskQuota {
    data: u64,
    reserve: u64,
}
impl DiskQuota {
    pub fn new(data: u64, reserve: u64) -> Result<Self, StorageError> {
        if data < 3 * HEADER_LEN as u64 || reserve < METADATA_HEADROOM + ROTATION_HEADROOM
            || data.checked_add(reserve).is_none()
        {
            return Err(StorageError::Config("disk quota: data >= 96, reserve >= 416, representable total"));
        }
        Ok(Self { data, reserve })
    }
    pub fn data_bytes(self) -> u64 { self.data }
    pub fn reserve_bytes(self) -> u64 { self.reserve }
    pub fn total_bytes(self) -> u64 { self.data + self.reserve }
}
impl Default for DiskQuota {
    fn default() -> Self { Self { data: 126 * 1024 * 1024, reserve: 2 * 1024 * 1024 } }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskState {
    pub quota: DiskQuota,
    pub file_bytes: u64,
    pub reserved_bytes: u64,
    pub stopped: bool,
}
impl DiskState {
    pub fn charged_upper_bound(self) -> Option<u64> {
        // A failed write can leave an unmeasured prefix. Only recovery re-establishes exact lengths.
        if self.stopped { None }
        else { Some(self.file_bytes + self.reserved_bytes + METADATA_HEADROOM) }
    }
}

```

## Share the ledger, not the right to spend twice

`DiskBudget` is a cheap cloneable handle to one `Arc<Mutex<DiskState>>`. Its methods are crate-private; callers outside the core receive a `DiskState` snapshot, not an API for manufacturing credits. Normal updates reject mutex poisoning. Terminal `stop` and diagnostic `state` recover the inner value only to close admission or report it, not to resume service.

Test `used > limit` before subtracting. A recovered maintenance header may leave `L` above normal data capacity even with no pending appends. Subtraction without this check would underflow. The constructor accepts that bounded state, while ordinary producer admission remains closed until reclamation makes room.

```rust title="crates/broker-core/src/budget.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiskRefusal { Full, Stopped }

#[derive(Clone)]
pub(crate) struct DiskBudget(Arc<Mutex<DiskState>>);
impl DiskBudget {
    pub(crate) fn new(quota: DiskQuota, live: u64) -> Result<Self, StorageError> {
        if live > quota.data + ROTATION_HEADROOM {
            return Err(StorageError::Metadata("segment lengths exceed data quota plus one rotation header"));
        }
        Ok(Self(Arc::new(Mutex::new(DiskState {
            quota, file_bytes: live, reserved_bytes: 0, stopped: false,
        }))))
    }

    pub(crate) fn state(&self) -> DiskState {
        *self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn stop(&self) {
        self.0.lock().unwrap_or_else(|error| error.into_inner()).stopped = true;
    }

    fn reserve(&self, bytes: u64, maintenance: bool) -> Result<DiskPermit, DiskRefusal> {
        let mut state = self.0.lock().expect("disk ledger mutex poisoned");
        if state.stopped { return Err(DiskRefusal::Stopped); }
        let limit = state.quota.data + if maintenance { ROTATION_HEADROOM } else { 0 };
        let used = state.file_bytes + state.reserved_bytes;
        if used > limit || bytes > limit - used { return Err(DiskRefusal::Full); }
        state.reserved_bytes += bytes;
        Ok(DiskPermit { budget: self.clone(), bytes, active: true })
    }

    pub(crate) fn reserve_append(&self, encoded: usize) -> Result<DiskPermit, DiskRefusal> {
        let bytes = u64::try_from(encoded).ok()
            .and_then(|n| n.checked_add(HEADER_LEN as u64)).ok_or(DiskRefusal::Full)?;
        // The owner may rotate before this queued request; reserve its worst case now.
        self.reserve(bytes, false)
    }

    pub(crate) fn reserve_rotation(&self, maintenance: bool) -> Result<DiskPermit, DiskRefusal> {
        self.reserve(HEADER_LEN as u64, maintenance)
    }

    pub(crate) fn reclaimed(&self, bytes: u64) {
        let mut state = self.0.lock().expect("disk ledger mutex poisoned");
        state.file_bytes = state.file_bytes.checked_sub(bytes).expect("only owned live files reclaimed");
    }
}

```

## A permit has exactly one settlement or cleanup path

Two partitions may have identical quotas and counters. Their permits are still not interchangeable. `Arc::ptr_eq` tests whether two handles own the same allocation, rather than whether their contents happen to compare equal ([Rust `Arc::ptr_eq`](https://doc.rust-lang.org/std/sync/struct.Arc.html#method.ptr_eq)).

`DiskPermit` has no `Clone` implementation. `settle` consumes it, changes both counters while holding one lock, and sets `active=false` before its destructor runs. The destructor returns only an unresolved reservation. Rust also destroys remaining struct fields during ordinary cleanup; the enclosing command does not need a bespoke destructor merely to drop this field ([Rust `Drop`](https://doc.rust-lang.org/std/ops/trait.Drop.html)). Ordinary unwinding is covered; process termination does not run these destructors, which is why restart reconstructs the ledger from file lengths.

```rust title="crates/broker-core/src/budget.rs"
// Non-Clone. Ownership follows the queued command, not its reply receiver.
pub(crate) struct DiskPermit {
    budget: DiskBudget,
    bytes: u64,
    active: bool,
}
impl DiskPermit {
    pub(crate) fn belongs_to(&self, budget: &DiskBudget) -> bool {
        self.active && Arc::ptr_eq(&self.budget.0, &budget.0)
    }

    pub(crate) fn settle(mut self, growth: u64) {
        assert!(growth <= self.bytes, "file growth exceeded its reservation");
        let mut state = self.budget.0.lock().expect("disk ledger mutex poisoned");
        state.reserved_bytes -= self.bytes;
        state.file_bytes += growth;
        self.active = false;
    }
}
impl Drop for DiskPermit {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.budget.0.lock().unwrap_or_else(|error| error.into_inner());
            state.reserved_bytes -= self.bytes;
        }
    }
}

#[cfg(test)]
mod tests;
```

`reserve_append` converts `usize` to `u64` and checks the extra-header addition. The reserve path bounds every later counter addition. The quota constructor also bounds `data + reserve`, and reserve is at least `H + E`; hence a healthy `charged_upper_bound` is representable. Do not weaken those checks and keep the unchecked-looking additions unchanged.

One subtle failure rule is essential: stop admission **before** dropping reservations after uncertain I/O. Otherwise another producer could acquire credit that a partial write still occupies on disk. The stopped state makes no claim that its old `file_bytes` is exact. Part 2 reconstructs it after restart.

## A floor file authorizes deletion; filenames alone do not

Create one root file `retention-p{partition}.floor`. The candidate has suffix `.next`. The authority records both a retained floor and the partition quota. Persisting the quota prevents restart from silently replacing a small configured envelope with a default. This chapter does not implement quota resizing.

| Byte range | Width | Meaning |
| --- | ---: | --- |
| 0–7 | 8 | `PBRF`, version 1, total length 48 |
| 8–9 | 2 | Partition, big-endian |
| 10–11 | 2 | Zero reserved bytes |
| 12–19 | 8 | Publishing startup epoch, big-endian |
| 20–27 | 8 | Retained exclusive-prefix floor, big-endian |
| 28–35 | 8 | Normal data quota in bytes |
| 36–43 | 8 | Reserved quota in bytes |
| 44–47 | 4 | CRC32 of bytes 0–43, big-endian |

The floor is a **start offset**, not a count of files already removed. After floor 4 publishes, leftover files below 4 are obsolete whether zero, one, or all of them remain. Conversely, a missing file *at or above* 4 is not explained by that authority.

For partition 2, epoch 7, floor 19, and quota `(236,416)`, the independent fixture is:

```text
50 42 52 46 00 01 00 30 00 02 00 00 00 00 00 00
00 00 00 07 00 00 00 00 00 00 00 13 00 00 00 00
00 00 00 ec 00 00 00 00 00 00 01 a0 d3 2b 8f cb
```

The decoder checks size before slicing, then checksum, fixed fields, identity range, nonzero epoch, and quota validity. Runtime scope validation separately requires the expected partition and an epoch no newer than `broker.meta`. An earlier epoch is legitimate when that partition has not needed another retention publication.

## Encode and validate retained authority

Add `storage/retention.rs`. The public value type supports exact codec fixtures; the methods that publish or install authority remain crate-private. A caller cannot turn a guessed floor into an owner command.

```rust title="crates/broker-core/src/storage/retention.rs"
use std::fs::{self, File};
use std::path::Path;

use crate::budget::{DiskQuota, DiskState};
use crate::io::{open_file, read_prefix};
use crate::types::{Epoch, Offset, PARTITION_COUNT, PartitionId};
use super::error::StorageError;
use super::metadata;
use super::sync::SyncIo;

pub const FLOOR_LEN: usize = 48;
pub fn name(partition: PartitionId) -> String { format!("retention-p{}.floor", partition.get()) }
pub fn pending_name(partition: PartitionId) -> String { format!("{}.next", name(partition)) }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub partition: PartitionId,
    pub epoch: Epoch,
    pub floor: Offset,
    pub quota: DiskQuota,
}
impl Retention {
    pub fn encode(self) -> [u8; FLOOR_LEN] {
        let mut raw = [0; FLOOR_LEN];
        raw[..8].copy_from_slice(b"PBRF\x00\x01\x00\x30");
        raw[8..10].copy_from_slice(&self.partition.get().to_be_bytes());
        raw[12..20].copy_from_slice(&self.epoch.get().to_be_bytes());
        raw[20..28].copy_from_slice(&self.floor.get().to_be_bytes());
        raw[28..36].copy_from_slice(&self.quota.data_bytes().to_be_bytes());
        raw[36..44].copy_from_slice(&self.quota.reserve_bytes().to_be_bytes());
        let crc = crc32fast::hash(&raw[..44]);
        raw[44..].copy_from_slice(&crc.to_be_bytes());
        raw
    }

    pub fn decode(raw: &[u8]) -> Result<Self, StorageError> {
        if raw.len() != FLOOR_LEN { return Err(StorageError::Metadata("retained floor length is not 48")); }
        let crc = u32::from_be_bytes(raw[44..].try_into().expect("fixed CRC field"));
        if crc != crc32fast::hash(&raw[..44]) {
            return Err(StorageError::Metadata("retained floor checksum mismatch"));
        }
        if &raw[..8] != b"PBRF\x00\x01\x00\x30" || raw[10..12] != [0, 0] {
            return Err(StorageError::Metadata("invalid retained floor header"));
        }
        let partition = PartitionId::new(u16::from_be_bytes(raw[8..10].try_into().expect("fixed partition")))
            .map_err(|_| StorageError::Metadata("retained floor partition is out of range"))?;
        let epoch = Epoch::new(u64::from_be_bytes(raw[12..20].try_into().expect("fixed epoch")))
            .map_err(|_| StorageError::Metadata("retained floor epoch is zero"))?;
        let floor = Offset::new(u64::from_be_bytes(raw[20..28].try_into().expect("fixed floor")));
        let data = u64::from_be_bytes(raw[28..36].try_into().expect("fixed data quota"));
        let reserve = u64::from_be_bytes(raw[36..44].try_into().expect("fixed reserve quota"));
        Ok(Self { partition, epoch, floor, quota: DiskQuota::new(data, reserve)? })
    }

    pub(crate) fn validate(self, partition: PartitionId, epoch: Epoch) -> Result<Self, StorageError> {
        if self.partition != partition || self.epoch > epoch {
            return Err(StorageError::Metadata("retained floor has a different partition or a future epoch"));
        }
        Ok(self)
    }
}

```

## Read only the authority; reuse the publication protocol

`Retained` is the result of a completed retention request, including its actual deleted-byte count and current ledger snapshot. `read` requires an ordinary 48-byte file, not a symlink or an arbitrary-length candidate. `publish` reuses Chapter 13's same-directory candidate/write/synchronize/close/replace/directory-synchronize helper.

Keeping candidate and target in the same directory avoids a cross-filesystem rename. Rust's rename operation replaces an existing target and can fail across filesystems ([Rust `rename`](https://doc.rust-lang.org/std/fs/fn.rename.html)). A failure after replacement is uncertain completion, not rollback. Only validated authority permits candidate cleanup during recovery.

```rust title="crates/broker-core/src/storage/retention.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retained {
    pub before: Offset,
    pub floor: Offset,
    pub removed_segments: usize,
    pub removed_bytes: u64,
    pub rotated: bool,
    pub disk: DiskState,
}

pub fn read(root: &Path, partition: PartitionId) -> Result<Retention, StorageError> {
    let path = root.join(name(partition));
    let info = fs::symlink_metadata(&path)
        .map_err(|e| StorageError::path("inspect required retained floor", &path, e))?;
    if !info.file_type().is_file() || info.len() != FLOOR_LEN as u64 {
        return Err(StorageError::Metadata("retained floor must be a regular 48-byte file"));
    }
    let mut file = File::open(&path).map_err(|e| StorageError::path("open retained floor", &path, e))?;
    let mut raw = [0; FLOOR_LEN];
    let count = read_prefix(&mut file, &mut raw)
        .map_err(|e| StorageError::path("read retained floor", &path, e))?;
    Retention::decode(&raw[..count])
}

pub(crate) fn is_name(candidate: &str) -> bool {
    (0..PARTITION_COUNT).any(|p| {
        let p = PartitionId::new(p).expect("fixed partitions");
        candidate == name(p) || candidate == pending_name(p)
    })
}

pub(crate) fn publish<S: SyncIo + ?Sized>(root: &Path, value: Retention, io: &mut S)
    -> Result<(), StorageError>
{
    metadata::publish_bytes(root, &name(value.partition), &pending_name(value.partition),
        &value.encode(), io, &mut |_| Ok(()))
}

pub(crate) fn synchronize<S: SyncIo + ?Sized>(root: &Path, partition: PartitionId, io: &mut S)
    -> Result<(), StorageError>
{
    let path = root.join(name(partition));
    let mut file = open_file(&path).map_err(|e| StorageError::path("reopen retained floor", &path, e))?;
    io.sync_retention(&mut file).map_err(|e| StorageError::path("synchronize retained floor", &path, e))
}

pub(crate) fn discard_pending(root: &Path, partition: PartitionId) -> Result<(), StorageError> {
    metadata::discard_candidate(root, &pending_name(partition))
}
```

## Connect the modules and distinguish overload from I/O failure

Replace the crate module index and storage module index with these complete files. The crate exposes quota values and diagnostics; ledger and permit operations retain their narrower visibility.

```rust title="crates/broker-core/src/lib.rs"
pub mod budget;
pub mod codec;
pub mod delivery;
pub mod error;
pub mod io;
pub mod mailbox;
pub mod storage;
pub mod types;
pub mod worker;
```

```rust title="crates/broker-core/src/storage/mod.rs"
pub mod checkpoint;
pub mod directory;
pub mod error;
pub mod index;
pub mod log;
pub mod metadata;
pub mod recovery;
pub mod retention;
pub mod segment;
pub mod sync;
```

Add `StorageError::DiskFull` through this complete error-module replacement. It describes a logical admission refusal, not a synthesized operating-system error. Existing path, byte-position, and nested-source context remains intact. Do not turn a real failed write into `DiskFull` and then continue using that owner.

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
    DiskFull,
    Range { start: Offset, upper: Offset, floor: Offset, end: Offset },
    Poisoned,
}

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
            Self::DiskFull => write!(f, "logical disk admission is full"),
            Self::SegmentLimit => write!(f, "segment-count limit reached"),
            Self::Range { start, upper, floor, end } => write!(f,
                "requested [{}, {}) outside [{}, {})", start.get(), upper.get(), floor.get(), end.get()),
            Self::Poisoned => write!(f, "storage owner stopped after an uncertain operation"),
        }
    }
}

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

The synchronization interface gets separate retained-authority synchronization and a segment-removal seam. A successful override of `remove_segment` must actually unlink the named closed file. Test adapters may fail before or after that real operation; they must not report success without doing it. `FileSync` still supplies actual file synchronization. Closing a Rust file ignores close errors; explicit synchronization remains the fallible boundary ([Rust `File`](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all)).

```rust title="crates/broker-core/src/storage/sync.rs"
use std::fs::File;
use std::io;
use std::path::Path;

use crate::io::StorageFile;
use crate::types::Offset;
use super::metadata::PublicationStep;

// Each successful method must complete the named synchronization operation.
pub trait SyncIo {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()>;

    fn sync_metadata(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    fn sync_checkpoint(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    fn sync_retention(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }

    // Successful implementations must unlink the named, closed segment.
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    // Test observation point AFTER each actual publication operation.
    fn publication_step(&mut self, _target: &Path, _step: PublicationStep) -> io::Result<()> {
        Ok(())
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

## Preserve separate data and control lanes

Replace `mailbox.rs` with the complete resulting file below. Only the admission classifications change: `DiskFull` and `InvalidRecord` are now distinct from queue `Full` and `Closed`. Both lanes retain their own count limits and FIFO order. Disk reservation occurs outside this mailbox lock; no filesystem operation runs while the lock is held.

```rust title="crates/broker-core/src/mailbox.rs"
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError { Full, Closed, DiskFull, InvalidRecord }

#[derive(Debug)]
pub struct Rejected<T> {
    pub reason: AdmissionError,
    pub value: T,
}

#[derive(Debug)]
pub enum Event<D, C> { Data(D), Control(C), Deadline, Closed }

struct Queues<D, C> {
    data: VecDeque<D>,
    control: VecDeque<C>,
    closed: bool,
    prefer_control: bool,
    #[cfg(test)]
    waiting: bool,
}

pub struct Mailbox<D, C> {
    queues: Mutex<Queues<D, C>>,
    changed: Condvar,
    data_capacity: usize,
    control_capacity: usize,
    #[cfg(test)]
    observed: Condvar,
}

impl<D, C> Mailbox<D, C> {
    pub fn new(data_capacity: usize, control_capacity: usize) -> Result<Self, &'static str> {
        if !(1..=4096).contains(&data_capacity) || !(1..=4096).contains(&control_capacity) {
            return Err("each mailbox lane needs 1..=4096 slots");
        }
        Ok(Self {
            queues: Mutex::new(Queues {
                data: VecDeque::with_capacity(data_capacity),
                control: VecDeque::with_capacity(control_capacity),
                closed: false, prefer_control: true,
                #[cfg(test)]
                waiting: false,
            }),
            changed: Condvar::new(), data_capacity, control_capacity,
            #[cfg(test)]
            observed: Condvar::new(),
        })
    }

    pub fn try_data(&self, value: D) -> Result<(), Rejected<D>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.data.len() == self.data_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.data.push_back(value);
        self.changed.notify_one();
        Ok(())
    }

    pub fn try_control(&self, value: C) -> Result<(), Rejected<C>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.control.len() == self.control_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.control.push_back(value);
        self.changed.notify_one();
        Ok(())
    }

    // Exactly one owner calls recv. A deadline is absolute, not a fresh delay per wakeup.
```

The receive loop is unchanged in purpose: an expired absolute deadline takes precedence, busy lanes alternate, and empty waiters recheck predicates after notification. Full **data** admission does not occupy the separately bounded control lane. This does not promise acknowledgement admission when that control lane itself is full; Chapter 15 adds the reserved pressure-safe path.

```rust title="crates/broker-core/src/mailbox.rs"
    pub fn recv(&self, deadline: Option<Instant>) -> Event<D, C> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        loop {
            if deadline.is_some_and(|end| Instant::now() >= end) { return Event::Deadline; }
            if !q.control.is_empty() && (q.prefer_control || q.data.is_empty()) {
                q.prefer_control = false;
                return Event::Control(q.control.pop_front().expect("checked control queue"));
            }
            if let Some(value) = q.data.pop_front() {
                q.prefer_control = true;
                return Event::Data(value);
            }
            if q.closed { return Event::Closed; }
            #[cfg(test)]
            { q.waiting = true; self.observed.notify_all(); }
            q = match deadline {
                Some(end) => self.changed.wait_timeout(q, end.saturating_duration_since(Instant::now()))
                    .expect("mailbox mutex poisoned").0,
                None => self.changed.wait(q).expect("mailbox mutex poisoned"),
            };
            #[cfg(test)]
            { q.waiting = false; }
        }
    }

    // Close admission, but keep accepted commands available to the owner.
    pub fn close(&self) {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        q.closed = true;
        self.changed.notify_all();
    }

    // Terminal cleanup only: move values out, then drop/respond outside this mutex.
    pub fn abort(&self) -> (VecDeque<D>, VecDeque<C>) {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        q.closed = true;
        let values = (std::mem::take(&mut q.data), std::mem::take(&mut q.control));
        self.changed.notify_all();
        values
    }
}

```

Keep the existing mailbox tests in this replacement. They are prerequisite tests, not new Chapter 14 cases. Their observed-wait handshake is still preferable to assuming that a sleep proves a thread has blocked.

```rust title="crates/broker-core/src/mailbox.rs"
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn observe_wait<D, C>(mailbox: &Mailbox<D, C>) {
        let end = Instant::now() + Duration::from_secs(5);
        let mut q = mailbox.queues.lock().unwrap();
        while !q.waiting {
            assert!(Instant::now() < end, "owner did not reach the wait");
            q = mailbox.observed.wait_timeout(q, end.saturating_duration_since(Instant::now()))
                .unwrap().0;
        }
    }

    #[test]
    fn lanes_have_independent_caps_and_return_rejected_ownership() {
        let q = Mailbox::<Vec<u8>, u8>::new(1, 1).unwrap();
        q.try_data(vec![1]).unwrap();
        let refusal = q.try_data(vec![2]).unwrap_err();
        assert_eq!(refusal.reason, AdmissionError::Full);
        assert_eq!(refusal.value, vec![2]);
        q.try_control(9).unwrap();
        assert_eq!(q.try_control(8).unwrap_err().reason, AdmissionError::Full);
        assert!(matches!(q.recv(None), Event::Control(9)));
        assert!(matches!(q.recv(None), Event::Data(v) if v == vec![1]));
    }

    #[test]
    fn both_busy_lanes_alternate_and_preserve_each_fifo() {
        let q = Mailbox::new(3, 3).unwrap();
        for n in 0..3 { q.try_data(n).unwrap(); q.try_control(n + 10).unwrap(); }
        for n in 0..3 {
            assert!(matches!(q.recv(None), Event::Control(v) if v == n + 10));
            assert!(matches!(q.recv(None), Event::Data(v) if v == n));
        }
    }

    #[test]
    fn expired_deadline_precedes_ready_work() {
        let q = Mailbox::<u8, u8>::new(1, 1).unwrap();
        q.try_data(1).unwrap(); q.try_control(2).unwrap();
        assert!(matches!(q.recv(Some(Instant::now())), Event::Deadline));
        assert!(matches!(q.recv(None), Event::Control(2)));
        assert!(matches!(q.recv(None), Event::Data(1)));
    }

```

The remaining retained tests cover deadline wakeup, control notification, close-versus-abort ownership, and invalid lane sizes. Closure drains already admitted values; abort returns them so their owned permits and replies can be destroyed outside the queue lock.

```rust title="crates/broker-core/src/mailbox.rs"
    #[test]
    fn timed_empty_wait_reaches_its_deadline() {
        let q = Mailbox::<u8, u8>::new(1, 1).unwrap();
        let end = Instant::now() + Duration::from_millis(5);
        assert!(matches!(q.recv(Some(end)), Event::Deadline));
        assert!(Instant::now() >= end);
    }

    #[test]
    fn a_control_command_wakes_an_observed_waiter() {
        let q = Arc::new(Mailbox::<u8, u8>::new(1, 1).unwrap());
        let receiver = Arc::clone(&q);
        let thread = thread::spawn(move || receiver.recv(Some(Instant::now() + Duration::from_secs(5))));
        observe_wait(&q);
        q.try_control(7).unwrap();
        assert!(matches!(thread.join().unwrap(), Event::Control(7)));
    }

    #[test]
    fn closure_wakes_an_observed_waiter_and_rejects_new_work() {
        let q = Arc::new(Mailbox::<u8, u8>::new(1, 1).unwrap());
        let receiver = Arc::clone(&q);
        let thread = thread::spawn(move || receiver.recv(Some(Instant::now() + Duration::from_secs(5))));
        observe_wait(&q); q.close();
        assert!(matches!(thread.join().unwrap(), Event::Closed));
        assert_eq!(q.try_data(3).unwrap_err().reason, AdmissionError::Closed);
        assert_eq!(q.try_control(4).unwrap_err().reason, AdmissionError::Closed);
    }

    #[test]
    fn close_drains_but_abort_returns_all_remaining_values() {
        let q = Mailbox::<u8, u8>::new(2, 2).unwrap();
        q.try_data(1).unwrap(); q.try_control(2).unwrap(); q.close();
        assert!(matches!(q.recv(None), Event::Control(2)));
        assert!(matches!(q.recv(None), Event::Data(1)));
        assert!(matches!(q.recv(None), Event::Closed));
        let q = Mailbox::<u8, u8>::new(1, 1).unwrap();
        q.try_data(3).unwrap(); q.try_control(4).unwrap();
        let (data, control) = q.abort();
        assert_eq!(data.into_iter().collect::<Vec<_>>(), vec![3]);
        assert_eq!(control.into_iter().collect::<Vec<_>>(), vec![4]);
        assert!(matches!(q.recv(None), Event::Closed));
    }

    #[test]
    fn invalid_lane_sizes_are_rejected() {
        assert!(Mailbox::<(), ()>::new(0, 1).is_err());
        assert!(Mailbox::<(), ()>::new(1, 4097).is_err());
    }
}
```

## The ownership boundary to carry into storage

At this point the new types define a contract but are not yet sufficient for a build: the log, recovery module, and worker still need their replacements. The key lifecycle is `producer owns payload -> command owns payload and permit -> owner settles file growth -> pending reply owns only completion state`. A refused mailbox insertion reverses the first handoff. An abandoned reply does not reverse the append.

In Part 2, file deletion follows a different irreversible order: synchronize a replacement active file when necessary, publish the floor, close obsolete handles, unlink obsolete names, synchronize their directory, and only then return disk credit. Publication is the authority boundary; reclamation is the accounting boundary.

