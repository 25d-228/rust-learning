# Chapter 13 — Durable consumer progress
## Part 1 — Checkpoint bytes and validated startup

> **Learning objectives**
> Separate an accepted completion from persisted progress. Publish a contiguous exclusive frontier without overwriting its only authoritative copy. Validate every checkpoint against the recovered log before repairing any record file, then carry the validated starts into the owning partition threads.

**Implementation time:** 4.5 hours for all three parts together: 3.0 for the format, startup, policy, and owner integration; 1.5 for checkpoint, restart, and failure checks. Reading is separate. [Part 2 — Accepted completion and checkpointed progress](https://25d-228.github.io/rust-learning/chapter-13-part-02.html) adds the runtime policy and commands. [Part 3 — Publication cuts and repeat ranges](https://25d-228.github.io/rust-learning/chapter-13-part-03.html) supplies complete tests, an ordinary demonstration, and a process-kill harness.

**Prerequisite code:** the complete [Chapter 12](https://25d-228.github.io/rust-learning/chapter-12.html) workspace, including the storage publication/recovery code from [Chapter 10](https://25d-228.github.io/rust-learning/chapter-10.html). `LeaseWindow` already retires only a completed contiguous prefix; the owner already reads only synchronized records. Do not replace either of those rules with a checkpoint counter.

This chapter supplies a **15-file overlay** under `book/media/chapter-13/`. Seven files are new: `storage/checkpoint.rs`, `delivery/progress.rs`, `delivery/progress/tests.rs`, `tests/progress.rs`, `examples/progress_probe.rs`, `examples/checkpoint_crash_probe.rs`, and `checks/check_progress_crash.py`. The first six are under `crates/broker-core/` with `src/` before the storage/delivery paths. Eight existing files receive complete replacements: the storage module index, metadata helper, synchronization interface, log, recovery module, delivery module index, leased-group adapter, and worker. Earlier tests and demonstrations remain in the workspace. No manifest, dependency, record layout, or broker-metadata encoding changes.

For every filename, **concatenate its labelled listings in order**. These are complete resulting files, including retained prerequisite code. A later fragment may continue an open `impl`, function, or `match`; it is not an alternative implementation. Finish all three parts before building. The reference overlay provides the identical files separately, but does not replace the explanation.

**Directory compatibility:** initialize a new demonstration directory with this chapter's code. An established directory from the earlier implementation has no group checkpoints; this implementation refuses it rather than assuming zero. There is no implicit migration. Keep valuable earlier data intact; creating eight zero files by hand is not a safe upgrade procedure, particularly once retention exists.

## An acknowledgement can survive in the client and disappear in the broker

The owner returns an accepted acknowledgement for offsets 0, 1, and 2. The consumer has already performed their external effects. The broker process then exits. Its record files still contain four records, but the in-memory completion window is gone. Record presence cannot reveal which effects happened elsewhere, and the broker cannot ask a vanished process what it remembers.

We therefore save a different fact: **the first offset that this group must consider again after restart**. It is an exclusive completed frontier, not the greatest completed offset. Saving “2 was completed” as a start of 3 would lose offsets 0 and 1 if they were still a gap.

Let $F$ be the retained log floor, $P$ the persisted next offset, $C$ the volatile contiguous-completion end, $D$ the issued end, $V$ the synchronized visible end, and $W$ the delivery-window width. All offset ends are exclusive. In this chapter $F=0$ because nothing has been deleted.

$$
F \le P \le C \le D \le V, \qquad D-C \le W.
$$

An acknowledgement can move $C$, but it does not move $P$. A successful checkpoint publishes the current $C$ and only then changes $P$. A restart loads validated $P$ and creates empty windows:

$$
C_{\mathrm{restart}}=D_{\mathrm{restart}}=P_{\mathrm{loaded}}.
$$

The first equation combines storage availability with delivery state. The second does not restore old lease generations, deadlines, or completed followers. Those remain volatile and the new startup epoch fences their old tokens.

| Operation on group 0, with visible end 4 | Volatile next | Persisted next | Consequence of restarting now |
| --- | ---: | ---: | --- |
| Issue offsets 0–3 | 0 | 0 | Reconsider all four |
| Complete offset 2 | 0 | 0 | Offset 2 can repeat; a gap remains |
| Complete offset 0 | 1 | 0 | Even offset 0 can repeat |
| Checkpoint successfully | 1 | 1 | Resume at offset 1 |
| Complete offset 1 | 3 | 1 | Offsets 1 and 2 can repeat |
| Restart | 1 | 1 | Deliver offsets 1, 2, and 3 |

The interval $[P,C)$ contains contiguous completions that are not yet protected by a successful checkpoint. Completed followers above $C$ can also repeat. Thus $C-P$ alone is not the total number of possible repeated external effects. The consumer still performs work **before** acknowledgement. This protocol does not atomically commit an external database update with its checkpoint.

## Make a checkpoint small, scoped, and exact

Use one file per partition/group pair at the broker root. For example, `progress-p2-g1.ckp` is partition 2, group 1. Its temporary candidate is `progress-p2-g1.ckp.next`, in the same directory. Eight authoritative files cover four partitions and two groups. Segment discovery remains restricted to segment files inside each partition directory.

| Bytes, end exclusive | Width | Meaning |
| --- | ---: | --- |
| 0–4 | 4 | Magic `PBGP` |
| 4–6 | 2 | Version 1, big-endian |
| 6–8 | 2 | Total length 48, big-endian |
| 8–10 | 2 | Partition identifier |
| 10–11 | 1 | Group identifier |
| 11–12 | 1 | Reserved zero |
| 12–20 | 8 | Epoch that published this checkpoint |
| 20–28 | 8 | Persisted next offset |
| 28–44 | 16 | Reserved zeros |
| 44–48 | 4 | CRC-32 of bytes 0–44 |

The byte at position 10 is a group identifier, not an integer pointer or a platform-sized index. The offset begins at byte 20; its alignment in the file is irrelevant because decoding copies the field's bytes into an integer. Never serialize the memory layout of the Rust struct.

The stored epoch may be older than the current broker metadata: an unchanged checkpoint need not be rewritten on every restart. A future epoch is inconsistent and must be refused. The checksum detects accidental damage; it does not authenticate a checkpoint or detect every manually restored older valid copy. The directory is exclusively owned and not concurrently edited by other programs.

Create `crates/broker-core/src/storage/checkpoint.rs`. The name functions accept range-checked identifiers, so their strings cannot contain a caller-supplied path. The codec starts from a zeroed fixed array, writes explicit big-endian fields, and checksums only the first 44 bytes.

```rust title="crates/broker-core/src/storage/checkpoint.rs"
use std::fs::{self, File};
use std::path::Path;

use crate::io::{open_file, read_prefix};
use crate::types::{Epoch, GROUP_COUNT, GroupId, Offset, PARTITION_COUNT, PartitionId};
use super::error::StorageError;
use super::metadata::{self, PublicationStep};
use super::sync::SyncIo;

pub const CHECKPOINT_LEN: usize = 48;

pub fn name(partition: PartitionId, group: GroupId) -> String {
    format!("progress-p{}-g{}.ckp", partition.get(), group.get())
}
pub fn pending_name(partition: PartitionId, group: GroupId) -> String {
    format!("{}.next", name(partition, group))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    pub partition: PartitionId,
    pub group: GroupId,
    pub epoch: Epoch,
    pub next: Offset,
}
impl Checkpoint {
    pub fn encode(self) -> [u8; CHECKPOINT_LEN] {
        let mut raw = [0; CHECKPOINT_LEN];
        raw[..8].copy_from_slice(b"PBGP\x00\x01\x00\x30");
        raw[8..10].copy_from_slice(&self.partition.get().to_be_bytes());
        raw[10] = self.group.get();
        raw[12..20].copy_from_slice(&self.epoch.get().to_be_bytes());
        raw[20..28].copy_from_slice(&self.next.get().to_be_bytes());
        let crc = crc32fast::hash(&raw[..44]);
        raw[44..48].copy_from_slice(&crc.to_be_bytes());
        raw
    }

```

### Validate structure before interpreting authority

`decode` requires exactly 48 bytes before slicing. It checks the checksum and the version/reserved bytes before constructing typed identifiers. `try_into()` here cannot fail after the fixed-length check: every field slice has its specified constant width. A zero epoch remains invalid. An offset of `u64::MAX`, however, can be a valid **exclusive end**; no successor is computed by this codec.

```rust title="crates/broker-core/src/storage/checkpoint.rs"
    pub fn decode(raw: &[u8]) -> Result<Self, StorageError> {
        if raw.len() != CHECKPOINT_LEN {
            return Err(StorageError::Metadata("checkpoint length is not 48"));
        }
        let stored = u32::from_be_bytes(raw[44..48].try_into().expect("fixed CRC field"));
        if stored != crc32fast::hash(&raw[..44]) {
            return Err(StorageError::Metadata("checkpoint checksum mismatch"));
        }
        if &raw[..8] != b"PBGP\x00\x01\x00\x30" || raw[11] != 0
            || raw[28..44].iter().any(|b| *b != 0)
        {
            return Err(StorageError::Metadata("checkpoint header or reserved fields are invalid"));
        }
        let partition = PartitionId::new(u16::from_be_bytes(raw[8..10].try_into().expect("fixed partition")))
            .map_err(|_| StorageError::Metadata("checkpoint partition is out of range"))?;
        let group = GroupId::new(raw[10])
            .map_err(|_| StorageError::Metadata("checkpoint group is out of range"))?;
        let epoch = Epoch::new(u64::from_be_bytes(raw[12..20].try_into().expect("fixed epoch")))
            .map_err(|_| StorageError::Metadata("checkpoint epoch is zero"))?;
        let next = Offset::new(u64::from_be_bytes(raw[20..28].try_into().expect("fixed next offset")));
        Ok(Self { partition, group, epoch, next })
    }

```

A valid encoding is not yet a valid checkpoint for this directory. `validate` checks the expected partition/group, the upper bound on its publishing epoch, and the recovered interval. Equality with the recovered end is allowed: a group may have completed every currently available record. A value below the retained floor is not repaired by clamping it upward; that would silently discard the group's requested history.

```rust title="crates/broker-core/src/storage/checkpoint.rs"
    pub fn validate(self, partition: PartitionId, group: GroupId, epoch: Epoch,
        floor: Offset, end: Offset) -> Result<Offset, StorageError>
    {
        if self.partition != partition || self.group != group {
            return Err(StorageError::Metadata("checkpoint scope differs from its filename"));
        }
        if self.epoch > epoch {
            return Err(StorageError::Metadata("checkpoint comes from a future startup epoch"));
        }
        if floor > end || self.next < floor || self.next > end {
            return Err(StorageError::Metadata("checkpoint is outside the recovered record interval"));
        }
        Ok(self.next)
    }
}

```

### Read the authoritative name only

`read` rejects a symlink, a nonregular file, or a length other than 48. It does not open a `.next` candidate when the authoritative path is missing or damaged. Missing checkpoints are allowed only while exclusive first initialization is still in progress and no broker authority has been published; ordinary recovery has no zero-default branch.

The metadata inspection and subsequent open are separate calls. The existing directory lock is the cooperating-writer exclusion mechanism; this is not a secure sandbox against an unrelated process that ignores that lock.

```rust title="crates/broker-core/src/storage/checkpoint.rs"
pub fn read(root: &Path, partition: PartitionId, group: GroupId) -> Result<Checkpoint, StorageError> {
    let path = root.join(name(partition, group));
    let info = fs::symlink_metadata(&path)
        .map_err(|e| StorageError::path("inspect required checkpoint; never default to zero", &path, e))?;
    if !info.file_type().is_file() || info.len() != CHECKPOINT_LEN as u64 {
        return Err(StorageError::Metadata("checkpoint must be a regular 48-byte file"));
    }
    let mut file = File::open(&path).map_err(|e| StorageError::path("open checkpoint", &path, e))?;
    let mut raw = [0; CHECKPOINT_LEN];
    let count = read_prefix(&mut file, &mut raw)
        .map_err(|e| StorageError::path("read checkpoint", &path, e))?;
    Checkpoint::decode(&raw[..count])
}

```

The remaining helpers enumerate only the eight canonical pairs and their candidate names. `is_name` is used by root-layout validation; a filename such as `progress-p4-g0.ckp` is still an error. `publish` delegates to the shared transaction below. `synchronize` reestablishes the recovered checkpoint file's synchronization boundary before startup returns, and candidate cleanup is permitted only after authoritative validation.

`RecoveredProgress` deliberately does not implement `Clone`. It carries one startup epoch and exactly two validated next offsets into one owned log. The public codec type is useful for fixtures; it is not the client's checkpoint command.

```rust title="crates/broker-core/src/storage/checkpoint.rs"
pub(crate) fn is_name(candidate: &str) -> bool {
    (0..PARTITION_COUNT).any(|p| (0..GROUP_COUNT).any(|g| {
        let partition = PartitionId::new(p).expect("fixed partitions");
        let group = GroupId::new(g).expect("fixed groups");
        candidate == name(partition, group) || candidate == pending_name(partition, group)
    }))
}

pub(crate) fn publish<S, H>(root: &Path, value: Checkpoint, io: &mut S, after: &mut H)
    -> Result<(), StorageError>
where S: SyncIo + ?Sized, H: FnMut(PublicationStep) -> std::io::Result<()> {
    metadata::publish_bytes(root, &name(value.partition, value.group),
        &pending_name(value.partition, value.group), &value.encode(), io, after)
}

pub(crate) fn synchronize<S: SyncIo + ?Sized>(root: &Path, partition: PartitionId,
    group: GroupId, io: &mut S) -> Result<(), StorageError>
{
    let path = root.join(name(partition, group));
    let mut file = open_file(&path).map_err(|e| StorageError::path("reopen checkpoint for sync", &path, e))?;
    io.sync_checkpoint(&mut file)
        .map_err(|e| StorageError::path("synchronize recovered checkpoint", &path, e))
}

pub(crate) fn discard_pending(root: &Path, partition: PartitionId, group: GroupId)
    -> Result<(), StorageError>
{
    metadata::discard_candidate(root, &pending_name(partition, group))
}

// Not Clone: startup attaches this permission to exactly one owned log.
pub(crate) struct RecoveredProgress {
    pub(crate) epoch: Epoch,
    pub(crate) next: [Offset; GROUP_COUNT as usize],
}
```

For partition 2, group 1, publishing epoch 7 and next offset 19, the last four bytes are `f8 0f 9b 4d`. The independent golden array appears in Part 3. Notice that neither 19 nor 7 requires a host-endian interpretation: 19 is the final byte `13` in its eight-byte field. The eight authoritative bodies total $8\times48=384$ bytes, before filesystem allocation overhead. This number is not a disk quota or a memory/RSS measurement.

## Reuse the publication transaction, not a database

[Chapter 10's helper](https://25d-228.github.io/rust-learning/chapter-10-part-02.html) already writes, synchronizes, replaces, and synchronizes the containing directory. Generalize its two names and byte body instead of creating a second subtly different transaction. Rust's [`rename`](https://doc.rust-lang.org/std/fs/fn.rename.html) replaces an existing destination and cannot move between mount points. Keeping the candidate next to the authority removes that cross-filesystem case.

The order is:

```text
create_new(candidate)
write the complete candidate
synchronize candidate file
close candidate handle
rename candidate -> authority
synchronize containing directory
advance in-memory persisted frontier
return Checkpointed
```

A replacement is a namespace transition, not an implicit confirmation that all its surrounding durability work succeeded. Apple's [archived rename manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/rename.2.html) describes same-filesystem replacement; do not turn that into a guarantee for arbitrary hardware power loss. The course retains its process-crash failure model.

`create_file` still uses exclusive creation. [`OpenOptions::create_new`](https://doc.rust-lang.org/std/fs/struct.OpenOptions.html#method.create_new) refuses an existing path, including a symlink. A leftover candidate therefore stops a live publication; it is not overwritten, promoted, or blindly retried. Recovery decides whether it is safe to discard it.

### Observe steps only after the real operation

Replace `storage/sync.rs` with the following complete file. `publication_step` is a test seam called **after** each named operation. Its default does nothing; it does not implement the operation. A gate can pause there, and an injected error can test uncertainty even after directory synchronization. `sync_checkpoint` separates synchronization of an already recovered checkpoint from synchronization of a new publication candidate.

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

The native adapter continues to use the existing `StorageFile::sync` and directory `File::sync_all`. [`File::sync_all`](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all) reports synchronization errors; merely dropping a file does not report its close-time errors. Directory synchronization errors remain errors, not a portable-success fiction. Use a compatible local filesystem and diagnose an unsupported operation rather than deleting the check.

Apple's [archived fsync manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html) distinguishes host flushing from stronger device-cache guarantees and discusses `F_FULLFSYNC`. This chapter does not add such a backend, infer your macOS version, or claim that the process-kill harness tests sudden device power loss.

### Keep the existing broker metadata encoding

Replace `storage/metadata.rs`. Its first codec/read blocks retain the 48-byte `broker.meta` format from Chapter 10; the generalized publication code follows them. Group progress gets its own magic and files, not an overloaded broker epoch field.

```rust title="crates/broker-core/src/storage/metadata.rs"
use std::fs::{self, File};
use std::io;
use std::path::Path;

use crate::io::{create_file, read_prefix, write_fully};
use crate::types::{Epoch, GROUP_COUNT, PARTITION_COUNT};
use super::error::StorageError;
use super::segment::StorageConfig;
use super::sync::SyncIo;

pub const META_NAME: &str = "broker.meta";
pub const TEMP_NAME: &str = "broker.meta.next";
pub const META_LEN: usize = 48;

#[derive(Debug, Clone, Copy)]
pub struct Metadata {
    epoch: Epoch,
    config: StorageConfig,
}

impl Metadata {
    pub fn new(epoch: Epoch, config: StorageConfig) -> Self { Self { epoch, config } }
    pub fn epoch(self) -> Epoch { self.epoch }
    pub fn config(self) -> StorageConfig { self.config }

    pub fn encode(self) -> [u8; META_LEN] {
        let mut raw = [0; META_LEN];
        raw[..4].copy_from_slice(b"PBMD");
        raw[4..6].copy_from_slice(&1_u16.to_be_bytes());
        raw[6..8].copy_from_slice(&48_u16.to_be_bytes());
        raw[8..16].copy_from_slice(&self.epoch.get().to_be_bytes());
        raw[16..18].copy_from_slice(&PARTITION_COUNT.to_be_bytes());
        raw[18] = GROUP_COUNT;
        raw[20..28].copy_from_slice(&self.config.segment_bytes().to_be_bytes());
        raw[28..36].copy_from_slice(&self.config.index_stride().to_be_bytes());
        raw[36..40].copy_from_slice(&(self.config.max_segments() as u32).to_be_bytes());
        let crc = crc32fast::hash(&raw[..44]);
        raw[44..48].copy_from_slice(&crc.to_be_bytes());
        raw
    }

```

The broker authority still validates its fixed partition/group counts and storage configuration. Do not reinterpret a valid group checkpoint as a replacement broker metadata file; the magic, field layout, and purpose differ.

```rust title="crates/broker-core/src/storage/metadata.rs"
    pub fn decode(raw: &[u8]) -> Result<Self, StorageError> {
        if raw.len() != META_LEN { return Err(StorageError::Metadata("length is not 48")); }
        let stored = u32::from_be_bytes(raw[44..48].try_into().expect("fixed CRC field"));
        if stored != crc32fast::hash(&raw[..44]) {
            return Err(StorageError::Metadata("checksum mismatch"));
        }
        if &raw[..8] != b"PBMD\x00\x01\x00\x30" {
            return Err(StorageError::Metadata("unsupported magic, version, or header length"));
        }
        let partitions = u16::from_be_bytes(raw[16..18].try_into().expect("fixed partition count"));
        if partitions != PARTITION_COUNT || raw[18] != GROUP_COUNT {
            return Err(StorageError::Metadata("partition or group count differs"));
        }
        if raw[19] != 0 || raw[40..44].iter().any(|b| *b != 0) {
            return Err(StorageError::Metadata("reserved fields are nonzero"));
        }
        let epoch = Epoch::new(u64::from_be_bytes(raw[8..16].try_into().expect("fixed epoch")))
            .map_err(StorageError::Epoch)?;
        let segment_bytes = u64::from_be_bytes(raw[20..28].try_into().expect("fixed segment cap"));
        let stride = u64::from_be_bytes(raw[28..36].try_into().expect("fixed stride"));
        let count = u32::from_be_bytes(raw[36..40].try_into().expect("fixed segment count"));
        let count = usize::try_from(count)
            .map_err(|_| StorageError::Metadata("segment count is not representable"))?;
        let config = StorageConfig::new(segment_bytes, stride, count)?;
        Ok(Self { epoch, config })
    }
}

```

The authoritative read and `PublicationStep` enum retain their previous roles. Step names describe what has already happened, which is essential when a failure is injected at `Renamed` rather than before replacement.

```rust title="crates/broker-core/src/storage/metadata.rs"
pub fn read(root: &Path) -> Result<Metadata, StorageError> {
    let path = root.join(META_NAME);
    let info = fs::symlink_metadata(&path)
        .map_err(|e| StorageError::path("inspect authoritative metadata; do not initialize on failure", &path, e))?;
    if !info.file_type().is_file() || info.len() != META_LEN as u64 {
        return Err(StorageError::Metadata("expected a regular 48-byte file"));
    }
    let mut file = File::open(&path).map_err(|e| StorageError::path("open metadata", &path, e))?;
    let mut raw = [0; META_LEN];
    let count = read_prefix(&mut file, &mut raw)
        .map_err(|e| StorageError::path("read metadata", &path, e))?;
    Metadata::decode(&raw[..count])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationStep {
    TemporaryCreated,
    TemporaryWritten,
    TemporarySynced,
    Renamed,
    DirectorySynced,
}

```

`discard_pending` now delegates to a name-parameterized cleanup helper. Only a regular candidate file can be removed. The old metadata entry point stays intact so the earlier startup tests keep observing the same broker-publication stages.

```rust title="crates/broker-core/src/storage/metadata.rs"
pub(crate) fn discard_pending(root: &Path) -> Result<(), StorageError> {
    discard_candidate(root, TEMP_NAME)
}

pub(crate) fn discard_candidate(root: &Path, name: &str) -> Result<(), StorageError> {
    let path = root.join(name);
    match fs::symlink_metadata(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StorageError::path("inspect pending metadata", &path, e)),
        Ok(info) if info.file_type().is_file() => fs::remove_file(&path)
            .map_err(|e| StorageError::path("remove abandoned metadata candidate", &path, e)),
        Ok(_) => Err(StorageError::Metadata("pending metadata is not a regular file")),
    }
}

pub(crate) fn publish<S, H>(root: &Path, value: Metadata, io: &mut S, after: &mut H)
    -> Result<(), StorageError>
where S: SyncIo + ?Sized, H: FnMut(PublicationStep) -> io::Result<()> {
    publish_bytes(root, META_NAME, TEMP_NAME, &value.encode(), io, after)
}

```

`mark` calls the synchronization adapter's observer and then the caller's existing callback. Both errors retain the target path. Generic `S: SyncIo + ?Sized` accepts a concrete adapter or a borrowed trait object; no boxed database or opaque checkpoint implementation is involved.

```rust title="crates/broker-core/src/storage/metadata.rs"
fn mark<S, H>(io: &mut S, after: &mut H, target: &Path, step: PublicationStep)
    -> Result<(), StorageError>
where S: SyncIo + ?Sized, H: FnMut(PublicationStep) -> io::Result<()> {
    io.publication_step(target, step)
        .map_err(|e| StorageError::path("publication observer failed", target, e))?;
    after(step).map_err(|e| StorageError::path("interrupted metadata publication", target, e))
}

// Names are crate-owned constants or generated from validated fixed-range IDs.
```

The complete replacement routine follows. There is deliberately no cleanup guard that erases the candidate on failure: the directory remains in the actual interrupted state for recovery to inspect. All names here originate from crate-owned constants or typed-identifier formatting. Neither a network client nor an arbitrary filesystem path can select the authority being replaced.

```rust title="crates/broker-core/src/storage/metadata.rs"
pub(crate) fn publish_bytes<S, H>(root: &Path, current_name: &str, temp_name: &str,
    bytes: &[u8], io: &mut S, after: &mut H) -> Result<(), StorageError>
where S: SyncIo + ?Sized, H: FnMut(PublicationStep) -> io::Result<()> {
    let temp = root.join(temp_name);
    let current = root.join(current_name);
    let mut file = create_file(&temp)
        .map_err(|e| StorageError::path("create metadata candidate", &temp, e))?;
    mark(io, after, &current, PublicationStep::TemporaryCreated)?;
    write_fully(&mut file, bytes)
        .map_err(|e| StorageError::path("write metadata candidate", &temp, e))?;
    mark(io, after, &current, PublicationStep::TemporaryWritten)?;
    io.sync_metadata(&mut file)
        .map_err(|e| StorageError::path("synchronize metadata candidate", &temp, e))?;
    mark(io, after, &current, PublicationStep::TemporarySynced)?;
    drop(file);
    fs::rename(&temp, &current)
        .map_err(|e| StorageError::path("replace authoritative metadata", &current, e))?;
    mark(io, after, &current, PublicationStep::Renamed)?;
    io.sync_directory(root)
        .map_err(|e| StorageError::path("synchronize metadata directory", root, e))?;
    mark(io, after, &current, PublicationStep::DirectorySynced)?;
    Ok(())
}
```

| Last successful stage | Authoritative file in a live-process cut | In-memory persisted frontier | Client interpretation without success |
| --- | --- | --- | --- |
| Candidate created/written/synchronized | Old checkpoint | Old | Completion may repeat |
| Candidate renamed | New checkpoint | Old | Publication outcome is uncertain to caller |
| Directory synchronized, observer fails | New checkpoint | Old | Still no success reply; do not roll back |
| Entire helper returns | New checkpoint | New | A `Checkpointed` response may be sent |

The “live-process cut” column describes the deterministic process tests, not all possible filesystem states after an operating-system crash. Restart validates whatever authoritative file exists; it does not infer the caller's response history. An error after replacement may therefore lead to a later restart at the new frontier despite the caller having received no checkpoint success.

## Bind validated starts to the log that owns the directory

The runtime must not accept a client-supplied “resume from here” integer. Add a private startup seed to `PartitionLog`, set it only during initialization/recovery, and consume it when durable delivery starts. `Option::as_ref` borrows the seed for the epoch check. [`Option::take`](https://doc.rust-lang.org/std/option/enum.Option.html#method.take) then moves it out and leaves `None`; checking first avoids consuming a seed merely because a caller supplied the wrong epoch.

Replace `storage/log.rs` with these complete blocks. The segment code is retained so cursor placement, poisoning, sparse reads, and data synchronization continue to have exactly the same implementation. The new field and methods do not open or repair checkpoint files.

```rust title="crates/broker-core/src/storage/log.rs"
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::codec::SegmentHeader;
use crate::io::{create_file, open_file};
use crate::types::{Epoch, GROUP_COUNT, Offset, PartitionId};
use super::checkpoint::RecoveredProgress;
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

`recovered_progress` begins as `None` in both low-level constructors. Creating segment files or strictly reopening them does not prove that all eight group authorities were validated, or that a restart epoch was published. The existing `_directory` share still owns exclusion for the log's lifetime.

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
    recovered_progress: Option<RecoveredProgress>,
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
            poisoned: false, recovered_progress: None, _directory: directory })
    }

```

Strict reopening remains a reconstruction of complete records and disposable indexes. It is intentionally not a fallback checkpoint initializer. A low-level log can still serve earlier storage exercises, but it cannot start the new durable-delivery mode without startup provenance.

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
        let dirty = vec![true; segments.len()];
        Ok(Self { segments, dirty, directory_dirty: true, root, partition, config, poisoned: false, recovered_progress: None, _directory: directory })
    }

```

### Consume the seed once

`install_progress` is crate-private and called by successful startup construction. `take_progress` returns an owned root path and a copied fixed array only after validating the session epoch. The log itself remains owned by the worker; its guard, not the path string, keeps the data directory exclusive. No returned state query gives a client the ability to install a seed.

```rust title="crates/broker-core/src/storage/log.rs"
    pub fn partition(&self) -> PartitionId { self.partition }

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

Rotation and preflight remain the Chapter 8/9 logic. A checkpoint does not rotate the log, assign an offset, or create a data synchronization group. Keeping those mutations separate prevents a consumer command from accidentally acknowledging producer work.

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
        if size as u64 > self.config.segment_bytes() - tail.bytes
            && self.segments.len() == self.config.max_segments()
        {
            return Err(StorageError::SegmentLimit);
        }
        Ok(size)
    }

```

Append and synchronization keep their fail-stop behavior. A checkpoint frontier will be bounded by the worker's synchronized `visible_end`, not by a file's current length or by `written_end` while a producer batch is pending.

```rust title="crates/broker-core/src/storage/log.rs"
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

The bounded read is retained unchanged. In particular, it uses the actual segment floor, not a checkpoint-derived guess. Chapter 14 will make that floor persistent before deleting any segment; this chapter deletes nothing.

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

## Validate eight authorities before applying even one repair

The startup plan already scans all four partitions without modifying records. Now it also loads each group's checkpoint against the corresponding **planned complete end**, before the first truncation. A checkpoint that points into an incomplete record must not cause recovery to truncate the record and then quietly move the checkpoint backward.

For example, partition 1 contains one torn record after its 32-byte segment header. Its planned complete end is 0. A checksummed checkpoint with next offset 1 is invalid even though the file contains some bytes for record 0. Startup returns an error while preserving the torn bytes for diagnosis. It does not claim that offset 0 was available or completed.

Replace `storage/recovery.rs` with the following complete blocks. Most of the scanning functions are carried forward from Chapter 10. The new pieces are the checkpoint startup event, recognized root names, first-creation publications, all-checkpoint validation before repair, and seed attachment after recovered synchronization.

```rust title="crates/broker-core/src/storage/recovery.rs"
use std::fs;
use std::io::{self, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

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

```

The scan's `end` remains the next complete record offset, not a byte count and not a delivery frontier. `StorageSession::into_parts` keeps its existing interface: the validated progress is transported inside each log rather than in a second freely paired vector of public offsets.

```rust title="crates/broker-core/src/storage/recovery.rs"
struct Scan {
    end: Offset,
    repair: Option<Repair>,
}

struct PartitionPlan {
    partition: PartitionId,
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

The partial-record validator still checks whether an interrupted suffix could have been a legal record in this segment. Checkpoints neither relax those format checks nor authorize removal of unexplained corruption.

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

The first half of `scan` distinguishes complete segment headers from the narrow interrupted-rotation case. Only a matching prefix in a rotated active tail can be completed. An incomplete original segment header is still not an initialized empty partition.

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
```

The record loop advances `end` only after the full header and payload validate. Its proposed repair is a later operation. This separation gives checkpoint validation a trustworthy upper bound before any truncation occurs.

```rust title="crates/broker-core/src/storage/recovery.rs"
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

Partition planning checks contiguous bases and carries the single permitted active-tail repair. Root validation then allows only the known metadata, lock, partition directories, and checkpoint names. A nonregular candidate is refused before repair just like any other unexplained root entry.

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
        if [".broker.lock", META_NAME, TEMP_NAME].contains(&name) || checkpoint::is_name(name) {
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

`apply` is the retained repair routine. Its length recheck and synchronization remain necessary even though a checkpoint was validated. A group frontier cannot substitute for byte-level recovery checks or make a failed synchronization successful.

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

### First creation publishes broker authority last

Initialization exclusively creates an absent directory, creates and synchronizes the four logs, then publishes all eight zero checkpoints under epoch 1. Only after those publications succeed is `broker.meta` created. Interruption before that final authority leaves a failed initialization, not an invitation to infer a new empty broker.

The callback wraps checkpoint stages in `StartupStep::Checkpoint`, preserving their partition/group identity. Earlier tests that cut only `StartupStep::Metadata` still target the final broker authority rather than accidentally stopping at the first group checkpoint.

```rust title="crates/broker-core/src/storage/recovery.rs"
pub fn initialize(path: &Path, config: StorageConfig) -> Result<StorageSession, StorageError> {
    initialize_with(path, config, &mut FileSync, &mut |_| Ok(()))
}

pub fn initialize_with<S, H>(path: &Path, config: StorageConfig, io: &mut S, after: &mut H)
    -> Result<StorageSession, StorageError>
where S: SyncIo + ?Sized, H: FnMut(StartupStep) -> io::Result<()> {
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

### Recovery must finish validation before mutation

`read(...).and_then(...)` runs semantic validation only after decoding succeeds. The added `InFile` wrapper keeps the checkpoint filename with either failure. All eight entries are checked before `PlanValidated` and before `apply`.

After repairs, recovery strictly reopens and synchronizes each log, synchronizes its already validated checkpoints, and discards abandoned candidates. It then publishes the new broker epoch. A failed final publication returns no session even though some earlier operations may already have succeeded. The attached seeds cannot escape because the local vector is dropped on error.

Checkpoint-file synchronization has its own adapter method so the tests can distinguish a recovered authority that fails to synchronize from a newly written candidate that fails to publish. The final root-directory synchronization in broker metadata publication also covers completed candidate cleanup; there is no success path that omits it.

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
        plans.push(plan_partition(root, PartitionId::new(n).expect("fixed partition range"), config)?);
    }
    let mut starts = Vec::with_capacity(PARTITION_COUNT as usize);
    for plan in &plans {
        let mut next = [Offset::new(0); GROUP_COUNT as usize];
        for g in 0..GROUP_COUNT {
            let group = GroupId::new(g).expect("fixed group range");
            next[usize::from(g)] = checkpoint::read(root, plan.partition, group)
                .and_then(|value| value.validate(plan.partition, group, old.epoch(), Offset::new(0), plan.end))
                .map_err(|source| StorageError::InFile {
                    path: root.join(checkpoint::name(plan.partition, group)), source: Box::new(source),
                })?;
        }
        starts.push(next);
    }
    notify(root, after, StartupStep::PlanValidated)?;
    let mut repairs = Vec::new();
    for plan in &plans {
        if let Some(repair) = plan.repair {
            apply(root, repair, io, after)?;
            repairs.push(repair);
        }
    }
    let mut logs = Vec::with_capacity(PARTITION_COUNT as usize);
    for (plan, next) in plans.into_iter().zip(starts) {
        let mut log = PartitionLog::open_strict(Arc::clone(&directory), plan.partition, config)?;
        if log.written_end() != plan.end {
            return Err(StorageError::Topology("validated end changed while reconstructing indexes"));
        }
        log.synchronize(io)?;
        for g in 0..GROUP_COUNT {
            let group = GroupId::new(g).expect("fixed group range");
            checkpoint::synchronize(root, plan.partition, group, io)?;
            checkpoint::discard_pending(root, plan.partition, group)?;
        }
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

Update the storage module index so the new codec is part of the crate.

```rust title="crates/broker-core/src/storage/mod.rs"
pub mod checkpoint;
pub mod directory;
pub mod error;
pub mod index;
pub mod log;
pub mod metadata;
pub mod recovery;
pub mod segment;
pub mod sync;
```

## What startup now proves

A returned session has four reconstructed logs, eight valid and synchronized progress authorities, and a newly published epoch. Each group's loaded frontier lies within its log's available complete interval. Each log retains the directory lock and one consumable seed for that new epoch.

A missing or corrupt authority, future checkpoint epoch, scope mismatch, or frontier beyond the recovered end produces no session. There is no old-checkpoint fallback and no candidate promotion. Once retention removes records, falling back below the published floor would claim unavailable history; the explicit lower-bound check is already present in the checkpoint validator, although the recovery floor remains zero here.

Do not start the durable runtime with an epoch invented from a constant. [Part 2](https://25d-228.github.io/rust-learning/chapter-13-part-02.html) consumes the seeds and makes the acknowledgement/checkpoint distinction visible in command responses.

