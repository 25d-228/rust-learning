# Chapter 10 — Recovery, metadata publication, and restart epochs
## Part 2 — Publish the startup epoch

This part continues [Part 1](https://25d-228.github.io/rust-learning/chapter-10.html). All three parts share the chapter's **6.0 implementation hours**. Keep the `recovery.rs` fragments from Part 1; the later fragments here complete that same file. `metadata.rs` is new. Tests and the native process harness follow in [Part 3](https://25d-228.github.io/rust-learning/chapter-10-part-03.html).

## A directory needs an authoritative initialization record

A directory containing some segment files is not enough evidence that broker initialization completed. First creation can stop after creating partition 0 but before partition 3, or after writing a temporary metadata file but before publishing it. Reopening such a directory as “new” could reset epochs around old data.

The authority is exactly `broker.meta`, not the newest-looking file and not whichever candidate has the larger counter. `broker.meta.next` is a same-directory replacement candidate. Recovery never falls back to it when `broker.meta` is missing or invalid. A valid current file is a prerequisite for discarding an abandoned candidate and attempting the next publication.

The root layout at this chapter's endpoint is:

```text
broker/
  .broker.lock
  broker.meta
  broker.meta.next       only while publication is pending or abandoned
  partition-0/
    00000000000000000000.seg
  partition-1/
    00000000000000000000.seg
  partition-2/
    00000000000000000000.seg
  partition-3/
    00000000000000000000.seg
```

There is no persisted consumer progress or retention floor yet. Those will extend the storage protocol in their own chapters. Unknown entries currently fail closed rather than being guessed to be harmless future files.

## Specify metadata as bytes, not Rust struct memory

The fixed metadata record is 48 bytes. Integers are big-endian. The checksum covers bytes 0 through 43, so version, epoch, counts, configuration, and reserved fields are all covered. This is the same CRC primitive used by the record codec, not authentication against a malicious writer.

| Byte interval | Width | Meaning |
| --- | ---: | --- |
| 0–3 | 4 | `PBMD` magic |
| 4–5 | 2 | Version 1 |
| 6–7 | 2 | Header length 48 |
| 8–15 | 8 | Nonzero restart epoch |
| 16–17 | 2 | Fixed partition count: 4 |
| 18 | 1 | Fixed group count: 2 |
| 19 | 1 | Reserved zero |
| 20–27 | 8 | Segment byte cap |
| 28–35 | 8 | Sparse-index stride |
| 36–39 | 4 | Maximum segment count per partition |
| 40–43 | 4 | Reserved zeros |
| 44–47 | 4 | CRC of the preceding 44 bytes |

Configuration is stored so a restart cannot reinterpret old files under an accidentally different cap. This baseline also persists index stride for reproducibility even though indexes themselves are disposable. Changing stored configuration is not silently allowed by `recover`; it reads the authoritative values. Later configuration evolution needs an explicit protocol rather than a caller-supplied override.

Create `src/storage/metadata.rs`. This first piece defines the layout and all codec operations.

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

`encode` accepts already validated types. The segment-count cast fits because `StorageConfig` caps that count at 64. Decoding cannot trust that premise yet, so it checks the checksum, format, topology counts, reserved bytes, epoch, integer conversion, and `StorageConfig::new` in order.

The `expect` calls convert slices with statically established lengths after the exact 48-byte check. They are not substitutes for checking untrusted lengths. A zero epoch returns the typed identity error; a maximum epoch can be decoded but cannot be incremented during recovery.

For epoch 7, cap 102, stride 2, and at most 8 segments, the expected bytes are:

```text
50 42 4d 44 00 01 00 30
00 00 00 00 00 00 00 07
00 04 02 00 00 00 00 00
00 00 00 66 00 00 00 00
00 00 00 02 00 00 00 08
00 00 00 00 82 b5 3a 19
```

The final four bytes are the checksum, not another counter. Part 3 compares against this independently specified golden array and also changes meaningful fields while recomputing their checksums; a checksum-valid incompatible format must still be refused.

## Read exactly the authoritative file

`read` rejects a nonregular file and any length other than 48 before allocating payload storage. Its buffer is a fixed stack array. The [symlink metadata API](https://doc.rust-lang.org/std/fs/fn.symlink_metadata.html) inspects the named entry rather than following its final symlink; this prevents silently accepting a symlink as the authoritative metadata file in this trusted directory layout.

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

```

A missing file remains an error with its path and original I/O cause. It is not converted into epoch zero or an “uninitialized but reusable” state. If an external editor changes the file after the metadata check, the lock has already been violated; the count and format checks still catch many such changes, but path-based access is not presented as adversarial race protection.

The publication phases below identify completed operations. `TemporaryWritten` is before synchronization; `Renamed` is after replacement but before the containing-directory synchronization. Tests can stop at these distinctions without inventing timing through sleeps.

`discard_pending` removes only a regular candidate. It is crate-visible because startup must call it only after reading a valid authority and validating the broker. No code path promotes its content.

```rust title="crates/broker-core/src/storage/metadata.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationStep {
    TemporaryCreated,
    TemporaryWritten,
    TemporarySynced,
    Renamed,
    DirectorySynced,
}

pub(crate) fn discard_pending(root: &Path) -> Result<(), StorageError> {
    let path = root.join(TEMP_NAME);
    match fs::symlink_metadata(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StorageError::path("inspect pending metadata", &path, e)),
        Ok(info) if info.file_type().is_file() => fs::remove_file(&path)
            .map_err(|e| StorageError::path("remove abandoned metadata candidate", &path, e)),
        Ok(_) => Err(StorageError::Metadata("pending metadata is not a regular file")),
    }
}

```

## Write, synchronize, replace, synchronize the directory

Rust's [exclusive file creation](https://doc.rust-lang.org/std/fs/struct.OpenOptions.html#method.create_new) refuses an already existing entry, including a symlink. `create_file` from Chapter 7 uses it for the candidate. The same-directory placement avoids a cross-filesystem move; [Rust's rename documentation](https://doc.rust-lang.org/std/fs/fn.rename.html) describes replacement of an existing destination and the filesystem restriction. Apple's [rename manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/rename.2.html) describes the native name-replacement behavior.

The algorithm is not “write metadata in place.” A partial write to `broker.meta.next` must not destroy the old authority. Complete the candidate, synchronize it, close its handle, replace the authoritative name, then synchronize the containing directory. The caller retains the directory lock throughout.

```rust title="crates/broker-core/src/storage/metadata.rs"
pub(crate) fn publish<S, H>(root: &Path, value: Metadata, io: &mut S, after: &mut H)
    -> Result<(), StorageError>
where
    S: SyncIo + ?Sized,
    H: FnMut(PublicationStep) -> io::Result<()>,
{
    let temp = root.join(TEMP_NAME);
    let current = root.join(META_NAME);
    let mut mark = |point| after(point)
        .map_err(|e| StorageError::path("interrupted metadata publication", root, e));
    let mut file = create_file(&temp)
        .map_err(|e| StorageError::path("create metadata candidate", &temp, e))?;
    mark(PublicationStep::TemporaryCreated)?;
    write_fully(&mut file, &value.encode())
        .map_err(|e| StorageError::path("write metadata candidate", &temp, e))?;
    mark(PublicationStep::TemporaryWritten)?;
    io.sync_metadata(&mut file)
        .map_err(|e| StorageError::path("synchronize metadata candidate", &temp, e))?;
    mark(PublicationStep::TemporarySynced)?;
    drop(file);
    fs::rename(&temp, &current)
        .map_err(|e| StorageError::path("replace authoritative metadata", &current, e))?;
    mark(PublicationStep::Renamed)?;
    io.sync_directory(root)
        .map_err(|e| StorageError::path("synchronize metadata directory", root, e))?;
    mark(PublicationStep::DirectorySynced)?;
    Ok(())
}
```

`mark` borrows the caller's `FnMut` callback and adds publication context if it requests a stop. It owns no file. An error from writing, synchronization, renaming, directory synchronization, or the callback returns immediately. We deliberately do not delete either authoritative state or the remaining candidate on error: a subsequent startup decides from the actual names.

The candidate's file synchronization and the directory synchronization are separately fallible. If rename succeeds and directory sync fails, the process can observe the new metadata even though this attempt returns an error. It must not serve clients and must not restore the old epoch. A later startup rereads the authority rather than interpreting an error as rollback.

## An epoch is a checked incarnation counter, not a clock

Let $E_d$ be the epoch read from valid authoritative metadata and $E_n$ the candidate for the new session. Compute:

$$
E_n = E_d + 1.
$$

The addition must be representable in a nonzero 64-bit epoch. At $E_d=2^{64}-1$, startup fails before repairing files. There is no wrap to zero or one. Fresh explicit initialization publishes epoch 1, and the first restart normally publishes 2.

Consider an old epoch of 7. The candidate contains 8:

| Interruption point | Authority observed in this process-crash fixture | Next successful startup |
| --- | --- | --- |
| After candidate creation/write/sync, before rename | 7 | Publishes 8 |
| After rename, before directory sync or return | 8 | Publishes 9 |
| After all publication operations and service begins | 8 | Publishes 9 |

Reusing candidate 8 in the first row is safe because that candidate was never exposed as a session. Skipping 8 in the second row is safe because freshness requires nonreuse of a served incarnation, not dense numbering. These are controlled process-interruption states, not a prediction of arbitrary power-loss persistence.

This epoch domain belongs to one authoritative directory lineage. Restoring an old backup over it or replacing it with a newly initialized directory can roll that history back; it is outside this protocol. Do not treat the counter as a globally unique broker identifier. Delivery-generation tokens and acknowledgement comparison arrive in later chapters; this chapter supplies the published incarnation they will include.

## Validate the namespace before applying a plan

Resume `src/storage/recovery.rs` immediately after `plan_partition` from Part 1. First check the root itself is an actual directory, and require exactly the four partition directories plus the recognized regular files. Partition discovery already rejects symlinks and unfamiliar entries inside those directories.

The fixed boolean array tracks presence of partitions 0 through 3. `find` checks the four allowed names; it does not parse arbitrary user-controlled numbers and allocate a table of that size. Missing partitions are not created during recovery.

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
        if [".broker.lock", META_NAME, TEMP_NAME].contains(&name) {
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

`DirectoryGuard::acquire` is still the cooperative exclusive-owner primitive. We retain the same non-replaced `.broker.lock` handle; metadata replacement never replaces the lock inode. Permission errors, unknown entries, or missing required directories stop startup. Do not “fix” a busy directory by unlinking the lock file.

## Apply only the already validated repair

A `Repair` is a private planner's result within this startup call, not an arbitrary external truncate command. The function opens its canonical file and rechecks the inspected length. Under the single-owner assumption the bytes cannot have changed between plan and apply. A different length invalidates the plan.

For a record repair, reduce the length to the last complete boundary. For a partial rotated segment header, write the exact expected header at position zero. Both cases then set the cursor to the resulting end and synchronize the file before reporting the repair as synchronized.

The explicit seek matters: [Rust's file truncation API](https://doc.rust-lang.org/std/fs/struct.File.html#method.set_len) does not move the cursor. Truncating a file from 101 to 67 bytes while leaving a cursor at 101 would be an incorrect basis for an append. We use absolute seeks here and retain Chapter 8's absolute seek on every append.

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

Stopping after `RepairApplied` can leave the shorter file or completed header visible but not yet synchronized by this attempt. The next startup scans it again. A completed truncation requires no second truncation; an incomplete header completion remains an exact prefix if the underlying write stopped within it. Full invalid headers remain failures, not invitations to retry arbitrary rewriting.

A repair is therefore rerunnable, but the entire startup is not a multi-file atomic transaction. One partition's permitted repair may complete before an I/O failure in another partition. No clients are exposed in that interval; the next recovery validates the remaining state again.

Make one precise insertion in the existing `src/storage/segment.rs`: inside `Segment::open_strict`, immediately after the scan loop and immediately before its final `Ok(segment)`, insert these lines. Keep all earlier append, read, and index code unchanged. The downloadable overlay contains the complete resulting file.

```rust title="crates/broker-core/src/storage/segment.rs — insert before open_strict returns"
        segment.file.seek(SeekFrom::Start(segment.bytes))
            .map_err(|e| StorageError::io("reset append cursor", segment.bytes, e))?;
```

This explicitly establishes the reopened append position even though the successful scan already normally ends there. It does not replace the independent absolute seek performed by `append`; a later read can still move the cursor.

## First creation has one deliberate path

`initialize` is the ordinary entry point. Its `_with` counterpart accepts the synchronization adapter and interruption callback for tests. A no-op callback returns `Ok(())`; it does not bypass any filesystem operation.

Call [create_dir](https://doc.rust-lang.org/std/fs/fn.create_dir.html), not `create_dir_all` or an “open or initialize” helper. The leaf must be absent and its parent must already exist. After creating the leaf and acquiring its guard, construct all four partition logs, synchronize their initial segment headers and directories, synchronize the root and the root's parent, and only then publish epoch 1.

The parent synchronization covers the new broker-directory name. Root synchronization covers the partition-directory and lock-file names. Each partition's synchronization covers its segment name. These are distinct namespace levels.

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
    metadata::publish(root, Metadata::new(epoch, config), io,
        &mut |point| after(StartupStep::Metadata(point)))?;
    Ok(StorageSession { epoch, logs, repairs: Vec::new() })
}

```

If initialization stops before `broker.meta` is renamed into place, the existing directory is incomplete and ordinary recovery refuses it. Initialization also refuses to reuse it. The caller must inspect and quarantine an abandoned fixture or choose a new path; the library does not recursively delete an existing directory in an error handler. The provided tests use exclusively owned temporary directories, where cleanup is explicit.

If initialization stops after the authoritative rename, recovery can validate the finished layout and increment the epoch. The initializer still returned no session in that interrupted attempt. There is no special “resume initialization by trusting a candidate” shortcut.

## Reopen all partitions before returning any of them

`recover` takes no storage configuration argument: it uses the stored values. It first acquires the directory guard, reads the authority, computes a checked successor epoch, and validates the layout. It then plans all four partitions without mutation. Only a successful complete plan permits repair.

After repairs it strictly reopens each log, rebuilds its indexes, checks the reconstructed end against the plan, and synchronizes every log and partition directory. Only then does it discard a stale candidate and publish the new metadata. This order also synchronizes complete unconfirmed records before they can become visible.

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
    notify(root, after, StartupStep::PlanValidated)?;
    let mut repairs = Vec::new();
    for plan in &plans {
        if let Some(repair) = plan.repair {
            apply(root, repair, io, after)?;
            repairs.push(repair);
        }
    }
    let mut logs = Vec::with_capacity(PARTITION_COUNT as usize);
    for plan in plans {
        let mut log = PartitionLog::open_strict(Arc::clone(&directory), plan.partition, config)?;
        if log.written_end() != plan.end {
            return Err(StorageError::Topology("validated end changed while reconstructing indexes"));
        }
        log.synchronize(io)?;
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

A repaired file is synchronized in `apply` and then synchronized again during strict reopening's startup pass. That small redundancy is intentional: the reopened logs all begin with the same conservative dirty-state rule. It avoids a special “already synchronized because of some earlier repair” path through log construction. A future optimization would need to retain the same failure ordering and be justified by measurement.

`logs` does not leave the function until `metadata::publish` returns successfully. On any error, local logs and the last guard share are dropped, and no client is created. If publication succeeds but a later caller cannot start an owner thread, the consumed epoch remains used; it is not rolled back.

To integrate with Chapter 9, consume the returned session, retain the epoch for delivery identities, and move each `PartitionLog` into its `WorkerHandle::start`. That constructor synchronizes its log before returning a client; recovered logs are already clean, so the call establishes the visible end without repeating file operations. The complete `recovery_probe` in Part 3 shows this handoff for partition 0 while retaining the other logs. The later supervisor will handle the whole service lifecycle.

The return boundary guarantees readiness of storage, not readiness of network listeners. It also does not authorize treating every stored record as producer-confirmed. In the earlier `cat`/`dog` trace, recovery exposes both after synchronization but cannot reconstruct which producer saw success.

## Diagnose by phase, not by retrying blindly

If startup reports missing authoritative metadata, inspect the selected root and whether first creation was interrupted; do not manufacture epoch 1. If it reports a bad complete checksum or wrong partition/base, preserve the files for diagnosis; do not truncate until the error disappears. If it reports synchronization failure, retain the error chain and stop service. A later deliberate restart must reread disk state, because an earlier rename or write may already have happened.

The important checks in [Part 3](https://25d-228.github.io/rust-learning/chapter-10-part-03.html) exercise each distinction: partial versus complete records, active versus immutable files, old versus newly renamed metadata, and ordinary error unwinding versus actual process termination.

