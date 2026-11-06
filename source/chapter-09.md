# Chapter 9, Part 1 — Dirty-file synchronization and timed mailboxes

The previous chapter can write `cat` at offset 0 and immediately read it back. That proves neither that a synchronization completed nor that a producer received a success. Adding threads without separating those facts would make a faster-looking broker with an undefined publication boundary.

We will give each partition one owning thread. That thread writes records, collects a bounded group, synchronizes every dirty segment, and only then advances a visible boundary and completes producer replies. A separate control lane lets reads and observations receive recurring service between data commands. A timer prevents a small final group from waiting indefinitely for more traffic.

**Prerequisites:** all of Chapters 1–8, including both parts of Chapters 7 and 8. In particular, retain `broker-core`'s typed identities, record codec, `StorageFile`, directory guard, sparse index, storage errors, test directory helper, and all earlier examples/tests. Chapter 5 supplies the mutex/condition-variable foundation; Chapter 6 supplies the distinction between abandoning a reply and cancelling transferred work. No dependency or manifest changes are needed. The existing edition and file-lock API requirements remain unchanged; a newer compatible native Rust toolchain is also suitable.

**Implementation time for all of Chapter 9:** 6.0 hours: 4.25 for the storage extension, mailbox, owner loop, batching, and driver; 1.75 for controlled interleavings, failures, and diagnosis. These three linked parts share one estimate. Reading is excluded. Record actual implementation time; there is no need to keep working merely to match an estimate.

> **Learning objectives**
> Separate assigned, written, visible, and replied work; synchronize a group across rotation; implement timed two-lane service without holding a queue lock during I/O; carry owned commands and replies across threads; stop after uncertain storage failure; and join every owner through an observable result path.

This part supplies the synchronization seam and mailbox. [Part 2 — Owned commands and synchronized publication](https://25d-228.github.io/rust-learning/chapter-09-part-02.html) implements the owner. [Part 3 — Controlled failures and four-partition checks](https://25d-228.github.io/rust-learning/chapter-09-part-03.html) supplies the complete integration tests and driver. Finish all parts before building the new targets: the module root below already names `worker.rs`.

The complete nine-file reference overlay is under `book/media/chapter-09/`. It replaces `src/lib.rs`, `src/storage/{mod,segment,log}.rs` and adds `src/storage/sync.rs`, `src/mailbox.rs`, `src/worker.rs`, `tests/group_commit.rs`, and `examples/owner_probe.rs`, all beneath `crates/broker-core/`. Apply it to the prerequisite workspace, not inside a media directory. Every new file and the complete replacement `log.rs` appear below or in the following parts. For the existing `segment.rs`, this part gives its only two edits precisely; its other routines remain the Chapter 8 prerequisite. Repeated filename-labelled listings concatenate in order, except those two explicitly labelled insertions. Do not insert extra braces between split `impl` blocks.

## Four facts that a single offset cannot represent

Use exclusive ends: an end of 3 describes offsets 0, 1, and 2. Let $A$ be the assigned end, $W$ the fully written end, and $V$ the synchronized visible end. During ordinary operation:

$$
0 \le V \le W \le A.
$$

An accepted mailbox command has no assigned offset yet. After preflight succeeds, the owner reserves the next offset in its local state, increasing $A$. A complete append increases $W$. Finishing all synchronization needed by a group increases $V$. Successful replies may then be placed into their individual reply channels. Receiving a ticket from admission is not receiving that success.

For an empty partition, followed by `cat` and `dog` in a two-record group, the intended trace is:

| Point | Assigned end | Written end | Visible end | Successful publish replies permitted |
| --- | ---: | ---: | ---: | --- |
| Both commands only queued | 0 | 0 | 0 | None |
| Assign `cat`, before writing | 1 | 0 | 0 | None |
| Complete `cat` write | 1 | 1 | 0 | None |
| Complete `dog` write | 2 | 2 | 0 | None |
| Synchronization is blocked | 2 | 2 | 0 | None |
| Every required sync succeeds | 2 | 2 | 2 | Offsets 0 and 1 |

Because writing is synchronous on this owner, a snapshot command normally observes $A=W$ between commands. A failed append can leave $A>W$ in the joined failure report. That owner stops rather than guessing whether partial bytes justify reusing its assigned offset.

There is deliberately no scalar `replied_end`. A producer may drop the receiver for offset 0 while another receives offset 1. Let $R$ denote the set of offsets for which a success was placed in a reply channel. The required condition is:

$$
R \subseteq \{o \mid 0 \le o < V\}.
$$

This is not a claim that each producer observed its reply. A reply can be lost after the synchronized append. Retrying such an uncertain request may create a second record; this chapter does not implement producer deduplication or an atomic transaction spanning external effects.

## Keep the log owned, not shared behind another mutex

There are four owner threads, one for each `PartitionId`. Each receives a whole `PartitionLog` by move. No client receives a reference to that log, its file cursor, its pending group, or its visible boundary. Clients share only a mailbox handle. A fetched `ReadBatch` owns its bytes and can move back to the caller after the owner finishes reading.

The directory guard remains inside the log as an `Arc<DirectoryGuard>`. Sharing that guard keeps the process's directory lock alive; it does not share mutable segment cursors. Chapter 8's absolute seeks and strictly owned cursors remain necessary even with one thread per partition.

Replace the crate root and storage module root with these complete files:

```rust title="crates/broker-core/src/lib.rs"
pub mod codec;
pub mod error;
pub mod io;
pub mod mailbox;
pub mod storage;
pub mod types;
pub mod worker;
```

```rust title="crates/broker-core/src/storage/mod.rs"
pub mod directory;
pub mod error;
pub mod index;
pub mod log;
pub mod segment;
pub mod sync;
```

## Make synchronization replaceable without replacing the storage engine

Chapter 7's `StorageFile::sync` already calls `File::sync_all` for real files. The official [file synchronization documentation](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all) describes a fallible request to synchronize file data and metadata. A successful `write`, a file read-back, or dropping a handle is not a substitute for checking that result.

The test seam below intercepts that operation while still receiving the actual file owned by the segment. A normal implementation forwards to `StorageFile::sync`; a test can stop at the boundary, release it, or return an error. The test does not supply a fake log, codec, index, or worker.

Create `src/storage/sync.rs`:

```rust title="crates/broker-core/src/storage/sync.rs"
use std::io;

use crate::io::StorageFile;
use crate::types::Offset;

// Implementations must either synchronize this file or report failure.
// The base identifies the file for fault injection; it is not a byte position.
pub trait SyncIo {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()>;
}

pub struct FileSync;

impl SyncIo for FileSync {
    fn sync(&mut self, _base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()
    }
}
```

`&mut dyn StorageFile` is a trait-object borrow for the duration of one call. The adapter cannot retain that borrow in this interface. `base` labels the segment, allowing a test to distinguish files beginning at offsets 0 and 2. It is not a position to seek to.

This is a trusted implementation boundary. An adapter that returns `Ok(())` without synchronizing violates its contract; the Rust type system cannot prove that an operating-system action occurred. `FileSync` forwards the existing operation rather than implementing a different durability policy. The fault adapter in Part 3 forwards real synchronization whenever it is told to proceed.

In the existing `src/storage/segment.rs`, add this import next to `SparseIndex`:

```rust title="crates/broker-core/src/storage/segment.rs — add import"
use super::sync::SyncIo;
```

Then insert the following complete method inside `impl<F: StorageFile> Segment<F>`, immediately after `into_file` and before `append`. Do not change the codec, append, index, or read routines.

```rust title="crates/broker-core/src/storage/segment.rs — insert after into_file"
    pub(crate) fn synchronize<S: SyncIo + ?Sized>(&mut self, io: &mut S)
        -> Result<(), StorageError>
    {
        if self.poisoned { return Err(StorageError::Poisoned); }
        self.poisoned = true;
        io.sync(self.header.base, &mut self.file)
            .map_err(|e| StorageError::io("synchronize segment", self.bytes, e))?;
        self.poisoned = false;
        Ok(())
    }
```

The method marks this segment poisoned before asking the adapter to act. The `?` on an error exits with that flag still set. Even when an injected failure happens before the operating-system call, the production contract must treat an arbitrary synchronization failure as uncertain, not as a safe rollback. A successful call clears the flag. We use the known byte end as diagnostic context, not as evidence that a sync covered only that byte.

`SyncIo + ?Sized` allows a concrete adapter or a borrowed trait object; it does not require the adapter to be shared among owners. The worker later requires only `Send` because it moves one adapter into one thread.

## Rotation makes a group larger than its active file

Take three-byte payloads, so each encoded record occupies $32+3=35$ bytes. With a 102-byte segment cap, the 32-byte segment header leaves 70 bytes: exactly two such records. A five-record group therefore occupies these files:

| Segment base | Record offsets | Complete file bytes | Needs synchronization? |
| ---: | --- | ---: | --- |
| 0 | 0, 1 | $32+2(35)=102$ | Yes |
| 2 | 2, 3 | $32+2(35)=102$ | Yes |
| 4 | 4 | $32+35=67$ | Yes |

Syncing only base 4 would let the owner reply for records in bases 0 and 2 without completing their required sync operations. We instead keep one dirty flag for every segment. A new header is dirty; an append makes its target dirty; strict reopening initially marks every file dirty because validation alone did not synchronize it. A successful synchronization clears only that file's flag.

The flags are in-memory bookkeeping, not authoritative recovery metadata. If the process restarts, it must inspect files again. The current log retains at most the configured number of segments, so the flag array is bounded by the existing 64-segment configuration maximum.

Replace `src/storage/log.rs` with the following complete file in the displayed order. The discovery routine is unchanged from Chapter 8: it accepts canonical segment filenames, rejects unexpected entries, bounds the number collected, and sorts by base offset.

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

The log stores matching `segments` and `dirty` vectors. Their lengths change together only when constructing or rotating. The guard is still declared after the segment handles, preserving the existing destruction order.

```rust title="crates/broker-core/src/storage/log.rs"
pub struct PartitionLog {
    // Declaration order closes segment handles before dropping this log's guard share.
    segments: Vec<Segment<File>>,
    dirty: Vec<bool>,
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
        Ok(Self { segments: vec![segment], dirty: vec![true], root, partition, config,
            poisoned: false, _directory: directory })
    }

```

Strict reopening still refuses incomplete or corrupt files and never repairs them. Its additional responsibility is to start with no assumed synchronization history:

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
        Ok(Self { segments, dirty, root, partition, config, poisoned: false, _directory: directory })
    }

```

The new `partition()` getter gives the thread a diagnostic name. Rotation continues to create before installing a new segment; its new dirty flag covers that segment's header even when it contains no records yet.

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
        self.poisoned = false;
        Ok(())
    }

```

Preflight must happen before the worker advances its assigned end. Extract the previous size, successor, and segment-limit checks into `check_append`. It checks whether rotation would exceed the segment count without attempting to create anything. This is a crate-visible decision used by the owner, not a public promise that a future unrelated append will fit.

The same owner then calls `append`; no other thread can insert a record between the check and the mutation. Rechecking inside `append` keeps the standalone log API safe for Chapter 8's examples as well.

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

Marking the dirty flag before the write is conservative. A failed write may leave a prefix in the file, but a poisoned log cannot advertise a new visible end or retry the uncertain append. A preflight refusal does not set the poison flag and can leave earlier pending records eligible to commit.

Now synchronize every dirty segment in file order:

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
        self.poisoned = false;
        Ok(self.written_end())
    }

```

`zip` pairs each segment with its flag; both vectors are private and constructed with identical lengths. A failure leaves the log poisoned and prevents the method from returning a new end. Some earlier files may already have synchronized, but the worker has not published any part of this group yet. Clearing those earlier flags is harmless: the poisoned owner cannot resume normal service.

With no dirty files, the method returns the current written end without issuing a file operation. Initial owner construction invokes it even when there is no producer group, so an empty first header or a complete strictly reopened prefix is synchronized before a client is returned.

The final read method is unchanged. Its explicit `upper` argument is the mechanism that lets the owner hide written-but-unsynchronized records; it is not safe to substitute `written_end()` for the worker's visible end.

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

## State the durability boundary narrowly

This code performs file synchronization and process-local publication. It does not implement parent-directory synchronization, atomic metadata replacement, first-creation completion markers, or restart epochs. It therefore makes no claim about arbitrary power loss or a half-created data directory being a valid broker. Chapter 10 adds recovery and metadata publication before a restartable service is presented.

The failure experiment in this chapter is particularly important: one file can have synchronized before the next file fails, yet **no member of that group receives a successful reply**. That is an all-or-no-success visibility decision in the running process, not an all-or-nothing disk transaction. A later recovery may retain complete records whose producer never received success.

## Two bounded lanes, one receiver

Publish commands use the data lane. Snapshots, bounded fetches, and explicit flushes use the control lane. Giving each its own item cap means a full data lane does not consume the control lane's slots. It does not guarantee a free control slot when control traffic itself is excessive.

Unlike Chapter 5's reservation exercise, this API accepts an already-owned command through a nonblocking `try_*` call. It either enqueues that exact owner or returns it unchanged. There is no separate preallocation reservation here. The final network admission and byte permits arrive in Chapter 15; this small in-process interface does not yet bound arbitrary caller allocation or total retained replies.

Create `src/mailbox.rs`:

```rust title="crates/broker-core/src/mailbox.rs"
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError { Full, Closed }

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

```

`Event<D, C>` keeps the command types distinct without a cast. `Rejected<T>` carries the rejected value back to the sender, so overload cannot silently destroy the only owned payload. The queue mutex protects both lanes, closure, and the next lane preference. It does not protect disk state; disk state belongs to the owner thread.

The two `#[cfg(test)]` fields are compiled only into the library's unit-test build. They let a test observe that `recv` actually reached its condition-variable wait, rather than infer that fact from a sleep.

```rust title="crates/broker-core/src/mailbox.rs"
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

```

[`VecDeque::with_capacity`](https://doc.rust-lang.org/std/collections/struct.VecDeque.html#method.with_capacity) prepares storage, but the chosen cap is our explicit length check, not the allocator's returned capacity. The maximum of 4096 slots per lane is a local configuration limit, not a record-byte budget. Both queues start open and empty, with control preferred when both have work.

```rust title="crates/broker-core/src/mailbox.rs"
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

```

The admission decision and `push_back` occur under the same mutex. Once `close` wins that mutex, later admissions return `Closed`, even if a slot is available. If an enqueue wins first, it is accepted work that graceful closure must drain.

Only the single receiver waits for these notifications. Producers never wait for a slot in this version, so `notify_one` is sufficient for enqueue; closure uses `notify_all` to express the terminal transition. Notifications do not represent stored work—the queues do. The [condition-variable contract](https://doc.rust-lang.org/std/sync/struct.Condvar.html#method.wait_timeout) requires rechecking state after every wakeup, including spurious ones.

## An expired timer must win against a busy queue

Let $d$ be the absolute deadline of the current group and $t$ the current `Instant`. The receiver waits for at most the remaining duration $\max(0,d-t)$, then checks predicates again. It must not restart a full delay after every notification.

There is a second trap: checking the timer only when both queues are empty permits endless traffic to postpone synchronization forever. Our receiver checks expiry before taking either lane. Between nonexpired receives it alternates lanes when both are nonempty.

```rust title="crates/broker-core/src/mailbox.rs"
    // Exactly one owner calls recv. A deadline is absolute, not a fresh delay per wakeup.
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

```

After a control command, `prefer_control` is false; after data it is true. Therefore a continuously nonempty opposing lane gets a turn after at most one command from the other lane, excluding deadline events and time already spent inside a command. There is no wall-clock fairness guarantee: a file sync or bounded read can still take a long time.

The timer uses [`Instant`](https://doc.rust-lang.org/std/time/struct.Instant.html), not a wall-clock timestamp. A timed wait is a scheduling mechanism, not a precise latency guarantee. Once the mutex is reacquired, the loop recomputes the remaining duration from the same deadline. A wakeup is never treated as proof that the deadline expired or that a command arrived.

If both lanes are empty and admission is closed, `Closed` ends draining. A nonempty closed mailbox is still serviced. The owner releases the queue mutex when `recv` returns, before it calls any file or reply operation.

```rust title="crates/broker-core/src/mailbox.rs"
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

`close` preserves accepted work. `abort` is different: it closes admission and moves all queued values out for terminal failure handling. `mem::take` leaves empty queues behind. The returned values are processed or destroyed **after** the mutex guard has been released, so their destructors do not run while the queue lock is held.

Normal operations use `expect` on a [poisoned mailbox mutex](https://doc.rust-lang.org/std/sync/struct.Mutex.html#poisoning) rather than continue with possibly inconsistent state. Only these terminal cleanup methods extract a poisoned guard to set closure and take ownership of remaining values. That is not a general recovery from mutex poisoning and does not authorize another healthy owner to reuse these queues.

## Observe waiting and lane order directly

Append this unit-test module to the same file. `observe_wait` acquires the same mutex as `recv`. Seeing `waiting=true` after acquiring it proves that the receiver released the lock into its wait path; the helper's deadline is a test watchdog, not a synchronization assumption.

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

```

The first two tests establish independent admission limits, preserved rejected ownership, per-lane FIFO, and recurring control service. They do not assert a total order between lanes.

```rust title="crates/broker-core/src/mailbox.rs"
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

```

The next pair separates timer priority from actual timed waiting. An already-expired deadline must win even with both queues nonempty. An empty mailbox must eventually return `Deadline` without anyone notifying it.

```rust title="crates/broker-core/src/mailbox.rs"
    #[test]
    fn expired_deadline_precedes_ready_work() {
        let q = Mailbox::<u8, u8>::new(1, 1).unwrap();
        q.try_data(1).unwrap(); q.try_control(2).unwrap();
        assert!(matches!(q.recv(Some(Instant::now())), Event::Deadline));
        assert!(matches!(q.recv(None), Event::Control(2)));
        assert!(matches!(q.recv(None), Event::Data(1)));
    }

    #[test]
    fn timed_empty_wait_reaches_its_deadline() {
        let q = Mailbox::<u8, u8>::new(1, 1).unwrap();
        let end = Instant::now() + Duration::from_millis(5);
        assert!(matches!(q.recv(Some(end)), Event::Deadline));
        assert!(Instant::now() >= end);
    }

```

Now test an observed waiter rather than a thread that merely announced that it was about to call `recv`. Both tests have a five-second watchdog so a missing notification is diagnosed instead of hanging indefinitely. They assume no particular submillisecond scheduling latency.

```rust title="crates/broker-core/src/mailbox.rs"
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

```

The final tests distinguish draining closure from abort and reject invalid capacities:

```rust title="crates/broker-core/src/mailbox.rs"
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

## Predict the order before writing the owner

Suppose data command `D0` is queued, then control command `C0`, before the receiver takes either. The initial preference returns `C0` first, then `D0`. The two enqueue times do not make `C0` a fence after `D0`. This is why the next part's explicit flush is defined over already-processed records, not over everything previously admitted to the other lane.

For a useful variation, temporarily move the deadline check below the data dequeue. The `expired_deadline_precedes_ready_work` test should fail even though ordinary idle tests can still pass. Restore the original order before continuing. Do not turn this experiment into a different public contract.

Continue with [Part 2 — Owned commands and synchronized publication](https://25d-228.github.io/rust-learning/chapter-09-part-02.html). The runnable commands and whole-chapter checks are in Part 3; the current module root deliberately requires the owner file that Part 2 supplies.
