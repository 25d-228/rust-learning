# Chapter 15 — Byte budgets and pressure-safe admission
## Part 2 — Bounded storage copies and owner integration

**Implementation time:** shared with Part 1's 5.0-hour total. **Prerequisite:** Part 1's pools and mailbox, plus Chapter 14's exact record, lease, checkpoint, and retention behavior. [Part 3](https://25d-228.github.io/rust-learning/chapter-15-part-03.html) connects these routines to asynchronous callers.

A byte-accounting wrapper around the previous fetch routine would be insufficient. That routine allocates an `OwnedRecord` for each result, and append allocates a complete encoded vector. Reserving a response buffer and then calling those routines would temporarily own both sets of bytes. This part supplies a serving path that never makes those extra payload allocations.

The older routines remain useful reference implementations. Bounded owners use the new path exclusively; raw owners retain the old one. Neither path changes the stored record format.

## Encode the header without allocating an encoded vector

Append the following function to `src/codec.rs`. It is the same field layout and CRC calculation as the existing encoder: validate the offset successor and payload size, write offset and length, hash the payload, then seal the fixed header. The return value is a 32-byte array; the owner supplies storage for the body.

```rust title="crates/broker-core/src/codec.rs"

// The bounded owner supplies storage for the encoded body; this helper allocates none.
pub(crate) fn encode_record_header(offset: Offset, payload: &[u8])
    -> Result<[u8; HEADER_LEN], FormatError>
{
    offset.next().map_err(FormatError::Identity)?;
    record_len(payload.len())?;
    let length = u32::try_from(payload.len()).map_err(|_| FormatError::LengthOverflow)?;
    let mut h = start_header(RECORD_MAGIC);
    h[8..16].copy_from_slice(&offset.get().to_be_bytes());
    h[16..20].copy_from_slice(&length.to_be_bytes());
    h[20..24].copy_from_slice(&crc32fast::hash(payload).to_be_bytes());
    seal_header(&mut h);
    Ok(h)
}
```

Append these declarations to `src/storage/segment.rs`. A child module may implement methods for its parent's type and access the parent's private fields; it does not make those fields public. `RecordCopy` is exposed only within the crate. The complete new file follows.

```rust title="crates/broker-core/src/storage/segment.rs"

mod pooled;
pub(crate) use pooled::RecordCopy;
```

## One scratch buffer for the one serial owner

Create `src/storage/segment/pooled.rs`. `append_using` validates capacity before changing storage, copies the header and payload into the supplied scratch prefix, then follows the earlier seek/write/index/end sequence. At the copy point, the append pool owns the source and the fixed scratch owns the encoded copy. Both were included in the memory envelope.

The scratch slice is borrowed for this call only. It cannot escape into a reply. No other operation runs concurrently on this owner, so append encoding and skipped-record validation can reuse the same storage. The filesystem's buffering is not represented by this Rust slice and is not a third application-owned payload vector.

```rust title="crates/broker-core/src/storage/segment/pooled.rs"
use super::*;

#[derive(Debug, Clone, Copy)]
pub(crate) enum RecordCopy {
    Copied { bytes: usize, scanned: usize },
    TooSmall { required: usize, scanned: usize },
}

impl<F: StorageFile> Segment<F> {
    pub(crate) fn append_using(&mut self, payload: &[u8], scratch: &mut [u8])
        -> Result<Offset, StorageError>
    {
        if self.poisoned { return Err(StorageError::Poisoned); }
        let size = self.config.check_record(payload.len())?;
        let offset = self.end;
        let next = next_offset(offset, self.bytes)?;
        if size as u64 > self.config.segment_bytes - self.bytes { return Err(StorageError::SegmentFull); }
        if size > scratch.len() { return Err(StorageError::Config("owner scratch cannot hold a record")); }
        let header = codec::encode_record_header(offset, payload)
            .map_err(|e| StorageError::format(self.bytes, e))?;
        scratch[..HEADER_LEN].copy_from_slice(&header);
        scratch[HEADER_LEN..size].copy_from_slice(payload);
        self.poisoned = true;
        self.file.seek(SeekFrom::Start(self.bytes))
            .map_err(|e| StorageError::io("seek bounded append", self.bytes, e))?;
        write_fully(&mut self.file, &scratch[..size])
            .map_err(|e| StorageError::io("append bounded record", self.bytes, e))?;
        self.index.record(offset, self.bytes);
        self.bytes += size as u64;
        self.end = next;
        self.poisoned = false;
        Ok(offset)
    }
```

## Copy one selected record, validating the indexed prefix

A sparse index points to a predecessor, not necessarily the requested record. Starting at the predecessor, the loop validates every skipped record in scratch. The selected record goes directly into the response slice. Let $u$ be bytes already occupied in the response and $L$ its requested encoded-byte limit. The destination length is $L-u$. A header declaring $r$ bytes fits only when

$$
r\le L-u.
$$

For `cat`, $r=35$. With 34 remaining bytes, return `TooSmall { required: 35 }` after validating the header, without reading a body into an undersized slice and without spending a lease attempt. With 35 remaining bytes, fill exactly that prefix, validate its payload checksum, and report one copied record.

The explicit reborrows `&mut *output` and `&mut *scratch` select a temporary mutable slice without moving the original mutable-reference binding out of the loop. A failed read leaves the outer log poisoned; no partially filled batch is returned as success.

```rust title="crates/broker-core/src/storage/segment/pooled.rs"
    pub(crate) fn copy_record(&mut self, target: Offset, output: &mut [u8], scratch: &mut [u8])
        -> Result<RecordCopy, StorageError>
    {
        if self.poisoned { return Err(StorageError::Poisoned); }
        let anchor = self.index.predecessor(target)
            .ok_or(StorageError::Topology("bounded read has no index predecessor"))?;
        let mut position = anchor.position;
        let mut expected = anchor.offset;
        let mut scanned = 0;
        while expected <= target && expected < self.end {
            let (raw, header) = read_header(&mut self.file, position, self.bytes, expected)?;
            let size = header.total_len();
            if expected == target && size > output.len() {
                return Ok(RecordCopy::TooSmall { required: size, scanned });
            }
            let destination = if expected == target { &mut *output } else { &mut *scratch };
            if size > destination.len() {
                return Err(StorageError::Config("owner scratch cannot validate a skipped record"));
            }
            destination[..HEADER_LEN].copy_from_slice(&raw);
            let count = read_prefix(&mut self.file, &mut destination[HEADER_LEN..size])
                .map_err(|e| StorageError::io("copy bounded payload", position + HEADER_LEN as u64, e))?;
            if count != header.payload_len() {
                return Err(StorageError::Incomplete { position, required: size as u64,
                    available: (HEADER_LEN + count) as u64 });
            }
            match codec::decode_record(&destination[..size]).map_err(|e| StorageError::format(position, e))? {
                Probe::Complete { .. } => {},
                Probe::Incomplete { .. } => return Err(StorageError::Topology("filled bounded read is incomplete")),
            }
            scanned += 1;
            if expected == target { return Ok(RecordCopy::Copied { bytes: size, scanned }); }
            position += size as u64;
            expected = next_offset(expected, position)?;
        }
        Err(StorageError::Topology("bounded read ended before the requested record"))
    }
}
```

## Preserve the disk reservation and retained interval

Append the child declaration to `src/storage/log.rs`, then create `src/storage/log/pooled.rs` below. `append_pooled` requires an initialized/recovered log's own disk permit. It settles actual file growth only after successful append. Failed writes keep the existing conservative stop/recovery rule.

`copy_record` checks the retained interval before selecting a segment. It borrows response/scratch slices and returns only counts, not an allocation. A nonfitting record is a healthy result. An unexpected storage failure is not.

```rust title="crates/broker-core/src/storage/log.rs"

mod pooled;
```

```rust title="crates/broker-core/src/storage/log/pooled.rs"
use super::*;
use crate::storage::segment::RecordCopy;

impl PartitionLog {
    pub(crate) fn append_pooled(&mut self, payload: &[u8], permit: Option<DiskPermit>, scratch: &mut [u8])
        -> Result<Offset, StorageError>
    {
        let size = self.check_append(payload.len())?;
        match (&self.disk, &permit) {
            (Some(disk), Some(permit)) if permit.belongs_to(disk) => {},
            _ => return Err(StorageError::Config("bounded append requires its disk reservation")),
        }
        let before = self.live_bytes();
        let result = (|| {
            let tail = self.segments.last().expect("log has a segment").info();
            if size as u64 > self.config.segment_bytes() - tail.bytes { self.rotate_file(false)?; }
            self.poisoned = true;
            let last = self.segments.len() - 1;
            self.dirty[last] = true;
            self.segments[last].append_using(payload, scratch)
        })();
        match result {
            Ok(offset) => {
                permit.expect("validated disk permit").settle(self.live_bytes() - before);
                self.poisoned = false;
                Ok(offset)
            }
            Err(error) => { self.stop_admission(); Err(error) }
        }
    }
```

The selected record's exclusive successor must be inside the written interval. Visibility is a separate restriction enforced by the lease planner: it plans only offsets below the owner's synchronized-visible end. A retained floor is not an invitation to renumber offsets. The same original offset identifies the record before and after old files are deleted.

```rust title="crates/broker-core/src/storage/log/pooled.rs"
    pub(crate) fn copy_record(&mut self, target: Offset, output: &mut [u8], scratch: &mut [u8])
        -> Result<RecordCopy, StorageError>
    {
        self.healthy()?;
        let floor = self.retained_floor();
        let upper = target.next().map_err(|e| StorageError::format(0, crate::error::FormatError::Identity(e)))?;
        if target < floor || upper > self.written_end() {
            return Err(StorageError::Range { start: target, upper, floor, end: self.written_end() });
        }
        let right = self.segments.partition_point(|segment| segment.info().base <= target);
        let index = right.checked_sub(1).ok_or(StorageError::Topology("no bounded-read segment"))?;
        self.poisoned = true;
        let result = self.segments[index].copy_record(target, output, scratch);
        if result.is_ok() { self.poisoned = false; }
        result
    }
}
```

## Return borrowed record views backed by an owned batch

Append the following declarations to `src/delivery/leased_groups.rs` and `src/delivery/mod.rs`, respectively. The older `LeaseBatch` is retained for raw regression paths; the bounded interface returns `PooledLeaseBatch`.

```rust title="crates/broker-core/src/delivery/leased_groups.rs"

mod pooled;
pub use pooled::{PooledLeaseBatch, PooledRecord};
```

```rust title="crates/broker-core/src/delivery/mod.rs"
pub use leased_groups::{PooledLeaseBatch, PooledRecord};
```

Create `src/delivery/leased_groups/pooled.rs`. An entry stores a lease token and a `Range<usize>` into the shared response allocation. Cloning a range copies two indices, not bytes. `records()` returns an iterator whose lifetime is tied to `&self`; each `PooledRecord` contains a borrowed encoded slice. Its explicit lifetime on `payload()` preserves that same relationship.

The batch's payload owner and entries are private. Public access returns views and copied scalar metadata. There is no consuming iterator that moves payload allocations out and drops their accounting early. A caller may deliberately copy a view into unrelated memory, but that copy belongs to the application and lies outside the core's bound.

```rust title="crates/broker-core/src/delivery/leased_groups/pooled.rs"
use std::ops::Range;

use super::*;
use crate::budget::memory::PoolBuffer;
use crate::codec::HEADER_LEN;
use crate::storage::segment::RecordCopy;

#[derive(Debug)]
struct Entry { token: LeaseToken, range: Range<usize> }

#[derive(Debug)]
pub struct PooledLeaseBatch {
    buffer: PoolBuffer,
    entries: Vec<Entry>,
    pub state: GroupState,
    pub encoded_bytes: usize,
    pub scanned_records: usize,
    pub stop: LeaseStop,
}
#[derive(Debug, Clone, Copy)]
pub struct PooledRecord<'a> {
    pub token: LeaseToken,
    pub encoded: &'a [u8],
}
impl<'a> PooledRecord<'a> {
    pub fn payload(&self) -> &'a [u8] { &self.encoded[HEADER_LEN..] }
}
impl PooledLeaseBatch {
    pub fn records(&self) -> impl ExactSizeIterator<Item = PooledRecord<'_>> + '_ {
        self.entries.iter().map(|entry| PooledRecord {
            token: entry.token, encoded: &self.buffer.as_slice()[entry.range.clone()],
        })
    }
}
```

## Install attempts only after copying succeeds

`deliver_pooled` reuses the established selection/install split. It allocates bounded **metadata** for ranges and entries before token installation. Every selected body goes into the already reserved response prefix; skipped bodies use scratch. There is no `OwnedRecord`, `to_vec`, or collected payload vector in this path.

Only the copied prefix of the plan receives tokens. Sampling the lease clock after copying keeps the broker's own read time out of the lease duration. A read failure, metadata reservation error, or second clock error returns the buffer through ordinary ownership cleanup without installing a partial attempt set. The earlier planner and token installer still bound metadata by the configured delivery window and at most 1,024 requested records.

```rust title="crates/broker-core/src/delivery/leased_groups/pooled.rs"
impl LeasedGroups {
    pub(crate) fn deliver_pooled(&mut self, log: &mut PartitionLog, visible: Offset,
        group: GroupId, limits: ReadLimits, mut buffer: PoolBuffer, scratch: &mut [u8])
        -> Result<PooledLeaseBatch, LeaseReadError>
    {
        if log.partition() != self.partition || limits.bytes() > buffer.as_slice().len() {
            return Err(StorageError::Config("bounded delivery has the wrong scope or output capacity").into());
        }
        let selected_at = self.sample()?;
        let index = usize::from(group.get());
        let plan = self.windows[index].plan(visible, limits.records(), selected_at)?;
        let mut stop = if plan.len() == limits.records() { LeaseStop::RecordLimit } else { LeaseStop::NoEligible };
        let mut ranges = Vec::new();
        ranges.try_reserve_exact(plan.len()).map_err(|source| StorageError::Allocation {
            operation: "bounded delivery ranges", source,
        })?;
        // Allocate result metadata before installing any token. Payload bytes are already reserved.
        let mut entries = Vec::new();
        entries.try_reserve_exact(plan.len()).map_err(|source| StorageError::Allocation {
            operation: "bounded delivery entries", source,
        })?;
        let mut encoded_bytes = 0;
        let mut scanned_records = 0;
        for selected in &plan {
            let result = log.copy_record(selected.offset,
                &mut buffer.as_mut_slice()[encoded_bytes..limits.bytes()], scratch)?;
            match result {
                RecordCopy::TooSmall { required, scanned } => {
                    scanned_records += scanned; stop = LeaseStop::ByteLimit { required }; break;
                }
                RecordCopy::Copied { bytes, scanned } => {
                    scanned_records += scanned;
                    ranges.push(encoded_bytes..encoded_bytes + bytes);
                    encoded_bytes += bytes;
                }
            }
        }
        let tokens = if ranges.is_empty() { Vec::new() } else {
            let issued_at = self.sample()?;
            self.windows[index].install(&plan[..ranges.len()], visible, issued_at)?
        };
        assert_eq!(ranges.len(), tokens.len());
        for (range, token) in ranges.into_iter().zip(tokens) { entries.push(Entry { token, range }); }
        buffer.set_len(encoded_bytes);
        Ok(PooledLeaseBatch { buffer, entries, state: self.state(group), encoded_bytes,
            scanned_records, stop })
    }
}
```

## The owner has two reply transports, not two semantics

Replace `src/worker.rs` with the following complete chunks. Most state-machine behavior is retained from Chapter 14. Its inclusion keeps the new command variants and cleanup exhaustive, instead of leaving the reader to guess which match arms must change.

The new imports expose only the internal pool owner and the public pooled result. Batch configuration, visibility, and errors retain their old meanings. A failed request remains distinct from a preflight refusal; memory accounting does not make accepted storage effects reversible.

```rust title="crates/broker-core/src/worker.rs"
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::budget::memory::PoolBuffer;
use crate::delivery::PooledLeaseBatch;

use crate::budget::{DiskBudget, DiskPermit, DiskRefusal, DiskState};
use crate::codec::{self, HEADER_LEN};
use crate::delivery::{Ack, DeliveryBatch, DeliveryConfig, GroupCompletion, GroupState, Groups,
    LeaseBatch, LeaseConfig, LeaseError, LeaseReadError, LeaseToken, LeasedGroups};
use crate::delivery::clock::Clock;
use crate::delivery::progress::{Checkpointed, CheckpointPolicy, Progress, ProgressAck, ProgressState};
use crate::delivery::window::WindowError;
use crate::mailbox::{AdmissionError, Event, Mailbox, Rejected};
use crate::storage::error::StorageError;
use crate::storage::log::PartitionLog;
use crate::storage::retention::Retained;
use crate::storage::segment::{ReadBatch, ReadLimits};
use crate::storage::sync::SyncIo;
use crate::types::{GROUP_COUNT, GroupId, Offset};

#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    records: usize,
    bytes: usize,
    delay: Duration,
}

impl BatchConfig {
    pub fn new(records: usize, bytes: usize, delay: Duration) -> Result<Self, StorageError> {
        if !(1..=1024).contains(&records) || !(HEADER_LEN..=4 * 1024 * 1024).contains(&bytes)
            || delay.is_zero() || delay > Duration::from_secs(60)
        {
            return Err(StorageError::Config("batch: 1..=1024 records, 32..=4194304 bytes, delay in (0, 60s]"));
        }
        Ok(Self { records, bytes, delay })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerState {
    pub assigned_end: Offset,
    pub written_end: Offset,
    pub visible_end: Offset,
    pub pending_records: usize,
    pub pending_bytes: usize,
    pub sync_groups: u64,
}

#[derive(Debug, Clone)]
pub enum RequestError {
    // Refused by the owner before assigning this request an offset.
    Refused(Arc<StorageError>),
    // Accepted work may have reached storage. This is NOT rollback.
    Failed(Arc<StorageError>),
    // Includes ordinary unwinding; the caller cannot infer a disk outcome.
    OwnerStopped,
    // A trusted completion request did not name an issued position.
    Delivery(WindowError),
    Lease(LeaseError),
    DeliveryDisabled,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(_) => write!(f, "request refused before mutation"),
            Self::Failed(_) => write!(f, "partition failed; accepted work has an uncertain outcome"),
            Self::OwnerStopped => write!(f, "partition stopped without completing this reply"),
            Self::Delivery(error) => write!(f, "delivery request refused: {error}"),
            Self::Lease(error) => write!(f, "lease request refused: {error}"),
            Self::DeliveryDisabled => write!(f, "requested delivery mode is not enabled on this owner"),
        }
    }
}

impl Error for RequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Refused(e) | Self::Failed(e) => Some(e.as_ref()),
            Self::Delivery(error) => Some(error),
            Self::Lease(error) => Some(error),
            _ => None,
        }
    }
}
```

The legacy ticket remains a synchronous receiver. `ReplySender` can also hold a Tokio one-shot sender; sending a one-shot result is synchronous and therefore usable from the existing owner thread ([one-shot channels](https://docs.rs/tokio/latest/tokio/sync/oneshot/index.html)). No blocking-receive task is created per request.

Both transports deliver the same `Result<T, RequestError>`. Taking the optional sender before sending prevents the destructor from sending a second result. On ordinary unwinding, the remaining sender reports `OwnerStopped`. If an asynchronous receiver has been dropped, sending fails and drops the result—including its pooled response buffer. This releases memory, not the lease attempt already installed.

```rust title="crates/broker-core/src/worker.rs"
pub type Ticket<T> = mpsc::Receiver<Result<T, RequestError>>;

enum ReplySender<T> {
    Sync(mpsc::SyncSender<Result<T, RequestError>>),
    Async(tokio::sync::oneshot::Sender<Result<T, RequestError>>),
}
struct Reply<T>(Option<ReplySender<T>>);
impl<T> Reply<T> {
    fn pair() -> (Self, Ticket<T>) {
        let (sender, receiver) = mpsc::sync_channel(1);
        (Self(Some(ReplySender::Sync(sender))), receiver)
    }
    fn async_pair() -> (Self, tokio::sync::oneshot::Receiver<Result<T, RequestError>>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        (Self(Some(ReplySender::Async(sender))), receiver)
    }
    fn send(sender: ReplySender<T>, result: Result<T, RequestError>) {
        match sender {
            ReplySender::Sync(sender) => { let _ = sender.try_send(result); }
            ReplySender::Async(sender) => { let _ = sender.send(result); }
        }
    }
    fn finish(mut self, result: Result<T, RequestError>) {
        if let Some(sender) = self.0.take() { Self::send(sender, result); }
    }
}
impl<T> Drop for Reply<T> {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() { Self::send(sender, Err(RequestError::OwnerStopped)); }
    }
}

enum Payload { Raw(Vec<u8>), Pooled(PoolBuffer) }
impl Payload {
    fn as_slice(&self) -> &[u8] {
        match self { Self::Raw(bytes) => bytes, Self::Pooled(buffer) => buffer.as_slice() }
    }
    fn len(&self) -> usize { self.as_slice().len() }
}
struct Publish {
    payload: Payload,
    reply: Reply<Offset>,
    disk: Option<DiskPermit>,
}
```

`Payload::Raw` preserves prerequisite callers; `Payload::Pooled` carries the new lifetime owner. The owner only asks either form for a borrowed slice. The new `PooledLease` control owns a buffer acquired before enqueue. The exhaustive failure match ensures that terminal failure drops every buffer regardless of its command variant.

```rust title="crates/broker-core/src/worker.rs"
enum Control {
    PooledLease { group: GroupId, limits: ReadLimits, buffer: PoolBuffer, reply: Reply<PooledLeaseBatch> },
    Snapshot(Reply<WorkerState>),
    Fetch { start: Offset, limits: ReadLimits, reply: Reply<ReadBatch> },
    Flush(Reply<WorkerState>),
    GroupState { group: GroupId, reply: Reply<GroupState> },
    Deliver { group: GroupId, limits: ReadLimits, reply: Reply<DeliveryBatch> },
    Complete { group: GroupId, offset: Offset, reply: Reply<GroupCompletion> },
    Lease { group: GroupId, limits: ReadLimits, reply: Reply<LeaseBatch> },
    Acknowledge { group: GroupId, token: LeaseToken, reply: Reply<Ack> },
    AckProgress { group: GroupId, token: LeaseToken, reply: Reply<ProgressAck> },
    Progress { group: GroupId, reply: Reply<ProgressState> },
    Checkpoint { group: GroupId, reply: Reply<Checkpointed> },
    Retain(Reply<Retained>),
}

impl Control {
    fn fail(self, cause: Arc<StorageError>) {
        match self {
            Self::PooledLease { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Snapshot(reply) | Self::Flush(reply) => reply.finish(Err(RequestError::Failed(cause))),
            Self::Fetch { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::GroupState { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Deliver { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Complete { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Lease { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Acknowledge { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::AckProgress { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Progress { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Checkpoint { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Retain(reply) => reply.finish(Err(RequestError::Failed(cause))),
        }
    }
}
```

## Retain the raw API without exposing it through the bounded handle

The following `Client` methods preserve old tests and examples. Its raw append refusal extracts the original `Vec` from the raw payload variant. These methods do not silently gain a memory guarantee; the new public service facade in Part 3 hides this client and admits only prepared buffers.

The old `try_*` methods are shown in full because changing their return types would unnecessarily rewrite the preceding chapters. Their control-lane ordering, trusted-mode restriction, and explicit checkpoint/floor requests remain unchanged.

```rust title="crates/broker-core/src/worker.rs"
#[derive(Clone)]
pub struct Client {
    mailbox: Arc<Mailbox<Publish, Control>>,
    disk: Option<DiskBudget>,
}

impl Client {
    pub fn try_publish(&self, payload: Vec<u8>) -> Result<Ticket<Offset>, Rejected<Vec<u8>>> {
        let disk = if let Some(budget) = &self.disk {
            let encoded = match codec::record_len(payload.len()) {
                Ok(value) => value,
                Err(_) => return Err(Rejected { reason: AdmissionError::InvalidRecord, value: payload }),
            };
            match budget.reserve_append(encoded) {
                Ok(permit) => Some(permit),
                Err(reason) => return Err(Rejected {
                    reason: match reason { DiskRefusal::Full => AdmissionError::DiskFull,
                        DiskRefusal::Stopped => AdmissionError::Closed }, value: payload,
                }),
            }
        } else { None };
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_data(Publish { payload: Payload::Raw(payload), reply, disk }).map_err(|error| {
            let Payload::Raw(value) = error.value.payload else { unreachable!("raw API constructed raw payload") };
            Rejected { reason: error.reason, value }
        })?;
        Ok(ticket)
    }

    pub fn disk_state(&self) -> Option<DiskState> { self.disk.as_ref().map(DiskBudget::state) }

    // No caller-selected floor: the owner reads its two persisted checkpoints.
    pub fn try_retention(&self) -> Result<Ticket<Retained>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Retain(reply)).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    pub fn try_snapshot(&self) -> Result<Ticket<WorkerState>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Snapshot(reply)).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    pub fn try_fetch(&self, start: Offset, limits: ReadLimits) -> Result<Ticket<ReadBatch>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Fetch { start, limits, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }
```

The remaining raw methods retain their earlier small control messages and response types. In particular, the caller never chooses a persisted progress offset or retained floor. Bounded acknowledgement/checkpoint admission will use a separate urgent lane, but the owner will execute these same control variants.

```rust title="crates/broker-core/src/worker.rs"
    pub fn try_group_state(&self, group: GroupId) -> Result<Ticket<GroupState>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::GroupState { group, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    pub fn try_deliver(&self, group: GroupId, limits: ReadLimits)
        -> Result<Ticket<DeliveryBatch>, AdmissionError>
    {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Deliver { group, limits, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    // Available only on the explicitly trusted Chapter 11 mode, never on a leased owner.
    pub fn try_complete(&self, group: GroupId, offset: Offset)
        -> Result<Ticket<GroupCompletion>, AdmissionError>
    {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Complete { group, offset, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    pub fn try_lease(&self, group: GroupId, limits: ReadLimits)
        -> Result<Ticket<LeaseBatch>, AdmissionError>
    {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Lease { group, limits, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    pub fn try_ack(&self, group: GroupId, token: LeaseToken)
        -> Result<Ticket<Ack>, AdmissionError>
    {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Acknowledge { group, token, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    pub fn try_ack_progress(&self, group: GroupId, token: LeaseToken)
        -> Result<Ticket<ProgressAck>, AdmissionError>
    {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::AckProgress { group, token, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    pub fn try_progress(&self, group: GroupId) -> Result<Ticket<ProgressState>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Progress { group, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    // The owner selects its contiguous frontier; the caller supplies NO offset.
    pub fn try_checkpoint(&self, group: GroupId) -> Result<Ticket<Checkpointed>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Checkpoint { group, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }

    // Flushes data ALREADY processed by this owner, not data still in the other lane.
    pub fn try_flush(&self) -> Result<Ticket<WorkerState>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Flush(reply)).map_err(|e| e.reason)?;
        Ok(ticket)
    }
}
```

## Build the delivery mode before starting the thread

The durable mode still consumes the recovered progress seed for the correct startup epoch. The new bounded constructor selects only that mode. The optional scratch field is present only on bounded owners; raw examples continue with their earlier allocation-based routines.

`Pending` contains only offset and reply metadata. It does not retain append payload bytes while awaiting synchronization. Therefore returning an append buffer after the synchronous write does not create a hidden uncharged copy in the pending batch.

```rust title="crates/broker-core/src/worker.rs"
enum DeliverySetup {
    Trusted(DeliveryConfig),
    Leased { config: LeaseConfig, clock: Arc<dyn Clock> },
    Durable { config: LeaseConfig, clock: Arc<dyn Clock>, policy: CheckpointPolicy },
}

enum DeliveryMode {
    Disabled,
    Trusted(Groups),
    Leased(LeasedGroups),
    Durable { groups: LeasedGroups, progress: Progress },
}

impl DeliveryMode {
    fn new(log: &mut PartitionLog, visible: Offset, setup: Option<DeliverySetup>)
        -> Result<Self, StorageError>
    {
        let partition = log.partition();
        if log.retained_floor() != Offset::new(0)
            && matches!(&setup, Some(DeliverySetup::Trusted(_) | DeliverySetup::Leased { .. }))
        {
            return Err(StorageError::Config("delivery after retention requires persisted progress"));
        }
        Ok(match setup {
            None => Self::Disabled,
            Some(DeliverySetup::Trusted(config)) => Self::Trusted(Groups::new(partition, config)),
            Some(DeliverySetup::Leased { config, clock }) => {
                Self::Leased(LeasedGroups::new(partition, config, clock))
            }
            Some(DeliverySetup::Durable { config, clock, policy }) => {
                let (root, starts) = log.take_progress(config.epoch)?;
                let progress = Progress::new(root, config.epoch, partition, starts, visible, policy)?;
                Self::Durable { groups: LeasedGroups::at(partition, config, clock, starts), progress }
            }
        })
    }
}

struct Pending {
    offset: Offset,
    reply: Reply<Offset>,
}

struct Worker<S: SyncIo> {
    log: PartitionLog,
    sync: S,
    mailbox: Arc<Mailbox<Publish, Control>>,
    config: BatchConfig,
    assigned_end: Offset,
    visible_end: Offset,
    pending: Vec<Pending>,
    pending_bytes: usize,
    deadline: Option<Instant>,
    sync_groups: u64,
    delivery: DeliveryMode,
    scratch: Option<Vec<u8>>,
}
```

## Choose the bounded append path before publishing visibility

The worker's commit routine is unchanged: synchronize all dirty files and names, advance visible end, then complete producer replies. The publish routine chooses `append_pooled` when fixed scratch exists and `append_reserved` otherwise. It drops the source payload before storing the small pending reply.

When the owner receives a pooled payload, the allocation remains charged through the complete write call. A successful file write does not require retaining the caller's memory for later synchronization. A failed operation stops the owner; dropping memory owners does not erase a prefix already written.

```rust title="crates/broker-core/src/worker.rs"
impl<S: SyncIo> Worker<S> {
    fn state(&self) -> WorkerState {
        WorkerState {
            assigned_end: self.assigned_end, written_end: self.log.written_end(),
            visible_end: self.visible_end, pending_records: self.pending.len(),
            pending_bytes: self.pending_bytes, sync_groups: self.sync_groups,
        }
    }

    fn commit(&mut self) -> Result<(), Arc<StorageError>> {
        if self.pending.is_empty() { return Ok(()); }
        let end = self.log.synchronize(&mut self.sync).map_err(Arc::new)?;
        // No successful response or visible advance is allowed above the preceding line.
        self.visible_end = end;
        self.sync_groups = self.sync_groups.saturating_add(1);
        self.pending_bytes = 0;
        self.deadline = None;
        for pending in self.pending.drain(..) { pending.reply.finish(Ok(pending.offset)); }
        Ok(())
    }

    fn publish(&mut self, mut command: Publish) -> Result<(), Arc<StorageError>> {
        let size = match self.log.check_append(command.payload.len()) {
            Ok(size) if size <= self.config.bytes => size,
            Ok(_) => {
                command.reply.finish(Err(RequestError::Refused(Arc::new(
                    StorageError::Config("record exceeds batch-byte limit")))));
                return Ok(());
            }
            Err(error) => {
                command.reply.finish(Err(RequestError::Refused(Arc::new(error))));
                return Ok(());
            }
        };
        if size > self.config.bytes - self.pending_bytes {
            if let Err(error) = self.commit() {
                command.reply.finish(Err(RequestError::Failed(Arc::clone(&error))));
                return Err(error);
            }
        }
        if self.pending.is_empty() {
            let Some(deadline) = Instant::now().checked_add(self.config.delay) else {
                command.reply.finish(Err(RequestError::Refused(Arc::new(
                    StorageError::Config("batch deadline cannot be represented")))));
                return Ok(());
            };
            self.deadline = Some(deadline);
        }
        // check_append already checked successor arithmetic; no other thread owns this log.
        self.assigned_end = self.log.written_end().next().expect("preflight checked successor");
        let result = match &mut self.scratch {
            Some(scratch) => self.log.append_pooled(command.payload.as_slice(), command.disk.take(), scratch),
            None => self.log.append_reserved(command.payload.as_slice(), command.disk.take()),
        };
        let offset = match result {
            Ok(offset) => offset,
            Err(error) => {
                let error = Arc::new(error);
                command.reply.finish(Err(RequestError::Failed(Arc::clone(&error))));
                return Err(error);
            }
        };
        drop(command.payload);
        self.pending.push(Pending { offset, reply: command.reply });
        self.pending_bytes += size;
        if self.pending.len() == self.config.records || self.pending_bytes == self.config.bytes {
            self.commit()?;
        }
        Ok(())
    }
```

## Execute a bounded fetch without waiting for capacity

The new first control arm takes the buffer it was given and borrows scratch. It never awaits, checks out another buffer, or waits for a consumer to drop an older reply. Mode errors are clean refusals. Clock/lease errors remain typed, and storage errors preserve the terminal stop behavior.

The subsequent trusted and raw-lease arms are retained as regression paths. They are not reachable through `BoundedClient`. This separation is what makes the serving allocation proof meaningful rather than merely placing a budget next to an unrestricted API.

```rust title="crates/broker-core/src/worker.rs"
    fn control(&mut self, command: Control) -> Result<(), Arc<StorageError>> {
        match command {
            Control::PooledLease { group, limits, buffer, reply } => {
                let DeliveryMode::Durable { groups, .. } = &mut self.delivery else {
                    reply.finish(Err(RequestError::DeliveryDisabled)); return Ok(());
                };
                let Some(scratch) = &mut self.scratch else {
                    reply.finish(Err(RequestError::DeliveryDisabled)); return Ok(());
                };
                match groups.deliver_pooled(&mut self.log, self.visible_end, group, limits, buffer, scratch) {
                    Ok(batch) => reply.finish(Ok(batch)),
                    Err(LeaseReadError::Lease(error)) => reply.finish(Err(RequestError::Lease(error))),
                    Err(LeaseReadError::Storage(error)) => {
                        let error = Arc::new(error);
                        reply.finish(Err(RequestError::Failed(Arc::clone(&error)))); return Err(error);
                    }
                }
            }
            Control::Snapshot(reply) => reply.finish(Ok(self.state())),
            Control::GroupState { group, reply } => {
                let result = match &self.delivery {
                    DeliveryMode::Trusted(groups) => Ok(groups.state(group)),
                    DeliveryMode::Leased(groups) | DeliveryMode::Durable { groups, .. } => Ok(groups.state(group)),
                    DeliveryMode::Disabled => Err(RequestError::DeliveryDisabled),
                };
                reply.finish(result);
            }
            Control::Complete { group, offset, reply } => {
                let result = match &mut self.delivery {
                    DeliveryMode::Trusted(groups) => groups.complete(group, offset).map_err(RequestError::Delivery),
                    _ => Err(RequestError::DeliveryDisabled),
                };
                reply.finish(result);
            }
            Control::Deliver { group, limits, reply } => {
                let DeliveryMode::Trusted(groups) = &mut self.delivery else {
                    reply.finish(Err(RequestError::DeliveryDisabled));
                    return Ok(());
                };
                match groups.deliver(&mut self.log, self.visible_end, group, limits) {
                    Ok(batch) => reply.finish(Ok(batch)),
                    Err(error) => {
                        let error = Arc::new(error);
                        reply.finish(Err(RequestError::Failed(Arc::clone(&error))));
                        return Err(error);
                    }
                }
            }
            Control::Acknowledge { group, token, reply } => {
                let result = match &mut self.delivery {
                    DeliveryMode::Leased(groups) => groups.acknowledge(group, token).map_err(RequestError::Lease),
                    _ => Err(RequestError::DeliveryDisabled),
                };
                reply.finish(result);
            }
            Control::Lease { group, limits, reply } => {
                let groups = match &mut self.delivery {
                    DeliveryMode::Leased(groups) | DeliveryMode::Durable { groups, .. } => groups,
                    _ => {
                        reply.finish(Err(RequestError::DeliveryDisabled));
                        return Ok(());
                    }
                };
                match groups.deliver(&mut self.log, self.visible_end, group, limits) {
                    Ok(batch) => reply.finish(Ok(batch)),
                    Err(LeaseReadError::Lease(error)) => reply.finish(Err(RequestError::Lease(error))),
                    Err(LeaseReadError::Storage(error)) => {
                        let error = Arc::new(error);
                        reply.finish(Err(RequestError::Failed(Arc::clone(&error))));
                        return Err(error);
                    }
                }
            }
```

The next arms preserve volatile-versus-persisted progress, checkpoint publication, and retention based on both persisted fronts. Their input and output are small metadata; no append or response pool is acquired. Retention is therefore allowed to run while every payload buffer is checked out, provided its own control slot is available and storage operations succeed.

```rust title="crates/broker-core/src/worker.rs"
            Control::Progress { group, reply } => {
                let result = match &self.delivery {
                    DeliveryMode::Durable { progress, .. } => Ok(progress.state(group)),
                    _ => Err(RequestError::DeliveryDisabled),
                };
                reply.finish(result);
            }
            Control::AckProgress { group, token, reply } => {
                let DeliveryMode::Durable { groups, progress } = &mut self.delivery else {
                    reply.finish(Err(RequestError::DeliveryDisabled));
                    return Ok(());
                };
                let ack = match groups.acknowledge(group, token) {
                    Ok(ack) => ack,
                    Err(error) => { reply.finish(Err(RequestError::Lease(error))); return Ok(()); }
                };
                match progress.observe(ack, self.visible_end, Instant::now()) {
                    Ok(accepted) => reply.finish(Ok(accepted)),
                    Err(error) => {
                        // The lease window may already have advanced. Do not keep serving.
                        let error = Arc::new(error);
                        reply.finish(Err(RequestError::Failed(Arc::clone(&error))));
                        return Err(error);
                    }
                }
            }
            Control::Checkpoint { group, reply } => {
                let DeliveryMode::Durable { progress, .. } = &mut self.delivery else {
                    reply.finish(Err(RequestError::DeliveryDisabled));
                    return Ok(());
                };
                match progress.checkpoint(group, self.visible_end, &mut self.sync) {
                    Ok(checkpointed) => reply.finish(Ok(checkpointed)),
                    Err(error) => {
                        let error = Arc::new(error);
                        reply.finish(Err(RequestError::Failed(Arc::clone(&error))));
                        return Err(error);
                    }
                }
            }
            Control::Retain(reply) => {
                let DeliveryMode::Durable { progress, .. } = &self.delivery else {
                    reply.finish(Err(RequestError::DeliveryDisabled));
                    return Ok(());
                };
                let ids = [GroupId::new(0).expect("group zero"), GroupId::new(1).expect("group one")];
                let epoch = progress.state(ids[0]).epoch;
                let fronts = ids.map(|g| progress.state(g).persisted_next);
                match self.log.retain(epoch, fronts, &mut self.sync) {
                    Ok(retained) => reply.finish(Ok(retained)),
                    Err(error) => {
                        let error = Arc::new(error);
                        reply.finish(Err(RequestError::Failed(Arc::clone(&error))));
                        return Err(error);
                    }
                }
            }
```

Flush and raw diagnostic fetch retain the earlier semantics. A flush is not a cross-lane barrier. Range refusals do not poison the owner, while unexpected reads do. In the bounded facade, ordinary snapshot/progress queries remain separate from the reserved acknowledgement path.

```rust title="crates/broker-core/src/worker.rs"
            Control::Flush(reply) => {
                if let Err(error) = self.commit() {
                    reply.finish(Err(RequestError::Failed(Arc::clone(&error))));
                    return Err(error);
                }
                reply.finish(Ok(self.state()));
            }
            Control::Fetch { start, limits, reply } => {
                match self.log.read(start, self.visible_end, limits) {
                    Ok(batch) => reply.finish(Ok(batch)),
                    Err(error @ StorageError::Range { .. }) => {
                        reply.finish(Err(RequestError::Refused(Arc::new(error))));
                    }
                    Err(error) => {
                        let error = Arc::new(error);
                        reply.finish(Err(RequestError::Failed(Arc::clone(&error))));
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }
```

## Cleanup must resolve ownership as well as replies

The worker still waits on the earliest producer or progress deadline and drains accepted work on a healthy close. On failure, it closes disk admission, removes queued values, resolves pending replies, and fails controls. Removing a pooled command also returns its buffer. On panic, destructors perform the corresponding ownership cleanup; they cannot undo external effects.

The mailbox abort wakes asynchronous admission waiters through the shared notification. They observe closure on their next iteration even when no payload allocation has been returned yet.

```rust title="crates/broker-core/src/worker.rs"
    fn checkpoint_due(&mut self, now: Instant) -> Result<(), Arc<StorageError>> {
        if let DeliveryMode::Durable { progress, .. } = &mut self.delivery {
            for g in 0..GROUP_COUNT {
                let group = GroupId::new(g).expect("fixed groups");
                if progress.due(group, now) {
                    progress.checkpoint(group, self.visible_end, &mut self.sync).map_err(Arc::new)?;
                }
            }
        }
        Ok(())
    }

    fn run(&mut self) -> Result<WorkerState, Arc<StorageError>> {
        loop {
            let progress_deadline = match &self.delivery {
                DeliveryMode::Durable { progress, .. } => progress.deadline(),
                _ => None,
            };
            let deadline = self.deadline.into_iter().chain(progress_deadline).min();
            match self.mailbox.recv(deadline) {
                Event::Data(command) => self.publish(command)?,
                Event::Control(command) => self.control(command)?,
                Event::Deadline => {
                    if self.deadline.is_some_and(|end| end <= Instant::now()) { self.commit()?; }
                    self.checkpoint_due(Instant::now())?;
                }
                // Joining is not a consumer checkpoint. Request one explicitly before closing.
                Event::Closed => { self.commit()?; return Ok(self.state()); }
            }
        }
    }

    fn fail(&mut self, cause: Arc<StorageError>) {
        self.log.stop_admission();
        let (data, control) = self.mailbox.abort();
        for pending in self.pending.drain(..) {
            pending.reply.finish(Err(RequestError::Failed(Arc::clone(&cause))));
        }
        for command in data { command.reply.finish(Err(RequestError::Failed(Arc::clone(&cause)))); }
        for command in control { command.fail(Arc::clone(&cause)); }
        self.pending_bytes = 0;
        self.deadline = None;
    }
}

impl<S: SyncIo> Drop for Worker<S> {
    fn drop(&mut self) {
        self.log.stop_admission();
        // On unwinding, queued and pending Reply values deliver OwnerStopped.
        // abort moves the queued values out before their destructors run.
        drop(self.mailbox.abort());
    }
}
```

`WorkerError` and the join helper retain the observable failure information. A join error is not a successful shutdown. The bounded facade will delegate to this same join ownership instead of detaching a second worker or silently starting a bridge thread.

```rust title="crates/broker-core/src/worker.rs"
#[derive(Debug)]
pub enum WorkerError {
    Setup(StorageError),
    Spawn(io::Error),
    Failed { state: WorkerState, cause: Arc<StorageError> },
    Panicked,
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(_) => write!(f, "partition owner initialization failed"),
            Self::Spawn(_) => write!(f, "partition thread could not start"),
            Self::Failed { state, .. } => write!(f, "partition stopped at visible end {}", state.visible_end.get()),
            Self::Panicked => write!(f, "partition thread panicked"),
        }
    }
}

impl Error for WorkerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Setup(error) => Some(error),
            Self::Spawn(error) => Some(error),
            Self::Failed { cause, .. } => Some(cause.as_ref()),
            Self::Panicked => None,
        }
    }
}

pub struct WorkerHandle {
    client: Client,
    thread: Option<JoinHandle<Result<WorkerState, WorkerError>>>,
}

fn join_thread(handle: JoinHandle<Result<WorkerState, WorkerError>>) -> Result<WorkerState, WorkerError> {
    match handle.join() { Ok(result) => result, Err(_) => Err(WorkerError::Panicked) }
}
```

The four legacy constructors pass no bounded resources. The internal constructor additionally accepts a prebuilt mailbox and scratch buffer; only the new bounded facade supplies them. Startup still synchronizes the recovered log before exposing a client, constructs the matching delivery mode, and moves the exclusive log owner into one named thread.

```rust title="crates/broker-core/src/worker.rs"
impl WorkerHandle {
    pub fn start<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S, config: BatchConfig,
        data_slots: usize, control_slots: usize) -> Result<Self, WorkerError>
    {
        Self::start_inner(log, sync, config, data_slots, control_slots, None, None)
    }

    pub fn start_with_delivery<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize, delivery: DeliveryConfig)
        -> Result<Self, WorkerError>
    {
        Self::start_inner(log, sync, config, data_slots, control_slots, Some(DeliverySetup::Trusted(delivery)), None)
    }

    pub fn start_with_leases<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize,
        leases: LeaseConfig, clock: Arc<dyn Clock>) -> Result<Self, WorkerError>
    {
        Self::start_inner(log, sync, config, data_slots, control_slots,
            Some(DeliverySetup::Leased { config: leases, clock }), None)
    }

    pub fn start_with_progress<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize,
        leases: LeaseConfig, clock: Arc<dyn Clock>, policy: CheckpointPolicy) -> Result<Self, WorkerError>
    {
        Self::start_inner(log, sync, config, data_slots, control_slots,
            Some(DeliverySetup::Durable { config: leases, clock, policy }), None)
    }
```

Constructing the pools happens before this call in the bounded facade. A setup or spawn failure drops those resources and returns an error. A successful thread uses the injected mailbox, so queue slots reserved by the facade belong to the actual owner, not a disconnected proxy queue.

```rust title="crates/broker-core/src/worker.rs"
    fn start_inner<S: SyncIo + Send + 'static>(mut log: PartitionLog, mut sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize,
        delivery: Option<DeliverySetup>,
        resources: Option<(Arc<Mailbox<Publish, Control>>, Vec<u8>)>) -> Result<Self, WorkerError>
    {
        let (mailbox, scratch) = match resources {
            Some((mailbox, scratch)) => (mailbox, Some(scratch)),
            None => (Arc::new(Mailbox::new(data_slots, control_slots)
                .map_err(|e| WorkerError::Setup(StorageError::Config(e)))?), None),
        };
        // Strict reopening alone does not establish a synchronized visible prefix.
        // Initialization runs on the caller; no client exists until it succeeds.
        let visible_end = log.synchronize(&mut sync).map_err(WorkerError::Setup)?;
        let name = format!("partition-{}", log.partition().get());
        let delivery = DeliveryMode::new(&mut log, visible_end, delivery).map_err(WorkerError::Setup)?;
        let disk = log.disk_admission();
        let mut worker = Worker {
            log, sync, mailbox: Arc::clone(&mailbox), config,
            assigned_end: visible_end, visible_end,
            pending: Vec::with_capacity(config.records), pending_bytes: 0, deadline: None, sync_groups: 0, delivery, scratch,
        };
        let thread = thread::Builder::new().name(name).spawn(move || {
            match worker.run() {
                Ok(state) => Ok(state),
                Err(cause) => {
                    let state = worker.state();
                    worker.fail(Arc::clone(&cause));
                    Err(WorkerError::Failed { state, cause })
                }
            }
        }).map_err(WorkerError::Spawn)?;
        Ok(Self { client: Client { mailbox, disk }, thread: Some(thread) })
    }
```

Joining remains a blocking operation. Part 3's executable calls it outside its runtime's serving future. A future supervisor may place a bounded number of such joins on appropriate blocking execution; it must not manufacture a blocking receiver task for every request.

```rust title="crates/broker-core/src/worker.rs"
    pub fn client(&self) -> Client { self.client.clone() }
    pub fn close(&self) { self.client.mailbox.close(); }

    pub fn join(mut self) -> Result<WorkerState, WorkerError> {
        self.close();
        join_thread(self.thread.take().expect("join permission owned once"))
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.close();
            if let Err(error) = join_thread(thread) {
                use std::io::Write;
                let _ = writeln!(std::io::stderr(), "owner joined during Drop: {error:?}");
            }
        }
    }
}

mod bounded;
pub use bounded::{AsyncTicket, BoundedClient, BoundedHandle, MemoryState, PreparedAppend, PreparedLease};
```

## Trace simultaneous copies explicitly

For a three-byte append, the append pool owns `cat` in one fixed buffer. The owner copies a 32-byte header and those three bytes into scratch, writes that 35-byte prefix, then returns the append buffer. Scratch stays allocated for the next operation. Only after synchronization does the small offset reply report success.

For a two-record fetch, scratch validates any skipped predecessor records. Selected encoded records are copied directly into consecutive response ranges. At return, the owner has no borrowed view into that response; the response batch owns it. Retention can delete a segment after copying without invalidating the batch.

A response still held by a consumer is therefore a capacity reservation even after its record was acknowledged, checkpointed, and removed from disk. Part 3 demonstrates this exact sequence while all other payload resources are saturated.
