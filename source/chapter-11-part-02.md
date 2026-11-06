# Chapter 11 — Bounded delivery and out-of-order completion
## Part 2 — Delivery on the partition owner

This part continues [Part 1](https://25d-228.github.io/rust-learning/chapter-11.html). Its implementation is included in the chapter's **5.0-hour total**, not a second estimate. The window is complete; now connect it to the recovered log, bounded reads, and existing request/reply path. [Part 3](https://25d-228.github.io/rust-learning/chapter-11-part-03.html) completes the test target and executable.

## Keep group state beside the log, not inside every client

A partition has one file-owning thread. Give that thread two windows as well. Clients send commands naming a `GroupId`; they do not borrow the windows or mutate a shared offset. This serializes issuance and completion with reads and appends, without adding a lock around every slot.

The ownership path is:

```text
StorageSession -> (published epoch, four PartitionLogs)
    each log -> one Worker -> one Groups -> two Windows
Client -> bounded control lane -> Worker
Worker -> copied OwnedRecords -> one reply ticket -> consumer
```

The epoch comes from `StorageSession::into_parts()`, after Chapter 10's publication succeeds. It labels this session; it is not yet a validated acknowledgement token. Do not substitute a hard-coded epoch or infer it from filenames. The low-level configuration constructor does not authenticate its caller: startup must pass the returned epoch and the matching logs.

Create `crates/broker-core/src/delivery/groups.rs`. `DeliveryConfig` contains an already validated width and the session epoch. Group identities are still the two fixed `GroupId` values defined in Chapter 7.

```rust title="crates/broker-core/src/delivery/groups.rs"
use crate::storage::error::StorageError;
use crate::storage::log::PartitionLog;
use crate::storage::segment::{OwnedRecord, ReadLimits, ReadStop};
use crate::types::{Epoch, GROUP_COUNT, GroupId, Offset, PartitionId};
use super::window::{Completion, Window, WindowError, WindowState, validate_width};

#[derive(Debug, Clone, Copy)]
pub struct DeliveryConfig {
    epoch: Epoch,
    width: usize,
}

impl DeliveryConfig {
    pub fn new(epoch: Epoch, width: usize) -> Result<Self, WindowError> {
        validate_width(width)?;
        Ok(Self { epoch, width })
    }
}

```

## Make the response explain why delivery stopped

A successful delivery can be empty. The response must distinguish “there is no visible suffix,” “the group's window is full,” and “the next record cannot fit this request's byte allowance.” Repeating the same request is useful only for some of those situations.

`DeliveryBatch` owns its records. Its `state` is the post-issuance group snapshot, not a reference to the owner. `encoded_bytes` counts returned record buffers, including their 32-byte record headers. `scanned_records` preserves storage's diagnostic count, including any validated records skipped from a sparse-index anchor. It is not the number newly issued.

`GroupCompletion` carries both the group identity and the pure-window transition result. Its `state.window` equals `completion.state`; the duplication makes the boundary visible here, not a second independently maintained state.

```rust title="crates/broker-core/src/delivery/groups.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupState {
    pub epoch: Epoch,
    pub partition: PartitionId,
    pub group: GroupId,
    pub window: WindowState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStop {
    VisibleEnd,
    WindowFull,
    RecordLimit,
    ByteLimit { required: usize },
}

#[derive(Debug)]
pub struct DeliveryBatch {
    pub state: GroupState,
    pub records: Vec<OwnedRecord>,
    pub encoded_bytes: usize,
    pub scanned_records: usize,
    pub stop: DeliveryStop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCompletion {
    pub state: GroupState,
    pub completion: Completion,
}

```

If a request reaches the visible end exactly as it fills the window, report `VisibleEnd`. Otherwise a newly full window reports `WindowFull`; only then distinguish the storage record and byte limits. This priority is observational: a snapshot's occupied count still tells the caller that a window is full even when its stop reason is `VisibleEnd`.

| Stop reason | Meaning | Useful next action |
| --- | --- | --- |
| `VisibleEnd` | All currently visible new positions have been issued | Wait for more synchronized records; complete existing work |
| `WindowFull` | Visible work exists, but no tracking slots are free | Complete the missing prefix; do not spin on delivery |
| `RecordLimit` | The request's record cap stopped an otherwise possible read | Request another bounded batch |
| `ByteLimit { required }` | The next record's encoded size exceeds the remaining byte allowance | Use a sufficient next request allowance; no record was skipped |

`required` is the **whole encoded size** of the next record, not the additional shortfall and not merely its payload size. A 3-byte payload requires 35 encoded bytes. An empty payload still requires 32.

## Construct two independent owners of slot storage

`[Window; GROUP_COUNT as usize]` is a fixed-length array; its size is known from the course's group count. [`std::array::from_fn`](https://doc.rust-lang.org/std/array/fn.from_fn.html) invokes the closure separately for each element. Each invocation allocates its own boxed slots. The ignored closure argument would be the array index.

Do not write `[one_window; 2]`: repeating an owned non-`Copy` value is not the construction we want. We deliberately do not add `Clone` to `Window` just to make that spelling convenient. At this stage both groups begin at zero, because consumer progress has not been persisted and retention has not removed a prefix.

```rust title="crates/broker-core/src/delivery/groups.rs"
pub(crate) struct Groups {
    epoch: Epoch,
    partition: PartitionId,
    windows: [Window; GROUP_COUNT as usize],
}

impl Groups {
    pub(crate) fn new(partition: PartitionId, config: DeliveryConfig) -> Self {
        Self {
            epoch: config.epoch, partition,
            windows: std::array::from_fn(|_| Window::new(Offset::new(0), config.width)
                .expect("DeliveryConfig validated the width")),
        }
    }

```

The array access converts a validated `GroupId` to `usize`. No network integer is accepted here without Chapter 7's identity validation. `complete` borrows exactly the selected window, applies one transition, and then obtains a fresh snapshot. There is no storage write and no synchronization in this path.

```rust title="crates/broker-core/src/delivery/groups.rs"
    pub(crate) fn state(&self, group: GroupId) -> GroupState {
        GroupState { epoch: self.epoch, partition: self.partition, group,
            window: self.windows[usize::from(group.get())].state() }
    }

    pub(crate) fn complete(&mut self, group: GroupId, offset: Offset)
        -> Result<GroupCompletion, WindowError>
    {
        let completion = self.windows[usize::from(group.get())].complete(offset)?;
        Ok(GroupCompletion { state: self.state(group), completion })
    }

```

## Copy before issuing, and issue only the returned count

The delivery operation has three boundaries. First compute a safe upper bound without mutation. Then let `PartitionLog::read` copy a contiguous checked batch. Only after that succeeds does the window record the returned count. A read error discards any local partial batch and leaves issuance unchanged; the owner will stop conservatively because the storage read may have detected corruption or poisoned its log.

The method borrows `Groups` and the matching log exclusively. It verifies partition identity, obtains the selected window, and avoids file I/O entirely when the bounded interval is empty. The owner's visible end is supplied as an argument; it is **not** replaced with `log.written_end()`.

The count/end consistency check is a defensive internal check, not a second record decoder. The existing reader validates the records' offsets, headers, and checksums. If a future reader refactor returns a different next offset than its contiguous count implies, stop rather than quietly issue a gap.

```rust title="crates/broker-core/src/delivery/groups.rs"
    pub(crate) fn deliver(&mut self, log: &mut PartitionLog, visible: Offset,
        group: GroupId, limits: ReadLimits) -> Result<DeliveryBatch, StorageError>
    {
        if log.partition() != self.partition {
            return Err(StorageError::Topology("group attached to a different partition"));
        }
        let window = &mut self.windows[usize::from(group.get())];
        let start = window.state().issued_end;
        let upper = window.read_upper(visible)
            .map_err(|_| StorageError::Topology("delivery window exceeds visible prefix"))?;
        if start == upper {
            let stop = if start == visible { DeliveryStop::VisibleEnd }
                else { DeliveryStop::WindowFull };
            return Ok(DeliveryBatch { state: self.state(group), records: Vec::new(),
                encoded_bytes: 0, scanned_records: 0, stop });
        }
        // read owns its copied buffers. No window changes if it returns an error.
        let batch = log.read(start, upper, limits)?;
        let count = batch.records.len();
        let expected_next = start.get().checked_add(count as u64)
            .ok_or(StorageError::Topology("delivery count overflows offset"))?;
        if batch.next != Offset::new(expected_next) || batch.next > upper {
            return Err(StorageError::Topology("read returned an inconsistent delivery interval"));
        }
        // The end promised by the caller is not the number actually copied.
        window.admit(count, visible)
            .map_err(|_| StorageError::Topology("bounded read cannot fit its delivery window"))?;
        let stop = if batch.next == visible { DeliveryStop::VisibleEnd }
            else if window.state().free() == 0 { DeliveryStop::WindowFull }
            else {
                match batch.stop {
                    ReadStop::RecordLimit => DeliveryStop::RecordLimit,
                    ReadStop::ByteLimit { required } => DeliveryStop::ByteLimit { required },
                    ReadStop::End => unreachable!("bounded upper is the visible or window end"),
                }
            };
        Ok(DeliveryBatch { state: self.state(group), records: batch.records,
            encoded_bytes: batch.encoded_bytes, scanned_records: batch.scanned_records, stop })
    }
}
```

Consider three `cat` records, a width-four window, and a 70-byte request. Storage copies two records, each `32 + 3 = 35` bytes. It sees that the next 35-byte record cannot fit the remaining zero bytes, returns `next=2`, and reports a byte limit. The window admits two positions; it does not reserve the third or fourth. The next request begins at offset 2.

If the byte allowance is 34 for an initial `cat`, the returned count is zero and `admit(0, visible)` changes nothing. Repeating 34 bytes cannot make progress. A response does not silently enlarge a caller's budget to fit a record.

Returned bytes are not all read-time memory or I/O. A sparse-index predecessor may require validating skipped records before the requested offset. Chapter 8's reader holds those temporary encoded buffers one at a time, and `scanned_records` exposes that additional work. The delivery window adds no second payload cache.

## Register the new modules

Create the delivery module root, then replace the crate root with the complete list below. `pub(crate)` exposes `Groups` to the worker inside this crate without making it a client-facing ownership API. The public exports describe requests, observations, and results. Module visibility was introduced in Chapter 1.

```rust title="crates/broker-core/src/delivery/mod.rs"
mod groups;
pub mod window;

pub use groups::{DeliveryBatch, DeliveryConfig, DeliveryStop, GroupCompletion, GroupState};
pub(crate) use groups::Groups;
```

```rust title="crates/broker-core/src/lib.rs"
pub mod codec;
pub mod delivery;
pub mod error;
pub mod io;
pub mod mailbox;
pub mod storage;
pub mod types;
pub mod worker;
```

## Extend the owner without changing group commit

Replace `crates/broker-core/src/worker.rs` with the following pieces in order. This is a full resulting file, not a patch with hidden unchanged sections. Its storage-only `start` remains available for earlier fixtures. A new `start_with_delivery` enables the groups explicitly, using the startup epoch. Earlier Chapter 9 synchronization and shutdown semantics remain in force.

The new imports name the group boundary and the pure window's request error. No async runtime is used on the file-owning thread. Async connection code will communicate with this owner later.

```rust title="crates/broker-core/src/worker.rs"
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::codec::HEADER_LEN;
use crate::delivery::{DeliveryBatch, DeliveryConfig, GroupCompletion, GroupState, Groups};
use crate::delivery::window::WindowError;
use crate::mailbox::{AdmissionError, Event, Mailbox, Rejected};
use crate::storage::error::StorageError;
use crate::storage::log::PartitionLog;
use crate::storage::segment::{ReadBatch, ReadLimits};
use crate::storage::sync::SyncIo;
use crate::types::{GroupId, Offset};

```

`BatchConfig` still limits an append synchronization group by records, encoded bytes, and time. `WorkerState` still reports storage progress, not consumer progress. Keeping these types distinct prevents a completion cursor from accidentally becoming a storage visibility watermark.

```rust title="crates/broker-core/src/worker.rs"
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

```

Two new request outcomes are ordinary refusals: delivery was not enabled on this owner, or the trusted completion did not name an issued position. They do not stop the partition. A storage failure still means accepted work may have reached disk; it is never described as rollback. `Error::source` retains the specific completion cause as well as storage causes.

```rust title="crates/broker-core/src/worker.rs"
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
    DeliveryDisabled,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(_) => write!(f, "request refused before mutation"),
            Self::Failed(_) => write!(f, "partition failed; accepted work has an uncertain outcome"),
            Self::OwnerStopped => write!(f, "partition stopped without completing this reply"),
            Self::Delivery(error) => write!(f, "delivery request refused: {error}"),
            Self::DeliveryDisabled => write!(f, "delivery is not enabled on this owner"),
        }
    }
}

impl Error for RequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Refused(e) | Self::Failed(e) => Some(e.as_ref()),
            Self::Delivery(error) => Some(error),
            _ => None,
        }
    }
}

```

## A reply channel is not an acknowledgement of processing

The reply wrapper still owns a single send permission into a one-slot channel. Its destructor sends `OwnerStopped` when normal unwinding drops an unfinished reply. `finish` consumes that permission. A disconnected receiver makes the result unavailable; it does not reverse the operation that produced it.

Rust's [`SyncSender` documentation](https://doc.rust-lang.org/std/sync/mpsc/struct.SyncSender.html) distinguishes successful buffered sending from receiver observation, and documents the nonblocking `try_send` failure cases. Here one sender sends at most once, so a correctly used one-slot channel cannot already be full. The owner never blocks waiting for a consumer to receive its result.

```rust title="crates/broker-core/src/worker.rs"
pub type Ticket<T> = mpsc::Receiver<Result<T, RequestError>>;

struct Reply<T>(Option<mpsc::SyncSender<Result<T, RequestError>>>);

impl<T> Reply<T> {
    fn pair() -> (Self, Ticket<T>) {
        let (sender, receiver) = mpsc::sync_channel(1);
        (Self(Some(sender)), receiver)
    }

    fn finish(mut self, result: Result<T, RequestError>) {
        // Exactly one sender and one send: the one-slot channel cannot be full.
        // A disconnected reader means abandonment, not permission to undo a write.
        if let Some(sender) = self.0.take() { let _ = sender.try_send(result); }
    }
}

impl<T> Drop for Reply<T> {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.try_send(Err(RequestError::OwnerStopped));
        }
    }
}

```

The control enum gains `GroupState`, `Deliver`, and `Complete`. Each carries its own correctly typed reply. `Control::fail` must handle every variant so a partition failure terminates all queued requests, including the newly added ones. Leaving an unhandled reply in a queue would make a caller wait for work that has no owner.

```rust title="crates/broker-core/src/worker.rs"
struct Publish {
    payload: Vec<u8>,
    reply: Reply<Offset>,
}

enum Control {
    Snapshot(Reply<WorkerState>),
    Fetch { start: Offset, limits: ReadLimits, reply: Reply<ReadBatch> },
    Flush(Reply<WorkerState>),
    GroupState { group: GroupId, reply: Reply<GroupState> },
    Deliver { group: GroupId, limits: ReadLimits, reply: Reply<DeliveryBatch> },
    Complete { group: GroupId, offset: Offset, reply: Reply<GroupCompletion> },
}

impl Control {
    fn fail(self, cause: Arc<StorageError>) {
        match self {
            Self::Snapshot(reply) | Self::Flush(reply) => reply.finish(Err(RequestError::Failed(cause))),
            Self::Fetch { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::GroupState { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Deliver { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
            Self::Complete { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
        }
    }
}

```

## Preserve diagnostic reads, add delivery reads

The existing `try_fetch(start, limits)` is a diagnostic read of visible storage. It does **not** issue group work. Keep it for storage tests, but do not use it as the consumer delivery protocol. `try_publish` still returns a rejected payload owner to the caller when mailbox admission fails.

```rust title="crates/broker-core/src/worker.rs"
#[derive(Clone)]
pub struct Client {
    mailbox: Arc<Mailbox<Publish, Control>>,
}

impl Client {
    pub fn try_publish(&self, payload: Vec<u8>) -> Result<Ticket<Offset>, Rejected<Vec<u8>>> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_data(Publish { payload, reply }).map_err(|error| Rejected {
            reason: error.reason, value: error.value.payload,
        })?;
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

The three new methods use the bounded control lane. Admission success means the command was enqueued, not that its operation succeeded. The returned ticket distinguishes the later result. `try_complete` is deliberately named and commented as a trusted in-process operation: it has no lease, attempt generation, deadline, or client-supplied epoch to validate.

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

    // Trusted in-process completion only: no lease/token validation yet.
    pub fn try_complete(&self, group: GroupId, offset: Offset)
        -> Result<Ticket<GroupCompletion>, AdmissionError>
    {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Complete { group, offset, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }

```

`try_flush` is unchanged. It flushes publications already processed by the owner, not arbitrary publications still waiting in the separate data lane. A group delivery is also not a barrier for that lane. Tests that require visible data wait for successful publish replies or first observe the desired written state and then flush.

```rust title="crates/broker-core/src/worker.rs"
    // Flushes data ALREADY processed by this owner, not data still in the other lane.
    pub fn try_flush(&self) -> Result<Ticket<WorkerState>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Flush(reply)).map_err(|e| e.reason)?;
        Ok(ticket)
    }
}

```

## One thread owns both storage and delivery transitions

`groups: Option<Groups>` makes the mode explicit. `None` is the compatibility path for earlier storage-only examples; it does not manufacture epoch zero or pretend that a recovery session was supplied. In the delivery path, the two windows live for exactly the worker's lifetime.

The other fields and storage snapshot retain their earlier meanings. In particular, `assigned_end`, `written_end`, and `visible_end` are not replaced by either group's `completed_end`.

```rust title="crates/broker-core/src/worker.rs"
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
    groups: Option<Groups>,
}

impl<S: SyncIo> Worker<S> {
    fn state(&self) -> WorkerState {
        WorkerState {
            assigned_end: self.assigned_end, written_end: self.log.written_end(),
            visible_end: self.visible_end, pending_records: self.pending.len(),
            pending_bytes: self.pending_bytes, sync_groups: self.sync_groups,
        }
    }

```

`commit` first completes all required file and directory synchronization through `PartitionLog::synchronize`. Only after success does it advance the visible end and finish producer replies. A later delivery can then issue the newly visible positions. No consumer completion is required for storage visibility.

```rust title="crates/broker-core/src/worker.rs"
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

```

The publish path is unchanged but included in full. It preflights the record and batch size, flushes an older batch when necessary, sets a checked deadline, appends under exclusive ownership, and commits on a count or byte boundary. The payload is dropped after its bytes have been written; its reply remains pending until synchronization succeeds.

```rust title="crates/broker-core/src/worker.rs"
    fn publish(&mut self, command: Publish) -> Result<(), Arc<StorageError>> {
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
        let offset = match self.log.append(&command.payload) {
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

## Route refusals and fatal storage errors differently

For a group snapshot, immutable access through `as_ref()` is enough. Completion needs `as_mut()` because it changes a window. Delivery additionally borrows `self.log` mutably while `self.groups` is mutably borrowed. These are disjoint fields of the same owner, so Rust can track them separately; the opening example in [Splitting Borrows](https://doc.rust-lang.org/nomicon/borrow-splitting.html) explains this field distinction. No unsafe borrow splitting is used here.

An invalid completion produces `RequestError::Delivery` and returns to the command loop. A delivery read error instead sends `Failed`, returns the storage cause, and stops the owner. A logically impossible window/read disagreement is also fatal: allowing the partition to continue would make its delivery bookkeeping untrustworthy.

```rust title="crates/broker-core/src/worker.rs"
    fn control(&mut self, command: Control) -> Result<(), Arc<StorageError>> {
        match command {
            Control::Snapshot(reply) => reply.finish(Ok(self.state())),
            Control::GroupState { group, reply } => {
                reply.finish(self.groups.as_ref().map(|groups| groups.state(group))
                    .ok_or(RequestError::DeliveryDisabled));
            }
            Control::Complete { group, offset, reply } => {
                let result = match self.groups.as_mut() {
                    Some(groups) => groups.complete(group, offset).map_err(RequestError::Delivery),
                    None => Err(RequestError::DeliveryDisabled),
                };
                reply.finish(result);
            }
            Control::Deliver { group, limits, reply } => {
                let Some(groups) = self.groups.as_mut() else {
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

The existing receive loop handles one command at a time. Two callers cannot concurrently issue the same group's next offset through this path: the first delivery changes the group's issued end before the next command is serviced. Completion order is the order commands are processed, which need not be offset order or the order external work began.

Graceful closure drains admitted commands and commits pending appends. Fatal failure aborts the mailbox and completes every pending or queued reply with the cause. Neither path persists group completion in this chapter.

```rust title="crates/broker-core/src/worker.rs"
    fn run(&mut self) -> Result<WorkerState, Arc<StorageError>> {
        loop {
            match self.mailbox.recv(self.deadline) {
                Event::Data(command) => self.publish(command)?,
                Event::Control(command) => self.control(command)?,
                Event::Deadline => self.commit()?,
                Event::Closed => { self.commit()?; return Ok(self.state()); }
            }
        }
    }

    fn fail(&mut self, cause: Arc<StorageError>) {
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

```

During unwinding, queued reply owners are dropped after the mailbox values have been moved out. The group's boxed slot arrays are then ordinary owned fields to destroy. Dropping a `Window` does not invoke file I/O or try to acknowledge its outstanding positions.

```rust title="crates/broker-core/src/worker.rs"
impl<S: SyncIo> Drop for Worker<S> {
    fn drop(&mut self) {
        // On unwinding, queued and pending Reply values deliver OwnerStopped.
        // abort moves the queued values out before their destructors run.
        drop(self.mailbox.abort());
    }
}

```

The join result still differentiates setup failure, thread-creation failure, storage failure, and panic. Its storage snapshot is captured at failure; it does not claim that a consumer performed or rolled back an external effect.

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

```

## Start with the published epoch and retain explicit join ownership

`WorkerHandle` still owns the thread join permission. The common constructor below centralizes mailbox creation, initial synchronization, thread naming, and failure cleanup. The new wrapper merely supplies the optional delivery configuration; it does not clone the log or spawn a second file owner.

```rust title="crates/broker-core/src/worker.rs"
pub struct WorkerHandle {
    client: Client,
    thread: Option<JoinHandle<Result<WorkerState, WorkerError>>>,
}

fn join_thread(handle: JoinHandle<Result<WorkerState, WorkerError>>) -> Result<WorkerState, WorkerError> {
    match handle.join() { Ok(result) => result, Err(_) => Err(WorkerError::Panicked) }
}

```

The `Groups` value is constructed before spawning and moved into the worker. Initialization still synchronizes the log before any client is returned. Normal service startup obtains all logs and the epoch from Chapter 10, then passes each log once to `start_with_delivery` with that epoch. Do not increment the epoch separately for each partition: it identifies the whole recovered session.

```rust title="crates/broker-core/src/worker.rs"
impl WorkerHandle {
    pub fn start<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S, config: BatchConfig,
        data_slots: usize, control_slots: usize) -> Result<Self, WorkerError>
    {
        Self::start_inner(log, sync, config, data_slots, control_slots, None)
    }

    pub fn start_with_delivery<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize, delivery: DeliveryConfig)
        -> Result<Self, WorkerError>
    {
        Self::start_inner(log, sync, config, data_slots, control_slots, Some(delivery))
    }

    fn start_inner<S: SyncIo + Send + 'static>(mut log: PartitionLog, mut sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize,
        delivery: Option<DeliveryConfig>) -> Result<Self, WorkerError>
    {
        let mailbox = Arc::new(Mailbox::new(data_slots, control_slots)
            .map_err(|e| WorkerError::Setup(StorageError::Config(e)))?);
        // Strict reopening alone does not establish a synchronized visible prefix.
        // Initialization runs on the caller; no client exists until it succeeds.
        let visible_end = log.synchronize(&mut sync).map_err(WorkerError::Setup)?;
        let name = format!("partition-{}", log.partition().get());
        let groups = delivery.map(|value| Groups::new(log.partition(), value));
        let mut worker = Worker {
            log, sync, mailbox: Arc::clone(&mailbox), config,
            assigned_end: visible_end, visible_end,
            pending: Vec::with_capacity(config.records), pending_bytes: 0, deadline: None, sync_groups: 0, groups,
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
        Ok(Self { client: Client { mailbox }, thread: Some(thread) })
    }

```

Calling `join` first closes admission, then consumes the stored handle. `Drop` remains the fallback that joins rather than silently detaching a file-owning thread. As before, explicit joining is preferable when the caller needs to inspect a failure. These lifecycle operations do not reset a live group's positions; they end the worker that owns them.

```rust title="crates/broker-core/src/worker.rs"
    pub fn client(&self) -> Client { self.client.clone() }
    pub fn close(&self) { self.client.mailbox.close(); }

    pub fn join(mut self) -> Result<WorkerState, WorkerError> {
        self.close();
        join_thread(self.thread.take().expect("join permission owned once"))
    }
}

```

```rust title="crates/broker-core/src/worker.rs"
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
```

## Cancellation can lose a reply, not undo issuance

The delivery ordering is intentional:

```text
read and validate copied records
    -> mark exactly those positions outstanding
    -> move the records into the reply
    -> consumer may receive, process, and later complete them
```

If reading fails, no slots have changed and no partial successful batch escapes. Once slots have been issued, a disconnected ticket does not rewind them. Even a successful channel send is not evidence that the receiver processed anything. Trying to undo issuance only on a failed send would produce different semantics for two indistinguishable consumer-loss cases: disconnect just before sending and disconnect just after sending.

The present consequence is deliberately visible: abandoning a delivery can stall that group's window until a trusted completion arrives or the process restarts. It does not stall the other group, and it does not authorize eviction of the missing position. Chapter 12 adds bounded retries and lease generations to make that loss recoverable during a running session. This intermediate trusted API is **not yet the final at-least-once network service**.

A dropped completion reply is different: the transition may already have applied. Retrying an offset that remains completed returns `Duplicate`; retrying one below the retired end returns `AlreadyRetired`. Neither response repeats an external effect or proves one happened.

## Resource and failure boundaries to keep visible

There is one on-disk log per partition, not one per group. Different groups can receive separate owned copies of the same encoded records. Windows retain only slot metadata; once a reply owns a buffer, dropping or retaining it is independent of the window's lifetime.

The existing mailbox bounds queued **commands**, not total bytes in completed reply tickets or buffers retained by consumers. Delivery and completion share the control lane. A control-lane flood can cause a completion admission to return `Full`; this chapter does not claim pressure-proof acknowledgement admission. The byte-budget and service chapters must preserve progress paths under sustained load.

Likewise, a long bounded disk read can occupy the owner for its duration. Slot width bounds bookkeeping, not wall-clock latency of the filesystem. `scanned_records`, the read limits, and later measurements make the cost observable rather than hiding it behind an async method name.

Finally, every group position is volatile. A clean shutdown is not a consumer checkpoint. Restart rebuilds the logs and publishes a new epoch, but this chapter creates both windows at zero and can repeat previously completed records. Chapter 13 will load a justified persisted next offset; Chapter 14 will use persisted progress, never this volatile completion end, to decide retention.

The owner integration is complete. [Part 3](https://25d-228.github.io/rust-learning/chapter-11-part-03.html) checks these boundaries through the real startup and owner APIs and against a separate set-based reference.
