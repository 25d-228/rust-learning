# Chapter 13 — Durable consumer progress
## Part 2 — Accepted completion and checkpointed progress

> **Learning objectives**
> Expose volatile and persisted frontiers as different results. Coalesce progress under a first-dirty deadline that cannot be postponed by busy traffic. Serialize checkpoint publication with lease processing and stop the owner on uncertain persistence failure.

This part continues [Part 1](https://25d-228.github.io/rust-learning/chapter-13.html). The **4.5-hour estimate covers all three parts together**. Use its checkpoint codec, publication helper, and startup seeds. [Part 3](https://25d-228.github.io/rust-learning/chapter-13-part-03.html) supplies all executable checks. The complete resulting-file convention still applies: concatenate same-filename blocks in order.

## Give the two promises different names

An acknowledgement says that a token was accepted by this owner, or that this exact still-retained attempt had already completed. It does not say that restart will preserve that completion. A checkpoint response says which contiguous frontier completed the persistence protocol. It does not say that every currently issued record has completed, or that an external effect happened exactly once.

`AckAccepted` contains the existing `Ack` classification and a `ProgressState`. `ProgressAck::Accepted` includes `Applied` and `Duplicate`; inspect `ack.outcome` to distinguish a new completion from recognition of a previously completed follower. `AlreadyRetired` is deliberately `NotApplied`: Chapter 12 does not retain historical tokens for that interval. Rejected tokens are also `NotApplied`, never success disguised as an unchanged integer.

`Checkpointed` carries the persisted state and a `published` flag. When volatile and persisted frontiers are already equal, the response can report `published=false` without creating a file. That is still a valid report of known persisted progress, not a new synchronization measurement.

Snapshots are copied observations, not capabilities to modify the owner's fields. Clients cannot request “checkpoint offset 10.” They can request **checkpoint this group's current contiguous progress**, and the owner decides the value when it processes the command.

Create `delivery/progress.rs`. The periodic constructor accepts a positive delay of at most 60 seconds. `explicit_only()` installs no timer; persistence then requires an explicit request. This deliberate mode makes examples and crash fixtures independent of pauses in the surrounding test process. It must not be confused with an automatic durability promise.

```rust title="crates/broker-core/src/delivery/progress.rs"
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::storage::checkpoint::{self, Checkpoint};
use crate::storage::error::StorageError;
use crate::storage::sync::SyncIo;
use crate::types::{Epoch, GROUP_COUNT, GroupId, Offset, PartitionId};
use super::lease::{Ack, AckOutcome};

#[derive(Debug, Clone, Copy)]
pub struct CheckpointPolicy {
    delay: Option<Duration>,
}
impl CheckpointPolicy {
    pub fn explicit_only() -> Self { Self { delay: None } }

    pub fn new(delay: Duration) -> Result<Self, StorageError> {
        if delay.is_zero() || delay > Duration::from_secs(60) {
            return Err(StorageError::Config("checkpoint delay must be in (0, 60s]"));
        }
        Ok(Self { delay: Some(delay) })
    }
}

```

The fields name exclusive next offsets rather than last-completed offsets. A frontier of zero is therefore a natural empty prefix, and `u64::MAX` can remain representable without adding one. These small values are copied into replies; no response borrows a worker-owned slot or checkpoint buffer.

```rust title="crates/broker-core/src/delivery/progress.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgressState {
    pub epoch: Epoch,
    pub partition: PartitionId,
    pub group: GroupId,
    pub volatile_next: Offset,
    pub persisted_next: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckAccepted {
    pub ack: Ack,
    pub progress: ProgressState,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressAck {
    Accepted(AckAccepted),
    NotApplied { ack: Ack, progress: ProgressState },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpointed {
    pub progress: ProgressState,
    pub published: bool,
}

```

### Keep policy state bounded

Each group gets one `Frontier`: volatile next, persisted next, and an optional first-dirty deadline. The array has exactly two entries. There is no queue entry per acknowledgement and no completed-offset history stored here; the lease window remains responsible for gaps.

`starts.map(...)` is **array mapping**, not collection growth. It consumes the fixed array and produces a same-length array of a different element type; see the [`array::map` documentation](https://doc.rust-lang.org/std/primitive.array.html#method.map). The elements are copied offsets, and construction creates no new payload buffers. The path is owned so it can live with the thread; the associated log still owns the directory guard.

```rust title="crates/broker-core/src/delivery/progress.rs"
#[derive(Clone, Copy)]
struct Frontier {
    volatile: Offset,
    persisted: Offset,
    due: Option<Instant>,
}

pub(crate) struct Progress {
    root: PathBuf,
    epoch: Epoch,
    partition: PartitionId,
    policy: CheckpointPolicy,
    fronts: [Frontier; GROUP_COUNT as usize],
}
impl Progress {
    pub(crate) fn new(root: PathBuf, epoch: Epoch, partition: PartitionId,
        starts: [Offset; GROUP_COUNT as usize], visible: Offset, policy: CheckpointPolicy)
        -> Result<Self, StorageError>
    {
        if starts.iter().any(|start| *start > visible) {
            return Err(StorageError::Metadata("checkpoint exceeds synchronized visibility"));
        }
        Ok(Self { root, epoch, partition, policy,
            fronts: starts.map(|start| Frontier { volatile: start, persisted: start, due: None }) })
    }

```

`state` copies a snapshot. `deadline` scans two optional instants and selects the minimum; `filter_map` emits no item for `None`. `due` includes equality, matching the mailbox's absolute-deadline rule. These are scheduling instants, not the manually controlled lease-clock integers from Chapter 12.

```rust title="crates/broker-core/src/delivery/progress.rs"
    pub(crate) fn state(&self, group: GroupId) -> ProgressState {
        let f = self.fronts[usize::from(group.get())];
        ProgressState { epoch: self.epoch, partition: self.partition, group,
            volatile_next: f.volatile, persisted_next: f.persisted }
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.fronts.iter().filter_map(|f| f.due).min()
    }

    pub(crate) fn due(&self, group: GroupId, now: Instant) -> bool {
        self.fronts[usize::from(group.get())].due.is_some_and(|end| end <= now)
    }

```

### Observe contiguous completion, not the greatest acknowledged offset

`observe` receives the result of the actual lease-window operation, checks its owner identity, and reads `completed_end`. It refuses regression or a value above synchronized visibility. A follower acknowledgement can be `Applied` while leaving `completed_end` unchanged; in that case it creates no new dirty deadline when the frontier is still clean.

The first advance above persisted progress sets a deadline. Later acknowledgements retain it. For first dirty observation time $t_0$ and configured delay $\Delta$, the scheduled attempt is:

$$
t_{\mathrm{due}}=t_0+\Delta.
$$

With $t_0=100$ milliseconds and a 250-millisecond delay, an acknowledgement at 340 milliseconds must not move the due time from 350 to 590. Otherwise a sufficiently busy consumer could postpone persistence forever. This is a scheduling policy, not a hard bound on synchronization latency: the owner cannot interrupt a filesystem operation already in progress.

`Instant::checked_add` handles representability without panicking; the official [`Instant` documentation](https://doc.rust-lang.org/std/time/struct.Instant.html#method.checked_add) also cautions against treating platform timing as a universal steady wall clock. The lease clock and this scheduling clock have separate jobs.

```rust title="crates/broker-core/src/delivery/progress.rs"
    pub(crate) fn observe(&mut self, ack: Ack, visible: Offset, now: Instant)
        -> Result<ProgressAck, StorageError>
    {
        if ack.state.epoch != self.epoch || ack.state.partition != self.partition {
            return Err(StorageError::Topology("acknowledgement belongs to another progress owner"));
        }
        let group = ack.state.group;
        let f = &mut self.fronts[usize::from(group.get())];
        let next = ack.state.window.completed_end;
        if next < f.volatile || next > visible || f.persisted > next {
            return Err(StorageError::Topology("invalid contiguous progress observation"));
        }
        // Check the first dirty deadline before changing this progress state.
        let due = if next > f.persisted && f.due.is_none() {
            match self.policy.delay {
                Some(delay) => Some(now.checked_add(delay)
                    .ok_or(StorageError::Config("checkpoint deadline cannot be represented"))?),
                None => None,
            }
        } else { f.due };
        f.volatile = next;
        f.due = due;
        let progress = self.state(group);
        Ok(match ack.outcome {
            AckOutcome::Applied | AckOutcome::Duplicate => ProgressAck::Accepted(AckAccepted { ack, progress }),
            AckOutcome::AlreadyRetired | AckOutcome::Rejected(_) => ProgressAck::NotApplied { ack, progress },
        })
    }

```

### Publish before advancing the persisted copy

The owner calls `checkpoint` with its actual visible end. It rechecks $P\le C\le V$, avoids a publication when $P=C$, otherwise encodes exactly $C$. The persisted field and timer clear occur only after the shared transaction returns successfully.

A publication error leaves this in-memory persisted field unchanged, but may already have replaced the file. The object is private to the owner, whose handler stops on that error. Calling this routine repeatedly in the same failed owner would violate the intended lifecycle; recovery, not an automatic retry loop, resolves the directory.

A no-op checkpoint does not reread the filesystem to detect an unrelated manual edit. Its validity relies on the directory-ownership contract and the last validated/publication result. The broker is not designed to tolerate external programs rewriting its authorities while it is running.

```rust title="crates/broker-core/src/delivery/progress.rs"
    pub(crate) fn checkpoint<S: SyncIo + ?Sized>(&mut self, group: GroupId,
        visible: Offset, io: &mut S) -> Result<Checkpointed, StorageError>
    {
        let index = usize::from(group.get());
        let f = self.fronts[index];
        if f.persisted > f.volatile || f.volatile > visible {
            return Err(StorageError::Topology("checkpoint would exceed contiguous visible progress"));
        }
        if f.persisted == f.volatile {
            return Ok(Checkpointed { progress: self.state(group), published: false });
        }
        let value = Checkpoint { partition: self.partition, group, epoch: self.epoch, next: f.volatile };
        checkpoint::publish(&self.root, value, io, &mut |_| Ok(()))?;
        // No response and no persisted frontier advance before full publication succeeds.
        self.fronts[index].persisted = f.volatile;
        self.fronts[index].due = None;
        Ok(Checkpointed { progress: self.state(group), published: true })
    }
}

#[cfg(test)]
mod tests;
```

## Start the windows where persistence says to start

`LeasedGroups::new` remains for Chapter 12's intentionally volatile demonstration. It now delegates to `at` with two zeros. The new durable owner uses `at` with the consumed startup starts instead. No lease attempts are loaded from disk.

For persisted starts `[3, 1]` and width 4, group 0 starts with completed and issued ends both 3, and group 1 with both ends 1. Both have four empty slots. The physical attempt array is empty even though its future indexing uses `offset % width`; Chapter 12's constructor already handles arbitrary exclusive starts.

Replace `delivery/leased_groups.rs` with these blocks. Only construction changes; token installation still follows successful bounded record copying.

```rust title="crates/broker-core/src/delivery/leased_groups.rs"
use std::sync::Arc;

use crate::storage::error::StorageError;
use crate::storage::log::PartitionLog;
use crate::storage::segment::{OwnedRecord, ReadLimits, ReadStop};
use crate::types::{GROUP_COUNT, GroupId, Offset, PartitionId};
use super::clock::{Clock, ClockError};
use super::groups::GroupState;
use super::lease::{Ack, LeaseConfig, LeaseError, LeaseToken, LeaseWindow};

#[derive(Debug)]
pub struct LeasedRecord {
    pub token: LeaseToken,
    pub record: OwnedRecord,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseStop {
    NoEligible,
    RecordLimit,
    ByteLimit { required: usize },
}
#[derive(Debug)]
pub struct LeaseBatch {
    pub state: GroupState,
    pub records: Vec<LeasedRecord>,
    pub encoded_bytes: usize,
    pub scanned_records: usize,
    pub stop: LeaseStop,
}

pub(crate) enum LeaseReadError {
    Lease(LeaseError),
    Storage(StorageError),
}
impl From<LeaseError> for LeaseReadError {
    fn from(error: LeaseError) -> Self { Self::Lease(error) }
}
impl From<StorageError> for LeaseReadError {
    fn from(error: StorageError) -> Self { Self::Storage(error) }
}

```

`at` is crate-private. It accepts the two offsets supplied by validated startup; it is not a client command. Each `LeaseWindow` receives its own group ID and start, preserving the independence of the two consumer groups.

```rust title="crates/broker-core/src/delivery/leased_groups.rs"
pub(crate) struct LeasedGroups {
    partition: PartitionId,
    windows: [LeaseWindow; GROUP_COUNT as usize],
    clock: Arc<dyn Clock>,
    last_now: Option<u64>,
}
impl LeasedGroups {
    pub(crate) fn new(partition: PartitionId, config: LeaseConfig, clock: Arc<dyn Clock>) -> Self {
        Self::at(partition, config, clock, [Offset::new(0); GROUP_COUNT as usize])
    }

    pub(crate) fn at(partition: PartitionId, config: LeaseConfig, clock: Arc<dyn Clock>,
        starts: [Offset; GROUP_COUNT as usize]) -> Self
    {
        Self {
            partition,
            windows: std::array::from_fn(|index| LeaseWindow::new(partition,
                GroupId::new(index as u8).expect("fixed group array"), starts[index], config)),
            clock, last_now: None,
        }
    }

```

Clock monotonicity and processing-time acknowledgement validation are retained. The progress layer does not bypass epoch, partition, group, generation, deadline, or expiry checks. An invalid lease result simply observes the same contiguous frontier.

```rust title="crates/broker-core/src/delivery/leased_groups.rs"
    fn sample(&mut self) -> Result<u64, LeaseError> {
        let now = self.clock.now()?;
        if self.last_now.is_some_and(|previous| now < previous) {
            return Err(ClockError::Regressed.into());
        }
        self.last_now = Some(now);
        Ok(now)
    }

    pub(crate) fn state(&self, group: GroupId) -> GroupState {
        self.windows[usize::from(group.get())].state()
    }

    pub(crate) fn acknowledge(&mut self, group: GroupId, token: LeaseToken) -> Result<Ack, LeaseError> {
        let now = self.sample()?;
        Ok(self.windows[usize::from(group.get())].acknowledge(token, now))
    }

```

Delivery still plans without mutation, copies records under the byte limit, samples installation time after copying, and installs only the returned prefix. A lost response still leaves an attempt outstanding. Neither checkpointing nor restart changes the fact that an external side effect may outlive its lease.

```rust title="crates/broker-core/src/delivery/leased_groups.rs"
    pub(crate) fn deliver(&mut self, log: &mut PartitionLog, visible: Offset,
        group: GroupId, limits: ReadLimits) -> Result<LeaseBatch, LeaseReadError>
    {
        if log.partition() != self.partition {
            return Err(StorageError::Topology("leases attached to a different partition").into());
        }
        let selected_at = self.sample()?;
        let index = usize::from(group.get());
        let plan = self.windows[index].plan(visible, limits.records(), selected_at)?;
        let mut stop = if plan.len() == limits.records() {
            LeaseStop::RecordLimit
        } else { LeaseStop::NoEligible };
        let mut records = Vec::with_capacity(plan.len());
        let mut encoded_bytes = 0;
        let mut scanned_records = 0;
        for selected in &plan {
            let upper = selected.offset.next().expect("a planned offset has a successor");
            // A one-byte allowance can inspect a nonfitting header even when no bytes remain.
            // Every encoded record is at least 32 bytes; none can slip through that probe.
            let remaining = limits.bytes() - encoded_bytes;
            let one = ReadLimits::new(1, remaining.max(1))?;
            let mut batch = log.read(selected.offset, upper, one)?;
            scanned_records += batch.scanned_records;
            if batch.records.is_empty() {
                if let ReadStop::ByteLimit { required } = batch.stop {
                    stop = LeaseStop::ByteLimit { required };
                    break;
                }
                return Err(StorageError::Topology("planned delivery record was not returned").into());
            }
            if batch.records.len() != 1 || batch.next != upper
                || batch.records[0].offset() != selected.offset || batch.encoded_bytes > remaining
            {
                return Err(StorageError::Topology("single-record lease read is inconsistent").into());
            }
            encoded_bytes += batch.encoded_bytes;
            records.push(batch.records.pop().expect("one validated record"));
        }
        // Do not spend lease time on our own synchronous storage reads.
        // This second observation can fail; until installation no delivery state changed.
        let tokens = if records.is_empty() { Vec::new() } else {
            let issued_at = self.sample()?;
            self.windows[index].install(&plan[..records.len()], visible, issued_at)?
        };
        assert_eq!(records.len(), tokens.len());
        let records = records.into_iter().zip(tokens).map(|(record, token)| {
            LeasedRecord { token, record }
        }).collect();
        Ok(LeaseBatch { state: self.state(group), records,
            encoded_bytes, scanned_records, stop })
    }
}
```

Update the delivery module index. `progress` is public for its request/result types, while the runtime's mutable `Progress` remains crate-private.

```rust title="crates/broker-core/src/delivery/mod.rs"
mod groups;
pub mod progress;
pub mod window;
pub mod clock;
pub mod lease;
mod leased_groups;

pub use groups::{DeliveryBatch, DeliveryConfig, DeliveryStop, GroupCompletion, GroupState};
pub use lease::{Ack, AckOutcome, AckRejection, LeaseConfig, LeaseError, LeaseToken};
pub use leased_groups::{LeaseBatch, LeasedRecord, LeaseStop};
pub(crate) use groups::Groups;
pub(crate) use leased_groups::{LeaseReadError, LeasedGroups};
```

## Integrate one durable mode rather than two competing completion paths

There are now four explicit modes: no delivery, the old trusted-window exercise, volatile leased delivery, and durable leased delivery. Keeping the earlier modes preserves their exercises, but they are not alternate interfaces into a durable owner's windows.

| Command | Durable owner behavior |
| --- | --- |
| `try_lease` | Issue or retry under the existing lease rules |
| `try_ack_progress` | Validate token, update volatile frontier, return its relation to persisted progress |
| `try_progress` | Return both frontiers without changing them |
| `try_checkpoint` | Publish the current contiguous frontier; no caller-supplied offset |
| `try_ack`, `try_complete`, `try_deliver` | Refuse the wrong delivery mode |
| `try_fetch` | Diagnostic record read, not delivery or completion |
| `try_flush` | Synchronize already processed producer writes, not consumer progress |

Rejecting the old `try_ack` on the durable owner prevents a caller from accidentally receiving a reply shape that hides persisted progress. It also keeps one completion path responsible for notifying the checkpoint policy.

Replace `worker.rs` with these complete blocks. The request/reply ownership, bounded mailbox, producer batching, and thread cleanup are retained. Important changed arms are explained as they appear.

```rust title="crates/broker-core/src/worker.rs"
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::codec::HEADER_LEN;
use crate::delivery::{Ack, DeliveryBatch, DeliveryConfig, GroupCompletion, GroupState, Groups,
    LeaseBatch, LeaseConfig, LeaseError, LeaseReadError, LeaseToken, LeasedGroups};
use crate::delivery::clock::Clock;
use crate::delivery::progress::{Checkpointed, CheckpointPolicy, Progress, ProgressAck, ProgressState};
use crate::delivery::window::WindowError;
use crate::mailbox::{AdmissionError, Event, Mailbox, Rejected};
use crate::storage::error::StorageError;
use crate::storage::log::PartitionLog;
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

```

The producer state remains distinct from progress state. An owner may have assigned and written records above its visible end. Those values must not be used as a consumer checkpoint limit. Ordinary lease errors remain healthy request refusals; uncertain storage/publication failures use the existing fatal error path.

```rust title="crates/broker-core/src/worker.rs"
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

The one-slot reply channel still has one sender and at most one send. Dropping the receiving ticket is abandonment, not permission to undo a completion or replacement. A dropped `Reply` on unwinding sends `OwnerStopped` when possible; it cannot reconstruct a successful checkpoint response.

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

struct Publish {
    payload: Vec<u8>,
    reply: Reply<Offset>,
}

```

### Add owned control commands

The new commands own their `Reply` values just like the old ones. `Control::fail` includes every new variant, so a fatal checkpoint error closes admission and fails queued acknowledgement, progress, and checkpoint requests instead of leaving their callers waiting indefinitely.

```rust title="crates/broker-core/src/worker.rs"
enum Control {
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
}

impl Control {
    fn fail(self, cause: Arc<StorageError>) {
        match self {
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
        }
    }
}

```

The client remains a cloneable mailbox handle. Its earlier request functions are retained so Chapter 9–12 programs still compile in their explicitly selected modes. `try_*` means immediate bounded admission, not guaranteed processing or eventual success.

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

```

The new client functions carry a group and, for acknowledgement, a token. The checkpoint function carries **no offset**. When submitting from multiple threads, reason about actual mailbox insertion order rather than the order in which the application started its calls. A caller that needs one accepted completion included can wait for that acknowledgement before submitting its checkpoint.

```rust title="crates/broker-core/src/worker.rs"
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

### Consume startup provenance before spawning

The setup enum describes what to construct; the mode enum owns the resulting state. For durable setup, `take_progress` verifies the configured lease epoch against the session and consumes its seed. `Progress::new` verifies both starts against synchronized visibility before the thread or client is returned.

The immutable fixed array is used twice because `[Offset; 2]` is `Copy`: once to initialize frontiers, once to initialize empty windows. This copies two integers, not checkpoint authority or a second running owner. The non-Clone log remains the single directory-owning object moved into the thread.

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

```

The worker still owns the log, the synchronization adapter, pending producer replies, and the delivery mode together. No asynchronous task borrows a checkpoint transaction across an `await`. Filesystem operations here intentionally block this partition's owner; later network adapters must not confuse that with permission to block the runtime's core threads.

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
    delivery: DeliveryMode,
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

Producer group commit is retained unchanged. It advances `visible_end` only after all dirty data and required names synchronize. That sequencing means a delivery window can never legitimately complete above the visibility value checked by `Progress`. A consumer checkpoint does not need to resynchronize records already below that boundary.

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

The existing publish routine still preflights capacity, batches by encoded bytes/count/deadline, and returns success only through `commit`. A checkpoint arriving in the other mailbox lane is not a barrier for data commands that have not been processed.

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

### Keep old modes isolated

The initial control arms retain trusted delivery behavior. The group-state query supports both leased modes but does not expose a mutable window. Tokenless completion is still available only on the explicitly trusted exercise owner.

```rust title="crates/broker-core/src/worker.rs"
    fn control(&mut self, command: Control) -> Result<(), Arc<StorageError>> {
        match command {
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
```

The legacy acknowledgement arm handles only volatile `Leased`. The leased-read arm supports both leased modes, because token creation is the same mechanism in each. A storage failure during copying is still fatal; a clock/deadline/generation refusal remains a healthy lease error.

```rust title="crates/broker-core/src/worker.rs"
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

### Observe only after lease validation, and fail conservatively

`AckProgress` first asks `LeasedGroups` to validate and apply the token. It then passes the resulting state to `Progress::observe`. A wrong token can return `NotApplied` normally. A storage invariant or unrepresentable scheduling deadline after a window change is different: the window may already have advanced, so the owner stops instead of pretending it rolled back.

`Checkpoint` invokes the persistence routine while the same thread owns both frontiers and the log. Any error is fatal and its own reply receives `Failed`. No later command gets to reuse a possibly replaced checkpoint as if publication had certainly not happened.

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
```

The retained flush and diagnostic fetch arms finish the match. Neither changes consumer progress. A diagnostic fetch above visibility remains an ordinary range refusal; a damaged record remains a terminal storage error.

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

### Wait for the earliest producer or consumer deadline

A dirty periodic group contributes one deadline; a clean group or explicit-only policy contributes none. The worker takes the earliest consumer deadline, combines it with its producer deadline, and waits on the existing mailbox. `Option<Instant>::into_iter()` yields zero or one item; [`Iterator::chain`](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.chain) appends the other optional item without allocating a collection. `min()` returns `None` only when neither kind of deadline exists.

The mailbox already tests an expired deadline before ready queue items, so busy traffic cannot continually postpone an already-due attempt. `Event::Deadline` independently checks which work is actually due: it does not prematurely commit a producer batch merely because a consumer timer fired. At most two checkpoint publications are attempted by one `checkpoint_due` call.

For producer due time 180 and consumer due time 350, the worker first services producer durability at 180. With no new producer batch, the next wait remains 350. If a second group becomes due at 400, it is not reset by group 0's publication. A slow synchronization may delay either event; this baseline does not promise real-time deadlines or preempt synchronous I/O.

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

```

The failure path drains pending and queued replies, closes admission, and clears the producer timer. A failed progress publication never retries inside the owner. The group state is dropped with the stopped worker; the application must join the failed session's owners and recover under a fresh epoch before replacing them. Chapter 17 will make session-wide supervision explicit.

```rust title="crates/broker-core/src/worker.rs"
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

impl<S: SyncIo> Drop for Worker<S> {
    fn drop(&mut self) {
        // On unwinding, queued and pending Reply values deliver OwnerStopped.
        // abort moves the queued values out before their destructors run.
        drop(self.mailbox.abort());
    }
}

```

The join error still retains both the producer-state snapshot and the cause. It is not a persisted-progress report. A panic or `Failed` response cannot be interpreted as “nothing reached disk.” In particular, the checkpoint failure tests deliberately include a file that has already been replaced.

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

The handle retains one join permission. The old constructors continue selecting their old modes, preserving prerequisite programs. A low-level directory/log fixture without startup seeds remains valid for the storage-only constructor, but is refused by `start_with_progress`.

```rust title="crates/broker-core/src/worker.rs"
pub struct WorkerHandle {
    client: Client,
    thread: Option<JoinHandle<Result<WorkerState, WorkerError>>>,
}

fn join_thread(handle: JoinHandle<Result<WorkerState, WorkerError>>) -> Result<WorkerState, WorkerError> {
    match handle.join() { Ok(result) => result, Err(_) => Err(WorkerError::Panicked) }
}

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
        Self::start_inner(log, sync, config, data_slots, control_slots, Some(DeliverySetup::Trusted(delivery)))
    }

    pub fn start_with_leases<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize,
        leases: LeaseConfig, clock: Arc<dyn Clock>) -> Result<Self, WorkerError>
    {
        Self::start_inner(log, sync, config, data_slots, control_slots,
            Some(DeliverySetup::Leased { config: leases, clock }))
    }

```

The durable constructor adds the policy and the same clock interface used for leases. It does not accept persisted offsets or a mutable checkpoint object from the caller. `start_inner` first establishes synchronized visibility, constructs the selected mode, and only then spawns the owner and exposes its client.

```rust title="crates/broker-core/src/worker.rs"
    pub fn start_with_progress<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize,
        leases: LeaseConfig, clock: Arc<dyn Clock>, policy: CheckpointPolicy) -> Result<Self, WorkerError>
    {
        Self::start_inner(log, sync, config, data_slots, control_slots,
            Some(DeliverySetup::Durable { config: leases, clock, policy }))
    }

    fn start_inner<S: SyncIo + Send + 'static>(mut log: PartitionLog, mut sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize,
        delivery: Option<DeliverySetup>) -> Result<Self, WorkerError>
    {
        let mailbox = Arc::new(Mailbox::new(data_slots, control_slots)
            .map_err(|e| WorkerError::Setup(StorageError::Config(e)))?);
        // Strict reopening alone does not establish a synchronized visible prefix.
        // Initialization runs on the caller; no client exists until it succeeds.
        let visible_end = log.synchronize(&mut sync).map_err(WorkerError::Setup)?;
        let name = format!("partition-{}", log.partition().get());
        let delivery = DeliveryMode::new(&mut log, visible_end, delivery).map_err(WorkerError::Setup)?;
        let mut worker = Worker {
            log, sync, mailbox: Arc::clone(&mailbox), config,
            assigned_end: visible_end, visible_end,
            pending: Vec::with_capacity(config.records), pending_bytes: 0, deadline: None, sync_groups: 0, delivery,
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

### Joining is not a hidden consumer checkpoint

Closing drains accepted mailbox commands and synchronizes pending producer writes as before. It does **not** force an extra checkpoint of every group. In periodic mode, an already-due checkpoint may still run before closure; in explicit-only mode none is invented. For a deliberate consumer checkpoint on healthy shutdown, request it and receive its result before closing.

The `Drop` fallback still joins and reports a join error to standard error; an explicit `join` is the way to handle that result programmatically. There is no asynchronous destructor that can safely promise later persistence.

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
```

## Calculate the repeat range and the cost of the policy

Suppose synchronized visibility is 10, the loaded frontier is 3, and the current window has completed continuously through offset 6. Then $P=3$ and $C=7$. A successful checkpoint changes $P$ from 3 to 7; a restart after that publication starts at 7. A restart before it starts at 3 and can repeat offsets 3, 4, 5, and 6. If offset 8 had completed above a gap at 7, that follower can repeat as well.

A periodic policy coalesces many completions into one 48-byte publication. Its two synchronization operations can cost far more than copying those 48 bytes. There is no performance claim without measurement. Explicit-only mode removes the automatic attempts but can leave arbitrarily old persisted progress if the application never requests a checkpoint. Periodic scheduling bounds the requested delay, not a record-count repeat window or an uninterruptible storage operation's duration.

The runtime uses a fixed two-entry frontier array and the existing bounded lease arrays. It creates no per-acknowledgement journal. At normal runtime each partition owner publishes one checkpoint at a time, so at most four owners can have candidates in flight; authoritative filenames do not grow with the number of records. Failed candidates remain for recovery. These counts are not the later payload-byte budget and do not replace accounting for strings, record copies, channels, or filesystem allocation.

The essential boundary is now visible to a caller: `AckAccepted` may report different volatile and persisted next offsets; `Checkpointed` reports the persisted value only after the publication protocol succeeds. [Part 3](https://25d-228.github.io/rust-learning/chapter-13-part-03.html) tests both sides of every replacement boundary and shows the resulting repeat interval.

