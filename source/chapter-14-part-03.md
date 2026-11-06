# Chapter 14 — Safe retention and disk admission
## Part 3 — Owner integration and a pressure trace

> **Learning objectives**
> Reserve disk capacity before queue admission, settle it at file growth rather than reply delivery, and derive retention only on the partition owner. Demonstrate that persisted progress releases space without blocking the owner on work only it can process.

**Implementation time:** part of the same 4.5 hours in [Part 1](https://25d-228.github.io/rust-learning/chapter-14.html). Apply [Part 2](https://25d-228.github.io/rust-learning/chapter-14-part-02.html) first. This part replaces `worker.rs` and adds `examples/retention_probe.rs`. [Part 4](https://25d-228.github.io/rust-learning/chapter-14-part-04.html) completes the tests and process-interruption harness.

## Disk admission is a handoff, not a wait inside the owner

When a data request cannot fit, `try_publish` returns `Rejected<Vec<u8>>` with reason `DiskFull`. The caller still owns its payload. It can stop producing, allow consumer progress, and retry that same buffer later. It must not spin indefinitely or pretend that the request received an offset.

The owner must never wait for disk capacity while holding the only execution path for acknowledgements, checkpoints, and retention. That would deadlock: space depends on controls the waiting owner cannot process. The present solution is immediate bounded refusal, separate control admission, and explicit maintenance requests. It does not add an unbounded retry queue.

There are now three distinct capacity/validity refusals. `DiskFull` means the shared ledger cannot reserve worst-case file growth. `Full` means the chosen mailbox lane has no item slot. `InvalidRecord` means the encoded record format cannot represent this payload. Segment-fit and batch-fit checks still belong to the owner and can return a healthy `RequestError::Refused` before offset assignment.

## Preserve the two ownership chains

A producer buffer and disk permit move together through `Publish`. A reply receiver does not own either of them. After the file write, the permit has settled into `file_bytes`, while the producer reply waits in the batch until synchronization succeeds. Consumer responses contain owned payloads and token values; deletion does not invalidate those copies, but their memory is not charged by this disk ledger.

Retained copied payloads and queued request buffers make clear why disk capacity is not the final memory budget. Chapter 15 supplies byte-memory admission and reserved acknowledgement/control resources. Do not call this implementation fully pressure-safe merely because data and control have separate item counts.

## State, errors, and reply ownership

The first chunk retains batch configuration, observable write/visibility ends, and error composition. The `Reply` wrapper still sends at most one result and reports `OwnerStopped` on ordinary destruction when unfinished. An abandoned receiver never grants permission to undo a write.

```rust title="crates/broker-core/src/worker.rs"
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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

## Attach the permit to the command

`Publish` now owns `Option<DiskPermit>`. The new `Retain` control has only a reply; there is no client-specified floor or quota. `Control::fail` includes the new result type so uncertain failure drains it with the same cause as other accepted controls.

```rust title="crates/broker-core/src/worker.rs"
struct Publish {
    payload: Vec<u8>,
    reply: Reply<Offset>,
    disk: Option<DiskPermit>,
}

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
    Retain(Reply<Retained>),
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
            Self::Retain(reply) => reply.finish(Err(RequestError::Failed(cause))),
        }
    }
}

```

## Reserve before trying the data lane

A cloned client shares the same mailbox and optional disk ledger. `try_publish` first validates the encoded record size, then reserves its worst-case growth. Only then does it construct a command and try the mailbox. On queue rejection, `error.value.payload` moves the vector back to the caller. The remaining command fields are destroyed, returning its disk permit and abandoning its unused reply.

The mutex guard used for reservation is gone before `try_data` locks the mailbox. This avoids a disk-ledger/mailbox lock-order cycle. `disk_state` is a snapshot, not permission to reserve by checking first and incrementing later; only the locked reserve method can admit a producer.

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
        self.mailbox.try_data(Publish { payload, reply, disk }).map_err(|error| Rejected {
            reason: error.reason, value: error.value.payload,
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

```

## Leave existing request semantics explicit

The established snapshot/fetch/delivery/lease/progress methods remain here in full. They use the control lane without a data-disk permit. A full disk budget alone cannot reject an acknowledgement or checkpoint, although a full control mailbox can still return `Full`. A caller must handle that refusal with bounded retry or backpressure rather than dropping a completion silently.

```rust title="crates/broker-core/src/worker.rs"
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

## Restore progress before serving a retained log

Legacy trusted or volatile leased delivery starts at zero and therefore cannot serve a log whose floor is nonzero. The setup check rejects those combinations. Durable mode consumes the validated startup seed and constructs each group at its persisted next offset. Diagnostic-only owners remain possible, but retention itself requires durable mode and its validated frontiers.

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

```

## Settlement is not publication to producers

`Pending` contains only the producer reply and assigned offset. `commit` synchronizes storage before visibility and replies advance. In `publish`, preflight refusals leave the owner healthy and drop the command's unspent permit. Once accepted for mutation, `command.disk.take()` moves the permit out while leaving `None` behind, then `append_reserved` consumes it. On success the ledger reflects file growth even while the producer is still waiting for synchronization.

This explains a useful observation during a gated sync: `reserved_bytes` can be zero for the current written record while its reply is pending. The bytes have not disappeared; they moved into `file_bytes`.

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
        let offset = match self.log.append_reserved(&command.payload, command.disk.take()) {
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

## Keep delivery and acknowledgement handling on the owner

The existing control arms still distinguish trusted delivery, leased delivery, and durable progress. Storage copies and lease installation occur on the same owner as retention, so a deletion cannot race a read halfway through its file scan. Only a complete owned batch escapes to the caller. Lease expiration still governs acknowledgement acceptance; retention does not cancel external consumer work.

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
            Control::Progress { group, reply } => {
                let result = match &self.delivery {
                    DeliveryMode::Durable { progress, .. } => Ok(progress.state(group)),
                    _ => Err(RequestError::DeliveryDisabled),
                };
                reply.finish(result);
            }
```

## Retention reads persisted state, never a caller's assertion

The durable acknowledgement path updates volatile progress. The checkpoint path publishes the current contiguous frontier. The new `Retain` arm reads both **persisted** frontiers from the owner, takes their session epoch, and invokes the log's retention operation. Any error in that operation stops the owner; a possibly published floor is not rolled back.

This code also preserves diagnostic `Fetch`: requesting an offset below the retained floor is a healthy range refusal, not a corruption event. A subsequent snapshot must still work.

```rust title="crates/broker-core/src/worker.rs"
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

## Timers, closure, and failure must still drain ownership

The owner continues to wait for the earliest producer or checkpoint deadline. Retention is explicit in this chapter; it has no new hidden timer and is not silently triggered by every acknowledgement. A caller can request it after checkpoint success or after observing disk admission refusal.

Both terminal `fail` and `Drop` stop disk admission before draining queued commands. Their permits are then destroyed. No new producer may use returned credit on an owner whose file lengths are uncertain. Healthy join also closes the old ledger; a restart constructs a different ledger from files, rather than reopening old client handles.

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

## Spawn only after synchronized startup and seed validation

The remaining lifecycle code is complete. `start_inner` synchronizes the log, configures delivery, clones the ledger handle for the client, and then moves the log into its named owner thread. If spawn fails, the moved closure's owned values are destroyed; no usable client is returned. Explicit join consumes the one join handle, while `Drop` is a fallback that still joins rather than detaching the owner.

The thread owns filesystem mutation; clients own only request admission and immutable snapshots. None of these paths performs asynchronous filesystem work or promises that blocking synchronization has a bounded latency.

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
        let disk = log.disk_admission();
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
        Ok(Self { client: Client { mailbox, disk }, thread: Some(thread) })
    }

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

## A seven-line pressure trace with real state behind every number

Use a 102-byte segment cap, four normal segment slots, a 236-byte data quota, and 416 reserved bytes. Every payload in this example has three bytes, so an encoded record occupies 35 bytes. A segment fits two records:

$$
32+2(32+3)=102.
$$

Four records occupy two files and charge 204 bytes. A fifth append requires a 67-byte reservation:

$$
204+67=271>236.
$$

There is no accepted fifth command, assigned offset, or hidden reservation. The rejection returns the `yak` payload. Group 0 checkpoints 4. Group 1 acknowledges offsets 0 and 1 but initially does not checkpoint, so `C_1=2` and `P_1=0`. Retention remains at floor 0.

After group 1 checkpoints 2, `[0,2)` becomes eligible. Deleting its 102-byte file leaves `L=102` and floor 2. Group 1's previously returned batch still owns the copied bytes for `cat`; those bytes remain readable without keeping the deleted file open.

After group 1 checkpoints 4, both groups have consumed the active `[2,4)` segment. Retention rotates an empty `[4,4)` active file, publishes floor 4, and removes the old file. `L=32`. Retrying the owned rejected payload appends offset 4, bringing `L` to 67. The offset is not reset by deletion.

## Build a complete ordinary demonstration

Add `examples/retention_probe.rs`. The helper functions retain the admission/error boundary and require accepted acknowledgements for the returned tokens. Each owner uses explicit-only checkpoints and a manual clock, so an elapsed wall-clock delay cannot accidentally publish the frontier being tested. The fixture starts all four owners but drives partition 0.

```rust title="crates/broker-core/examples/retention_probe.rs"
use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use broker_core::budget::DiskQuota;
use broker_core::delivery::{LeaseConfig, LeaseToken};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::{CheckpointPolicy, ProgressAck};
use broker_core::mailbox::AdmissionError;
use broker_core::storage::recovery::{self, StorageSession};
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::GroupId;
use broker_core::worker::{BatchConfig, Client, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}
fn accept(client: &Client, token: LeaseToken) -> Result<(), Box<dyn Error>> {
    match request(client.try_ack_progress(token.group, token))? {
        ProgressAck::Accepted(_) => Ok(()),
        other => Err(io::Error::other(format!("unexpected acknowledgement: {other:?}")).into()),
    }
}
fn owners(session: StorageSession) -> Result<Vec<WorkerHandle>, Box<dyn Error>> {
    let (epoch, logs) = session.into_parts();
    let mut result = Vec::new();
    for log in logs {
        result.push(WorkerHandle::start_with_progress(log, FileSync,
            BatchConfig::new(1, 4096, Duration::from_secs(1))?, 8, 8,
            LeaseConfig::new(epoch, 8, 10_000)?, Arc::new(ManualClock::new(0)),
            CheckpointPolicy::explicit_only())?);
    }
    Ok(result)
}
```

The main function requires an absent directory. It checks ledger values before printing them, exercises volatile-versus-persisted progress, retains an owned response across deletion, retries the rejected producer payload, and joins every owner before recovering under a fresh epoch.

```rust title="crates/broker-core/examples/retention_probe.rs"
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 2 { return Err(io::Error::other("usage: retention_probe ABSENT_DIRECTORY").into()); }
    let root = Path::new(&args[1]);
    let running = owners(recovery::initialize_budgeted(root, StorageConfig::new(102, 2, 4)?,
        DiskQuota::new(236, 416)?)?)?;
    let client = running[0].client();
    for payload in [b"cat", b"dog", b"owl", b"fox"] { request(client.try_publish(payload.to_vec()))?; }
    let refusal = client.try_publish(b"yak".to_vec()).unwrap_err();
    assert_eq!(refusal.reason, AdmissionError::DiskFull);
    let disk = client.disk_state().unwrap();
    assert_eq!((disk.file_bytes, disk.reserved_bytes, disk.quota.data_bytes()), (204, 0, 236));
    println!("full: file_bytes={} reserved_bytes={} data_limit={}",
        disk.file_bytes, disk.reserved_bytes, disk.quota.data_bytes());
    let g0 = GroupId::new(0)?; let g1 = GroupId::new(1)?;
    let first = request(client.try_lease(g0, ReadLimits::new(4, 140)?))?;
    for record in &first.records { accept(&client, record.token)?; }
    request(client.try_checkpoint(g0))?;
    let other = request(client.try_lease(g1, ReadLimits::new(4, 140)?))?;
    for record in &other.records[..2] { accept(&client, record.token)?; }
    let blocked = request(client.try_retention())?;
    assert_eq!((blocked.floor.get(), blocked.removed_segments, blocked.rotated), (0, 0, false));
    let progress = request(client.try_progress(g1))?;
    assert_eq!((progress.volatile_next.get(), progress.persisted_next.get()), (2, 0));
    println!("group1: volatile=2 persisted=0; floor=0 removed=0");
    request(client.try_checkpoint(g1))?;
    let reclaimed = request(client.try_retention())?;
    assert_eq!((reclaimed.floor.get(), reclaimed.removed_bytes, reclaimed.disk.file_bytes), (2, 102, 102));
    assert!(!reclaimed.rotated);
    println!("checkpointed: floor=2 removed_bytes=102 file_bytes=102 active_preserved=true");
    assert_eq!(first.records[0].record.payload(), b"cat");
    println!("copied response after deletion: cat");
    for record in &other.records[2..] { accept(&client, record.token)?; }
    request(client.try_checkpoint(g1))?;
    let reclaimed = request(client.try_retention())?;
    assert!(reclaimed.rotated);
    assert_eq!((reclaimed.floor.get(), reclaimed.disk.file_bytes), (4, 32));
    println!("caught up: rotated=true floor=4 file_bytes=32");
    assert_eq!(request(client.try_publish(refusal.value))?.get(), 4);
    println!("producer resumed: offset=4");
    for owner in running { owner.join()?; } drop(client);
    let session = recovery::recover(root)?;
    let (epoch, logs) = session.into_parts();
    assert_eq!(logs[0].retained_floor().get(), 4);
    assert_eq!(logs[0].written_end().get(), 5);
    assert_eq!(logs[0].disk_state().unwrap().file_bytes, 67);
    println!("restart: epoch={} floor=4 written_end=5 file_bytes=67", epoch.get());
    drop(logs);
    Ok(())
}
```

From the workspace root, use a new child of a temporary parent:

```sh
parent="$(mktemp -d)"
cargo run -p broker-core --example retention_probe -- "$parent/broker"
```

Expected program output, excluding Cargo's build messages:

```text
full: file_bytes=204 reserved_bytes=0 data_limit=236
group1: volatile=2 persisted=0; floor=0 removed=0
checkpointed: floor=2 removed_bytes=102 file_bytes=102 active_preserved=true
copied response after deletion: cat
caught up: rotated=true floor=4 file_bytes=32
producer resumed: offset=4
restart: epoch=2 floor=4 written_end=5 file_bytes=67
```

These are expected values derived from the implementation, not an authoring-environment run. Keep the directory when a check fails. Do not repair an assertion by deleting authority or moving the floor by hand. The restart's end 5 includes offsets 0–4 in its history, although only offset 4 is still retained. Its next append remains 5.

## Backpressure does not license data loss

A permanently stalled group can keep its persisted frontier behind the first segment boundary and eventually stop producer admission. That is the chosen retention contract, not a defect to hide by deleting its required input. There is no time-based expiry of a group, administrative skip, or forced-drop API in this chapter.

The control lane remains separately bounded. In the demonstrated pressure case it is available, so acknowledgements, checkpoint publication, and retention can run while data admission is full. If a caller floods that lane with fetches, it can still prevent new acknowledgements from entering until slots drain. Chapter 15 must solve that separate admission problem; merely adding more disk reserve would not help.

A retention request is also not a barrier over another lane or over every producer thread. Receive the necessary checkpoint successes before expecting their persisted fronts to influence a later maintenance request. Commands admitted concurrently can be interleaved according to the established two-lane policy. The owner always derives a safe floor from the state it actually processes, even when a caller's expected progress has not arrived yet.

Continue to Part 4 for real filesystem failure cuts, a process-kill harness, and complete tests for queued reservations and restart authority.

