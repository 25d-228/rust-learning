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

    fn finish(&mut self) -> Result<WorkerState, Arc<StorageError>> {
        // recv returned Closed only after all three committed queues became empty.
        self.commit()?;
        if self.mailbox.checkpoint_requested() {
            if let DeliveryMode::Durable { progress, .. } = &mut self.delivery {
                for g in 0..GROUP_COUNT {
                    progress.checkpoint(GroupId::new(g).expect("fixed groups"),
                        self.visible_end, &mut self.sync).map_err(Arc::new)?;
                }
            }
        }
        Ok(self.state())
    }

    fn run(&mut self) -> Result<WorkerState, Arc<StorageError>> {
        loop {
            let progress_deadline = match &self.delivery {
                DeliveryMode::Durable { progress, .. } => progress.deadline(),
                _ => None,
            };
            let deadline = self.deadline.into_iter().chain(progress_deadline).min();
            let event = self.mailbox.recv(deadline); // Queue/idle wait is outside service.
            let started = self.sync.observes_service().then(Instant::now);
            use crate::storage::sync::OwnerActivity;
            let kind = match &event {
                Event::Data(_) => OwnerActivity::Append,
                Event::Control(_) => OwnerActivity::Control,
                Event::Deadline => OwnerActivity::Deadline,
                Event::Closed => OwnerActivity::Finish,
            };
            let result: Result<Option<WorkerState>, Arc<StorageError>> = match event {
                Event::Data(command) => self.publish(command).map(|()| None),
                Event::Control(command) => self.control(command).map(|()| None),
                Event::Deadline => (|| {
                    if self.deadline.is_some_and(|end| end <= Instant::now()) { self.commit()?; }
                    self.checkpoint_due(Instant::now())?;
                    Ok(None)
                })(),
                Event::Closed => self.finish().map(Some),
            };
            if let Some(started) = started {
                self.sync.service_observed(kind,
                    Instant::now().checked_duration_since(started), result.is_err());
            }
            if let Some(state) = result? { return Ok(state); }
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
    exited: ExitWatch,
}

fn join_thread(handle: JoinHandle<Result<WorkerState, WorkerError>>) -> Result<WorkerState, WorkerError> {
    match handle.join() { Ok(result) => result, Err(_) => Err(WorkerError::Panicked) }
}

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
        let (notice, exited) = tokio::sync::watch::channel(None);
        let closing = Arc::clone(&mailbox);
        let thread = thread::Builder::new().name(name).spawn(move || {
            // The moved Worker is destroyed inside this boundary on return or unwinding.
            // We report a panic; we never continue using a possibly damaged worker.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                match worker.run() {
                    Ok(state) => Ok(state),
                    Err(cause) => {
                        let state = worker.state();
                        worker.fail(Arc::clone(&cause));
                        Err(WorkerError::Failed { state, cause })
                    }
                }
            })).unwrap_or(Err(WorkerError::Panicked));
            let exit = match &result {
                Ok(state) => OwnerExit::Stopped { state: *state,
                    checkpointed: closing.checkpoint_requested() },
                Err(WorkerError::Failed { state, cause }) =>
                    OwnerExit::Failed { state: *state, cause: Arc::clone(cause) },
                Err(WorkerError::Panicked) => OwnerExit::Panicked,
                Err(WorkerError::Setup(_) | WorkerError::Spawn(_)) =>
                    unreachable!("setup errors occur before the thread starts"),
            };
            notice.send_replace(Some(exit));
            result
        }).map_err(WorkerError::Spawn)?;
        Ok(Self { client: Client { mailbox, disk }, thread: Some(thread), exited })
    }

    pub fn client(&self) -> Client { self.client.clone() }
    pub fn close(&self) { self.client.mailbox.close(); }
    pub fn exits(&self) -> ExitWatch { self.exited.clone() }
    pub(crate) fn finish(&self) -> bool { self.client.mailbox.finish() }

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

mod lifecycle;
pub use lifecycle::{ExitWatch, OwnerExit};

mod bounded;
pub use bounded::{AsyncTicket, BoundedClient, BoundedHandle, MemoryState, PreparedAppend, PreparedLease};
