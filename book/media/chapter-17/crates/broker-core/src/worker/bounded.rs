use std::sync::Mutex;
use tokio::sync::{Notify, oneshot};
use tokio::time::{Instant as AsyncInstant, timeout_at};

use super::*;
use crate::budget::memory::{BufferPool, WaitGate, allocate_bytes};
use crate::budget::{APPEND_BYTES, MemoryConfig, OWNER_SCRATCH_BYTES, PoolState, RESPONSE_BYTES};
use crate::codec::MAX_PAYLOAD;
use crate::mailbox::Slot;

pub type AsyncTicket<T> = oneshot::Receiver<Result<T, RequestError>>;

#[derive(Debug, Clone, Copy)]
pub struct MemoryState {
    pub append: PoolState,
    pub response: PoolState,
    // Startup capacity; after owner exit this remains a conservative allowance, not live RSS.
    pub scratch_bytes: usize,
    pub append_waiters: usize,
    pub response_waiters: usize,
}
#[derive(Clone)]
pub struct BoundedClient {
    client: Client,
    append: BufferPool,
    response: BufferPool,
    admission: Arc<Mutex<()>>,
    changed: Arc<Notify>,
    append_waiters: WaitGate,
    response_waiters: WaitGate,
    scratch_bytes: usize,
}

pub struct PreparedAppend {
    slot: Slot<Publish, Control>,
    buffer: PoolBuffer,
    disk: Option<DiskPermit>,
}
impl PreparedAppend {
    pub fn payload_mut(&mut self) -> &mut [u8] { self.buffer.as_mut_slice() }
    pub fn submit(self) -> Result<AsyncTicket<Offset>, AdmissionError> {
        let (reply, ticket) = Reply::async_pair();
        self.slot.send_data(Publish { payload: Payload::Pooled(self.buffer), reply, disk: self.disk })
            .map_err(|e| e.reason)?;
        Ok(ticket)
    }
}
pub struct PreparedLease {
    slot: Slot<Publish, Control>,
    buffer: PoolBuffer,
    group: GroupId,
    limits: ReadLimits,
}
impl PreparedLease {
    pub fn submit(self) -> Result<AsyncTicket<PooledLeaseBatch>, AdmissionError> {
        let (reply, ticket) = Reply::async_pair();
        self.slot.send_query(Control::PooledLease { group: self.group, limits: self.limits,
            buffer: self.buffer, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }
}

impl BoundedClient {
    pub fn memory_state(&self) -> MemoryState {
        MemoryState { append: self.append.state(), response: self.response.state(),
            scratch_bytes: self.scratch_bytes, append_waiters: self.append_waiters.used(),
            response_waiters: self.response_waiters.used() }
    }
    pub fn disk_state(&self) -> DiskState { self.client.disk_state().expect("bounded owner has disk admission") }

    pub fn try_prepare_append(&self, len: usize) -> Result<PreparedAppend, AdmissionError> {
        if len > MAX_PAYLOAD { return Err(AdmissionError::InvalidRecord); }
        if len > self.append.state().block_bytes { return Err(AdmissionError::TooLarge); }
        // Serializes only bounded admission attempts, never the owner or an await.
        let _transaction = self.admission.lock().expect("admission mutex poisoned");
        let slot = self.client.mailbox.reserve_data()?;
        let buffer = match self.append.take(len) {
            Ok(buffer) => buffer,
            Err(error) => { slot.cancel_quietly(); return Err(error); }
        };
        let encoded = codec::record_len(len).map_err(|_| AdmissionError::InvalidRecord)?;
        let disk = self.client.disk.as_ref().expect("bounded owner has disk admission")
            .reserve_append(encoded).map_err(|error| match error {
                DiskRefusal::Full => AdmissionError::DiskFull,
                DiskRefusal::Stopped => AdmissionError::Closed,
            })?;
        Ok(PreparedAppend { slot, buffer, disk: Some(disk) })
    }

    pub fn try_prepare_lease(&self, group: GroupId, limits: ReadLimits)
        -> Result<PreparedLease, AdmissionError>
    {
        if limits.bytes() > self.response.state().block_bytes { return Err(AdmissionError::TooLarge); }
        let _transaction = self.admission.lock().expect("admission mutex poisoned");
        let slot = self.client.mailbox.reserve_query()?;
        let buffer = match self.response.take(limits.bytes()) {
            Ok(buffer) => buffer,
            Err(error) => { slot.cancel_quietly(); return Err(error); }
        };
        Ok(PreparedLease { slot, buffer, group, limits })
    }

    pub async fn prepare_append(&self, len: usize, deadline: AsyncInstant)
        -> Result<PreparedAppend, AdmissionError>
    {
        // Validate impossible sizes before joining even the bounded waiter set.
        if len > MAX_PAYLOAD { return Err(AdmissionError::InvalidRecord); }
        if len > self.append.state().block_bytes { return Err(AdmissionError::TooLarge); }
        let _waiter = self.append_waiters.enter()?;
        loop {
            if self.client.mailbox.is_closed() { return Err(AdmissionError::Closed); }
            if AsyncInstant::now() >= deadline { return Err(AdmissionError::Deadline); }
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_prepare_append(len) {
                Ok(prepared) => return Ok(prepared),
                Err(AdmissionError::Full | AdmissionError::MemoryFull) => {},
                Err(error) => return Err(error), // Disk pressure is explicit, not an indefinite wait.
            }
            if timeout_at(deadline, notified).await.is_err() { return Err(AdmissionError::Deadline); }
        }
    }

    pub async fn prepare_lease(&self, group: GroupId, limits: ReadLimits, deadline: AsyncInstant)
        -> Result<PreparedLease, AdmissionError>
    {
        if limits.bytes() > self.response.state().block_bytes { return Err(AdmissionError::TooLarge); }
        let _waiter = self.response_waiters.enter()?;
        loop {
            if self.client.mailbox.is_closed() { return Err(AdmissionError::Closed); }
            if AsyncInstant::now() >= deadline { return Err(AdmissionError::Deadline); }
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_prepare_lease(group, limits) {
                Ok(prepared) => return Ok(prepared),
                Err(AdmissionError::Full | AdmissionError::MemoryFull) => {},
                Err(error) => return Err(error),
            }
            if timeout_at(deadline, notified).await.is_err() { return Err(AdmissionError::Deadline); }
        }
    }

    pub fn try_ack_progress(&self, group: GroupId, token: LeaseToken)
        -> Result<AsyncTicket<ProgressAck>, AdmissionError>
    {
        let (reply, ticket) = Reply::async_pair();
        self.client.mailbox.try_urgent(Control::AckProgress { group, token, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }
    pub fn try_checkpoint(&self, group: GroupId) -> Result<AsyncTicket<Checkpointed>, AdmissionError> {
        let (reply, ticket) = Reply::async_pair();
        self.client.mailbox.try_urgent(Control::Checkpoint { group, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }
    pub fn try_retention(&self) -> Result<AsyncTicket<Retained>, AdmissionError> {
        let (reply, ticket) = Reply::async_pair();
        self.client.mailbox.try_urgent(Control::Retain(reply)).map_err(|e| e.reason)?;
        Ok(ticket)
    }
    pub fn try_progress(&self, group: GroupId) -> Result<AsyncTicket<ProgressState>, AdmissionError> {
        let (reply, ticket) = Reply::async_pair();
        self.client.mailbox.try_control(Control::Progress { group, reply }).map_err(|e| e.reason)?;
        Ok(ticket)
    }
    pub fn try_snapshot(&self) -> Result<AsyncTicket<WorkerState>, AdmissionError> {
        let (reply, ticket) = Reply::async_pair();
        self.client.mailbox.try_control(Control::Snapshot(reply)).map_err(|e| e.reason)?;
        Ok(ticket)
    }
}

pub struct BoundedHandle { owner: WorkerHandle, client: BoundedClient }
impl BoundedHandle {
    pub fn start<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S, batch: BatchConfig,
        lanes: [usize; 3], leases: LeaseConfig, clock: Arc<dyn Clock>, policy: CheckpointPolicy,
        memory: MemoryConfig) -> Result<Self, WorkerError>
    {
        if log.disk_admission().is_none() {
            return Err(WorkerError::Setup(StorageError::Config("bounded service requires initialized/recovered storage")));
        }
        let mailbox = Arc::new(Mailbox::with_urgent(lanes[0], lanes[1], lanes[2])
            .map_err(|e| WorkerError::Setup(StorageError::Config(e)))?);
        let changed = mailbox.notification();
        let append = BufferPool::new(memory.append_size, memory.append_count, APPEND_BYTES, Arc::clone(&changed))
            .map_err(WorkerError::Setup)?;
        let response = BufferPool::new(memory.response_size, memory.response_count, RESPONSE_BYTES, Arc::clone(&changed))
            .map_err(WorkerError::Setup)?;
        let scratch = allocate_bytes(OWNER_SCRATCH_BYTES).map_err(WorkerError::Setup)?;
        let scratch_bytes = scratch.capacity();
        if scratch_bytes > OWNER_SCRATCH_BYTES {
            return Err(WorkerError::Setup(StorageError::Config("allocator-reported scratch capacity exceeds its ceiling")));
        }
        let owner = WorkerHandle::start_inner(log, sync, batch, lanes[0], lanes[1],
            Some(DeliverySetup::Durable { config: leases, clock, policy }), Some((mailbox, scratch)))?;
        let client = BoundedClient { client: owner.client(), append, response,
            admission: Arc::new(Mutex::new(())), changed,
            append_waiters: WaitGate::new(memory.waiters), response_waiters: WaitGate::new(memory.waiters),
            scratch_bytes };
        Ok(Self { owner, client })
    }
    pub fn client(&self) -> BoundedClient { self.client.clone() }
    pub fn close(&self) { self.owner.close(); }
    pub fn begin_shutdown(&self) -> bool { self.owner.finish() }
    pub fn exits(&self) -> ExitWatch { self.owner.exits() }
    // Blocking join: use only outside an async serving task, or via a bounded supervisor.
    pub fn join(self) -> Result<WorkerState, WorkerError> { self.owner.join() }
}
