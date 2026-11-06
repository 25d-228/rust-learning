# Chapter 15 — Byte budgets and pressure-safe admission
## Part 3 — Asynchronous admission and an owned response

**Implementation time:** shared with Part 1's 5.0-hour total. **Prerequisite:** Parts 1–2, including the complete updated worker. [Part 4](https://25d-228.github.io/rust-learning/chapter-15-part-04.html) supplies the deterministic pressure and cancellation checks.

The asynchronous facade reserves resources before a network body would be read. There is no TCP server yet; the executable fills the same prepared body through a mutable slice. Chapter 16 will use that slice directly for bounded input rather than allocate an unaccounted frame vector.

## Separate preparation from handoff

Preparation owns a reserved mailbox slot and one pool buffer. Append preparation additionally owns the existing disk-growth reservation. Submission consumes all three into the owner command. The caller receives only a one-shot ticket, not the command's permit.

For a request length $x$, the successful append-admission state transition reserves one queue slot, one whole buffer with capacity $c$, and $x+64$ logical disk bytes. The three checks are not one global storage transaction; they are a short, serialized admission attempt with rollback of unexposed reservations. No asynchronous wait occurs inside that attempt.

Failure before handoff means no owner operation was accepted. Failure or abandonment after handoff can leave a real append or installed lease. Resource cleanup and operation rollback are different questions.

Create `src/worker/bounded.rs`. `MemoryState` contains per-pool snapshots, not an atomic snapshot of all resources. Each component respects its own bound even if another operation runs between observations. The scratch field records startup capacity and remains a conservative allowance after the owner exits; it is not a live RSS measurement.

```rust title="crates/broker-core/src/worker/bounded.rs"
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
```

`PreparedAppend::payload_mut` exposes only the requested prefix of a fixed allocation. The caller cannot grow it. Dropping a prepared request returns its slot, buffer, and disk credit; submitting it consumes that ownership. The same structure applies to a prepared lease, without a disk permit.

A one-shot receiver is itself awaitable. The sender runs synchronously on the owner; the caller does not call a blocking `recv` inside the runtime. Closing a receiver and dropping it are not interchangeable ownership events: a sent value may remain stored until the receiver is dropped ([receiver lifecycle](https://docs.rs/tokio/latest/tokio/sync/oneshot/struct.Receiver.html)). Use ordinary scope/drop or an explicit timeout that owns the receive future, rather than parking completed response tickets indefinitely.

## A short transaction across independent ledgers

The admission mutex serializes **only facade admission attempts**. Slot reservation takes and releases the mailbox mutex, buffer checkout takes and releases the pool mutex, and disk reservation takes and releases its ledger mutex. The outer mutex is dropped when this ordinary function returns. The owner never needs it to drain a queue or complete a control.

If no buffer is available, cancel the temporary slot quietly. Another facade attempt cannot observe that transient reservation because it must first acquire the outer admission mutex. A returned prepared request is different: it is visible ownership, and its destructor notifies normally. This distinction prevents self-wake busy loops without losing an enabling transition.

Disk refusal is returned immediately after rolling back the other resources. The async adapter does not wait for a stalled consumer to checkpoint forever. The caller receives `DiskFull` and may make a deliberate bounded retry after observed progress.

```rust title="crates/broker-core/src/worker/bounded.rs"
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
```

## Register the notification before testing the predicate

The ordering is register, test, await, and recheck—not test, then register. Create a `Notified` future and pin it before trying admission. `enable()` registers it explicitly. Releases use `notify_waiters`, waking current waiters without accumulating an unbounded event history; the predicate remains the actual source of truth ([Notify semantics](https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html)).

Consider a response return between the failed predicate check and `.await`: the already registered future observes that notification. On wake, another caller may have taken the buffer first, so the loop repeats. There is no claim of FIFO allocation fairness between waiters. Their number is bounded, and each has an absolute deadline.

Only `Full` and `MemoryFull` wait. Impossible shapes, disk pressure, closure, and exhausted waiter capacity are explicit refusals. The waiter guard is released both on a normal return and when the caller drops the pending future.

```rust title="crates/broker-core/src/worker/bounded.rs"
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
```

The response loop uses a separate waiter gate but the same release notification. A notification can be irrelevant to this request class; that is a harmless recheck, not permission to bypass its pool. No slot or payload is retained by a pending loop.

The deadline is created once by the caller. Each iteration checks it before attempting admission; the wait uses the same absolute time. This avoids resetting a full timeout after each unrelated wake. The explicit precheck matters because `timeout_at` can accept an immediately ready future even when its deadline has passed ([absolute timeout behavior](https://docs.rs/tokio/latest/tokio/time/fn.timeout_at.html)). These are cooperative scheduling checks, not a hard real-time guarantee or a body-read deadline. Chapter 16 must separately time a caller that has obtained resources and then stops sending.

```rust title="crates/broker-core/src/worker/bounded.rs"
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
```

## Keep small progress controls off the query lane

Acknowledgement, checkpoint, and retention use `try_urgent`. They acquire no payload buffer, append slot, or disk append permit. Snapshot and progress inspection remain ordinary queries. A full query lane can therefore reject a diagnostic call while an acknowledgement is admitted.

The control lane is still finite. The transport must bound its callers and report `Full` rather than silently append to an unbounded retry list. Progress also depends on valid lease tokens, healthy storage, and owner scheduling; independent capacity is necessary, not sufficient for arbitrary liveness.

```rust title="crates/broker-core/src/worker/bounded.rs"
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
```

## Expose the facade only after all capacities are installed

`BoundedHandle::start` accepts already initialized or recovered storage. It constructs the actual mailbox, gives its notification handle to both pools, allocates fixed scratch, verifies the reported capacity, and starts the durable owner. It then returns a facade that hides the raw `Client`.

The lane array is `[data, ordinary_query, urgent_control]`. It limits queued plus reserved requests for the first two lanes and queued controls for the third. The memory shape separately limits payload-bearing requests; neither is inferred from the other. Pool ceilings are independent per partition, so a hot partition cannot borrow a neighbor's idle buffers.

```rust title="crates/broker-core/src/worker/bounded.rs"
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
    // Blocking join: use only outside an async serving task, or via a bounded supervisor.
    pub fn join(self) -> Result<WorkerState, WorkerError> { self.owner.join() }
}
```

## Follow cancellation at each ownership boundary

| Cancellation point | What is dropped | What is not undone |
| --- | --- | --- |
| Future never polled | No acquired resource | Nothing was admitted |
| Waiting for a slot or buffer | Waiter guard and notification registration | Another caller's reservation |
| Prepared body exists | Slot, buffer, disk permit | No command was yet submitted |
| Submitted append, ticket abandoned | Receiver only | Queued/written append and its disk outcome |
| Submitted fetch, ticket abandoned | Receiver; returned batch is later dropped on failed send | Attempt installation and lease duration |
| Caller holds a completed batch | Batch return owner when dropped | Acknowledgement, checkpoint, or retention already executed |

Dropping a `Pin<&mut F>` borrow does not necessarily destroy the future owner; Chapter 6's distinction still applies. Cancellation tests below own their futures in `Box::pin` and drop those owners. A server timeout should likewise own the operation being abandoned or explicitly manage the remaining owner.

Holding a reply is permitted, but it consumes a response buffer. The design prefers bounded refusal to copying the data elsewhere and pretending the response budget was released. A slow consumer can prevent further fetches from its partition while small acknowledgements/checkpoints remain admissible.

## A pressure trace with exact small numbers

The demonstration uses two 64-byte append buffers, two 128-byte response buffers, data capacity two, query capacity one, and urgent capacity two. Owner scratch remains 4 MiB. With ordinary allocator-reported capacities equal to the requested sizes, payload allocations total

$$
2(64)+2(128)+4{,}194{,}304=4{,}194{,}688\text{ bytes}.
$$

Only the fixed payload allocation is being calculated; this is not measured process memory. The executable prints actual reported capacities rather than assuming them. The default four-owner configuration instead totals 64 MiB.

Publish `cat` and `dog`. Group 0 fetches the two 35-byte encoded records into one response buffer: 70 bytes used, 128 bytes checked out. Hold a prepared group-1 fetch, taking the second response buffer and the only query slot. Hold two prepared three-byte appends, taking both append buffers/data reservations and `2 × 67 = 134` disk-reservation bytes.

At that point all four payload buffers are held. A progress query is refused because its lane is full. Nevertheless group 0 acknowledges both records and checkpoints next offset 2 through the urgent lane. The held group-1 request has not been submitted, so it has installed no attempt.

Drop the unsubmitted bodies, fetch for group 1, acknowledge and checkpoint it, and retain. The old record file is now safely removed, while group 0's held reply still reads `cat`. Finally drop both batches. Checked-out payload charges become zero, but the preallocated pools remain allocated for reuse.

Create `examples/memory_pressure_probe.rs`. The helpers unwrap expected demonstration outcomes with descriptive watchdogs; library admission still returns explicit errors. The new directory is a required argument so the example never guesses a data location or removes an existing broker.

```rust title="crates/broker-core/examples/memory_pressure_probe.rs"
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broker_core::budget::MemoryConfig;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::GroupId;
use broker_core::worker::{AsyncTicket, BatchConfig, BoundedClient, BoundedHandle};

fn g(n: u8) -> GroupId { GroupId::new(n).unwrap() }
async fn reply<T>(ticket: AsyncTicket<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), ticket).await.expect("reply watchdog")
        .expect("owner reply channel").expect("owner operation")
}
async fn exercise(client: &BoundedClient) {
    for bytes in [b"cat", b"dog"] {
        let mut request = client.try_prepare_append(bytes.len()).unwrap();
        request.payload_mut().copy_from_slice(bytes);
        reply(request.submit().unwrap()).await;
    }
```

The first two producer replies establish synchronized records. The next statements reserve but do not submit query/append work. That distinction makes saturation deterministic: the owner cannot drain those prepared values while the test examines the control path.

```rust title="crates/broker-core/examples/memory_pressure_probe.rs"
    let batch = reply(client.try_prepare_lease(g(0), ReadLimits::new(2, 70).unwrap()).unwrap()
        .submit().unwrap()).await;
    let held_query = client.try_prepare_lease(g(1), ReadLimits::new(2, 70).unwrap()).unwrap();
    let held_append0 = client.try_prepare_append(3).unwrap();
    let held_append1 = client.try_prepare_append(3).unwrap();
    let state = client.memory_state();
    assert_eq!(state.append.free_buffers, 0); assert_eq!(state.response.free_buffers, 0);
    assert!(matches!(client.try_progress(g(0)), Err(AdmissionError::Full)));
    println!("SATURATED append_buffers=2 response_buffers=2 query_slots=1 disk_reserved={}",
        client.disk_state().reserved_bytes);
    for record in batch.records() { reply(client.try_ack_progress(g(0), record.token).unwrap()).await; }
    let checkpoint = reply(client.try_checkpoint(g(0)).unwrap()).await;
    println!("CONTROL persisted_next={} payload_buffers_still_held=4", checkpoint.progress.persisted_next.get());
    drop((held_append0, held_append1, held_query));
    let other = reply(client.try_prepare_lease(g(1), ReadLimits::new(2, 70).unwrap()).unwrap()
        .submit().unwrap()).await;
    for record in other.records() { reply(client.try_ack_progress(g(1), record.token).unwrap()).await; }
    reply(client.try_checkpoint(g(1)).unwrap()).await;
    let retained = reply(client.try_retention().unwrap()).await;
    assert_eq!(batch.records().next().unwrap().payload(), b"cat");
    println!("RETAINED floor={} held_reply_first=cat", retained.floor.get());
    drop((batch, other));
    let state = client.memory_state();
    println!("RELEASED append_checked_out={} response_checked_out={} disk_reserved={}",
        state.append.checked_out_bytes, state.response.checked_out_bytes, client.disk_state().reserved_bytes);
    println!("ALLOCATED append={} response={} scratch={} (allocator-reported capacities)",
        state.append.allocated_bytes, state.response.allocated_bytes, state.scratch_bytes);
}
```

`main` initializes one demonstration owner and runs the asynchronous exercise on a current-thread runtime. The owner itself remains the dedicated storage thread. `join` is invoked after `block_on` returns, so there is no synchronous join inside a serving future. Other partition logs are deliberately dropped; their initialized metadata remains on disk for later valid recovery.

```rust title="crates/broker-core/examples/memory_pressure_probe.rs"
fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: memory_pressure_probe NEW_DIRECTORY")?);
    if args.next().is_some() { return Err("usage: memory_pressure_probe NEW_DIRECTORY".into()); }
    let (epoch, mut logs) = recovery::initialize(&root, StorageConfig::new(102, 2, 4)?)?.into_parts();
    let log = logs.remove(0); drop(logs);
    let owner = BoundedHandle::start(log, FileSync,
        BatchConfig::new(1, 4096, Duration::from_secs(1))?, [2, 1, 2],
        LeaseConfig::new(epoch, 8, 10000)?, Arc::new(ManualClock::new(0)),
        CheckpointPolicy::explicit_only(), MemoryConfig::new(64, 2, 128, 2, 2)?)?;
    let client = owner.client();
    let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
    runtime.block_on(exercise(&client));
    let state = owner.join()?;
    println!("JOINED visible_end={}", state.visible_end.get());
    Ok(())
}
```

Run from the reconstructed workspace root:

```sh
cargo run -p broker-core --example memory_pressure_probe -- /tmp/partition-broker-memory-demo
```

Use a fresh directory name for another run. Do not delete a directory that contains data you intend to keep. Expected output for the requested small capacities is:

```text
SATURATED append_buffers=2 response_buffers=2 query_slots=1 disk_reserved=134
CONTROL persisted_next=2 payload_buffers_still_held=4
RETAINED floor=2 held_reply_first=cat
RELEASED append_checked_out=0 response_checked_out=0 disk_reserved=0
ALLOCATED append=128 response=256 scratch=4194304 (allocator-reported capacities)
JOINED visible_end=2
```

The `ALLOCATED` line may show larger append/response capacities when the allocator reports more, provided constructor ceilings still hold. The other lines express state-machine results, not throughput measurements. This Rust executable has **not been run in the authoring environment**; the output is an expected local check.

## A lifetime error that protects the accounting boundary

This deliberately failing fragment belongs in a temporary experiment, not the reference executable:

```rust
let record = batch.records().next().unwrap();
drop(batch);
println!("{}", record.payload().len());
```

The record borrows the response allocation. Dropping its batch would return that allocation for reuse, so later access must be rejected. The complete repair is to finish using the view before dropping the batch:

```rust
{
    let record = batch.records().next().unwrap();
    println!("{}", record.payload().len());
}
drop(batch);
```

Do not automatically “repair” this by copying every response into an unbudgeted vector. That would change the memory model. The eventual network writer should borrow each encoded/payload slice while the batch remains alive, then release the batch when output finishes or the connection is abandoned.
