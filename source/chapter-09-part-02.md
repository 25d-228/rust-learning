# Chapter 9, Part 2 — Owned commands and synchronized publication

A client should not need to borrow a partition's mutable log while its request is in flight. It sends owned input and receives an independently owned reply endpoint. The partition thread then makes every ordering decision: when to assign an offset, when to write, when to synchronize, and when a fetch may include the record.

**Prerequisites:** the complete Chapter 8 workspace and [Chapter 9, Part 1](https://25d-228.github.io/rust-learning/chapter-09.html), including the dirty-file synchronization extension and timed mailbox. This is the second part of one **6.0-hour** chapter, not another estimate. All code in this part creates the single new file `crates/broker-core/src/worker.rs`; append its listings in order. The complete driver and tests follow in [Part 3](https://25d-228.github.io/rust-learning/chapter-09-part-03.html).

> **Learning objectives**
> Design move-based commands and one-shot reply ownership; group already-written records under three limits; publish only a synchronized prefix; distinguish refusal from uncertainty; and separate ordinary storage errors from thread panics when joining.

## Limit the group, not merely the queue

Let $N$ be the maximum number of records in a synchronization group, $B$ its maximum sum of encoded-record bytes, and $\Delta$ the maximum batching delay before the owner attempts a commit. Each pending record contributes $r_i=32+p_i$ bytes, where $p_i$ is its payload length. Thus a pending group must satisfy:

$$
n \le N, \qquad b=\sum_{i=0}^{n-1}(32+p_i) \le B.
$$

The segment headers are excluded from this byte counter but are still covered by dirty-file synchronization. The counter controls group size; it is not total memory use or a filesystem allocation measurement.

A record must fit the batch byte cap by itself. Otherwise endlessly flushing an empty group would not help it fit. The constructor validates the limits, while each publish request separately checks its actual encoded length. The delay may be at most 60 seconds in this local API; the demonstration uses five milliseconds, and controlled tests use a long delay to keep unrelated timers out of their interleavings.

```rust title="crates/broker-core/src/worker.rs"
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::codec::HEADER_LEN;
use crate::mailbox::{AdmissionError, Event, Mailbox, Rejected};
use crate::storage::error::StorageError;
use crate::storage::log::PartitionLog;
use crate::storage::segment::{ReadBatch, ReadLimits};
use crate::storage::sync::SyncIo;
use crate::types::Offset;

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

`WorkerState` is a copied observation, not shared mutable state. The owner builds it between commands or when returning a failure. `sync_groups` counts successful nonempty runtime groups, excluding startup synchronization. It saturates instead of overflowing; it is a diagnostic counter, never an offset or correctness condition.

`BatchConfig`'s fields are private. Callers cannot bypass its constructor and install a zero record limit or a byte cap that cannot hold even an empty record header.

## Refusal and uncertainty need different error names

There are three materially different outcomes after admission. A preflight refusal did not assign this publish an offset. A storage failure can follow completed writes and does not promise rollback. An owner can also unwind before it explicitly finishes the reply. Keep those distinctions in the error type rather than returning an undifferentiated string.

```rust title="crates/broker-core/src/worker.rs"
#[derive(Debug, Clone)]
pub enum RequestError {
    // Refused by the owner before assigning this request an offset.
    Refused(Arc<StorageError>),
    // Accepted work may have reached storage. This is NOT rollback.
    Failed(Arc<StorageError>),
    // Includes ordinary unwinding; the caller cannot infer a disk outcome.
    OwnerStopped,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(_) => write!(f, "request refused before mutation"),
            Self::Failed(_) => write!(f, "partition failed; accepted work has an uncertain outcome"),
            Self::OwnerStopped => write!(f, "partition stopped without completing this reply"),
        }
    }
}

impl Error for RequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self { Self::Refused(e) | Self::Failed(e) => Some(e.as_ref()), _ => None }
    }
}

```

One storage error may need to reach every pending producer, queued control caller, and the joining supervisor. `io::Error` is not made clonable by pretending to copy its text. Instead, each error wrapper owns an `Arc` pointing to the same immutable `StorageError`. `Error::source` borrows that cause so the diagnostic can retain its operation, location, and underlying I/O failure.

`Refused` also represents a read-range refusal: no storage mutation is attempted and the owner remains healthy. By contrast, an unexpected read error stops the owner, because a previously trusted file or index is no longer safe to serve from its cached state.

## A reply has one owner and one slot

Use a standard channel for the reply primitive, not for the broker's scheduling policy. A `Ticket<T>` is the receiving endpoint. The private `Reply<T>` owns the only sending endpoint, never clones it, and can complete it once.

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

[`SyncSender::try_send`](https://doc.rust-lang.org/std/sync/mpsc/struct.SyncSender.html#method.try_send) does not block for buffer space. The channel has capacity one, and this wrapper has one sender with at most one send. A caller that does not read its reply therefore cannot block the partition owner. If that caller has dropped its ticket, completion simply releases the undeliverable result.

The `Option` allows `finish` to take the sender out even though `Reply` implements `Drop`. When the wrapper is subsequently dropped, it has no sender left and cannot send twice. If ordinary unwinding destroys an unfinished wrapper, `Drop` sends `OwnerStopped` instead. This uses Rust's [destruction rules](https://doc.rust-lang.org/reference/destructors.html); it does not run after process termination or when unwinding is disabled by an aborting panic strategy.

The reply is an observation of processing, not its cancellation handle. Once a publish has entered the mailbox, dropping the ticket does not remove the command or undo any later write. Chapter 6 demonstrated the same ownership distinction asynchronously; this implementation keeps it explicit on dedicated threads.

## Route owned payloads and bounded reads

A publish owns a `Vec<u8>`. A fetch owns only its small request parameters and reply endpoint. `ReadLimits` is the validated Chapter 8 type, so the owner cannot be asked through this API for an unlimited result.

```rust title="crates/broker-core/src/worker.rs"
struct Publish {
    payload: Vec<u8>,
    reply: Reply<Offset>,
}

enum Control {
    Snapshot(Reply<WorkerState>),
    Fetch { start: Offset, limits: ReadLimits, reply: Reply<ReadBatch> },
    Flush(Reply<WorkerState>),
}

impl Control {
    fn fail(self, cause: Arc<StorageError>) {
        match self {
            Self::Snapshot(reply) | Self::Flush(reply) => reply.finish(Err(RequestError::Failed(cause))),
            Self::Fetch { reply, .. } => reply.finish(Err(RequestError::Failed(cause))),
        }
    }
}

```

`Control::fail` consumes the command and completes whichever reply type it owns. The two state-reply variants share one pattern arm because their inner types are identical. Fetch has a different result type and therefore a separate arm.

Clients are clonable mailbox endpoints. They do not clone the worker handle or gain permission to join the owner. Every `try_*` method either enqueues one whole command or returns an admission failure immediately.

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

    // Flushes data ALREADY processed by this owner, not data still in the other lane.
    pub fn try_flush(&self) -> Result<Ticket<WorkerState>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Flush(reply)).map_err(|e| e.reason)?;
        Ok(ticket)
    }
}

```

`try_publish` returns the original payload on `Full` or `Closed`. Its temporary reply channel is then dropped; no accepted command exists. A successful admission instead returns a ticket whose later `Result` describes processing. These are two distinct error layers:

```text
try_publish(payload)
  Err(Rejected { reason, value })   => never admitted; caller regains payload
  Ok(ticket)
      ticket receives Ok(offset)   => required synchronization succeeded
      ticket receives Err(...)     => refusal or uncertain processing failure
      caller drops ticket          => admitted work still belongs to owner
```

The `map_err` expression moves the rejected command's payload out of `Publish`. `Publish` itself has no custom destructor, so moving that field is allowed; its remaining `Reply` field is then destroyed. No payload clone is needed.

An explicit `Flush` is **not a cross-lane barrier**. If a data command and a flush are both queued before the next receive, control preference may let the flush run first. Its meaning is “commit the records already processed by this owner.” The tests observe written progress before relying on a flush. A future protocol requiring a fence after a particular publish must name a target or use the publish's receipt, not infer ordering from separate queue admission times.

## The owner retains only reply metadata after writing

The owner has exclusive mutable access to the log and its synchronization adapter. Pending entries store offsets and reply wrappers, not copies of the original payloads. This is group commit of separate record writes, not yet vectored I/O or concatenation into one large write buffer.

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

`pending_bytes` is the sum of encoded lengths for that pending reply group. It is not the capacity of `pending`, the payload queue, or the returned read buffers. `deadline` is absent when no group exists. Keeping it as `Option<Instant>` makes the empty-state distinction explicit rather than inventing a sentinel timestamp.

The worker does not need to implement `Sync`: its mutable state is not shared. Moving it into a thread will require `Send` for the synchronization adapter. In the tests, that adapter contains a channel receiver that can move to one owner without needing concurrent access from multiple owners.

## Put the visibility advance after the entire synchronization loop

The complete commit routine is small enough to inspect as one unit:

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

There is exactly one publication point: the assignment to `visible_end`, after `PartitionLog::synchronize` returns `Ok`. The log method has already visited every dirty segment needed by the written prefix. A failed sync returns before visibility, the successful-group counter, and every success reply.

The owner clears the pending byte count and deadline, then drains the pending entries in append order. `Vec::drain` retains the vector's allocation for later groups. Each entry transfers its reply endpoint into `finish`; a dropped ticket changes neither the visible end nor the assigned offset sequence.

The one-slot channels can receive these successes one at a time; producers may observe them in a different scheduling order. The contract is per-partition assignment and visibility, not a global schedule for client threads.

## Admit, flush if necessary, assign, and write

The following complete method applies all preflight checks before assigning this request. A record that would exceed the remaining group byte capacity causes the previous group to commit first. An exact count or byte limit commits immediately after the current write.

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

Follow the order carefully. `check_append` verifies codec/segment fit, successor arithmetic, and whether a required rotation would exceed the segment count. The batch byte cap is a second policy check. Both refusal paths finish only this command and return `Ok(())` from the handler: the owner should continue, and an earlier pending group still has its original deadline.

If the current record does not fit the remaining group budget, `commit` runs before this command's offset is assigned. A failure there returns an uncertain failure for this accepted command as well as stopping the owner. It need not have reached storage itself; `Failed` means that accepted work lacks a successful outcome, not that every such record was written.

For an empty group, [`Instant::checked_add`](https://doc.rust-lang.org/std/time/struct.Instant.html#method.checked_add) establishes the deadline without an overflowing time addition. A representational refusal leaves the record unassigned. When a group already exists, its deadline is **not reset**. The delay starts just before its first append, not when the producer originally entered the queue.

The checked successor advances `assigned_end` before the synchronous append. Since this thread is the only log owner, the preflight result cannot be invalidated by another appender. Nevertheless, the file operation can fail; in that case the assigned frontier may exceed the fully written frontier in the joined report.

After a successful append, `drop(command.payload)` promptly releases the original owned payload. The encoder's temporary record vector has already been released inside the storage layer. Only the offset and reply remain pending. This does not yet provide admission-time byte accounting: caller-owned vectors can have capacities much larger than their lengths, and accepted commands can wait in the data queue before validation.

### Calculate a byte boundary rather than guessing it

Set $N=8$ and $B=71$ bytes. The records `cat`, `dog`, and `bird` have encoded sizes 35, 35, and 36. The first two leave $b=70$. The third tests the remaining capacity:

$$
36 > 71-70 = 1.
$$

The owner commits offsets 0 and 1, then assigns and writes `bird` at offset 2. It now has $V=2$, $W=A=3$, one pending reply, and 36 pending bytes. A later deadline, flush, or closure commits that group. The third record is not split across groups or inserted before the old group's synchronization finishes.

With $B=70$, the first two records instead trigger an immediate exact-byte commit; no third record is necessary. With $N=2$ and a much larger byte limit, the same two records trigger the count limit. Part 3 tests these independently so one trigger cannot accidentally mask a broken alternative.

### Calculate the time boundary separately

If the first append begins at an illustrative time of 100 milliseconds and the delay is 10 milliseconds, the group's deadline is 110 milliseconds. A second append at 106 milliseconds keeps that deadline. A control request at 109 milliseconds also leaves it unchanged. Once the owner returns to the receive loop at or after 110 milliseconds, the deadline event wins over ready queues.

This does not promise a reply by 110 milliseconds. The thread may be descheduled, a prior command may still be running, and synchronization itself takes time. The implementation bounds the deliberate batching interval when the owner is able to service its loop, not storage or scheduler latency. `Condvar` timing is best effort, as described in its [timed-wait documentation](https://doc.rust-lang.org/std/sync/struct.Condvar.html#method.wait_timeout).

## Let fetches see only the committed prefix

A snapshot is a copy of owner state. A flush invokes the same commit path. A fetch supplies the owner's synchronized end as `upper`, never the log's written end.

```rust title="crates/broker-core/src/worker.rs"
    fn control(&mut self, command: Control) -> Result<(), Arc<StorageError>> {
        match command {
            Control::Snapshot(reply) => reply.finish(Ok(self.state())),
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

A fetch beginning exactly at the visible end returns an empty batch even when more complete bytes have been written. A fetch beginning beyond it is refused as an invalid range. Both outcomes are consistent: the caller cannot request unsynchronized records simply because it guessed their offsets.

`error @ StorageError::Range { .. }` binds the whole error while matching its variant. It allows that one known preflight error to retain its details without poisoning the owner. Any other read error follows the fatal path, including corruption discovered in a supposedly valid stored record. No partial read batch escapes through that `Err`.

All three control operations execute on the owner thread. While that thread is inside a blocking synchronization call, controls can enter their bounded lane but cannot execute. This intentional limitation makes the visibility proof simple and must not be misreported as an always-responsive control plane. Other partition owners can continue independently.

## End the loop by draining, or stop it on failure

The receive loop has only four events. Graceful closure becomes `Closed` after both queues drain, then commits the last partial group. A timeout uses the same commit routine; no second implementation of publication is hidden in a timer callback.

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

`run` returns a shared storage cause on a fatal operation. The thread entry point will capture the failure state, call `fail`, and return a structured error to its joiner. `fail` closes admission before moving queued values out, then completes pending and queued replies outside the mailbox lock. Pending payloads have already been released; queued payloads are dropped as their commands are consumed.

Ordinary unwinding is a different path. The worker's destructor closes and empties its mailbox. The pending reply wrappers are subsequently destroyed with its fields, completing their fallback errors:

```rust title="crates/broker-core/src/worker.rs"
impl<S: SyncIo> Drop for Worker<S> {
    fn drop(&mut self) {
        // On unwinding, queued and pending Reply values deliver OwnerStopped.
        // abort moves the queued values out before their destructors run.
        drop(self.mailbox.abort());
    }
}

```

This destructor does not try to commit during unwinding. It does not reuse a possibly inconsistent log, and it cannot return an error. The thread's join result is therefore the place to observe a panic. The directory guard still remains owned until the log's segment handles have been destroyed.

## Keep setup, storage failure, and panic distinct when joining

The public owner handle needs an error for failed initialization, failed thread creation, a returned storage failure, or a panic. A returned storage failure includes the state captured **before** terminal cleanup resets the pending fields.

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

The handle owns exactly one join permission. Cloning a `Client` does not duplicate that permission. The standard [`JoinHandle`](https://doc.rust-lang.org/std/thread/struct.JoinHandle.html) detaches its thread when dropped, so wrapping one without a lifecycle policy would quietly undermine our ownership design.

```rust title="crates/broker-core/src/worker.rs"
pub struct WorkerHandle {
    client: Client,
    thread: Option<JoinHandle<Result<WorkerState, WorkerError>>>,
}

fn join_thread(handle: JoinHandle<Result<WorkerState, WorkerError>>) -> Result<WorkerState, WorkerError> {
    match handle.join() { Ok(result) => result, Err(_) => Err(WorkerError::Panicked) }
}

```

`join_thread` distinguishes the thread's returned `Result` from the outer result of joining. `Ok(Err(WorkerError::Failed { ... }))` is an ordinary reported failure; an outer `Err` means the owner panicked. This version reports that category but does not downcast or republish an arbitrary panic payload.

Construction validates the mailbox before touching synchronization. It synchronizes the initial complete log on the caller, then transfers the log and adapter to a named thread. No `Client` is returned before this initial boundary succeeds.

```rust title="crates/broker-core/src/worker.rs"
impl WorkerHandle {
    pub fn start<S: SyncIo + Send + 'static>(mut log: PartitionLog, mut sync: S, config: BatchConfig,
        data_slots: usize, control_slots: usize) -> Result<Self, WorkerError>
    {
        let mailbox = Arc::new(Mailbox::new(data_slots, control_slots)
            .map_err(|e| WorkerError::Setup(StorageError::Config(e)))?);
        // Strict reopening alone does not establish a synchronized visible prefix.
        // Initialization runs on the caller; no client exists until it succeeds.
        let visible_end = log.synchronize(&mut sync).map_err(WorkerError::Setup)?;
        let name = format!("partition-{}", log.partition().get());
        let mut worker = Worker {
            log, sync, mailbox: Arc::clone(&mailbox), config,
            assigned_end: visible_end, visible_end,
            pending: Vec::with_capacity(config.records), pending_bytes: 0, deadline: None, sync_groups: 0,
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

Strict reopening in Chapter 8 proved that records were complete and contiguous, not that this owner had successfully synchronized them. The initial call closes that gap for an already-valid log. It is not crash-tail recovery and cannot turn an incomplete directory into a valid broker. The owner starts with all three ends equal to this synchronized prefix; `sync_groups` starts at zero because these were not new producer groups.

The [`thread::Builder::spawn`](https://doc.rust-lang.org/std/thread/struct.Builder.html#method.spawn) result makes thread-creation failure reportable. `Send + 'static` is required on the moved adapter, not `Sync`: no thread shares mutable access to it. A startup synchronization failure returns before spawning, and a spawn failure drops the captured worker rather than returning a client for a missing owner.

`worker.run()` borrows the worker mutably. When it returns an error, that borrow ends, so the entry point can copy its state and fail outstanding replies before returning `WorkerError::Failed`. The log itself is never moved back into service after such an error.

```rust title="crates/broker-core/src/worker.rs"
    pub fn client(&self) -> Client { self.client.clone() }
    pub fn close(&self) { self.client.mailbox.close(); }

    pub fn join(mut self) -> Result<WorkerState, WorkerError> {
        self.close();
        join_thread(self.thread.take().expect("join permission owned once"))
    }
}

```

Calling `join` first closes admission, so a caller cannot accidentally wait forever for an open idle owner simply by forgetting a separate `close`. `Option::take` moves out the join permission once; the handle's subsequent destructor sees `None`.

The fallback below also joins during ordinary scope cleanup. It reports an otherwise unobserved failure to standard error rather than silently discarding it. Normal application code should still call `join` explicitly to inspect the structured result.

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

Closing all four handles before joining any gives every partition a chance to drain. A caller should collect every join result rather than return early on the first failure and forget the remaining owners. Part 3's driver does this explicitly; the destructor is only its cleanup fallback.

There is no safe general mechanism here to interrupt a stuck synchronous file operation. A close request or batching deadline does not abort `sync_all`. Consequently, explicit join or destructor-based join may block while storage is stalled. Later supervision will coordinate admission, tasks, and shutdown reporting; it must not pretend that dropping a thread handle stopped the underlying thread.

## Review an uncertain failure as a state transition

Suppose a four-record group spans segment bases 0 and 2. Synchronization of base 0 succeeds. Synchronization of base 2 fails. The runtime sequence is:

```text
assigned=4, written=4, visible=0, pending=[0,1,2,3]
sync(base=0) -> Ok
sync(base=2) -> Err
no visible advance
no success replies
close admission; fail pending and queued requests
join -> Failed { assigned=4, written=4, visible=0, cause }
```

Some bytes may already be persistent. That uncertainty is exactly why the error path stops this owner rather than rewriting a counter and retrying the same append. A future recovery scans the stored bytes; it cannot reconstruct which producer read which reply.

For contrast, after an earlier successful group advanced the visible end to 2, failure of a later group leaves the reported visible end at 2. It does not revoke earlier success. These two cases require separate tests, because checking only an empty initial prefix can miss a mistaken reset-to-zero failure path.

Continue with [Part 3 — Controlled failures and four-partition checks](https://25d-228.github.io/rust-learning/chapter-09-part-03.html). It constructs those exact interleavings, runs a four-partition workload, and distinguishes expected local results from authoring-environment checks.
