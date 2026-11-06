# Chapter 12 — Lease expiration, retries, and stale-token fencing
## Part 2 — Clocked reads and owner integration

> **Learning objectives**
> Separate monotonic lease time from wall-clock labels and storage commit timers. Copy a bounded response before installing attempts. Integrate token-validated commands without leaving a tokenless completion bypass.

This is the second part of the same **4.5-hour chapter**, not another implementation estimate. Start with [Part 1](https://25d-228.github.io/rust-learning/chapter-12.html). Keep all named Chapter 11 and Chapter 10 prerequisite code. [Part 3](https://25d-228.github.io/rust-learning/chapter-12-part-03.html) completes the source with tests and a runnable demonstration.

## One clock domain per live session

A lease needs elapsed-time comparison, not a human date. Use a `Clock` trait returning integer milliseconds from a process-local origin. The owner requires nondecreasing observations; it never compares these values to a calendar timestamp or a client's clock.

The real adapter measures an [`Instant`](https://doc.rust-lang.org/std/time/struct.Instant.html) relative to its own origin. Rust documents platform caveats: an instant is not a guarantee of a steady rate, and whether system suspension counts is not specified uniformly. Do not turn the lease into a wall-clock deadline or promise identical suspend behavior across macOS versions. The adapter checks reversed ordering rather than silently converting it into zero elapsed time.

The manual adapter is deliberately small: one integer behind a mutex, shared by the test and owner. Advancing it does not sleep and does not wake an expiry thread, because this implementation has no expiry thread. A following owner command observes the new value.

```rust title="crates/broker-core/src/delivery/clock.rs"
use std::error::Error;
use std::fmt;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockError {
    Regressed,
    Exhausted,
    Poisoned,
}

impl fmt::Display for ClockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Regressed => write!(f, "lease clock moved backwards"),
            Self::Exhausted => write!(f, "lease clock milliseconds cannot be represented"),
            Self::Poisoned => write!(f, "manual clock mutex is poisoned"),
        }
    }
}
impl Error for ClockError {}

// Milliseconds in one process-local domain, never a Unix timestamp.
// Successful observations must be nondecreasing.
pub trait Clock: Send + Sync {
    fn now(&self) -> Result<u64, ClockError>;
}

```

`Clock: Send + Sync` makes its implementers suitable for shared ownership across the test thread and partition owners. An `Arc<dyn Clock>` later stores any compatible implementation behind a single field. `dyn` gives runtime method dispatch through a trait object; it does not dynamically compile the clock. Rust's [trait-object reference](https://doc.rust-lang.org/reference/types/trait-object.html) describes this pointer-and-dispatch model.

The real implementation converts [`Duration::as_millis`](https://doc.rust-lang.org/std/time/struct.Duration.html#method.as_millis), which returns whole milliseconds as `u128`, to `u64` with a checked conversion. No overflowing cast can turn a very large elapsed duration into a small apparently recent time.

```rust title="crates/broker-core/src/delivery/clock.rs"
pub struct MonotonicClock {
    origin: Instant,
}

impl MonotonicClock {
    pub fn new() -> Self { Self { origin: Instant::now() } }
}
impl Default for MonotonicClock {
    fn default() -> Self { Self::new() }
}
impl Clock for MonotonicClock {
    fn now(&self) -> Result<u64, ClockError> {
        let elapsed = Instant::now().checked_duration_since(self.origin)
            .ok_or(ClockError::Regressed)?;
        u64::try_from(elapsed.as_millis()).map_err(|_| ClockError::Exhausted)
    }
}

```

Whole-millisecond conversion makes the comparison precise in integer ticks, not in submillisecond wall time. For example, an actual elapsed value of 100.9 milliseconds is represented as 100; adding a ten-tick lease gives deadline 110. The exact real duration left need not be a full ten milliseconds. Use an operationally sensible lease duration and measure processing delays later; the ten-tick fixtures are small arithmetic examples, not a timeout recommendation.

The manual clock computes a checked successor before writing it back. Overflow leaves the old value intact. Its guard is held only for this short operation and is never retained across I/O or suspension. On [mutex poisoning](https://doc.rust-lang.org/std/sync/struct.Mutex.html#poisoning), this test adapter reports an error rather than pretending the guarded state is known to be healthy.

```rust title="crates/broker-core/src/delivery/clock.rs"
pub struct ManualClock {
    millis: Mutex<u64>,
}

impl ManualClock {
    pub fn new(millis: u64) -> Self { Self { millis: Mutex::new(millis) } }

    pub fn advance(&self, by: u64) -> Result<u64, ClockError> {
        let mut millis = self.millis.lock().map_err(|_| ClockError::Poisoned)?;
        let next = millis.checked_add(by).ok_or(ClockError::Exhausted)?;
        *millis = next;
        Ok(next)
    }
}
impl Clock for ManualClock {
    fn now(&self) -> Result<u64, ClockError> {
        Ok(*self.millis.lock().map_err(|_| ClockError::Poisoned)?)
    }
}
```

Chapter 9's synchronization timer remains an `Instant` deadline managed by the mailbox. Advancing `ManualClock` changes lease decisions only; it does not force a producer batch to synchronize. The new fixtures use record-count-triggered commits to avoid confusing those independent clocks.

## Expose validated limits, not unchecked configuration fields

The lease reader may fetch a retry and then a nonadjacent new offset in the same response. It must know how much count and byte allowance remains. In `crates/broker-core/src/storage/segment.rs`, replace only the existing `impl ReadLimits` block with this complete block. Keep its struct, derives, and every other segment routine unchanged.

```rust title="crates/broker-core/src/storage/segment.rs"
impl ReadLimits {
    pub fn new(records: usize, bytes: usize) -> Result<Self, StorageError> {
        if !(1..=1024).contains(&records) || !(1..=4 * 1024 * 1024).contains(&bytes) {
            return Err(StorageError::Config("read limits: 1..=1024 records, 1..=4194304 bytes"));
        }
        Ok(Self { records, bytes })
    }

    pub fn records(self) -> usize { self.records }
    pub fn bytes(self) -> usize { self.bytes }
}
```

The two getters copy a previously validated `ReadLimits`; they do not permit clients to construct zero or excessive limits. A response still charges complete encoded record bytes, including each record's 32-byte header. It does **not** yet charge token metadata, vector capacities, transport frames, or buffers retained by callers.

## Pair each copied record with the attempt that authorizes it

Create `crates/broker-core/src/delivery/leased_groups.rs`. A `LeasedRecord` owns the existing `OwnedRecord` and a copied token. There are no parallel public token and record vectors that a caller must keep aligned. The single paired value moves through the reply channel.

`LeaseStop::NoEligible` means the selected window had no further eligible work at the selection sample. It may be waiting for a head to expire, have no visible new records, or have no free slots. It is not a promise that the next poll will also be empty. `RecordLimit` means the selection count limit was reached; it does not assert that an additional record definitely exists. `ByteLimit.required` is the full encoded size of the next selected record, not the number of additional bytes missing.

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

`LeaseReadError` is an internal composition point. An invalid clock observation or unrepresentable deadline is an ordinary lease refusal before installation. A storage error follows the existing terminal owner policy, because an unexpected read failure may leave storage poisoned. Keeping these variants distinct prevents a retryable malformed request from looking like disk corruption.

Each owner contains exactly two independent leased windows. The shared clock does not share their progress. `Arc` retains ownership of the clock as long as the owner needs it; cloning that handle does not duplicate the clock's time state. See the [shared ownership and thread-safety explanation](https://doc.rust-lang.org/std/sync/struct.Arc.html#thread-safety) in the Rust documentation.

```rust title="crates/broker-core/src/delivery/leased_groups.rs"
pub(crate) struct LeasedGroups {
    partition: PartitionId,
    windows: [LeaseWindow; GROUP_COUNT as usize],
    clock: Arc<dyn Clock>,
    last_now: Option<u64>,
}
impl LeasedGroups {
    pub(crate) fn new(partition: PartitionId, config: LeaseConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            partition,
            windows: std::array::from_fn(|index| LeaseWindow::new(partition,
                GroupId::new(index as u8).expect("fixed group array"), Offset::new(0), config)),
            clock, last_now: None,
        }
    }

```

## Sample time at the owner, and refuse regression

The real adapter checks its origin relationship, but the owner also compares every successful sample with its previous sample. This catches a custom adapter returning an earlier value and protects the sequential state machine from time moving backwards. [`Option::is_some_and`](https://doc.rust-lang.org/std/option/enum.Option.html#method.is_some_and) is false for the first observation and applies its predicate only when a previous value exists.

An acknowledgement uses processing time: the sample is taken when the owner handles its command, not when a client claims to have finished or when it enqueued the request. Waiting in the control queue therefore consumes lease time. Retaining a client's enqueue timestamp as authority would let an arbitrarily delayed command accept expired work.

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

An invalid acknowledgement does not change slot status, attempts, frontiers, or scheduler position. The monotonic clock observation watermark can still advance. This small piece of observation state is not delivery progress, and it must not be rewound to make a later backwards clock sample appear valid.

If the clock reports an error, acknowledgement processing returns a lease error without entering the window. A regressed sample does not replace the saved watermark. A persistently broken clock keeps refusing lease operations; it does not justify inventing time or resetting generation counters.

## Read a candidate prefix before installing any of it

There are two clock samples on a nonempty successful delivery path. The first determines which old attempts are eligible to retry. The second occurs after copying the selected records and establishes the new deadlines. This avoids consuming the entire lease on our own synchronous disk reads. No other owner command runs between the samples.

The plan can contain nonadjacent offsets, so read one complete record per selection using the existing checksum-validating storage path. Sum the actual encoded lengths. Stop at the first selected record that does not fit; install only the prefix actually copied. If any read fails unexpectedly, drop the partial response and install nothing.

When the byte remainder is zero, `ReadLimits` cannot represent zero bytes. A one-byte probe is sufficient: every encoded record is at least 32 bytes, so none can accidentally fit, while the reader can still validate the candidate header and report its actual required size. This is not a one-byte payload read or an exception to the response budget.

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

The second `sample()` can fail after reads succeeded. Those copied buffers then drop normally, but no attempt has been installed. Likewise, deadline overflow is detected before installing any prefix. The storage read cursor may have moved; Chapter 8's absolute positioning makes that harmless for future valid operations. A failed storage read itself is different and terminates the owner.

After installation, `zip` consumes the record and token vectors together. The explicit length assertion guards their internal one-to-one relationship. Moving an `OwnedRecord` into a pair does not clone its encoded payload. The final paired vector can allocate descriptor storage, and allocation failure has normal Rust allocator behavior; the course's comprehensive byte admission comes later.

For the three-byte payloads `cat` and `dog`, each encoded record costs $32+3=35$ bytes. With a 35-byte limit, one candidate fits; the next reports `required: 35`, even though zero bytes remain. With a 34-byte limit, no candidate fits and no token changes. With two empty payloads and a 32-byte limit, one header-only record fits: empty payload does not mean free delivery.

### Bounded does not mean costless

Let $R$ be the requested count, at most 1,024; let $W$ be the configured window width, at most 4,096; and let $s$ be the existing sparse-index stride. The retry scan visits at most $W$ positions and the plan contains at most $R$ selections. Each independent single-record lookup can validate up to $s-1$ predecessors plus its selected record. Thus a conservative bound on fully validated bodies in this straightforward path is:

$$
\text{validated bodies} \le R s.
$$

With four selected records and stride 2, the bound is eight bodies, not necessarily four. A byte-refused candidate can still require predecessor validation before its header is inspected. The response byte cap does not bound all disk work. Grouping adjacent selections into ranges may reduce this cost later, but must preserve selection order, byte refusal, and attempt installation semantics.

The state is bounded by width; planning and response descriptors are bounded by count; returned encoded bytes are bounded by the supplied byte limit. They are different bounds. They exclude existing storage indexes, transient record scratch, allocator overhead, and copies callers retain from previous replies. A maximum of one window is not a process-wide memory budget.

## Register the new modules

Replace `crates/broker-core/src/delivery/mod.rs` with the following. The earlier trusted group implementation remains for its existing exercises. The worker selects **one** mode; it never keeps both trusted and leased windows active for the same partition owner.

```rust title="crates/broker-core/src/delivery/mod.rs"
mod groups;
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

## The owner must not expose an acknowledgement bypass

Retaining `try_complete(group, offset)` unchanged on the new leased path would defeat all token checking. Removing it outright would unnecessarily break the earlier standalone learning programs. Instead, explicitly separate three owner modes: storage-only, trusted Chapter 11 delivery, and leased delivery.

`start_with_delivery` remains a trusted teaching mode. `start_with_leases` selects the token-validated mode used from this chapter onward. A leased owner refuses both the old delivery command and tokenless completion. A trusted owner refuses lease commands. Diagnostic `try_fetch` remains a raw storage observation and never issues group slots; it must not be exposed as an alternate consumer-group delivery endpoint later.

Replace `crates/broker-core/src/worker.rs` by concatenating the following listings. Keeping the full resulting file makes the integration reproducible; the append, synchronization, mailbox, and joining rules from Chapter 9 remain intact. The changed pieces are the request variants, mode selection, and lease dispatch. No storage format or commit-order rule changes.

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
use crate::delivery::window::WindowError;
use crate::mailbox::{AdmissionError, Event, Mailbox, Rejected};
use crate::storage::error::StorageError;
use crate::storage::log::PartitionLog;
use crate::storage::segment::{ReadBatch, ReadLimits};
use crate::storage::sync::SyncIo;
use crate::types::{GroupId, Offset, PartitionId};

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

The batch configuration and producer frontiers above are unchanged. A lease does not make an unsynchronized record visible. The owner still reads against `visible_end`, which advances only after all required synchronization succeeds.

Add `RequestError::Lease` alongside the earlier concrete window error. Both preserve their underlying causes and keep the owner available for unrelated valid commands. `DeliveryDisabled` now also means that the requested delivery API does not match this owner's mode.

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

A `Ticket` still receives one `Result`. `Reply` still owns the unique sending permission and completes unfinished requests with `OwnerStopped` on ordinary destruction. Installation is not rolled back when the receiving ticket disappears. The following unchanged reply mechanism applies equally to records, lease batches, and acknowledgement results.

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

The two new controls are `Lease` and `Acknowledge`. They carry typed group identities plus validated read limits or a copied token. They use the same bounded control mailbox as group snapshots and flushes. Terminal storage failure must complete every new reply variant, not accidentally leave one waiting indefinitely.

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
    Lease { group: GroupId, limits: ReadLimits, reply: Reply<LeaseBatch> },
    Acknowledge { group: GroupId, token: LeaseToken, reply: Reply<Ack> },
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
        }
    }
}

```

Keep all original client methods and mark the tokenless path's limited scope explicitly. Enqueueing a command is not the same as receiving a successful owner result. The returned ticket separates admission from processing.

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

```

`try_lease` requests copied records with fresh tokens; `try_ack` submits a token for validation in the explicitly selected group. Keeping the group parameter separate from the token's group lets the owner reject a token presented to the wrong group instead of silently rerouting it.

```rust title="crates/broker-core/src/worker.rs"
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

    // Flushes data ALREADY processed by this owner, not data still in the other lane.
    pub fn try_flush(&self) -> Result<Ticket<WorkerState>, AdmissionError> {
        let (reply, ticket) = Reply::pair();
        self.mailbox.try_control(Control::Flush(reply)).map_err(|e| e.reason)?;
        Ok(ticket)
    }
}

```

The setup enum owns any shared clock handle until it is moved into the chosen mode. This is why the worker does not need a second generic type parameter for the clock. The production synchronization adapter remains generic; the clock is a narrow trait object selected at construction. There is no mutable global clock and no public access to either live group's window.

```rust title="crates/broker-core/src/worker.rs"
enum DeliverySetup {
    Trusted(DeliveryConfig),
    Leased { config: LeaseConfig, clock: Arc<dyn Clock> },
}

enum DeliveryMode {
    Disabled,
    Trusted(Groups),
    Leased(LeasedGroups),
}

impl DeliveryMode {
    fn new(partition: PartitionId, setup: Option<DeliverySetup>) -> Self {
        match setup {
            None => Self::Disabled,
            Some(DeliverySetup::Trusted(config)) => Self::Trusted(Groups::new(partition, config)),
            Some(DeliverySetup::Leased { config, clock }) => {
                Self::Leased(LeasedGroups::new(partition, config, clock))
            }
        }
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
}

```

The synchronized commit function is unchanged. It finishes producer tickets only after the storage operation returns success. An acknowledgement handled between publishes cannot advance this producer visibility frontier; it changes a different state machine.

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

```

Appending still preflights size and successor arithmetic, commits a previous full byte group when necessary, writes the record, drops the input payload, and retains only pending reply metadata. Lease time is not consulted here. A manually advanced lease clock cannot accidentally acknowledge a producer batch.

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

The first control branches preserve Chapter 11 behavior only inside `DeliveryMode::Trusted`. `GroupState` can observe either live group implementation. The following fragment opens `control` and its `match`; the next two fragments continue and close them.

```rust title="crates/broker-core/src/worker.rs"
    fn control(&mut self, command: Control) -> Result<(), Arc<StorageError>> {
        match command {
            Control::Snapshot(reply) => reply.finish(Ok(self.state())),
            Control::GroupState { group, reply } => {
                let result = match &self.delivery {
                    DeliveryMode::Trusted(groups) => Ok(groups.state(group)),
                    DeliveryMode::Leased(groups) => Ok(groups.state(group)),
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

In leased mode, acknowledgement samples time and validates the token before changing completion state. Lease reads distinguish a nonterminal clock/configuration refusal from terminal storage failure. On a terminal failure, finish the current reply with the same shared cause that the owner will use when failing queued work.

```rust title="crates/broker-core/src/worker.rs"
            Control::Acknowledge { group, token, reply } => {
                let result = match &mut self.delivery {
                    DeliveryMode::Leased(groups) => groups.acknowledge(group, token).map_err(RequestError::Lease),
                    _ => Err(RequestError::DeliveryDisabled),
                };
                reply.finish(result);
            }
            Control::Lease { group, limits, reply } => {
                let DeliveryMode::Leased(groups) = &mut self.delivery else {
                    reply.finish(Err(RequestError::DeliveryDisabled));
                    return Ok(());
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

Flush and diagnostic reads retain their prior behavior. Flush is still a processed-prefix operation rather than a fence over the separate producer lane. Raw reads do not create tokens. The completed `control` method returns normally after ordinary lease refusals; it does not clear windows or start a replacement owner.

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

The run loop has no lease timer branch. Retrying requires a lease request; rejecting an expired acknowledgement requires only processing that acknowledgement. Storage group-commit deadlines remain the mailbox's only timed event here. Closure drains accepted work; terminal failure aborts queues and resolves pending replies; destruction prevents silent detachment of unfinished requests.

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

impl<S: SyncIo> Drop for Worker<S> {
    fn drop(&mut self) {
        // On unwinding, queued and pending Reply values deliver OwnerStopped.
        // abort moves the queued values out before their destructors run.
        drop(self.mailbox.abort());
    }
}

```

The worker's error and join types remain unchanged. A clock refusal is a request error, not an unexplained panic. A storage error or panic still stops the owner and must be observed through its handle.

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

Pass the epoch obtained from the same recovered `StorageSession` as the partition log. The constructor does not authenticate a manually fabricated configuration. Its lifecycle precondition is that an owner is not recreated with reset lease state under an epoch already exposed to clients. In the later broker supervisor, loss of an owner must stop the session and go through epoch-advancing recovery before resuming delivery.

For actual elapsed time, create an `Arc::new(MonotonicClock::new())`; tests pass an `Arc<ManualClock>` or the scripted clock in Part 3. An `Arc<ManualClock>` can be coerced to the `Arc<dyn Clock>` parameter while the test retains another concrete handle for `advance`. No borrowed stack clock must outlive its creator.

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
        Self::start_inner(log, sync, config, data_slots, control_slots, Some(DeliverySetup::Trusted(delivery)))
    }

    pub fn start_with_leases<S: SyncIo + Send + 'static>(log: PartitionLog, sync: S,
        config: BatchConfig, data_slots: usize, control_slots: usize,
        leases: LeaseConfig, clock: Arc<dyn Clock>) -> Result<Self, WorkerError>
    {
        Self::start_inner(log, sync, config, data_slots, control_slots,
            Some(DeliverySetup::Leased { config: leases, clock }))
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
        let delivery = DeliveryMode::new(log.partition(), delivery);
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

Initial storage synchronization still runs before the owner is exposed. The mode contains the clock and group windows before the thread is spawned. Failed spawning drops that owned state normally. Closing and joining an owner remains explicit, and dropping a handle still closes and joins rather than leaving a detached thread with storage ownership.

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

## Follow ownership across the reply boundary

A successful lease request goes through these owners: the log reader creates encoded vectors; the lease handler holds those vectors while reading the whole response prefix; installation creates copied tokens and updates private window metadata; pairing moves each vector into a `LeasedRecord`; the reply channel takes the batch. Window metadata contains no borrow into those vectors.

If the response receiver is already gone, the attempted send drops the batch. Its record buffers are released, but the installed attempts remain outstanding. A later poll after their deadlines can recover the work under fresh generations. Undoing issuance on a failed send would require different semantics and would not address replies that were delivered but later abandoned.

Dropping a token does not cancel an attempt. Dropping a payload does not acknowledge it. Expiration does not destroy an external consumer's payload or undo its side effect. These independent lifetimes are the reason the broker must tolerate duplicate processing.

| Failure or refusal | Delivery state | Owner behavior |
| --- | --- | --- |
| Wrong scope, stale generation, altered deadline, or expired acknowledgement | No slot, frontier, attempt, or scheduler changes | Return classification |
| First candidate does not fit the byte allowance | No installed prefix | Return empty byte-limited batch |
| Later candidate does not fit | Install only the copied prefix | Return that bounded prefix |
| Clock error, regression, or deadline overflow | No new attempt installation | Return lease error; diagnostic operations remain possible |
| Unexpected storage read failure | No part of this response installed | Fail the owner; do not return a partial success |
| Receiver disappears after installation | Issued attempts remain | Buffers can drop; work becomes retryable on expiry |
| Broker restarts | Volatile attempts disappear | New epoch fences old tokens; progress currently starts at zero |

The control lane is still a shared bounded queue. Acknowledgement admission can be refused when that queue is full, and a long synchronous operation can delay processing beyond a deadline. Alternating producer/control service is not reserved acknowledgement capacity or a real-time guarantee. Pressure-safe control admission and complete byte accounting remain separate mechanisms later in the course.

The next part makes the exact equality boundary, rotating retry cursor, no-mutation refusals, and restart collisions executable without sleeping to advance lease time.
