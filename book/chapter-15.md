# Chapter 15 — Byte budgets and pressure-safe admission
## Part 1 — Capacity-owned buffers and reserved lanes

> **Learning objectives**
> Account for allocated capacity rather than payload length. Reserve a mailbox slot before accepting a body. Move reusable buffers with commands and replies. Keep acknowledgement admission independent from append and fetch pressure.

**Implementation time:** 5.0 hours for all four parts: 3.5 for the pools, storage-copy path, mailbox integration, and asynchronous adapter; 1.5 for saturation, cancellation, and failure checks. Reading is separate. [Part 2 — Bounded storage copies and owner integration](https://25d-228.github.io/rust-learning/chapter-15-part-02.html), [Part 3 — Asynchronous admission and an owned response](https://25d-228.github.io/rust-learning/chapter-15-part-03.html), and [Part 4 — Pressure, cancellation, and executable checks](https://25d-228.github.io/rust-learning/chapter-15-part-04.html) complete this chapter.

**Prerequisite code:** the complete [Chapter 14](https://25d-228.github.io/rust-learning/chapter-14.html) workspace, including its disk ledger, retained-floor authority, and pressure tests. Chapter 6 supplies the future, pinning, and cancellation vocabulary. Do not change record bytes, checkpoint bytes, lease validation, or the meaning of synchronized visibility.

The **18-file overlay** is under `book/media/chapter-15/`. It adds **seven new files and replaces eleven**. New modules hold the memory pool, the bounded segment/log copy routines, pooled lease results, the asynchronous client, and the pressure tests; the executable is also new. The complete source files are supplied, but the text also explains every new operation. For small module additions, append exactly the labelled fragment to the named prerequisite file. Other files explicitly say “replace”; their labelled chunks concatenate in order. Earlier overlays remain immutable.

Use `BoundedHandle` and `BoundedClient` for the service built from this chapter onward. Older `WorkerHandle`/`Client` APIs remain for prerequisite demonstrations and regression tests; they are **not** covered by the payload-memory bound. The new handle exposes no raw client. An application cannot combine unrestricted raw clients with bounded clients and still claim the bound. There is no on-disk format migration in this chapter.

## A queue limit is not a memory limit

Suppose a queue holds eight `Vec<u8>` values. One producer gives it three-byte vectors with capacity three; another gives it three-byte vectors with capacity one mebibyte. The queue length is eight in both cases, but the allocated payload storage differs dramatically.

For one vector, let $x$ be its initialized payload length and $c$ its allocation capacity, both in bytes. For this broker's byte vectors, $0\le x\le c$. A record's encoded length is $r=32+x$. Neither $x$ nor $r$ necessarily equals $c$. Rust distinguishes vector length from capacity, and even an exact-reservation request can produce more capacity than requested; inspect the returned capacity instead of assuming equality ([vector guarantees and reservation](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.try_reserve_exact)).

Four other places can retain bytes after a queue pop: a producer still filling an accepted body, the owner encoding it, a completed fetch reply waiting for its receiver, and a consumer still using that reply. Moving a vector between these places changes its owner, not the allocation size. Releasing a byte permit at dequeue would undercount later ownership.

This chapter uses **fixed reusable buffers**, not a variable-sized allocator. It is a deliberate baseline: overcharge small messages, eliminate payload allocation during service, and make the ownership bound inspectable. It does not claim optimal packing or throughput.

## Derive the payload envelope

Each partition has an append pool, a response pool, and one owner scratch buffer. Defaults are eight append buffers of 1 MiB, two response buffers of 2 MiB, and 4 MiB of scratch. Here 1 MiB is $2^{20}$ bytes. Let $A_p$, $R_p$, and $S_p$ be the actual reported capacities of those three categories for partition $p$. Successful construction enforces

$$
A_p\le8\text{ MiB},\qquad R_p\le4\text{ MiB},\qquad S_p\le4\text{ MiB}.
$$

For four bounded owners, the payload allocation envelope is

$$
M_{\mathrm{payload}}=\sum_{p=0}^{3}(A_p+R_p+S_p)\le4(8+4+4)\text{ MiB}=64\text{ MiB}.
$$

All buffers, including currently free ones, count. A pool does not allocate more when it runs out; admission refuses or waits within a fixed waiter limit. With defaults, even a three-byte append checks out an entire 1 MiB allocation. Its logical disk reservation is still only `32 + 3 + 32 = 67` bytes. Memory and disk credits have different units of accounting and different release points.

This is **not a 64 MiB process-RSS guarantee**. It excludes allocator bookkeeping and physical rounding not reported by capacity, thread stacks, runtime/channel objects, sparse indexes, delivery metadata, and application-owned copies. Those must be bounded separately and measured as process resources. Recovery and construction happen before the serving profile exists. If an allocator reports excess capacity that violates a ceiling, construction drops the partial pool and refuses to start; no exact transient-startup allocation bound is claimed.

A resource set can outlive its worker. A cloned client keeps its pools alive, and a returned batch keeps its response pool alive; joining the worker does not free those allocations. The 64 MiB envelope therefore covers **one four-partition resource generation**, including its remaining handles and replies. Do not create replacement pools while retaining an old generation and still report the single-generation bound. The later supervisor must release those owners before replacement or end the process; transparent in-process pool replacement is not implemented here.

The response block size is also a request-shape limit. Default responses accept at most 2 MiB of encoded bytes per request, even though their pool totals 4 MiB. They never stitch multiple buffers together. One maximum record occupies `1,048,576 + 32 = 1,048,608` bytes. Two such records need `2,097,216`, which is 64 bytes more than a 2 MiB response buffer. The second record must remain unissued. Existing raw diagnostic APIs retain their older 4 MiB read limit; the bounded service uses the smaller configured response shape.

## Add asynchronous primitives to the core

Replace `crates/broker-core/Cargo.toml` with the following. Tokio supplies notifications, one-shot replies, a timer, and the example runtime. Pool policy, multi-resource admission, queue reservations, scheduling, and cleanup remain our code. The existing edition and minimum compiler declaration are retained for the features introduced earlier. A compatible maintained toolchain is sufficient; no new compiler feature is required by this manifest change.

```toml title="crates/broker-core/Cargo.toml"
[package]
name = "broker-core"
version = "0.1.0"
edition = "2024"
rust-version = "1.89"

[dependencies]
crc32fast = "1"
tokio = { version = "1", default-features = false, features = ["rt", "macros", "sync", "time"] }

[[bin]]
name = "directory-probe"
path = "src/bin/directory_probe.rs"
```

Append these declarations to `src/budget.rs`, after its existing disk-ledger code and test-module declaration. The disk implementation is unchanged. Keeping the memory mechanism in a child module prevents a single file from becoming a mixture of unrelated accounting rules.

```rust title="crates/broker-core/src/budget.rs"

pub(crate) mod memory;
pub use memory::{APPEND_BYTES, RESPONSE_BYTES, OWNER_SCRATCH_BYTES, MemoryConfig, PoolState};
```

## Make allocation failure a composed error

Replace `src/storage/error.rs`. The only new error case is `Allocation`: it retains a `TryReserveError` as its cause rather than reducing it to a misleading disk error. The remainder preserves the prerequisite variants, display text, and sources. Reported reservation failure and process-wide memory exhaustion are different events; this code handles the former. It cannot promise cleanup after process abort, forced termination, or an allocator that terminates instead of returning an error ([reservation error type](https://doc.rust-lang.org/std/collections/struct.TryReserveError.html)).

```rust title="crates/broker-core/src/storage/error.rs"
use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::codec::SegmentHeader;
use crate::error::{DirectoryError, FormatError, IdError};
use crate::types::Offset;

#[derive(Debug)]
pub enum StorageError {
    Config(&'static str),
    Allocation { operation: &'static str, source: std::collections::TryReserveError },
    Metadata(&'static str),
    InFile { path: PathBuf, source: Box<StorageError> },
    Epoch(IdError),
    Directory(DirectoryError),
    Topology(&'static str),
    Name(PathBuf),
    Identity { expected: SegmentHeader, found: SegmentHeader },
    Offset { position: u64, expected: Offset, found: Offset },
    Incomplete { position: u64, required: u64, available: u64 },
    Format { position: u64, source: FormatError },
    Io { operation: &'static str, position: u64, source: io::Error },
    PathIo { operation: &'static str, path: PathBuf, source: io::Error },
    RecordTooLarge { encoded: usize, segment_cap: u64 },
    SegmentFull,
    SegmentLimit,
    DiskFull,
    Range { start: Offset, upper: Offset, floor: Offset, end: Offset },
    Poisoned,
}

impl StorageError {
    pub(crate) fn io(operation: &'static str, position: u64, source: io::Error) -> Self {
        Self::Io { operation, position, source }
    }

    pub(crate) fn path(operation: &'static str, path: &std::path::Path, source: io::Error) -> Self {
        Self::PathIo { operation, path: path.to_path_buf(), source }
    }

    pub(crate) fn format(position: u64, source: FormatError) -> Self {
        Self::Format { position, source }
    }
}
```

`Error::source` preserves the allocation cause just as it preserves an I/O cause. A later owner may conservatively stop after a failed response-metadata allocation; that does not permit advancing a lease attempt or forgetting a checked-out buffer.

```rust title="crates/broker-core/src/storage/error.rs"
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allocation { operation, .. } => write!(f, "allocation failed: {operation}"),
            Self::InFile { path, .. } => write!(f, "recovery scan: {}", path.display()),
            Self::Metadata(message) => write!(f, "authoritative metadata: {message}"),
            Self::Epoch(_) => write!(f, "cannot allocate a restart epoch"),
            Self::Directory(_) => write!(f, "cannot own the data directory"),
            Self::Config(message) => write!(f, "storage configuration: {message}"),
            Self::Topology(message) => write!(f, "segment topology: {message}"),
            Self::Name(path) => write!(f, "unexpected segment entry: {}", path.display()),
            Self::Identity { expected, found } => write!(f,
                "segment identity {found:?} differs from {expected:?}"),
            Self::Offset { position, expected, found } => write!(f,
                "byte {position}: expected offset {}, found {}", expected.get(), found.get()),
            Self::Incomplete { position, required, available } => write!(f,
                "byte {position}: need {required} bytes, have {available}"),
            Self::Format { position, .. } => write!(f, "invalid format at byte {position}"),
            Self::Io { operation, position, .. } => write!(f, "{operation} at byte {position}"),
            Self::PathIo { operation, path, .. } => write!(f, "{operation}: {}", path.display()),
            Self::RecordTooLarge { encoded, segment_cap } => write!(f,
                "{encoded}-byte record plus segment header exceeds cap {segment_cap}"),
            Self::SegmentFull => write!(f, "active segment has insufficient room; rotate first"),
            Self::DiskFull => write!(f, "logical disk admission is full"),
            Self::SegmentLimit => write!(f, "segment-count limit reached"),
            Self::Range { start, upper, floor, end } => write!(f,
                "requested [{}, {}) outside [{}, {})", start.get(), upper.get(), floor.get(), end.get()),
            Self::Poisoned => write!(f, "storage owner stopped after an uncertain operation"),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Allocation { source, .. } => Some(source),
            Self::InFile { source, .. } => Some(source.as_ref()),
            Self::Epoch(source) => Some(source),
            Self::Directory(source) => Some(source),
            Self::Format { source, .. } => Some(source),
            Self::Io { source, .. } | Self::PathIo { source, .. } => Some(source),
            _ => None,
        }
    }
}
```

## Validate the pool shape before allocating

Create `src/budget/memory.rs`. `MemoryConfig` has private fields: external code cannot supply a zero-sized class, unchecked product, or arbitrary count through a struct literal. The configuration permits smaller pools for deterministic tests but never enlarges the per-partition ceilings. `is_none_or` treats multiplication overflow as a rejected configuration, not a wrapped small number.

`allocate_bytes` first reserves, then initializes only the requested length. It does not assert that capacity equals length. `BufferPool::new` below inspects and sums the actual capacities before returning a usable pool.

```rust title="crates/broker-core/src/budget/memory.rs"
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::codec::{HEADER_LEN, MAX_PAYLOAD};
use crate::mailbox::AdmissionError;
use crate::storage::error::StorageError;

pub const APPEND_BYTES: usize = 8 * 1024 * 1024;
pub const RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const OWNER_SCRATCH_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct MemoryConfig {
    pub(crate) append_size: usize,
    pub(crate) append_count: usize,
    pub(crate) response_size: usize,
    pub(crate) response_count: usize,
    pub(crate) waiters: usize,
}
impl MemoryConfig {
    pub fn new(append_size: usize, append_count: usize, response_size: usize,
        response_count: usize, waiters: usize) -> Result<Self, StorageError>
    {
        if !(1..=MAX_PAYLOAD).contains(&append_size) || !(1..=4096).contains(&append_count)
            || !(HEADER_LEN..=RESPONSE_BYTES).contains(&response_size)
            || !(1..=4096).contains(&response_count) || !(1..=64).contains(&waiters)
            || append_size.checked_mul(append_count).is_none_or(|n| n > APPEND_BYTES)
            || response_size.checked_mul(response_count).is_none_or(|n| n > RESPONSE_BYTES)
        {
            return Err(StorageError::Config("memory pool sizes, counts, or waiter limit are invalid"));
        }
        Ok(Self { append_size, append_count, response_size, response_count, waiters })
    }
}
impl Default for MemoryConfig {
    fn default() -> Self {
        Self { append_size: MAX_PAYLOAD, append_count: 8,
            response_size: 2 * 1024 * 1024, response_count: 2, waiters: 8 }
    }
}

// Startup only. No payload allocation occurs in checkout, resize, or return.
pub(crate) fn allocate_bytes(size: usize) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(|source| StorageError::Allocation {
        operation: "preallocate payload buffer", source,
    })?;
    bytes.resize(size, 0);
    Ok(bytes)
}
```

## Count both free and checked-out allocations

Let the capacities of a pool's buffers be $c_0,\ldots,c_{n-1}$. Its permanent allocation charge is $C=\sum_i c_i$. Its current checked-out charge is $U=\sum_{i\in\mathcal O}c_i$, where $\mathcal O$ is the set of buffers outside the free list. The conservation rule is

$$
C=U+\sum_{i\notin\mathcal O}c_i,\qquad 0\le U\le C.
$$

`allocated_bytes` never shrinks when a response is dropped: the allocation returns to the free list for reuse. `checked_out_bytes` does shrink. Confusing these two numbers would turn a successful recycling policy into a false claim that memory was returned to the operating system.

The free list itself contains vector descriptors, not duplicate payload arrays. Its entry storage is reserved once. `take` removes one owning descriptor, updates the checked-out capacity under the pool mutex, then releases the mutex before clearing the caller-visible slice. The initialized vector length stays at its original block size; `used` tracks the shorter view.

```rust title="crates/broker-core/src/budget/memory.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolState {
    pub allocated_bytes: usize,
    pub checked_out_bytes: usize,
    pub free_buffers: usize,
    pub block_bytes: usize,
}
struct PoolInner {
    free: Vec<Vec<u8>>,
    allocated: usize,
    checked_out: usize,
    block: usize,
}
#[derive(Clone)]
pub(crate) struct BufferPool {
    inner: Arc<Mutex<PoolInner>>,
    changed: Arc<Notify>,
}
impl BufferPool {
    pub(crate) fn new(size: usize, count: usize, ceiling: usize, changed: Arc<Notify>)
        -> Result<Self, StorageError>
    {
        let mut free = Vec::new();
        free.try_reserve_exact(count).map_err(|source| StorageError::Allocation {
            operation: "preallocate free-buffer entries", source,
        })?;
        let mut allocated = 0usize;
        for _ in 0..count {
            let bytes = allocate_bytes(size)?;
            allocated = allocated.checked_add(bytes.capacity())
                .filter(|n| *n <= ceiling)
                .ok_or(StorageError::Config("allocator-reported capacities exceed the pool ceiling"))?;
            free.push(bytes);
        }
        Ok(Self { inner: Arc::new(Mutex::new(PoolInner {
            free, allocated, checked_out: 0, block: size,
        })), changed })
    }
    pub(crate) fn state(&self) -> PoolState {
        let p = self.inner.lock().expect("buffer-pool mutex poisoned");
        PoolState { allocated_bytes: p.allocated, checked_out_bytes: p.checked_out,
            free_buffers: p.free.len(), block_bytes: p.block }
    }
    pub(crate) fn take(&self, len: usize) -> Result<PoolBuffer, AdmissionError> {
        let mut p = self.inner.lock().expect("buffer-pool mutex poisoned");
        if len > p.block { return Err(AdmissionError::TooLarge); }
        let bytes = p.free.pop().ok_or(AdmissionError::MemoryFull)?;
        p.checked_out += bytes.capacity();
        drop(p);
        let mut value = PoolBuffer { pool: self.clone(), bytes: Some(bytes), used: len };
        // A caller never sees an earlier user's contents, even on a partially filled request.
        value.as_mut_slice().fill(0);
        Ok(value)
    }
}
```

## Make the allocation follow the value

`PoolBuffer` owns both the vector and a handle to its return pool. The `Option` supports moving the vector out exactly once in `Drop`; it is not an optional budget. There is no public way to clone the buffer, grow its vector, or extract the vector while dropping the accounting owner.

`set_len` changes the exposed prefix, not vector length or allocation. On return, the entire original allocation is credited, even when the reply used zero bytes. Clearing on the next checkout prevents a new caller from observing previous contents through a partially filled request; this is not a secure-erasure promise.

The destructor updates the free list, unlocks, and then wakes admission tasks. Returning a buffer neither acknowledges work nor rolls back a completed append. The ownership/drop distinction follows Rust's normal destruction rules ([destructors](https://doc.rust-lang.org/reference/destructors.html)).

```rust title="crates/broker-core/src/budget/memory.rs"
// No Clone, DerefMut<Vec<u8>>, or into_vec: a payload cannot escape its lifetime owner.
pub(crate) struct PoolBuffer {
    pool: BufferPool,
    bytes: Option<Vec<u8>>,
    used: usize,
}
impl PoolBuffer {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes.as_ref().expect("live pooled buffer")[..self.used]
    }
    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes.as_mut().expect("live pooled buffer")[..self.used]
    }
    pub(crate) fn set_len(&mut self, len: usize) {
        assert!(len <= self.bytes.as_ref().expect("live pooled buffer").len());
        self.used = len;
    }
}
impl std::fmt::Debug for PoolBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolBuffer").field("used", &self.used).finish_non_exhaustive()
    }
}
impl Drop for PoolBuffer {
    fn drop(&mut self) {
        if let Some(bytes) = self.bytes.take() {
            let mut p = self.pool.inner.lock().unwrap_or_else(|e| e.into_inner());
            p.checked_out -= bytes.capacity();
            p.free.push(bytes); // At most the original count; no growth allocation.
            drop(p);
            self.pool.changed.notify_waiters();
        }
    }
}
```

## A bounded number of waiters is another resource

The waiter counter is separate from payload checkout. An admission future waiting for a buffer owns only a `WaitGuard`, not a payload or mailbox slot. Once it returns a prepared request, its waiter guard is gone and the prepared request owns the actual resources. Separate append and response gates prevent producers from exhausting the response-wait allowance.

The gate counts futures that have actually entered admission, not every task an external application might create. Chapter 16 must bound accepted connections and outstanding operations before creating these futures. This core rejects excess entrants immediately with `WaitersFull`; it is not a substitute for a connection limit.

```rust title="crates/broker-core/src/budget/memory.rs"
#[derive(Clone)]
pub(crate) struct WaitGate(Arc<Mutex<(usize, usize)>>);
impl WaitGate {
    pub(crate) fn new(limit: usize) -> Self { Self(Arc::new(Mutex::new((0, limit)))) }
    pub(crate) fn enter(&self) -> Result<WaitGuard, AdmissionError> {
        let mut state = self.0.lock().expect("waiter counter poisoned");
        if state.0 == state.1 { return Err(AdmissionError::WaitersFull); }
        state.0 += 1;
        Ok(WaitGuard(self.clone()))
    }
    pub(crate) fn used(&self) -> usize { self.0.lock().expect("waiter counter poisoned").0 }
}
pub(crate) struct WaitGuard(WaitGate);
impl Drop for WaitGuard {
    fn drop(&mut self) {
        self.0.0.lock().unwrap_or_else(|e| e.into_inner()).0 -= 1;
    }
}
```

The five focused tests check actual capacity, pointer-preserving reuse, clearing on checkout, refusal without mutation, startup failure, ordinary unwinding, and waiter-guard release. The impossible allocation request tests a deterministic capacity error, not a fabricated claim that physical memory was exhausted. A panic is deliberately raised outside the pool lock so the test checks ordinary cleanup rather than corruption of the ledger itself.

```rust title="crates/broker-core/src/budget/memory.rs"
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_not_payload_length_is_charged_and_returned() {
        let pool = BufferPool::new(64, 1, 128, Arc::new(Notify::new())).unwrap();
        let allocated = pool.state().allocated_bytes;
        let mut first = pool.take(3).unwrap();
        assert_eq!(pool.state().checked_out_bytes, allocated);
        let pointer = first.as_slice().as_ptr();
        first.as_mut_slice().copy_from_slice(b"cat");
        assert_eq!(pool.take(0).err(), Some(AdmissionError::MemoryFull));
        drop(first);
        let second = pool.take(3).unwrap();
        assert_eq!(second.as_slice().as_ptr(), pointer);
        assert_eq!(second.as_slice(), &[0, 0, 0]);
        drop(second);
        assert_eq!(pool.state().checked_out_bytes, 0);
        assert_eq!(pool.state().allocated_bytes, allocated);
    }
    #[test]
    fn impossible_buffer_requests_do_not_change_the_pool() {
        let pool = BufferPool::new(64, 1, 128, Arc::new(Notify::new())).unwrap();
        let before = pool.state();
        assert_eq!(pool.take(65).err(), Some(AdmissionError::TooLarge));
        assert_eq!(pool.state(), before);
    }
    #[test]
    fn startup_reports_capacity_overflow_and_refuses_over_budget_capacity() {
        assert!(matches!(allocate_bytes(usize::MAX), Err(StorageError::Allocation { .. })));
        assert!(BufferPool::new(64, 1, 63, Arc::new(Notify::new())).is_err());
        assert!(MemoryConfig::new(1024 * 1024, 9, 32, 1, 1).is_err());
        assert!(MemoryConfig::new(1, 1, 4 * 1024 * 1024, 2, 1).is_err());
        assert!(MemoryConfig::new(1, 1, 32, 1, 0).is_err());
    }
    #[test]
    fn ordinary_unwind_returns_the_owned_buffer() {
        let pool = BufferPool::new(64, 1, 128, Arc::new(Notify::new())).unwrap();
        let handle = pool.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _buffer = handle.take(64).unwrap();
            panic!("controlled unwind outside the pool mutex");
        }));
        assert!(result.is_err());
        assert_eq!(pool.state().checked_out_bytes, 0);
        assert_eq!(pool.state().free_buffers, 1);
    }
    #[test]
    fn waiter_limit_counts_live_guards_not_notifications() {
        let gate = WaitGate::new(1);
        let first = gate.enter().unwrap();
        assert_eq!(gate.enter().err().map(|_| AdmissionError::WaitersFull), Some(AdmissionError::WaitersFull));
        drop(first);
        let second = gate.enter().unwrap(); assert_eq!(gate.used(), 1); drop(second);
        assert_eq!(gate.used(), 0);
    }
}
```

## Reserve a queue slot before reading the body

A response buffer alone does not guarantee somewhere to enqueue the command. Add reserved counts to the mailbox. For a data lane with queued count $Q_d$, reserved-but-uncommitted count $T_d$, and capacity $K_d$, enforce

$$
Q_d+T_d\le K_d.
$$

A reservation increments $T_d$. Submission moves one unit from $T_d$ to $Q_d$ under the same queue mutex; cancellation decrements $T_d$. The query lane uses the same rule. Reservations are not queued commands and do not delay a closed owner's exit: after closure their later submissions are rejected and their bodies are released.

Replace `src/mailbox.rs` with the following chunks. There are now three independently bounded lanes: data, ordinary queries, and urgent controls. The urgent lane holds small acknowledgements, checkpoints, and retention requests; neither an append body nor a fetch reply takes one of its slots. The older `new(data, control)` constructor allocates a third lane with the control capacity so prerequisite examples remain valid.

```rust title="crates/broker-core/src/mailbox.rs"
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    Full, Closed, DiskFull, InvalidRecord, MemoryFull, TooLarge, WaitersFull, Deadline,
}

#[derive(Debug)]
pub struct Rejected<T> { pub reason: AdmissionError, pub value: T }
#[derive(Debug)]
pub enum Event<D, C> { Data(D), Control(C), Deadline, Closed }

#[derive(Clone, Copy)]
enum Lane { Data, Query }
struct Queues<D, C> {
    data: VecDeque<D>,
    control: VecDeque<C>,
    urgent: VecDeque<C>,
    reserved_data: usize,
    reserved_query: usize,
    closed: bool,
    next_lane: usize,
    #[cfg(test)]
    waiting: bool,
}
pub struct Mailbox<D, C> {
    queues: Mutex<Queues<D, C>>,
    changed: Condvar,
    space: Arc<Notify>,
    data_capacity: usize,
    control_capacity: usize,
    urgent_capacity: usize,
    #[cfg(test)]
    observed: Condvar,
}
impl<D, C> Mailbox<D, C> {
    pub fn new(data: usize, control: usize) -> Result<Self, &'static str> {
        Self::with_urgent(data, control, control)
    }
    pub fn with_urgent(data: usize, control: usize, urgent: usize) -> Result<Self, &'static str> {
        if [data, control, urgent].iter().any(|n| !(1..=4096).contains(n)) {
            return Err("each mailbox lane needs 1..=4096 slots");
        }
        fn lane<T>(count: usize) -> Result<VecDeque<T>, &'static str> {
            let mut queue = VecDeque::new();
            queue.try_reserve_exact(count).map_err(|_| "mailbox allocation failed")?;
            Ok(queue)
        }
        Ok(Self {
            queues: Mutex::new(Queues {
                data: lane(data)?, control: lane(control + urgent)?, urgent: lane(urgent)?,
                reserved_data: 0, reserved_query: 0, closed: false, next_lane: 1,
                #[cfg(test)] waiting: false,
            }),
            changed: Condvar::new(), space: Arc::new(Notify::new()),
            data_capacity: data, control_capacity: control, urgent_capacity: urgent,
            #[cfg(test)] observed: Condvar::new(),
        })
    }
    pub(crate) fn notification(&self) -> Arc<Notify> { Arc::clone(&self.space) }
    pub(crate) fn is_closed(&self) -> bool {
        self.queues.lock().unwrap_or_else(|e| e.into_inner()).closed
    }
```

`try_data` and `try_control` include the reserved count when checking fullness. `try_urgent` checks only its own capacity. No producer may silently borrow an urgent slot. An urgent flood can still fill that lane, so the API still returns `Full`; “reserved” means independent from other traffic, not infinite or guaranteed immediate acceptance.

```rust title="crates/broker-core/src/mailbox.rs"
    pub fn try_data(&self, value: D) -> Result<(), Rejected<D>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.data.len() + q.reserved_data == self.data_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.data.push_back(value); self.changed.notify_one(); Ok(())
    }
    pub fn try_control(&self, value: C) -> Result<(), Rejected<C>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.control.len() + q.reserved_query == self.control_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.control.push_back(value); self.changed.notify_one(); Ok(())
    }
    pub fn try_urgent(&self, value: C) -> Result<(), Rejected<C>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.urgent.len() == self.urgent_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.urgent.push_back(value); self.changed.notify_one(); Ok(())
    }
```

A `Slot` stores an `Arc` to the exact mailbox and a lane identity. It is not `Clone`. The queue mutex checks closure, tests the sum, and increments the reserved count in one transition. There is no check-then-reserve race between concurrent producers. No body allocation or file operation occurs while that mutex is held.

```rust title="crates/broker-core/src/mailbox.rs"
    fn reserve(self: &Arc<Self>, lane: Lane) -> Result<Slot<D, C>, AdmissionError> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(AdmissionError::Closed); }
        match lane {
            Lane::Data => {
                if q.data.len() + q.reserved_data == self.data_capacity { return Err(AdmissionError::Full); }
                q.reserved_data += 1;
            }
            Lane::Query => {
                if q.control.len() + q.reserved_query == self.control_capacity { return Err(AdmissionError::Full); }
                q.reserved_query += 1;
            }
        }
        Ok(Slot { mailbox: Arc::clone(self), lane, active: true, signal: true })
    }
    pub(crate) fn reserve_data(self: &Arc<Self>) -> Result<Slot<D, C>, AdmissionError> {
        self.reserve(Lane::Data)
    }
    pub(crate) fn reserve_query(self: &Arc<Self>) -> Result<Slot<D, C>, AdmissionError> {
        self.reserve(Lane::Query)
    }
```

## Round-robin service is not a wall-clock deadline

The owner starts with ordinary queries, then urgent controls, then data, skipping empty lanes. When all three remain ready, each receives one turn per cycle. Per-lane FIFO remains intact; submission into one lane is not an ordering barrier for another.

Deadlines still precede queued work so producer synchronization and periodic progress publication remain timely in the existing sense. A single control may block on a real file synchronization. Reserved capacity and round-robin selection cannot make a hung filesystem finish, cannot cancel an already running operation, and do not establish a maximum acknowledgement latency.

```rust title="crates/broker-core/src/mailbox.rs"
    // Exactly one owner; each continuously ready lane gets one turn in three.
    pub fn recv(&self, deadline: Option<Instant>) -> Event<D, C> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        loop {
            if deadline.is_some_and(|end| Instant::now() >= end) { return Event::Deadline; }
            for distance in 0..3 {
                let lane = (q.next_lane + distance) % 3;
                let event = match lane {
                    0 => q.data.pop_front().map(Event::Data),
                    1 => q.control.pop_front().map(Event::Control),
                    _ => q.urgent.pop_front().map(Event::Control),
                };
                if let Some(event) = event {
                    q.next_lane = (lane + 1) % 3;
                    self.space.notify_waiters();
                    return event;
                }
            }
            if q.closed { return Event::Closed; }
            #[cfg(test)] { q.waiting = true; self.observed.notify_all(); }
            q = match deadline {
                Some(end) => self.changed.wait_timeout(q, end.saturating_duration_since(Instant::now()))
                    .expect("mailbox mutex poisoned").0,
                None => self.changed.wait(q).expect("mailbox mutex poisoned"),
            };
            #[cfg(test)] { q.waiting = false; }
        }
    }
    pub fn close(&self) {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        q.closed = true; self.changed.notify_all(); self.space.notify_waiters();
    }
    pub fn abort(&self) -> (VecDeque<D>, VecDeque<C>) {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        q.closed = true;
        let data = std::mem::take(&mut q.data);
        let mut control = std::mem::take(&mut q.control);
        control.append(&mut q.urgent); // Capacity was reserved for both lanes at construction.
        self.changed.notify_all(); self.space.notify_waiters();
        (data, control)
    }
}
```

## Submission consumes the reservation once

Both `send_data` and `send_query` consume their slot, subtract its reserved count, and either enqueue or return the rejected value. A failed submission retains ownership long enough for the command's payload/disk/reply fields to clean up normally.

`cancel_quietly` has a narrow role: a combined admission attempt that temporarily took a slot but could not obtain a buffer must not notify itself repeatedly. Part 3 serializes those composite attempts with a separate admission mutex. Other composite attempts cannot observe its temporary slot. Ordinary cancellation of an exposed prepared request uses the normal notifying destructor. Making every rollback silent without that serialization would be wrong: another caller could observe temporary fullness and never receive the enabling wakeup.

```rust title="crates/broker-core/src/mailbox.rs"
// A reservation counts before its body exists; it does not own an enqueued command yet.
pub(crate) struct Slot<D, C> {
    mailbox: Arc<Mailbox<D, C>>,
    lane: Lane,
    active: bool,
    signal: bool,
}
impl<D, C> Slot<D, C> {
    pub(crate) fn cancel_quietly(mut self) { self.signal = false; }
    pub(crate) fn send_data(mut self, value: D) -> Result<(), Rejected<D>> {
        assert!(matches!(self.lane, Lane::Data));
        let mut q = self.mailbox.queues.lock().expect("mailbox mutex poisoned");
        q.reserved_data -= 1; self.active = false;
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        q.data.push_back(value); self.mailbox.changed.notify_one(); Ok(())
    }
    pub(crate) fn send_query(mut self, value: C) -> Result<(), Rejected<C>> {
        assert!(matches!(self.lane, Lane::Query));
        let mut q = self.mailbox.queues.lock().expect("mailbox mutex poisoned");
        q.reserved_query -= 1; self.active = false;
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        q.control.push_back(value); self.mailbox.changed.notify_one(); Ok(())
    }
}
impl<D, C> Drop for Slot<D, C> {
    fn drop(&mut self) {
        if self.active {
            let mut q = self.mailbox.queues.lock().unwrap_or_else(|e| e.into_inner());
            match self.lane {
                Lane::Data => q.reserved_data -= 1,
                Lane::Query => q.reserved_query -= 1,
            }
            drop(q);
            if self.signal { self.mailbox.space.notify_waiters(); }
        }
    }
}
```

The original mailbox tests remain below. They still exercise two busy lanes, absolute deadlines, closure, and observed condition-variable waits. With no urgent traffic, their alternating query/data order is unchanged. Keeping them is a check that adding a lane did not silently alter the earlier queue contract.

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

The four additional tests cover reservation pressure, urgent admission with a reserved query slot, three-lane scheduling, and closure/abort ownership. Abort combines control values outside the serving path; construction reserved enough control entry storage to hold both control queues during that cleanup.

```rust title="crates/broker-core/src/mailbox.rs"
#[cfg(test)]
mod reservation_tests {
    use super::*;

    #[test]
    fn reserved_slots_count_before_enqueue_and_cancellation_returns_them() {
        let q = Arc::new(Mailbox::<u8, u8>::with_urgent(1, 1, 1).unwrap());
        let slot = q.reserve_data().unwrap();
        assert_eq!(q.try_data(8).unwrap_err().reason, AdmissionError::Full);
        drop(slot); q.try_data(9).unwrap();
        assert!(matches!(q.recv(None), Event::Data(9)));
    }
    #[test]
    fn reserved_queries_cannot_consume_the_urgent_lane() {
        let q = Arc::new(Mailbox::<u8, u8>::with_urgent(1, 1, 1).unwrap());
        let query = q.reserve_query().unwrap();
        assert_eq!(q.try_control(2).unwrap_err().reason, AdmissionError::Full);
        q.try_urgent(3).unwrap();
        assert_eq!(q.try_urgent(4).unwrap_err().reason, AdmissionError::Full);
        assert!(matches!(q.recv(None), Event::Control(3)));
        query.send_query(5).unwrap();
        assert!(matches!(q.recv(None), Event::Control(5)));
    }
    #[test]
    fn three_busy_lanes_get_round_robin_service() {
        let q = Mailbox::with_urgent(2, 2, 2).unwrap();
        for n in 0..2 { q.try_data(n).unwrap(); q.try_control(n + 10).unwrap(); q.try_urgent(n + 20).unwrap(); }
        for n in 0..2 {
            assert!(matches!(q.recv(None), Event::Control(v) if v == n + 10));
            assert!(matches!(q.recv(None), Event::Control(v) if v == n + 20));
            assert!(matches!(q.recv(None), Event::Data(v) if v == n));
        }
    }
    #[test]
    fn close_rejects_a_reserved_commit_and_abort_preserves_every_lane() {
        let q = Arc::new(Mailbox::<u8, u8>::with_urgent(2, 2, 2).unwrap());
        let slot = q.reserve_data().unwrap();
        q.try_data(1).unwrap(); q.try_control(2).unwrap(); q.try_urgent(3).unwrap(); q.close();
        assert_eq!(slot.send_data(4).unwrap_err().reason, AdmissionError::Closed);
        let (data, controls) = q.abort();
        assert_eq!(data.into_iter().collect::<Vec<_>>(), vec![1]);
        assert_eq!(controls.into_iter().collect::<Vec<_>>(), vec![2, 3]);
    }
}
```

## Read the ownership table before continuing

| State | Payload owner | Queue charge | Disk reservation | Can the owner execute it? |
| --- | --- | --- | --- | --- |
| Waiting for admission | None inside this API | None | None | No |
| Prepared append | `PreparedAppend` | Reserved data slot | Worst-case append growth | No |
| Submitted append | `Publish` | Queued data slot until pop | Follows command until settlement | Yes |
| Written append | Scratch retains reusable bytes; source buffer returned | None | Settled to file growth | Await synchronization before success |
| Prepared fetch | `PreparedLease` | Reserved query slot | None | No |
| Returned fetch | `PooledLeaseBatch` | None | None | Copy is already complete |

A source buffer can be reused after the owner has completed its synchronous write, before the producer success reply. That is safe because synchronization operates on the file, not the old source vector. It does not make the write durable early. Part 2 implements that distinction without allocating an extra encoded vector.
