# Chapter 18 — Deterministic failure schedules and recovery checks

## Part 1 — A small reference state for composed failures

> **Learning objectives**
> Keep observed success distinct from stored presence; express delivery, checkpoint, and retention invariants in a small reference model; generate replayable command sequences; and retain useful failure context without building a general model checker.

**Implementation time: 4.5 hours across all four parts**, not per page: 1.0 for the reference state and repeatable inputs, 1.0 for the command driver, 0.75 for composed fault fixtures, and 1.75 for running checks, diagnosing failures, and the edit-and-restore exercise. Reading is excluded. Continue with [Part 2 — Reproducible owner schedules](https://25d-228.github.io/rust-learning/chapter-18-part-02.html), [Part 3 — Publication and deletion cuts](https://25d-228.github.io/rust-learning/chapter-18-part-03.html), and [Part 4 — Interrupting recovery again](https://25d-228.github.io/rust-learning/chapter-18-part-04.html).

**Prerequisite code:** the complete workspace through [Chapter 17](https://25d-228.github.io/rust-learning/chapter-17.html). In particular, retain `broker-core`'s record codec, `FaultFile`, segmented log, `ManualClock`, leased delivery, durable `Progress`, retained-floor authority, recovery callbacks, and owner joining. The chapter also reuses the existing `crates/broker-core/tests/common/mod.rs` and its `TestDir`: exclusive creation of a fresh temporary directory and removal of only that owned directory at destruction.

The **seven-file overlay** at `book/media/chapter-18/` adds one integration target and six supporting modules. It changes **no production file, dependency, persistent format, or wire format**. Apply it after overlays 1–17 in your separate implementation workspace. All new source is printed here in explained fragments; concatenate blocks with the same filename in page order. Assemble all four parts before compiling the target. No future chapter's code is needed.

## Why passing subsystem tests is not enough

A lease test can correctly reject a stale generation while a checkpoint test correctly stores a contiguous frontier. Neither alone establishes what happens when completed followers occupy a full window, the missing head expires, its retry closes the gap, the checkpoint reply disappears, and retention runs afterward. The interesting state exists **between** the subsystems.

We will test the real owner and real record files against a deliberately smaller description. The description stores attempts in an ordered map, completed status in a boolean, records in a short vector, and segment intervals as pairs of integers. It does not reuse the production ring, sparse index, checkpoint encoder, or retention routine to calculate expected state. Reusing those routines as the oracle would make some implementation errors agree with themselves.

The generated schedules are sequential at command boundaries. A command's reply is observed before selecting the next operation, except for two explicitly fenced abandoned-reply cases. Record batching is set to one; checkpoints are explicit; lease time comes from a manual clock. These decisions remove incidental scheduling choices from this particular experiment. They do **not** claim that the operating-system scheduler is deterministic, that the broker is linearizable under every concurrent history, or that seeded testing replaces Chapters 9, 15, and 17's controlled concurrency tests.

The generated driver exercises partition 0 and both groups. Initialization and recovery still inspect all four partition directories; a later damaged-partition fixture checks validation across them. Four-owner networking and sustained load are separate concerns, not smuggled into this small state checker.

## Three failure experiments, three different claims

This chapter's injected error returns through ordinary Rust error handling. Its destructors run, owners are joined, and file handles close. It establishes behavior at a named application/I/O boundary under that error model. The earlier process-termination harnesses establish a different observation: what restart sees when the child does not execute its graceful cleanup. Neither experiment disconnects power or controls filesystem writeback ordering.

Synthetic corruption is different again. A fixture deliberately changes a complete payload byte while leaving its stored checksum unchanged. That asks whether the parser refuses unexplained corruption; it does not demonstrate that any particular crash would create those bytes. Keep the fixture's description with the result.

| Experiment | Controlled event | What remains unknown |
| --- | --- | --- |
| Returned I/O or observer error | Exact method or post-operation callback | Arbitrary kernel/device failure behavior |
| Process termination, from the earlier harnesses | Child stops at an announced boundary | Which unsynchronized bytes survive a power loss |
| Deliberately damaged file | Exact invalid bytes supplied to recovery | The physical event that could produce them |

Our sync adapter must call the real synchronization method on every successful path. Rust documents `File::sync_all` as a fallible attempt to synchronize file contents and metadata; dropping a file is not a replacement for observing that result. See the [file synchronization API](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all). A fake successful sync would weaken the very boundary the broker's success contract depends on.

## Name the frontiers before writing the checker

Let $n$ be the synchronized exclusive record end after a completed command, and $f$ the first retained offset. For group $g$, let $p_g$ be persisted next offset, $c_g$ contiguous accepted-completion next offset, and $i_g$ issued next offset. These are **exclusive** frontiers: next offset 3 describes offsets 0, 1, and 2, not offset 3 itself. In a healthy state:

$$
f \le p_g \le c_g \le i_g \le n.
$$

The four-slot window counts every issued offset not yet retired, including completed followers above a gap:

$$
0 \le i_g-c_g \le 4.
$$

Suppose group 0 has issued offsets 0–3 and completed 1, 2, and 3. Its state is still $c_0=0$, $i_0=4$, with four occupied slots. A checkpoint cannot advance it. Completing a valid attempt for offset 0 retires the whole prefix, making $c_0=4$. Persisted progress remains $p_0=0$ until publication succeeds.

After an ordinary restart, the lease map is empty and both volatile frontiers start at persisted progress: $c_g=i_g=p_g$. A previously accepted completion above that persisted boundary may repeat. Retention instead uses the slower persisted group:

$$
s=\min(p_0,p_1), \qquad f_{\mathrm{new}}\le s.
$$

The coverage check compares bytes only in $[f,n)$. It must not call an intentionally reclaimed record missing. Conversely, checking only records that happen to be returned would miss a shortened read, so the checker also asserts the returned count and exclusive end.

## Fix the geometry so arithmetic is visible

Use eight-byte logical input identifiers as payloads. The existing record header is 32 bytes, so each encoded record is $32+8=40$ bytes. Each segment also starts with a 32-byte header. A 112-byte segment therefore holds exactly two records:

$$
32+2\times40=112.
$$

Let $m$ be the number of retained segment files, including the active file. With no partial write in flight, their logical lengths sum to:

$$
B=32m+40(n-f).
$$

For two full segments, $m=2$, $n-f=4$, and $B=224$ bytes. If both groups persist next offset 4, retention first creates a 32-byte empty replacement, then removes the two old files. Final file length is 32 bytes. The intermediate extra header comes from the maintenance reserve, not from deleting first and hoping replacement succeeds.

The fixture's normal data quota is 512 bytes and its metadata/maintenance reserve is 416 bytes. Append admission reserves the 40-byte record plus a possible 32-byte rotation header, so a fresh reservation needs 72 bytes even when the active file happens to have room. Eight records occupy four segments and 448 bytes; the ninth reservation would require $448+72=520$, so it is refused. This is logical-length accounting, not physical allocated blocks or guaranteed free disk space.

## Create the integration target and fixed fixture

Only the top-level `tests/schedules.rs` becomes a Cargo integration-test target. Its explicit `#[path = "..."]` attributes locate supporting source without creating six unrelated test executables. `mod common` resolves the named prerequisite helper. The module declarations are the complete target entry point; there is no separate handwritten `main`.

```rust title="crates/broker-core/tests/schedules.rs"
mod common;
#[path = "schedules/support.rs"]
mod support;
#[path = "schedules/model.rs"]
mod model;
#[path = "schedules/driver.rs"]
mod driver;
#[path = "schedules/sequences.rs"]
mod sequences;
#[path = "schedules/cuts.rs"]
mod cuts;
#[path = "schedules/recovery.rs"]
mod recovery;
```

Start `schedules/support.rs` with the imports and fixed identities below. `p()` and `g()` are constructors for valid typed identities, not casts between partition and group domains. The file naming function remains the production canonical name; **expected record contents and frontier calculations do not come from the storage implementation**.

```rust title="crates/broker-core/tests/schedules/support.rs"
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broker_core::budget::DiskQuota;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::io::StorageFile;
use broker_core::storage::{checkpoint, retention};
use broker_core::storage::log::segment_name;
use broker_core::storage::metadata::PublicationStep;
use broker_core::storage::recovery::{self, StorageSession};
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::SyncIo;
use broker_core::types::{GroupId, Offset, PartitionId};
use broker_core::worker::{BatchConfig, Client, RequestError, Ticket, WorkerHandle};
use crate::common::TestDir;

pub const WINDOW: usize = 4;
pub const LEASE_MS: u64 = 10;
pub const QUOTA: u64 = 512;
pub fn p() -> PartitionId { PartitionId::new(0).unwrap() }
pub fn g(index: usize) -> GroupId { GroupId::new(index as u8).unwrap() }
pub fn config() -> StorageConfig { StorageConfig::new(112, 2, 16).unwrap() }
pub fn segment(root: &Path, base: u64) -> PathBuf {
    root.join("partition-0").join(segment_name(Offset::new(base)))
}
```

`setup` initializes a fresh broker directory under `TestDir`; it never opens an arbitrary user-supplied data path. `start` moves partition 0's log into a real owner. Dropping the unused logs does not release the directory guard while the selected log still owns its shared guard reference. The fixture is intentionally the raw synchronous `Client`, not the network client or pooled serving API. That lets the schedule use standard receive timeouts without building an executor and makes its single control-lane ordering explicit.

The five-second receive limit is a watchdog against a missing reply, not a timing assertion about throughput. A failure still unwinds and joins the owner. It cannot forcibly terminate an operating-system call that never returns; diagnosing such an external stall still needs the lifecycle tools from Chapter 17. No sleep determines whether an operation happened.

```rust title="crates/broker-core/tests/schedules/support.rs"
pub fn setup() -> (TestDir, PathBuf, StorageSession) {
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("broker");
    let session = recovery::initialize_budgeted(&root, config(),
        DiskQuota::new(QUOTA, 416).unwrap()).unwrap();
    (directory, root, session)
}
pub fn start<S: SyncIo + Send + 'static>(session: StorageSession, io: S,
    clock: Arc<ManualClock>) -> (WorkerHandle, Client)
{
    let (epoch, mut logs) = session.into_parts();
    let log = logs.remove(0);
    drop(logs); // Partition zero's log still retains the whole-directory guard.
    let owner = WorkerHandle::start_with_progress(log, io,
        BatchConfig::new(1, 4096, Duration::from_secs(60)).unwrap(), 8, 8,
        LeaseConfig::new(epoch, WINDOW, LEASE_MS).unwrap(), clock,
        CheckpointPolicy::explicit_only()).unwrap();
    let client = owner.client();
    (owner, client)
}
pub fn receive<T>(ticket: Ticket<T>) -> Result<T, RequestError> {
    ticket.recv_timeout(Duration::from_secs(5)).expect("schedule reply watchdog")
}
pub fn get<T, E: Debug>(request: Result<Ticket<T>, E>) -> T {
    receive(request.unwrap()).unwrap()
}
pub fn payload(id: u64) -> [u8; 8] { id.to_be_bytes() }
pub fn record_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    for partition in 0..4 {
        for entry in fs::read_dir(root.join(format!("partition-{partition}"))).unwrap() {
            let path = entry.unwrap().path();
            files.insert(path.strip_prefix(root).unwrap().to_path_buf(), fs::read(path).unwrap());
        }
    }
    files
}

```

`record_files` stores a small byte-for-byte image of record files only. It excludes startup metadata, whose epoch intentionally changes on a successful restart. Its use is bounded by these tiny fixtures; it is not a production backup routine. `BTreeMap` also gives stable path ordering in a failed comparison, as described in the [ordered map documentation](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html).

## Keep the last transitions, not an unbounded journal

`Trace` records at most 24 descriptions. Each includes a monotonically increasing local sequence number, so an abbreviated tail does not pretend to be the whole history. Declare the trace before the fixture: on an assertion panic, the fixture joins/drops its resources before the trace's destructor prints the retained context. The destructor ignores stderr write errors rather than causing a second panic during cleanup.

```rust title="crates/broker-core/tests/schedules/support.rs"
pub struct Trace {
    seed: u64,
    case: &'static str,
    next: usize,
    tail: VecDeque<String>,
}
impl Trace {
    pub fn new(seed: u64, case: &'static str) -> Self {
        Self { seed, case, next: 0, tail: VecDeque::new() }
    }
    pub fn step(&mut self, description: impl Into<String>) {
        if self.tail.len() == 24 { self.tail.pop_front(); }
        self.tail.push_back(format!("{:03}: {}", self.next, description.into()));
        self.next += 1;
    }
}
impl Drop for Trace {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let mut output = io::stderr().lock();
            let _ = writeln!(output, "schedule case={} seed=0x{:016x}; last {} transitions:",
                self.case, self.seed, self.tail.len());
            for line in &self.tail { let _ = writeln!(output, "  {line}"); }
        }
    }
}

```

`thread::panicking()` asks whether the current thread is unwinding, not whether some child once panicked. An owner failure becomes a failed assertion on this test thread through its reply or join result. See the [panic-state API](https://doc.rust-lang.org/std/thread/fn.panicking.html). An abort or external process kill does not run this diagnostic destructor; these fixtures do not claim otherwise.

## A reproducible generator, not a source of security randomness

Let $x_k$ be the 64-bit generator state before draw $k$. We choose explicit modular arithmetic:

$$
x_{k+1}=(6364136223846793005\,x_k+1442695040888963407)\bmod 2^{64}.
$$

For an operation with $b$ choices, use the high 32 bits of the next state, then reduce modulo $b$. The reduction can be biased; uniform statistical sampling is not the claim. The constants, seed, draw order, and 96-step count define a repeatable input sequence. `wrapping_mul` and `wrapping_add` make the same modular arithmetic explicit in debug and release builds; see the [wrapping integer methods](https://doc.rust-lang.org/std/primitive.u64.html#method.wrapping_mul).

From seed zero, the first three states are 1442695040888963407, 1876011003808476466, and 11166244414315200793. Part 2 fixes those values in a test. Invalid environment text is an error rather than an unnoticed return to a default seed. No global generator or shared test ordering is involved.

```rust title="crates/broker-core/tests/schedules/support.rs"
pub struct Generator(pub u64);
impl Generator {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }
    pub fn pick(&mut self, bound: usize) -> usize {
        assert!(bound > 0);
        ((self.next() >> 32) % bound as u64) as usize
    }
}
pub fn selected_seeds() -> Vec<u64> {
    match std::env::var("BROKER_SCHEDULE_SEED") {
        Ok(text) => {
            let seed = if let Some(hex) = text.strip_prefix("0x") {
                u64::from_str_radix(hex, 16).expect("hexadecimal BROKER_SCHEDULE_SEED")
            } else { text.parse().expect("decimal BROKER_SCHEDULE_SEED") };
            vec![seed]
        }
        Err(std::env::VarError::NotPresent) => vec![0, 1, 7, 0x5eed, 0xdead_beef, u64::MAX],
        Err(error) => panic!("BROKER_SCHEDULE_SEED: {error}"),
    }
}

```

Stop this support file here for now; Part 3 appends its fault adapter and recovered view. Do not invent an empty implementation for them. They are provided in full before the chapter's run commands.

## Model storage with intervals, not the sparse index

Create `schedules/model.rs`. The reference retains logical payloads even after retention; their offsets remain indexes into `records`. The `segments` vector, not the retained payload vector, determines which bytes must still exist on disk. This separation is useful: an oracle should remember what was legitimately deleted instead of quietly forgetting the evidence needed to detect an incorrect deletion.

Each group uses a map keyed by the full offset. There is no modulo indexing, wrap cursor, or physical slot reuse in this model. That makes it structurally different from the production window. These allocations are tiny test-only bookkeeping, not part of the serving memory budget.

```rust title="crates/broker-core/tests/schedules/model.rs"
use std::collections::{BTreeMap, BTreeSet};
use broker_core::delivery::{AckOutcome, AckRejection, LeaseBatch, LeaseToken};
use super::support::{LEASE_MS, QUOTA, WINDOW, g, p};

#[derive(Clone, Copy)]
pub struct Attempt {
    pub generation: u64,
    pub deadline: u64,
    pub done: bool,
}
#[derive(Default)]
pub struct Group {
    pub next: u64,
    pub issued: u64,
    pub persisted: u64,
    pub attempts: BTreeMap<u64, Attempt>,
}
pub struct Model {
    pub epoch: u64,
    pub now: u64,
    pub records: Vec<[u8; 8]>,
    pub groups: [Group; 2],
    pub segments: Vec<(u64, u64)>,
}
impl Model {
    pub fn new(epoch: u64) -> Self {
        Self { epoch, now: 0, records: Vec::new(), groups: [Group::default(), Group::default()],
            segments: vec![(0, 0)] }
    }
    pub fn end(&self) -> u64 { self.records.len() as u64 }
    pub fn floor(&self) -> u64 { self.segments[0].0 }
    pub fn bytes(&self) -> u64 {
        32 * self.segments.len() as u64 + 40 * (self.end() - self.floor())
    }
    pub fn can_append(&self) -> bool { self.bytes() + 72 <= QUOTA }
    pub fn append(&mut self, payload: [u8; 8]) -> u64 {
        assert!(self.can_append());
        let offset = self.end();
        let tail = *self.segments.last().unwrap();
        if tail.1 - tail.0 == 2 { self.segments.push((offset, offset)); }
        self.segments.last_mut().unwrap().1 = offset + 1;
        self.records.push(payload);
        offset
    }
```

`can_append` checks the conservative reservation, while `append` charges actual growth. When the active interval contains two records, `append` adds a new interval before increasing its end. A rejected logical input never reaches this method and does not consume an offset. The bounded schedule is far from integer exhaustion; the production checked-overflow tests remain responsible for representational extremes.

## Check eligible delivery without copying its scheduling algorithm

The model does not duplicate the retry cursor or alternate-retry policy. It permits any valid order of expired retries and sequential new offsets. That checks safety without asserting that one internal fair ordering is the only correct one; Chapter 12's focused tests continue to check the chosen fairness policy.

Let $e$ be the number of expired, incomplete attempts, $u$ free window slots, $q$ the requested record limit, and $z$ the encoded-byte limit. With fixed 40-byte records, the expected return count is:

$$
r=\min\left(e+\min(u,n-i_g),\ q,\ \left\lfloor z/40\right\rfloor\right).
$$

For two expired attempts, no new records, a count limit of two, and $z=79$, exactly one record fits. A second token must not be installed for the nonreturned attempt. A limit of 39 returns none. Equal-sized fixtures make this count derivation independent of the order the implementation chooses.

```rust title="crates/broker-core/tests/schedules/model.rs"
    pub fn fetch(&mut self, index: usize, count: usize, bytes: usize, batch: &LeaseBatch) {
        let end = self.end();
        let floor = self.floor();
        let group = &mut self.groups[index];
        let free = WINDOW - group.attempts.len();
        let expired = group.attempts.values().filter(|a| !a.done && a.deadline <= self.now).count();
        let eligible = expired + free.min((end - group.issued) as usize);
        assert_eq!(batch.records.len(), eligible.min(count).min(bytes / 40));
        assert_eq!(batch.encoded_bytes, batch.records.len() * 40);
        let mut distinct = BTreeSet::new();
        for record in &batch.records {
            let token = record.token;
            let offset = token.offset.get();
            assert!(distinct.insert(offset), "one batch repeats an offset");
            assert!(floor <= offset && offset < end);
            assert_eq!(record.record.offset(), token.offset);
            assert_eq!(record.record.payload(), &self.records[offset as usize]);
            assert_eq!((token.epoch.get(), token.partition, token.group), (self.epoch, p(), g(index)));
            assert_eq!(token.deadline_ms, self.now + LEASE_MS);
            if offset == group.issued {
                assert!(group.attempts.len() < WINDOW);
                assert_eq!(token.generation, 1);
                group.issued += 1;
            } else {
                let old = group.attempts.get(&offset).expect("retry must name an occupied offset");
                assert!(!old.done && old.deadline <= self.now);
                assert_eq!(token.generation, old.generation + 1);
            }
            group.attempts.insert(offset, Attempt {
                generation: token.generation, deadline: token.deadline_ms, done: false,
            });
        }
        assert_eq!(batch.state.epoch.get(), self.epoch);
        assert_eq!((batch.state.partition, batch.state.group), (p(), g(index)));
        assert_eq!((batch.state.window.completed_end.get(), batch.state.window.issued_end.get()),
            (group.next, group.issued));
        assert_eq!((batch.state.window.occupied, batch.state.window.capacity),
            (group.attempts.len(), WINDOW));
    }
```

A fresh offset must equal `group.issued` and begin at generation 1. Any other returned offset must already have an incomplete expired attempt and advance its generation by one. Every token must carry the fixture's epoch and identities and a deadline exactly ten manual milliseconds after issuance. A set rejects duplicate offsets inside one batch. Finally, the returned window description must agree with the model's map size and exclusive frontiers.

Notice the limits of this oracle: it checks membership, payloads, count, generations, deadlines, and frontier accounting, **not** every possible fairness guarantee. Deliberately excluding one policy is clearer than accidentally deriving expected order by calling the production planner.

## Accept completion, then retire only an unbroken prefix

The acknowledgement reference follows the public rejection order. Identity mismatches are checked before offset status. An old epoch is rejected even if the offset has already retired. A matching completed follower is a duplicate even after its original deadline: that describes an already accepted completion, not newly accepted late work.

```rust title="crates/broker-core/tests/schedules/model.rs"
    pub fn acknowledge(&mut self, index: usize, token: LeaseToken) -> (AckOutcome, usize) {
        use AckOutcome::{AlreadyRetired, Applied, Duplicate, Rejected};
        use AckRejection::*;
        if token.epoch.get() != self.epoch { return (Rejected(WrongEpoch), 0); }
        if token.partition != p() { return (Rejected(WrongPartition), 0); }
        if token.group != g(index) { return (Rejected(WrongGroup), 0); }
        let group = &mut self.groups[index];
        let offset = token.offset.get();
        if offset < group.next { return (AlreadyRetired, 0); }
        let Some(attempt) = group.attempts.get_mut(&offset) else { return (Rejected(NotIssued), 0); };
        if token.generation != attempt.generation { return (Rejected(StaleGeneration), 0); }
        if token.deadline_ms != attempt.deadline { return (Rejected(WrongDeadline), 0); }
        if attempt.done { return (Duplicate, 0); }
        if self.now >= attempt.deadline { return (Rejected(Expired), 0); }
        attempt.done = true;
        let before = group.next;
        while group.attempts.get(&group.next).is_some_and(|entry| entry.done) {
            group.attempts.remove(&group.next);
            group.next += 1;
        }
        (Applied, (group.next - before) as usize)
    }
```

After marking one entry done, the loop removes consecutive completed entries starting at `group.next`. It does not remove an isolated follower. For completed offsets 1, 2, and 3, completing 0 executes the loop four times, changing next from 0 to 4 and returning retired count 4. The `BTreeMap` lookup and removal replace the production ring's slot arithmetic; both must produce the same visible result.

The code uses a temporary mutable borrow of one map entry. That borrow ends after `attempt.done = true` because the reference is no longer used; the subsequent lookups and removals borrow the map again. This is the last-use rule from Chapter 2, not a reason to clone the entire map.

## Model checkpoints, retention, and restarting separately

A successful checkpoint moves only persisted progress. Retention computes a safe segment boundary from the two persisted frontiers. When the entire nonempty active segment is safe, the model first adds an empty replacement interval. It then counts removed file headers and records independently of the broker's reported removal totals.

```rust title="crates/broker-core/tests/schedules/model.rs"
    pub fn checkpoint(&mut self, index: usize) -> bool {
        let group = &mut self.groups[index];
        let changed = group.persisted != group.next;
        group.persisted = group.next;
        changed
    }
    pub fn retain(&mut self) -> (u64, usize, u64, bool) {
        let before = self.floor();
        let safe = self.groups.iter().map(|group| group.persisted).min().unwrap();
        let end = self.end();
        let tail = *self.segments.last().unwrap();
        let rotated = tail.0 < tail.1 && safe == end;
        if rotated { self.segments.push((end, end)); }
        let floor = self.segments.iter().map(|(base, _)| *base)
            .filter(|base| *base <= safe).max().unwrap();
        let removed = self.segments.iter().filter(|(base, _)| *base < floor).count();
        let bytes = 32 * removed as u64 + 40 * (floor - before);
        self.segments.retain(|(base, _)| *base >= floor);
        (floor, removed, bytes, rotated)
    }
    pub fn restart(&mut self, epoch: u64) {
        assert!(epoch > self.epoch);
        self.epoch = epoch;
        self.now = 0;
        for group in &mut self.groups {
            group.next = group.persisted;
            group.issued = group.persisted;
            group.attempts.clear();
        }
    }
}
```

`restart` intentionally discards every volatile attempt, including completed followers. It restores both volatile next and issued next to persisted next and resets process-local lease time to zero. Old token history stays in the driver, outside this model reset, so later commands can test epoch fencing against a restarted owner.

This reset is a restart contract, not the behavior of supervised graceful shutdown. Chapter 17's supervisor requests a final checkpoint. Here the driver deliberately uses the **ordinary low-level join**, which drains appends but keeps the explicit checkpoint policy. Since each scheduled append has already reached its required fence, joining does not introduce a hidden new confirmed append. The fault fixtures later stop owners through actual failure instead.

At this point the reference can describe all relevant healthy states. In Part 2 we put real commands on the owner, compare every completed transition, and make the abandoned-reply fences explicit.
