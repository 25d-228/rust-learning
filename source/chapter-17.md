# Chapter 17 — Cancellation boundaries and supervised shutdown

## Part 1 — Admission closure and the final owner barrier

> **Learning objectives**
> Define exactly which work shutdown must finish; close admission even when every lane is full; preserve accepted appends while cancelling their clients; checkpoint only contiguous completed progress; and distinguish an owner-exit notification from actually joining its thread.

**Implementation time: 4.5 hours for all four parts**, not per page: 0.5 for the owner barrier, 1.25 for the supervisor, 1.25 for session/command integration, and 1.5 for lifecycle tests and debugging. Reading is excluded. Continue with [Part 2 — A supervisor that retains its children](https://25d-228.github.io/rust-learning/chapter-17-part-02.html), [Part 3 — Sessions, commands, and a joined restart](https://25d-228.github.io/rust-learning/chapter-17-part-03.html), and [Part 4 — Controlled shutdown and failure checks](https://25d-228.github.io/rust-learning/chapter-17-part-04.html).

**Prerequisite code:** the complete workspace through [Chapter 16](https://25d-228.github.io/rust-learning/chapter-16.html), especially the three-lane mailbox, `BoundedHandle`, durable `Progress`, pooled responses, and framed sessions. Chapter 5 supplies condition variables and thread joining; Chapter 6 supplies future ownership, pinning, and cancellation. No wire bytes, record bytes, checkpoint bytes, retained-floor rules, or startup-epoch rules change here.

The **16-file overlay** at `book/media/chapter-17/` replaces seven implementation files and adds nine files. Apply it after overlays 1–16 in your learner workspace. The seven replacements are `broker-core/src/mailbox.rs`, `worker.rs`, `worker/bounded.rs`, and `broker-app/src/lib.rs`, `local.rs`, `server.rs`, `bin/brokerd.rs`, all below `crates/`. New files are the core exit-notice module, app supervisor and its unit tests, the shutdown example, and the five-file lifecycle integration target. The manifests and `brokerctl` are unchanged.

For new files, concatenate same-filename blocks in their displayed order. For existing files, replace only the named complete functions, structures, or declarations; the earlier chapter supplies the unchanged code. Full replacement files are also present in the overlay. Assemble all four parts before building.

## Shutdown is an ownership protocol

The server now has two different kinds of children. A Tokio session owns a socket, possibly an unsubmitted request reservation, and possibly a pooled response. An operating-system thread owns each partition's files, delivery windows, progress, and accepted commands. Cancelling the former must not pretend to cancel the latter.

Tokio's [shutdown discussion](https://tokio.rs/tokio/topics/shutdown) separates detecting a reason to stop, notifying participants, and waiting for them. We implement the dependency order ourselves. In particular, a queued `Shutdown` command would be a poor sole mechanism: the lane might already be full, and cross-lane service order would not make that command a barrier over the other lanes.

Our phases are:

```text
Starting -> Serving -> Quiescing -> Draining -> Stopped
    |                      ^           |
    +----------------------+           +----> Failed
```

A startup error can go directly from `Starting` through cleanup. A fatal owner or session panic can move a serving broker into cleanup. `Quiescing` is a short synchronous transition: discard listeners, close all owner admission, notify sessions, and record one network deadline. `Draining` can last arbitrarily long if storage I/O does not return. `Stopped` and `Failed` are terminal reports **after** explicit task and thread joins, not synonyms for “a stop request was sent.”

A clean report does not imply that every consumer finished its external work, that every response reached its peer, or that every outstanding delivery was acknowledged. It means the owned shutdown protocol finished without a recorded fatal failure. Forced network cancellation can coexist with a clean storage shutdown.

## Locate the accepted-work boundary

For partition $p$, let $Q_p$ be the set of commands whose insertion into a mailbox queue completed before admission was closed. A prepared append has a slot reservation, buffer, and disk permit, but is **not yet a member** of $Q_p$. The distinction is observable under the same mailbox mutex used by submission.

| State when admission closes | Owner of resources | Required outcome |
| --- | --- | --- |
| Waiting to reserve capacity | Admission future | Wake, observe `Closed`, release the waiter count |
| Reading a prepared append body | Session | Cannot submit after closure; dropping it returns slot, buffer, and disk credit |
| Command already queued or executing | Partition owner | Process or fail it; abandoning the reply is not rollback |
| Append written but not synchronized | Partition owner | Synchronize before success; retain uncertainty on failure |
| Fetch reply being written | Session/pooled batch | Keep response capacity until the write finishes or the session is dropped |
| External consumer work | Consumer application | Not cancelled or made exactly-once by broker shutdown |

There is one closure point **per partition**, not a global transaction over all four. There is also no new cross-lane FIFO guarantee. A queued acknowledgement can still expire when the owner processes it; shutdown does not override the lease clock. The final checkpoint uses accepted completions, not the consumer's intention to acknowledge.

Consider a data lane with capacity two. The owner is blocked synchronizing command A. B and C are queued, so D cannot reserve another data slot. In a separate trace, B is queued and D holds the second slot as an unsubmitted preparation: the closed owner must drain B but must not wait for D’s missing body. Closing admission must require zero additional queue slots. Once A's synchronization returns, the owner can resolve A, drain B/C and the other committed lanes, perform its final synchronization/checkpoints, and exit. If A never returns, neither a timer nor dropping its response receiver creates a safe replacement owner.

## Make finish intent atomic with admission closure

In `crates/broker-core/src/mailbox.rs`, replace `Queues` with this definition. The new Boolean is protected by the existing mutex; it is not a separately sampled atomic flag.

```rust title="crates/broker-core/src/mailbox.rs"
struct Queues<D, C> {
    data: VecDeque<D>,
    control: VecDeque<C>,
    urgent: VecDeque<C>,
    reserved_data: usize,
    reserved_query: usize,
    closed: bool,
    checkpoint_on_close: bool,
    next_lane: usize,
    #[cfg(test)]
    waiting: bool,
}
```

Replace `Mailbox::with_urgent` with the following complete function. The new field starts false; capacities and preallocation remain as in Chapter 15. The nested `lane` helper stays local because it has no state outside this constructor.

```rust title="crates/broker-core/src/mailbox.rs"
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
                reserved_data: 0, reserved_query: 0, closed: false, checkpoint_on_close: false, next_lane: 1,
                #[cfg(test)] waiting: false,
            }),
            changed: Condvar::new(), space: Arc::new(Notify::new()),
            data_capacity: data, control_capacity: control, urgent_capacity: urgent,
            #[cfg(test)] observed: Condvar::new(),
        })
    }
```

Keep ordinary `close` unchanged. Add these two methods immediately after it inside `impl<D, C> Mailbox<D, C>`. `finish` returns whether the supervised finish request was recorded. Repeating an already recorded finish is harmless. A mailbox closed by a different path cannot be retroactively certified as having executed this barrier.

```rust title="crates/broker-core/src/mailbox.rs"
    // The flag and admission closure have one linearization point. No queue slot is needed.
    pub(crate) fn finish(&self) -> bool {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        if q.closed { return q.checkpoint_on_close; }
        q.checkpoint_on_close = true;
        q.closed = true;
        self.changed.notify_all();
        self.space.notify_waiters();
        true
    }
    pub(crate) fn checkpoint_requested(&self) -> bool {
        self.queues.lock().unwrap_or_else(|e| e.into_inner()).checkpoint_on_close
    }
```

`finish` changes the intent and the admission predicate while holding one lock. It then notifies both kinds of waiters: the condition variable wakes the synchronous owner, and `Notify` wakes the asynchronous capacity waiters. Chapter 15's admission loops already register notifications before rechecking predicates, so no new polling loop is needed.

The poison recovery here does not restore damaged storage. It permits fail-closed cleanup and notification; if an owner panicked, its exit is still fatal. The existing `recv` checks committed queues before returning `Event::Closed`. It does not wait for `reserved_data` or `reserved_query` to become zero. That is deliberate: a stalled body reader must not delay final owner cleanup indefinitely. A later reservation submission sees `closed`, returns ownership, and cannot resurrect the owner.

Do not implement this by sending one urgent command and immediately joining. Urgent capacity can be full; even when available, urgent service can precede queued data. The closure flag plus “all queues empty” predicate is the barrier.

## Commit, then checkpoint, then report an exit

Inside `impl<S: SyncIo> Worker<S>` in `crates/broker-core/src/worker.rs`, add this complete `finish` method before `run`. Replace `run` with the version immediately following it. All command handling and deadline calculations keep their earlier implementations.

```rust title="crates/broker-core/src/worker.rs"
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
            match self.mailbox.recv(deadline) {
                Event::Data(command) => self.publish(command)?,
                Event::Control(command) => self.control(command)?,
                Event::Deadline => {
                    if self.deadline.is_some_and(|end| end <= Instant::now()) { self.commit()?; }
                    self.checkpoint_due(Instant::now())?;
                }
                // Plain close retains the old policy; finish explicitly requests checkpoints.
                Event::Closed => return self.finish(),
            }
        }
    }

```

The first line in `finish` calls the existing group-commit path. It synchronizes dirty record files and created names before moving visible offsets or issuing producer success. A checkpoint must not persist consumer progress beyond synchronized records. Only after commit succeeds do we publish each dirty group's contiguous frontier through the existing same-directory publication protocol.

For a group, define $f$ as the retained floor, $d$ as its persisted next offset, $v$ as its volatile contiguous next offset, and $s$ as the synchronized record end. The existing invariant is

$$
f \,\leq\, d \,\leq\, v \,\leq\, s.
$$

With three issued records at offsets 0, 1, and 2, accepting completion for 2 and then 0 leaves $v=1$. Final checkpointing may move $d$ from 0 to 1; it must not write 3. Record 2 still repeats after restart because the missing completion at 1 prevents retirement of the whole prefix. A different group that did no work retains its own $d=0$.

The group loop is **not an atomic transaction across groups**. If publishing group 0 succeeds and publishing group 1 fails, group 0's authority remains advanced. The owner fails instead of rolling it back or claiming a clean finish. No final retention operation is added. Reclamation remains governed by the earlier explicit maintenance policy.

Ordinary low-level `close` and `join` retain their previous meaning: flush accepted appends, but do not implicitly request consumer checkpoints. Only this explicit finish path changes the policy. That keeps the Chapter 13 restart examples meaningful and gives applications a visible choice rather than silently changing an old method's durability contract.

## Send an exit notice without giving away join ownership

Create `crates/broker-core/src/worker/lifecycle.rs`. The parent already defines `WorkerState` and imports `StorageError`; the child imports them explicitly.

```rust title="crates/broker-core/src/worker/lifecycle.rs"
use std::sync::Arc;
use tokio::sync::watch;
use super::WorkerState;
use crate::storage::error::StorageError;

// A completion notice does not consume the unique permission to join the thread.
#[derive(Debug, Clone)]
pub enum OwnerExit {
    Stopped { state: WorkerState, checkpointed: bool },
    Failed { state: WorkerState, cause: Arc<StorageError> },
    Panicked,
}
pub type ExitWatch = watch::Receiver<Option<OwnerExit>>;
```

A watch channel stores the latest value, not a history of values. Its receiver can inspect the current value and asynchronously wait for a change. Here there is only one meaningful transition, `None` to `Some(exit)`, so coalescing intermediate events cannot lose an exit. See Tokio's [watch channel contract](https://docs.rs/tokio/latest/tokio/sync/watch/index.html). The receiver is cloneable observation permission; it is **not** permission to join or replace a worker.

Replace the `WorkerHandle` structure with the following definition, keeping its existing `Client` and thread-handle imports. Add the lifecycle module declarations near the bottom of `worker.rs`, before `mod bounded`. The bounded child uses the parent's re-export, so it needs no new import list.

```rust title="crates/broker-core/src/worker.rs"
pub struct WorkerHandle {
    client: Client,
    thread: Option<JoinHandle<Result<WorkerState, WorkerError>>>,
    exited: ExitWatch,
}

```

```rust title="crates/broker-core/src/worker.rs"
mod lifecycle;
pub use lifecycle::{ExitWatch, OwnerExit};

```

Replace `WorkerHandle::start_inner` with this complete function. Its startup checks and synchronized initial visibility are unchanged. The new outer thread body translates both an ordinary failure and an unwinding panic into an exit notice after the moved worker has been destroyed.

```rust title="crates/broker-core/src/worker.rs"
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

```

`catch_unwind` returns `Result<R, panic_payload>`; our callable's `R` is itself `Result<WorkerState, WorkerError>`. The final `unwrap_or` maps only an unwinding panic to `WorkerError::Panicked`. Ordinary storage errors still follow `worker.fail`, which closes admission and resolves queued/pending replies as failures.

`AssertUnwindSafe` is an explicit assertion about this boundary, not a repair mechanism. The closure **moves the entire worker** and does not return it on either path. Consequently the program never resumes operations on a partially mutated worker. Shared mailbox and ledger state are only used for refusal and cleanup afterward. The [standard-library panic contract](https://doc.rust-lang.org/std/panic/fn.catch_unwind.html) limits this to unwinding: process abort, forced termination, a double panic during cleanup, and arbitrary failure inside a destructor are not converted into a healthy service. A panic hook can still print a panic even when the test later observes the `Panicked` notice. Review the meaning of [AssertUnwindSafe](https://doc.rust-lang.org/std/panic/struct.AssertUnwindSafe.html) before extending the closure's captures.

`send_replace` retains the terminal value even if no receiver is currently waiting. It runs after the closure has returned or unwound and released the worker's files. The operating-system thread, however, can still be finishing its epilogue or thread-local destruction. Rust's [JoinHandle](https://doc.rust-lang.org/std/thread/struct.JoinHandle.html) gives unique ownership of the final join and the completion synchronization it establishes. **A notice is not that join.** Keep `join_thread`, `WorkerHandle::join`, and the blocking `Drop` fallback unchanged.

Inside `impl WorkerHandle`, add these accessors beside `close`:

```rust title="crates/broker-core/src/worker.rs"
    pub fn exits(&self) -> ExitWatch { self.exited.clone() }
    pub(crate) fn finish(&self) -> bool { self.client.mailbox.finish() }

```

Inside `impl BoundedHandle` in `crates/broker-core/src/worker/bounded.rs`, add the following methods beside the existing `close`. The supervised application uses only durable bounded owners, so a successful checkpointed exit here means that both group checkpoints completed or were already current.

```rust title="crates/broker-core/src/worker/bounded.rs"
    pub fn begin_shutdown(&self) -> bool { self.owner.finish() }
    pub fn exits(&self) -> ExitWatch { self.owner.exits() }
```

## Read the failure trace before adding a supervisor

```text
client             owner 0                    supervisor
submit A --------> write A
                    enter sync gate
cancel reply wait                            request finish
                                              close admission
                    sync returns error
                    fail queued/pending work
                    drop files
                    publish Failed notice --> observe failure
                    return from thread        keep remaining owners
                                              drain all sessions
                                              join all four threads
                                              report Failed
```

No success for A appears in this trace. Its bytes may nevertheless exist on disk; recovery, not a guessed rollback, decides what complete records survive. If the gate never returns, there is no exit notice and the supervisor must remain in `Draining`. The next part owns that state and the handles required to resume waiting.

**Check before continuing:** explain why a full urgent lane cannot prevent `finish`, why a held prepared body does not belong to the barrier, and why the `checkpointed` field is not proof that external consumer effects happened exactly once. Then continue to [Part 2](https://25d-228.github.io/rust-learning/chapter-17-part-02.html).

