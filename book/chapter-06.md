# Chapter 6 — Futures, async ownership, and cancellation

**Prerequisites:** [Chapter 5](https://25d-228.github.io/rust-learning/chapter-05.html) supplies shared ownership, `Send`/`Sync`, condition-variable waiting, owned reservations, destruction, and joining. Chapters 1–4 supply the Cargo workspace, borrowed slices, fallible functions, generic bounds, and closures. Keep all five earlier programs. Their example-private types remain private; this chapter does not invent imports from them.

**Implementation time:** 2.5 hours: 1.5 for the program and its focused tests, and 1.0 for cancellation and compiler experiments. Reading is excluded. Record actual coding/debugging time separately; the estimate is not a requirement to spend longer after the behavior is understood.

A thread waiting on Chapter 5's condition variable stops running until it can recheck the protected state. An asynchronous task instead preserves its unfinished computation in a value and returns control to a runtime. That value still owns things: a payload, a capacity reservation, a partially completed operation, or just a receiver waiting for somebody else's result. Cancelling the value destroys what it owns. It does not destroy what it has already transferred elsewhere.

We will make that distinction observable with one request, one worker, and one outstanding-work permit. The worker's calculation is deliberately just a payload byte count. The exercise concerns resource ownership and cancellation, not a miniature storage engine. The actual broker begins in Chapter 7.

> **Learning objectives**
>
> Explain what polling and waking do; distinguish a suspended task from a blocked thread; use `Pin` without unsafe code; carry an owned permit through suspension and handoff; predict the effects of dropping a future, dropping a task handle, and aborting a task; and establish cancellation boundaries with explicit handshakes rather than sleeps.

## Extend the workspace with only the required runtime features

The workspace still has one package, `language-lab`, and uses edition 2024. Leave the root `Cargo.toml`, library modules, and earlier examples alone. Replace the package manifest with the following complete contents. Tokio is a development dependency because it is used by this package's examples and tests, not by its library interface.

```toml title="crates/language-lab/Cargo.toml"
[package]
name = "language-lab"
version = "0.1.0"
edition = "2024"
publish = false

[dev-dependencies]
tokio = { version = "1", default-features = false, features = ["rt", "macros", "sync", "time"] }
```

The `rt` feature supplies the runtime and task spawning. `macros` supplies the asynchronous entry/test attributes and `select!`. `sync` supplies channels and permits. `time` supplies diagnostic watchdogs. We do not need networking, filesystem wrappers, or the multithreaded scheduler for this exercise. Tokio's [feature list](https://docs.rs/tokio/latest/tokio/index.html#feature-flags) describes these boundaries.

`version = "1"` accepts compatible 1.x releases; it is not an exact lock or an instruction to install the oldest 1.x version. Use a maintained compatible toolchain and dependency resolution. Keep the workspace's existing edition requirement, inspect the resolved dependency with `cargo tree -p language-lab`, and retain the workspace `Cargo.lock` that Cargo generates. An existing lockfile may keep a compatible version already in use. Do not invent a Rust LTS channel or downgrade to match a documentation screenshot.

Create `crates/language-lab/examples/cancel_permits.rs`. Every Rust fence whose title is **exactly that path**, in reading order, contributes to the file. Later scratch compiler experiments have different titles and do not belong in it. The reference [source](media/chapter-06/crates/language-lab/examples/cancel_permits.rs) and complete updated [package manifest](media/chapter-06/crates/language-lab/Cargo.toml) are provided as an overlay on Chapter 1's workspace plus Chapters 2–5. These media files are not a separate Cargo workspace.

```sh title="Terminal — prepare the example location"
mkdir -p crates/language-lab/examples
rustc --version
cargo --version
```

The entry point and asynchronous tests explicitly select `current_thread`. Tokio can alternate between runnable tasks on that thread; this is concurrency, not parallel execution. No background executor thread is implied. The runtime must remain driven for these tasks to progress. See the [current-thread builder](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.new_current_thread). This setup works with the native Apple-silicon target; it has no Linux-only dependency or machine-specific performance claim.

## A future is stored work, not a newly created thread

Calling an `async fn` produces a future. Its arguments are evaluated and transferred according to ordinary Rust ownership rules at the call. The function body executes only when that future is polled. Thus `request(..., b"cat".to_vec(), ...)` allocates and transfers a vector immediately, even though the request body has not yet acquired capacity. “Lazy body” does not mean “no resources have been acquired anywhere.”

An `.await` polls a child future. If the child is ready, execution can continue immediately. If it is pending, the surrounding computation can suspend, keeping the state needed to resume. Not every `.await` yields to another task. A long non-yielding loop still monopolizes the runtime thread. Tokio's [async model](https://tokio.rs/tokio/tutorial/async#futures) explains this transformation; here we will inspect its ownership consequences directly.

Start with the imports:

```rust title="crates/language-lab/examples/cancel_permits.rs"
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle};
```

A future implements the `Future` trait. Its associated type `Output` names its one eventual result type; it is chosen by the implementation, not independently by each caller. The method `poll` receives a pinned mutable reference to the computation and a `Context` carrying its current task's wake handle. Its result is `Poll::Pending` or `Poll::Ready(value)`. An error can itself be the value: `Poll::Ready(Err(error))` is a completed fallible operation, not unfinished work.

On a pending path, something must arrange a wake when progress becomes possible. A wake requests another poll; it does not run the continuation inline or promise completion. Do not busy-loop over `poll`, and do not poll a completed future again. These are the operational rules of [`Future::poll`](https://doc.rust-lang.org/std/future/trait.Future.html#tymethod.poll).

Implement a small future that requests exactly one repoll, followed by a borrowed asynchronous function:

```rust title="crates/language-lab/examples/cancel_permits.rs"
#[derive(Default)]
struct YieldOnce {
    yielded: bool,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.yielded {
            Poll::Ready(())
        } else {
            this.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn borrowed_len(bytes: &[u8]) -> usize {
    YieldOnce::default().await;
    bytes.len()
}
```

`YieldOnce` stores one Boolean. On its first poll, it changes that Boolean, requests a wake, and returns pending. On the next poll it returns the unit value `()`. It does not allocate a task, read the clock, or sleep. This is a finite polling demonstration, not a fairness primitive: a requested repoll does not guarantee that any particular other task runs first.

`type Output = ();` is the associated-type definition. `Self::Output` in the method signature names that choice. `self.get_mut()` is safe here because a Boolean-only `YieldOnce` is automatically `Unpin`: it has no address-sensitive state. We mutate `yielded` through the resulting ordinary `&mut YieldOnce`.

`borrowed_len` keeps a slice reference through the suspension, then reads its length. Awaiting it locally is valid while the caller's bytes remain borrowed. Async borrowing does not require all data to be owned, cloned, or static. What matters is how long the future may remain alive. Spawning it independently introduces additional requirements later in this chapter.

## Pinning protects a pointee, not a task or a thread

A compiler-generated asynchronous state machine can hold a reference into another part of its own saved state. Moving that state after the reference is established could invalidate the reference. `Pin` represents the restriction needed to prevent such a move for an address-sensitive value. `Unpin` marks types that do not require that restriction. Neither trait concerns whether a task can cross threads; that is the separate role of `Send`.

For our tests, `Box::pin(future)` creates an owned `Pin<Box<F>>` with a stable allocation for `F`. Moving the box handle does not move its pointee. Calling `as_mut()` creates a temporary `Pin<&mut F>` that borrows that same pointee. Dropping the borrowing wrapper ends a borrow; dropping the owning box destroys the future in place. Pinning does not prevent destruction, poll the future, or spawn it. The standard library's [pinning contract and `Unpin` discussion](https://doc.rust-lang.org/std/pin/index.html) specify these distinctions.

We will not implement a self-referential type, project pinned fields manually, or use `unsafe`. Tokio and the compiler manage the necessary polling machinery. The one manual future above and a one-poll test probe are enough to see the mechanism without writing an executor.

## Attach capacity to work rather than to the waiting caller

A mailbox slot and an outstanding-work permit have different lifetimes. A mailbox slot becomes available when its queued job is removed. The outstanding-work permit must remain held while that job is executing. Otherwise a fast dequeue would free admission even though the worker still owns the payload and unfinished work.

Define the small error and ownership types:

```rust title="crates/language-lab/examples/cancel_permits.rs"
#[derive(Debug, PartialEq, Eq)]
enum RequestError {
    BudgetClosed,
    Empty,
    GateClosed,
    InboxClosed,
    ReplyLost,
    Cancelled,
}

struct Job {
    payload: Vec<u8>,
    permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct WorkReport {
    bytes: usize,
    reply_sent: bool,
}
```

`Job` owns the vector, the permit, and the reply sender. None is cloned during handoff. [`OwnedSemaphorePermit`](https://docs.rs/tokio/latest/tokio/sync/struct.OwnedSemaphorePermit.html) owns a reference to its semaphore instead of borrowing a caller-local semaphore handle, so it can travel with a job. Its destructor returns its capacity. We do not call `forget`, add permits dynamically, or release by incrementing a separate counter.

A one-shot channel carries at most one result. Its sender transfers a value synchronously; its receiver is a future. Sending can succeed before the receiver is polled. Dropping the sender without a value makes the receiver fail. These properties come from Tokio's [one-shot channel](https://docs.rs/tokio/latest/tokio/sync/oneshot/index.html), not a polling loop in our code.

`reply_sent` means the worker's attempt to send was accepted by that channel. It does **not** prove that the caller consumed the result, stored it durably, or performed an external action. The stronger statement in the normal-completion test comes from also joining the caller and checking its result.

`RequestError` remains a concrete enum for assertions. It need not implement `Display` or `Error` merely to use `Result`; those traits become useful at a reporting boundary, as Chapter 3 explained. `Cancelled` describes the caller's stopped wait. It does not encode whether the worker has already accepted the job. `ReplyLost` similarly is not a proof that no work happened.

### Count the current owner of each permit

Let $C$ be the configured outstanding-work limit, $f$ the currently free permits, $p$ permits owned by preparing requests, $q$ permits in queued jobs, and $x$ permits in executing jobs. At our handshaken snapshots, with no acquisition holding an internally assigned but not yet returned permit:

$$
f+p+q+x=C
$$

An asynchronous semaphore can also assign capacity to an acquisition that has not yet returned its owned permit. Call that internal amount $a$. The general accounting includes it:

$$
f+a+p+q+x=C
$$

A pending waiter does not necessarily own a permit; $a$ counts assigned permits, not the number of waiting tasks. The controlled one-request traces keep $a=0$. Do not infer admission correctness from a racing `available_permits()` observation. The acquire operation, not an observed count, grants permission.

For $C=1$, the intended states are:

| State | Free f | Preparing p | Queued q | Executing x |
| --- | ---: | ---: | ---: | ---: |
| No work | 1 | 0 | 0 | 0 |
| Request holds a permit | 0 | 1 | 0 | 0 |
| Job transferred to mailbox | 0 | 0 | 1 | 0 |
| Worker owns job | 0 | 0 | 0 | 1 |
| Payload destroyed and permit released | 1 | 0 | 0 | 0 |

Handoff changes *which owner* contributes one; it does not create another permit. Cancelling a preparing request can change $p$ from one to zero. Cancelling a reply wait cannot change $x$, because the worker owns that permit now. This difference is the central invariant the executable will check.

## Use a handshake to stop at a named boundary

A sleep does not prove another task has reached a particular instruction. Build a two-message gate instead: the operation sends `reached`, then awaits `resume`. The controller waits for `reached` before making observations, and controls whether `resume` is ever sent.

```rust title="crates/language-lab/examples/cancel_permits.rs"
struct Gate {
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

struct Controller {
    reached: oneshot::Receiver<()>,
    resume: oneshot::Sender<()>,
}

fn gate() -> (Gate, Controller) {
    let (reached_tx, reached_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    (
        Gate {
            reached: reached_tx,
            resume: resume_rx,
        },
        Controller {
            reached: reached_rx,
            resume: resume_tx,
        },
    )
}

impl Gate {
    async fn pause(self) -> Result<(), RequestError> {
        let Self { reached, resume } = self;
        reached.send(()).map_err(|_| RequestError::GateClosed)?;
        resume.await.map_err(|_| RequestError::GateClosed)
    }
}
```

Each pair of fields points in opposite directions. `Gate` belongs to the operation; `Controller` belongs to the experiment. The tuple return is ordinary ownership transfer. `let Self { reached, resume } = self;` destructures the owned gate, moving each endpoint out once.

The message is retained even when its receiver is not yet waiting. There is no “notify before registration” race to solve with timing. Seeing `reached` proves that all code before that send has run; withholding `resume` prevents code after the gate from running. We do not need to assert the exact instruction where the other task is currently parked.

A missing controller is not silently treated as permission. Both failure paths return `GateClosed`. In the request, that happens before handoff and releases local resources. An executing gate is test instrumentation: abandoning it drops the accepted fixture job without computing a result. That case must not be confused with the production broker's eventual policy of finishing accepted operations under a supervisor.

The gate is single-use, not a broadcast shutdown mechanism. It supplies an exact schedule for a small ownership experiment.

## Make the handoff instruction visible

The request first acquires an outstanding-work permit, checks the payload, optionally stops at its preparation gate, reserves a mailbox slot, transfers a `Job`, and finally waits for the reply. Add the complete function:

```rust title="crates/language-lab/examples/cancel_permits.rs"
async fn request(
    budget: Arc<Semaphore>,
    inbox: mpsc::Sender<Job>,
    payload: Vec<u8>,
    prepared: Option<Gate>,
) -> Result<usize, RequestError> {
    let permit = budget
        .acquire_owned()
        .await
        .map_err(|_| RequestError::BudgetClosed)?;
    if payload.is_empty() {
        return Err(RequestError::Empty);
    }
    if let Some(point) = prepared {
        point.pause().await?;
    }

    let slot = inbox
        .reserve()
        .await
        .map_err(|_| RequestError::InboxClosed)?;
    let (reply, response) = oneshot::channel();
    slot.send(Job {
        payload,
        permit,
        reply,
    });
    response.await.map_err(|_| RequestError::ReplyLost)
}
```

`acquire_owned()` consumes the `Arc<Semaphore>` argument and eventually produces a permit or `BudgetClosed`. A caller that wants to inspect the same semaphore keeps its own `Arc` and passes a clone. A cancelled acquisition loses its queue position; preserving capacity accounting is not the same as preserving admission fairness across repeated cancellation and retry. See [`Semaphore::acquire_owned`](https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html#method.acquire_owned).

Validation intentionally occurs **after** admission here so an early `return` demonstrates permit cleanup. A real interface should reject an obvious empty payload before waiting when that is its policy. With our chosen order, a closed budget takes precedence over `Empty`, and an empty request can wait behind a full budget. The test states this choice explicitly rather than leaving it accidental.

The binding `permit` is an owner with a destructor. Do not replace it with `let _ = budget.acquire_owned().await...`: the wildcard pattern does not retain the successful owned permit. A name such as `_permit` would retain it, but we use `permit` because we subsequently move it into the job.

`inbox.reserve().await` waits only for a queue slot; it has not taken ownership of a `Job`. Its borrowed slot permit is consumed by the synchronous `slot.send(...)`. There is no `.await` between reserving the slot and moving the job. The transfer instruction is therefore easy to locate. It is an ownership handoff, **not** durable acceptance or proof that a worker will finish. A receiver can still disappear. Tokio documents [slot reservation and dropping unused reservations](https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Sender.html#method.reserve).

Calling `send(job).await` inside a cancelled branch instead moves the job into the send future, which can be dropped before transfer. Reserving first separates “wait for room” from “move the value.” But our cancellation wrapper owns the **whole request**, including its payload. Cancelling that whole request before handoff still destroys that payload. This example does not return an unsent vector for retry. To retain it across cancellation, the retry owner must keep it outside the cancelled scope or receive it through an explicit ownership-returning API.

After `slot.send`, `request` no longer owns the vector or permit. It owns the response receiver. Destroying that receiver does not reach into the mailbox or worker and reclaim the moved job. This follows from the exact fields being moved, not from a special immunity granted to accepted work.

## Release payload resources before reporting normal completion

The worker's complete operation is small enough to inspect in full:

```rust title="crates/language-lab/examples/cancel_permits.rs"
async fn complete(job: Job, executing: Option<Gate>) -> Result<WorkReport, RequestError> {
    if let Some(point) = executing {
        point.pause().await?;
    }
    let Job {
        payload,
        permit,
        reply,
    } = job;
    let bytes = payload.len();
    drop(payload);
    drop(permit);
    let reply_sent = reply.send(bytes).is_ok();
    Ok(WorkReport { bytes, reply_sent })
}
```

The optional executing gate precedes the calculation and keeps the whole `Job` alive through suspension. Destructuring after the gate gives separate names to the three owners. The byte count uses the vector length, not UTF-8 character count or capacity. `cat` contains three bytes, so `bytes` is three.

The worker then destroys the vector, returns its outstanding-work permit, and attempts the reply, in that order. Consequently a caller that receives a normal reply can observe that this job no longer holds its permit. In a larger concurrent system another job might already have acquired that capacity; the promise is release by this job, not a globally free count of one.

This function deliberately finishes even when the receiver has gone away. `reply.send(bytes).is_ok()` records success or rejection without turning an abandoned reply into a panic. The work has already happened. There is no `reply.is_closed()` condition skipping the calculation.

An early error at the fixture gate or ordinary destruction of an unfinished `complete` future drops its `Job` fields. That restores the permit but does not complete the calculation. Cleanup is therefore not synonymous with successful processing. Nor does a destructor perform asynchronous durability work: an explicit flush/drain/join path is needed for that later. Process termination can bypass Rust destructors altogether.

## Select a caller outcome without inventing rollback

The caller can either wait for its operation or stop waiting. Implement that policy once:

```rust title="crates/language-lab/examples/cancel_permits.rs"
async fn cancel_on<F>(operation: F, cancelled: oneshot::Receiver<()>) -> Result<usize, RequestError>
where
    F: Future<Output = Result<usize, RequestError>>,
{
    tokio::select! {
        biased;
        _ = cancelled => Err(RequestError::Cancelled),
        result = operation => result,
    }
}
```

`F: Future<Output = Result<usize, RequestError>>` constrains the associated output type. The helper accepts any future with that result shape. It does not itself need `F: Send` or `F: 'static` when awaited locally; spawning the resulting future imposes those bounds at the spawn call.

The macro owns both branch futures. `biased;` makes it check the cancellation branch first whenever both can produce a matching result. The `_` pattern accepts either a cancellation message or closure of its sender. Here losing the cancellation controller is deliberately a reason to cancel, not a reason to keep waiting forever. Keep that sender alive on a normal path.

When a branch wins, the remaining owned future is dropped. Tokio's [`select!` cancellation documentation](https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety) makes this dependence on the passed future explicit. A branch that only borrows an externally owned future would have a different destruction boundary. We test that distinction with a pinned borrowing wrapper below.

`biased` is a polling preference, not a transaction protocol. It cannot undo a handoff performed on a previous poll. A cancellation signal can also arrive while the operation is running its current poll. Even when cancellation wins, the job might already be queued, executing, or completed. Our controlled experiments know which state they established; an ordinary caller receiving `Cancelled` does not. A retry policy must not reinterpret that result as “definitely not accepted.”

There is no `await` inside the synchronous transfer, but that does not make all surrounding side effects globally atomic. It only gives this example a precise point where ownership changes.

## Spawn owned state; observe task termination separately

Tokio's [`spawn` bounds](https://tokio.rs/tokio/tutorial/spawning#send-bound) require the future and its output to be `Send + 'static`. `Send` allows the runtime to move suspended task state between threads; it does not mean two threads poll the same future simultaneously. `Sync` is not an additional general spawn requirement. Our current-thread runtime still uses this same `tokio::spawn` API and its bounds.

`'static` forbids borrowed data that expires before the independently owned task could finish. It does not mean the task lives forever. Owned vectors, channel endpoints, and `Arc` handles can satisfy it; references to genuinely static data can too. `async move` moves captured values, but moving a short-lived reference does not extend its referent's lifetime.

A `JoinHandle` is permission to observe task completion. Dropping it detaches the task rather than cancelling it. Awaiting a mutable borrow of the handle preserves ownership of the handle if another branch or timeout wins. The [`JoinHandle` contract](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html) also distinguishes cancellation errors from the operation's own result.

Add two fixture helpers. Their five-second timeouts diagnose missing progress; neither establishes task ordering.

```rust title="crates/language-lab/examples/cancel_permits.rs"
async fn observed<T>(receiver: oneshot::Receiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), receiver)
        .await
        .expect("fixture notification timed out")
        .expect("fixture sender disappeared")
}

async fn joined<T>(mut task: JoinHandle<T>) -> Result<T, JoinError> {
    match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
        Ok(result) => result,
        Err(_) => {
            task.abort();
            let _ = task.await;
            panic!("fixture task timed out; cancellation was requested and joined");
        }
    }
}
```

`observed` waits for an actual one-shot message, failing visibly on timeout or sender disappearance. It returns the message's generic type `T`; most gates use `T = ()`.

`joined` passes `&mut task` into the timeout, not the owned handle. If the deadline expires, it still owns the handle, requests abortion, and awaits termination before reporting the fixture failure. Passing the handle by value into a timeout and simply dropping the timed-out wrapper would detach the task instead.

An abort request is not immediate preemption. A task that never yields can prevent a current-thread runtime from servicing the watchdog at all; blocking work already started on a blocking thread is not stopped by an ordinary task abort. The post-abort await is intentionally not advertised as a hard five-second termination guarantee. See Tokio's [task-abort behavior](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html#method.abort) and [timeout behavior](https://docs.rs/tokio/latest/tokio/time/fn.timeout.html).

For a task whose output is `Result<usize, RequestError>`, `joined` returns a nested result: the outside reports a task panic/cancellation, while the inside reports the operation's outcome. The demonstration uses one `unwrap()` to require normal task termination, then compares the operation result. A `Cancelled` operation result is not a `JoinError` cancellation.

## Cancel before transferring the job

The first demonstration stops with a permit in the request, before mailbox reservation and handoff. The nested pattern `let (point, Controller { reached, resume }) = gate()` binds the operation gate and moves both controller endpoints out of the returned tuple:

```rust title="crates/language-lab/examples/cancel_permits.rs"
async fn before_handoff_demo() {
    let budget = Arc::new(Semaphore::new(1));
    let (tx, mut rx) = mpsc::channel(1);
    let (point, Controller { reached, resume }) = gate();
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let operation = request(Arc::clone(&budget), tx.clone(), b"cat".to_vec(), Some(point));
    let caller = tokio::spawn(cancel_on(operation, cancel_rx));

    observed(reached).await;
    assert_eq!(budget.available_permits(), 0);
    cancel_tx.send(()).unwrap();
    assert_eq!(joined(caller).await.unwrap(), Err(RequestError::Cancelled));
    assert_eq!(budget.available_permits(), 1);
    assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(resume.send(()).is_err());
}
```

The original `tx` remains in this function; the request gets a clone. After cancellation the receiver must therefore be **empty**, not disconnected. That stronger observation checks that no hidden job crossed the boundary.

Follow the schedule:

| Controller event | Request-owned state | Available capacity |
| --- | --- | ---: |
| Spawn caller | Not yet known; spawning is not a barrier. | Not asserted. |
| Receive `reached` | Payload and one permit; no job has been sent. | 0 |
| Send cancellation, then join caller | Request future has been destroyed. | 1 |
| Try mailbox receive | No queued job; original sender is still alive. | 1 |
| Try sending `resume` | Rejected because the gate receiver was destroyed. | 1 |

The resumed-operation alternative cannot win: the controller never sends `resume` before cancelling. Waiting for the joined caller ensures cleanup is finished before the capacity assertion. No millisecond delay stands in for that evidence.

## Cancel after the worker owns the job

The second demonstration moves the gate to the worker. The controller does not cancel until that worker has received the `Job`:

```rust title="crates/language-lab/examples/cancel_permits.rs"
async fn after_handoff_demo() {
    let budget = Arc::new(Semaphore::new(1));
    let (tx, mut rx) = mpsc::channel(1);
    let (point, Controller { reached, resume }) = gate();
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let operation = request(Arc::clone(&budget), tx, b"cat".to_vec(), None);
    let caller = tokio::spawn(cancel_on(operation, cancel_rx));
    let worker = tokio::spawn(async move {
        let job = rx.recv().await.expect("one accepted job");
        complete(job, Some(point)).await
    });

    observed(reached).await;
    assert_eq!(budget.available_permits(), 0);
    cancel_tx.send(()).unwrap();
    assert_eq!(joined(caller).await.unwrap(), Err(RequestError::Cancelled));
    assert_eq!(budget.available_permits(), 0);
    resume.send(()).unwrap();
    assert_eq!(
        joined(worker).await.unwrap(),
        Ok(WorkReport {
            bytes: 3,
            reply_sent: false,
        })
    );
    assert_eq!(budget.available_permits(), 1);
}
```

The worker closure is `async move`: it owns `rx` and the gate. The separately spawned caller owns its request. These are two futures, not two borrows into one future. Dropping one cannot drop fields owned exclusively by the other.

At the first `reached`, the mailbox has already been drained into `complete`. Its queue slot is free, but the outstanding-work permit is not. Joining the cancelled caller leaves the available count at zero. Only after `resume` can the worker calculate three bytes, drop the payload and permit, and discover that its reply receiver has disappeared.

The report `WorkReport { bytes: 3, reply_sent: false }` confirms two different facts: the calculation ran, and the attempted response had no receiver. The caller's cancellation result alone confirmed neither. This is the small-scale version of a client disconnecting after a broker accepts an append: the response path and work lifetime must be designed separately.

The worker performs exactly one fixture job. It is not a service loop or a production supervisor. We retain and join its handle, so the normal demonstration observes both task results rather than inferring completion from a dropped sender.

## Drop a task handle and watch the task continue

A future can also be a handle to computation owned elsewhere. The third demonstration intentionally drops one `JoinHandle`, while a separate signal proves that the task continues:

```rust title="crates/language-lab/examples/cancel_permits.rs"
async fn detached_handle_demo() {
    let budget = Arc::new(Semaphore::new(1));
    let task_budget = Arc::clone(&budget);
    let (point, Controller { reached, resume }) = gate();
    let (done_tx, done_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let permit = task_budget.acquire_owned().await.unwrap();
        point.pause().await.unwrap();
        drop(permit);
        done_tx.send(()).unwrap();
    });

    observed(reached).await;
    drop(task);
    assert_eq!(budget.available_permits(), 0);
    resume.send(()).unwrap();
    observed(done_rx).await;
    assert_eq!(budget.available_permits(), 1);
}
```

The gate keeps the task's permit alive after the handle is dropped. At that observation, zero permits are free. The controller then resumes the task; the task explicitly drops its permit **before** sending `done`. Receiving `done` permits the final capacity assertion.

This is intentionally a detachment experiment, not the recommended service lifecycle. The auxiliary message proves execution reached a particular point after resource release. It does not replace joining to observe every possible panic or the precise task termination boundary. If `done` never arrives, the fixture fails; it does not declare the task stopped. Outside this experiment, keep task handles and observe termination.

A related trap is putting an owned `JoinHandle` directly into a losing `select!` branch. Dropping that branch drops the handle, not the spawned task. To retain control, keep the handle outside, select on `&mut handle`, and explicitly decide whether to abort and await it, as `joined` does on timeout.

## Connect the executable

Add the entry point. The runtime attribute creates and drives the selected runtime; it does not change the ownership rules of the ordinary Rust functions above.

```rust title="crates/language-lab/examples/cancel_permits.rs"
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let bytes = vec![10, 20, 30];
    assert_eq!(borrowed_len(&bytes).await, 3);
    println!("polling: borrowed length=3");
    before_handoff_demo().await;
    println!("before handoff: cancelled, free=1, queue=empty");
    after_handoff_demo().await;
    println!("after handoff: cancelled, worker bytes=3, reply_sent=false, free=1");
    detached_handle_demo().await;
    println!("dropped handle: task continued, free=0 then 1");
}
```

The borrowed `bytes` vector lives in `main` until its local asynchronous borrow has completed. Nothing is spawned for `borrowed_len`. The later demonstrations each create their own independent semaphore and channels, so their observations cannot interfere.

Expected output is:

```text title="Expected output — cancel_permits"
polling: borrowed length=3
before handoff: cancelled, free=1, queue=empty
after handoff: cancelled, worker bytes=3, reply_sent=false, free=1
dropped handle: task continued, free=0 then 1
```

These lines are printed only after the corresponding assertions finish. They are not latency measurements. The deterministic output does not require a deterministic runtime schedule; it requires the explicit relationships created by the gates and completion observations.

## Inspect polling without building an executor

The tests use two small instruments. A counting `Waker` checks that `YieldOnce` actually requests a repoll. `poll_once` asks a pinned future for one result using Tokio's current task context and returns that result as data. Add the opening of the test module and its first three tests:

```rust title="crates/language-lab/examples/cancel_permits.rs"
#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, poll_fn, ready};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
    }

    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn yield_once_requests_a_repoll() {
        let count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        let mut future = YieldOnce::default();
        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Pending);
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Ready(()));
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unpolled_body_is_lazy_but_captures_are_owned() {
        let budget = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&budget).acquire_owned().await.unwrap();
        let (started_tx, mut started_rx) = oneshot::channel();
        let future = async move {
            started_tx.send(()).unwrap();
            pending::<()>().await;
            drop(permit);
        };
        assert_eq!(budget.available_permits(), 0);
        assert!(matches!(
            started_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(future);
        assert_eq!(budget.available_permits(), 1);
        assert!(matches!(
            started_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_pending_acquire_does_not_invent_capacity() {
        let budget = Arc::new(Semaphore::new(1));
        let held = Arc::clone(&budget).acquire_owned().await.unwrap();
        let mut waiting = Box::pin(Arc::clone(&budget).acquire_owned());
        assert!(poll_once(waiting.as_mut()).await.is_pending());
        drop(waiting);
        assert_eq!(budget.available_permits(), 0);
        drop(held);
        assert_eq!(budget.available_permits(), 1);
    }
```

This fence intentionally leaves `mod tests` open; the next four test fences continue and close it. They are parts of the same file, not independent modules with duplicate imports.

`Wake` is a safe standard-library interface for an `Arc`-owned wake target. `Waker::from` constructs the task-facing handle without raw pointers or an unsafe `RawWaker`. Our implementation counts calls using the already familiar atomic counter. The default `wake_by_ref` implementation calls through a cloned owner, so the first poll adds one. This counter records a synchronous test event; it is not used to synchronize payload data. See [`Wake`](https://doc.rust-lang.org/std/task/trait.Wake.html).

Read `poll_once` from inside outward. `future.as_mut().poll(cx)` returns an **inner** `Poll<F::Output>`. Wrapping it in `Poll::Ready(...)` makes the **probe** complete immediately with that inner value, even when the value is `Poll::Pending`. [`poll_fn`](https://doc.rust-lang.org/std/future/fn.poll_fn.html) adapts the closure into the outer future. We call this probe at named points, not in a spin loop. It does not promise eventual completion, replace an executor, or consume a ready future twice.

The second test demonstrates a distinction that prose alone often hides: the unpolled body has not sent `started`, but its captured permit is already owned. Dropping the future closes the unexecuted sender and releases the captured permit. No instruction after the permanent `pending()` executes. Destruction, not running the rest of the body, performs cleanup.

The third test starts with the only permit held elsewhere. One poll makes a new acquisition pending. Dropping that acquisition must not invent a free permit: the external holder still owns it. Only dropping `held` restores capacity. This checks cancellation while waiting for admission, before the request can have any owned permit of its own.

## Exercise ordinary errors before exercising cancellation

Add the next tests inside the still-open module:

```rust title="crates/language-lab/examples/cancel_permits.rs"
    #[tokio::test(flavor = "current_thread")]
    async fn early_validation_error_returns_the_permit() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        assert_eq!(
            request(Arc::clone(&budget), tx.clone(), vec![], None).await,
            Err(RequestError::Empty)
        );
        assert_eq!(budget.available_permits(), 1);
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_budget_precedes_payload_validation() {
        let budget = Arc::new(Semaphore::new(1));
        budget.close();
        let (tx, _rx) = mpsc::channel(1);
        assert_eq!(
            request(Arc::clone(&budget), tx, vec![], None).await,
            Err(RequestError::BudgetClosed)
        );
        assert!(budget.is_closed());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_inbox_returns_the_permit() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        assert_eq!(
            request(Arc::clone(&budget), tx, b"cat".to_vec(), None).await,
            Err(RequestError::InboxClosed)
        );
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abandoned_controller_returns_the_permit() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, _rx) = mpsc::channel(1);
        let (point, Controller { reached, resume }) = gate();
        drop(resume);
        assert_eq!(
            request(Arc::clone(&budget), tx, b"cat".to_vec(), Some(point)).await,
            Err(RequestError::GateClosed)
        );
        observed(reached).await;
        assert_eq!(budget.available_permits(), 1);
    }
```

These cases distinguish four decisions: successful admission followed by empty-input rejection; a closed admission source before validation; a receiver gone before slot reservation; and a preparation controller disappearing. The returned error changes, but a permit that was obtained must not remain charged after the request finishes.

The closed-budget test does not interpret `available_permits()` as permission to acquire from a closed semaphore. Closure is a separate condition. Likewise, an existing acquired permit is not retroactively revoked merely because its semaphore closes; the lifetime of its owner still matters.

The abandoned-controller case retains `reached` long enough for the gate's first send to succeed, while dropping `resume` forces the second step to fail. That isolates failure while awaiting permission to continue, rather than accidentally failing an earlier notification.

## Check both sides of handoff and an unsuccessful worker

Add the boundary and completion tests:

```rust title="crates/language-lab/examples/cancel_permits.rs"
    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_before_handoff_restores_capacity() {
        before_handoff_demo().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_after_handoff_keeps_the_worker_permit() {
        after_handoff_demo().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn normal_completion_releases_capacity_before_reply() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        let caller = tokio::spawn(request(Arc::clone(&budget), tx, b"cat".to_vec(), None));
        let job = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.available_permits(), 0);
        assert_eq!(
            complete(job, None).await,
            Ok(WorkReport {
                bytes: 3,
                reply_sent: true,
            })
        );
        assert_eq!(budget.available_permits(), 1);
        assert_eq!(joined(caller).await.unwrap(), Ok(3));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_accepted_job_reports_a_lost_reply() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        let caller = tokio::spawn(request(Arc::clone(&budget), tx, b"cat".to_vec(), None));
        let job = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        drop(job);
        assert_eq!(joined(caller).await.unwrap(), Err(RequestError::ReplyLost));
        assert_eq!(budget.available_permits(), 1);
    }
```

The first two tests reuse the complete demonstration schedules, not a second approximation of the same logic. The normal completion test also joins the caller, so `reply_sent: true` is supplemented by an observed `Ok(3)`. Its semaphore is private to the test; no other request can reacquire between the worker's release and the count assertion.

In the final test, the controller acts as a failed worker: it removes the accepted job and destroys it without calling `complete`. The permit is restored, but the live caller gets `ReplyLost`, not success. The channel therefore did not guarantee processing just because ownership transferred. This is why durable acceptance and supervised worker failure need their own mechanisms in the broker.

The receive timeouts diagnose a stalled fixture. They do not create the ordering: receipt of the actual job does. On a failed assertion or watchdog, this is a failed test, not evidence that all tasks completed normally.

## Distinguish dropping a borrow from dropping an owner

Add two resource-owner tests:

```rust title="crates/language-lab/examples/cancel_permits.rs"
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_a_pin_borrow_does_not_drop_the_owned_future() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        let (point, Controller { reached, resume }) = gate();
        let mut operation = Box::pin(request(
            Arc::clone(&budget),
            tx.clone(),
            b"cat".to_vec(),
            Some(point),
        ));
        assert!(poll_once(operation.as_mut()).await.is_pending());
        observed(reached).await;
        let borrowed = operation.as_mut();
        drop(borrowed);
        assert_eq!(budget.available_permits(), 0);
        drop(operation);
        assert_eq!(budget.available_permits(), 1);
        assert!(resume.send(()).is_err());
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_a_slot_wait_preserves_the_queued_job() {
        let budget = Arc::new(Semaphore::new(2));
        let (tx, mut rx) = mpsc::channel(1);
        let (reply, _response) = oneshot::channel();
        let held = Arc::clone(&budget).acquire_owned().await.unwrap();
        assert!(tx
            .try_send(Job {
                payload: b"first".to_vec(),
                permit: held,
                reply,
            })
            .is_ok());
        let (point, Controller { reached, resume }) = gate();
        let mut operation = Box::pin(request(
            Arc::clone(&budget),
            tx.clone(),
            b"second".to_vec(),
            Some(point),
        ));
        assert!(poll_once(operation.as_mut()).await.is_pending());
        observed(reached).await;
        resume.send(()).unwrap();
        assert!(poll_once(operation.as_mut()).await.is_pending());
        assert_eq!(budget.available_permits(), 0);
        drop(operation);
        assert_eq!(budget.available_permits(), 1);
        let job = rx.try_recv().unwrap();
        assert_eq!(job.payload.as_slice(), b"first");
        drop(job);
        assert_eq!(budget.available_permits(), 2);
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }
```

The first test pins the **owner** in a box, advances it to the preparation gate, then destroys only a `Pin<&mut F>` borrowed from it. The permit stays charged. Destroying the box finally destroys the request and returns the permit. This is also why dropping the temporary result of `as_mut()` is not a cancellation technique.

The second test uses two outstanding-work permits but only one mailbox slot. The already queued `first` job owns one permit. The `second` request obtains the other, passes its gate, and waits for the occupied queue slot. Nothing receives `first` until after the waiting request is destroyed.

The counts are concrete: starting with $C=2$, the queued job leaves one free permit; admitting the second request leaves zero; cancelling it returns exactly one; removing and destroying the first job returns the second. The queued payload remains `first`, proving cancellation did not consume or replace somebody else's accepted work.

The second probe occurs after the resume signal is sent and while the mailbox is still full. It cannot report completion by successfully transferring the second job. The gate and bounded mailbox make the stopping point inspectable without an assumed scheduling delay.

## Give cancellation priority an explicit meaning

Finish the test module:

```rust title="crates/language-lab/examples/cancel_permits.rs"
    #[tokio::test(flavor = "current_thread")]
    async fn ready_cancel_or_closed_sender_wins_a_ready_result() {
        for send_signal in [false, true] {
            let (tx, rx) = oneshot::channel();
            if send_signal {
                tx.send(()).unwrap();
            } else {
                drop(tx);
            }
            assert_eq!(
                cancel_on(ready(Ok(7)), rx).await,
                Err(RequestError::Cancelled)
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_a_join_handle_does_not_abort_the_task() {
        detached_handle_demo().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_then_join_observes_permit_cleanup() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        let (point, Controller { reached, resume }) = gate();
        let task = tokio::spawn(request(
            Arc::clone(&budget),
            tx.clone(),
            b"cat".to_vec(),
            Some(point),
        ));
        observed(reached).await;
        assert_eq!(budget.available_permits(), 0);
        task.abort();
        assert!(joined(task).await.unwrap_err().is_cancelled());
        assert_eq!(budget.available_permits(), 1);
        assert!(resume.send(()).is_err());
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }
}
```

The first test makes the operation result and cancellation branch ready together. With `biased;`, cancellation wins both for an explicit signal and for the controller's disappearance. Choosing to cancel can therefore discard an available result. This behavior is policy, not lost channel data or a promise that the operation had no effects.

The detachment test reuses the third demonstration. The final test stops a real spawned request at its gate, requests `abort`, and then observes a cancellation `JoinError` through `joined`. The gate cannot complete normally because the controller still withholds `resume`. Only after joining does the test assert restored capacity and destruction of the gate receiver.

There are **16 required tests** in this example. They exercise specific resource lifetimes and schedules; they do not enumerate all possible interleavings or prove a production shutdown protocol. No semaphore implementation, executor, or channel implementation is being substituted for the broker mechanisms planned in later chapters. Here those primitives make the language's ownership boundaries visible.

## Read spawn failures as saved-state descriptions

Perform these experiments in a **temporary example file**, not in `cancel_permits.rs`. The file is part of the same Cargo package so it can use the development dependency. Remove it after the experiments; otherwise an intentionally failing example will break `--all-targets`.

### Moving a reference does not extend the referent's life

Create this complete temporary program:

```rust title="crates/language-lab/examples/spawn_bounds.rs — intentionally rejected borrow"
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let text = String::from("cat");
    let task = tokio::spawn(async {
        tokio::task::yield_now().await;
        text.len()
    });
    assert_eq!(task.await.unwrap(), 3);
}
```

```sh title="Terminal — inspect the borrowed spawn rejection"
cargo check -p language-lab --example spawn_bounds
```

Expected behavior is a compiler rejection because the future borrows the caller's local `text` but `spawn` requires an independently ownable `'static` future. Joining immediately does not change that function's type contract. Replace the whole scratch file with this repair:

```rust title="crates/language-lab/examples/spawn_bounds.rs — complete owned repair"
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let text = String::from("cat");
    let task = tokio::spawn(async move {
        tokio::task::yield_now().await;
        text.len()
    });
    assert_eq!(task.await.unwrap(), 3);
}
```

`async move` now transfers the `String` itself. `main` cannot use that moved binding afterward. Creating a local `&str` first and moving only that reference would not be the same repair. Run `cargo run -p language-lab --example spawn_bounds`; expected behavior is successful silent completion.

### A synchronous lock guard must end before suspension

Replace that same temporary file with the next complete, intentionally rejected program:

```rust title="crates/language-lab/examples/spawn_bounds.rs — intentionally rejected guard"
use std::sync::{Arc, Mutex};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let shared = Arc::new(Mutex::new(String::from("cat")));
    let worker_shared = Arc::clone(&shared);
    let task = tokio::spawn(async move {
        let guard = worker_shared.lock().unwrap();
        tokio::task::yield_now().await;
        guard.len()
    });
    assert_eq!(task.await.unwrap(), 3);
}
```

Repeat the `cargo check` command. The future retains a `std::sync::MutexGuard` across `.await`; that guard is not `Send`. The diagnostic should identify saved non-sendable state, not a problem with `Arc` itself. Keeping the task on a local executor to bypass `Send` would not fix the locking hazard: another task trying to lock the same mutex can block the only thread that could resume the lock holder.

Replace the scratch file with this complete version:

```rust title="crates/language-lab/examples/spawn_bounds.rs — complete scoped-guard repair"
use std::sync::{Arc, Mutex};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let shared = Arc::new(Mutex::new(String::from("cat")));
    let worker_shared = Arc::clone(&shared);
    let task = tokio::spawn(async move {
        let length = {
            let guard = worker_shared.lock().unwrap();
            guard.len()
        };
        tokio::task::yield_now().await;
        length
    });
    assert_eq!(task.await.unwrap(), 3);
}
```

The inner block returns an integer and destroys the guard. Only the small integer is needed after suspension. A short synchronous mutex-protected operation can be appropriate; a long blocking critical section on a runtime thread is not. Switching mechanically to an async mutex does not eliminate logical deadlocks or justify holding a lock while awaiting work that needs it. Tokio's [shared-state discussion](https://tokio.rs/tokio/tutorial/shared-state#holding-a-mutexguard-across-an-await) explains both the `Send` diagnostic and the need for deliberate lock scope.

```sh title="Terminal — check the repair and remove the temporary target"
cargo run -p language-lab --example spawn_bounds
rm crates/language-lab/examples/spawn_bounds.rs
```

None of the required example's own state is protected by a user-held lock guard across `.await`. The channel and semaphore manage their internal synchronization. The permit that crosses `.await` is a capacity reservation, not a held mutex guard; confusing those two owners would defeat the design.

## Run the program and keep the cumulative foundation suite

From the learner workspace root, after removing the scratch example:

```sh title="Terminal — build, run, and test Chapter 6"
cargo check -p language-lab --all-targets
cargo run -p language-lab --example cancel_permits
cargo test -p language-lab --example cancel_permits
cargo test -p language-lab --all-targets
cargo fmt --all
cargo fmt --all -- --check
```

The targeted test run should report **16 passed**. The required cumulative count is **75 tests**: 6 library, 8 ownership, 13 parsing, 16 generic-collection, 16 synchronization, and 16 asynchronous tests. Optional additions can increase it. Cargo prints separate target summaries; `--all-targets` or explicit example selection is necessary to include the example tests.

The ordinary example entry points contain additional assertions. Keep running them as programs, not just compiling their test harnesses:

```sh title="Terminal — retain earlier executable checks"
cargo run -p language-lab --example workspace_probe
cargo run -p language-lab --example borrowed_record
cargo run -p language-lab --example fallible_frame
cargo run -p language-lab --example generic_scan
cargo run -p language-lab --example thread_handoff
```

The outputs, test counts, and compiler rejections above are **expected local checks**. No Rust compilation, Cargo execution, rustfmt result, intentional compiler diagnostic, or native macOS run is asserted here. A dependency-resolution failure is not a program test result. Once resolution succeeds, preserve the actual versions in the learner's lockfile and record any platform-specific diagnostics rather than rewriting expectations to hide them.

For a focused cancellation check:

```sh title="Terminal — run one ownership boundary"
cargo test -p language-lab --example cancel_permits cancellation_after_handoff_keeps_the_worker_permit -- --nocapture
```

### Change one boundary and predict the new trace

Temporarily move the `drop(permit)` in `complete` from before `reply.send(bytes)` to immediately after it. The worker still returns capacity on normal completion, but the *general interface guarantee* changes: a concurrently scheduled caller could receive a reply before that release. The current-thread fixture may still pass because there is no intervening suspension in the worker. Passing that fixture does not establish the stronger release-before-reply property. Restore the original order. This exercise separates a code-level ordering argument from a test that merely happened not to expose a race.

Then temporarily remove `biased;` and run `ready_cancel_or_closed_sender_wins_a_ready_result` several times. Without a fixed first branch, its required preference is no longer justified, even if a particular run happens to pass. Restore the policy rather than changing the expectation to accept every outcome. These edits are diagnostic experiments, not alternate reference versions.

## Diagnose the owner and the suspension point

| Symptom | Likely mistake | Correction |
| --- | --- | --- |
| Work runs before the future is awaited | Confusing argument/capture construction with the async body, or spawning it independently. | Identify exactly which function or expression performs the work and who polls its future. |
| A permanently pending operation spins | It wakes itself on every poll without any new progress. | Wake on a real readiness change; keep the finite self-wake only in `YieldOnce`. |
| Capacity returns before the worker finishes | Permit stayed in the caller or was discarded with a wildcard binding. | Move the named permit into the job and retain it until the worker releases the resource. |
| A cancelled reply wait appears to cancel accepted work | The worker lives inside the cancelled future or accepted work is tied to reply interest. | Give accepted work an independent owner and an explicit completion/failure policy. |
| A cancellation error is treated as “never accepted” | The caller's interest was confused with the handoff history. | Track accepted/ambiguous outcomes separately; do not infer rollback. |
| Dropping `as_mut()` does not free capacity | Only a pinned borrow was destroyed. | Drop the owning future or deliberately keep it for resumption. |
| A timed-out task keeps running unexpectedly | An owned `JoinHandle` was dropped by a timeout. | Retain it, await a mutable borrow, then choose abort-and-join or continued observation. |
| Abort is followed by a failing immediate count assertion | Cancellation was requested but task destruction was not observed. | Await the handle before asserting released resources. |
| A test hangs despite a timeout | The runtime thread is blocked or a poll never yields. | Find blocking work or a non-yielding loop; a timer cannot preempt it. |
| All-target checks fail on `spawn_bounds` | An intentional failure target was left in the examples directory. | Complete the repair, then remove the temporary file as instructed. |

## Preserve the boundary without claiming a memory budget

The semaphore limits admitted unfinished jobs, including preparation, queued work, and execution. It does **not** bound the number of tasks waiting to acquire a permit. Each waiting request already owns its payload vector, and allocator capacity can exceed initialized length. The channel's one-slot limit likewise does not include the worker's current job or waiting callers. The owned byte count and the payload allocation's capacity are different quantities.

For a vector-capacity subtotal, let $A_i$ be the vector capacity of each payload still owned anywhere in this fixture. The subtotal in bytes is:

$$
M_{\mathrm{payload}}=\sum_i A_i
$$

This excludes task allocations, channel metadata, vector handles, allocator overhead, and the runtime itself. A limit of one admitted job does not bound this sum when arbitrarily many waiting request futures can each own a vector. The three-byte demonstration is finite because the harness creates a fixed number of tiny requests, not because a semaphore magically enforces a process-wide byte limit.

Later admission work must reserve byte capacity before allocating, bound waiting callers, account for simultaneous copies, and preserve independent progress for acknowledgements. Do not transplant this fixture's preallocated payload or development-only gate into that design without changing its policy.

The six foundation programs are now complete: executable workspace checks; borrowed ownership views; fallible framed parsing; generic bounded storage and lookup; threaded handoff; and asynchronous resource lifetimes. Keep them runnable as the broker develops. [Chapter 7 — Record format, typed offsets, and the I/O boundary](https://25d-228.github.io/rust-learning/index.html#chapter-7-record-format-typed-offsets-and-the-io-boundary) begins `broker-core`. The important carry-forward rule is precise: abandoning an observer is not cancelling a transferred operation, and returning a permit is not proof of successful or durable work.
