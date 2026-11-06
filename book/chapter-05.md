# Chapter 5 — Shared ownership, threads, and synchronization

A full data queue must not prevent the very control operation that lets the system make progress. Nor should a producer permanently consume capacity because it reserved a slot and then returned an error. These are different problems: scheduling chooses which accepted message to remove; admission decides whether a new message may enter; cleanup returns capacity that was acquired but never used.

This chapter implements those mechanisms in a small **two-lane, in-memory handoff**. Producers transfer owned strings to one consumer thread. Data and control have independent item limits. A reservation owns one unfilled slot, a successful commit transfers a message, and closure rejects future commits while allowing already committed messages to drain. This is language and synchronization groundwork, not a durable broker or a completed task-processing system.

**Implementation time:** 2.5 hours: 1.5 for the program and 1.0 for synchronization, cleanup, and compiler experiments. Reading is separate. Record actual implementation and debugging time; the length of the explanation does not change that estimate.

**Prerequisites:** the Cargo workspace from [Chapter 1](https://25d-228.github.io/rust-learning/chapter-01.html), ownership and lifetime-bound views from [Chapter 2](https://25d-228.github.io/rust-learning/chapter-02.html), fallible control flow from [Chapter 3](https://25d-228.github.io/rust-learning/chapter-03.html), and traits, closures, and bounded collections from [Chapter 4](https://25d-228.github.io/rust-learning/chapter-04.html). Keep all their files unchanged. No library export, manifest edit, new dependency, or additional toolchain feature is required.

> **Learning objectives**
> Separate shared ownership from shared mutation; explain `Send` and `Sync`; protect a compound capacity invariant with one mutex; wait on predicates rather than notifications; implement ownership-bearing reservations with `Drop`; distinguish admission closure from processing completion; and join the worker before interpreting its final observations.

## Add one synchronized example

Create `crates/language-lab/examples/thread_handoff.rs`. The **15 Rust panels labelled with that exact path**, in page order and with a blank line between them, form the complete file. Several panels continue one test module; their boundaries are stated. Scratch compiler experiments have different labels and do not belong in the example.

The assembled [reference source](https://25d-228.github.io/rust-learning/media/chapter-05/crates/language-lab/examples/thread_handoff.rs) is included under `book/media/chapter-05/`. Copy it into the existing learner workspace or a separate scratch workspace, not another Cargo project. Do not build inside the book's media directories.

Chapter 4's `Ring<T>` remains private to `generic_scan.rs`. This example uses the standard library's `VecDeque` as the lane's storage container; it does not pretend that the earlier ring is an exported library type. You implement reservation accounting, synchronization, wakeup policy, lane selection, closure, and ownership transfer yourself. A container is not a channel and supplies none of those policies.

## Shared ownership does not imply permission to mutate

`Rc<T>` gives several owners access to one allocation on one thread. `Rc::clone` creates another owner of that allocation; it does not clone the inner `T`. The last owning handle's destruction releases the inner value. Its non-atomic reference count is unsuitable for cross-thread ownership; see the standard library's [`Rc` documentation](https://doc.rust-lang.org/std/rc/index.html).

`RefCell<T>` solves a different problem: it checks borrowing dynamically. `borrow()` returns a shared-borrow guard; `borrow_mut()` requests an exclusive guard. Conflicting requests panic, while the corresponding `try_` operations report errors. A guard releases its borrow when dropped, not simply when its last ordinary read occurs. These are the [`RefCell` borrowing rules](https://doc.rust-lang.org/std/cell/struct.RefCell.html#method.borrow), not synchronization between threads.

Start the example with its imports and a local experiment:

```rust title="crates/language-lab/examples/thread_handoff.rs"
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;

fn local_aliases() {
    let notes = Rc::new(RefCell::new(vec![String::from("created")]));
    let alias = Rc::clone(&notes);
    {
        let view = notes.borrow();
        assert_eq!(view.len(), 1);
        assert!(alias.try_borrow_mut().is_err());
    }
    alias.borrow_mut().push(String::from("ready"));
    assert_eq!(notes.borrow()[1], "ready");
    assert_eq!(Rc::strong_count(&notes), 2);
    drop(alias);
    assert_eq!(Rc::strong_count(&notes), 1);
}
```

There is one vector allocation owned through two `Rc` handles. While `view` exists, the attempted exclusive borrow through `alias` fails even though `view` was last read on the preceding line. The closing brace drops the guard. Only then does `borrow_mut().push(...)` succeed. That temporary exclusive guard ends after the statement. Dropping `alias` changes the owner count, not the vector's contents.

This is **interior mutability**: a type provides controlled mutation through a shared reference. It does not mean that ordinary `&T` references suddenly permit arbitrary writes. `RefCell` supplies runtime borrow checks; a mutex will supply a lock and a guard. Neither `Rc` nor `Arc` by itself makes an inner mutable collection safely shareable.

### What `Send` and `Sync` actually permit

`Send` permits transferring ownership of a value between threads. `Sync` permits sharing references between threads: precisely, `T: Sync` means `&T: Send`. These marker traits describe valid transfers and access, not queue order, fairness, or termination. Their role is explained in the Rust Book's [`Send` and `Sync` chapter](https://doc.rust-lang.org/book/ch16-04-extensible-concurrency-sync-and-send.html) and the [`Sync` definition](https://doc.rust-lang.org/std/marker/trait.Sync.html).

For the ordinary forms used here:

| Type | Ownership and mutation | Cross-thread condition |
| --- | --- | --- |
| `Rc<T>` | Shared allocation, non-atomic owner count. | Neither `Send` nor `Sync`. |
| `RefCell<T>` | Runtime-checked borrowing of one owned value. | `Send` when `T: Send`, but not `Sync`. |
| `Arc<T>` | Shared allocation, atomic owner count. | `Send` and `Sync` when `T: Send + Sync`. |
| `Mutex<T>` | A guard permits exclusive access to `T`. | `Send` and `Sync` when `T: Send`. |

The subtle row is `RefCell<T>`: moving a whole `RefCell<String>` to another thread is permitted, but sharing simultaneous references to it is not. Consequently `Arc<RefCell<String>>` is not the threaded replacement for `Rc<RefCell<String>>`. The atomic owner count does not make the inner borrow counter synchronized. The [`Arc` thread-safety contract](https://doc.rust-lang.org/std/sync/struct.Arc.html#thread-safety) states this distinction directly.

Our shared object will instead contain `Mutex<State>`. A producer moves a `String` into that state; the consumer later moves it out. The string is never mutated simultaneously through aliases. No manual `unsafe impl Send` or `unsafe impl Sync` is needed or appropriate.

## Count promises as well as queued messages

Let lane index $i=0$ identify data and $i=1$ identify control. For each lane, define:

- $c_i$: its configured positive item limit;
- $q_i$: the number of committed messages still in its queue;
- $r_i$: the number of live, uncommitted reservations.

All three are counts of items, not bytes. Available capacity is:

$$
f_i = c_i - q_i - r_i
$$

The invariant is:

$$
0 \le q_i + r_i \le c_i
$$

A reservation must count immediately. Otherwise two producers can both see an empty final slot, prepare messages independently, and both claim it. Protecting only the eventual `push_back` would serialize insertion but still permit over-admission.

For a data limit of three, two queued messages and one reservation give $f_0=3-2-1=0$. An empty control lane with limit two still has $f_1=2-0-0=2$. Control admission consults its own lane; it never first acquires a data reservation.

The transitions, all under one state mutex, are:

| Operation on lane i | Queue count | Reservation count | Message owner afterward |
| --- | --- | --- | --- |
| Reserve one free slot | Unchanged | Add one | Caller still owns any message. |
| Commit an active reservation | Add one | Subtract one | Lane queue. |
| Drop an unused reservation | Unchanged | Subtract one | Caller, if it still has a message. |
| Receive a committed message | Subtract one | Unchanged | Consumer. |
| Close admission | Unchanged | Unchanged | Existing ownership is not erased. |

The commit row preserves $q_i+r_i$: a promise becomes an actual queued item. Receive and cancellation reduce it. Only reserve increases it, and reserve first checks room.

Add the lane vocabulary and container:

```rust title="crates/language-lab/examples/thread_handoff.rs"
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lane {
    Data,
    Control,
}

impl Lane {
    fn index(self) -> usize {
        match self {
            Self::Data => 0,
            Self::Control => 1,
        }
    }

    fn other(self) -> Self {
        match self {
            Self::Data => Self::Control,
            Self::Control => Self::Data,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReserveError {
    Full,
    Closed,
}

struct LaneState {
    queue: VecDeque<String>,
    reserved: usize,
    limit: usize,
}

impl LaneState {
    fn new(limit: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(limit),
            reserved: 0,
            limit,
        }
    }

    fn has_space(&self) -> bool {
        self.queue.len() < self.limit - self.reserved
    }
}
```

`Lane::index` maps semantic names to the fixed two-element array. `other` selects the alternative without unexplained numeric arithmetic. Both take `self` by value because `Lane` is `Copy` metadata.

`has_space` uses `queue.len() < limit - reserved` rather than adding two machine integers. The invariant guarantees `reserved <= limit`, so subtraction is defined. This is the integer form of $q_i+r_i<c_i$. Do not replace the independent lane checks with one combined total: unused control slots must not become overflow room for data.

[`VecDeque::with_capacity`](https://doc.rust-lang.org/std/collections/struct.VecDeque.html#method.with_capacity) reserves room for at least the requested number of elements; it does not impose a maximum. Our transitions impose the maximum. Since an active reservation implies the queue has room below its configured limit, commit does not need to grow the deque's slot allocation. Allocator failure during construction is not represented by `ReserveError`.

## Put the whole invariant behind one mutex

A `Mutex<State>` protects both lanes, closure, and the scheduling cursor together. Locking returns a `MutexGuard`; accessing fields through the guard uses its dereferencing implementations. A mutable guard permits mutation, and dropping it unlocks the mutex. The [`MutexGuard` contract](https://doc.rust-lang.org/std/sync/struct.MutexGuard.html) also makes it non-`Send`: transfer owned work, not an acquired lock, to the worker.

The next declarations include a reservation that owns an `Arc<Handoff>`. It keeps the shared state alive until it either commits or returns its slot. It is deliberately neither `Clone` nor `Copy`.

```rust title="crates/language-lab/examples/thread_handoff.rs"
struct State {
    lanes: [LaneState; 2],
    closed: bool,
    next: Lane,
    popped: usize,
    #[cfg(test)]
    waits_started: usize,
}

struct Handoff {
    state: Mutex<State>,
    changed: Condvar,
    #[cfg(test)]
    wait_observed: Condvar,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Snapshot {
    queued: [usize; 2],
    reserved: [usize; 2],
    closed: bool,
    popped: usize,
}

struct Reservation {
    handoff: Arc<Handoff>,
    lane: Lane,
    active: bool,
}

impl Handoff {
    fn new(data_limit: usize, control_limit: usize) -> Option<Arc<Self>> {
        if data_limit == 0 || control_limit == 0 {
            return None;
        }
        Some(Arc::new(Self {
            state: Mutex::new(State {
                lanes: [LaneState::new(data_limit), LaneState::new(control_limit)],
                closed: false,
                next: Lane::Control,
                popped: 0,
                #[cfg(test)]
                waits_started: 0,
            }),
            changed: Condvar::new(),
            #[cfg(test)]
            wait_observed: Condvar::new(),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(state) => state,
            Err(_) => std::process::abort(),
        }
    }
}
```

`Handoff::new` returns `None` for either zero limit. That is its only recoverable configuration rejection; it is not an allocator-failure interface. `Arc::new` owns the complete handoff, and later `Arc::clone` operations share it without copying its queues. The `Snapshot` contains only copied metadata and no references into the lock-protected state.

The two `#[cfg(test)]` fields exist only in the test build. They let tests observe that an operation entered its waiting path without sleeping and guessing. They are not required to make admission or selection correct. The annotation is the same conditional-compilation mechanism used for the test modules in earlier chapters.

### Choose a failure policy for poisoned state

A standard mutex can report poisoning after a panic while locked. Poisoning is advisory, not a complete proof that state is damaged or sound; [`Mutex` documents its limits](https://doc.rust-lang.org/std/sync/struct.Mutex.html#poisoning). Continuing through `into_inner()` would require a reason why every possibly interrupted transition remains usable.

This exercise instead makes poisoned internal state **process-fatal**. `lock` aborts rather than pretending to repair accounting or panicking again from a reservation destructor. Normal fullness, closure, and validation failures do not poison anything. No user handler, formatter, I/O operation, or payload destructor is intentionally run while holding the state lock.

[`process::abort`](https://doc.rust-lang.org/std/process/fn.abort.html) does not unwind or run destructors. Therefore our cleanup and join guarantees concern normal returns and ordinary Rust unwinding outside the critical section, not abort, allocator termination, or process killing. This is an explicit limit of the example, not a claim of graceful recovery from arbitrary panic. Later storage owners require their own recoverable I/O-error protocol.

## A condition variable waits for a predicate to change

The producer's waiting predicate for lane $i$ is:

$$
W_i = (\text{admission is open}) \land (q_i+r_i=c_i)
$$

The consumer's waiting predicate is:

$$
W_{\text{consumer}} = (\text{admission is open}) \land (q_0=0) \land (q_1=0)
$$

A false predicate does not necessarily mean success: a producer can wake to discover closure; a consumer can wake to discover closure with nothing remaining. Each operation interprets the state again under the lock.

`Condvar::wait` consumes the guard, atomically releases the mutex as it begins waiting, and returns a guard after reacquiring that mutex. A return can be spurious or irrelevant to this particular caller. Notifications are not buffered messages. These are the essential rules in the [`Condvar` API](https://doc.rust-lang.org/std/sync/struct.Condvar.html#method.wait).

Add the helper and snapshot operation:

```rust title="crates/language-lab/examples/thread_handoff.rs"
impl Handoff {
    fn wait<'a>(&self, state: MutexGuard<'a, State>) -> MutexGuard<'a, State> {
        #[cfg(test)]
        let state = {
            let mut state = state;
            state.waits_started += 1;
            self.wait_observed.notify_all();
            state
        };
        match self.changed.wait(state) {
            Ok(state) => state,
            Err(_) => std::process::abort(),
        }
    }

    fn snapshot(&self) -> Snapshot {
        let state = self.lock();
        Snapshot {
            queued: [state.lanes[0].queue.len(), state.lanes[1].queue.len()],
            reserved: [state.lanes[0].reserved, state.lanes[1].reserved],
            closed: state.closed,
            popped: state.popped,
        }
    }
}
```

The lifetime `'a` belongs to the incoming guard and is preserved in the returned guard. Waiting does not create a new arbitrary lifetime for the protected state. Every call in this program passes a guard from this handoff's one `state` mutex; neither condition variable is shared with an unrelated mutex.

In a test build, the inner `let mut state = state` moves the existing guard into a mutable binding, increments the observation counter, and signals the test observer while still holding the mutex. The resulting guard then enters the ordinary wait. In a non-test build, that entire shadowing statement is absent. The test observer cannot acquire the state mutex between notification and the atomic release-and-wait step. It therefore witnesses entry into the wait protocol, rather than a producer's earlier intention to call the method.

`Snapshot` copies both lanes and the cursor-independent metrics in one lock acquisition. The copy is coherent at that moment, but another thread can change the handoff immediately afterward. A snapshot is for observation, not permission to enqueue later.

## Reserve capacity before handing off ownership

There are two admission interfaces. `try_reserve` reports a full lane immediately after inspecting it. `reserve` waits for capacity or closure. The word “try” refers to **not waiting for capacity**; it still acquires the mutex and is not a lock-free or strictly non-blocking operation.

```rust title="crates/language-lab/examples/thread_handoff.rs"
impl Handoff {
    fn try_reserve(self: &Arc<Self>, lane: Lane) -> Result<Reservation, ReserveError> {
        let mut state = self.lock();
        if state.closed {
            return Err(ReserveError::Closed);
        }
        let target = &mut state.lanes[lane.index()];
        if !target.has_space() {
            return Err(ReserveError::Full);
        }
        target.reserved += 1;
        Ok(Reservation {
            handoff: Arc::clone(self),
            lane,
            active: true,
        })
    }

    fn reserve(self: &Arc<Self>, lane: Lane) -> Result<Reservation, ReserveError> {
        let mut state = self.lock();
        loop {
            if state.closed {
                return Err(ReserveError::Closed);
            }
            let target = &mut state.lanes[lane.index()];
            if target.has_space() {
                target.reserved += 1;
                return Ok(Reservation {
                    handoff: Arc::clone(self),
                    lane,
                    active: true,
                });
            }
            state = self.wait(state);
        }
    }
}
```

The receiver syntax `self: &Arc<Self>` borrows an existing `Arc<Handoff>`, rather than just `&Handoff`. That lets the method clone an owner into the returned reservation. It neither moves away the caller's handle nor returns a borrow into a stack frame.

Both methods check `closed` first. Closure wins even if there is spare capacity. After a successful capacity check, they increment `reserved` under the same lock and construct exactly one active reservation. Releasing the lock does not return the reserved capacity: the reservation now owns that responsibility.

`reserve` returns `Closed` on shutdown; it never returns `Full`. Its full case goes around the loop through `wait`. The short `target` mutable borrow ends before the guard is moved into `wait`; do not retain a lane reference across that call. The returned guard replaces the old guard, and the next iteration reads closure and capacity again.

Two atomic counters would not automatically replace this transaction. Even sequentially consistent reads can let two producers both observe capacity before either increments a reservation count. A correct alternative would need an atomic state-transition design, not simply a stronger ordering on separate operations. Here one mutex keeps the relationship visible.

## A reservation returns its slot exactly once

A reservation is an ownership-bearing resource guard. This pattern is often called **RAII**: acquiring a resource creates an owning value whose destruction releases it. The resource here is one item slot, not an allocation or a lock held for the entire reservation lifetime.

Commit consumes the reservation. If admission has closed, it returns the original `String`. Otherwise it transfers that string into the queue, changes one reserved slot into one queued slot, and disables rollback:

```rust title="crates/language-lab/examples/thread_handoff.rs"
impl Reservation {
    fn commit(mut self, message: String) -> Result<(), String> {
        let mut state = self.handoff.lock();
        if state.closed {
            drop(state);
            return Err(message);
        }
        let target = &mut state.lanes[self.lane.index()];
        target.queue.push_back(message);
        target.reserved -= 1;
        self.active = false;
        drop(state);
        self.handoff.changed.notify_all();
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.handoff.lock();
            state.lanes[self.lane.index()].reserved -= 1;
            drop(state);
            self.handoff.changed.notify_all();
        }
    }
}
```

The important line is `self.active = false`. On successful return, Rust still drops the consumed `Reservation`, but its destructor must not subtract a second reservation. It then drops the reservation's `Arc` field normally. The [`Drop` documentation](https://doc.rust-lang.org/std/ops/trait.Drop.html) distinguishes the custom destructor from automatic destruction of a value's fields.

On the closed path, `active` remains true. The explicit `drop(state)` releases the mutex before scope cleanup invokes the reservation's destructor, which acquires that same mutex. Omitting that ordering in a refactored scope can create a self-deadlock. There is no cloning or implicit acceptance of the rejected message: `Err(message)` transfers its owner back to the caller.

Consider one lane with limit two:

| Point | Queued | Reserved | Free | Ownership |
| --- | ---: | ---: | ---: | --- |
| Initially | 0 | 0 | 2 | No payload. |
| Caller reserves | 0 | 1 | 1 | Reservation owns one promise. |
| Caller commits `alpha` | 1 | 0 | 1 | Queue owns `alpha`; reservation is inactive. |
| Another caller reserves | 1 | 1 | 0 | Second reservation owns the final promise. |
| Validation returns an error | 1 | 0 | 1 | Second reservation is dropped. |
| Consumer receives `alpha` | 0 | 0 | 2 | Consumer owns `alpha`. |

The queue slot is released on receive, not when application processing finishes. This is intentionally a **handoff bound**, not a bound on all outstanding work. Returning a queue slot while the receiver retains a string does not make that string's bytes disappear.

A live reservation should always have an ordinary owner. Do not use `mem::forget`, an ownership cycle, or an indefinitely retained reservation as a cancellation mechanism. Rust permits values to be leaked; [`mem::forget`](https://doc.rust-lang.org/std/mem/fn.forget.html) does not run their destructors. Our protocol relies on normal ownership discipline for capacity recovery, not on a guarantee that destructors run under every possible program behavior.

## Service both lanes, then interpret closure

The consumer prefers `state.next`. After taking a message, it prefers the other lane next time. If the preferred lane is empty, it takes from the other without waiting. Each lane remains first-in, first-out in **commit order**, not reservation-acquisition order.

```rust title="crates/language-lab/examples/thread_handoff.rs"
impl Handoff {
    fn recv(&self) -> Option<(Lane, String)> {
        let mut state = self.lock();
        loop {
            for lane in [state.next, state.next.other()] {
                if let Some(message) = state.lanes[lane.index()].queue.pop_front() {
                    state.next = lane.other();
                    state.popped = state.popped.wrapping_add(1);
                    drop(state);
                    self.changed.notify_all();
                    return Some((lane, message));
                }
            }
            if state.closed {
                return None;
            }
            state = self.wait(state);
        }
    }

    fn close(&self) {
        {
            let mut state = self.lock();
            state.closed = true;
        }
        self.changed.notify_all();
    }
}
```

`pop_front()` moves the string out of the deque. The lock protects selection, removal, the preference change, and the `popped` observation counter. The explicit unlock happens before notification and before any caller processes the message.

The `wrapping_add` is deliberate only for telemetry. It gives the counter a defined wrap behavior instead of allowing an overflow panic inside the state transition. It is not used to compute capacity, closure, message identity, or progress. Those decisions retain their own bounded state.

The placement of the closed check is part of the contract. **Inspect both queues first; only an empty closed handoff returns `None`.** Checking closure first would abandon already committed work. Conversely, do not wait for all uncommitted reservations after closure: they can no longer commit, and one producer may retain a reservation indefinitely. The consumer is allowed to finish while such a reservation exists; the remaining `Arc` keeps the state alive until its owner drops it.

`close` is idempotent and does not need a data or control slot. It marks a state transition, then wakes waiting operations. Dropping one `Arc` is not closure. The handoff does not count producer handles or guess that no future caller exists.

### The fairness guarantee has a precise boundary

When both lanes remain nonempty, service alternates. If a control message is at the front, at most one data removal precedes its selection. More generally, let $k$ be a committed control message's one-based position within its own lane. With a continuing consumer and continuously available data, its removal occurs within:

$$
N_{\text{removals}} \le 2k
$$

subsequent removals: at most one data removal before each of the $k$ required control removals. For a control message second in its lane, the worst preference starts with data and yields data, control, data, control: four removals. When the initial preference is control, that message arrives on removal three.

This is a bound on **selection steps**, not elapsed time. A blocked handler, an unscheduled consumer, or contention can delay all service. Mutex acquisition and competing producer admission are not promised to be FIFO. An uncommitted control message has not entered this scheduling guarantee. One dedicated consumer is used here; parallel receivers would also separate dequeue order from completion order.

### Why one condition variable uses `notify_all`

Our `changed` condition variable has heterogeneous waiters: a data producer, a control producer, and the consumer can wait for different predicates. A notification is not directed to the waiter whose predicate became true. Waking only one could select a producer for a still-full lane while leaving the consumer asleep with newly committed work. Waking all makes every current waiter eligible to recheck. It is simple, not necessarily optimal at high contention.

Every potentially enabling transition notifies: commit can enable receive; receive or reservation drop can enable admission; closure terminates either kind of wait. Reserving a slot does not enable anyone and needs no notification. A later design may split condition variables by predicate, but must preserve all enabling paths.

The classic lost-notification error separates checking from registering the wait:

| Step | Broken consumer | Producer |
| --- | --- | --- |
| 1 | Locks, observes no message, unlocks. | |
| 2 | Has not begun waiting. | Commits a message and notifies. |
| 3 | Acquires a lock again and waits without rechecking. | Has no further work or reason to notify. |

The notification is gone, but the queue is nonempty. Our consumer keeps its guard from the predicate check into `Condvar::wait`; a competing producer either changes the state before the check or after the atomic release-and-wait boundary. An unrelated wake merely starts another loop iteration. The mutex protects the predicate; the notification only makes re-examination possible.

## Return errors without leaking capacity or messages

The next adapter deliberately performs validation **after** reserving, so an early return exercises rollback. Real cheap validation could usually precede admission; work that needs reserved resources may still fail afterward.

```rust title="crates/language-lab/examples/thread_handoff.rs"
#[derive(Debug, PartialEq, Eq)]
enum PublishError {
    Closed(String),
    Empty(String),
}

fn publish_checked(
    handoff: &Arc<Handoff>,
    lane: Lane,
    message: String,
) -> Result<(), PublishError> {
    let reservation = match handoff.reserve(lane) {
        Ok(reservation) => reservation,
        Err(_) => return Err(PublishError::Closed(message)),
    };
    if message.is_empty() {
        return Err(PublishError::Empty(message));
    }
    reservation.commit(message).map_err(PublishError::Closed)
}

fn thread_traits() {
    fn needs_send<T: Send>() {}
    fn needs_sync<T: Sync>() {}
    needs_send::<String>();
    needs_send::<RefCell<String>>();
    needs_send::<Reservation>();
    needs_send::<Arc<Handoff>>();
    needs_sync::<Handoff>();
    needs_sync::<Arc<Handoff>>();
}
```

`PublishError` retains the rejected owner. A closed reservation request returns `Closed(message)` without ever acquiring a slot. An empty message returns `Empty(message)` while the local reservation is still active; its destructor returns that slot. A concurrent close between validation and commit is handled by the commit result, not by an earlier optimistic check.

`map_err(PublishError::Closed)` passes the enum variant constructor as a function from `String` to `PublishError`, the same callable idea used by Chapter 4's iterator adapters. `Result` does not itself require the error type to implement `std::error::Error`; this local interface compares typed variants rather than erasing them into a generic error object.

The helper `thread_traits` consists of compile-time trait-bound checks. Its empty generic functions perform no runtime synchronization. Their calls document the intended transfers: `RefCell<String>` can be moved, and a reservation may be moved because it owns thread-safe shared state. The compiler derives the required traits from the fields; it does not verify our capacity or shutdown policy.

## Keep worker completion separate from observation

The worker owns the message after `recv` returns. In this exercise, “processing” means appending that message to a finite demonstration report. The report is intentionally outside the mailbox bound: four messages in the ordinary run, 64 in one test. It is not a production strategy for retaining every processed payload.

Two observations are useful. `State::popped` counts removals while holding the state mutex. A separate [`AtomicUsize`](https://doc.rust-lang.org/std/sync/atomic/type.AtomicUsize.html) counts completed report insertions without acquiring that mutex. An atomic `fetch_add` is one read-modify-write operation; a separate atomic load followed by store would not be an equivalent multi-writer increment. Atomic integer arithmetic wraps on overflow, so the finite fixture keeps both counters far below that boundary.

We use `Ordering::Relaxed` because this number is not used to publish another object, grant capacity, stop a thread, or establish visibility of the report. The [`Ordering` contract](https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html#variant.Relaxed) does not turn a relaxed counter into synchronization for unrelated memory. The report itself comes back through `join`.

Add the worker and its closure guard:

```rust title="crates/language-lab/examples/thread_handoff.rs"
struct CloseOnExit(Arc<Handoff>);

impl Drop for CloseOnExit {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn consume(
    handoff: Arc<Handoff>,
    completed: Arc<AtomicUsize>,
) -> Vec<(Lane, String)> {
    let _close_on_exit = CloseOnExit(Arc::clone(&handoff));
    let mut report = Vec::new();
    while let Some(item) = handoff.recv() {
        report.push(item);
        completed.fetch_add(1, Ordering::Relaxed);
    }
    report
}
```

`CloseOnExit(Arc<Handoff>)` is a tuple struct: `.0` accesses its one field. The binding `_close_on_exit` is still a real owning binding despite its leading underscore. It lives until the worker exits. Writing `let _ = CloseOnExit(...)` instead would discard the value immediately and close admission too early.

The guard closes the handoff on normal exit or ordinary unwinding, waking producers rather than leaving them blocked after the only consumer disappears. It does not requeue a message already owned by a failed consumer, finish the remaining queue, or claim successful processing. This is failure notification through closure, not durable recovery or a complete supervisor.

While the worker is active, it may have popped an item but not yet incremented `completed`. Moreover, a snapshot and an atomic load taken separately are not a single coherent observation. Do not use their momentary relationship as a correctness check. After the successful join, with no other consumers or counter writers, the finite successful fixture should have equal totals. This comparison is not a benchmark proving that atomics are faster than locks; the counters record different points in the operation.

## Drive a deterministic ownership trace and join the thread

The ordinary demonstration fills both lanes **before** starting the consumer. That makes its exact sequence independent of scheduling. It also holds one uncommitted data reservation across closure, proving that reservation success is not message acceptance.

A thread closure must own suitable `Send + 'static` captures. Here `move` transfers two cloned `Arc` handles into the closure. The `'static` bound does not require the owned objects to live forever; it excludes a short-lived borrowed capture. `move` alone would not make a captured reference valid forever. See [`thread::spawn`'s bounds](https://doc.rust-lang.org/std/thread/fn.spawn.html).

`Builder::spawn` reports thread-creation failure as an I/O result. After successful spawning, this main performs no fallible setup or assertions before joining. A dropped join handle would detach the worker, not join it or stop it. These distinctions are specified by [`Builder::spawn`](https://doc.rust-lang.org/std/thread/struct.Builder.html#method.spawn) and [`JoinHandle`](https://doc.rust-lang.org/std/thread/struct.JoinHandle.html).

```rust title="crates/language-lab/examples/thread_handoff.rs"
fn main() -> std::io::Result<()> {
    local_aliases();
    thread_traits();
    let handoff = Handoff::new(3, 2).expect("positive lane limits");
    assert_eq!(
        publish_checked(&handoff, Lane::Data, String::new()),
        Err(PublishError::Empty(String::new()))
    );
    assert_eq!(handoff.snapshot().reserved, [0, 0]);
    publish_checked(&handoff, Lane::Data, String::from("data-0")).unwrap();
    publish_checked(&handoff, Lane::Data, String::from("data-1")).unwrap();
    let pending = handoff.try_reserve(Lane::Data).unwrap();
    assert!(matches!(
        handoff.try_reserve(Lane::Data),
        Err(ReserveError::Full)
    ));
    publish_checked(&handoff, Lane::Control, String::from("control-0")).unwrap();
    publish_checked(&handoff, Lane::Control, String::from("control-1")).unwrap();
    let before = handoff.snapshot();
    assert_eq!(before.queued, [2, 2]);
    assert_eq!(before.reserved, [1, 0]);
    handoff.close();
    assert_eq!(pending.commit(String::from("late")), Err(String::from("late")));

    let completed = Arc::new(AtomicUsize::new(0));
    let worker_handoff = Arc::clone(&handoff);
    let worker_completed = Arc::clone(&completed);
    let worker = thread::Builder::new()
        .name(String::from("handoff-owner"))
        .spawn(move || consume(worker_handoff, worker_completed))?;
    let joined = worker.join();
    let report = match joined {
        Ok(report) => report,
        Err(_) => return Err(std::io::Error::other("handoff worker panicked")),
    };
    let expected = vec![
        (Lane::Control, String::from("control-0")),
        (Lane::Data, String::from("data-0")),
        (Lane::Control, String::from("control-1")),
        (Lane::Data, String::from("data-1")),
    ];
    assert_eq!(report, expected);
    let after = handoff.snapshot();
    assert_eq!(after.queued, [0, 0]);
    assert_eq!(after.reserved, [0, 0]);
    assert!(after.closed);
    assert_eq!(after.popped, 4);
    assert_eq!(completed.load(Ordering::Relaxed), 4);
    println!("full data lane; control admission remained available");
    println!("validation and closed-commit reservations returned");
    for (lane, message) in report {
        println!("{lane:?}: {message}");
    }
    println!("joined: popped=4 completed=4 queued=[0, 0] reserved=[0, 0]");
    Ok(())
}
```

The handoff is already closed when spawning begins. A spawn error therefore leaves no running worker and no waiter that needs to be woken. A worker panic is observed by `join` before being translated into a simple I/O error for this executable; this wrapper does not preserve an arbitrary panic payload as an error cause. All normal output and postconditions happen after the worker is joined.

A successful [`join`](https://doc.rust-lang.org/std/thread/struct.JoinHandle.html#method.join) also establishes the completion relationship needed before reading the final report and metric. No mutex guard is held across the call. Holding one could deadlock if the worker needed that mutex in order to finish.

Expected standard output, excluding Cargo's build messages:

```text title="Expected output — thread_handoff"
full data lane; control admission remained available
validation and closed-commit reservations returned
Control: control-0
Data: data-0
Control: control-1
Data: data-1
joined: popped=4 completed=4 queued=[0, 0] reserved=[0, 0]
```

Before closure the data lane has two queued messages and one reservation; the control lane has two queued messages and no reservations. Closure does not erase any of them. Rejecting `late` drops the pending reservation, changing data accounting from queued two/reserved one to queued two/reserved zero. The worker then alternates control and data, transfers four owners into its report, and returns `None` only after both queues are empty.

This driver deliberately starts no blocked producer. The following tests exercise real waiting separately, without weakening the deterministic demonstration into scheduler-dependent output.

## Observe waiting without using sleep as a proof

Start the test module below. It remains open through the next four panels.

`put` is a fixture helper that uses immediate admission: small tests must not silently block while setting up a queue. `saw_wait` uses the separate test observation condition variable. Its five-second timeout is only a diagnostic allowance for scheduling; it is not a performance requirement or a cancellation mechanism. The predicate is the cumulative number of entered waits, protected by the same state mutex. It also handles the case where the observed event occurred before the test began waiting.

```rust title="crates/language-lab/examples/thread_handoff.rs"
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn saw_wait(handoff: &Handoff, target: usize) -> bool {
        let (state, _) = handoff
            .wait_observed
            .wait_timeout_while(handoff.lock(), Duration::from_secs(5), |state| {
                state.waits_started < target
            })
            .unwrap();
        state.waits_started >= target
    }

    fn put(handoff: &Arc<Handoff>, lane: Lane, message: &str) {
        handoff
            .try_reserve(lane)
            .unwrap()
            .commit(message.to_owned())
            .unwrap();
    }

    #[test]
    fn local_borrows_and_thread_bounds_are_checked() {
        local_aliases();
        thread_traits();
    }

    #[test]
    fn both_lane_limits_must_be_positive() {
        assert!(Handoff::new(0, 1).is_none());
        assert!(Handoff::new(1, 0).is_none());
        assert!(Handoff::new(1, 1).is_some());
    }

    #[test]
    fn reservation_consumes_capacity_but_not_the_other_lane() {
        let handoff = Handoff::new(1, 1).unwrap();
        let data = handoff.try_reserve(Lane::Data).unwrap();
        assert!(matches!(
            handoff.try_reserve(Lane::Data),
            Err(ReserveError::Full)
        ));
        let control = handoff.try_reserve(Lane::Control).unwrap();
        assert_eq!(handoff.snapshot().reserved, [1, 1]);
        drop(data);
        drop(control);
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }

    #[test]
    fn early_validation_error_returns_capacity() {
        let handoff = Handoff::new(1, 1).unwrap();
        let result = publish_checked(&handoff, Lane::Data, String::new());
        assert_eq!(result, Err(PublishError::Empty(String::new())));
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
        put(&handoff, Lane::Data, "valid");
        assert_eq!(handoff.snapshot().queued, [1, 0]);
    }
```

These first four tests cover type constraints, invalid configuration, independent lane capacity, and early-error cleanup. A successful reservation need not contain a payload; that is why the full-capacity test counts reservations even with both queues empty.

### Check ordering and the closure boundary first

Append the next tests inside the same module:

```rust title="crates/language-lab/examples/thread_handoff.rs"
    #[test]
    fn ready_lanes_alternate_and_each_lane_is_fifo() {
        let handoff = Handoff::new(3, 2).unwrap();
        for text in ["d0", "d1", "d2"] {
            put(&handoff, Lane::Data, text);
        }
        for text in ["c0", "c1"] {
            put(&handoff, Lane::Control, text);
        }
        handoff.close();
        let mut actual = Vec::new();
        while let Some(item) = handoff.recv() {
            actual.push(item);
        }
        assert_eq!(
            actual,
            vec![
                (Lane::Control, "c0".to_owned()),
                (Lane::Data, "d0".to_owned()),
                (Lane::Control, "c1".to_owned()),
                (Lane::Data, "d1".to_owned()),
                (Lane::Data, "d2".to_owned()),
            ]
        );
        assert_eq!(handoff.snapshot().popped, 5);
    }

    #[test]
    fn commit_order_is_not_reservation_order() {
        let handoff = Handoff::new(2, 1).unwrap();
        let first = handoff.try_reserve(Lane::Data).unwrap();
        let second = handoff.try_reserve(Lane::Data).unwrap();
        second.commit("second".to_owned()).unwrap();
        first.commit("first".to_owned()).unwrap();
        handoff.close();
        assert_eq!(handoff.recv(), Some((Lane::Data, "second".to_owned())));
        assert_eq!(handoff.recv(), Some((Lane::Data, "first".to_owned())));
        assert_eq!(handoff.recv(), None);
    }

    #[test]
    fn close_drains_commits_and_rejects_pending_ownership() {
        let handoff = Handoff::new(2, 1).unwrap();
        put(&handoff, Lane::Data, "accepted");
        let pending = handoff.try_reserve(Lane::Data).unwrap();
        handoff.close();
        handoff.close();
        assert!(matches!(
            handoff.try_reserve(Lane::Control),
            Err(ReserveError::Closed)
        ));
        assert_eq!(pending.commit("late".to_owned()), Err("late".to_owned()));
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
        assert_eq!(handoff.recv(), Some((Lane::Data, "accepted".to_owned())));
        assert_eq!(handoff.recv(), None);
        assert_eq!(
            publish_checked(&handoff, Lane::Data, "owned".to_owned()),
            Err(PublishError::Closed("owned".to_owned()))
        );
    }

    #[test]
    fn closed_empty_receiver_does_not_wait_for_unused_reservations() {
        let handoff = Handoff::new(1, 1).unwrap();
        let pending = handoff.try_reserve(Lane::Data).unwrap();
        handoff.close();
        assert_eq!(handoff.recv(), None);
        assert_eq!(handoff.snapshot().reserved, [1, 0]);
        drop(pending);
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }
```

The alternation fixture expects control, data, control, data, data: once control is empty, data must continue. The next test acquires two reservations and commits the second one first. Receiving `second` before `first` is correct: no position was assigned at reservation time.

The closure tests distinguish accepted messages from unused promises. Calling `close` twice remains harmless. `late` and `owned` are returned to their callers, while `accepted` is still drained. An empty closed receiver returns immediately even while a pending reservation keeps the shared allocation alive.

### Exercise each enabling transition

Append these tests:

```rust title="crates/language-lab/examples/thread_handoff.rs"
    #[test]
    fn dropping_a_reservation_wakes_a_capacity_waiter() {
        let handoff = Handoff::new(1, 1).unwrap();
        let occupied = handoff.try_reserve(Lane::Data).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.reserve(Lane::Data));
        let observed = saw_wait(&handoff, 1);
        drop(occupied);
        let acquired = worker.join().unwrap();
        handoff.close();
        assert!(observed, "producer never reached its wait path");
        drop(acquired.unwrap());
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }

    #[test]
    fn removing_a_message_wakes_a_capacity_waiter() {
        let handoff = Handoff::new(1, 1).unwrap();
        put(&handoff, Lane::Data, "first");
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.reserve(Lane::Data));
        let observed = saw_wait(&handoff, 1);
        let first = handoff.recv();
        let acquired = worker.join().unwrap();
        handoff.close();
        assert!(observed, "producer never reached its wait path");
        assert_eq!(first, Some((Lane::Data, "first".to_owned())));
        drop(acquired.unwrap());
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }

    #[test]
    fn commit_wakes_an_empty_receiver() {
        let handoff = Handoff::new(1, 1).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.recv());
        let observed = saw_wait(&handoff, 1);
        let sent = publish_checked(&handoff, Lane::Control, "wake".to_owned());
        let received = worker.join().unwrap();
        handoff.close();
        assert!(observed, "receiver never reached its wait path");
        assert_eq!(sent, Ok(()));
        assert_eq!(received, Some((Lane::Control, "wake".to_owned())));
    }

    #[test]
    fn close_wakes_a_capacity_waiter() {
        let handoff = Handoff::new(1, 1).unwrap();
        let occupied = handoff.try_reserve(Lane::Data).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.reserve(Lane::Data));
        let observed = saw_wait(&handoff, 1);
        handoff.close();
        let result = worker.join().unwrap();
        drop(occupied);
        assert!(observed, "producer never reached its wait path");
        assert!(matches!(result, Err(ReserveError::Closed)));
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }

    #[test]
    fn unrelated_notification_rechecks_and_close_wakes_receiver() {
        let handoff = Handoff::new(1, 1).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.recv());
        let first_wait = saw_wait(&handoff, 1);
        handoff.changed.notify_all();
        let second_wait = saw_wait(&handoff, 2);
        handoff.close();
        let result = worker.join().unwrap();
        assert!(first_wait && second_wait, "receiver did not recheck and wait");
        assert_eq!(result, None);
    }
```

A capacity waiter is enabled by either a reservation drop or a queue removal; both are tested separately. The commit test joins its receiver **before** closing the handoff, so a shutdown notification cannot hide a missing commit notification. Another test closes while a producer is waiting, without first freeing capacity: closure must be sufficient to make it return `Closed`.

The last test deliberately notifies an empty, still-open receiver. It should enter its wait path again rather than return `None` or invent an item. This simulates an irrelevant notification, not a promise that the operating system generated a spurious wakeup. All worker handles are joined before interpreting the successful test outcomes.

The observation timeout cannot rescue a broken handoff whose worker is already stuck in `join`. A missing enabling notification can still hang a focused test; interrupt it and inspect the named transition rather than increasing arbitrary sleeps. Thread termination requires changing the protocol state, not timing out an unrelated observer.

### Distinguish unwinding from poisoned state

Append the failure tests:

```rust title="crates/language-lab/examples/thread_handoff.rs"
    #[test]
    fn unwinding_outside_the_lock_returns_a_reservation() {
        let handoff = Handoff::new(1, 1).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || {
            let _reservation = peer.try_reserve(Lane::Data).unwrap();
            panic!("injected after reserve released the mutex");
        });
        let result = worker.join();
        assert!(result.is_err());
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
        assert!(!handoff.state.is_poisoned());
        put(&handoff, Lane::Data, "still usable");
    }

    #[test]
    fn consumer_exit_guard_closes_and_releases_blocked_producer() {
        let handoff = Handoff::new(1, 1).unwrap();
        put(&handoff, Lane::Data, "accepted");
        let peer = Arc::clone(&handoff);
        let producer = thread::spawn(move || peer.reserve(Lane::Data));
        let observed = saw_wait(&handoff, 1);
        let owner = Arc::clone(&handoff);
        let consumer = thread::spawn(move || {
            let _close_on_exit = CloseOnExit(owner);
            panic!("injected consumer failure outside the mutex");
        });
        let consumer_result = consumer.join();
        let producer_result = producer.join().unwrap();
        assert!(observed, "producer never reached its wait path");
        assert!(consumer_result.is_err());
        assert!(matches!(producer_result, Err(ReserveError::Closed)));
        assert!(handoff.snapshot().closed);
        assert_eq!(handoff.snapshot().queued, [1, 0]);
    }
```

Both injected panics occur **outside** the handoff mutex. The first thread owns a reservation but not a lock when it panics; ordinary stack unwinding drops the reservation and returns capacity. `join` reports the panic. The state mutex remains unpoisoned.

The second failure closes admission through the consumer's exit guard and wakes the waiting producer. It intentionally leaves the committed message queued. A failed consumer did not successfully drain accepted work, and the test does not pretend otherwise. Panic-hook text may appear when running with `--nocapture`; these tests expect the joined panic result rather than marking the whole test `should_panic`.

These checks assume ordinary unwinding, as in the supplied normal Cargo test setup. A process configured to abort on panic cannot demonstrate destructor cleanup by unwinding. Do not put an intentional panic under the state lock in this test process: our selected poison policy is process termination, not in-process recovery.

### Close only after producers are finished in the normal path

Append the last test and close the module:

```rust title="crates/language-lab/examples/thread_handoff.rs"
    #[test]
    fn concurrent_producers_finish_before_normal_closure() {
        fn producer(
            handoff: Arc<Handoff>,
            lane: Lane,
        ) -> thread::JoinHandle<Result<(), PublishError>> {
            thread::spawn(move || {
                for index in 0..32 {
                    publish_checked(&handoff, lane, format!("{lane:?}-{index}"))?;
                }
                Ok(())
            })
        }

        let handoff = Handoff::new(2, 1).unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let owner = Arc::clone(&handoff);
        let metric = Arc::clone(&completed);
        let consumer = thread::spawn(move || consume(owner, metric));
        let data = producer(Arc::clone(&handoff), Lane::Data);
        let control = producer(Arc::clone(&handoff), Lane::Control);
        let data_result = data.join();
        let control_result = control.join();
        handoff.close();
        let consumer_result = consumer.join();

        assert_eq!(data_result.unwrap(), Ok(()));
        assert_eq!(control_result.unwrap(), Ok(()));
        let report = consumer_result.unwrap();
        assert_eq!(report.len(), 64);
        for lane in [Lane::Data, Lane::Control] {
            let actual: Vec<String> = report
                .iter()
                .filter(|(source, _)| *source == lane)
                .map(|(_, message)| message.clone())
                .collect();
            let expected: Vec<String> = (0..32)
                .map(|index| format!("{lane:?}-{index}"))
                .collect();
            assert_eq!(actual, expected);
        }
        let snapshot = handoff.snapshot();
        assert_eq!(snapshot.queued, [0, 0]);
        assert_eq!(snapshot.reserved, [0, 0]);
        assert_eq!(snapshot.popped, 64);
        assert_eq!(completed.load(Ordering::Relaxed), 64);
        assert!(snapshot.closed);
    }
}
```

This test starts one consumer and two producers. Each producer sends 32 numbered strings through its own lane, while the data lane holds at most two queued-or-reserved items and control holds at most one. Producer results are collected, admission is closed, and the consumer is joined **before** assertions unwrap those results. A consumer panic closes the queue through its guard, so producers can report closure instead of waiting forever on a nonexistent owner.

The test checks each lane's sequence independently. Exact interleaving across lanes depends on arrival times and is not promised. The extra string clones are confined to this 64-item comparison fixture; the handoff implementation itself transfers each payload without cloning it.

Closing before joining normally functioning producers would revoke outstanding reservations and reject remaining sends. Waiting for the consumer to finish before closing would instead leave it waiting for more input. The intended order is producer completion, admission closure, drain, consumer join. The ordinary demonstration closes earlier only because all work it intends to accept has already been committed.

There are **16 required tests** in this example. Test infrastructure uses `thread::spawn` for small controlled workers; the ordinary executable uses the fallible builder. As with ordinary assertion-based tests, arbitrary test-infrastructure panics or OS thread-creation exhaustion are not a general-purpose supervision strategy.

## Compile the sharing mistake, then repair the actual cause

Keep intentional failures outside `examples/` so the normal workspace remains buildable:

```sh title="Terminal — isolated compiler experiment"
mkdir -p scratch/chapter-05
```

Create this complete scratch file:

```rust title="scratch/chapter-05/shared_counter.rs — intentionally rejected Rc version"
use std::cell::RefCell;
use std::rc::Rc;
use std::thread;

fn main() {
    let count = Rc::new(RefCell::new(0_u32));
    let worker_count = Rc::clone(&count);
    let worker = thread::spawn(move || {
        *worker_count.borrow_mut() += 1;
    });
    worker.join().unwrap();
    assert_eq!(*count.borrow(), 1);
}
```

Compile it separately:

```sh title="Terminal — expect a Send-bound diagnostic"
rustc --edition=2024 scratch/chapter-05/shared_counter.rs -o scratch/chapter-05/shared_counter
```

The expected diagnostic says that the captured `Rc<RefCell<u32>>` cannot be sent between threads safely. The closure owns its `Rc`, but the other `Rc` still aliases the same non-atomic ownership counter. Exact compiler wording varies; the violated transfer bound is the useful information.

Now replace the entire scratch file with the apparent fix:

```rust title="scratch/chapter-05/shared_counter.rs — intentionally rejected Arc and RefCell version"
use std::cell::RefCell;
use std::sync::Arc;
use std::thread;

fn main() {
    let count = Arc::new(RefCell::new(0_u32));
    let worker_count = Arc::clone(&count);
    let worker = thread::spawn(move || {
        *worker_count.borrow_mut() += 1;
    });
    worker.join().unwrap();
    assert_eq!(*count.borrow(), 1);
}
```

Repeat the compiler command. This time the owner count is suitable, but `RefCell<u32>` is not `Sync`, so this `Arc` still cannot be transferred as required. Do not replace the diagnostic with an unsafe trait implementation. Fix the shared mutation policy instead:

```rust title="scratch/chapter-05/shared_counter.rs — complete repaired version"
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    let count = Arc::new(Mutex::new(0_u32));
    let worker_count = Arc::clone(&count);
    let worker = thread::spawn(move || {
        let mut guard = worker_count.lock().unwrap();
        *guard += 1;
    });
    worker.join().unwrap();
    let guard = count.lock().unwrap();
    assert_eq!(*guard, 1);
}
```

Compile with the same command, then run `./scratch/chapter-05/shared_counter`. Expected behavior is successful, silent completion. The worker drops its lock guard before it returns, and main acquires its own guard only after joining. The Rust Book's [shared-state discussion](https://doc.rust-lang.org/book/ch16-03-shared-state.html) explains the same separation between ownership and locking; our handoff adds compound invariants and explicit lifecycle policy to that foundation.

## Run the complete example and keep the earlier suite

From the learner workspace root:

```sh title="Terminal — build and run Chapter 5"
cargo check -p language-lab --all-targets
cargo run -p language-lab --example thread_handoff
cargo test -p language-lab --example thread_handoff
cargo test -p language-lab --all-targets
cargo fmt --all
cargo fmt --all -- --check
```

The targeted run should report **16 passed**. The required cumulative suite is now **59 tests**: six library tests, eight ownership tests, thirteen parser tests, sixteen generic-collection tests, and these sixteen synchronization tests. Cargo may print separate target summaries. Optional tests can increase the total; `cargo test -p language-lab` alone does not replace explicitly selecting example tests or `--all-targets`.

Run the earlier ordinary example entry points as well:

```sh title="Terminal — retain earlier executable assertions"
cargo run -p language-lab --example workspace_probe
cargo run -p language-lab --example borrowed_record
cargo run -p language-lab --example fallible_frame
cargo run -p language-lab --example generic_scan
```

The outputs and counts above are **expected local checks**, not claims that compilation, test execution, formatter checks, the intentional compiler rejections, or native macOS execution have already succeeded. A repeated test run samples more schedules but is not a proof of all possible interleavings.

For a focused wakeup diagnostic, use a test name rather than weakening the whole suite:

```sh title="Terminal — inspect one synchronization path"
cargo test -p language-lab --example thread_handoff dropping_a_reservation_wakes_a_capacity_waiter -- --nocapture
```

## Diagnose the invariant, not the symptom's timing

| Symptom | Likely error | Concrete correction |
| --- | --- | --- |
| Data pressure prevents control admission | Both lanes share one admission count or control acquires data capacity first. | Use each lane's own queued-plus-reserved predicate. |
| A failed validation gradually fills the handoff | Reserved slots have no live rollback owner. | Keep the reservation binding through validation; let its destructor return the slot. |
| A successful commit makes capacity grow unexpectedly | Both commit and the destructor decremented `reserved`. | Mark the consumed reservation inactive after committing under the lock. |
| A rejected commit hangs in cleanup | Reservation destruction reacquires a mutex that the same thread still holds. | Drop the state guard before returning the active reservation through cleanup. |
| A receiver hangs although the queue is nonempty | Check and wait were separated, or commit failed to notify. | Carry the checked guard into `wait`; notify after accepted insertion. |
| A wake lets an operation proceed with no room or no item | A notification was treated as permission. | Loop and recheck the predicate under the reacquired mutex. |
| Shutdown loses accepted items | `recv` checks closure before inspecting both queues. | Drain committed messages before returning `None`. |
| Normal producers unexpectedly receive `Closed` | Closure occurred before their final commits. | Join producers, close admission, then join the draining consumer. |
| Program exit is mistaken for completed work | A join handle was discarded or an atomic counter was treated as a completion barrier. | Keep the handle and join without holding the state mutex. |

## Know which resources this example bounds

Let $C=c_0+c_1$ be the total configured item budget. Then at any protected transition:

$$
q_0+r_0+q_1+r_1 \le C
$$

For the main program, $C=3+2=5$. Reservations consume slots without storing payloads. The deques have room for at least those five string handles, possibly more because allocation capacity may exceed the request. The actual policy still admits at most five queued-or-reserved items.

Let $A_i$ be lane $i$'s actual deque allocation capacity and $S$ be `size_of::<String>()` in the current build. Its element-storage contribution is approximately $(A_0+A_1)S$, excluding allocator overhead and deque metadata. It does **not** include the heap buffers owned by the strings. [`size_of`](https://doc.rust-lang.org/std/mem/fn.size_of.html) measures an in-memory value, not allocations that value owns, as discussed in Chapter 4. A queue of two strings can still own very large payload buffers.

Nor does this limit count the consumer's current message, its report, or strings held by blocked producers. `publish_checked` receives an already allocated string before it waits. The low-level reservation interface permits reserving before constructing a payload, but does not itself cap its byte length. There is no whole-process memory or throughput claim here. Later byte-budget work must track buffer ownership across the queue boundary, and a later service must limit outstanding producer work as well.

A focused variation checks the scheduler independently of timing. In `ready_lanes_alternate_and_each_lane_is_fifo`, insert this block immediately after constructing the handoff:

```rust title="Temporary statements inside the ready-lanes test"
{
    let mut state = handoff.lock();
    state.next = Lane::Data;
}
```

For that test only, change the expected sequence to `d0, c0, d1, c1, d2`, retaining each message's lane tag. Run the named test and restore both edits afterward. The normal main's expectation remains unchanged. The variation changes initial preference, not FIFO order or the at-most-one competing-lane service rule.

The next foundation chapter, [Futures, async ownership, and cancellation](https://25d-228.github.io/rust-learning/index.html#chapter-6-futures-async-ownership-and-cancellation), carries an owned permit through suspension and cancellation. Keep the present distinction intact: dropping an unused reservation returns capacity; dropping interest in a message after ownership has transferred does not retroactively cancel accepted work.
