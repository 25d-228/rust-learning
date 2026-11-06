# Chapter 17 — Cancellation boundaries and supervised shutdown

## Part 2 — A supervisor that retains its children

**Prerequisite:** [Part 1](https://25d-228.github.io/rust-learning/chapter-17.html). This part is included in the chapter's single **4.5-hour** estimate. It adds the complete `crates/broker-app/src/supervisor.rs`. Part 3 supplies the small `LocalBroker` methods and session entry point that it calls. Do not add a second runtime or a background thread merely to poll owner status.

## Keep handles in an owner, not inside a cancellable drive future

The supervisor owns one `JoinSet` for all accepted sessions and one `LocalBroker` containing the four thread handles. Its asynchronous methods borrow `&mut self`; their futures do not own those handles. Cancelling a borrowed `serve_until` or `drain_until` call therefore leaves the supervisor available for another call.

This is a useful distinction from spawning a supervisor task and dropping its only handle. It is also not automatic cleanup: a cancelled serving call leaves the service phase unchanged, with its listeners and sessions still owned. The caller must resume serving or request shutdown and drain. Destroying the **whole supervisor** is a different operation with a more limited fallback, discussed below.

Create the file in the following order. Its initial definitions use arrays because the topology is fixed: two listener roles and four partitions. No allocation proportional to connection lifetime is added.

```rust title="crates/broker-app/src/supervisor.rs"
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{Id, JoinError, JoinSet};
use tokio::time::{Instant, sleep_until};

use broker_core::types::PARTITION_COUNT;
use broker_core::worker::{ExitWatch, OwnerExit};
use crate::local::LocalBroker;
use crate::protocol::Endpoint;
use crate::server::{Clients, ListenerStats, SessionLimits, session_until};

const OWNERS: usize = PARTITION_COUNT as usize;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase { Starting, Serving, Quiescing, Draining, Stopped, Failed }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopCause { Requested, SignalFailed, StartupFailed, OwnerExited, TransportFailed }

#[derive(Debug, Clone, Copy)]
pub struct SupervisorConfig {
    pub sessions: SessionLimits,
    pub capacity: [usize; 2],
    pub network_grace: Duration,
}
impl Default for SupervisorConfig {
    fn default() -> Self {
        Self { sessions: SessionLimits::default(), capacity: [16, 16],
            network_grace: Duration::from_secs(2) }
    }
}
impl SupervisorConfig {
    pub fn validate(self) -> io::Result<()> {
        self.sessions.validate()?;
        if self.capacity.iter().any(|n| !(1..=64).contains(n))
            || self.network_grace > Duration::from_secs(60)
        { return Err(io::Error::new(io::ErrorKind::InvalidInput, "capacity 1..=64; network grace 0..=60s")); }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ShutdownReport {
    pub phase: Phase,
    pub cause: Option<StopCause>,
    pub first_failure: Option<String>,
    pub sessions: [ListenerStats; 2],
    pub active_sessions: usize,
    pub pending_owners: usize,
    pub network_forced: bool,
    pub owners: [Option<OwnerExit>; OWNERS],
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStatus { Pending, ReadyToJoin }

```

`ShutdownReport` separates the trigger from the outcome. A user interrupt can be the `cause`, yet a subsequent checkpoint failure makes the final `phase` failed. `first_failure` preserves the first explanation; per-owner notices and session counters retain additional structured outcomes. `pending_owners` counts missing exit observations, **not** threads proven joined. `ReadyToJoin` deliberately stops short of declaring termination.

The `watch::Receiver` returned by `observe` is a current-state view. A receiver may see `Serving` and then `Draining` without separately observing the brief `Quiescing` state. Do not build correctness around reading every phase transition from a latest-value channel.

## Observe four exits without four monitor tasks

`SessionJoin` names the result shape from `JoinSet::join_next_with_id`. A task can return its I/O result or terminate with a `JoinError`; both cases supply a task identity. That identity is needed to return capacity to the correct listener role.

```rust title="crates/broker-app/src/supervisor.rs"
type SessionJoin = Result<(Id, io::Result<()>), JoinError>;
enum Event {
    Stop(io::Result<()>),
    Owner(usize, Option<OwnerExit>),
    Session(SessionJoin),
    Accept(usize, io::Result<(TcpStream, SocketAddr)>),
}

async fn read_exit(receiver: &mut ExitWatch) -> Option<OwnerExit> {
    loop {
        let value = receiver.borrow_and_update().clone();
        if value.is_some() { return value; }
        if receiver.changed().await.is_err() { return None; }
    }
}
async fn next_exit(receivers: &mut [ExitWatch; OWNERS], seen: [bool; OWNERS])
    -> (usize, Option<OwnerExit>)
{
    // The topology is deliberately fixed at four. No monitor tasks or polling timer are needed.
    let [a, b, c, d] = receivers;
    tokio::select! {
        result = read_exit(a), if !seen[0] => (0, result),
        result = read_exit(b), if !seen[1] => (1, result),
        result = read_exit(c), if !seen[2] => (2, result),
        result = read_exit(d), if !seen[3] => (3, result),
        else => std::future::pending().await,
    }
}

```

`let [a, b, c, d] = receivers` destructures a mutable reference to a four-element array into four disjoint mutable element references. It avoids repeatedly indexing the same array in concurrent select branches. The code intentionally depends on the fixed four-partition topology; making the constant dynamic would require redesigning this observer.

`borrow_and_update()` returns a watch read guard. Cloning its optional notice into `value` ends the guard at the semicolon, before any `.await`. Holding that guard over an await could block the sender; the [receiver documentation](https://docs.rs/tokio/latest/tokio/sync/watch/struct.Receiver.html) explains this lock lifetime. A value already published is returned immediately, including when the observer was cloned after the exit. If the sender disappears without a notice, `None` becomes a failure report rather than an infinite wait.

`next_exit` disables branches already recorded. Its `else` branch remains pending when every notice has been seen; the drain driver also disables the whole call in that case. Tokio's [select contract](https://docs.rs/tokio/latest/tokio/macro.select.html) matters here: losing branch futures are dropped, so the operations selected must remain safe to reissue. A cancelled watch wait or `JoinSet` join does not consume a result that the supervisor then forgets.

## Store every durable piece of lifecycle state

```rust title="crates/broker-app/src/supervisor.rs"
pub struct Supervisor {
    // Task ownership is in the object, not in a cancellable drive future.
    tasks: JoinSet<io::Result<()>>,
    scopes: HashMap<Id, usize>,
    broker: LocalBroker,
    clients: Option<Clients>,
    listeners: [Option<TcpListener>; 2],
    stop: watch::Sender<bool>,
    exits: [ExitWatch; OWNERS],
    seen: [bool; OWNERS],
    notices: [Option<OwnerExit>; OWNERS],
    config: SupervisorConfig,
    phase: Phase,
    phase_changed: watch::Sender<Phase>,
    stats: [ListenerStats; 2],
    active: [usize; 2],
    cause: Option<StopCause>,
    first_failure: Option<String>,
    network_deadline: Option<Instant>,
    network_forced: bool,
}

impl Supervisor {
    pub fn new(broker: LocalBroker, config: SupervisorConfig) -> io::Result<Self> {
        config.validate()?;
        let clients = Some(broker.clients());
        let exits = broker.exits();
        let (stop, _) = watch::channel(false);
        let (phase_changed, _) = watch::channel(Phase::Starting);
        Ok(Self { tasks: JoinSet::new(), scopes: HashMap::with_capacity(config.capacity.iter().sum()),
            broker, clients, listeners: [None, None], stop, exits, seen: [false; OWNERS],
            notices: std::array::from_fn(|_| None), config, phase: Phase::Starting, phase_changed,
            stats: [ListenerStats::default(); 2], active: [0, 0], cause: None, first_failure: None,
            network_deadline: None, network_forced: false })
    }
    pub fn phase(&self) -> Phase { self.phase }
    pub fn observe(&self) -> watch::Receiver<Phase> { self.phase_changed.subscribe() }
    pub fn report(&self) -> ShutdownReport {
        ShutdownReport { phase: self.phase, cause: self.cause, first_failure: self.first_failure.clone(),
            sessions: self.stats, active_sessions: self.tasks.len(),
            pending_owners: self.seen.iter().filter(|seen| !**seen).count(),
            network_forced: self.network_forced, owners: self.notices.clone() }
    }
    fn phase_to(&mut self, phase: Phase) {
        self.phase = phase;
        self.phase_changed.send_replace(phase);
    }
    fn fail(&mut self, message: String) {
        if self.first_failure.is_none() { self.first_failure = Some(message); }
    }

```

`scopes` maps each currently tracked task ID to producer role 0 or consumer/control role 1. `active` counts accepted tasks whose join results have not yet been removed, including tasks that already completed. This prevents a backlog of finished-but-unreaped tasks from escaping the configured bound. The table's maximum occupancy is the sum of the two capacities, at most 128 here; the default is 32. Kernel connection queues and socket buffers remain outside this application-task bound.

`network_deadline` belongs to the supervisor, not to a temporary drain call. Resetting it on every retry would let a caller accidentally postpone forced network cancellation forever. Conversely, `observation_deadline` is a per-call limit for returning an honest unfinished report. The two deadlines serve different purposes.

`clients: Option<Clients>` lets the supervisor deliberately release its own pool-owning client generation after all sessions and owners have reported completion. It cannot release clones or response batches owned by its caller. A finished owner is not proof that every allocation from the generation has been freed.

## Bind both listeners before entering Serving

```rust title="crates/broker-app/src/supervisor.rs"
    pub async fn bind(&mut self, producer: SocketAddr, consumer: SocketAddr)
        -> io::Result<[SocketAddr; 2]>
    {
        if self.phase != Phase::Starting {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bind requires Starting"));
        }
        let result: io::Result<_> = async {
            let p = TcpListener::bind(producer).await?;
            let c = TcpListener::bind(consumer).await?;
            let addresses = [p.local_addr()?, c.local_addr()?];
            Ok(([Some(p), Some(c)], addresses))
        }.await;
        match result {
            Ok((listeners, addresses)) => {
                self.listeners = listeners;
                self.phase_to(Phase::Serving);
                Ok(addresses)
            }
            Err(error) => {
                self.fail(format!("listener startup: {error}"));
                self.quiesce(StopCause::StartupFailed);
                Err(error)
            }
        }
    }

```

The temporary producer listener is owned inside the `async` block while the consumer listener is being bound. Failure or cancellation before the pair is installed drops temporary sockets. A bind failure records a fatal reason and initiates cleanup of already started owners; the caller still drives the drain and joins. Cancelling the borrowed bind future before installation leaves the supervisor in `Starting`, from which a retry or explicit shutdown is possible.

This refactors Chapter 16's two listener tasks into one owning accept loop. Sessions are still individual tasks; listener acceptance is not. There is no untracked nested task set whose lifetime must be inferred from a listener's return value.

## Account for every session result, including panic

```rust title="crates/broker-app/src/supervisor.rs"
    fn spawn_session(&mut self, scope: usize, stream: TcpStream) {
        let endpoint = if scope == 0 { Endpoint::Producer } else { Endpoint::Consumer };
        let clients = Arc::clone(self.clients.as_ref().expect("serving clients"));
        let handle = self.tasks.spawn(session_until(stream, endpoint, clients,
            self.config.sessions, self.stop.subscribe()));
        self.scopes.insert(handle.id(), scope);
        self.active[scope] += 1;
        self.stats[scope].accepted = self.stats[scope].accepted.saturating_add(1);
    }
    fn joined_session(&mut self, joined: SessionJoin) {
        let id = match &joined { Ok((id, _)) => *id, Err(error) => error.id() };
        let Some(scope) = self.scopes.remove(&id) else {
            self.fail("joined an untracked session".into());
            return;
        };
        self.active[scope] -= 1;
        let stats = &mut self.stats[scope];
        match joined {
            Ok((_, Ok(()))) => stats.completed = stats.completed.saturating_add(1),
            Ok((_, Err(_))) => stats.session_errors = stats.session_errors.saturating_add(1),
            Err(error) if error.is_cancelled() => {
                stats.cancelled = stats.cancelled.saturating_add(1);
                if !self.network_forced { self.fail("session cancelled outside forced network drain".into()); }
            }
            Err(error) => {
                stats.panicked = stats.panicked.saturating_add(1);
                self.fail(format!("session task panicked: {error}"));
            }
        }
    }
```

For listener role $j$, define $a_j$ as accepted tasks, $c_j$ as successful sessions, $e_j$ as ordinary session errors, $x_j$ as cancellations, $p_j$ as panics, and $n_j$ as tracked tasks not yet joined. Before the diagnostic `u64` counters saturate, the accounting relation is

$$
a_j = c_j + e_j + x_j + p_j + n_j.
$$

For example, after accepting three producer sessions, one clean close and one malformed-frame error leave $a_0=3$, $c_0=1$, $e_0=1$, and $n_0=1$. Forcing and joining the last session increments $x_0$ to 1 and reduces $n_0$ to zero. Requesting abort alone does not change those counts.

A malformed peer is an ordinary session error, not evidence that all partitions are unsafe. A panicking session or unexpected cancellation is a supervisor failure. We manually join results rather than call a convenience shutdown operation that would hide panic details. The [JoinSet API](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html) distinguishes requesting `abort_all` from removing and joining task results. Keep that distinction in the counters as well as in the ownership model.

## Preserve owner failures even during a requested stop

```rust title="crates/broker-app/src/supervisor.rs"
    fn noted_owner(&mut self, index: usize, notice: Option<OwnerExit>, unexpected: bool) {
        self.seen[index] = true;
        let failure = match &notice {
            Some(OwnerExit::Failed { state, cause }) => Some(format!(
                "partition {index} failed at visible {}: {cause:?}", state.visible_end.get())),
            Some(OwnerExit::Panicked) => Some(format!("partition {index} panicked")),
            None => Some(format!("partition {index} lost its exit notice")),
            Some(OwnerExit::Stopped { checkpointed, .. }) if unexpected || !checkpointed =>
                Some(format!("partition {index} exited without the supervised finish barrier")),
            _ => None,
        };
        self.notices[index] = notice;
        if let Some(message) = failure { self.fail(message); }
    }

```

A clean exit while the service is still serving is unexpected: one partition disappearing is not a healthy smaller broker. During draining, a `Stopped` notice must also say the supervised barrier was requested. An ordinary close from some other path is not silently upgraded to a checkpointed shutdown.

No branch starts a replacement thread. A replacement needs recovered files, a fresh epoch, and a resource generation that does not overlap retained old pools. This chapter chooses process-level restart after cleanup, not an implicit restart loop that can repeatedly re-open damaged state.

## Drive signals, exits, joins, and accepts together

```rust title="crates/broker-app/src/supervisor.rs"
    pub async fn serve_until<F>(&mut self, stop: F) -> io::Result<()>
    where F: Future<Output = io::Result<()>> {
        if self.phase != Phase::Serving {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "serve requires Serving"));
        }
        tokio::pin!(stop);
        loop {
            let event = tokio::select! {
                result = &mut stop => Event::Stop(result),
                (index, notice) = next_exit(&mut self.exits, self.seen) => Event::Owner(index, notice),
                joined = self.tasks.join_next_with_id(), if !self.tasks.is_empty() =>
                    Event::Session(joined.expect("nonempty session set")),
                accepted = self.listeners[0].as_ref().expect("producer listener").accept(),
                    if self.active[0] < self.config.capacity[0] => Event::Accept(0, accepted),
                accepted = self.listeners[1].as_ref().expect("consumer listener").accept(),
                    if self.active[1] < self.config.capacity[1] => Event::Accept(1, accepted),
            };
            match event {
                Event::Stop(result) => {
                    let cause = match result {
                        Ok(()) => StopCause::Requested,
                        Err(error) => { self.fail(format!("shutdown signal: {error}")); StopCause::SignalFailed }
                    };
                    self.quiesce(cause);
                }
                Event::Owner(index, notice) => {
                    self.noted_owner(index, notice, true);
                    self.quiesce(StopCause::OwnerExited);
                }
                Event::Session(joined) => {
                    self.joined_session(joined);
                    if self.first_failure.is_some() { self.quiesce(StopCause::TransportFailed); }
                }
                Event::Accept(scope, Ok((stream, _))) => self.spawn_session(scope, stream),
                Event::Accept(_, Err(error)) => {
                    self.fail(format!("accept: {error}"));
                    self.quiesce(StopCause::TransportFailed);
                }
            }
            if self.phase != Phase::Serving { return Ok(()); }
        }
    }

```

`F: Future<Output = io::Result<()>>` lets the same loop accept an interrupt future, a one-shot test request, or a permanently pending future while testing fatal-owner detection. `tokio::pin!` gives repeated select iterations access to the same future rather than constructing a new signal registration each time. The phase check rejects accidental use after shutdown has begun.

The default select order is not biased toward accepting sockets. At capacity, only that role's accept branch is disabled; joining a completed task can enable it again. This is cooperative scheduling, not a hard latency guarantee under an adversarial executor. Owner exit observation does not depend on whether any client is waiting for that owner's reply.

The match contains no await. Once a selected event initiates quiescence, the loop returns without creating another listener-accept future. Errors from a peer do not initiate it; storage failure, unexpected owner exit, a task panic, signal failure, or listener failure do.

## Quiesce without reserving one more message

```rust title="crates/broker-app/src/supervisor.rs"
    pub fn request_shutdown(&mut self) { self.quiesce(StopCause::Requested); }

    fn quiesce(&mut self, cause: StopCause) {
        if !matches!(self.phase, Phase::Starting | Phase::Serving) { return; }
        self.cause = Some(cause);
        self.phase_to(Phase::Quiescing);
        // No await between listener removal and the four admission-closure boundaries.
        self.listeners = [None, None];
        for (index, recorded) in self.broker.begin_shutdown().into_iter().enumerate() {
            if !recorded { self.fail(format!("partition {index} was already closed before finish was requested")); }
        }
        self.stop.send_replace(true);
        self.network_deadline = Some(Instant::now() + self.config.network_grace);
        self.phase_to(Phase::Draining);
    }

```

The public method takes no caller-supplied cause; ordinary callers can request a stop but cannot label it as a fabricated internal failure. Internal paths supply their actual cause. The first transition wins; later calls are idempotent and do not overwrite either the cause or the original network deadline.

Listener removal prevents new accepts. The four finish requests stop new owner admissions and wake capacity waiters. Existing session tasks then see the stop notification at a safe request boundary. There is no `.await` in this sequence, but the four mailbox locks are separate linearization points: a command can race before one partition's closure and after another's. Treat accepted work independently per partition.

Let $t_q$ be the instant recorded after these finish requests and $g$ the configured network grace period. Define the stored deadline $D=t_q+g$. A caller may use several observation deadlines $E_1,E_2,\ldots$ while the single $D$ remains fixed. For $t_q=100$ milliseconds and $g=2000$ milliseconds, $D=2100$ milliseconds. Returning unfinished at 600 milliseconds and observing again at 2600 does not move $D$ to 4600.

## Bound network waiting without inventing a file-I/O timeout

```rust title="crates/broker-app/src/supervisor.rs"
    pub async fn drain_until(&mut self, observation_deadline: Instant) -> io::Result<DrainStatus> {
        if self.phase != Phase::Draining {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "drain requires Draining"));
        }
        loop {
            let network_deadline = self.network_deadline.expect("drain has one absolute deadline");
            if !self.network_forced && !self.tasks.is_empty() && Instant::now() >= network_deadline {
                self.network_forced = true;
                self.tasks.abort_all();
            }
            if self.tasks.is_empty() && self.seen.iter().all(|seen| *seen) {
                // Clients held elsewhere may still retain their own pools; do not restart in-process.
                self.clients.take();
                return Ok(DrainStatus::ReadyToJoin);
            }
            if Instant::now() >= observation_deadline { return Ok(DrainStatus::Pending); }
            tokio::select! {
                joined = self.tasks.join_next_with_id(), if !self.tasks.is_empty() =>
                    self.joined_session(joined.expect("nonempty session set")),
                (index, notice) = next_exit(&mut self.exits, self.seen),
                    if self.seen.iter().any(|seen| !*seen) => self.noted_owner(index, notice, false),
                _ = sleep_until(network_deadline), if !self.network_forced && !self.tasks.is_empty() => {},
                _ = sleep_until(observation_deadline) => return Ok(DrainStatus::Pending),
            }
        }
    }

```

At or after $D$, unfinished sessions receive an abort request. The `JoinSet` remains owned and is still drained. A network deadline is not a hard real-time bound: cancellation needs executor progress, and a task that never yields can delay it. Our session code performs asynchronous socket operations; synchronous storage I/O belongs to dedicated owner threads.

An observation deadline returning `Pending` transfers no handles and changes no phase. It is a useful place for a CLI to print which owner notices or tasks are still missing. Calling `join` early returns `WouldBlock`; it does not detach children to satisfy a deadline.

With $N$ tracked sessions and $U$ unseen owner notices, this method may return `ReadyToJoin` only when

$$
N=0 \quad\text{and}\quad U=0.
$$

The result still leaves the phase `Draining`, because an exit notice precedes the final operating-system thread join. A blocked file synchronization can keep $U>0$ forever. Sending a stop signal, wrapping waiting code in a timeout, or moving the I/O into `spawn_blocking` does not make started blocking I/O abortable; Tokio documents that limitation for [spawn_blocking](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html). We retain the handles and report unfinished work rather than weakening the directory-ownership contract.

If both deadlines are ready, returning `Pending` is still honest. A later call can collect the remaining results. There is no requirement that a diagnostic observation consume every concurrently ready notice before returning.

## Join synchronously, and make Drop's limitation explicit

```rust title="crates/broker-app/src/supervisor.rs"
    // Synchronous and called outside the async runtime. ReadyToJoin is not yet Stopped.
    pub fn join(&mut self) -> io::Result<ShutdownReport> {
        if self.phase != Phase::Draining || !self.tasks.is_empty() || self.seen.iter().any(|seen| !*seen) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "transport or owners remain unjoined"));
        }
        let results = self.broker.join_all();
        for (index, result) in results.into_iter().enumerate() {
            if let Err(error) = result { self.fail(format!("join partition {index}: {error:?}")); }
        }
        self.clients.take();
        self.phase_to(if self.first_failure.is_some() { Phase::Failed } else { Phase::Stopped });
        Ok(self.report())
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if !matches!(self.phase, Phase::Stopped | Phase::Failed) {
            self.request_shutdown();
            self.tasks.abort_all();
            // Drop cannot await. Report misuse; LocalBroker still joins every OS owner.
            use std::io::Write;
            let _ = writeln!(io::stderr(), "supervisor dropped before explicit task/thread joins; no clean shutdown report");
        }
    }
}

#[cfg(test)]
mod tests;
```

`join_all` collects every owner result before the supervisor considers the terminal report. A failure in partition 0 must not skip joins for partitions 1–3. We call this synchronous method **outside** the current-thread runtime's serving future. The final thread join can still wait for a thread epilogue or destructor; there is no claimed finite bound for arbitrary blocking cleanup.

Cancelling a borrowed drive future leaves this object intact. Dropping the entire object does not. `Drop` cannot perform an asynchronous join loop: it requests closure, aborts tasks, emits a misuse diagnostic, and relies on the existing blocking owner-handle cleanup to avoid quietly detaching OS owners. It does **not** produce a clean report or promise that every session task's destructor has already run. The explicit `request_shutdown` → `drain_until` → `join` protocol is the required successful path. Do not treat the fallback as an alternative normal API, especially inside an async task where blocking owner cleanup could stall the runtime.

The struct declares `tasks` before `broker`, so fallback field destruction drops the task set before the broker's blocking field cleanup. Rust's [destructor rules](https://doc.rust-lang.org/reference/destructors.html) define declaration-order field destruction. That order requests task cancellation first; it still cannot turn an async destructor wait into a synchronous guarantee. Part 4 uses a similar ordering technique to release an injected blocking gate before fixture cleanup attempts to join its owner.

**Check before continuing:** with one owner blocked, call `drain_until` with an already reached observation deadline. The expected result is `Pending`, phase `Draining`, and an early `join` error. Explain why changing that to `Stopped` would permit a second owner to race the first. Continue to [Part 3](https://25d-228.github.io/rust-learning/chapter-17-part-03.html).

