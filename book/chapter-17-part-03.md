# Chapter 17 — Cancellation boundaries and supervised shutdown

## Part 3 — Sessions, commands, and a joined restart

**Prerequisite:** [Parts 1–2](https://25d-228.github.io/rust-learning/chapter-17.html). This part completes the app integration within the same **4.5-hour chapter**. It changes no dependency versions or feature flags. Tokio's watch, task, timer, networking, and signal features were already enabled in the prerequisite manifests.

## Construct a broker with the same production and test seams

Replace `crates/broker-app/src/local.rs` by concatenating the following three blocks. The configuration is now a small copyable value; its defaults preserve the serving limits from Chapter 16. `Arc<dyn Clock>` remains a shared trait-object clock. The generic `SyncIo` type is still moved into each owner thread, so it must be `Send + 'static`; the factory itself only runs synchronously during construction and needs neither bound.

```rust title="crates/broker-app/src/local.rs"
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use broker_core::budget::MemoryConfig;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::{Clock, MonotonicClock};
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::storage::recovery::{self, StorageSession};
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{PARTITION_COUNT, PartitionId};
use broker_core::worker::{BatchConfig, BoundedHandle, ExitWatch, WorkerError, WorkerState};
use crate::BoxError;
use crate::server::Clients;

#[derive(Clone, Copy)]
pub struct OwnerConfig {
    pub batch: BatchConfig,
    pub lanes: [usize; 3],
    pub window: usize,
    pub lease_ms: u64,
    pub checkpoint: CheckpointPolicy,
    pub memory: MemoryConfig,
}
impl Default for OwnerConfig {
    fn default() -> Self {
        Self { batch: BatchConfig::new(64, 2 * 1024 * 1024, Duration::from_millis(5)).expect("fixed batch"),
            lanes: [8, 8, 16], window: 256, lease_ms: 30_000,
            checkpoint: CheckpointPolicy::explicit_only(), memory: MemoryConfig::default() }
    }
}

```

`from_session` consumes the recovered `StorageSession` instead of reacquiring files through an unguarded path. It validates the fixed ordered topology before starting workers. One shared monotonic clock serves the default owners; tokens are still scoped by epoch and partition. A test can supply `ManualClock` and an injected synchronization adapter without changing the application protocol.

```rust title="crates/broker-app/src/local.rs"
pub struct LocalBroker { owners: Vec<BoundedHandle> }
impl LocalBroker {
    // Synchronous startup: call before entering the async serving runtime.
    pub fn open(path: &Path, initialize: bool) -> Result<Self, BoxError> {
        let session = if initialize {
            recovery::initialize(path, StorageConfig::new(8 * 1024 * 1024, 64, 16)?)?
        } else { recovery::recover(path)? };
        Self::from_session(session, OwnerConfig::default(), Arc::new(MonotonicClock::new()), |_| FileSync)
    }
    // The same constructor supports real I/O and the existing controlled I/O/clock seams.
    pub fn from_session<S, F>(session: StorageSession, config: OwnerConfig,
        clock: Arc<dyn Clock>, mut sync_for: F) -> Result<Self, BoxError>
    where S: SyncIo + Send + 'static, F: FnMut(PartitionId) -> S {
        let (epoch, logs) = session.into_parts();
        if logs.len() != PARTITION_COUNT as usize
            || logs.iter().enumerate().any(|(i, log)| log.partition().get() as usize != i)
        { return Err("startup must supply four ordered partitions".into()); }
        let leases = LeaseConfig::new(epoch, config.window, config.lease_ms)?;
        let mut broker = Self { owners: Vec::with_capacity(PARTITION_COUNT as usize) };
        for log in logs {
            let sync = sync_for(log.partition());
            let owner = BoundedHandle::start(log, sync, config.batch, config.lanes, leases,
                Arc::clone(&clock), config.checkpoint, config.memory)?;
            broker.owners.push(owner);
        }
        Ok(broker)
    }
```

If a later owner fails to start, `broker` is dropped and closes/joins every owner already pushed into the vector. The unstarted logs are also dropped. No partially initialized broker escapes. The following accessors expose observation and coordinated finish only inside the app crate, while the existing public clients remain request handles rather than shutdown handles.

```rust title="crates/broker-app/src/local.rs"
    pub fn clients(&self) -> Clients {
        Arc::new(std::array::from_fn(|index| self.owners[index].client()))
    }
    pub(crate) fn exits(&self) -> [ExitWatch; PARTITION_COUNT as usize] {
        std::array::from_fn(|index| self.owners[index].exits())
    }
    pub(crate) fn begin_shutdown(&self) -> [bool; PARTITION_COUNT as usize] {
        std::array::from_fn(|index| self.owners[index].begin_shutdown())
    }
    pub(crate) fn join_all(&mut self) -> Vec<Result<WorkerState, WorkerError>> {
        for owner in &self.owners { owner.close(); }
        self.owners.drain(..).map(BoundedHandle::join).collect()
    }
    // Legacy close: flushes appends, but does not implicitly request consumer checkpoints.
    pub fn join(mut self) -> Result<(), BoxError> {
        let results = self.join_all();
        for result in results { result?; }
        Ok(())
    }
}
impl Drop for LocalBroker {
    fn drop(&mut self) {
        for owner in &self.owners { owner.close(); }
        // Each WorkerHandle joins on Drop. This is blocking fallback, not async shutdown.
    }
}
```

Notice that `join_all` first closes **every** owner, then consumes every handle into a collected result vector. It does not apply `?` while walking live handles. The legacy public `join` can return the first error only after all joins have been attempted. A preceding supervised finish remains recorded because ordinary `close` does not erase its checkpoint flag.

Replace `crates/broker-app/src/lib.rs` with the following complete module list. The error alias remains unchanged.

```rust title="crates/broker-app/src/lib.rs"
pub mod client;
pub mod local;
pub mod protocol;
pub mod server;
pub mod supervisor;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
```

## Interrupt only a safe session boundary during the grace period

In `crates/broker-app/src/server.rs`, replace the old `session` function with the following two functions. Its imports already include `watch`, `AsyncRead`, `AsyncWrite`, and `Instant`; keep those imports and all dispatch helpers. The old `serve_listener` remains available for the Chapter 16 regression tests, but the supervised `brokerd` no longer uses it.

```rust title="crates/broker-app/src/server.rs"
pub async fn session<S>(stream: S, endpoint: Endpoint, clients: Clients, limits: SessionLimits)
    -> io::Result<()>
where S: AsyncRead + AsyncWrite + Unpin {
    let (_keep_alive, stop) = watch::channel(false);
    session_until(stream, endpoint, clients, limits, stop).await
}

pub async fn session_until<S>(mut stream: S, endpoint: Endpoint, clients: Clients,
    limits: SessionLimits, mut stop: watch::Receiver<bool>) -> io::Result<()>
where S: AsyncRead + AsyncWrite + Unpin {
    limits.validate()?;
    loop {
        if *stop.borrow() { return Ok(()); }
        let incoming = tokio::select! {
            biased;
            _ = stop.changed() => return Ok(()),
            header = protocol::read_header(&mut stream, Instant::now() + limits.header) => header?,
        };
        let Some(header) = incoming else { return Ok(()); };
        let scope = protocol::validate_request(header);
        if scope.is_err() || !endpoint.accepts(header.op) {
            refuse(&mut stream, header, Status::Invalid, limits).await?;
            // In particular, do not try to drain a rejected append's unread body.
            return Ok(());
        }
        let (partition, _) = scope.expect("validated request");
        let client = &clients[usize::from(partition.get())];
        match dispatch(&mut stream, header, client, limits).await {
            Ok(output) => send_output(&mut stream, header, output, limits).await?,
            Err(CallError::Status(status)) => {
                refuse(&mut stream, header, status, limits).await?;
                return Ok(());
            }
            Err(CallError::Transport(error)) => return Err(error),
        }
        // Only after a full response do we read the next request's fixed header.
    }
}

```

The wrapper preserves the old no-stop session API by retaining a sender whose value remains false. The new `session_until` owns both socket and stop receiver. The small biased selection prioritizes a ready stop notification over starting another header read. Dropping a partial header is safe **because the function then returns and drops the entire socket**; it never reuses the stream at an unknown byte position.

After a complete header, dispatch and response writing run without selecting against the stop signal. Already accepted appends can finish and report their synchronized offsets during the grace period. A prepared partial body is not accepted work: after owner admission closes, its eventual submit is refused. If it stalls, the supervisor eventually aborts its session; normal ownership cleanup releases its slot, buffer, and disk reservation. Chapter 16's per-phase timeouts remain in effect and may end an exchange before the global grace period.

This is **not** a promise to let every request that reached a socket finish. A header can be complete while admission is still waiting. Such a request may receive `Closed`/refusal after quiescence. Nor does it cancel consumer application work. A valid acknowledgement accepted before closure is processed normally; one submitted afterward is rejected, and its record can repeat after restart.

A fetch response's batch remains owned across its socket writes. Aborting the session drops the batch and returns pool capacity. It does not undo the leases already installed by the owner. No extra payload vector or unbounded response queue is introduced.

Replace `ListenerStats` with the following definition. Existing immediate-listener accounting remains for its prerequisite API; the new supervisor uses its own identity-aware join accounting.

```rust title="crates/broker-app/src/server.rs"
#[derive(Debug, Default, Clone, Copy)]
pub struct ListenerStats {
    pub accepted: u64,
    pub completed: u64,
    pub session_errors: u64,
    pub cancelled: u64,
    pub panicked: u64,
}
```

## Keep the supervisor outside the drive future

Replace `crates/broker-app/src/bin/brokerd.rs` with this complete executable. It starts persistent owners before serving; after the async drain reports readiness, the synchronous thread joins run outside `block_on`.

```rust title="crates/broker-app/src/bin/brokerd.rs"
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::Instant;
use broker_app::BoxError;
use broker_app::local::LocalBroker;
use broker_app::supervisor::{DrainStatus, Phase, Supervisor, SupervisorConfig};

fn main() -> Result<(), BoxError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 || !matches!(args[0].as_str(), "init" | "open") {
        return Err("usage: brokerd init|open DIRECTORY PRODUCER_PORT CONSUMER_PORT".into());
    }
    let root = PathBuf::from(&args[1]);
    let producer_port: u16 = args[2].parse()?;
    let consumer_port: u16 = args[3].parse()?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let broker = LocalBroker::open(&root, args[0] == "init")?;
    // The supervisor outlives the borrowed asynchronous drive futures.
    let mut supervisor = Supervisor::new(broker, SupervisorConfig::default())?;
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    runtime.block_on(async {
        match supervisor.bind(SocketAddr::new(ip, producer_port), SocketAddr::new(ip, consumer_port)).await {
            Ok(addresses) => {
                println!("LISTEN producer={} consumer={}", addresses[0], addresses[1]);
                supervisor.serve_until(tokio::signal::ctrl_c()).await?;
            }
            Err(error) => eprintln!("listener startup failed: {error}"),
        }
        loop {
            match supervisor.drain_until(Instant::now() + Duration::from_secs(5)).await? {
                DrainStatus::ReadyToJoin => break,
                DrainStatus::Pending => eprintln!("shutdown still unfinished: {:?}", supervisor.report()),
            }
        }
        Ok::<(), io::Error>(())
    })?;
    let report = supervisor.join()?;
    println!("SHUTDOWN {:?}", report);
    if report.phase == Phase::Failed { return Err(io::Error::other("broker shutdown reported failure").into()); }
    Ok(())
}
```

The five-second value here is an **observation interval**, not a new network grace period and not a forced exit timer. The supervisor recorded its two-second network deadline once. If a file operation is still blocked, the CLI prints another unfinished report and continues to retain the owner handles and directory ownership. It never prints a terminal success merely because a timer fired.

A listener bind failure already initiated quiescence; the same drain/join path still runs. An interrupt registration error becomes a failure cause rather than bypassing cleanup. A terminal failed report gives the process an error exit status after cleanup. This executable handles Ctrl-C through the existing Tokio signal feature; it does not introduce a service manager or claim to intercept every operating-system termination mode.

## Calculate a complete normal shutdown

Start with two synchronized records, offsets 0 and 1, and both groups at persisted next offset 0. Group 0 fetches both. Completing 1 first leaves its volatile next offset at 0; completing 0 retires the contiguous pair and moves volatile next to 2. No ordinary checkpoint has yet been requested.

```text
                                        group 0              group 1
                                   volatile  persisted   volatile  persisted
initialized                           0          0          0          0
append cat; append dog                 0          0          0          0
acknowledge offset 1                  0          0          0          0
acknowledge offset 0                  2          0          0          0
final owner barrier                   2          2          0          0
joined restart                        2          2          0          0
```

The final owner barrier publishes group 0's checkpoint. Group 1 has no dirty advancement. Restart uses the persisted frontiers and a new epoch; it does not restore old in-flight leases. No record is deleted by this shutdown sequence.

Create `crates/broker-app/examples/shutdown_probe.rs`. The example owns its error path explicitly: it records an exercise error, still performs shutdown, and only then returns the exercise error. It drops old client-owned pools before opening another resource generation. The second broker performs only progress reads, so the legacy close/join suffices for that verification phase.

```rust title="crates/broker-app/examples/shutdown_probe.rs"
use std::io;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{Instant, timeout};
use broker_app::BoxError;
use broker_app::local::LocalBroker;
use broker_app::supervisor::{DrainStatus, Phase, Supervisor, SupervisorConfig};
use broker_core::delivery::progress::ProgressAck;
use broker_core::storage::segment::ReadLimits;
use broker_core::types::GroupId;
use broker_core::worker::AsyncTicket;

async fn receive<T>(ticket: AsyncTicket<T>) -> Result<T, BoxError> {
    let delivered = timeout(Duration::from_secs(10), ticket).await?;
    let result = delivered?;
    Ok(result?)
}
```

`receive` unwraps three different results in order: the observation timeout, the one-shot delivery, and the owner's result. None means the same thing as “the command was rolled back.” The helper is bounded for this executable check; an error from it is followed by the surrounding explicit cleanup.

```rust title="crates/broker-app/examples/shutdown_probe.rs"
fn main() -> Result<(), BoxError> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: shutdown_probe NEW_DIRECTORY")?);
    if args.next().is_some() { return Err("usage: shutdown_probe NEW_DIRECTORY".into()); }
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let broker = LocalBroker::open(&root, true)?;
    let clients = broker.clients();
    let mut supervisor = Supervisor::new(broker, SupervisorConfig::default())?;
    let g0 = GroupId::new(0)?; let g1 = GroupId::new(1)?;
    let exercise: Result<(), BoxError> = runtime.block_on(async {
        // Collect an exercise error, then still perform the normal shutdown protocol.
        let result: Result<(), BoxError> = async {
            for payload in [b"cat", b"dog"] {
                let mut request = clients[0].try_prepare_append(3)
                    .map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
                request.payload_mut().copy_from_slice(payload);
                receive(request.submit().map_err(|error| io::Error::other(format!("submit: {error:?}")))?).await?;
            }
            let request = clients[0].try_prepare_lease(g0, ReadLimits::new(2, 128)?)
                .map_err(|error| io::Error::other(format!("fetch admission: {error:?}")))?;
            let batch = receive(request.submit().map_err(|error| io::Error::other(format!("fetch submit: {error:?}")))?).await?;
            let tokens: Vec<_> = batch.records().map(|record| record.token).collect();
            if tokens.len() != 2 { return Err("expected two leased records".into()); }
            drop(batch);
            for token in [tokens[1], tokens[0]] {
                let ticket = clients[0].try_ack_progress(g0, token)
                    .map_err(|error| io::Error::other(format!("ack admission: {error:?}")))?;
                if !matches!(receive(ticket).await?, ProgressAck::Accepted(_)) {
                    return Err("acknowledgement was not accepted".into());
                }
            }
            let ticket = clients[0].try_progress(g0)
                .map_err(|error| io::Error::other(format!("progress admission: {error:?}")))?;
            let before = receive(ticket).await?;
            println!("before shutdown: group0 volatile={} persisted={}",
                before.volatile_next.get(), before.persisted_next.get());
            if before.volatile_next.get() != 2 || before.persisted_next.get() != 0 {
                return Err("unexpected pre-shutdown progress".into());
            }
            Ok(())
        }.await;
        supervisor.request_shutdown();
        loop {
            match supervisor.drain_until(Instant::now() + Duration::from_secs(5)).await? {
                DrainStatus::ReadyToJoin => break,
                DrainStatus::Pending => eprintln!("unfinished shutdown: {:?}", supervisor.report()),
            }
        }
        result
    });
```

The exercise deliberately sends the later acknowledgement first, and never calls `try_checkpoint`. That makes the difference between volatile progress and the final barrier visible. The response batch is dropped after copying its small tokens, so the example does not accidentally overlap a retained response allocation with its restarted service.

```rust title="crates/broker-app/examples/shutdown_probe.rs"
    let report = supervisor.join()?;
    println!("shutdown: {:?}; sessions={}; owners_pending={}",
        report.phase, report.active_sessions, report.pending_owners);
    // The new generation below must not overlap this generation's client-owned pools.
    drop(clients); drop(supervisor);
    exercise?;
    if report.phase != Phase::Stopped { return Err("shutdown failed; inspect its report".into()); }
    let restarted = LocalBroker::open(&root, false)?;
    let clients = restarted.clients();
    let verification: Result<(), BoxError> = runtime.block_on(async {
        for group in [g0, g1] {
            let ticket = clients[0].try_progress(group)
                .map_err(|error| io::Error::other(format!("progress admission: {error:?}")))?;
            let progress = receive(ticket).await?;
            let expected = if group == g0 { 2 } else { 0 };
            if progress.volatile_next.get() != expected || progress.persisted_next.get() != expected {
                return Err("restart did not use the saved contiguous frontier".into());
            }
            println!("after restart: group{} volatile={} persisted={}", group.get(),
                progress.volatile_next.get(), progress.persisted_next.get());
        }
        Ok(())
    });
    drop(clients);
    let joined = restarted.join();
    verification?; joined?;
    Ok(())
}
```

Run from the learner workspace after completing Part 4's files:

```bash
cargo check --workspace --all-targets
probe_parent=$(mktemp -d)
cargo run -p broker-app --example shutdown_probe -- "$probe_parent/broker"
```

The child path does not already exist; initialization must not overwrite an established broker. Expected output, **not a recorded execution**:

```text
before shutdown: group0 volatile=2 persisted=0
shutdown: Stopped; sessions=0; owners_pending=0
after restart: group0 volatile=2 persisted=2
after restart: group1 volatile=0 persisted=0
```

A read-only reopen can finish with legacy `join`. A service that wants final progress publication must use the supervisor's finish path. Do not infer from this example that every ordinary handle drop now saves all consumer progress.

## Exercise the same policy over TCP

Use a separate fresh child directory. In terminal A, from the workspace:

```bash
demo_parent=$(mktemp -d)
echo "$demo_parent/broker"
cargo run -p broker-app --bin brokerd -- init "$demo_parent/broker" 7000 7001
```

Keep the printed directory path. In terminal B, from the same workspace:

```bash
cargo run -p broker-app --bin brokerctl -- 127.0.0.1:7000 append 0 cat
cargo run -p broker-app --bin brokerctl -- 127.0.0.1:7000 append 0 dog
cargo run -p broker-app --bin brokerctl -- 127.0.0.1:7001 consume 0 0
cargo run -p broker-app --bin brokerctl -- 127.0.0.1:7001 progress 0 0
```

With both acknowledgements accepted, the last command reports volatile next 2 and persisted next 0. Now press Ctrl-C in terminal A **after the consume command has completed**. Expect a `SHUTDOWN` report with phase `Stopped`, no pending owners, and no active sessions. Its individual connection counters depend on which clients connected; do not match an entire debug string as a stable protocol.

In terminal A, reopen the same path:

```bash
cargo run -p broker-app --bin brokerd -- open "$demo_parent/broker" 7000 7001
```

In terminal B, query group 0 again and expect volatile/persisted next 2/2 with a new epoch. Query group 1 and expect 0/0. Stop this instance with Ctrl-C too. No `checkpoint` command was used: the new final barrier caused the persisted change. A forced process kill before final publication is a different experiment and can repeat more records; Chapters 10 and 13 define that recovery contract.

Ports 7000 and 7001 are examples, not requirements. An already occupied second port should produce a startup error and still clean up all owners. Never repeatedly retry initialization on a failed startup directory; use explicit recovery or a genuinely fresh fixture path as appropriate.

## What remains after Stopped

A caller can retain a `PooledLeaseBatch` after all workers are joined. Its payload still exists and its pool capacity remains charged to that old resource generation. An old `BoundedClient` clone can likewise retain idle buffers even though it refuses new requests. Neither keeps the partition file lock alive once the owner has released its storage.

Therefore the following implications are different:

```text
all owners joined         => no live owner threads from this instance
storage lock released     => this process no longer excludes another storage owner
all pool owners dropped   => old pool allocations can be freed
```

A lock acquired by the next instance is not proof of the last statement. This implementation has no automatic in-process replacement loop; explicit examples drop old clients/batches before starting another generation. That preserves Chapter 15's single-generation payload-capacity claim without pretending it is a whole-process RSS measurement.

**Next:** [Part 4 — Controlled shutdown and failure checks](https://25d-228.github.io/rust-learning/chapter-17-part-04.html) supplies the complete fixtures, failure cases, runnable checks, and diagnosis guide.

