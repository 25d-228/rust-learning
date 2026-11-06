# Chapter 16 — Framed TCP sessions and clients

## Part 3 — Cancellation-safe clients and loopback commands

> **Learning objectives**
> Make cancellation close the actual in-progress client socket, distinguish a transport success from an applied acknowledgement, initialize or recover the broker explicitly, and perform work before acknowledging without claiming an atomic external side effect.

This is the third part of the shared **4.5-hour chapter**. Assemble [Part 1](https://25d-228.github.io/rust-learning/chapter-16.html) and [Part 2](https://25d-228.github.io/rust-learning/chapter-16-part-02.html) first. This part completes the library client, local broker construction, and both command-line programs. [Part 4](https://25d-228.github.io/rust-learning/chapter-16-part-04.html) supplies all test helpers and 33 tests.

## A partial exchange must not leave a reusable socket behind

A tempting client implementation borrows `&mut self.stream` while sending a request and reading its response. If the caller cancels that future, the borrow ends but the stream remains in `self`. The next method can then read the tail of the abandoned response as though it were a new header. Serializing methods with `&mut self` alone does not solve this problem.

Instead, store `Option<TcpStream>`. At the start of a validated exchange, take the stream out, leaving `None`. The future owns the stream across every network suspension. Put it back only after the entire response is received and validated. If the future is dropped, its local stream is dropped with it. The client remains observably unusable and reconnecting is an explicit choice. [`Option::take`](https://doc.rust-lang.org/std/option/enum.Option.html#method.take) supplies exactly this move-and-leave-`None` operation.

Create `src/client.rs`. The error enum preserves an underlying I/O/protocol cause or a typed remote status; it does not convert every failure to a string. An acknowledgement reply carries its disposition and both progress frontiers.

```rust title="crates/broker-app/src/client.rs"
use std::error::Error;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::Instant;

use broker_core::delivery::{AckOutcome, LeaseToken};
use broker_core::delivery::progress::{Checkpointed, ProgressState};
use broker_core::types::{Epoch, GroupId, Offset, PartitionId};
use crate::protocol::{self, FetchedBatch, Header, Op, Status};

#[derive(Debug)]
pub enum ClientError { Io(io::Error), Remote(Status) }
impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "transport or protocol: {error}"),
            Self::Remote(status) => write!(f, "broker response: {status:?}"),
        }
    }
}
impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self { Self::Io(error) => Some(error), Self::Remote(_) => None }
    }
}
impl From<io::Error> for ClientError { fn from(error: io::Error) -> Self { Self::Io(error) } }

#[derive(Debug, Clone, Copy)]
pub struct AckReply { pub outcome: AckOutcome, pub progress: ProgressState, pub retired: u64 }
#[derive(Debug, Clone, Copy)]
pub struct RetentionReply {
    pub before: Offset, pub floor: Offset, pub removed_bytes: u64,
    pub removed_segments: u64, pub rotated: bool,
}
enum Reply { Small(Header), Batch(FetchedBatch) }

pub struct Client { stream: Option<TcpStream>, next_id: u64, timeout: Duration }
```

`connect` bounds its own establishment wait. Each subsequent `call` has one whole-exchange deadline, independent of the server's phase deadlines. A local invalid request is rejected before taking the socket. Once a network exchange starts, any error makes this connection unusable, including a complete remote refusal: the server's refusal policy closes too.

The checked request counter prevents wrapping to an old identifier. No deduplication table exists on the server. Repeating the same logical append after reconnecting can create another record even when its request identifier resembles an earlier one.

```rust title="crates/broker-app/src/client.rs"
impl Client {
    pub async fn connect(address: SocketAddr, timeout: Duration) -> Result<Self, ClientError> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "client timeout must be in (0, 60s]").into());
        }
        let stream = protocol::within(Instant::now() + timeout, TcpStream::connect(address)).await?;
        Ok(Self { stream: Some(stream), next_id: 1, timeout })
    }
    pub fn is_usable(&self) -> bool { self.stream.is_some() }

    async fn call(&mut self, mut request: Header, payload: &[u8]) -> Result<Reply, ClientError> {
        request.id = self.next_id;
        protocol::validate_request(request)?;
        if payload.len() != request.body as usize { return Err(protocol::invalid("request body mismatch").into()); }
        let next = self.next_id.checked_add(1).ok_or_else(|| protocol::invalid("request identifier exhausted"))?;
        // Once polled this far, cancellation must close rather than recycle a partial stream.
        let mut stream = self.stream.take().ok_or_else(||
            io::Error::new(io::ErrorKind::NotConnected, "client connection is closed; reconnect explicitly"))?;
        self.next_id = next;
        let deadline = Instant::now() + self.timeout;
        let result: Result<Reply, ClientError> = async {
            protocol::write_bytes(&mut stream, &request.encode(), deadline).await?;
            if !payload.is_empty() { protocol::write_bytes(&mut stream, payload, deadline).await?; }
            let response = protocol::read_header(&mut stream, deadline).await?
                .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing response"))?;
            protocol::validate_response(request, response)?;
            if response.status != Status::Ok { return Err(ClientError::Remote(response.status)); }
            if request.op != Op::Fetch { return Ok(Reply::Small(response)); }
            // Shape and request-specific bounds were checked before this allocation.
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(response.body as usize).map_err(io::Error::other)?;
            bytes.resize(response.body as usize, 0);
            protocol::read_body(&mut stream, &mut bytes, deadline).await?;
            let batch = FetchedBatch::decode(response, bytes)?;
            Ok(Reply::Batch(batch))
        }.await;
        if result.is_ok() { self.stream = Some(stream); }
        // On any error, or if this future is dropped while pending, the local socket is dropped.
        result
    }

```

The inner async block borrows the local `stream`; it does not borrow a stream still stored inside `self`. If `call` is cancelled while awaiting that block, dropping the enclosing future drops both the inner borrow and the actual local stream owner. This is stronger than merely dropping a temporary `Pin<&mut Future>`, which Chapter 6 showed can leave its underlying future alive.

`is_usable()` reports whether this client still owns a reusable stream. An unpolled call has not taken anything; cancelling after the `take` has. The tests arrange a known partial-response point before cancelling so they exercise the latter case.

A fetch response is validated from its 64-byte envelope **before** allocating its body. The requested record/byte caps constrain the server's dimensions, and the global wire maximum is another check. `try_reserve_exact` reports an allocation failure rather than deliberately relying on allocation panic behavior; reported capacity can still exceed requested capacity. The client vector is outside the server's pool accounting. After reading, checksums, ranges, tokens, and the complete body length are verified before the socket is restored.

The public methods below expose typed operations instead of requiring callers to fill the five argument words. They all reuse that single exchange path; none silently retries after `Uncertain` or a broken socket.

```rust title="crates/broker-app/src/client.rs"
    pub async fn append(&mut self, partition: PartitionId, payload: &[u8]) -> Result<Offset, ClientError> {
        let mut request = Header::request(Op::Append, partition, GroupId::new(0).expect("fixed group"));
        request.body = u32::try_from(payload.len()).map_err(|_| protocol::invalid("payload length overflow"))?;
        match self.call(request, payload).await? {
            Reply::Small(header) => Ok(Offset::new(header.args[0])),
            Reply::Batch(_) => unreachable!("append is a validated small response"),
        }
    }
    pub async fn fetch(&mut self, partition: PartitionId, group: GroupId, records: usize, bytes: usize)
        -> Result<FetchedBatch, ClientError>
    {
        let mut request = Header::request(Op::Fetch, partition, group);
        request.args = [records as u64, bytes as u64, 0, 0, 0];
        match self.call(request, &[]).await? {
            Reply::Batch(batch) => Ok(batch), Reply::Small(_) => unreachable!("fetch response is a batch"),
        }
    }
    pub async fn acknowledge(&mut self, token: LeaseToken) -> Result<AckReply, ClientError> {
        let mut request = Header::request(Op::Ack, token.partition, token.group);
        request.args = [token.epoch.get(), token.offset.get(), token.generation, token.deadline_ms, 0];
        match self.call(request, &[]).await? {
            Reply::Small(header) => Ok(AckReply {
                outcome: protocol::decode_ack(header.args[0]).expect("validated acknowledgement"),
                progress: progress(header, 1, 2), retired: header.args[3],
            }),
            Reply::Batch(_) => unreachable!("acknowledgement is a small response"),
        }
    }
    pub async fn checkpoint(&mut self, partition: PartitionId, group: GroupId) -> Result<Checkpointed, ClientError> {
        match self.call(Header::request(Op::Checkpoint, partition, group), &[]).await? {
            Reply::Small(header) => Ok(Checkpointed { progress: progress(header, 0, 1), published: header.args[2] == 1 }),
            Reply::Batch(_) => unreachable!("checkpoint is a small response"),
        }
    }
    pub async fn progress(&mut self, partition: PartitionId, group: GroupId) -> Result<ProgressState, ClientError> {
        match self.call(Header::request(Op::Progress, partition, group), &[]).await? {
            Reply::Small(header) => Ok(progress(header, 0, 1)),
            Reply::Batch(_) => unreachable!("progress is a small response"),
        }
    }
    pub async fn retain(&mut self, partition: PartitionId) -> Result<RetentionReply, ClientError> {
        let request = Header::request(Op::Retain, partition, GroupId::new(0).expect("fixed group"));
        match self.call(request, &[]).await? {
            Reply::Small(header) => Ok(RetentionReply {
                before: Offset::new(header.args[0]), floor: Offset::new(header.args[1]),
                removed_bytes: header.args[2], removed_segments: header.args[3], rotated: header.args[4] == 1,
            }),
            Reply::Batch(_) => unreachable!("retention is a small response"),
        }
    }
}
fn progress(header: Header, volatile: usize, persisted: usize) -> ProgressState {
    let (partition, group) = header.scope().expect("validated scope");
    ProgressState { epoch: Epoch::new(header.args[4]).expect("validated epoch"), partition, group,
        volatile_next: Offset::new(header.args[volatile]), persisted_next: Offset::new(header.args[persisted]) }
}
```

The small response helper uses `expect` only after `validate_response` has checked its epoch and scope. `Reply::Small` versus `Reply::Batch` is also determined by that validated operation. A programming mismatch reaches `unreachable!`; an invalid peer response must be rejected earlier as an ordinary protocol error.

The `FetchedBatch` is independent of the client socket once returned. Its record views borrow the batch, not `Client`. It is therefore legal to iterate over borrowed records and await `client.acknowledge(record.token)` while retaining the batch. This costs one client-side body allocation for the duration of processing. Nothing in this API guarantees bounded memory if the application saves an unlimited number of returned batches.

## Construct exactly one local resource generation

Create `src/local.rs`. Startup is synchronous and happens before entering `Runtime::block_on`. Existing storage recovery may scan files, repair permitted tails, publish a new epoch, and synchronize metadata. These are not operations to hide inside an async socket handler.

`init` explicitly creates a new store; `open` explicitly recovers an existing one. Missing or corrupt established metadata never triggers a fallback initializer. The store keeps four partitions and two groups, with the same directory guard, format, checkpoint, and retained-floor rules already implemented.

```rust title="crates/broker-app/src/local.rs"
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use broker_core::budget::MemoryConfig;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::MonotonicClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::storage::recovery;
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::FileSync;
use broker_core::worker::{BatchConfig, BoundedHandle};
use crate::BoxError;
use crate::server::Clients;

pub struct LocalBroker { owners: Vec<BoundedHandle> }
impl LocalBroker {
    // Synchronous startup: call before entering the async serving runtime.
    pub fn open(path: &Path, initialize: bool) -> Result<Self, BoxError> {
        let session = if initialize {
            recovery::initialize(path, StorageConfig::new(8 * 1024 * 1024, 64, 16)?)?
        } else { recovery::recover(path)? };
        let (epoch, logs) = session.into_parts();
        let mut broker = Self { owners: Vec::with_capacity(4) };
        for log in logs {
            let owner = BoundedHandle::start(log, FileSync,
                BatchConfig::new(64, 2 * 1024 * 1024, Duration::from_millis(5))?,
                [8, 8, 16], LeaseConfig::new(epoch, 256, 30_000)?,
                Arc::new(MonotonicClock::new()), CheckpointPolicy::explicit_only(), MemoryConfig::default())?;
            broker.owners.push(owner);
        }
        Ok(broker)
    }
    pub fn clients(&self) -> Clients {
        Arc::new(std::array::from_fn(|index| self.owners[index].client()))
    }
    // Blocking, and intentionally not claimed to have a finite deadline.
    pub fn join(mut self) -> Result<(), BoxError> {
        for owner in &self.owners { owner.close(); }
        let mut first = None;
        for owner in self.owners.drain(..) {
            if let Err(error) = owner.join() {
                if first.is_none() { first = Some(error); }
            }
        }
        match first { Some(error) => Err(error.into()), None => Ok(()) }
    }
}
impl Drop for LocalBroker {
    fn drop(&mut self) {
        for owner in &self.owners { owner.close(); }
        // Each prerequisite WorkerHandle also joins on Drop; never detach an owner.
    }
}
```

The constructor chooses 8 MiB segment files, sparse index stride 64, and at most 16 retained segments per partition. A batch commits at 64 records, 2 MiB encoded bytes, or 5 milliseconds under the existing owner policy. The three mailbox capacities are 8 data, 8 query, and 16 urgent reservations. Each group has a 256-slot lease window and a 30-second lease duration. These values bound the demonstration; they are not benchmark-derived recommendations.

`MemoryConfig::default()` reuses Chapter 15's capacity-owned pools and scratch. There is one resource generation, not a new pool per socket. A monotonic lease clock is created for each owner. Lease deadlines are opaque process-local numbers paired with the epoch and scope; clients must not interpret them as Unix time or compare deadlines from different partitions.

Partial setup failure drops already-created owners. `LocalBroker::Drop` first closes them all; the prerequisite handle destructors join rather than detach. The explicit `join` method also closes all before joining any, remembers the first error, and continues joining the rest. Synchronous storage may block, so this method has no claimed finite completion bound. It must run outside the async serving runtime.

The checkpoint policy is `explicit_only`. An acknowledged record can therefore repeat after restart unless the relevant contiguous progress was checkpointed. Shutdown is not silently turned into a checkpoint operation.

## Start two loopback listeners and join what you start

Create `src/bin/brokerd.rs`. Both ports bind to `127.0.0.1`; this chapter does not expose an unauthenticated broker on all network interfaces. Bind both before starting either accept loop so failure of the second port does not leave a partially started service running.

```rust title="crates/broker-app/src/bin/brokerd.rs"
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

use broker_app::BoxError;
use broker_app::local::LocalBroker;
use broker_app::protocol::Endpoint;
use broker_app::server::{Clients, SessionLimits, serve_listener};

async fn run(clients: Clients, producer_port: u16, consumer_port: u16) -> io::Result<()> {
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    // Bind both before exposing either accept loop. A bind failure drops the first listener.
    let producer = TcpListener::bind(SocketAddr::new(ip, producer_port)).await?;
    let consumer = TcpListener::bind(SocketAddr::new(ip, consumer_port)).await?;
    println!("LISTEN producer={} consumer={}", producer.local_addr()?, consumer.local_addr()?);
    let (stop, changed) = watch::channel(false);
    let mut listeners = JoinSet::new();
    listeners.spawn(serve_listener(producer, Endpoint::Producer, Arc::clone(&clients),
        SessionLimits::default(), 16, changed.clone()));
    listeners.spawn(serve_listener(consumer, Endpoint::Consumer, clients,
        SessionLimits::default(), 16, changed));
    let mut failure = tokio::select! {
        signal = tokio::signal::ctrl_c() => signal.err(),
        joined = listeners.join_next() => match joined.expect("two listener tasks") {
            Ok(Ok(stats)) => {
                eprintln!("listener stopped: {stats:?}");
                Some(io::Error::other("listener stopped before interrupt"))
            }
            Ok(Err(error)) => Some(error),
            Err(error) => Some(io::Error::other(format!("listener task failed: {error}"))),
        },
    };
    let _ = stop.send(true);
    while let Some(joined) = listeners.join_next().await {
        match joined {
            Ok(Ok(stats)) => eprintln!("listener joined: {stats:?}"),
            Ok(Err(error)) => { if failure.is_none() { failure = Some(error); } }
            Err(error) => { if failure.is_none() { failure = Some(io::Error::other(error)); } }
        }
    }
    match failure { Some(error) => Err(error), None => Ok(()) }
}
```

The outer task set tracks the two listener tasks; each listener tracks its own sessions. An interrupt or an unexpected listener exit signals both listeners to stop. Their internal cleanup aborts and joins sessions, then the outer loop joins the listeners. The first observed failure is preserved while cleanup continues.

This minimum lifecycle code prevents detached transport tasks in the ordinary run path. It does not yet monitor the four partition owners continuously or implement a multi-phase graceful shutdown. A partition failure is observed through requests or later joining. Those limitations are explicit inputs to the next chapter.

```rust title="crates/broker-app/src/bin/brokerd.rs"
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
    let serve_result = runtime.block_on(run(broker.clients(), producer_port, consumer_port));
    // All network tasks have been joined; now join owners outside the async runtime.
    let join_result = broker.join();
    if let Err(error) = &join_result { eprintln!("owner join failed: {error}"); }
    serve_result?;
    join_result?;
    Ok(())
}
```

`new_current_thread()` is deliberate: network readiness and timers share the current thread, while the four existing partition owners perform blocking storage work on their own threads. `enable_all()` enables available I/O and timer drivers; without them, compiling async methods is not enough to make network/time operations runnable. See [`Builder::enable_all`](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.enable_all).

The executable joins owners after `block_on` has returned and all transport task sets have been drained. If serving and owner joining both fail, the join failure is also printed before the serving error is returned. Printing a useful error is not a substitute for joining or supervising the failed resource.

## Process first, then acknowledge, then checkpoint explicitly

Create `src/bin/brokerctl.rs`. The producer accepts a UTF-8 command-line argument as its example payload; the library API accepts arbitrary bytes. The consumer's concrete application work is a byte sum and a flushed report to stdout. The small `stdout.lock()` scope ends before the acknowledgement await, avoiding a synchronous output guard across that suspension.

```rust title="crates/broker-app/src/bin/brokerctl.rs"
use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::Duration;
use broker_app::BoxError;
use broker_app::client::Client;
use broker_app::protocol::MAX_ENCODED;
use broker_core::delivery::AckOutcome;
use broker_core::types::{GroupId, PartitionId};

const USAGE: &str = "usage: brokerctl ADDRESS append PARTITION TEXT | consume PARTITION GROUP | progress PARTITION GROUP | checkpoint PARTITION GROUP | retain PARTITION";

async fn run(args: &[String]) -> Result<(), BoxError> {
    if args.len() < 3 { return Err(USAGE.into()); }
    let address: SocketAddr = args[0].parse()?;
    let partition = PartitionId::new(args[2].parse()?)?;
    let command = args[1].as_str();
    let expected = match command { "append" | "consume" | "progress" | "checkpoint" => 4,
        "retain" => 3, _ => return Err(USAGE.into()) };
    if args.len() != expected { return Err(USAGE.into()); }
    let group = if matches!(command, "consume" | "progress" | "checkpoint") {
        Some(GroupId::new(args[3].parse()?)?)
    } else { None };
    let mut client = Client::connect(address, Duration::from_secs(30)).await?;
    match command {
        "append" => {
            let offset = client.append(partition, args[3].as_bytes()).await?;
            println!("APPENDED partition={} offset={}", partition.get(), offset.get());
        }
        "consume" => {
            let group = group.expect("parsed group");
            let batch = client.fetch(partition, group, 8, MAX_ENCODED).await?;
            for record in batch.records() {
                // The chosen application work is a checksum plus an observed/flushed report.
                let checksum: u64 = record.payload.iter().map(|byte| u64::from(*byte)).sum();
                {
                    let stdout = io::stdout();
                    let mut output = stdout.lock();
                    writeln!(output, "PROCESSED partition={} group={} offset={} bytes={} checksum={}",
                        partition.get(), group.get(), record.token.offset.get(), record.payload.len(), checksum)?;
                    output.flush()?;
                } // No stdout lock is held over the acknowledgement await.
                let reply = client.acknowledge(record.token).await?;
                println!("ACK outcome={:?} volatile_next={} persisted_next={}", reply.outcome,
                    reply.progress.volatile_next.get(), reply.progress.persisted_next.get());
                if !matches!(reply.outcome, AckOutcome::Applied | AckOutcome::Duplicate | AckOutcome::AlreadyRetired) {
                    return Err("acknowledgement rejected after processing; duplicate processing is possible".into());
                }
            }
            println!("FETCH records={} encoded_bytes={} stop={:?}; no checkpoint requested",
                batch.records().len(), batch.encoded_bytes, batch.stop);
        }
        "progress" => {
            let p = client.progress(partition, group.expect("parsed group")).await?;
            println!("PROGRESS epoch={} volatile_next={} persisted_next={}", p.epoch.get(),
                p.volatile_next.get(), p.persisted_next.get());
        }
        "checkpoint" => {
            let p = client.checkpoint(partition, group.expect("parsed group")).await?;
            println!("CHECKPOINT published={} persisted_next={}", p.published, p.progress.persisted_next.get());
        }
        "retain" => {
            let r = client.retain(partition).await?;
            println!("RETAIN before={} floor={} removed_bytes={} removed_segments={} rotated={}",
                r.before.get(), r.floor.get(), r.removed_bytes, r.removed_segments, r.rotated);
        }
        _ => unreachable!("validated command"),
    }
    Ok(())
}
```

There are three distinct observations: `PROCESSED` reports completed application work, `ACK` reports the broker's completion decision, and `CHECKPOINT` reports persisted contiguous progress. The consume command issues no automatic checkpoint. If an acknowledgement is rejected, the application work has already happened and can happen again after redelivery. If stdout blocks long enough, the lease can expire before the acknowledgement is processed.

Flushing stdout is not a transaction with broker metadata and is not proof of durable external storage. The example makes the ordering visible; it does not promise exactly-once processing. A production consumer requiring idempotent effects would need its own effect identity and durable coordination, outside this course's bounded project scope.

```rust title="crates/broker-app/src/bin/brokerctl.rs"
fn main() -> Result<(), BoxError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(run(&args))
}
```

## Build and run the complete chapter

Assemble the four parts before building: the integration target in Part 4 names all of its test modules. In a separate implementation workspace with overlays 1–16 applied, run:

```sh
cargo fmt --all
cargo test -p broker-app --test wire
cargo test --workspace --all-targets
cargo build -p broker-app --bins
```

The new integration target supplies 33 tests; the cumulative workspace supplies 379 tests. These are **expected local checks, not observed passes in this authoring environment**. No Rust compiler, Cargo, rustfmt, dependency resolution, or native macOS execution was available here. Do not treat generated source or independent Python fixture checks as compiler evidence. Keep the `Cargo.lock` produced by your real Cargo resolution; no fabricated lockfile is supplied.

Use two terminals in that same workspace. Choose a directory that does not yet exist; its parent must exist. Terminal A:

```sh
target/debug/brokerd init "$PWD/broker-wire-demo" 7000 7001
```

Expected startup line:

```text
LISTEN producer=127.0.0.1:7000 consumer=127.0.0.1:7001
```

Terminal B, with each command allowed to finish before the next:

```sh
target/debug/brokerctl 127.0.0.1:7000 append 0 cat
target/debug/brokerctl 127.0.0.1:7000 append 0 dog
target/debug/brokerctl 127.0.0.1:7001 consume 0 0
target/debug/brokerctl 127.0.0.1:7001 progress 0 0
target/debug/brokerctl 127.0.0.1:7001 checkpoint 0 0
target/debug/brokerctl 127.0.0.1:7001 retain 0
```

On a fresh store, expected application output is:

```text
APPENDED partition=0 offset=0
APPENDED partition=0 offset=1
PROCESSED partition=0 group=0 offset=0 bytes=3 checksum=312
ACK outcome=Applied volatile_next=1 persisted_next=0
PROCESSED partition=0 group=0 offset=1 bytes=3 checksum=314
ACK outcome=Applied volatile_next=2 persisted_next=0
FETCH records=2 encoded_bytes=70 stop=NoEligible; no checkpoint requested
PROGRESS epoch=1 volatile_next=2 persisted_next=0
CHECKPOINT published=true persisted_next=2
RETAIN before=0 floor=0 removed_bytes=0 removed_segments=0 rotated=false
```

The first retention cannot remove the records because group 1 still has persisted next offset 0. Group 0's checkpoint alone is insufficient. The byte sums are $99+97+116=312$ and $100+111+103=314$. Each encoded record is $32+3=35$ bytes; the record data total is 70, not the 134-byte wire body that includes tokens.

Now complete and checkpoint group 1:

```sh
target/debug/brokerctl 127.0.0.1:7001 consume 0 1
target/debug/brokerctl 127.0.0.1:7001 checkpoint 0 1
target/debug/brokerctl 127.0.0.1:7001 retain 0
```

Group 1 processes the same two independent deliveries. Its checkpoint reaches 2. Expected retention result:

```text
RETAIN before=0 floor=2 removed_bytes=102 removed_segments=1 rotated=true
```

The removed segment has a 32-byte segment header and two 35-byte records, so $32+35+35=102$ bytes are removed. Retention first creates the synchronized empty replacement at base offset 2. The partition is not left without an active segment, and offsets are not reused.

Interrupt Terminal A with Control-C. Listener summary counters depend on scheduling and are not fixed expected numbers. Restart using `open`, not `init`:

```sh
target/debug/brokerd open "$PWD/broker-wire-demo" 7000 7001
```

Then in Terminal B:

```sh
target/debug/brokerctl 127.0.0.1:7001 progress 0 0
target/debug/brokerctl 127.0.0.1:7001 progress 0 1
target/debug/brokerctl 127.0.0.1:7000 append 0 owl
```

With no intervening failed startup publication, the progress commands show epoch 2 and both frontiers at 2. The new append returns offset 2. A failed startup can consume an epoch, so the required invariant is a newer epoch with preserved persisted progress, not an unconditional promise that every next launch is exactly one larger.

## Diagnose by layer instead of adding retries

| Observation | Interpretation and useful check |
| --- | --- |
| Address already in use | Another process owns that port. Stop it or select two unused ports; do not create a second broker on the same data directory |
| `Invalid` after using the other port | Append belongs on the producer port; all other operations belong on the consumer/control port |
| `Busy` or `AdmissionDeadline` | Capacity was unavailable before handoff. Inspect held sessions, buffers, or finite command lanes; the client connection closes after the refusal |
| Empty fetch with `ByteLimit` | The next eligible record does not fit the requested encoded-byte allowance. Increase that allowance within the 2 MiB cap; do not treat this as end of data |
| `Ok` envelope but rejected acknowledgement | Inspect the typed outcome. Expiration, stale generation, and epoch mismatch do not advance completion |
| EOF, timeout, or `Uncertain` after an append | Success may already be durable. Reconnecting is necessary, but retrying the same logical payload can duplicate it |
| Client reports it is closed after cancellation | This is the intended stream-safety boundary. Create a new client; never reuse an uncertain byte position |
| Restart repeats processing | Check persisted progress, not only accepted acknowledgements. The explicit checkpoint policy permits this repetition |

Continue to [Part 4](https://25d-228.github.io/rust-learning/chapter-16-part-04.html) for executable fragmentation, slow-peer, cancellation, and loopback checks.

