# Chapter 16 — Framed TCP sessions and clients

## Part 2 — Sessions that retain resource ownership

> **Learning objectives**
> Read an append directly into an admitted buffer, carry a pooled fetch response across socket suspension, distinguish failed admission from an uncertain accepted operation, and cap transport tasks without consuming the consumer listener's capacity with producers.

This is the second part of the chapter's shared **4.5-hour implementation estimate**. Prerequisite: [Part 1](https://25d-228.github.io/rust-learning/chapter-16.html), including every protocol helper, plus the Chapter 15 bounded owner APIs. This part supplies both server modules in full. [Part 3](https://25d-228.github.io/rust-learning/chapter-16-part-03.html) supplies the executable and client; [Part 4](https://25d-228.github.io/rust-learning/chapter-16-part-04.html) supplies the checks.

## A session has two different ownership boundaries

For an append, accepting a socket is not accepting a record. Reading a valid header is not accepting a record either. The session first asks `prepare_append` for an owned queue-slot reservation, a charged append buffer, and a disk reservation. It then reads the body into that buffer. The command becomes the partition owner's responsibility only when `submit` succeeds.

| Point of cancellation | Resource owner being dropped | Record or delivery consequence |
| --- | --- | --- |
| Waiting for append admission | Admission future | Nothing has been submitted; acquired partial reservations are rolled back |
| Reading an admitted append body | Prepared append | Buffer, queue slot, and disk reservation return; no offset was assigned |
| Waiting for an append reply after submit | Reply receiver | The owner may still synchronize the append; no rollback is implied |
| Waiting for a fetch reply after submit | Reply receiver | The owner may install leases; an undelivered batch will eventually be dropped |
| Writing a fetched batch | Batch owner | Buffer capacity returns on drop; installed attempts remain outstanding |

The last two rows are why “drop everything on disconnect” is not a complete protocol description. We drop the resources we own. We do not pretend to reverse another state machine's transition.

Each connection processes one request and writes its entire response before reading another header. There is no per-connection request queue, background response writer, multiplexing map, or unbounded vector of pending operations. TCP can already contain the next request's bytes, but those bytes remain outside our application parser until the current exchange finishes. Kernel socket memory is a separate resource, discussed below.

## Define phase deadlines and the fixed connection loop

Create `src/server.rs`. The `Clients` alias owns one cheap handle to each of the four partition owners through an `Arc`. The handles share the existing resource generation; cloning this array does not create another payload pool.

`SessionLimits` separates five phases. The default header deadline is 10 seconds, admission 1 second, body 5 seconds, owner reply 10 seconds, and response write 5 seconds. All are positive and capped at 60 seconds. These are demonstration policy values, not measurements of an M4 or promises about storage latency. A new deadline begins on entering a phase; one slow byte does not reset that phase's deadline.

```rust title="crates/broker-app/src/server.rs"
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

use broker_core::types::PARTITION_COUNT;
use broker_core::worker::BoundedClient;
use crate::protocol::{self, Endpoint, Header, Status};

mod dispatch;
use dispatch::{CallError, dispatch, send_output};

pub type Clients = Arc<[BoundedClient; PARTITION_COUNT as usize]>;

#[derive(Debug, Clone, Copy)]
pub struct SessionLimits {
    pub header: Duration,
    pub admission: Duration,
    pub body: Duration,
    pub operation: Duration,
    pub write: Duration,
}
impl Default for SessionLimits {
    fn default() -> Self {
        Self { header: Duration::from_secs(10), admission: Duration::from_secs(1),
            body: Duration::from_secs(5), operation: Duration::from_secs(10),
            write: Duration::from_secs(5) }
    }
}
impl SessionLimits {
    pub fn validate(self) -> io::Result<()> {
        if [self.header, self.admission, self.body, self.operation, self.write].iter()
            .any(|value| value.is_zero() || *value > Duration::from_secs(60))
        { return Err(io::Error::new(io::ErrorKind::InvalidInput, "session deadlines must be in (0, 60s]")); }
        Ok(())
    }
}
async fn refuse<W: AsyncWrite + Unpin>(stream: &mut W, request: Header,
    status: Status, limits: SessionLimits) -> io::Result<()>
{
    protocol::write_bytes(stream, &request.response(status, [0; 5], 0).encode(),
        Instant::now() + limits.write).await
}

```

The response to a recognized but invalid request is a small `Invalid` frame. We close immediately afterward instead of draining the declared body. Otherwise an attacker could force the server to discard an arbitrarily long stream before it could regain framing. A malformed common header cannot be trusted even for response correlation, so the header decoder returns an error and the socket simply closes.

The generic bounds `S: AsyncRead + AsyncWrite + Unpin` let one implementation serve a real `TcpStream`, an in-memory duplex stream, and the scripted test stream. `Unpin` lets the extension methods safely move the stream value while polling it. This is not a new executor or transport implementation; the state machine owns when these methods may be used. Tokio provides the actual network readiness machinery.

```rust title="crates/broker-app/src/server.rs"
pub async fn session<S>(mut stream: S, endpoint: Endpoint, clients: Clients, limits: SessionLimits)
    -> io::Result<()>
where S: AsyncRead + AsyncWrite + Unpin {
    limits.validate()?;
    loop {
        let Some(header) = protocol::read_header(&mut stream, Instant::now() + limits.header).await?
            else { return Ok(()); };
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

`scope.expect` cannot introduce a wire-controlled panic here: the preceding `is_err()` branch has returned, and `scope` has not changed. The typed partition index is consequently in `0..4`. Keeping validation before indexing matters more than replacing every `expect` with an unrelated generic error.

On a successful dispatch, `send_output` consumes the output owner. The session cannot accidentally start another request while a response batch is still held by a detached writer task, because there is no such task. A write error exits the function and drops `stream` and its current output. An ordinary clean EOF at the next header boundary returns success.

The rough sum of default phase budgets for an append is 31 seconds, but it is **not a hard shutdown or operating-system I/O deadline**. The runtime must be scheduled to observe timeouts, and a synchronous poll or a blocked partition thread is not preempted by the timer. A client also has its own whole-exchange deadline. It is legitimate for the client to leave before the owner finishes.

## Translate errors without erasing uncertainty

Create `src/server/dispatch.rs`. Its output has only two shapes: a small fixed argument array or an owning pooled batch. There is no arbitrary response byte vector.

`CallError::Transport` means the framing exchange cannot continue. `CallError::Status` means we can attempt one fixed-size error reply, then close. Admission errors occur before command handoff. `Full`, `MemoryFull`, and `WaitersFull` become `Busy`; disk quota refusal stays `DiskFull`; waiting until the admission deadline stays distinct from a timeout after submission.

```rust title="crates/broker-app/src/server/dispatch.rs"
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Instant, timeout_at};

use broker_core::delivery::{LeaseStop, PooledLeaseBatch};
use broker_core::delivery::progress::ProgressAck;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::segment::ReadLimits;
use broker_core::types::{Epoch, Offset};
use broker_core::worker::{AsyncTicket, BoundedClient, RequestError};
use crate::protocol::{self, Header, Op, Status, body_size, encode_ack, encode_token};
use super::SessionLimits;

pub(super) enum Output { Small([u64; 5]), Batch(PooledLeaseBatch) }
pub(super) enum CallError { Status(Status), Transport(io::Error) }
impl From<io::Error> for CallError {
    fn from(error: io::Error) -> Self { Self::Transport(error) }
}
fn admission(error: AdmissionError) -> CallError {
    CallError::Status(match error {
        AdmissionError::Full | AdmissionError::MemoryFull | AdmissionError::WaitersFull => Status::Busy,
        AdmissionError::DiskFull => Status::DiskFull,
        AdmissionError::Closed => Status::Unavailable,
        AdmissionError::InvalidRecord | AdmissionError::TooLarge => Status::Invalid,
        AdmissionError::Deadline => Status::AdmissionDeadline,
    })
}
async fn owner<T>(ticket: AsyncTicket<T>, limits: SessionLimits) -> Result<T, CallError> {
    let deadline = Instant::now() + limits.operation;
    let value = timeout_at(deadline, ticket).await;
    if Instant::now() >= deadline { return Err(CallError::Status(Status::Uncertain)); }
    match value {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(RequestError::Refused(_) | RequestError::Delivery(_)
            | RequestError::Lease(_) | RequestError::DeliveryDisabled))) =>
            Err(CallError::Status(Status::Refused)),
        // Includes a lost channel, panic, accepted I/O failure, and reply timeout.
        _ => Err(CallError::Status(Status::Uncertain)),
    }
}

```

The `owner` helper has three nested result layers. `timeout_at` reports whether our wait expired. The one-shot receiver reports whether a sender disappeared. The contained `Result<T, RequestError>` reports the owner's decision. Only `Ok(Ok(Ok(value)))` is an ordinary success.

A documented healthy refusal, delivery validation error, or lease validation error maps to `Refused`. These core paths validate before mutating the requested state. Accepted I/O failures, lost reply channels, owner failure, and deadline expiry map to `Uncertain`. The server does not promise that such a command failed to take effect. In particular, losing a success response after synchronization is indistinguishable to a client from several other failures.

The explicit clock check after `timeout_at` makes equality with the operation deadline a timeout even when a reply became ready in the same poll. This is conservative: a completed operation may be reported uncertain because the observation was late. A timer cannot retract the command from the owner. Tokio documents the readiness behavior of [`timeout_at`](https://docs.rs/tokio/latest/tokio/time/fn.timeout_at.html); the additional comparison is our policy, not an assumed rollback feature of that function.

If the command itself is read-only, such as a progress query, an uncertain reply does not make that query mutate storage. The common status means “the response is not a confirmed outcome,” with the operation's existing semantics still in force.

## Dispatch without a hidden payload staging allocation

The complete dispatch function follows. The request has already passed wire validation, so conversions to `usize` operate on limits no larger than 2 MiB or 64 records. The native target is 64-bit. `ReadLimits::new` still checks the core API's independent constraints.

```rust title="crates/broker-app/src/server/dispatch.rs"
pub(super) async fn dispatch<S: AsyncRead + Unpin>(stream: &mut S, header: Header,
    client: &BoundedClient, limits: SessionLimits) -> Result<Output, CallError>
{
    let (partition, group) = header.scope()?;
    match header.op {
        Op::Append => {
            let mut prepared = client.prepare_append(header.body as usize,
                Instant::now() + limits.admission).await.map_err(admission)?;
            // The mutable slice belongs to the already charged append pool.
            protocol::read_body(stream, prepared.payload_mut(), Instant::now() + limits.body).await?;
            let offset = owner(prepared.submit().map_err(admission)?, limits).await?;
            Ok(Output::Small([offset.get(), 0, 0, 0, 0]))
        }
        Op::Fetch => {
            let read = ReadLimits::new(header.args[0] as usize, header.args[1] as usize)
                .map_err(|_| CallError::Status(Status::Invalid))?;
            let prepared = client.prepare_lease(group, read,
                Instant::now() + limits.admission).await.map_err(admission)?;
            let batch = owner(prepared.submit().map_err(admission)?, limits).await?;
            Ok(Output::Batch(batch))
        }
        Op::Ack => {
            let token = broker_core::delivery::LeaseToken {
                epoch: Epoch::new(header.args[0]).map_err(|_| CallError::Status(Status::Invalid))?,
                partition, group, offset: Offset::new(header.args[1]),
                generation: header.args[2], deadline_ms: header.args[3],
            };
            let result = owner(client.try_ack_progress(group, token).map_err(admission)?, limits).await?;
            let (ack, progress) = match result {
                ProgressAck::Accepted(value) => (value.ack, value.progress),
                ProgressAck::NotApplied { ack, progress } => (ack, progress),
            };
            Ok(Output::Small([encode_ack(ack.outcome), progress.volatile_next.get(),
                progress.persisted_next.get(), ack.retired as u64, progress.epoch.get()]))
        }
        Op::Checkpoint => {
            let result = owner(client.try_checkpoint(group).map_err(admission)?, limits).await?;
            Ok(Output::Small([result.progress.volatile_next.get(), result.progress.persisted_next.get(),
                u64::from(result.published), 0, result.progress.epoch.get()]))
        }
        Op::Progress => {
            let result = owner(client.try_progress(group).map_err(admission)?, limits).await?;
            Ok(Output::Small([result.volatile_next.get(), result.persisted_next.get(), 0, 0, result.epoch.get()]))
        }
        Op::Retain => {
            let result = owner(client.try_retention().map_err(admission)?, limits).await?;
            Ok(Output::Small([result.before.get(), result.floor.get(), result.removed_bytes,
                result.removed_segments as u64, u64::from(result.rotated)]))
        }
    }
}

```

For append, the important ordering is:

```text
validated length
  -> prepare_append(length, admission_deadline)
  -> read exactly length bytes into prepared.payload_mut()
  -> prepared.submit()
  -> wait for synchronized success or an uncertain result
```

There is no `Vec` created from the untrusted length between the first two steps. A prepared record larger than the configured pool's buffer shape can be refused before a single body byte is read, even if it is below the protocol-wide maximum. The network protocol's limit and a particular resource configuration's capacity are separate checks.

Fetch reserves a response buffer and command slot before handoff. It does not read a payload body at all. The resulting `PooledLeaseBatch` retains that response buffer while the session is suspended on the socket. Its records carry the attempt generations and deadlines installed by the owner, not new deadlines invented by the transport.

Acknowledgement, checkpoint, and retention use the urgent lane and need neither an append buffer nor a response buffer. Progress is an ordinary query. Those choices preserve Chapter 15's separation rather than forwarding every operation through one common bounded queue. A full urgent lane can still return `Busy`; “separate” does not mean infinite.

An acknowledgement's envelope can have status `Ok` while its first argument encodes `Rejected(Expired)` or `Rejected(StaleGeneration)`. `Ok` says the owner evaluated the request and returned a well-formed result. It does not mean the completion advanced. Likewise, the returned volatile frontier is not a persisted frontier. The explicit `Checkpoint` operation carries the publication result.

## Keep the response owner alive through the final write

`send_output` first constructs the small response envelope and validates its own result against the original request. This catches accidental server-side disagreement between dimensions and actual output. A lease body contains the exact core record bytes, including their checksums; the server adds only a fixed 32-byte token before each record.

```rust title="crates/broker-app/src/server/dispatch.rs"
pub(super) async fn send_output<W: AsyncWrite + Unpin>(writer: &mut W, request: Header,
    output: Output, limits: SessionLimits) -> io::Result<()>
{
    let deadline = Instant::now() + limits.write;
    match output {
        Output::Small(args) => {
            let reply = request.response(Status::Ok, args, 0);
            protocol::validate_response(request, reply)?;
            protocol::write_bytes(writer, &reply.encode(), deadline).await
        }
        Output::Batch(batch) => {
            let count = batch.records().len();
            let (stop, required) = match batch.stop {
                LeaseStop::NoEligible => (0, 0), LeaseStop::RecordLimit => (1, 0),
                LeaseStop::ByteLimit { required } => (2, required as u64),
            };
            let size = body_size(count, batch.encoded_bytes)?;
            let reply = request.response(Status::Ok, [count as u64, batch.encoded_bytes as u64,
                stop, required, batch.state.epoch.get()], size as u32);
            protocol::validate_response(request, reply)?;
            protocol::write_bytes(writer, &reply.encode(), deadline).await?;
            for record in batch.records() {
                let token = encode_token(record.token);
                protocol::write_bytes(writer, &token, deadline).await?;
                protocol::write_bytes(writer, record.encoded, deadline).await?;
            }
            // `batch` stays alive across every await and returns its buffer on every exit.
            Ok(())
        }
    }
}
```

The batch is moved into this future. Each `record.encoded` slice borrows that batch while a `write_all` operation may suspend. Rust prevents destroying or mutating its owner during those borrows. The small token array also lives across its write. The loop deliberately uses one absolute `deadline` for the envelope, every token, and every record; otherwise a peer could obtain a fresh allowance by accepting only one small piece at a time.

For the `cat`, `dog` example, the calls offer 64 bytes, 32 bytes, 35 bytes, 32 bytes, and 35 bytes. Those are **write calls, not packet promises**. The operating system can combine or split them. No 134-byte body vector or 198-byte whole-frame vector is assembled inside the broker.

Suppose the peer reads the 64-byte header and one token, then stops. The pooled batch remains owned throughout the blocked record write. On deadline expiry, `?` exits, dropping the batch and then the socket. Its buffer becomes reusable, but the lease generation remains installed. A later fetch must wait for expiry and issue a new attempt under the existing lease policy. Returning memory is not cancelling delivery.

## Bound task creation before accepting another application connection

Finish `server.rs` with listener accounting and the accept loop. Each listener has an independent `JoinSet`. It accepts only while `tasks.len() < capacity`. A task whose result has not yet been collected still occupies this bound, so completed-but-unreaped tasks cannot accumulate outside the accounting.

The default executable uses capacity 16 for producers and 16 for consumers. The public helper permits 1 through 64. This makes the maximum number of tracked session tasks explicit, unlike an unconditional `spawn` for every accepted socket.

```rust title="crates/broker-app/src/server.rs"
#[derive(Debug, Default)]
pub struct ListenerStats {
    pub accepted: u64,
    pub completed: u64,
    pub session_errors: u64,
    pub cancelled: u64,
}
fn record_join(result: Result<io::Result<()>, tokio::task::JoinError>, stats: &mut ListenerStats)
    -> io::Result<()>
{
    match result {
        Ok(Ok(())) => stats.completed = stats.completed.saturating_add(1),
        Ok(Err(_)) => stats.session_errors = stats.session_errors.saturating_add(1),
        Err(error) if error.is_cancelled() => stats.cancelled = stats.cancelled.saturating_add(1),
        Err(error) => return Err(io::Error::other(format!("session task panicked: {error}"))),
    }
    Ok(())
}

pub async fn serve_listener(listener: TcpListener, endpoint: Endpoint, clients: Clients,
    limits: SessionLimits, capacity: usize, mut stop: watch::Receiver<bool>)
    -> io::Result<ListenerStats>
{
    limits.validate()?;
    if !(1..=64).contains(&capacity) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "listener capacity must be in 1..=64"));
    }
    let mut tasks = JoinSet::new();
    let mut stats = ListenerStats::default();
    let mut result = loop {
        if *stop.borrow() { break Ok(()); }
        tokio::select! {
            _ = stop.changed() => break Ok(()),
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = record_join(joined.expect("nonempty task set"), &mut stats) {
                    break Err(error);
                }
            }
            accepted = listener.accept(), if tasks.len() < capacity => {
                match accepted {
                    Ok((stream, _)) => {
                        stats.accepted = stats.accepted.saturating_add(1);
                        let clients = Arc::clone(&clients);
                        tasks.spawn(session(stream, endpoint, clients, limits));
                    }
                    Err(error) => break Err(error),
                }
            }
        }
    };
    drop(listener);
    // Chapter 16 stops transport immediately; it does not call this graceful draining.
    tasks.abort_all();
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = record_join(joined, &mut stats) {
            if result.is_ok() { result = Err(error); }
        }
    }
    result?;
    Ok(stats)
}
```

When the set is full, the `accept` branch is disabled; the listener waits for a session result or stop signal. When it is empty, the `join_next` branch is disabled; repeatedly polling an immediately empty set would otherwise be a busy loop. `select!` chooses among enabled ready branches. Correctness does not depend on a particular random choice between them. The same task executes these branch futures; this is not parallel execution within one listener. See Tokio's [`select!` documentation](https://docs.rs/tokio/latest/tokio/macro.select.html).

Cancelling an `accept` future when another branch wins does not consume a connection according to [`TcpListener::accept`'s cancellation contract](https://docs.rs/tokio/latest/tokio/net/struct.TcpListener.html#method.accept). A full application listener does **not** mean the OS cannot complete a TCP handshake into its backlog. The cap describes accepted session tasks owned by this program, not every kernel-side socket state or connecting client.

The stop path drops the listening socket, calls `abort_all`, then collects every task result. Calling `abort_all` alone is not joining. The [`JoinSet` documentation](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html#method.abort_all) explicitly requires further joining to observe completion. This loop counts ordinary session errors, counts cancellation, and preserves a panic as a listener error rather than silently treating it as a successful disconnect. Counters saturate instead of overflowing after long service.

At the default capacities there are at most 32 tracked session tasks and two listener tasks. At most 32 fixed input header arrays contribute $32\cdot64=2048$ bytes of parser-header storage; this is **not** a total future-size bound, because each future also owns control state, handles, and possibly other fixed headers or tokens. Append/response payloads remain in the existing 64 MiB resource generation. Socket send/receive buffers, the OS backlog, Tokio allocations, allocator overhead, storage indexes, and thread stacks are outside that payload number. Later measurements must report process memory and these caveats rather than relabel the pool limit as RSS.

## What this separation guarantees, and what it does not

A producer cannot consume the consumer listener's 16 accepted-task positions. Once a consumer/control request reaches the owner API, append/query saturation cannot consume its urgent queue reservation or force it to allocate a payload buffer. These are useful structural guarantees.

A peer can nevertheless occupy a consumer slot with a slow fetch response. Sixteen such peers can occupy all consumer slots until timeouts or disconnects. Existing admitted sockets and bounded deadlines do not provide authentication, per-user fairness, priority scheduling at the network edge, or denial-of-service protection. A blocked filesystem operation can also delay urgent owner work. Do not infer unconditional acknowledgement liveness from the two-listener arrangement.

Finally, the stop path here is an **immediate transport stop**, not graceful application draining. It can cancel a response after its command was accepted. Partition owners are closed and joined by the executable after all transport tasks finish; explicit checkpointing is still the consumer's operation. Monitoring owner failure, coordinating cancellation phases, and documenting a supervised shutdown contract belong to Chapter 17, not to an unimplemented claim in this chapter.

Continue to [Part 3](https://25d-228.github.io/rust-learning/chapter-16-part-03.html) to build the client and run the two-port loopback demonstration.

