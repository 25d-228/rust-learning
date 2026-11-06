# Chapter 16 — Framed TCP sessions and clients

## Part 4 — Fragmentation, slow peers, and ambiguity checks

> **Learning objectives**
> Test a byte stream without assuming packet boundaries, observe cancellation at a known ownership point, hold synchronization pending without arbitrary sleeps, and distinguish supplied executable checks from results actually observed.

This is the final part of Chapter 16's shared **4.5-hour estimate**, including **1.5 hours for these checks and practical diagnosis**. Prerequisites: all implementation from [Part 1](https://25d-228.github.io/rust-learning/chapter-16.html), [Part 2](https://25d-228.github.io/rust-learning/chapter-16-part-02.html), and [Part 3](https://25d-228.github.io/rust-learning/chapter-16-part-03.html). No later chapter is required.

The wire integration target contains **33 tests**: 12 format/I/O checks, 12 session checks, and 9 real-loopback/client checks. Combined with the prerequisite suites, that is **379 supplied tests**, not an assertion that they passed here. All five test files follow in full. Tests use a temporary store and the real core owners, not a replacement broker implemented inside the test harness.

## Register the target and give every fixture a clear owner

Create `tests/wire.rs`. The path attributes point at child files rather than creating four separately auto-discovered test targets.

```rust title="crates/broker-app/tests/wire.rs"
#[path = "wire/support.rs"] mod support;
#[path = "wire/format.rs"] mod format;
#[path = "wire/sessions.rs"] mod sessions;
#[path = "wire/clients.rs"] mod clients;
```

Create `tests/wire/support.rs`. The small constructors make scopes readable; `reply` puts a watchdog around owner replies, while `append` fills and submits the same prepared buffer used in the service. The watchdog is a test failure bound, not a claim that an accepted owner operation was cancelled.

```rust title="crates/broker-app/tests/wire/support.rs"
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, AtomicUsize, Ordering}, mpsc};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, oneshot};

use broker_app::protocol::{Header, Op};
use broker_app::server::Clients;
use broker_core::budget::MemoryConfig;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::storage::recovery;
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{GroupId, Offset, PartitionId};
use broker_core::worker::{AsyncTicket, BatchConfig, BoundedClient, BoundedHandle};

pub fn p(value: u16) -> PartitionId { PartitionId::new(value).unwrap() }
pub fn g(value: u8) -> GroupId { GroupId::new(value).unwrap() }
pub fn request(op: Op) -> Header { Header::request(op, p(0), g(0)) }
pub fn append_frame(bytes: &[u8]) -> Vec<u8> {
    let mut h = request(Op::Append); h.body = bytes.len() as u32;
    let mut frame = h.encode().to_vec(); frame.extend_from_slice(bytes); frame
}
pub async fn reply<T>(ticket: AsyncTicket<T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), ticket).await.expect("owner watchdog")
        .expect("reply channel").expect("owner result")
}
pub async fn append(client: &BoundedClient, bytes: &[u8]) -> Offset {
    let mut prepared = client.try_prepare_append(bytes.len()).unwrap();
    prepared.payload_mut().copy_from_slice(bytes);
    reply(prepared.submit().unwrap()).await
}

```

Each fixture gets a distinct temporary directory from process ID plus an atomic counter. `Ordering::Relaxed` is sufficient for uniqueness: the counter does not publish another object whose visibility depends on it. The fixture owns the four worker handles and closes and joins all of them before attempting to remove its directory.

Two 1024-byte append buffers and two 4096-byte response buffers per partition keep saturation tests small. The storage format still supports larger records, but the configured service pool refuses an append that cannot fit its block. A shared manual lease clock makes expiry deterministic without waiting ten real seconds. It does not control socket timeouts or file synchronization.

```rust title="crates/broker-app/tests/wire/support.rs"
static NEXT: AtomicU64 = AtomicU64::new(0);
pub struct Fixture {
    pub clients: Clients,
    pub clock: Arc<ManualClock>,
    owners: Vec<BoundedHandle>,
    root: PathBuf,
}
impl Fixture {
    pub fn new() -> Self { Self::with_sync(FileSync) }
    pub fn with_sync<S: SyncIo + Send + 'static>(first_sync: S) -> Self {
        let root = std::env::temp_dir().join(format!("broker-wire-{}-{}", std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)));
        let (epoch, logs) = recovery::initialize(&root, StorageConfig::new(4 * 1024 * 1024, 2, 4).unwrap())
            .unwrap().into_parts();
        let clock = Arc::new(ManualClock::new(0));
        let mut owners = Vec::new();
        let mut first_sync = Some(first_sync);
        for (index, log) in logs.into_iter().enumerate() {
            let batch = BatchConfig::new(1, 4096, Duration::from_millis(10)).unwrap();
            let leases = LeaseConfig::new(epoch, 64, 10_000).unwrap();
            let memory = MemoryConfig::new(1024, 2, 4096, 2, 2).unwrap();
            let owner = if index == 0 {
                BoundedHandle::start(log, first_sync.take().unwrap(), batch, [2, 2, 4], leases,
                    clock.clone(), CheckpointPolicy::explicit_only(), memory)
            } else {
                BoundedHandle::start(log, FileSync, batch, [2, 2, 4], leases,
                    clock.clone(), CheckpointPolicy::explicit_only(), memory)
            }.unwrap();
            owners.push(owner);
        }
        let clients = Arc::new(std::array::from_fn(|i| owners[i].client()));
        Self { clients, clock, owners, root }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for owner in &self.owners { owner.close(); }
        for owner in self.owners.drain(..) { let _ = owner.join(); }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

```

The synchronization gate signals exactly when the first partition's first record synchronization has been entered, then waits on a synchronous channel. This wait runs on the partition thread, not the Tokio thread. Releasing the gate resumes the real `FileSync` implementation.

`GateRelease` sends on ordinary drop as well as an explicit release. In tests, move the release guard into a binding declared **after** the fixture, so unwinding drops it before the fixture tries to join the blocked owner. Otherwise a failed assertion could deadlock test cleanup. This is an ownership-order requirement, not cosmetic test scaffolding.

```rust title="crates/broker-app/tests/wire/support.rs"
pub struct GateSync { entered: Option<oneshot::Sender<()>>, release: mpsc::Receiver<()> }
pub struct GateRelease(Option<mpsc::Sender<()>>);
impl GateRelease {
    pub fn release(&mut self) { if let Some(sender) = self.0.take() { let _ = sender.send(()); } }
}
impl Drop for GateRelease { fn drop(&mut self) { self.release(); } }
pub fn sync_gate() -> (GateSync, oneshot::Receiver<()>, GateRelease) {
    let (sender, entered) = oneshot::channel();
    let (release, receiver) = mpsc::channel();
    (GateSync { entered: Some(sender), release: receiver }, entered, GateRelease(Some(release)))
}
impl SyncIo for GateSync {
    fn sync(&mut self, base: Offset, file: &mut dyn broker_core::io::StorageFile) -> io::Result<()> {
        if let Some(sender) = self.entered.take() {
            let _ = sender.send(());
            self.release.recv().map_err(|_| io::Error::other("sync gate closed"))?;
        }
        FileSync.sync(base, file)
    }
}

```

`ScriptIo` is the framing probe. Its `cut` forces a read to stop at a chosen byte boundary even when the reader has more capacity. Writes can return only one byte, fail immediately, or become permanently pending after a chosen byte count. Captured output and read counts let tests prove whether a rejected body was consumed.

The pending writer deliberately has no later readiness transition; cancellation or a timer must end the wait. Its `Notify` signals that this exact blocked state has been reached. It is not a production stream adapter. `ReadBuf::put_slice` both copies bytes and advances the initialized/filled region; returning success with zero bytes at input exhaustion represents EOF for these tests.

```rust title="crates/broker-app/tests/wire/support.rs"
#[derive(Clone)]
pub struct Capture {
    pub output: Arc<Mutex<Vec<u8>>>,
    pub read_bytes: Arc<AtomicUsize>,
    pub blocked: Arc<Notify>,
}
pub struct ScriptIo {
    input: Vec<u8>,
    position: usize,
    cut: usize,
    max_write: usize,
    fail_write: bool,
    block_after: Option<usize>,
    written: usize,
    capture: Capture,
}
impl ScriptIo {
    pub fn new(input: Vec<u8>, cut: usize) -> (Self, Capture) {
        let capture = Capture { output: Arc::new(Mutex::new(Vec::new())),
            read_bytes: Arc::new(AtomicUsize::new(0)), blocked: Arc::new(Notify::new()) };
        (Self { input, position: 0, cut, max_write: usize::MAX, fail_write: false,
            block_after: None, written: 0, capture: capture.clone() }, capture)
    }
    pub fn short_writes(mut self) -> Self { self.max_write = 1; self }
    pub fn fail_writes(mut self) -> Self { self.fail_write = true; self }
    pub fn block_writes_after(mut self, bytes: usize) -> Self { self.block_after = Some(bytes); self }
}
impl AsyncRead for ScriptIo {
    fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, buffer: &mut ReadBuf<'_>)
        -> Poll<io::Result<()>>
    {
        let end = if self.position < self.cut { self.cut.min(self.input.len()) } else { self.input.len() };
        let count = (end - self.position).min(buffer.remaining());
        buffer.put_slice(&self.input[self.position..self.position + count]);
        self.position += count;
        self.capture.read_bytes.fetch_add(count, Ordering::Relaxed);
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for ScriptIo {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        if self.fail_write { return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "peer left"))); }
        let remaining = self.block_after.map_or(usize::MAX, |end| end.saturating_sub(self.written));
        if remaining == 0 {
            self.capture.blocked.notify_one();
            return Poll::Pending;
        }
        let count = bytes.len().min(self.max_write).min(remaining);
        self.capture.output.lock().unwrap().extend_from_slice(&bytes[..count]);
        self.written += count;
        Poll::Ready(Ok(count))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
}
pub fn captured_header(capture: &Capture) -> Header {
    let output = capture.output.lock().unwrap();
    Header::decode(output[..64].try_into().unwrap()).unwrap()
}
```

A two-call TCP writer would not force two reads: the network may coalesce them. The scripted read is therefore the correct place for exhaustive split tests; real loopback tests below establish integration separately. The transport abstraction is useful precisely because it allows these two kinds of evidence to remain distinct.

## Format tests: independent bytes and bounded dimensions

Create `tests/wire/format.rs`. The first header's expected 24-byte prefix is written independently rather than generated by the encoder under test. It fixes magic, version, operation, partition, length, and big-endian request identifier. The remaining forty bytes must be zero. A round-trip alone would not detect an encoder and decoder sharing the same incorrect field offset.

```rust title="crates/broker-app/tests/wire/format.rs"
use std::io;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use broker_app::protocol::*;
use broker_core::delivery::{AckOutcome, AckRejection};
use super::support::*;

#[test]
fn append_header_has_independent_golden_bytes() {
    let mut h = request(Op::Append); h.body = 3; h.partition = 2;
    h.id = 0x0102_0304_0506_0708;
    let mut golden = [0; 64];
    golden[..24].copy_from_slice(&[0x50,0x42,0x4e,0x57, 1,1,0,0, 0,2,0,0,
        0,0,0,3, 1,2,3,4,5,6,7,8]);
    assert_eq!(h.encode(), golden);
    assert_eq!(Header::decode(&golden).unwrap(), h);
}
#[test]
fn each_request_shape_round_trips() {
    for op in [Op::Append, Op::Fetch, Op::Ack, Op::Checkpoint, Op::Progress, Op::Retain] {
        let mut h = request(op);
        if op == Op::Fetch { h.args = [64, MAX_ENCODED as u64, 0, 0, 0]; }
        if op == Op::Ack { h.args = [1, 0, 1, 10_000, 0]; }
        validate_request(h).unwrap();
        assert_eq!(Header::decode(&h.encode()).unwrap(), h);
    }
}
```

The next checks mutate common reserved fields and exercise request bounds. Distinguish common decoding from operation validation: a syntactically decoded header can still have the wrong operation arguments or a forbidden body.

```rust title="crates/broker-app/tests/wire/format.rs"
#[test]
fn common_header_rejects_unknown_and_reserved_fields() {
    for (index, value) in [(0, 0), (4, 2), (5, 99), (6, 99), (7, 1), (11, 1)] {
        let mut raw = request(Op::Progress).encode(); raw[index] = value;
        assert!(Header::decode(&raw).is_err());
    }
    let mut raw = request(Op::Progress).encode(); raw[16..24].fill(0);
    assert!(Header::decode(&raw).is_err());
}
#[test]
fn request_limits_are_checked_before_body_work() {
    let mut h = request(Op::Append); h.body = 1_048_577;
    assert!(validate_request(h).is_err());
    h.body = 0; h.partition = 4; assert!(validate_request(h).is_err());
    h.partition = 0; h.group = 2; assert!(validate_request(h).is_err());
    h.group = 0; h.args[0] = 1; assert!(validate_request(h).is_err());
    for (count, bytes) in [(0, 64), (65, 64), (1, 0), (1, MAX_ENCODED as u64 + 1)] {
        let mut h = request(Op::Fetch); h.args = [count, bytes, 0, 0, 0];
        assert!(validate_request(h).is_err());
    }
    for op in [Op::Fetch, Op::Ack, Op::Checkpoint, Op::Progress, Op::Retain] {
        let mut h = request(op); h.body = 1; assert!(validate_request(h).is_err());
    }
}
```

Response correlation checks all of request ID, operation, partition, and group. Fetch validation also couples body size, record count, encoded bytes, and stop reason. For a request capped at two records and 70 encoded bytes, two 35-byte records must report `RecordLimit`, not `NoEligible`; zero returned records can report `ByteLimit` only if the next required record exceeds the still-available bytes.

```rust title="crates/broker-app/tests/wire/format.rs"
#[test]
fn response_must_match_scope_identifier_and_status() {
    let h = request(Op::Progress);
    let valid = h.response(Status::Ok, [2, 1, 0, 0, 1], 0);
    validate_response(h, valid).unwrap();
    for reply in [Header { id: 2, ..valid }, Header { partition: 1, ..valid },
        Header { group: 1, ..valid }, Header { op: Op::Ack, ..valid },
        Header { status: Status::Request, ..valid }, Header { body: 1, ..valid }]
    { assert!(validate_response(h, reply).is_err()); }
    assert!(validate_response(h, h.response(Status::Busy, [1, 0, 0, 0, 0], 0)).is_err());
}
#[test]
fn fetch_response_dimensions_and_stop_reason_are_exact() {
    let mut h = request(Op::Fetch); h.args = [2, 70, 0, 0, 0];
    validate_response(h, h.response(Status::Ok, [2, 70, 1, 0, 1], 134)).unwrap();
    validate_response(h, h.response(Status::Ok, [1, 35, 0, 0, 1], 67)).unwrap();
    validate_response(h, h.response(Status::Ok, [0, 0, 2, 71, 1], 0)).unwrap();
    for args in [[3, 105, 1, 0, 1], [2, 70, 0, 0, 1], [0, 0, 2, 70, 1],
        [1, 35, 0, 0, 0], [0, 1, 0, 0, 1]]
    { assert!(validate_response(h, h.response(Status::Ok, args, 0)).is_err()); }
}
```

The explicit body formula includes tokens, excludes the envelope, and rejects impossible dimensions such as two records in only 63 encoded bytes. Outcome-code coverage ensures a typed rejection survives the wire round trip rather than collapsing into a generic transport failure.

```rust title="crates/broker-app/tests/wire/format.rs"
#[test]
fn wire_body_formula_includes_tokens_but_excludes_envelope() {
    assert_eq!(body_size(0, 0).unwrap(), 0);
    assert_eq!(body_size(2, 70).unwrap(), 134);
    assert_eq!(body_size(64, MAX_ENCODED).unwrap(), MAX_WIRE_BODY);
    assert!(body_size(0, 1).is_err());
    assert!(body_size(2, 63).is_err());
    assert!(body_size(65, 2080).is_err());
}
#[test]
fn acknowledgement_codes_preserve_every_core_outcome() {
    let mut values = vec![AckOutcome::Applied, AckOutcome::Duplicate, AckOutcome::AlreadyRetired];
    values.extend([AckRejection::WrongEpoch, AckRejection::WrongPartition, AckRejection::WrongGroup,
        AckRejection::NotIssued, AckRejection::StaleGeneration, AckRejection::WrongDeadline,
        AckRejection::Expired].into_iter().map(AckOutcome::Rejected));
    for value in values { assert_eq!(decode_ack(encode_ack(value)).unwrap(), value); }
    assert!(decode_ack(9).is_err());
}
```

For a 64-byte header there are 63 interior split points. EOF at length zero is a clean boundary; EOF after 1 through 63 bytes is truncated input. Both cases are exercised explicitly.

```rust title="crates/broker-app/tests/wire/format.rs"
#[tokio::test]
async fn fixed_header_handles_every_split_and_clean_or_partial_eof() {
    let h = request(Op::Progress);
    for cut in 1..64 {
        let (mut stream, _) = ScriptIo::new(h.encode().to_vec(), cut);
        assert_eq!(read_header(&mut stream, Instant::now() + Duration::from_secs(2)).await.unwrap(), Some(h));
    }
    for len in 0..64 {
        let (mut stream, _) = ScriptIo::new(h.encode()[..len].to_vec(), len);
        let result = read_header(&mut stream, Instant::now() + Duration::from_secs(2)).await;
        if len == 0 { assert!(result.unwrap().is_none()); }
        else { assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof); }
    }
}
```

The remaining I/O tests use Tokio's paused test clock. A feeder offers one byte every 40 milliseconds while the entire header has a 100-millisecond deadline. The parser must not extend that deadline after each successful read. An already-expired deadline rejects even immediately ready I/O. A writer accepting only `abc` from `abcdef` must time out with that prefix visible, not claim an all-or-nothing write.

Paused Tokio time requires the current-thread runtime and the `test-util` feature; the development manifest enables it. Tokio may automatically advance to the next timer when no work is ready. These tests use only transport futures, not a real storage thread whose progress depends on real scheduling. See [`pause`](https://docs.rs/tokio/latest/tokio/time/fn.pause.html) and [`advance`](https://docs.rs/tokio/latest/tokio/time/fn.advance.html).

```rust title="crates/broker-app/tests/wire/format.rs"
#[tokio::test(start_paused = true)]
async fn header_deadline_is_not_reset_after_each_byte() {
    let (mut writer, mut reader) = tokio::io::duplex(1);
    let feeder = tokio::spawn(async move {
        for byte in request(Op::Progress).encode() {
            if writer.write_all(&[byte]).await.is_err() { break; }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    });
    let error = read_header(&mut reader, Instant::now() + Duration::from_millis(100)).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    feeder.abort(); let _ = feeder.await;
}
#[tokio::test(start_paused = true)]
async fn ready_io_does_not_bypass_an_expired_deadline() {
    let (mut stream, _) = ScriptIo::new(request(Op::Progress).encode().to_vec(), 64);
    assert_eq!(read_header(&mut stream, Instant::now()).await.unwrap_err().kind(), io::ErrorKind::TimedOut);
}
#[tokio::test(start_paused = true)]
async fn blocked_output_expires_under_one_absolute_budget() {
    let (stream, capture) = ScriptIo::new(Vec::new(), 0);
    let mut stream = stream.block_writes_after(3);
    let error = write_bytes(&mut stream, b"abcdef", Instant::now() + Duration::from_millis(100))
        .await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(capture.output.lock().unwrap().as_slice(), b"abc");
}
```

## Session tests: split frames, admission, and the handoff boundary

Create `tests/wire/sessions.rs`. The complete `cat` append is 67 bytes: a 64-byte envelope and a three-byte body. Splitting at every interior byte gives 66 cases, including both boundaries inside the body. One fixture accepts all 66 sequential records, so assigned offsets also reveal accidental duplicate dispatch or skipped requests. Each response is forced through one-byte writes.

The coalesced test puts two requests into a single input buffer and gives them different identifiers. Two complete 64-byte responses must emerge in request order with offsets 0 and 1. It does not depend on how many read syscalls would occur on a real socket.

```rust title="crates/broker-app/tests/wire/sessions.rs"
use std::io;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use broker_app::protocol::{Endpoint, Header, Op, Status};
use broker_app::server::{SessionLimits, session};
use broker_core::storage::segment::ReadLimits;
use super::support::*;

#[tokio::test]
async fn append_survives_every_header_and_body_split() {
    let fixture = Fixture::new();
    let frame = append_frame(b"cat");
    for cut in 1..frame.len() {
        let (stream, capture) = ScriptIo::new(frame.clone(), cut);
        session(stream.short_writes(), Endpoint::Producer, fixture.clients.clone(), SessionLimits::default()).await.unwrap();
        let h = captured_header(&capture);
        assert_eq!(h.status, Status::Ok);
        assert_eq!(h.args[0], (cut - 1) as u64);
        assert_eq!(capture.read_bytes.load(Ordering::Relaxed), frame.len());
    }
}
#[tokio::test]
async fn coalesced_requests_receive_separate_ordered_responses() {
    let fixture = Fixture::new();
    let mut frames = append_frame(b"cat");
    let mut second = append_frame(b"dog"); second[16..24].copy_from_slice(&2u64.to_be_bytes());
    frames.extend_from_slice(&second);
    let (stream, capture) = ScriptIo::new(frames, usize::MAX);
    session(stream, Endpoint::Producer, fixture.clients.clone(), SessionLimits::default()).await.unwrap();
    let raw = capture.output.lock().unwrap(); assert_eq!(raw.len(), 128);
    let a = Header::decode(raw[..64].try_into().unwrap()).unwrap();
    let b = Header::decode(raw[64..].try_into().unwrap()).unwrap();
    assert_eq!((a.id, a.args[0], b.id, b.args[0]), (1, 0, 2, 1));
}
```

Read counting is the important assertion in the refusal checks: it must remain exactly 64 bytes even if body bytes are already available. The oversized case also checks that no append memory or disk reservation remains and that no offset was assigned. Listener role checks reject append on the consumer endpoint and checkpoint on the producer endpoint.

```rust title="crates/broker-app/tests/wire/sessions.rs"
#[tokio::test]
async fn oversized_header_is_rejected_without_reading_its_body() {
    let fixture = Fixture::new();
    let mut h = request(Op::Append); h.body = 1_048_577;
    let mut input = h.encode().to_vec(); input.extend_from_slice(b"not read");
    let (stream, capture) = ScriptIo::new(input, usize::MAX);
    session(stream, Endpoint::Producer, fixture.clients.clone(), SessionLimits::default()).await.unwrap();
    assert_eq!(captured_header(&capture).status, Status::Invalid);
    assert_eq!(capture.read_bytes.load(Ordering::Relaxed), 64);
    assert_eq!(fixture.clients[0].memory_state().append.checked_out_bytes, 0);
    assert_eq!(fixture.clients[0].disk_state().reserved_bytes, 0);
    assert_eq!(reply(fixture.clients[0].try_snapshot().unwrap()).await.assigned_end.get(), 0);
}
#[tokio::test]
async fn producer_and_consumer_roles_are_enforced() {
    let fixture = Fixture::new();
    for (endpoint, bytes) in [(Endpoint::Consumer, append_frame(b"cat")),
        (Endpoint::Producer, request(Op::Checkpoint).encode().to_vec())]
    {
        let (stream, capture) = ScriptIo::new(bytes, usize::MAX);
        session(stream, endpoint, fixture.clients.clone(), SessionLimits::default()).await.unwrap();
        assert_eq!(captured_header(&capture).status, Status::Invalid);
        assert_eq!(capture.read_bytes.load(Ordering::Relaxed), 64);
    }
}
```

An admitted three-byte body can end after zero, one, or two bytes. Each truncated case must return the prepared resources without consuming offset zero. The next valid append proves that the partition still starts there. The slow-body case keeps the peer open, so a body deadline—not EOF or a guessed test sleep—ends the wait.

```rust title="crates/broker-app/tests/wire/sessions.rs"
#[tokio::test]
async fn every_partial_body_eof_returns_prepared_resources() {
    let fixture = Fixture::new();
    let frame = append_frame(b"cat");
    for end in 64..67 {
        let (stream, capture) = ScriptIo::new(frame[..end].to_vec(), usize::MAX);
        let error = session(stream, Endpoint::Producer, fixture.clients.clone(), SessionLimits::default()).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(capture.output.lock().unwrap().is_empty());
        assert_eq!(fixture.clients[0].memory_state().append.checked_out_bytes, 0);
        assert_eq!(fixture.clients[0].disk_state().reserved_bytes, 0);
    }
    assert_eq!(append(&fixture.clients[0], b"next").await.get(), 0);
}
#[tokio::test]
async fn slow_partial_body_expires_without_submitting() {
    let fixture = Fixture::new();
    let (mut peer, stream) = tokio::io::duplex(128);
    let limits = SessionLimits { body: Duration::from_millis(30), ..SessionLimits::default() };
    let task = tokio::spawn(session(stream, Endpoint::Producer, fixture.clients.clone(), limits));
    peer.write_all(&append_frame(b"cat")[..65]).await.unwrap();
    // Keep the peer open. The deadline, not EOF or a test sleep, ends the body wait.
    let error = tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(fixture.clients[0].memory_state().append.checked_out_bytes, 0);
    assert_eq!(fixture.clients[0].disk_state().reserved_bytes, 0);
    assert_eq!(append(&fixture.clients[0], b"next").await.get(), 0);
}
```

A broken success write happens **after** the owner reply was received. The record must still be visible and readable. The sync-gated test exercises the other ordering: the command is already submitted, synchronization is blocked, and the reply deadline expires. The session writes `Uncertain` and closes. Opening the gate then lets synchronization finish and visible next offset reach 1.

```text
session                     partition owner                client observation
submit cat                  write cat
wait for reply              enter gated sync
operation deadline fires    still blocked                  Uncertain, then close
session ends                release -> synchronize
                            advance visibility to 1        no later success reply
```

Dropping the reply receiver is not a message that removes the pending append. No compensating delete is attempted.

```rust title="crates/broker-app/tests/wire/sessions.rs"
#[tokio::test]
async fn broken_append_reply_does_not_rollback_synchronized_data() {
    let fixture = Fixture::new();
    let (stream, _) = ScriptIo::new(append_frame(b"cat"), usize::MAX);
    assert!(session(stream.fail_writes(), Endpoint::Producer, fixture.clients.clone(), SessionLimits::default()).await.is_err());
    assert_eq!(reply(fixture.clients[0].try_snapshot().unwrap()).await.visible_end.get(), 1);
    let batch = reply(fixture.clients[0].try_prepare_lease(g(0), ReadLimits::new(1, 64).unwrap())
        .unwrap().submit().unwrap()).await;
    assert_eq!(batch.records().next().unwrap().payload(), b"cat");
}
#[tokio::test]
async fn timed_out_owner_reply_is_uncertain_and_may_finish_later() {
    let (sync, entered, release) = sync_gate();
    let fixture = Fixture::with_sync(sync);
    let mut release = release; // Drops before the fixture even if an assertion unwinds.
    let (stream, capture) = ScriptIo::new(append_frame(b"cat"), usize::MAX);
    let limits = SessionLimits { operation: Duration::from_millis(30), ..SessionLimits::default() };
    let task = tokio::spawn(session(stream, Endpoint::Producer, fixture.clients.clone(), limits));
    tokio::time::timeout(Duration::from_secs(5), entered).await.unwrap().unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(captured_header(&capture).status, Status::Uncertain);
    release.release();
    assert_eq!(reply(fixture.clients[0].try_snapshot().unwrap()).await.visible_end.get(), 1);
}
```

The next pair waits for the scripted writer to report that it has sent the envelope and blocked on the body. Only then is a response-buffer ownership assertion meaningful. The timeout test pauses Tokio time **after** storage has returned the batch, advances to the write deadline, and awaits the session task to observe destructor completion. `advance` alone is not proof that every woken task has run. It resumes time before asking the real storage thread for another reply.

The manual lease clock has not advanced in that timeout case, so the record remains leased even though its response bytes have returned. In the cancellation case, abort and join the session, then explicitly advance the lease clock to expiry. The next issued token has generation 2. These are two separate clocks and two separate state transitions.

```rust title="crates/broker-app/tests/wire/sessions.rs"
#[tokio::test]
async fn slow_output_keeps_pool_ownership_until_deadline() {
    let fixture = Fixture::new(); append(&fixture.clients[0], b"cat").await;
    let mut h = request(Op::Fetch); h.args = [1, 64, 0, 0, 0];
    let (stream, capture) = ScriptIo::new(h.encode().to_vec(), usize::MAX);
    let limits = SessionLimits { write: Duration::from_secs(60), ..SessionLimits::default() };
    let task = tokio::spawn(session(stream.block_writes_after(64), Endpoint::Consumer, fixture.clients.clone(), limits));
    tokio::time::timeout(Duration::from_secs(5), capture.blocked.notified()).await.unwrap();
    tokio::time::pause();
    assert_eq!(fixture.clients[0].memory_state().response.free_buffers, 1);
    tokio::time::advance(Duration::from_secs(60)).await;
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    tokio::time::resume();
    assert_eq!(fixture.clients[0].memory_state().response.checked_out_bytes, 0);
    let empty = reply(fixture.clients[0].try_prepare_lease(g(0), ReadLimits::new(1, 64).unwrap())
        .unwrap().submit().unwrap()).await;
    assert_eq!(empty.records().len(), 0); // Bytes returned; already installed lease remains.
}
#[tokio::test]
async fn cancelling_a_fetch_writer_returns_bytes_but_not_its_attempt() {
    let fixture = Fixture::new(); append(&fixture.clients[0], b"cat").await;
    let mut h = request(Op::Fetch); h.args = [1, 64, 0, 0, 0];
    let (stream, capture) = ScriptIo::new(h.encode().to_vec(), usize::MAX);
    let task = tokio::spawn(session(stream.block_writes_after(64), Endpoint::Consumer,
        fixture.clients.clone(), SessionLimits::default()));
    tokio::time::timeout(Duration::from_secs(5), capture.blocked.notified()).await.unwrap();
    task.abort(); assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(fixture.clients[0].memory_state().response.checked_out_bytes, 0);
    fixture.clock.advance(10_000).unwrap();
    let retry = reply(fixture.clients[0].try_prepare_lease(g(0), ReadLimits::new(1, 64).unwrap())
        .unwrap().submit().unwrap()).await;
    assert_eq!(retry.records().next().unwrap().token.generation, 2);
}
```

The final session checks hold every append and response buffer while a checkpoint travels through the urgent lane, and reject a 1025-byte append against a fixture whose configured append block is only 1024 bytes. The latter request is globally wire-valid; the configured pool still refuses before reading a body. No extra buffer is allocated to make the service appear more permissive than its admission contract.

```rust title="crates/broker-app/tests/wire/sessions.rs"
#[tokio::test]
async fn checkpoint_needs_neither_append_nor_response_capacity() {
    let fixture = Fixture::new();
    let c = &fixture.clients[0];
    let held = (c.try_prepare_append(3).unwrap(), c.try_prepare_append(3).unwrap(),
        c.try_prepare_lease(g(0), ReadLimits::new(1, 64).unwrap()).unwrap(),
        c.try_prepare_lease(g(1), ReadLimits::new(1, 64).unwrap()).unwrap());
    let (stream, capture) = ScriptIo::new(request(Op::Checkpoint).encode().to_vec(), usize::MAX);
    session(stream, Endpoint::Consumer, fixture.clients.clone(), SessionLimits::default()).await.unwrap();
    assert_eq!(captured_header(&capture).status, Status::Ok);
    assert_eq!(c.memory_state().append.free_buffers, 0);
    assert_eq!(c.memory_state().response.free_buffers, 0);
    drop(held);
}
#[tokio::test]
async fn pool_shape_is_refused_before_an_append_body_is_read() {
    let fixture = Fixture::new();
    // A stricter configured pool is allowed to refuse a globally valid wire shape.
    let mut h = request(Op::Append); h.body = 1025; // Wire-valid, larger than this fixture's 1024-byte block.
    let (stream, capture) = ScriptIo::new(h.encode().to_vec(), usize::MAX);
    session(stream, Endpoint::Producer, Arc::clone(&fixture.clients), SessionLimits::default()).await.unwrap();
    assert_eq!(captured_header(&capture).status, Status::Invalid);
    assert_eq!(capture.read_bytes.load(Ordering::Relaxed), 64);
    assert_eq!(fixture.clients[0].disk_state().reserved_bytes, 0);
}
```

## Loopback and client checks

Create `tests/wire/clients.rs`. `Service` binds both listeners to port zero, asking the OS for unused local ports, then records the concrete addresses. This avoids hard-coded test ports. Its explicit `stop` method signals and joins both listener tasks. The fake peer helper sends a chosen response to test the client boundary independently of a well-behaved broker.

```rust title="crates/broker-app/tests/wire/clients.rs"
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use broker_app::client::{Client, ClientError};
use broker_app::protocol::*;
use broker_app::server::{Clients, ListenerStats, SessionLimits, serve_listener};
use broker_core::codec;
use broker_core::delivery::{AckOutcome, AckRejection, LeaseToken};
use broker_core::types::{Epoch, Offset};
use super::support::*;

struct Service {
    producer: SocketAddr,
    consumer: SocketAddr,
    stop: watch::Sender<bool>,
    tasks: JoinSet<io::Result<ListenerStats>>,
}
impl Service {
    async fn start(clients: Clients, capacity: usize) -> Self {
        let producer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let consumer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addresses = (producer.local_addr().unwrap(), consumer.local_addr().unwrap());
        let (stop, changed) = watch::channel(false);
        let mut tasks = JoinSet::new();
        tasks.spawn(serve_listener(producer, Endpoint::Producer, Arc::clone(&clients),
            SessionLimits::default(), capacity, changed.clone()));
        tasks.spawn(serve_listener(consumer, Endpoint::Consumer, clients,
            SessionLimits::default(), capacity, changed));
        Self { producer: addresses.0, consumer: addresses.1, stop, tasks }
    }
    async fn stop(mut self) -> Vec<ListenerStats> {
        let _ = self.stop.send(true);
        let mut reports = Vec::new();
        while let Some(joined) = self.tasks.join_next().await { reports.push(joined.unwrap().unwrap()); }
        reports
    }
}
async fn connect(address: SocketAddr) -> Client {
    Client::connect(address, Duration::from_secs(10)).await.unwrap()
}
async fn fake<F>(make: F) -> (SocketAddr, JoinHandle<()>)
where F: FnOnce(Header) -> (Header, Vec<u8>) + Send + 'static {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let h = read_header(&mut stream, Instant::now() + Duration::from_secs(5)).await.unwrap().unwrap();
        let (reply, body) = make(h);
        stream.write_all(&reply.encode()).await.unwrap();
        if !body.is_empty() { let _ = stream.write_all(&body).await; }
    });
    (address, task)
}
fn fetched_body(h: Header) -> (Header, Vec<u8>) {
    let token = LeaseToken { epoch: Epoch::new(1).unwrap(), partition: p(h.partition), group: g(h.group),
        offset: Offset::new(0), generation: 1, deadline_ms: 10_000 };
    let mut body = encode_token(token).to_vec();
    body.extend_from_slice(&codec::encode_record(Offset::new(0), b"cat").unwrap());
    (h.response(Status::Ok, [1, 35, 1, 0, 1], 67), body)
}

```

The end-to-end case uses partition 3 rather than accidentally exercising only array entry zero. Both groups process the same payload independently, acknowledge it, and explicitly checkpoint. The payload remains accessible in the client batch while these other calls execute because its body bytes are owned by the client, not borrowed from the server's storage file.

The expiry case receives an ordinary, reusable response with `Rejected(Expired)`, obtains a retry, then sees the old token rejected as `StaleGeneration`. These are application results under status `Ok`, not connection failures. The valid retry acknowledgement finally applies.

```rust title="crates/broker-app/tests/wire/clients.rs"
#[tokio::test]
async fn loopback_append_fetch_work_ack_checkpoint_and_retention() {
    let fixture = Fixture::new();
    let service = Service::start(fixture.clients.clone(), 2).await;
    let mut producer = connect(service.producer).await;
    let mut consumer = connect(service.consumer).await;
    assert_eq!(producer.append(p(3), b"cat").await.unwrap().get(), 0);
    for group in [g(0), g(1)] {
        let batch = consumer.fetch(p(3), group, 2, 128).await.unwrap();
        let record = batch.records().next().unwrap();
        assert_eq!(record.payload.iter().map(|b| u64::from(*b)).sum::<u64>(), 312);
        let ack = consumer.acknowledge(record.token).await.unwrap();
        assert_eq!(ack.outcome, AckOutcome::Applied);
        assert_eq!((ack.progress.volatile_next.get(), ack.progress.persisted_next.get()), (1, 0));
        let checkpoint = consumer.checkpoint(p(3), group).await.unwrap();
        assert!(checkpoint.published); assert_eq!(checkpoint.progress.persisted_next.get(), 1);
        assert_eq!(record.payload, b"cat"); // Client batch owns its separate receive bytes.
    }
    let retained = consumer.retain(p(3)).await.unwrap();
    assert_eq!(retained.floor.get(), 1);
    drop((producer, consumer));
    assert_eq!(service.stop().await.len(), 2);
}
#[tokio::test]
async fn expiry_and_stale_generation_are_normal_ack_results() {
    let fixture = Fixture::new(); append(&fixture.clients[0], b"cat").await;
    let service = Service::start(fixture.clients.clone(), 2).await;
    let mut consumer = connect(service.consumer).await;
    let first = consumer.fetch(p(0), g(0), 1, 64).await.unwrap();
    let old = first.records().next().unwrap().token;
    fixture.clock.advance(10_000).unwrap();
    assert_eq!(consumer.acknowledge(old).await.unwrap().outcome, AckOutcome::Rejected(AckRejection::Expired));
    let retry = consumer.fetch(p(0), g(0), 1, 64).await.unwrap();
    assert_eq!(consumer.acknowledge(old).await.unwrap().outcome, AckOutcome::Rejected(AckRejection::StaleGeneration));
    assert_eq!(consumer.acknowledge(retry.records().next().unwrap().token).await.unwrap().outcome, AckOutcome::Applied);
    assert!(consumer.is_usable());
    drop(consumer); service.stop().await;
}
```

To test separate listener capacity, gate the first producer's synchronization and set each listener's capacity to one. The gate handshake proves the first producer session was accepted and is still occupied. A second producer may complete its TCP handshake into the kernel backlog, but it must not become a second application session. Meanwhile a consumer queries partition 1, whose owner is not blocked on the first partition's gate.

After stopping the service, each listener must report exactly one accepted session. The test then releases the gate and observes the accepted append become visible. It deliberately does not claim that a control request to the same blocked owner would bypass synchronous I/O.

```rust title="crates/broker-app/tests/wire/clients.rs"
#[tokio::test]
async fn producer_connection_limit_does_not_use_consumer_capacity() {
    let (sync, entered, release) = sync_gate();
    let fixture = Fixture::with_sync(sync);
    let mut release = release;
    let service = Service::start(fixture.clients.clone(), 1).await;
    let mut blocked = TcpStream::connect(service.producer).await.unwrap();
    blocked.write_all(&append_frame(b"cat")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered).await.unwrap().unwrap();
    // The first session definitely occupies its slot; the gate is inside its owner request.
    let mut queued = TcpStream::connect(service.producer).await.unwrap();
    queued.write_all(&append_frame(b"dog")).await.unwrap();
    let mut consumer = connect(service.consumer).await;
    assert_eq!(consumer.progress(p(1), g(0)).await.unwrap().volatile_next.get(), 0);
    let reports = service.stop().await;
    assert!(reports.iter().all(|r| r.accepted == 1));
    release.release();
    assert_eq!(reply(fixture.clients[0].try_snapshot().unwrap()).await.visible_end.get(), 1);
    drop((blocked, queued, consumer));
}
```

The cancellation check's fake peer sends five header bytes and then signals that partial response. The client future is polled concurrently with that signal. Once the signal wins, leave the entire lexical scope containing the pinned operation. That drops the actual future owner and its local socket, not just a borrowed pin handle.

The server observes EOF or a socket error, `is_usable()` becomes false, and another call on the same client fails. This directly tests the `Option::take` ownership design rather than merely asserting a cancelled result.

```rust title="crates/broker-app/tests/wire/clients.rs"
#[tokio::test]
async fn cancelling_partial_response_permanently_closes_client_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let h = read_header(&mut stream, Instant::now() + Duration::from_secs(5)).await.unwrap().unwrap();
        stream.write_all(&h.response(Status::Ok, [0, 0, 0, 0, 1], 0).encode()[..5]).await.unwrap();
        sent.send(()).unwrap();
        let mut byte = [0];
        let end = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte)).await.unwrap();
        assert!(matches!(end, Ok(0) | Err(_)));
    });
    let mut client = connect(address).await;
    {
        let operation = client.progress(p(0), g(0));
        tokio::pin!(operation);
        tokio::select! {
            _ = received => {},
            result = &mut operation => panic!("response should still be incomplete: {result:?}"),
        }
    } // Drop the actual operation owner, not merely a Pin<&mut _>.
    assert!(!client.is_usable());
    assert!(client.progress(p(0), g(0)).await.is_err());
    server.await.unwrap();
}
```

The final malformed-peer checks cover an unrelated response identifier, a fetch envelope exceeding the exact request cap, a damaged record payload checksum, and a token whose offset disagrees with the encoded record. The oversized peer sends no body: the client must reject from the header rather than allocate or wait for the advertised amount. Remote refusal and uncertainty codes remain distinct enum values, and each closes this client's connection under the protocol policy.

```rust title="crates/broker-app/tests/wire/clients.rs"
#[tokio::test]
async fn mismatched_response_id_closes_client_before_body_read() {
    let (address, task) = fake(|h| (Header { id: h.id + 1,
        ..h.response(Status::Ok, [0, 0, 0, 0, 1], 0) }, Vec::new())).await;
    let mut client = connect(address).await;
    assert!(client.progress(p(0), g(0)).await.is_err());
    assert!(!client.is_usable()); task.await.unwrap();
}
#[tokio::test]
async fn oversized_fetch_response_is_rejected_from_its_header() {
    let (address, task) = fake(|h|
        (h.response(Status::Ok, [64, MAX_ENCODED as u64, 1, 0, 1], MAX_WIRE_BODY as u32), Vec::new())).await;
    let mut client = connect(address).await;
    assert!(client.fetch(p(0), g(0), 1, 64).await.is_err());
    assert!(!client.is_usable()); task.await.unwrap();
}
#[tokio::test]
async fn fetched_checksum_corruption_closes_client() {
    let (address, task) = fake(|h| {
        let (header, mut body) = fetched_body(h); *body.last_mut().unwrap() ^= 1; (header, body)
    }).await;
    let mut client = connect(address).await;
    assert!(client.fetch(p(0), g(0), 1, 64).await.is_err());
    assert!(!client.is_usable()); task.await.unwrap();
}
#[tokio::test]
async fn token_record_offset_mismatch_is_not_accepted() {
    let (address, task) = fake(|h| {
        let (header, mut body) = fetched_body(h); body[8..16].copy_from_slice(&1u64.to_be_bytes()); (header, body)
    }).await;
    let mut client = connect(address).await;
    assert!(client.fetch(p(0), g(0), 1, 64).await.is_err());
    task.await.unwrap();
}
#[tokio::test]
async fn explicit_refusal_and_uncertainty_remain_distinct_client_errors() {
    for status in [Status::Busy, Status::DiskFull, Status::Unavailable, Status::AdmissionDeadline,
        Status::Refused, Status::Uncertain]
    {
        let (address, task) = fake(move |h| (h.response(status, [0; 5], 0), Vec::new())).await;
        let mut client = connect(address).await;
        assert!(matches!(client.progress(p(0), g(0)).await, Err(ClientError::Remote(value)) if value == status));
        assert!(!client.is_usable()); task.await.unwrap();
    }
}
```

## Run focused checks, then the cumulative regression suite

From the assembled workspace:

```sh
cargo test -p broker-app --test wire format::
cargo test -p broker-app --test wire sessions::
cargo test -p broker-app --test wire clients::
cargo test -p broker-app --test wire
cargo test --workspace --all-targets
```

The expected target counts are 12, 12, 9, and 33, respectively. The cumulative supplied count is 379. Test ordering and timing are deliberately not fixed outputs. Slow storage can trip a watchdog; that is evidence to diagnose, not permission to label a timed-out operation rolled back. Start with the failing layer and run that named test alone with `-- --nocapture` to see its assertion context.

Rust execution was unavailable in this authoring environment: **no compilation, dependency resolution, Cargo test run, rustfmt run, or native macOS execution is claimed**. The commands and output traces are reproducible targets for a compatible local toolchain. Website/source reconstruction and independent arithmetic checks cannot establish Rust type correctness or the concurrency behavior of these tests.

## Two small changes that expose the contract

Change a client's fetch allowance to 31 encoded bytes after appending an empty payload. An encoded empty record is still 32 bytes. Expect an empty batch with `ByteLimit { required: 32 }`; retry with 32 bytes and expect the record. Token overhead is added outside that encoded allowance, so the successful body's size is $32+32=64$ bytes, and the envelope adds another 64.

Next, send the same application payload twice using two ordinary append calls. Expect two different offsets. Even reusing a request identifier in a separate raw connection does not deduplicate the operation; identifiers correlate exchanges only. A client that retries after an uncertain append must tolerate duplicates or supply a separate application identity and handling policy. Do not add an in-memory “seen ID” set and call it durable exactly-once semantics.

## Completion boundary

At the end of this chapter, a native loopback service and typed client exchange real records using the existing durable storage and lease mechanisms. The transport adds framing, bounded sessions, header-first admission, output ownership, and explicit ambiguity; it does not outsource or replace the core state machines. Consumer work precedes acknowledgement, and checkpointing remains visible and deliberate.

Keep the implementation on `work/partition-broker`. The next chapter will coordinate service lifecycle, owner failure, bounded network draining, and explicit joining. This chapter's immediate transport stop is complete and runnable as specified, but it is not already that future supervision policy.

