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
