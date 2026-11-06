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
