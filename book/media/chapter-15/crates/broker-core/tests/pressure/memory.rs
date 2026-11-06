use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use broker_core::budget::{MemoryConfig, OWNER_SCRATCH_BYTES};
use broker_core::delivery::{LeaseConfig, LeaseStop};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::{CheckpointPolicy, ProgressAck};
use broker_core::io::StorageFile;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{GroupId, Offset};
use broker_core::worker::{AsyncTicket, BatchConfig, BoundedClient, BoundedHandle, RequestError};
use crate::common::TestDir;

fn g(n: u8) -> GroupId { GroupId::new(n).unwrap() }
fn memory(count: usize, waiters: usize) -> MemoryConfig {
    MemoryConfig::new(64, count, 128, count, waiters).unwrap()
}
fn fixture<S: SyncIo + Send + 'static>(sync: S, lanes: [usize; 3], memory: MemoryConfig)
    -> (TestDir, BoundedHandle, BoundedClient, Arc<ManualClock>)
{
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("bounded");
    let (epoch, mut logs) = recovery::initialize(&root, StorageConfig::new(4096, 2, 8).unwrap())
        .unwrap().into_parts();
    let log = logs.remove(0); drop(logs);
    let clock = Arc::new(ManualClock::new(0));
    let owner = BoundedHandle::start(log, sync,
        BatchConfig::new(1, 4096, Duration::from_secs(1)).unwrap(), lanes,
        LeaseConfig::new(epoch, 8, 100).unwrap(), clock.clone(),
        CheckpointPolicy::explicit_only(), memory).unwrap();
    let client = owner.client(); (directory, owner, client, clock)
}
async fn receive<T>(ticket: AsyncTicket<T>) -> Result<T, RequestError> {
    tokio::time::timeout(Duration::from_secs(5), ticket).await.expect("reply watchdog")
        .expect("reply sender disappeared without its failure guard")
}
async fn publish(client: &BoundedClient, bytes: &[u8]) -> Offset {
    let mut prepared = client.try_prepare_append(bytes.len()).unwrap();
    prepared.payload_mut().copy_from_slice(bytes);
    receive(prepared.submit().unwrap()).await.unwrap()
}
async fn lease(client: &BoundedClient, group: u8, bytes: usize) -> broker_core::delivery::PooledLeaseBatch {
    receive(client.try_prepare_lease(g(group), ReadLimits::new(8, bytes).unwrap()).unwrap()
        .submit().unwrap()).await.unwrap()
}
fn deadline() -> tokio::time::Instant { tokio::time::Instant::now() + Duration::from_secs(10) }
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::Relaxed); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::Relaxed); }
}
fn poll<F: Future>(future: Pin<&mut F>, wakes: &Arc<Wakes>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::clone(wakes));
    future.poll(&mut Context::from_waker(&waker))
}

#[tokio::test(flavor = "current_thread")]
async fn prepared_append_drop_releases_slot_buffer_and_disk() {
    let (_d, owner, client, _) = fixture(FileSync, [1, 2, 2], memory(1, 2));
    let prepared = client.try_prepare_append(3).unwrap();
    let state = client.memory_state();
    assert_eq!(state.append.checked_out_bytes, state.append.allocated_bytes);
    assert_eq!(client.disk_state().reserved_bytes, 67);
    assert!(matches!(client.try_prepare_append(3), Err(AdmissionError::Full)));
    drop(prepared);
    assert_eq!(client.memory_state().append.checked_out_bytes, 0);
    assert_eq!(client.disk_state().reserved_bytes, 0);
    assert_eq!(publish(&client, b"cat").await.get(), 0);
    owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn pool_pressure_is_distinct_from_queue_pressure() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 2));
    let prepared = client.try_prepare_append(3).unwrap();
    assert!(matches!(client.try_prepare_append(1), Err(AdmissionError::MemoryFull)));
    assert!(matches!(client.try_prepare_append(65), Err(AdmissionError::TooLarge)));
    assert!(matches!(client.prepare_append(65, deadline()).await, Err(AdmissionError::TooLarge)));
    assert_eq!(client.memory_state().append_waiters, 0);
    drop(prepared); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn allocation_capacity_and_scratch_stay_constant_after_reuse() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(2, 2));
    let before = client.memory_state();
    for _ in 0..12 { publish(&client, b"cat").await; }
    let after = client.memory_state();
    assert_eq!(before.append.allocated_bytes, after.append.allocated_bytes);
    assert_eq!(before.response.allocated_bytes, after.response.allocated_bytes);
    assert_eq!(after.append.checked_out_bytes, 0);
    assert_eq!(after.scratch_bytes, OWNER_SCRATCH_BYTES);
    owner.join().unwrap();
    // This client still owns the pools after its worker exits. The scratch snapshot is an allowance.
    let retained = client.memory_state();
    assert_eq!(retained.append.allocated_bytes, before.append.allocated_bytes);
    assert_eq!(retained.response.allocated_bytes, before.response.allocated_bytes);
    assert_eq!(retained.append.free_buffers, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn close_rejects_a_prepared_submit_without_leaking_any_credit() {
    let (_d, owner, client, _) = fixture(FileSync, [2, 2, 2], memory(1, 2));
    let prepared = client.try_prepare_append(3).unwrap(); owner.close();
    assert!(matches!(prepared.submit(), Err(AdmissionError::Closed)));
    assert_eq!(client.disk_state().reserved_bytes, 0);
    assert_eq!(client.memory_state().append.checked_out_bytes, 0);
    owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn reply_owns_capacity_until_its_batch_is_dropped() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 2));
    publish(&client, b"cat").await;
    let batch = lease(&client, 0, 128).await;
    assert_eq!(batch.records().next().unwrap().payload(), b"cat");
    let state = client.memory_state();
    assert_eq!(state.response.checked_out_bytes, state.response.allocated_bytes);
    assert!(matches!(client.try_prepare_lease(g(1), ReadLimits::new(1, 128).unwrap()), Err(AdmissionError::MemoryFull)));
    let token = batch.records().next().unwrap().token;
    assert!(matches!(receive(client.try_ack_progress(g(0), token).unwrap()).await.unwrap(), ProgressAck::Accepted(_)));
    assert_ne!(client.memory_state().response.checked_out_bytes, 0);
    drop(batch); assert_eq!(client.memory_state().response.checked_out_bytes, 0);
    owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn acknowledgement_and_checkpoint_progress_with_all_payload_buffers_held() {
    let (_d, owner, client, _) = fixture(FileSync, [2, 1, 2], memory(2, 2));
    publish(&client, b"cat").await;
    let batch = lease(&client, 0, 128).await;
    let response = client.try_prepare_lease(g(1), ReadLimits::new(1, 128).unwrap()).unwrap();
    let append0 = client.try_prepare_append(3).unwrap();
    let append1 = client.try_prepare_append(3).unwrap();
    assert!(matches!(client.try_progress(g(0)), Err(AdmissionError::Full)));
    assert_eq!(client.memory_state().append.free_buffers, 0);
    assert_eq!(client.memory_state().response.free_buffers, 0);
    let token = batch.records().next().unwrap().token;
    receive(client.try_ack_progress(g(0), token).unwrap()).await.unwrap();
    let checkpoint = receive(client.try_checkpoint(g(0)).unwrap()).await.unwrap();
    assert_eq!(checkpoint.progress.persisted_next.get(), 1);
    assert_eq!(client.disk_state().reserved_bytes, 134); // Prepared producers still own disk credit.
    drop((append0, append1, response, batch)); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_waiter_owns_no_payload_and_leaves_no_waiter_entry() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 1));
    let held = client.try_prepare_append(3).unwrap();
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let mut waiting = Box::pin(client.prepare_append(3, deadline()));
    assert!(poll(waiting.as_mut(), &wakes).is_pending());
    assert_eq!(client.memory_state().append_waiters, 1);
    assert_eq!(client.disk_state().reserved_bytes, 67);
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0); // Failed transaction did not wake itself.
    drop(waiting); assert_eq!(client.memory_state().append_waiters, 0);
    drop(held); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn returned_buffer_wakes_an_enabled_waiter() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 2));
    let held = client.try_prepare_append(3).unwrap();
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let mut waiting = Box::pin(client.prepare_append(3, deadline()));
    assert!(poll(waiting.as_mut(), &wakes).is_pending()); drop(held);
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    let Poll::Ready(Ok(prepared)) = poll(waiting.as_mut(), &wakes) else { panic!("buffer return did not enable admission") };
    drop((prepared, waiting)); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn returned_slot_wakes_waiter_even_when_a_buffer_was_always_free() {
    let (_d, owner, client, _) = fixture(FileSync, [1, 4, 2], memory(2, 2));
    let held = client.try_prepare_append(3).unwrap();
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let mut waiting = Box::pin(client.prepare_append(3, deadline()));
    assert!(poll(waiting.as_mut(), &wakes).is_pending());
    assert_eq!(client.memory_state().append.free_buffers, 1);
    drop(held);
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    let Poll::Ready(Ok(prepared)) = poll(waiting.as_mut(), &wakes) else { panic!("slot return did not enable admission") };
    drop((prepared, waiting)); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn waiter_limit_is_bounded_and_independent_between_request_classes() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 1));
    let held = client.try_prepare_append(3).unwrap();
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let mut waiting = Box::pin(client.prepare_append(3, deadline()));
    assert!(poll(waiting.as_mut(), &wakes).is_pending());
    assert!(matches!(client.prepare_append(3, deadline()).await, Err(AdmissionError::WaitersFull)));
    let response = client.prepare_lease(g(0), ReadLimits::new(1, 128).unwrap(), deadline()).await.unwrap();
    drop((response, waiting, held)); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn expired_admission_deadline_does_not_take_immediately_available_capacity() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 1));
    assert!(matches!(client.prepare_append(3, tokio::time::Instant::now()).await, Err(AdmissionError::Deadline)));
    assert_eq!(client.memory_state().append.checked_out_bytes, 0);
    assert_eq!(client.memory_state().append_waiters, 0);
    owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn closure_wakes_admission_without_needing_capacity_to_be_returned() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 1));
    let held = client.try_prepare_append(3).unwrap();
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let mut waiting = Box::pin(client.prepare_append(3, deadline()));
    assert!(poll(waiting.as_mut(), &wakes).is_pending()); owner.close();
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    assert!(matches!(poll(waiting.as_mut(), &wakes), Poll::Ready(Err(AdmissionError::Closed))));
    drop((waiting, held)); owner.join().unwrap();
}

struct Gate { entered: mpsc::SyncSender<()>, release: Option<mpsc::Receiver<()>> }
impl SyncIo for Gate {
    fn sync(&mut self, _base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        if let Some(release) = self.release.take() {
            self.entered.send(()).map_err(io::Error::other)?;
            release.recv_timeout(Duration::from_secs(5)).map_err(io::Error::other)?;
        }
        file.sync()
    }
}
#[tokio::test(flavor = "current_thread")]
async fn abandoning_a_queued_publish_ticket_does_not_release_the_commands_buffer() {
    let (entered, observed) = mpsc::sync_channel(1); let (release, gate) = mpsc::channel();
    let (_d, owner, client, _) = fixture(Gate { entered, release: Some(gate) }, [2, 2, 2], memory(1, 2));
    let mut first = client.try_prepare_append(3).unwrap(); first.payload_mut().copy_from_slice(b"cat");
    let ticket = first.submit().unwrap();
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut second = client.try_prepare_append(3).unwrap(); second.payload_mut().copy_from_slice(b"dog");
    drop(second.submit().unwrap());
    assert_eq!(client.memory_state().append.free_buffers, 0);
    assert_eq!(client.disk_state().reserved_bytes, 67);
    release.send(()).unwrap(); receive(ticket).await.unwrap();
    assert_eq!(owner.join().unwrap().visible_end.get(), 2);
    assert_eq!(client.memory_state().append.checked_out_bytes, 0);
    assert_eq!(client.disk_state().reserved_bytes, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn unsubmitted_fetch_does_not_install_an_attempt() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 2));
    publish(&client, b"cat").await;
    drop(client.try_prepare_lease(g(0), ReadLimits::new(1, 128).unwrap()).unwrap());
    let batch = lease(&client, 0, 128).await;
    assert_eq!(batch.records().next().unwrap().token.generation, 1);
    drop(batch); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn abandoned_fetch_reply_returns_memory_but_does_not_undo_a_lease() {
    let (_d, owner, client, clock) = fixture(FileSync, [4, 4, 2], memory(1, 2));
    publish(&client, b"cat").await;
    drop(client.try_prepare_lease(g(0), ReadLimits::new(1, 128).unwrap()).unwrap().submit().unwrap());
    // Same query lane: this reply establishes that the preceding lease command ran.
    receive(client.try_snapshot().unwrap()).await.unwrap();
    assert_eq!(client.memory_state().response.checked_out_bytes, 0);
    let empty = lease(&client, 0, 128).await;
    assert_eq!(empty.records().len(), 0); drop(empty);
    clock.advance(100).unwrap();
    let retry = lease(&client, 0, 128).await;
    assert_eq!(retry.records().next().unwrap().token.generation, 2);
    drop(retry); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn nonfitting_header_spends_neither_output_bytes_nor_attempt() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 2));
    publish(&client, b"cat").await;
    let short = lease(&client, 0, 34).await;
    assert_eq!(short.records().len(), 0);
    assert_eq!(short.stop, LeaseStop::ByteLimit { required: 35 }); drop(short);
    let full = lease(&client, 0, 35).await;
    assert_eq!(full.records().next().unwrap().token.generation, 1);
    assert_eq!(full.encoded_bytes, 35); drop(full); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_encoding_and_sparse_forward_copy_match_the_record_codec() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 2));
    for bytes in [b"cat".as_slice(), b"", b"owl"] { publish(&client, bytes).await; }
    let batch = lease(&client, 0, 128).await;
    assert_eq!(batch.records().len(), 3);
    for record in batch.records() {
        assert_eq!(record.encoded, broker_core::codec::encode_record(record.token.offset, record.payload()).unwrap());
    }
    assert!(batch.scanned_records >= 3); drop(batch); owner.join().unwrap();
}

struct FailSync;
impl SyncIo for FailSync {
    fn sync(&mut self, _base: Offset, _file: &mut dyn StorageFile) -> io::Result<()> {
        Err(io::Error::other("controlled synchronization failure"))
    }
}
#[tokio::test(flavor = "current_thread")]
async fn storage_failure_stops_owner_and_returns_payload_capacity() {
    let (_d, owner, client, _) = fixture(FailSync, [4, 4, 2], memory(1, 2));
    let prepared = client.try_prepare_append(3).unwrap();
    assert!(matches!(receive(prepared.submit().unwrap()).await, Err(RequestError::Failed(_))));
    assert!(owner.join().is_err());
    assert_eq!(client.memory_state().append.checked_out_bytes, 0);
    assert_eq!(client.disk_state().reserved_bytes, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn disk_refusal_rolls_back_memory_and_slot_admission() {
    let directory = TestDir::new().unwrap();
    let (epoch, mut logs) = recovery::initialize_budgeted(&directory.path().join("small-disk"),
        StorageConfig::new(102, 2, 4).unwrap(), broker_core::budget::DiskQuota::new(100, 416).unwrap())
        .unwrap().into_parts();
    let log = logs.remove(0); drop(logs);
    let owner = BoundedHandle::start(log, FileSync,
        BatchConfig::new(1, 4096, Duration::from_secs(1)).unwrap(), [1, 2, 2],
        LeaseConfig::new(epoch, 8, 100).unwrap(), Arc::new(ManualClock::new(0)),
        CheckpointPolicy::explicit_only(), memory(1, 1)).unwrap();
    let client = owner.client(); publish(&client, b"cat").await;
    for _ in 0..4 {
        assert!(matches!(client.prepare_append(3, deadline()).await, Err(AdmissionError::DiskFull)));
        assert_eq!(client.memory_state().append.checked_out_bytes, 0);
        assert_eq!(client.disk_state().reserved_bytes, 0);
    }
    receive(client.try_checkpoint(g(0)).unwrap()).await.unwrap(); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn response_wait_cancellation_and_return_use_the_independent_pool() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 1));
    publish(&client, b"cat").await;
    let held = lease(&client, 0, 128).await;
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let limits = ReadLimits::new(1, 128).unwrap();
    let mut waiting = Box::pin(client.prepare_lease(g(1), limits, deadline()));
    assert!(poll(waiting.as_mut(), &wakes).is_pending());
    assert_eq!(client.memory_state().response_waiters, 1);
    drop(waiting); assert_eq!(client.memory_state().response_waiters, 0);
    let mut waiting = Box::pin(client.prepare_lease(g(1), limits, deadline()));
    assert!(poll(waiting.as_mut(), &wakes).is_pending()); drop(held);
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    let Poll::Ready(Ok(prepared)) = poll(waiting.as_mut(), &wakes) else { panic!("reply return did not enable admission") };
    drop((waiting, prepared)); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn maximum_payload_uses_scratch_and_a_preallocated_response() {
    let directory = TestDir::new().unwrap();
    let (epoch, mut logs) = recovery::initialize(&directory.path().join("maximum"),
        StorageConfig::new(2 * 1024 * 1024, 2, 4).unwrap()).unwrap().into_parts();
    let log = logs.remove(0); drop(logs);
    let owner = BoundedHandle::start(log, FileSync,
        BatchConfig::new(1, 2 * 1024 * 1024, Duration::from_secs(1)).unwrap(), [2, 2, 2],
        LeaseConfig::new(epoch, 8, 100).unwrap(), Arc::new(ManualClock::new(0)),
        CheckpointPolicy::explicit_only(), MemoryConfig::default()).unwrap();
    let client = owner.client(); let before = client.memory_state();
    assert_eq!(before.append.allocated_bytes + before.response.allocated_bytes + before.scratch_bytes,
        16 * 1024 * 1024);
    let mut prepared = client.try_prepare_append(broker_core::codec::MAX_PAYLOAD).unwrap();
    prepared.payload_mut().fill(0xa5); receive(prepared.submit().unwrap()).await.unwrap();
    let batch = lease(&client, 0, 2 * 1024 * 1024).await;
    let record = batch.records().next().unwrap();
    assert_eq!(record.payload().len(), broker_core::codec::MAX_PAYLOAD);
    assert!(record.payload().iter().all(|b| *b == 0xa5));
    assert_eq!(batch.encoded_bytes, broker_core::codec::MAX_PAYLOAD + 32);
    drop(batch); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn corrupt_read_returns_response_buffer_and_refuses_the_owner() {
    use std::io::{Seek, SeekFrom, Write};
    let (directory, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 2));
    publish(&client, b"cat").await;
    let path = directory.path().join("bounded/partition-0/00000000000000000000.seg");
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(64)).unwrap(); file.write_all(b"X").unwrap(); file.sync_all().unwrap();
    let result = receive(client.try_prepare_lease(g(0), ReadLimits::new(1, 128).unwrap()).unwrap()
        .submit().unwrap()).await;
    assert!(matches!(result, Err(RequestError::Failed(_))));
    assert!(owner.join().is_err());
    assert_eq!(client.memory_state().response.checked_out_bytes, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn waiting_deadline_expires_without_cancelling_an_existing_reservation() {
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 1));
    let held = client.try_prepare_append(3).unwrap();
    let end = tokio::time::Instant::now() + Duration::from_millis(5);
    let result = tokio::time::timeout(Duration::from_secs(5), client.prepare_append(3, end)).await
        .expect("admission deadline watchdog");
    assert!(matches!(result, Err(AdmissionError::Deadline)));
    assert_eq!(client.memory_state().append_waiters, 0);
    assert_eq!(client.disk_state().reserved_bytes, 67);
    drop(held); owner.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn admission_futures_are_send_without_a_mutex_guard_across_await() {
    fn require_send<T: Send>(_: T) {}
    let (_d, owner, client, _) = fixture(FileSync, [4, 4, 2], memory(1, 1));
    require_send(client.prepare_append(3, deadline()));
    require_send(client.prepare_lease(g(0), ReadLimits::new(1, 128).unwrap(), deadline()));
    assert_eq!(client.memory_state().append_waiters, 0); // Unpolled futures acquired nothing.
    owner.join().unwrap();
}
