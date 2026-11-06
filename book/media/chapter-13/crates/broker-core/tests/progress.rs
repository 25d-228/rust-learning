mod common;

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use broker_core::codec;
use broker_core::delivery::{AckOutcome, AckRejection, LeaseBatch, LeaseConfig, LeaseToken};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::{AckAccepted, CheckpointPolicy, ProgressAck, ProgressState};
use broker_core::io::StorageFile;
use broker_core::storage::checkpoint::{self, Checkpoint};
use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::log::{PartitionLog, segment_name};
use broker_core::storage::metadata::{self, META_NAME, PublicationStep};
use broker_core::storage::recovery::{self, StartupStep, StorageSession};
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{Epoch, GroupId, Offset, PartitionId};
use broker_core::worker::{BatchConfig, Client, RequestError, Ticket, WorkerError, WorkerHandle};
use common::TestDir;

fn offset(value: u64) -> Offset { Offset::new(value) }
fn partition(value: u16) -> PartitionId { PartitionId::new(value).unwrap() }
fn group(value: u8) -> GroupId { GroupId::new(value).unwrap() }
fn config() -> StorageConfig { StorageConfig::new(102, 2, 16).unwrap() }
fn batch(count: usize) -> BatchConfig { BatchConfig::new(count, 4096, Duration::from_secs(60)).unwrap() }
fn policy() -> CheckpointPolicy { CheckpointPolicy::explicit_only() }
fn receive<T>(ticket: Ticket<T>) -> Result<T, RequestError> {
    ticket.recv_timeout(Duration::from_secs(5)).expect("owner reply watchdog")
}
fn start<S: SyncIo + Send + 'static>(session: StorageSession, sync: S, delay: CheckpointPolicy,
    count: usize) -> (WorkerHandle, Client)
{
    let (epoch, mut logs) = session.into_parts();
    let log = logs.remove(0); drop(logs);
    let owner = WorkerHandle::start_with_progress(log, sync, batch(count), 16, 16,
        LeaseConfig::new(epoch, 4, 10_000).unwrap(), Arc::new(ManualClock::new(0)), delay).unwrap();
    let client = owner.client();
    (owner, client)
}
fn fixture<S: SyncIo + Send + 'static>(sync: S, delay: CheckpointPolicy)
    -> (TestDir, PathBuf, WorkerHandle, Client)
{
    let directory = TestDir::new().unwrap();
    let root = directory.path().join("broker");
    let (owner, client) = start(recovery::initialize(&root, config()).unwrap(), sync, delay, 1);
    for (i, payload) in [b"cat", b"dog", b"owl", b"fox"].iter().enumerate() {
        assert_eq!(receive(client.try_publish(payload.to_vec()).unwrap()).unwrap(), offset(i as u64));
    }
    (directory, root, owner, client)
}
fn lease(client: &Client, id: u8) -> LeaseBatch {
    receive(client.try_lease(group(id), ReadLimits::new(4, 140).unwrap()).unwrap()).unwrap()
}
fn accepted(client: &Client, token: LeaseToken) -> AckAccepted {
    match receive(client.try_ack_progress(token.group, token).unwrap()).unwrap() {
        ProgressAck::Accepted(value) => value,
        other => panic!("expected accepted completion, got {other:?}"),
    }
}
fn state(client: &Client, id: u8) -> ProgressState {
    receive(client.try_progress(group(id)).unwrap()).unwrap()
}
fn checkpoint(client: &Client, id: u8) -> u64 {
    receive(client.try_checkpoint(group(id)).unwrap()).unwrap().progress.persisted_next.get()
}
fn stored(root: &Path, p: u16, g: u8) -> Checkpoint {
    checkpoint::read(root, partition(p), group(g)).unwrap()
}
fn path(root: &Path, p: u16, g: u8) -> PathBuf { root.join(checkpoint::name(partition(p), group(g))) }
fn torn_tail(root: &Path) -> PathBuf {
    let path = root.join("partition-1").join(segment_name(offset(0)));
    OpenOptions::new().append(true).open(&path).unwrap()
        .write_all(&codec::encode_record(offset(0), b"cat").unwrap()[..34]).unwrap();
    path
}
fn reseal(raw: &mut [u8; 48]) {
    let crc = crc32fast::hash(&raw[..44]); raw[44..48].copy_from_slice(&crc.to_be_bytes());
}

#[test]
fn checkpoint_has_independent_golden_bytes() {
    let value = Checkpoint { partition: partition(2), group: group(1), epoch: Epoch::new(7).unwrap(), next: offset(19) };
    let golden = [
        0x50, 0x42, 0x47, 0x50, 0x00, 0x01, 0x00, 0x30,
        0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0xf8, 0x0f, 0x9b, 0x4d,
    ];
    assert_eq!(value.encode(), golden);
    assert_eq!(Checkpoint::decode(&golden).unwrap(), value);
}

#[test]
fn malformed_lengths_corruption_and_resealed_invalid_fields_are_refused() {
    let good = stored_fixture_bytes();
    for size in 0..48 { assert!(Checkpoint::decode(&good[..size]).is_err()); }
    let mut extra = good.to_vec(); extra.push(0); assert!(Checkpoint::decode(&extra).is_err());
    for byte in 0..48 {
        let mut bad = good; bad[byte] ^= 1;
        assert!(Checkpoint::decode(&bad).is_err(), "corrupt byte {byte}");
    }
    for field in 0..8 {
        let mut bad = good;
        match field {
            0 => bad[0] = b'X', 1 => bad[5] = 2, 2 => bad[7] = 47,
            3 => bad[9] = 4, 4 => bad[10] = 2, 5 => bad[11] = 1,
            6 => bad[12..20].fill(0), 7 => bad[28] = 1, _ => unreachable!(),
        }
        reseal(&mut bad); assert!(Checkpoint::decode(&bad).is_err());
    }
}
fn stored_fixture_bytes() -> [u8; 48] {
    Checkpoint { partition: partition(2), group: group(1), epoch: Epoch::new(7).unwrap(), next: offset(19) }.encode()
}

#[test]
fn semantic_validation_checks_identity_epoch_floor_and_end() {
    let good = Checkpoint::decode(&stored_fixture_bytes()).unwrap();
    assert_eq!(good.validate(partition(2), group(1), Epoch::new(8).unwrap(), offset(19), offset(19)).unwrap(), offset(19));
    for (p, g, e, floor, end) in [(1, 1, 8, 0, 20), (2, 0, 8, 0, 20), (2, 1, 6, 0, 20),
        (2, 1, 8, 20, 21), (2, 1, 8, 0, 18), (2, 1, 8, 20, 19)]
    {
        assert!(good.validate(partition(p), group(g), Epoch::new(e).unwrap(), offset(floor), offset(end)).is_err());
    }
}

#[test]
fn initialization_publishes_all_eight_zero_authorities_before_serving() {
    let directory = TestDir::new().unwrap(); let root = directory.path().join("broker");
    let session = recovery::initialize(&root, config()).unwrap();
    for p in 0..4 { for g in 0..2 {
        let value = stored(&root, p, g);
        assert_eq!((value.epoch, value.next), (session.epoch(), offset(0)));
        assert!(!root.join(checkpoint::pending_name(partition(p), group(g))).exists());
    } }
    drop(session);
}

#[test]
fn a_completed_follower_does_not_become_checkpointed_through_a_gap() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    let first = lease(&client, 0);
    let follower = accepted(&client, first.records[2].token);
    assert_eq!(follower.ack.retired, 0);
    assert_eq!(follower.progress.volatile_next, offset(0));
    let duplicate = accepted(&client, first.records[2].token);
    assert_eq!(duplicate.ack.outcome, AckOutcome::Duplicate);
    let clean = receive(client.try_checkpoint(group(0)).unwrap()).unwrap();
    assert!(!clean.published); assert_eq!(clean.progress.persisted_next, offset(0));
    accepted(&client, first.records[0].token);
    assert_eq!(checkpoint(&client, 0), 1);
    let closes_gap = accepted(&client, first.records[1].token);
    assert_eq!((closes_gap.ack.retired, closes_gap.progress.volatile_next.get(),
        closes_gap.progress.persisted_next.get()), (2, 3, 1));
    assert_eq!(checkpoint(&client, 0), 3);
    assert_eq!(stored(&root, 0, 0).next, offset(3));
    owner.join().unwrap();
}

#[test]
fn accepted_but_uncheckpointed_completion_repeats_after_restart() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    let first = lease(&client, 0);
    for record in &first.records { accepted(&client, record.token); }
    assert_eq!((state(&client, 0).volatile_next.get(), state(&client, 0).persisted_next.get()), (4, 0));
    owner.join().unwrap(); drop(client);
    let (owner, client) = start(recovery::recover(&root).unwrap(), FileSync, policy(), 1);
    let repeated = lease(&client, 0);
    assert_eq!(repeated.records.iter().map(|r| r.token.offset.get()).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    assert_eq!(repeated.state.epoch.get(), 2);
    owner.join().unwrap();
}

#[test]
fn two_groups_restore_different_frontiers_and_empty_windows() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    let a = lease(&client, 0); let b = lease(&client, 1);
    for record in &a.records[..2] { accepted(&client, record.token); }
    accepted(&client, b.records[0].token);
    assert_eq!(checkpoint(&client, 0), 2); assert_eq!(checkpoint(&client, 1), 1);
    accepted(&client, a.records[2].token); // C=3, but persisted remains 2.
    owner.join().unwrap(); drop(client);
    let (owner, client) = start(recovery::recover(&root).unwrap(), FileSync, policy(), 1);
    for (g, p) in [(0, 2), (1, 1)] {
        let state = receive(client.try_group_state(group(g)).unwrap()).unwrap();
        assert_eq!((state.window.completed_end.get(), state.window.issued_end.get(), state.window.occupied), (p, p, 0));
    }
    assert_eq!(lease(&client, 0).records[0].token.offset, offset(2));
    assert_eq!(lease(&client, 1).records[0].token.offset, offset(1));
    owner.join().unwrap();
}

#[test]
fn persisted_restart_still_fences_identical_old_attempt_numbers() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    let first = lease(&client, 0);
    accepted(&client, first.records[0].token); assert_eq!(checkpoint(&client, 0), 1);
    let old = first.records[1].token;
    owner.join().unwrap(); drop(client);
    let (owner, client) = start(recovery::recover(&root).unwrap(), FileSync, policy(), 1);
    let new = lease(&client, 0).records[0].token;
    assert_eq!((new.offset, new.generation, new.deadline_ms), (old.offset, old.generation, old.deadline_ms));
    let result = receive(client.try_ack_progress(group(0), old).unwrap()).unwrap();
    assert!(matches!(result, ProgressAck::NotApplied { ack, .. }
        if ack.outcome == AckOutcome::Rejected(AckRejection::WrongEpoch)));
    assert_eq!(state(&client, 0).volatile_next, offset(1));
    accepted(&client, new); owner.join().unwrap();
}

#[test]
fn abandoned_checkpoint_reply_does_not_undo_publication() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    let first = lease(&client, 0); accepted(&client, first.records[0].token);
    drop(client.try_checkpoint(group(0)).unwrap());
    // Same FIFO lane: this response follows the abandoned checkpoint command.
    assert_eq!(state(&client, 0).persisted_next, offset(1));
    assert_eq!(stored(&root, 0, 0).next, offset(1));
    owner.join().unwrap();
}

#[test]
fn durable_mode_rejects_legacy_acknowledgement_and_tokenless_completion() {
    let (_directory, _root, owner, client) = fixture(FileSync, policy());
    let token = lease(&client, 0).records[0].token;
    assert!(matches!(receive(client.try_ack(group(0), token).unwrap()), Err(RequestError::DeliveryDisabled)));
    assert!(matches!(receive(client.try_complete(group(0), offset(0)).unwrap()), Err(RequestError::DeliveryDisabled)));
    assert_eq!(state(&client, 0).volatile_next, offset(0));
    accepted(&client, token); owner.join().unwrap();
}

#[test]
fn checkpoint_is_not_a_producer_flush_or_a_cross_lane_barrier() {
    let directory = TestDir::new().unwrap(); let root = directory.path().join("broker");
    let (owner, client) = start(recovery::initialize(&root, config()).unwrap(), FileSync, policy(), 2);
    let ticket = client.try_publish(b"cat".to_vec()).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let s = receive(client.try_snapshot().unwrap()).unwrap();
        if s.pending_records == 1 { assert_eq!(s.visible_end, offset(0)); break; }
        assert!(Instant::now() < end, "data command never processed");
    }
    assert_eq!(checkpoint(&client, 0), 0);
    assert!(matches!(ticket.try_recv(), Err(mpsc::TryRecvError::Empty)));
    receive(client.try_flush().unwrap()).unwrap();
    assert_eq!(receive(ticket).unwrap(), offset(0));
    owner.join().unwrap();
}

#[test]
fn a_missing_checkpoint_never_uses_a_candidate_or_repairs_record_files() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    owner.join().unwrap(); drop(client);
    let tail = torn_tail(&root); let before = fs::read(&tail).unwrap();
    let target = path(&root, 0, 1);
    let pending = root.join(checkpoint::pending_name(partition(0), group(1)));
    fs::rename(&target, &pending).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert!(!target.exists()); assert!(pending.exists());
    assert_eq!(fs::read(tail).unwrap(), before);
    assert_eq!(metadata::read(&root).unwrap().epoch().get(), 1);
}

#[test]
fn corrupt_authority_never_uses_a_valid_candidate() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    owner.join().unwrap(); drop(client);
    let target = path(&root, 0, 0);
    fs::copy(&target, root.join(checkpoint::pending_name(partition(0), group(0)))).unwrap();
    let mut bad = fs::read(&target).unwrap(); bad[20] ^= 1; fs::write(&target, bad).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(metadata::read(&root).unwrap().epoch().get(), 1);
}

#[test]
fn wrong_scope_future_epoch_and_a_frontier_in_torn_data_fail_before_repair() {
    for case in 0..4 {
        let (_directory, root, owner, client) = fixture(FileSync, policy());
        owner.join().unwrap(); drop(client);
        let tail = torn_tail(&root); let before = fs::read(&tail).unwrap();
        let mut value = stored(&root, 1, 0);
        match case {
            0 => value.partition = partition(0), 1 => value.group = group(1),
            2 => value.epoch = Epoch::new(2).unwrap(), 3 => value.next = offset(1), _ => unreachable!(),
        }
        fs::write(path(&root, 1, 0), value.encode()).unwrap();
        assert!(recovery::recover(&root).is_err());
        assert_eq!(fs::read(tail).unwrap(), before);
        assert_eq!(metadata::read(&root).unwrap().epoch().get(), 1);
    }
}

#[test]
fn only_valid_authority_permits_discarding_an_abandoned_candidate() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    let first = lease(&client, 0); accepted(&client, first.records[0].token); checkpoint(&client, 0);
    owner.join().unwrap(); drop(client);
    let pending = root.join(checkpoint::pending_name(partition(0), group(0)));
    fs::write(&pending, b"incomplete").unwrap();
    let (owner, client) = start(recovery::recover(&root).unwrap(), FileSync, policy(), 1);
    assert_eq!(state(&client, 0).persisted_next, offset(1)); assert!(!pending.exists());
    owner.join().unwrap();
}

#[cfg(unix)]
#[test]
fn checkpoint_symlinks_are_refused_without_following_the_target() {
    let (directory, root, owner, client) = fixture(FileSync, policy());
    owner.join().unwrap(); drop(client);
    let target = path(&root, 0, 0); let outside = directory.path().join("outside");
    fs::rename(&target, &outside).unwrap(); let before = fs::read(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, &target).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::read(outside).unwrap(), before);
}

#[test]
fn partial_first_initialization_never_publishes_the_broker_authority() {
    for point in points() {
        let directory = TestDir::new().unwrap(); let root = directory.path().join("broker");
        let mut cut = |step| match step {
            StartupStep::Checkpoint { partition: p, group: g, step: s }
                if p == partition(0) && g == group(1) && s == point => Err(io::Error::other("initial checkpoint cut")),
            _ => Ok(()),
        };
        assert!(recovery::initialize_with(&root, config(), &mut FileSync, &mut cut).is_err());
        assert!(!root.join(META_NAME).exists());
        assert!(recovery::recover(&root).is_err());
        assert!(recovery::initialize(&root, config()).is_err());
    }
}

#[test]
fn unknown_checkpoint_names_and_nonregular_candidates_are_refused() {
    for directory_candidate in [false, true] {
        let (_directory, root, owner, client) = fixture(FileSync, policy());
        owner.join().unwrap(); drop(client);
        if directory_candidate {
            fs::create_dir(root.join(checkpoint::pending_name(partition(0), group(0)))).unwrap();
        } else { fs::write(root.join("progress-p4-g0.ckp"), stored_fixture_bytes()).unwrap(); }
        assert!(recovery::recover(&root).is_err());
    }
}

#[test]
fn durable_start_requires_startup_provenance_and_the_matching_epoch() {
    let directory = TestDir::new().unwrap();
    let guard = Arc::new(DirectoryGuard::acquire(directory.path()).unwrap());
    let log = PartitionLog::create(guard, partition(0), config()).unwrap();
    let result = WorkerHandle::start_with_progress(log, FileSync, batch(1), 4, 4,
        LeaseConfig::new(Epoch::new(1).unwrap(), 4, 100).unwrap(), Arc::new(ManualClock::new(0)), policy());
    assert!(matches!(result, Err(WorkerError::Setup(_))));
    let root = directory.path().join("broker");
    let (_, mut logs) = recovery::initialize(&root, config()).unwrap().into_parts();
    let result = WorkerHandle::start_with_progress(logs.remove(0), FileSync, batch(1), 4, 4,
        LeaseConfig::new(Epoch::new(2).unwrap(), 4, 100).unwrap(), Arc::new(ManualClock::new(0)), policy());
    assert!(matches!(result, Err(WorkerError::Setup(_))));
}

fn points() -> [PublicationStep; 5] {
    [PublicationStep::TemporaryCreated, PublicationStep::TemporaryWritten, PublicationStep::TemporarySynced,
        PublicationStep::Renamed, PublicationStep::DirectorySynced]
}
fn is_group_zero(target: &Path) -> bool { target.file_name() == Some(OsStr::new("progress-p0-g0.ckp")) }
struct Cut { point: PublicationStep }
impl SyncIo for Cut {
    fn sync(&mut self, _: Offset, file: &mut dyn StorageFile) -> io::Result<()> { file.sync() }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        if is_group_zero(target) && step == self.point { Err(io::Error::other("injected publication cut")) }
        else { Ok(()) }
    }
}

#[test]
fn each_publication_cut_stops_the_owner_and_restart_uses_actual_authority() {
    for point in points() {
        let (_directory, root, owner, client) = fixture(Cut { point }, policy());
        let first = lease(&client, 0);
        for record in &first.records[..2] { accepted(&client, record.token); }
        assert_eq!(state(&client, 0).persisted_next, offset(0));
        assert!(matches!(receive(client.try_checkpoint(group(0)).unwrap()), Err(RequestError::Failed(_))));
        assert!(owner.join().is_err()); drop(client);
        let replaced = matches!(point, PublicationStep::Renamed | PublicationStep::DirectorySynced);
        let expected = if replaced { 2 } else { 0 };
        assert_eq!(stored(&root, 0, 0).next, offset(expected));
        let (owner, client) = start(recovery::recover(&root).unwrap(), FileSync, policy(), 1);
        assert_eq!(state(&client, 0).persisted_next, offset(expected));
        assert_eq!(lease(&client, 0).records[0].token.offset, offset(expected));
        assert!(!root.join(checkpoint::pending_name(partition(0), group(0))).exists());
        owner.join().unwrap();
    }
}

struct Gate {
    reached: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}
impl SyncIo for Gate {
    fn sync(&mut self, _: Offset, file: &mut dyn StorageFile) -> io::Result<()> { file.sync() }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        if is_group_zero(target) && step == PublicationStep::DirectorySynced {
            self.reached.send(()).map_err(io::Error::other)?;
            self.release.recv_timeout(Duration::from_secs(5)).map_err(io::Error::other)?;
        }
        Ok(())
    }
}

#[test]
fn checkpoint_reply_is_withheld_until_the_entire_publication_returns() {
    let (reached, event) = mpsc::channel(); let (release, wait) = mpsc::channel();
    let (_directory, root, owner, client) = fixture(Gate { reached, release: wait }, policy());
    let first = lease(&client, 0); accepted(&client, first.records[0].token);
    let ticket = client.try_checkpoint(group(0)).unwrap();
    event.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(stored(&root, 0, 0).next, offset(1));
    assert!(matches!(ticket.try_recv(), Err(mpsc::TryRecvError::Empty)));
    release.send(()).unwrap();
    assert_eq!(receive(ticket).unwrap().progress.persisted_next, offset(1));
    owner.join().unwrap();
}

struct FailSync { directory: bool }
impl SyncIo for FailSync {
    fn sync(&mut self, _: Offset, file: &mut dyn StorageFile) -> io::Result<()> { file.sync() }
    fn sync_metadata(&mut self, file: &mut dyn StorageFile) -> io::Result<()> {
        if self.directory { file.sync() } else { Err(io::Error::other("checkpoint file sync failed")) }
    }
    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        if self.directory && path.join(META_NAME).exists() { Err(io::Error::other("checkpoint directory sync failed")) }
        else { File::open(path)?.sync_all() }
    }
}

#[test]
fn file_and_directory_sync_failures_do_not_report_checkpoint_success() {
    for directory in [false, true] {
        let (_directory, root, owner, client) = fixture(FailSync { directory }, policy());
        let first = lease(&client, 0); accepted(&client, first.records[0].token);
        assert!(matches!(receive(client.try_checkpoint(group(0)).unwrap()), Err(RequestError::Failed(_))));
        assert!(owner.join().is_err()); drop(client);
        assert_eq!(stored(&root, 0, 0).next, offset(if directory { 1 } else { 0 }));
        drop(recovery::recover(&root).unwrap());
    }
}

struct Observe { event: mpsc::Sender<()> }
impl SyncIo for Observe {
    fn sync(&mut self, _: Offset, file: &mut dyn StorageFile) -> io::Result<()> { file.sync() }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        if is_group_zero(target) && step == PublicationStep::DirectorySynced {
            self.event.send(()).map_err(io::Error::other)?;
        }
        Ok(())
    }
}

#[test]
fn an_idle_owner_checkpoints_on_its_deadline_without_another_request() {
    let (event, occurred) = mpsc::channel();
    let (_directory, root, owner, client) = fixture(Observe { event },
        CheckpointPolicy::new(Duration::from_millis(10)).unwrap());
    let first = lease(&client, 0); let response = accepted(&client, first.records[0].token);
    assert_eq!(response.progress.persisted_next, offset(0));
    occurred.recv_timeout(Duration::from_secs(5)).expect("automatic checkpoint watchdog");
    assert_eq!(state(&client, 0).persisted_next, offset(1));
    assert_eq!(stored(&root, 0, 0).next, offset(1));
    owner.join().unwrap();
}

struct FailRecoveredSync;
impl SyncIo for FailRecoveredSync {
    fn sync(&mut self, _: Offset, file: &mut dyn StorageFile) -> io::Result<()> { file.sync() }
    fn sync_checkpoint(&mut self, _: &mut dyn StorageFile) -> io::Result<()> {
        Err(io::Error::other("recovered checkpoint sync failed"))
    }
}

#[test]
fn recovered_checkpoint_sync_failure_prevents_a_new_session_epoch() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    owner.join().unwrap(); drop(client);
    assert!(recovery::recover_with(&root, &mut FailRecoveredSync, &mut |_| Ok(())).is_err());
    assert_eq!(metadata::read(&root).unwrap().epoch().get(), 1);
    assert_eq!(recovery::recover(&root).unwrap().epoch().get(), 2);
}

#[test]
fn checkpointing_changes_metadata_not_record_bytes_or_other_groups() {
    let (_directory, root, owner, client) = fixture(FileSync, policy());
    let paths = [0, 2].map(|base| root.join("partition-0").join(segment_name(offset(base))));
    let before = paths.each_ref().map(|p| fs::read(p).unwrap());
    let first = lease(&client, 0); for record in &first.records { accepted(&client, record.token); }
    assert_eq!(checkpoint(&client, 0), 4);
    for (p, bytes) in paths.iter().zip(before) { assert_eq!(fs::read(p).unwrap(), bytes); }
    assert_eq!(stored(&root, 0, 1).next, offset(0));
    assert_eq!(stored(&root, 1, 0).next, offset(0));
    owner.join().unwrap();
}
