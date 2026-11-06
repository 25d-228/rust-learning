mod common;

use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use broker_core::budget::DiskQuota;
use broker_core::codec;
use broker_core::delivery::{LeaseBatch, LeaseConfig, LeaseToken};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::{CheckpointPolicy, ProgressAck};
use broker_core::io::StorageFile;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::checkpoint;
use broker_core::storage::error::StorageError;
use broker_core::storage::log::segment_name;
use broker_core::storage::metadata::PublicationStep;
use broker_core::storage::recovery::{self, StorageSession};
use broker_core::storage::retention::{self, Retention};
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{Epoch, GroupId, Offset, PartitionId};
use broker_core::worker::{BatchConfig, Client, RequestError, Ticket, WorkerHandle};
use common::TestDir;

fn p(n: u16) -> PartitionId { PartitionId::new(n).unwrap() }
fn g(n: u8) -> GroupId { GroupId::new(n).unwrap() }
fn receive<T>(ticket: Ticket<T>) -> Result<T, RequestError> {
    ticket.recv_timeout(Duration::from_secs(5)).expect("owner reply watchdog")
}
fn get<T, E: Debug>(request: Result<Ticket<T>, E>) -> T { receive(request.unwrap()).unwrap() }
fn start<S: SyncIo + Send + 'static>(session: StorageSession, sync: S, slots: usize)
    -> (WorkerHandle, Client)
{
    let (epoch, mut logs) = session.into_parts();
    let log = logs.remove(0); drop(logs);
    let owner = WorkerHandle::start_with_progress(log, sync,
        BatchConfig::new(1, 4096, Duration::from_secs(60)).unwrap(), slots, 8,
        LeaseConfig::new(epoch, 8, 10_000).unwrap(), Arc::new(ManualClock::new(0)),
        CheckpointPolicy::explicit_only()).unwrap();
    let client = owner.client(); (owner, client)
}
fn fixture<S: SyncIo + Send + 'static>(sync: S, cap: u64, count: usize, data: u64, records: u64)
    -> (TestDir, PathBuf, WorkerHandle, Client)
{
    let directory = TestDir::new().unwrap(); let root = directory.path().join("broker");
    let session = recovery::initialize_budgeted(&root, StorageConfig::new(cap, 2, count).unwrap(),
        DiskQuota::new(data, 416).unwrap()).unwrap();
    let (owner, client) = start(session, sync, 8);
    for n in 0..records { assert_eq!(get(client.try_publish(b"cat".to_vec())).get(), n); }
    (directory, root, owner, client)
}
fn lease(client: &Client, group: u8) -> LeaseBatch {
    get(client.try_lease(g(group), ReadLimits::new(8, 4096).unwrap()))
}
fn ack(client: &Client, token: LeaseToken) {
    assert!(matches!(get(client.try_ack_progress(token.group, token)), ProgressAck::Accepted(_)));
}
fn complete(client: &Client, group: u8) -> LeaseBatch {
    let batch = lease(client, group);
    for record in &batch.records { ack(client, record.token); }
    batch
}
fn persist(client: &Client, group: u8) -> u64 {
    get(client.try_checkpoint(g(group))).progress.persisted_next.get()
}
fn persist_both(client: &Client) {
    for group in 0..2 { complete(client, group); persist(client, group); }
}
fn seg(root: &Path, base: u64) -> PathBuf { root.join("partition-0").join(segment_name(Offset::new(base))) }
fn floor(root: &Path) -> u64 { retention::read(root, p(0)).unwrap().floor.get() }
fn reseal(raw: &mut [u8; 48]) {
    let crc = crc32fast::hash(&raw[..44]); raw[44..].copy_from_slice(&crc.to_be_bytes());
}

#[test]
fn retained_floor_has_an_independent_golden_encoding() {
    let value = Retention { partition: p(2), epoch: Epoch::new(7).unwrap(), floor: Offset::new(19),
        quota: DiskQuota::new(236, 416).unwrap() };
    let golden = [
        0x50, 0x42, 0x52, 0x46, 0x00, 0x01, 0x00, 0x30,
        0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0xec, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0xa0, 0xd3, 0x2b, 0x8f, 0xcb,
    ];
    assert_eq!(value.encode(), golden);
    assert_eq!(Retention::decode(&golden).unwrap(), value);
}

#[test]
fn floor_decoder_refuses_bad_lengths_crc_scope_and_quotas() {
    let good = Retention { partition: p(0), epoch: Epoch::new(1).unwrap(), floor: Offset::new(0),
        quota: DiskQuota::new(236, 416).unwrap() }.encode();
    for size in 0..48 { assert!(Retention::decode(&good[..size]).is_err()); }
    let mut extra = good.to_vec(); extra.push(0); assert!(Retention::decode(&extra).is_err());
    for byte in 0..48 {
        let mut bad = good; bad[byte] ^= 1; assert!(Retention::decode(&bad).is_err());
    }
    for field in 0..7 {
        let mut bad = good;
        match field {
            0 => bad[0] = b'X', 1 => bad[5] = 2, 2 => bad[9] = 4,
            3 => bad[10] = 1, 4 => bad[12..20].fill(0),
            5 => bad[28..36].fill(0), 6 => bad[36..44].fill(0), _ => unreachable!(),
        }
        reseal(&mut bad); assert!(Retention::decode(&bad).is_err());
    }
}

#[test]
fn initial_authorities_persist_fixed_quotas_and_zero_floors() {
    let (_d, root, owner, client) = fixture(FileSync, 102, 4, 236, 0);
    for n in 0..4 {
        let value = retention::read(&root, p(n)).unwrap();
        assert_eq!((value.floor.get(), value.quota.data_bytes(), value.quota.reserve_bytes()), (0, 236, 416));
    }
    assert_eq!(client.disk_state().unwrap().file_bytes, 32);
    owner.join().unwrap();
}

#[test]
fn volatile_completion_and_interior_checkpoints_do_not_delete_whole_segments() {
    let (_d, root, owner, client) = fixture(FileSync, 102, 4, 236, 4);
    complete(&client, 0); assert_eq!(persist(&client, 0), 4);
    let second = lease(&client, 1); ack(&client, second.records[0].token);
    assert_eq!(get(client.try_retention()).removed_segments, 0);
    assert_eq!(persist(&client, 1), 1);
    assert_eq!(get(client.try_retention()).removed_segments, 0);
    assert!(seg(&root, 0).exists());
    ack(&client, second.records[1].token); assert_eq!(persist(&client, 1), 2);
    let retained = get(client.try_retention());
    assert_eq!((retained.floor.get(), retained.removed_bytes, retained.removed_segments), (2, 102, 1));
    assert!(!retained.rotated); assert!(!seg(&root, 0).exists()); assert!(seg(&root, 2).exists());
    assert_eq!(retained.disk.file_bytes, 102);
    owner.join().unwrap();
}

#[test]
fn full_data_quota_still_allows_acknowledgement_checkpoint_and_retention() {
    let (_d, _root, owner, client) = fixture(FileSync, 102, 4, 236, 4);
    let rejected = client.try_publish(b"owl".to_vec()).unwrap_err();
    assert_eq!(rejected.reason, AdmissionError::DiskFull); assert_eq!(rejected.value, b"owl");
    complete(&client, 0); persist(&client, 0);
    assert_eq!(get(client.try_retention()).floor.get(), 0); // Group 1 is still stalled.
    complete(&client, 1); persist(&client, 1);
    let retained = get(client.try_retention());
    assert_eq!((retained.floor.get(), retained.disk.file_bytes), (4, 32));
    assert_eq!(get(client.try_publish(rejected.value)).get(), 4);
    owner.join().unwrap();
}

#[test]
fn a_fully_consumed_active_file_is_rotated_not_unlinked_as_active() {
    let (_d, root, owner, client) = fixture(FileSync, 67, 2, 134, 2);
    assert_eq!(client.disk_state().unwrap().file_bytes, 134);
    persist_both(&client);
    let retained = get(client.try_retention());
    assert!(retained.rotated);
    assert_eq!((retained.floor.get(), retained.removed_segments, retained.removed_bytes), (2, 2, 134));
    assert_eq!(retained.disk.file_bytes, 32);
    assert!(seg(&root, 2).exists()); assert!(!seg(&root, 0).exists()); assert!(!seg(&root, 1).exists());
    assert_eq!(get(client.try_publish(b"dog".to_vec())).get(), 2);
    owner.join().unwrap();
}

#[test]
fn owned_response_bytes_survive_deletion_and_old_fetches_are_refused() {
    let (_d, _root, owner, client) = fixture(FileSync, 102, 4, 236, 4);
    let held = complete(&client, 0); persist(&client, 0);
    complete(&client, 1); persist(&client, 1); get(client.try_retention());
    assert_eq!(held.records[0].record.payload(), b"cat");
    match receive(client.try_fetch(Offset::new(0), ReadLimits::new(1, 35).unwrap()).unwrap()) {
        Err(RequestError::Refused(error)) => assert!(matches!(*error, StorageError::Range { .. })),
        other => panic!("unexpected old fetch: {other:?}"),
    }
    assert_eq!(get(client.try_snapshot()).visible_end.get(), 4);
    owner.join().unwrap();
}

#[test]
fn restart_rebuilds_at_the_authoritative_floor_and_never_reuses_offsets() {
    let (_d, root, owner, client) = fixture(FileSync, 102, 4, 236, 4);
    persist_both(&client); get(client.try_retention()); owner.join().unwrap(); drop(client);
    let (owner, client) = start(recovery::recover(&root).unwrap(), FileSync, 8);
    assert_eq!(floor(&root), 4); assert_eq!(client.disk_state().unwrap().file_bytes, 32);
    for group in 0..2 { assert_eq!(get(client.try_progress(g(group))).persisted_next.get(), 4); }
    assert!(lease(&client, 0).records.is_empty());
    assert_eq!(get(client.try_publish(b"dog".to_vec())).get(), 4);
    owner.join().unwrap();
}

#[test]
fn retaining_an_already_empty_active_tail_is_a_clean_noop() {
    let (_d, root, owner, client) = fixture(FileSync, 102, 4, 236, 4);
    persist_both(&client); get(client.try_retention());
    let before = fs::read(root.join(retention::name(p(0)))).unwrap();
    let result = get(client.try_retention());
    assert_eq!((result.before.get(), result.floor.get(), result.removed_segments), (4, 4, 0));
    assert!(!result.rotated);
    assert_eq!(fs::read(root.join(retention::name(p(0)))).unwrap(), before);
    owner.join().unwrap();
}

#[derive(Clone, Copy)]
enum Cut { Floor(PublicationStep), BeforeUnlink(usize), AfterUnlink(usize), DeleteSync, RotationSync(u64) }
struct Fault { cut: Cut, removed: usize }
impl SyncIo for Fault {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        if matches!(self.cut, Cut::RotationSync(n) if n == base.get()) {
            return Err(io::Error::other("new active sync failed"));
        }
        file.sync()
    }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        if target.file_name() == Some(OsStr::new("retention-p0.floor"))
            && matches!(self.cut, Cut::Floor(point) if point == step)
        { return Err(io::Error::other("retained floor publication interrupted")); }
        Ok(())
    }
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        let next = self.removed + 1;
        if matches!(self.cut, Cut::BeforeUnlink(n) if n == next) {
            return Err(io::Error::other("unlink failed before removal"));
        }
        fs::remove_file(path)?; self.removed = next;
        if matches!(self.cut, Cut::AfterUnlink(n) if n == next) {
            return Err(io::Error::other("interrupted after unlink"));
        }
        Ok(())
    }
    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        if self.removed > 0 && matches!(self.cut, Cut::DeleteSync) {
            return Err(io::Error::other("deletion directory sync failed"));
        }
        File::open(path)?.sync_all()
    }
}
fn interrupted(cut: Cut) -> (TestDir, PathBuf) {
    let (d, root, owner, client) = fixture(Fault { cut, removed: 0 }, 102, 4, 236, 4);
    persist_both(&client);
    assert!(receive(client.try_retention().unwrap()).is_err());
    assert!(owner.join().is_err()); assert!(client.disk_state().unwrap().stopped);
    assert_eq!(client.try_publish(b"dog".to_vec()).unwrap_err().reason, AdmissionError::Closed);
    drop(client); (d, root)
}

#[test]
fn each_floor_publication_cut_recovers_only_the_visible_authority() {
    for (point, expected) in [(PublicationStep::TemporaryCreated, 0), (PublicationStep::TemporaryWritten, 0),
        (PublicationStep::TemporarySynced, 0), (PublicationStep::Renamed, 4), (PublicationStep::DirectorySynced, 4)]
    {
        let (_d, root) = interrupted(Cut::Floor(point));
        assert_eq!(floor(&root), expected); assert!(seg(&root, 0).exists());
        let session = recovery::recover(&root).unwrap();
        assert_eq!(session.epoch().get(), 2);
        let (_, logs) = session.into_parts(); assert_eq!(logs[0].retained_floor().get(), expected);
        assert_eq!(seg(&root, 0).exists(), expected == 0);
        assert!(!root.join(retention::pending_name(p(0))).exists());
    }
}

#[test]
fn every_interrupted_deletion_suffix_finishes_on_restart() {
    for cut in [Cut::BeforeUnlink(1), Cut::AfterUnlink(1), Cut::BeforeUnlink(2),
        Cut::AfterUnlink(2), Cut::DeleteSync]
    {
        let (_d, root) = interrupted(cut); assert_eq!(floor(&root), 4);
        let (_, logs) = recovery::recover(&root).unwrap().into_parts();
        assert_eq!(logs[0].retained_floor().get(), 4);
        assert_eq!(logs[0].disk_state().unwrap().file_bytes, 32);
        assert!(!seg(&root, 0).exists()); assert!(!seg(&root, 2).exists()); assert!(seg(&root, 4).exists());
    }
}

#[test]
fn the_reserved_extra_header_and_count_slot_survive_interrupted_rotation() {
    let (_d, root, owner, client) = fixture(Fault { cut: Cut::RotationSync(2), removed: 0 }, 67, 2, 134, 2);
    persist_both(&client); assert!(receive(client.try_retention().unwrap()).is_err());
    assert!(owner.join().is_err()); drop(client);
    assert_eq!(floor(&root), 0); assert!(seg(&root, 2).exists());
    let (owner, client) = start(recovery::recover(&root).unwrap(), FileSync, 8);
    assert_eq!(client.disk_state().unwrap().file_bytes, 166);
    assert_eq!(client.try_publish(b"cat".to_vec()).unwrap_err().reason, AdmissionError::DiskFull);
    let result = get(client.try_retention()); assert!(!result.rotated);
    assert_eq!((result.floor.get(), result.disk.file_bytes), (2, 32));
    assert_eq!(get(client.try_publish(b"cat".to_vec())).get(), 2);
    owner.join().unwrap();
}

#[test]
fn an_extra_nonempty_segment_is_not_a_maintenance_tail() {
    let (_d, root, owner, client) = fixture(Fault { cut: Cut::RotationSync(2), removed: 0 }, 67, 2, 134, 2);
    persist_both(&client); assert!(receive(client.try_retention().unwrap()).is_err());
    assert!(owner.join().is_err()); drop(client);
    OpenOptions::new().append(true).open(seg(&root, 2)).unwrap()
        .write_all(&codec::encode_record(Offset::new(2), b"cat").unwrap()).unwrap();
    assert!(recovery::recover(&root).is_err()); assert!(seg(&root, 0).exists());
}

#[test]
fn obsolete_prefix_gaps_are_allowed_but_a_missing_floor_segment_is_not() {
    let (_d, root) = interrupted(Cut::Floor(PublicationStep::Renamed));
    fs::remove_file(seg(&root, 2)).unwrap(); // Arbitrary already-deleted obsolete subset.
    let session = recovery::recover(&root).unwrap(); drop(session);
    assert!(!seg(&root, 0).exists());
    fs::remove_file(seg(&root, 4)).unwrap();
    assert!(recovery::recover(&root).is_err());
}

#[test]
fn a_missing_retained_interior_segment_is_not_treated_as_cleanup() {
    let (_d, root, owner, client) = fixture(FileSync, 67, 4, 400, 4);
    owner.join().unwrap(); drop(client);
    fs::remove_file(seg(&root, 1)).unwrap();
    assert!(recovery::recover(&root).is_err()); assert!(seg(&root, 0).exists());
}

#[test]
fn obsolete_corruption_is_discarded_but_retained_corruption_refuses_startup() {
    let (_d, root) = interrupted(Cut::Floor(PublicationStep::Renamed));
    fs::write(seg(&root, 0), b"obsolete").unwrap();
    let session = recovery::recover(&root).unwrap(); drop(session);
    assert!(!seg(&root, 0).exists());
    let mut bytes = fs::read(seg(&root, 4)).unwrap(); bytes[0] ^= 1;
    fs::write(seg(&root, 4), &bytes).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::read(seg(&root, 4)).unwrap(), bytes);
}

#[test]
fn missing_or_corrupt_floor_authority_never_promotes_a_candidate() {
    for corrupt in [false, true] {
        let (_d, root, owner, client) = fixture(FileSync, 102, 4, 236, 0);
        owner.join().unwrap(); drop(client);
        let current = root.join(retention::name(p(0))); let pending = root.join(retention::pending_name(p(0)));
        fs::copy(&current, &pending).unwrap();
        if corrupt { fs::write(&current, b"broken").unwrap(); } else { fs::remove_file(&current).unwrap(); }
        assert!(recovery::recover(&root).is_err()); assert!(pending.exists());
        assert_eq!(current.exists(), corrupt);
    }
}

#[test]
fn scope_future_epoch_nonboundary_floor_and_lowered_quota_are_refused() {
    let (_d, root, owner, client) = fixture(FileSync, 102, 4, 236, 4);
    owner.join().unwrap(); drop(client);
    let good = retention::read(&root, p(0)).unwrap();
    for bad in [Retention { partition: p(1), ..good }, Retention { epoch: Epoch::new(2).unwrap(), ..good },
        Retention { floor: Offset::new(1), ..good }, Retention { quota: DiskQuota::new(96, 416).unwrap(), ..good }]
    {
        fs::write(root.join(retention::name(p(0))), bad.encode()).unwrap();
        assert!(recovery::recover(&root).is_err()); assert!(seg(&root, 0).exists());
    }
}

#[test]
fn checkpoint_below_floor_refuses_startup_without_deleting_leftovers() {
    let (_d, root) = interrupted(Cut::Floor(PublicationStep::Renamed));
    let mut bad = checkpoint::read(&root, p(0), g(1)).unwrap(); bad.next = Offset::new(3);
    fs::write(root.join(checkpoint::name(p(0), g(1))), bad.encode()).unwrap();
    assert!(recovery::recover(&root).is_err()); assert!(seg(&root, 0).exists());
}

#[test]
fn all_authorities_are_validated_before_any_repair_or_obsolete_unlink() {
    let (_d, root) = interrupted(Cut::Floor(PublicationStep::Renamed));
    let tail = root.join("partition-1").join(segment_name(Offset::new(0)));
    OpenOptions::new().append(true).open(&tail).unwrap()
        .write_all(&codec::encode_record(Offset::new(0), b"cat").unwrap()[..34]).unwrap();
    let before = fs::read(&tail).unwrap();
    let mut bad = checkpoint::read(&root, p(3), g(1)).unwrap(); bad.next = Offset::new(1);
    fs::write(root.join(checkpoint::name(p(3), g(1))), bad.encode()).unwrap();
    assert!(recovery::recover(&root).is_err());
    assert_eq!(fs::read(&tail).unwrap(), before); assert!(seg(&root, 0).exists());
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

#[test]
fn mailbox_rejection_releases_disk_but_abandoned_queued_replies_do_not() {
    let d = TestDir::new().unwrap(); let root = d.path().join("broker");
    let session = recovery::initialize_budgeted(&root, StorageConfig::new(102, 2, 4).unwrap(),
        DiskQuota::new(600, 416).unwrap()).unwrap();
    let (entered, observed) = mpsc::sync_channel(1); let (release, gate) = mpsc::channel();
    let (owner, client) = start(session, Gate { entered, release: Some(gate) }, 1);
    let first = client.try_publish(b"cat".to_vec()).unwrap();
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(client.try_publish(b"dog".to_vec()).unwrap()); // Queued command still owns 67 bytes.
    assert_eq!(client.disk_state().unwrap().reserved_bytes, 67);
    let refusal = client.try_publish(b"owl".to_vec()).unwrap_err();
    assert_eq!(refusal.reason, AdmissionError::Full); assert_eq!(refusal.value, b"owl");
    assert_eq!(client.disk_state().unwrap().reserved_bytes, 67);
    let control = client.try_checkpoint(g(0)).unwrap(); // Separate existing lane; no special reserved ack slot yet.
    release.send(()).unwrap(); receive(first).unwrap(); receive(control).unwrap();
    owner.join().unwrap();
    assert_eq!((client.disk_state().unwrap().file_bytes, client.disk_state().unwrap().reserved_bytes), (102, 0));
}

#[test]
fn owner_preflight_refusal_and_invalid_record_admission_leave_no_reservations() {
    let (_d, _root, owner, client) = fixture(FileSync, 67, 2, 400, 0);
    let rejected = receive(client.try_publish(vec![0; 36]).unwrap()); // 68 encoded bytes cannot fit cap 67.
    assert!(matches!(rejected, Err(RequestError::Refused(_))));
    assert_eq!(client.disk_state().unwrap().reserved_bytes, 0);
    assert_eq!(client.disk_state().unwrap().file_bytes, 32);
    let rejected = client.try_publish(vec![0; codec::MAX_PAYLOAD + 1]).unwrap_err();
    assert_eq!(rejected.reason, AdmissionError::InvalidRecord);
    assert_eq!(client.disk_state().unwrap().reserved_bytes, 0);
    owner.join().unwrap();
}

#[test]
fn fixed_partition_quotas_do_not_borrow_from_idle_neighbors() {
    let d = TestDir::new().unwrap(); let root = d.path().join("broker");
    let (epoch, logs) = recovery::initialize_budgeted(&root, StorageConfig::new(67, 2, 2).unwrap(),
        DiskQuota::new(134, 416).unwrap()).unwrap().into_parts();
    let mut owners = Vec::new();
    for log in logs {
        owners.push(WorkerHandle::start_with_progress(log, FileSync,
            BatchConfig::new(1, 4096, Duration::from_secs(1)).unwrap(), 4, 4,
            LeaseConfig::new(epoch, 4, 10000).unwrap(), Arc::new(ManualClock::new(0)),
            CheckpointPolicy::explicit_only()).unwrap());
    }
    let hot = owners[0].client(); let cool = owners[1].client();
    get(hot.try_publish(b"cat".to_vec())); get(hot.try_publish(b"dog".to_vec()));
    assert_eq!(hot.try_publish(b"owl".to_vec()).unwrap_err().reason, AdmissionError::DiskFull);
    assert_eq!(get(cool.try_publish(b"owl".to_vec())).get(), 0);
    for owner in owners { owner.join().unwrap(); }
}

#[path = "pressure/memory.rs"]
mod memory_pressure;
