# Chapter 14 — Safe retention and disk admission
## Part 4 — Failure cuts and recovery checks

> **Learning objectives**
> Check reservation ownership independently of mailbox occupancy. Interrupt real floor publication and deletion at named boundaries. Prove that restart preserves the authoritative retained interval, and distinguish these checks from filesystem power-loss claims.

**Implementation time:** the final portion of the 4.5-hour Chapter 14 estimate in [Part 1](https://25d-228.github.io/rust-learning/chapter-14.html). Complete [Part 2](https://25d-228.github.io/rust-learning/chapter-14-part-02.html) and [Part 3](https://25d-228.github.io/rust-learning/chapter-14-part-03.html) first. This part adds the nine budget unit tests, 23 pressure integration tests, Rust process probe, and Python parent harness.

**Execution boundary:** the supplied Chapter 14 suite contains 32 new tests, bringing the cumulative workspace to 313 supplied tests. Rust tooling was unavailable in the authoring environment; these counts are not observed passes. The Python harness can be syntax-checked without running its Rust child, but that is not execution of the crash test. Run the commands below on the native development machine and retain actual results separately from expected behavior.

## Check the ledger before involving the filesystem

The budget tests isolate arithmetic, permit identity, and cleanup. A normal refusal does not mutate counters. Settlement replaces reserved worst-case growth with actual growth. Ordinary unwinding drops unspent permits. The maintenance allowance admits one extra header, not an arbitrary draw on metadata headroom.

The concurrent test starts 12 contenders together. Its successful permits stay alive inside their thread results until every thread has finished. Dropping each permit immediately in its own thread would test repeated reuse, not simultaneous reservation pressure, and could legitimately admit more than the intended number over time.

## Quota validation, reservation arithmetic, and unwinding

Create this child module next to `budget.rs`. Its helper constructs a real ledger at a specified live length; it is not an alternate algorithm. The unwind fixture intentionally panics inside `catch_unwind`, then checks that normal cleanup returned the reservation. A panic-hook message during this test is expected even when the assertion passes.

```rust title="crates/broker-core/src/budget/tests.rs"
use super::*;
use std::sync::{Arc, Barrier};
use std::thread;

fn budget(data: u64, live: u64) -> DiskBudget {
    DiskBudget::new(DiskQuota::new(data, 416).unwrap(), live).unwrap()
}

#[test]
fn quota_checks_minimum_headroom_and_total_overflow() {
    assert!(DiskQuota::new(95, 416).is_err());
    assert!(DiskQuota::new(96, 415).is_err());
    assert!(DiskQuota::new(u64::MAX, 416).is_err());
    assert_eq!(4 * DiskQuota::default().total_bytes(), 512 * 1024 * 1024);
}

#[test]
fn admission_counts_live_and_reserved_bytes_together() {
    let b = budget(166, 32);
    let first = b.reserve_append(35).unwrap();
    let second = b.reserve_append(35).unwrap();
    assert_eq!(b.state().reserved_bytes, 134);
    assert!(matches!(b.reserve_append(35), Err(DiskRefusal::Full)));
    assert!(b.state().charged_upper_bound().unwrap() <= b.state().quota.total_bytes());
    drop(first); drop(second);
    assert_eq!(b.state().reserved_bytes, 0);
}

#[test]
fn settlement_converts_only_actual_growth_to_live_bytes() {
    let b = budget(200, 32);
    b.reserve_append(35).unwrap().settle(35);
    assert_eq!((b.state().file_bytes, b.state().reserved_bytes), (67, 0));
    b.reserve_append(35).unwrap().settle(67);
    assert_eq!((b.state().file_bytes, b.state().reserved_bytes), (134, 0));
}

#[test]
fn ordinary_unwinding_returns_an_unsettled_reservation() {
    let b = budget(200, 32); let inner = b.clone();
    let result = std::panic::catch_unwind(move || {
        let _permit = inner.reserve_append(35).unwrap();
        panic!("before handoff");
    });
    assert!(result.is_err());
    assert_eq!(b.state().reserved_bytes, 0);
    assert!(b.reserve_append(35).is_ok());
}

```

## Maintenance recovery and terminal state

The next tests separate the normal data cap from the emergency header, exercise a recovered ledger already above normal capacity, and ensure `stop` closes only new admission. Existing permits can still settle or drop while draining. A stopped snapshot deliberately has no trustworthy charged upper bound.

```rust title="crates/broker-core/src/budget/tests.rs"
#[test]
fn maintenance_can_use_one_header_but_not_metadata_capacity() {
    let b = budget(134, 134);
    assert!(matches!(b.reserve_append(32), Err(DiskRefusal::Full)));
    b.reserve_rotation(true).unwrap().settle(32);
    assert_eq!(b.state().file_bytes, 166);
    assert!(matches!(b.reserve_rotation(true), Err(DiskRefusal::Full)));
    b.reclaimed(134);
    assert!(b.reserve_append(35).is_ok());
}

#[test]
fn recovered_maintenance_header_is_charged_before_admission() {
    let b = budget(134, 166);
    assert!(matches!(b.reserve_append(32), Err(DiskRefusal::Full)));
    assert!(DiskBudget::new(DiskQuota::new(134, 416).unwrap(), 167).is_err());
}

#[test]
fn stop_closes_new_admission_but_existing_permits_still_settle_or_drop() {
    let b = budget(200, 32);
    let written = b.reserve_append(35).unwrap();
    let rejected = b.reserve_append(35).unwrap();
    b.stop();
    assert!(matches!(b.reserve_append(35), Err(DiskRefusal::Stopped)));
    written.settle(35); drop(rejected);
    assert_eq!((b.state().file_bytes, b.state().reserved_bytes), (67, 0));
    assert!(b.state().stopped);
    assert_eq!(b.state().charged_upper_bound(), None);
}

```

## Identity and simultaneous contenders

Equal quota values do not authorize cross-partition settlement. The final test keeps successful owned permits alive across the full contention interval. The barrier coordinates an attempted start; it does not impose a deterministic winning thread order, and the test does not assert one.

```rust title="crates/broker-core/src/budget/tests.rs"
#[test]
fn permits_are_scoped_to_one_ledger_not_equal_numeric_quotas() {
    let a = budget(200, 32); let b = budget(200, 32);
    let permit = a.reserve_append(35).unwrap();
    assert!(permit.belongs_to(&a)); assert!(!permit.belongs_to(&b));
}

#[test]
fn racing_reservations_cannot_oversubscribe_the_remaining_bytes() {
    let b = budget(233, 32); // Exactly three 67-byte reservations.
    let gate = Arc::new(Barrier::new(12));
    let mut handles = Vec::new();
    for _ in 0..12 {
        let b = b.clone(); let gate = Arc::clone(&gate);
        handles.push(thread::spawn(move || { gate.wait(); b.reserve_append(35) }));
    }
    // Successful permits remain owned by the JoinHandle result, then by this vector.
    let permits: Vec<_> = handles.into_iter().filter_map(|h| h.join().unwrap().ok()).collect();
    assert_eq!(permits.len(), 3);
    assert_eq!(b.state().reserved_bytes, 201);
    drop(permits); assert_eq!(b.state().reserved_bytes, 0);
}
```

Run the focused budget suite after all files are in place:

```sh
cargo test -p broker-core --lib budget::tests
```

Expected: nine selected tests pass. The library also contains prior tests that this filter does not run. Do not replace the full workspace run with only this filter.

## Build fixtures from the production startup path

Pressure tests must initialize real authority and start durable owners. Writing a few arbitrary files and attaching a fake floor would bypass the invariant under test. The helpers below use the real `initialize_budgeted`, leased delivery, persisted checkpoints, `FileSync`, and owner join paths.

`TestDir` is the complete helper already supplied in `crates/broker-core/tests/common/mod.rs` by Chapter 7. Keep that named prerequisite file; do not invent another cleanup policy. Other imports and local helpers are included here. Five-second receive timeouts are watchdogs, not proofs that a thread reached a state. Tests that require a particular state use a handshake.

## Production fixtures and exact floor bytes

Add `tests/pressure.rs`. The fixture uses a one-record commit batch so a successful append ticket establishes visibility. `complete` acknowledges actual tokens returned by the owner, and `persist` waits for actual checkpoint success. Resealing a deliberately malformed fixture distinguishes semantic validation from a mere checksum mismatch.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## Codec and initialized-authority checks

Compare against an independent literal 48-byte fixture, not `encode(decode(...))` alone. Invalid quota, reserved bytes, version, partition, and zero epoch cases are rechecksummed where necessary, so an implementation that validates only CRC cannot pass them. Initialization then verifies all four persisted quotas and zero floors.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## Whole-file safety and data-full progress

The first case distinguishes volatile progress, checkpoint progress inside a segment, and progress at a segment boundary. The next fills data capacity, confirms ownership-preserving producer rejection, then runs acknowledgement/checkpoint/retention controls. The fully consumed active-file case reaches both the normal byte cap and normal segment-count cap; reclamation must use only the explicit extra header and count slot.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## Owned reads, restart offsets, and clean no-ops

Keep a copied response across deletion, then request an old offset and require a healthy range refusal. Restart must rebuild from the authoritative floor and continue at the old exclusive end rather than reset offsets. Repeating retention on an empty active tail should neither create another empty file nor republish authority unnecessarily.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## Fail after actual operations, not instead of them

`Fault` can interrupt each completed floor publication step, fail before or after a numbered unlink, fail deletion-directory synchronization, or fail synchronization of a selected newly rotated segment. Its successful methods perform the real filesystem operation. The `interrupted` fixture persists both groups before asking retention to fail; owner join must report failure, not a successful deletion with a lost test marker.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## Publication and deletion cuts

Before replacement, startup must keep the old floor and its records. After replacement, it must honor the new floor and finish any obsolete suffix. Testing both sides of each unlink distinguishes “not deleted yet” from “deleted but no success returned.” No running owner is reused after an injected uncertain error.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## The maintenance exception must remain narrow

A failed rotation sync can leave the third empty file in a two-slot configuration. Recovery accepts exactly that bounded state, blocks ordinary append growth, and lets explicit retention remove the old prefix. A third file containing a record is not an emergency header and must be refused. These tests prevent the recovery exception from becoming an unbounded capacity bypass.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## Obsolete absence and retained loss are different

Deleting one obsolete file before startup is an allowed interruption state. Removing the file at the floor or a retained interior segment is not. Likewise, damaged bytes below a validated floor are no longer retained input, whereas damaged retained bytes must fail checksum validation. The tests intentionally exercise both sides rather than asserting that every corrupt file has the same meaning.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## Refuse missing or contradictory authority without cleanup

A valid candidate never substitutes for a missing or corrupt authority. Scoped identity, epoch, boundary existence, and persisted quota are checked independently. A checkpoint below the floor refuses startup before any obsolete file is removed. The cross-partition case additionally leaves an incomplete active tail elsewhere and confirms that invalid authority prevents both repair and cleanup.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## Observe queued reservations while synchronization is blocked

`Gate` reports that a real append reached the chosen synchronization boundary, then waits for release. While the owner is blocked, another append occupies the data lane with its permit. Dropping that second ticket must not release its reservation. A third mailbox refusal must release only its own just-acquired permit. A separately admitted checkpoint demonstrates control-lane independence; it does not execute until the storage gate opens.

```rust title="crates/broker-core/tests/pressure.rs"
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

```

## Healthy refusal and fixed partition ownership

An encoded record may be valid for the format but too large for a particular configured segment; the owner refuses it before mutation and returns its reservation. A payload above the format's `MAX_PAYLOAD` is rejected earlier as `InvalidRecord`. The last test fills one partition while another remains idle and confirms that quota is not implicitly transferred between them.

```rust title="crates/broker-core/tests/pressure.rs"
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
```

Run the focused integration target and then every target in the workspace:

```sh
cargo test -p broker-core --test pressure
cargo test --workspace --all-targets
```

Expected: 23 pressure tests and 313 tests across the supplied cumulative native workspace. Counts exclude the Python process harness and intentionally separate compiler-failure exercises in early chapters. Cargo may print multiple test binaries, each with its own count. Treat warnings, a compile error, a hung watchdog, or any failed test as observations to diagnose, not as permission to call this suite verified.

## A real process interruption at nine boundaries

Returned errors exercise failure handling while destructors still run. A process kill exercises a different path: no permit destructor, owner drain, or orderly file close is guaranteed to execute in that process. The operating system remains running. This is not a power-loss test and does not simulate loss of operating-system buffers, device caches, or filesystem reordering after power failure.

Use four three-byte records in two 102-byte files and persist both groups at 4. A replacement empty file at base 4 produces a file sum of 236. The probe parks at one of nine completed operations and announces `READY phase`. The parent waits for that exact line, kills the process, waits for termination, and starts a separate verifier.

| Phase | Completed event | Expected recovered floor | Expected recovered file bytes |
| --- | --- | ---: | ---: |
| `rotate` | Replacement active header synchronized | 0 | 236 |
| `created` | Floor candidate created | 0 | 236 |
| `written` | Floor candidate bytes written | 0 | 236 |
| `synced` | Floor candidate synchronized | 0 | 236 |
| `renamed` | Candidate replaced the floor authority | 4 | 32 |
| `directory` | Floor-authority directory synchronized | 4 | 32 |
| `unlink-1` | First obsolete file actually unlinked | 4 | 32 |
| `unlink-2` | Second obsolete file actually unlinked | 4 | 32 |
| `cleanup` | Deletion directory synchronized | 4 | 32 |

For the first four cases, recovery must not guess that a synchronized candidate or empty tail means the floor advanced. It keeps the old retained interval. The verifier then requests explicit retention, so *every* case ultimately reaches floor 4 and 32 file bytes. For the other five cases, recovery itself completes the obsolete-prefix cleanup.

## The Rust child owns the real operations

Add the complete probe below. Its helper starts durable partition 0 and drops the other unused startup logs while the owner still retains directory ownership. The `Gate` writes and flushes a marker before parking. A parked child is deliberately not a normal interactive demo; always run it through the parent harness so termination and cleanup are bounded.

```rust title="crates/broker-core/examples/retention_crash_probe.rs"
use std::error::Error;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use broker_core::budget::DiskQuota;
use broker_core::delivery::LeaseConfig;
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::{CheckpointPolicy, ProgressAck};
use broker_core::io::StorageFile;
use broker_core::storage::metadata::PublicationStep;
use broker_core::storage::recovery::{self, StorageSession};
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{GroupId, Offset};
use broker_core::worker::{BatchConfig, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(30))??)
}
fn start<S: SyncIo + Send + 'static>(session: StorageSession, sync: S)
    -> Result<WorkerHandle, Box<dyn Error>>
{
    let (epoch, mut logs) = session.into_parts();
    let log = logs.remove(0); drop(logs);
    Ok(WorkerHandle::start_with_progress(log, sync,
        BatchConfig::new(1, 4096, Duration::from_secs(1))?, 8, 8,
        LeaseConfig::new(epoch, 8, 10000)?, Arc::new(ManualClock::new(0)),
        CheckpointPolicy::explicit_only())?)
}
struct Gate { phase: String, removed: usize }
impl Gate {
    fn reached(&self, phase: &str) -> io::Result<()> {
        if self.phase == phase {
            println!("READY {phase}"); io::stdout().flush()?;
            // The parent kills the process. A timeout is a failed harness, not permission to continue.
            loop { std::thread::park(); }
        }
        Ok(())
    }
}
```

The gate delegates synchronization and removal to the real implementation before reporting the chosen completed boundary. Floor publication is recognized by its target filename, so checkpoint and broker-epoch publication do not accidentally trigger the gate. The rotation gate selects base 4, not an earlier group-commit sync.

```rust title="crates/broker-core/examples/retention_crash_probe.rs"
impl SyncIo for Gate {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        file.sync()?;
        if base.get() == 4 { self.reached("rotate")?; }
        Ok(())
    }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        if target.file_name() == Some(OsStr::new("retention-p0.floor")) {
            self.reached(match step {
                PublicationStep::TemporaryCreated => "created",
                PublicationStep::TemporaryWritten => "written",
                PublicationStep::TemporarySynced => "synced",
                PublicationStep::Renamed => "renamed",
                PublicationStep::DirectorySynced => "directory",
            })?;
        }
        Ok(())
    }
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)?; self.removed += 1;
        self.reached(&format!("unlink-{}", self.removed))
    }
    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()?;
        if self.removed > 0 { self.reached("cleanup")?; }
        Ok(())
    }
}
```

The `cut` mode creates the fixture, persists both groups, and enters retention. The separate `verify` mode recovers the directory, prints the actual epoch/floor/file bytes, completes explicit retention when still needed, and joins the owner. CLI validation keeps incorrect phases from producing a plausible but unrelated trace.

```rust title="crates/broker-core/examples/retention_crash_probe.rs"
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() < 3 { return Err(io::Error::other("usage: retention_crash_probe cut ROOT PHASE | verify ROOT").into()); }
    let root = Path::new(&args[2]);
    match args[1].to_str() {
        Some("cut") if args.len() == 4 => {
            let phase = args[3].to_str().ok_or_else(|| io::Error::other("phase must be UTF-8"))?;
            if !["rotate", "created", "written", "synced", "renamed", "directory", "unlink-1", "unlink-2", "cleanup"].contains(&phase) {
                return Err(io::Error::other("unknown retention gate").into());
            }
            let session = recovery::initialize_budgeted(root, StorageConfig::new(102, 2, 4)?, DiskQuota::new(236, 416)?)?;
            let owner = start(session, Gate { phase: phase.to_owned(), removed: 0 })?;
            let client = owner.client();
            for _ in 0..4 { request(client.try_publish(b"cat".to_vec()))?; }
            for group in [GroupId::new(0)?, GroupId::new(1)?] {
                let batch = request(client.try_lease(group, ReadLimits::new(4, 140)?))?;
                for record in batch.records {
                    assert!(matches!(request(client.try_ack_progress(group, record.token))?, ProgressAck::Accepted(_)));
                }
                request(client.try_checkpoint(group))?;
            }
            request(client.try_retention())?;
            return Err(io::Error::other("retention finished without entering its gate").into());
        }
        Some("verify") if args.len() == 3 => {
            let session = recovery::recover(root)?;
            let epoch = session.epoch();
            let owner = start(session, FileSync)?; let client = owner.client();
            let state = client.disk_state().unwrap();
            let value = broker_core::storage::retention::read(root, broker_core::types::PartitionId::new(0)?)?;
            println!("RECOVERED {} {} {}", epoch.get(), value.floor.get(), state.file_bytes);
            let retained = request(client.try_retention())?;
            assert_eq!((retained.floor.get(), retained.disk.file_bytes), (4, 32));
            println!("RECLAIMED 4 32"); owner.join()?;
        }
        _ => return Err(io::Error::other("invalid mode or argument count").into()),
    }
    Ok(())
}
```

## The Python parent proves the marker precedes termination

The parent uses a selector and a bounded line buffer rather than sleeping and assuming that the child reached a cut. A timeout or wrong marker fails the case. Standard error goes to a temporary file so an unread pipe cannot block the child; the reported error excerpt is bounded. Every exceptional path kills and waits for a still-running child.

The verifier's output must match both the intermediate recovered state and the final reclaimed state. The parent does not infer either value from a successful exit code alone.

Create the workspace-root harness with all imports and the marker reader. This harness targets the native Unix-like environment used by the course; it does not add a Windows process-control abstraction.

```python title="checks/check_retention_crash.py"
#!/usr/bin/env python3
"""Kill a real owner across rotation, floor publication, and obsolete-segment deletion."""
from __future__ import annotations
import argparse
import os
from pathlib import Path
import selectors
import signal
import subprocess
import tempfile
import time


def read_marker(process: subprocess.Popen[bytes], timeout: float = 10.0) -> str:
    assert process.stdout is not None
    end = time.monotonic() + timeout
    data = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while b"\n" not in data:
            remaining = end - time.monotonic()
            if remaining <= 0 or not selector.select(remaining):
                raise TimeoutError("child did not reach the retention gate")
            chunk = os.read(process.stdout.fileno(), 4096)
            if not chunk:
                raise RuntimeError(f"child exited before its gate: {process.poll()}")
            data.extend(chunk)
            if len(data) > 4096:
                raise RuntimeError("unexpectedly large marker output")
    return bytes(data).decode("utf-8").strip()


```

Each case owns one child and one fresh absent broker path. After receiving the exact marker, `kill` and `wait` establish termination before another process tries to acquire the directory. The verifier receives a separate timeout, and both its output lines are checked.

```python title="checks/check_retention_crash.py"
def check(binary: Path, root: Path, phase: str, expected: int) -> None:
    with tempfile.TemporaryFile(mode="w+b") as errors:
        process = subprocess.Popen([str(binary), "cut", str(root), phase],
            stdout=subprocess.PIPE, stderr=errors)
        try:
            marker = read_marker(process)
            if marker != f"READY {phase}":
                raise RuntimeError(f"wrong marker: {marker!r}")
            process.kill()  # SIGKILL on the native macOS/Unix target, not graceful shutdown.
            code = process.wait(timeout=5)
            if code != -signal.SIGKILL:
                raise RuntimeError(f"unexpected child status: {code}")
        except Exception:
            errors.seek(0)
            print(errors.read(16_384).decode("utf-8", errors="replace"))
            raise
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            if process.stdout is not None:
                process.stdout.close()
    verified = subprocess.run([str(binary), "verify", str(root)],
        check=True, capture_output=True, text=True, timeout=15)
    wanted = f"RECOVERED 2 {expected} {236 if expected == 0 else 32}\nRECLAIMED 4 32"
    if verified.stdout.strip() != wanted:
        raise AssertionError(f"{phase}: {verified.stdout!r}, expected {wanted!r}")
    print(f"{phase}: recovered_floor={expected}; final_floor=4; final_file_bytes=32; epoch=2")


```

The nine phases are explicit data, not guessed by searching for arbitrary log messages. The temporary parent directory contains independent fixtures; no phase inherits a newer epoch or a partially reclaimed prefix from another.

```python title="checks/check_retention_crash.py"
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if os.name != "posix":
        parser.error("this process-kill check targets native macOS/Unix")
    with tempfile.TemporaryDirectory(prefix="retention-crash-") as parent:
        for phase, expected in [("rotate", 0), ("created", 0), ("written", 0), ("synced", 0),
                                ("renamed", 4), ("directory", 4), ("unlink-1", 4),
                                ("unlink-2", 4), ("cleanup", 4)]:
            check(binary, Path(parent) / phase, phase, expected)


if __name__ == "__main__":
    main()
```

Build and run from the workspace root:

```sh
cargo build -p broker-core --example retention_crash_probe
python3 checks/check_retention_crash.py target/debug/examples/retention_crash_probe
```

Expected output:

```text
rotate: recovered_floor=0; final_floor=4; final_file_bytes=32; epoch=2
created: recovered_floor=0; final_floor=4; final_file_bytes=32; epoch=2
written: recovered_floor=0; final_floor=4; final_file_bytes=32; epoch=2
synced: recovered_floor=0; final_floor=4; final_file_bytes=32; epoch=2
renamed: recovered_floor=4; final_floor=4; final_file_bytes=32; epoch=2
directory: recovered_floor=4; final_floor=4; final_file_bytes=32; epoch=2
unlink-1: recovered_floor=4; final_floor=4; final_file_bytes=32; epoch=2
unlink-2: recovered_floor=4; final_floor=4; final_file_bytes=32; epoch=2
cleanup: recovered_floor=4; final_floor=4; final_file_bytes=32; epoch=2
```

The harness was not executed in the authoring environment. Its output above is an expected contract, not evidence of native filesystem behavior. A failed process check is worth preserving and diagnosing at its named operation; do not simply rerun until a green result appears.

## Measure the quantities this chapter actually owns

The ordinary demonstration gives deterministic accounting numbers, not throughput or latency results. On a native run, record `disk_state` immediately after the full-quota refusal, after the first successful reclamation, and after restart. The expected healthy file-byte sequence is `204 -> 102 -> 32 -> 67`, with zero pending reservations at those received-result boundaries. During a controlled sync gate, record both `file_bytes` and `reserved_bytes`; counting only queued messages misses the request already written but not yet acknowledged.

A logical file-length sum is the sum of segment lengths, including their headers. It excludes in-memory sparse indexes, response vectors, filesystem allocation rounding, directory storage, and unrelated processes. It is useful because this implementation controls and tests it exactly. It must not be reported as measured resident memory or actual device space. No optimization is justified from a paper calculation alone; later chapters measure the complete workload.

## Diagnose the violated boundary

| Observation | Boundary to inspect |
| --- | --- |
| A volatile acknowledgement frees a file | Retention read `C` instead of persisted `P`. |
| A checkpoint inside a segment deletes its first half | Eligibility used record progress without whole-file exclusive ends. |
| Full data quota prevents checkpoint admission | Disk permits leaked into the control path, or the control lane is independently full. |
| Reclamation hangs waiting for producer capacity | The owner is waiting for space that requires its own controls to run. |
| A full, fully consumed log cannot rotate for retention | The emergency header/count allowance is missing or accidentally shared with producers. |
| Abandoning a ticket lowers reserved bytes for queued work | The permit follows the reply receiver rather than the command. |
| Mailbox rejection permanently consumes bytes | A rejected command's permit was leaked instead of dropped. |
| Startup accepts a missing file at the floor | The floor was inferred from filenames or candidate data instead of authority. |
| Startup refuses only because an obsolete prefix has a gap | It incorrectly scans below the already validated retained floor. |
| A failed unlink leaves a healthy owner with lower charged bytes | Credit was returned before complete deletion and directory synchronization. |
| Restart begins offsets again at zero | Retained start was confused with the append end. |
| A retained response becomes invalid after deletion | A file-backed borrow escaped instead of an owned payload. |

## Two bounded variations

Change only group 1's checkpoint in the ordinary demo from next 2 to next 1 by acknowledging one record before the first checkpoint. Predict the result: no whole segment is eligible, so the floor remains 0 and the fifth producer stays refused. Then complete and checkpoint the second record and confirm that exactly the first 102-byte segment becomes eligible. Restore the original program after the check.

Next, use the existing fully full fixture `(segment cap=67, normal slots=2, data quota=134)`. Record the intermediate 166-byte, three-file state at the rotation gate and the final 32-byte, one-file state after reclamation. Explain why the former does not violate the *total* envelope but would violate normal producer admission. Do not “fix” the test by increasing the normal cap or allowing arbitrary extra nonempty files.

## Completion criteria and the next resource boundary

Complete the chapter when the focused suites and cumulative suite pass locally, the ordinary trace matches the documented states, and each process phase recovers the correct authority before final reclamation. Be able to explain separately why a floor may have published without a success reply, why cleanup may be incomplete after that publication, and why neither fact permits deleting any retained gap.

The broker now ties whole-file deletion to persisted consumer progress and charges queued append growth under fixed disk quotas. Its payload vectors and response queues still need a complete byte-memory budget, and a bounded control lane is not yet a reserved acknowledgement path. Chapter 15 implements **Byte budgets and pressure-safe admission** without weakening this chapter's retained-floor or disk-reservation contracts.

