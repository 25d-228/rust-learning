# Chapter 13 — Durable consumer progress
## Part 3 — Publication cuts and repeat ranges

> **Learning objectives**
> Check exact bytes independently of the encoder. Exercise gap-aware persistence, missing authority, restored starts, and error paths at all publication boundaries. Distinguish injected return failures, orderly restart, real process termination, and power loss.

Complete [Part 1](https://25d-228.github.io/rust-learning/chapter-13.html) and [Part 2](https://25d-228.github.io/rust-learning/chapter-13-part-02.html) first. This is the final part of their shared **4.5-hour implementation estimate**. Retain all earlier tests; add the two test files below and both examples. The Python harness is under the workspace's `checks/` directory.

The chapter supplies **34 new Rust tests: nine policy/state unit tests and 25 integration tests on the native Unix target**. Together with the prerequisite workspace, that is **281 supplied tests**, not a claim that they have passed. The process-kill harness is an additional executable check, not included in that count. Expected outputs below are specifications to check locally; no Rust compilation, Cargo execution, dependency resolution, formatter run, native macOS execution, or Rust process-harness execution was available in the authoring environment.

## First check the 48 bytes without the Rust encoder

This standard-library Python calculation packs the independent golden fixture. The `>` prefix means big-endian, `H` is an unsigned two-byte field, `B` a one-byte field, `x` one zero pad byte, and `Q` an unsigned eight-byte field. The last sixteen zeros are the reserved area. The CRC covers 44 bytes and is appended as a big-endian four-byte value.

```sh
python3 - <<'PY'
import struct
import zlib
prefix = b"PBGP" + struct.pack(">HHHBxQQ", 1, 48, 2, 1, 7, 19) + bytes(16)
raw = prefix + struct.pack(">I", zlib.crc32(prefix))
print(len(raw), raw[-4:].hex(" "))
print(raw.hex(" "))
PY
```

The first output line is `48 f8 0f 9b 4d`. Compare the second line with the integration test's literal byte array; neither fixture calls the Rust encoder to create its expectation.

## Unit checks: policy and frontier transitions

Create `crates/broker-core/src/delivery/progress/tests.rs`. As a child module, this file can inspect the parent's private policy and frontier fields. Its synthetic `Ack` values deliberately test invariant enforcement; production acknowledgements still come from `LeaseWindow`, not from a caller supplying an arbitrary completed offset.

```rust title="crates/broker-core/src/delivery/progress/tests.rs"
use super::*;
use crate::delivery::GroupState;
use crate::delivery::lease::AckRejection;
use crate::delivery::window::WindowState;
use crate::io::StorageFile;

fn group(value: u8) -> GroupId { GroupId::new(value).unwrap() }
fn offset(value: u64) -> Offset { Offset::new(value) }
fn progress(starts: [u64; 2], visible: u64) -> Progress {
    Progress::new(PathBuf::new(), Epoch::new(1).unwrap(), PartitionId::new(0).unwrap(),
        starts.map(offset), offset(visible), CheckpointPolicy::new(Duration::from_secs(1)).unwrap()).unwrap()
}
// Synthetic internal observations exercise guards; production obtains Ack only from LeaseWindow.
fn ack(g: u8, next: u64, outcome: AckOutcome) -> Ack {
    Ack { outcome, retired: 0, state: GroupState {
        epoch: Epoch::new(1).unwrap(), partition: PartitionId::new(0).unwrap(), group: group(g),
        window: WindowState { completed_end: offset(next), issued_end: offset(next), occupied: 0, capacity: 4 },
    } }
}

```

The first tests reject meaningless policies and impossible recovered starts. Advancing completion again does not move an already-installed deadline: the first dirty completion determines when the next periodic attempt becomes due.

```rust title="crates/broker-core/src/delivery/progress/tests.rs"
#[test]
fn policy_rejects_zero_and_more_than_sixty_seconds() {
    assert!(CheckpointPolicy::new(Duration::ZERO).is_err());
    assert!(CheckpointPolicy::new(Duration::from_secs(61)).is_err());
    assert!(CheckpointPolicy::new(Duration::from_secs(60)).is_ok());
}

#[test]
fn recovered_frontiers_cannot_exceed_visibility() {
    assert!(Progress::new(PathBuf::new(), Epoch::new(1).unwrap(), PartitionId::new(0).unwrap(),
        [offset(3), offset(4)], offset(3), CheckpointPolicy::new(Duration::from_secs(1)).unwrap()).is_err());
    let p = progress([2, 3], 3);
    assert_eq!(p.state(group(1)).persisted_next, offset(3));
    assert!(p.deadline().is_none());
}

#[test]
fn first_dirty_deadline_is_not_postponed_by_later_completions() {
    let mut p = progress([0, 0], 10);
    let now = Instant::now();
    p.observe(ack(0, 1, AckOutcome::Applied), offset(10), now).unwrap();
    let first = p.deadline().unwrap();
    p.observe(ack(0, 9, AckOutcome::Applied), offset(10), now + Duration::from_millis(900)).unwrap();
    assert_eq!(p.deadline(), Some(first));
    assert!(!p.due(group(0), first - Duration::from_nanos(1)));
    assert!(p.due(group(0), first));
    assert_eq!(p.state(group(0)).persisted_next, offset(0));
}

```

Each group owns its own frontier and deadline. The invariant checks run before updating a frontier, so a regressing or out-of-range synthetic result leaves its state unchanged. Matching the partition and epoch also matters: identical numeric offsets are not sufficient identity.

```rust title="crates/broker-core/src/delivery/progress/tests.rs"
#[test]
fn group_deadlines_are_independent_and_the_earliest_wins() {
    let mut p = progress([0, 0], 4);
    let now = Instant::now();
    p.observe(ack(1, 1, AckOutcome::Applied), offset(4), now).unwrap();
    p.observe(ack(0, 2, AckOutcome::Applied), offset(4), now + Duration::from_millis(100)).unwrap();
    let deadline = p.deadline().unwrap();
    assert!(p.due(group(1), deadline));
    assert!(!p.due(group(0), deadline));
    assert_eq!(p.state(group(0)).volatile_next, offset(2));
    assert_eq!(p.state(group(1)).volatile_next, offset(1));
}

#[test]
fn regressions_overshoots_and_wrong_scope_preserve_progress() {
    let mut p = progress([2, 0], 5);
    let before = p.state(group(0));
    let mut cases = vec![ack(0, 1, AckOutcome::Applied), ack(0, 6, AckOutcome::Applied)];
    let mut wrong = ack(0, 3, AckOutcome::Applied); wrong.state.epoch = Epoch::new(2).unwrap(); cases.push(wrong);
    let mut wrong = ack(0, 3, AckOutcome::Applied); wrong.state.partition = PartitionId::new(1).unwrap(); cases.push(wrong);
    for a in cases {
        assert!(p.observe(a, offset(5), Instant::now()).is_err());
        assert_eq!(p.state(group(0)), before);
        assert!(p.deadline().is_none());
    }
}

```

An already-retired or rejected attempt does not become a fresh applied acknowledgement. A clean checkpoint should return without opening a candidate or calling the synchronization adapter; the deliberately panicking adapter detects an accidental publication.

```rust title="crates/broker-core/src/delivery/progress/tests.rs"
#[test]
fn rejected_and_retired_reports_are_not_new_acceptances() {
    let mut p = progress([0, 0], 1);
    for outcome in [AckOutcome::Rejected(AckRejection::Expired), AckOutcome::AlreadyRetired] {
        assert!(matches!(p.observe(ack(0, 0, outcome), offset(1), Instant::now()).unwrap(),
            ProgressAck::NotApplied { .. }));
        assert!(p.deadline().is_none());
    }
    assert!(matches!(p.observe(ack(0, 0, AckOutcome::Duplicate), offset(1), Instant::now()).unwrap(),
        ProgressAck::Accepted(_)));
}

struct NoIo;
impl SyncIo for NoIo {
    fn sync(&mut self, _: Offset, _: &mut dyn StorageFile) -> std::io::Result<()> { panic!("unexpected data sync") }
    fn sync_metadata(&mut self, _: &mut dyn StorageFile) -> std::io::Result<()> { panic!("unexpected publication") }
}

#[test]
fn clean_checkpoint_needs_no_file_or_synchronization() {
    let mut p = progress([7, 3], 7);
    let result = p.checkpoint(group(0), offset(7), &mut NoIo).unwrap();
    assert!(!result.published);
    assert_eq!((result.progress.volatile_next, result.progress.persisted_next), (offset(7), offset(7)));
    assert!(p.deadline().is_none());
}

```

An exclusive frontier may equal the maximum offset value without this layer adding one to it. Window issuance owns that separate arithmetic. Explicit-only mode can be dirty with no timer: `due == None` is not a synonym for `volatile == persisted`.

```rust title="crates/broker-core/src/delivery/progress/tests.rs"
#[test]
fn max_is_a_valid_exclusive_frontier_without_addition() {
    let mut p = progress([u64::MAX - 1, 0], u64::MAX);
    p.observe(ack(0, u64::MAX, AckOutcome::Applied), offset(u64::MAX), Instant::now()).unwrap();
    assert_eq!(p.state(group(0)).volatile_next, offset(u64::MAX));
    assert_eq!(p.state(group(0)).persisted_next, offset(u64::MAX - 1));
}

#[test]
fn explicit_policy_can_be_dirty_without_installing_a_timer() {
    let mut p = progress([0, 0], 1);
    p.policy = CheckpointPolicy::explicit_only();
    p.observe(ack(0, 1, AckOutcome::Applied), offset(1), Instant::now()).unwrap();
    assert_eq!(p.state(group(0)).volatile_next, offset(1));
    assert_eq!(p.state(group(0)).persisted_next, offset(0));
    assert!(p.deadline().is_none());
}
```

## Integration checks: real files, owner replies, and recovery

Create `crates/broker-core/tests/progress.rs`. It reuses the complete `tests/common/mod.rs` supplied earlier. `TestDir` supplies isolated temporary directories and removes them on normal test cleanup; no existing user data is used.

Most cases choose explicit-only checkpointing so wall-clock scheduling cannot move a frontier between assertions. The one automatic-policy case observes a synchronization event. Five-second receive limits are watchdogs to stop a broken test from hanging; they are not evidence that another thread reached a particular state. No sleep is used as a synchronization proof.

```rust title="crates/broker-core/tests/progress.rs"
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

```

### Validate bytes before trusting offsets

The literal golden bytes test the encoder independently. Every truncated length, an extra byte, and each single-byte alteration must be refused. Some invalid fields are deliberately given a freshly calculated checksum: a valid checksum cannot make a bad format version, reserved field, identity, or frontier semantically valid.

```rust title="crates/broker-core/tests/progress.rs"
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

```

### A completed follower is not a persisted prefix

Initialization must publish all eight zero checkpoints. The owner test then acknowledges a follower, checkpoints without a prefix advance, closes the head gap, and checks the distinction between volatile completion, duplicate acknowledgement, and published progress.

```rust title="crates/broker-core/tests/progress.rs"
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

```

The next tests establish that an accepted completion alone does not survive even an orderly join. Two groups restore different persisted starts, each with an empty window; neither inherits the other's progress or restores volatile outstanding attempts.

```rust title="crates/broker-core/tests/progress.rs"
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

```

### Restart fences still apply

Checkpointing does not replace the startup epoch. The restart case deliberately collides the numeric offset, generation, and deadline with an old token. Another case abandons the checkpoint reply, then uses the same FIFO control lane to query progress after that request: dropping a reply receiver did not cancel its command.

```rust title="crates/broker-core/tests/progress.rs"
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

```

The durable owner rejects both legacy acknowledgement shortcuts. A separate test leaves one producer write pending and proves that a group checkpoint neither flushes it nor acts as a cross-lane barrier. The loop observes the owner's explicit snapshot under a watchdog instead of guessing how quickly the data lane ran.

```rust title="crates/broker-core/tests/progress.rs"
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

```

### All authorities must validate before any repair

A plausible candidate cannot replace a missing or corrupt authoritative file. The tests combine such a failure in one partition with an incomplete tail in another and compare the original bytes afterward. Validation failure must not repair that unrelated tail or consume a new startup epoch.

```rust title="crates/broker-core/tests/progress.rs"
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

```

A correctly checksummed checkpoint is still refused when its scope is wrong, its publishing epoch is from the future, or its next offset lies in an incomplete record rather than at or below the planned complete end. Conversely, a junk candidate does not invalidate a good authority: it is discarded only after validation succeeds.

```rust title="crates/broker-core/tests/progress.rs"
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

```

The native Unix check refuses symbolic links where regular authoritative checkpoint files are required. Initialization cuts at each real publication boundary also verify that the final broker metadata was not installed before all initial checkpoint publications completed.

```rust title="crates/broker-core/tests/progress.rs"
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

```

Unknown checkpoint-shaped names and nonregular paths are not silently accepted. Durable startup also refuses a log built through the low-level storage API without validated progress seeds, or a seed paired with the wrong lease epoch.

```rust title="crates/broker-core/tests/progress.rs"
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

```

### Errors before and after replacement are different disk states

`points()` lists the five observation boundaries. `Cut` returns an error *after* the corresponding real filesystem step. Every case must stop the owner without returning successful checkpoint completion. Recovery finds old progress before replacement and new progress after replacement; it never infers the result from the failed caller's missing reply.

```rust title="crates/broker-core/tests/progress.rs"
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

```

The gate pauses after successful directory synchronization but before publication returns. The checkpoint ticket cannot yet report success. Releasing the gate lets the publication return and only then lets the owner report the new persisted frontier. The test uses an explicit rendezvous, not a delay.

```rust title="crates/broker-core/tests/progress.rs"
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

```

The next adapter makes the actual candidate-file or directory synchronization call fail. A directory-sync failure happens after replacement in this protocol, so a subsequent same-machine process recovery can see the new checkpoint even though the caller received no success. That observation is not a machine-power-loss guarantee.

```rust title="crates/broker-core/tests/progress.rs"
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

```

The automatic-policy test waits for an observed checkpoint directory publication while the owner is otherwise idle. This checks that the worker actually includes the consumer deadline in its wait, not merely that the `Progress` object can calculate one.

```rust title="crates/broker-core/tests/progress.rs"
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

```

Recovery must synchronize the authoritative checkpoints before returning a new session; an injected failure refuses that session and leaves the startup epoch unchanged. Finally, publishing progress must neither rewrite record bytes nor alter the other group's checkpoint. In that last test, `paths.each_ref()` borrows each array element and returns an array of references. Mapping those references reads the files without moving the original `PathBuf` values; the later comparison can still iterate `paths`. This is the borrowing counterpart to the [array mapping](https://doc.rust-lang.org/std/primitive.array.html#method.each_ref) used in Part 2.

```rust title="crates/broker-core/tests/progress.rs"
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
```

Run the focused checks and then the cumulative suite from the workspace root:

```sh
cargo fmt --all
cargo test -p broker-core delivery::progress::tests
cargo test -p broker-core --test progress
cargo test --workspace --all-targets
```

Expected counts are nine new unit tests and 25 new integration tests on macOS/Unix. A successful cumulative run should retain the earlier 247 tests as well. These are supplied expectations, not recorded execution results. The `--all-targets` command compiles the examples as test targets, but does not run their `main` functions as the following demonstrations do.

## Demonstration: the repeat interval is visible in the replies

Create `crates/broker-core/examples/progress_probe.rs`. Four partition owners start, but the trace uses partition 0. Both groups observe the same records. An explicit-only policy makes the two deliberate checkpoint calls the only publications of runtime consumer progress. The manual lease clock is separate from the owner's real-time checkpoint scheduling clock.

```rust title="crates/broker-core/examples/progress_probe.rs"
use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use broker_core::delivery::{AckOutcome, AckRejection, LeaseConfig, LeaseToken};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::{AckAccepted, CheckpointPolicy, ProgressAck};
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::FileSync;
use broker_core::types::GroupId;
use broker_core::worker::{BatchConfig, Client, Ticket, WorkerHandle};

fn request<T, E: Debug>(admission: Result<Ticket<T>, E>) -> Result<T, Box<dyn Error>> {
    let ticket = admission.map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}
fn accept(client: &Client, token: LeaseToken) -> Result<AckAccepted, Box<dyn Error>> {
    match request(client.try_ack_progress(token.group, token))? {
        ProgressAck::Accepted(value) => Ok(value),
        other => Err(io::Error::other(format!("expected accepted completion, got {other:?}")).into()),
    }
}
```

The first phase publishes `cat`, `dog`, `owl`, and `fox`. Group 0 completes offset 2 above a gap, then offset 0. Its checkpoint saves next offset 1. Completing offset 1 subsequently retires both 1 and 2, but there is no second checkpoint before shutdown.

```rust title="crates/broker-core/examples/progress_probe.rs"
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 2 { return Err(io::Error::other("usage: progress_probe ABSENT_DIRECTORY").into()); }
    let root = Path::new(&args[1]);
    let (epoch, logs) = recovery::initialize(root, StorageConfig::new(102, 2, 16)?)?.into_parts();
    let batch = BatchConfig::new(1, 1000, Duration::from_secs(1))?;
    let mut owners = Vec::new();
    for log in logs {
        owners.push(WorkerHandle::start_with_progress(log, FileSync, batch, 8, 8,
            LeaseConfig::new(epoch, 4, 10_000)?, Arc::new(ManualClock::new(0)), CheckpointPolicy::explicit_only())?);
    }
    println!("epoch={} partitions={} policy=explicit", epoch.get(), owners.len());
    let client = owners[0].client();
    let g0 = GroupId::new(0)?; let g1 = GroupId::new(1)?;
    for payload in [b"cat", b"dog", b"owl", b"fox"] { request(client.try_publish(payload.to_vec()))?; }
    let first = request(client.try_lease(g0, ReadLimits::new(4, 140)?))?;
    for index in [2usize, 0] {
        let a = accept(&client, first.records[index].token)?;
        println!("ack={} retired={} volatile={} persisted={}", index, a.ack.retired,
            a.progress.volatile_next.get(), a.progress.persisted_next.get());
    }
    let cp = request(client.try_checkpoint(g0))?;
    assert_eq!(cp.progress.persisted_next.get(), 1);
    println!("checkpoint group=0 persisted={} published={}", cp.progress.persisted_next.get(), cp.published);
    let a = accept(&client, first.records[1].token)?;
    assert_eq!((a.progress.volatile_next.get(), a.progress.persisted_next.get()), (3, 1));
    println!("ack=1 retired={} volatile={} persisted={}", a.ack.retired,
        a.progress.volatile_next.get(), a.progress.persisted_next.get());
    let other = request(client.try_lease(g1, ReadLimits::new(1, 35)?))?;
    let a = accept(&client, other.records[0].token)?;
    println!("group=1 volatile={} persisted={}", a.progress.volatile_next.get(), a.progress.persisted_next.get());
    let old = first.records[1].token;
    for owner in owners { owner.join()?; }
    drop(client);

```

The restart loads group 0 at 1 and group 1 at 0. The old group-0 token for offset 1 collides with the new attempt's numeric generation and deadline; only the epoch differs. After the new deliveries are completed, the second explicit checkpoint moves group 0 to 4.

```rust title="crates/broker-core/examples/progress_probe.rs"
    let (epoch, mut logs) = recovery::recover(root)?.into_parts();
    let owner = WorkerHandle::start_with_progress(logs.remove(0), FileSync, batch, 8, 8,
        LeaseConfig::new(epoch, 4, 10_000)?, Arc::new(ManualClock::new(0)), CheckpointPolicy::explicit_only())?;
    drop(logs);
    let client = owner.client();
    for group in [g0, g1] {
        let s = request(client.try_group_state(group))?;
        println!("recovered epoch={} group={} completed={} issued={} occupied={}", epoch.get(), group.get(),
            s.window.completed_end.get(), s.window.issued_end.get(), s.window.occupied);
    }
    let repeated = request(client.try_lease(g0, ReadLimits::new(4, 140)?))?;
    let offsets: Vec<_> = repeated.records.iter().map(|r| r.token.offset.get()).collect();
    assert_eq!(offsets, vec![1, 2, 3]);
    println!("group=0 repeated={offsets:?}");
    let new = repeated.records[0].token;
    assert_eq!((new.offset, new.generation, new.deadline_ms), (old.offset, old.generation, old.deadline_ms));
    match request(client.try_ack_progress(g0, old))? {
        ProgressAck::NotApplied { ack, .. } => {
            assert_eq!(ack.outcome, AckOutcome::Rejected(AckRejection::WrongEpoch));
            println!("old_token={:?}", ack.outcome);
        }
        other => return Err(io::Error::other(format!("old token was not fenced: {other:?}")).into()),
    }
    for record in repeated.records { accept(&client, record.token)?; }
    let cp = request(client.try_checkpoint(g0))?;
    assert_eq!(cp.progress.persisted_next.get(), 4);
    println!("checkpoint group=0 persisted={}", cp.progress.persisted_next.get());
    owner.join()?;
    Ok(())
}
```

Use a fresh, absent child path. The shell trap removes only the parent directory created by this command:

```sh
cargo build -p broker-core --example progress_probe
PARENT="$(mktemp -d "${TMPDIR:-/tmp}/progress-probe.XXXXXX")"
trap 'rm -rf -- "$PARENT"' EXIT
cargo run -p broker-core --example progress_probe -- "$PARENT/broker"
```

Expected application output, excluding Cargo's own build messages:

```text
epoch=1 partitions=4 policy=explicit
ack=2 retired=0 volatile=0 persisted=0
ack=0 retired=1 volatile=1 persisted=0
checkpoint group=0 persisted=1 published=true
ack=1 retired=2 volatile=3 persisted=1
group=1 volatile=1 persisted=0
recovered epoch=2 group=0 completed=1 issued=1 occupied=0
recovered epoch=2 group=1 completed=0 issued=0 occupied=0
group=0 repeated=[1, 2, 3]
old_token=Rejected(WrongEpoch)
checkpoint group=0 persisted=4
```

Offsets 1 and 2 are repeated despite their previously accepted completions. Offset 3 is also delivered again, but it was never completed in the first session. Group 1 repeats from 0 because none of its progress was checkpointed. The orderly join does not conceal this distinction by adding an undocumented final checkpoint.

## Terminate a real process at five publication boundaries

An injected `Err` exercises the owner's failure handling, including destructors and joins. It does not demonstrate recovery when those cleanup paths never run. The second example exposes an explicit marker and parks the owner after a selected real filesystem step; the Python parent then sends `SIGKILL` and waits for termination before opening the directory again.

Create `crates/broker-core/examples/checkpoint_crash_probe.rs`. The `cut` mode has already received accepted acknowledgements for offsets 0 and 1 before requesting its checkpoint. Its explicit-only policy prevents a background timer from publishing first. The `verify` mode opens only the actual authoritative checkpoint and checks the first repeated offset.

```rust title="crates/broker-core/examples/checkpoint_crash_probe.rs"
use std::error::Error;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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
    Ok(ticket.recv_timeout(Duration::from_secs(5))??)
}
fn phase(name: &str) -> Result<PublicationStep, io::Error> {
    match name {
        "created" => Ok(PublicationStep::TemporaryCreated),
        "written" => Ok(PublicationStep::TemporaryWritten),
        "synced" => Ok(PublicationStep::TemporarySynced),
        "renamed" => Ok(PublicationStep::Renamed),
        "directory" => Ok(PublicationStep::DirectorySynced),
        _ => Err(io::Error::other("unknown checkpoint phase")),
    }
}
```

`Pause` delegates the real synchronization operations to `FileSync`. Its observation hook matches the exact group-0 checkpoint path, prints and flushes the readiness marker, then parks in a loop. A spurious unpark cannot let publication finish and send a reply before the parent terminates it.

```rust title="crates/broker-core/examples/checkpoint_crash_probe.rs"
struct Pause { phase: PublicationStep, label: String }
impl SyncIo for Pause {
    fn sync(&mut self, _: Offset, file: &mut dyn StorageFile) -> io::Result<()> { file.sync() }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        if target.file_name() == Some(OsStr::new("progress-p0-g0.ckp")) && step == self.phase {
            println!("READY {}", self.label);
            io::stdout().flush()?;
            // The parent kills this process. Spurious unparks cannot release the gate.
            loop { std::thread::park(); }
        }
        Ok(())
    }
}
```

The helper constructs the same durable owner used by ordinary callers. Neither mode sets progress by hand. `cut` obtains it by publishing records, leasing them, applying acknowledgements, and submitting a checkpoint command.

```rust title="crates/broker-core/examples/checkpoint_crash_probe.rs"
fn start<S: SyncIo + Send + 'static>(session: StorageSession, sync: S) -> Result<WorkerHandle, Box<dyn Error>> {
    let (epoch, mut logs) = session.into_parts();
    let log = logs.remove(0); drop(logs);
    Ok(WorkerHandle::start_with_progress(log, sync,
        BatchConfig::new(1, 1000, Duration::from_secs(1))?, 8, 8,
        LeaseConfig::new(epoch, 4, 10_000)?, Arc::new(ManualClock::new(0)), CheckpointPolicy::explicit_only())?)
}
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() < 3 {
        return Err(io::Error::other("usage: checkpoint_crash_probe cut DIRECTORY PHASE | verify DIRECTORY").into());
    }
    let root = Path::new(&args[2]);
    let group = GroupId::new(0)?;
    if args[1] == "cut" && args.len() == 4 {
        let label = args[3].to_str().ok_or_else(|| io::Error::other("phase is not UTF-8"))?;
        let session = recovery::initialize(root, StorageConfig::new(102, 2, 16)?)?;
        let owner = start(session, Pause { phase: phase(label)?, label: label.to_owned() })?;
        let client = owner.client();
        for payload in [b"cat", b"dog", b"owl", b"fox"] { request(client.try_publish(payload.to_vec()))?; }
        let batch = request(client.try_lease(group, ReadLimits::new(4, 140)?))?;
        for record in &batch.records[..2] {
            assert!(matches!(request(client.try_ack_progress(group, record.token))?, ProgressAck::Accepted(_)));
        }
        let state = request(client.try_progress(group))?;
        assert_eq!((state.volatile_next.get(), state.persisted_next.get()), (2, 0));
        let ticket = client.try_checkpoint(group).map_err(|e| io::Error::other(format!("admission: {e:?}")))?;
        // This cannot complete: the owner stops at the selected publication phase.
        let result = ticket.recv();
        return Err(io::Error::other(format!("checkpoint escaped its gate: {result:?}")).into());
    }
```

The verification process recovers under epoch 2, inspects both groups, leases the next record, and reports the persisted frontier and first repeated offset. A new initialization is not an acceptable fallback if recovery refuses the directory.

```rust title="crates/broker-core/examples/checkpoint_crash_probe.rs"
    if args[1] == "verify" && args.len() == 3 {
        let owner = start(recovery::recover(root)?, FileSync)?;
        let client = owner.client();
        let state = request(client.try_progress(group))?;
        let batch = request(client.try_lease(group, ReadLimits::new(4, 140)?))?;
        let first = batch.records.first().ok_or_else(|| io::Error::other("missing repeat interval"))?.token.offset;
        assert_eq!(first, state.persisted_next);
        assert_eq!(request(client.try_progress(GroupId::new(1)?))?.persisted_next.get(), 0);
        println!("RECOVERED {} {} {}", state.epoch.get(), state.persisted_next.get(), first.get());
        owner.join()?;
        return Ok(());
    }
    Err(io::Error::other("invalid mode or arguments").into())
}
```

Create `checks/check_progress_crash.py`. The readiness reader uses a selector and a bounded marker buffer. Standard error goes to a temporary file rather than an unread pipe that could block the child. The watchdogs are failure limits; the flushed marker is what establishes the selected publication boundary.

```python title="checks/check_progress_crash.py"
#!/usr/bin/env python3
"""Kill a real broker owner at five acknowledged checkpoint-publication boundaries."""
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
                raise TimeoutError("child did not reach the publication gate")
            chunk = os.read(process.stdout.fileno(), 4096)
            if not chunk:
                raise RuntimeError(f"child exited before its gate: {process.poll()}")
            data.extend(chunk)
            if len(data) > 4096:
                raise RuntimeError("unexpectedly large marker output")
    return bytes(data).decode("utf-8").strip()


```

The `finally` block ensures a failed assertion does not leave a child process alive holding the directory lock. `wait()` is essential: requesting termination is not the same as observing that the owner has exited. Verification starts only after that observation.

```python title="checks/check_progress_crash.py"
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
    wanted = f"RECOVERED 2 {expected} {expected}"
    if verified.stdout.strip() != wanted:
        raise AssertionError(f"{phase}: {verified.stdout!r}, expected {wanted!r}")
    print(f"{phase}: killed at gate; persisted={expected}; first_repeat={expected}; epoch=2")


```

Each phase gets a fresh directory. The expected frontier is 0 after candidate creation, writing, or file synchronization; it is 2 after replacement or directory synchronization. These expectations describe process termination on a still-running machine, where rename has already changed the visible directory entry.

```python title="checks/check_progress_crash.py"
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if os.name != "posix":
        parser.error("this process-kill check targets native macOS/Unix")
    with tempfile.TemporaryDirectory(prefix="progress-crash-") as parent:
        for phase, expected in [("created", 0), ("written", 0), ("synced", 0),
                                ("renamed", 2), ("directory", 2)]:
            check(binary, Path(parent) / phase, phase, expected)


if __name__ == "__main__":
    main()
```

Build the probe and run the parent harness from the workspace root:

```sh
cargo build -p broker-core --example checkpoint_crash_probe
python3 checks/check_progress_crash.py target/debug/examples/checkpoint_crash_probe
```

Expected output:

```text
created: killed at gate; persisted=0; first_repeat=0; epoch=2
written: killed at gate; persisted=0; first_repeat=0; epoch=2
synced: killed at gate; persisted=0; first_repeat=0; epoch=2
renamed: killed at gate; persisted=2; first_repeat=2; epoch=2
directory: killed at gate; persisted=2; first_repeat=2; epoch=2
```

Every case kills the owner before a successful checkpoint reply. The final two nevertheless recover the newer frontier. That is the concrete reason a missing reply cannot imply that a checkpoint did not publish.

The checks exercise three different things: ordinary restart with volatile progress, returned errors at real publication boundaries, and process termination without cleanup. They do not simulate loss of the operating system's buffers, disk-controller cache behavior, arbitrary filesystem reordering after power loss, or a failed physical device. Do not rename this harness a power-loss test. In the authoring environment the Python source was syntax-checked; the Rust probe and the process-kill harness were not executed.

## Diagnose the invariant, not only the failing assertion

| Observed failure | Likely boundary to inspect |
| --- | --- |
| A completed follower lets restart skip a gap | The checkpoint used the maximum completed offset rather than the contiguous frontier. |
| An acknowledgement always reports identical volatile and persisted values | The acknowledgement path is advancing `persisted` without publication. |
| A missing authoritative file starts at zero | Recovery is inventing progress instead of refusing missing authority. |
| A valid candidate repairs a corrupt authority | Candidate cleanup has become an undocumented promotion procedure. |
| An invalid checkpoint still shortens another partition's tail | Checkpoint validation happened after applying recovery repairs. |
| Busy completion traffic prevents periodic checkpoints | Each acknowledgement is postponing an already-installed deadline. |
| Checkpoint timers work only when new traffic arrives | The owner wait omitted the consumer deadline. |
| A failed publication silently retries on the same owner | An uncertain replacement result is being treated as rollback. |
| The gated checkpoint ticket reports success too early | The reply or persisted frontier advances before publication returns. |
| An old token is accepted after restart | The restored frontier replaced, rather than complemented, the epoch fence. |

For a storage error, keep the original directory for inspection and record the failing path and stage. Do not delete the authoritative file, promote `.next`, or rewrite a zero checkpoint to make a test proceed. The safe response to an uncertain running owner is to stop using that session and recover; later supervision makes that lifecycle mandatory across all partitions.

## Two small variations and a scheduling check

First, add an explicit checkpoint for group 1 after its accepted completion in `progress_probe`. Predict the restart trace before running: group 1 should restart at 1, while group 0 still restarts at 1. Their equal numbers result from independent publications, not shared state.

Second, move group 0's first checkpoint to immediately after its acknowledgement of offset 2, before acknowledging offset 0. That checkpoint should be a clean no-op with persisted next 0. With no later checkpoint before shutdown, restart must repeat offsets 0 through 3. Do not keep the original assertions unchanged; replace them with these predicted outcomes and restore the original program afterward.

Finally, compare explicit-only mode with `CheckpointPolicy::new(Duration::from_millis(250))`. Use an observation event like the integration test's `Observe` adapter to establish publication, then query progress. Sleeping for 250 milliseconds is not proof that the owner completed a filesystem transaction. Report a measured delay only from an actual run; the configured duration is a scheduling policy, not a measured latency.

## Completion criteria and the next storage boundary

The chapter is complete when all prerequisite and new tests pass locally, the ordinary demonstration shows the expected repeat interval, and each process-kill phase recovers the expected authoritative frontier. Be able to explain why an accepted acknowledgement may repeat, why a missing checkpoint reply may still correspond to persisted progress, and why neither fact permits an incomplete prefix to be skipped.

The broker now separates delivery state from durable restart position without persisting timers or unbounded completion histories. No segment has been deleted. Chapter 14 uses the **persisted** frontiers of both groups—not current volatile completion, not outstanding lease counts—to derive the safe whole-segment retention boundary and enforce a disk budget.

