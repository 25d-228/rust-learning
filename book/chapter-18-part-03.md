# Chapter 18 — Deterministic failure schedules and recovery checks

## Part 3 — Publication and deletion cuts

> **Learning objectives**
> Fail at a named operation boundary rather than after an arbitrary delay; prove that the requested fault was reached; distinguish authority replacement from reply observation; and rerun obsolete-prefix cleanup after a second interruption.

This is Part 3 of the shared **4.5-hour chapter**. Continue from [Part 2 — Reproducible owner schedules](https://25d-228.github.io/rust-learning/chapter-18-part-02.html). The production `SyncIo`, `PublicationStep`, checkpoint publisher, retained-floor publisher, and recovery implementation are unchanged.

## A fault description must identify both place and side

“Fail synchronization” is underspecified. Does the failure occur before calling the real operation, after it succeeds, or while its outcome is unknown? Similarly, “crash during retention” says little unless we know whether the floor was published and which names were removed.

The adapter has three deliberately narrow cuts. `SyncBefore(n)` returns an error before the nth data-file synchronization after arming. `Publication { name, step }` returns an error at the inherited callback **after** the named publication operation. `UnlinkAfter(n)` removes the nth obsolete segment and then returns an error. The latter two explicitly leave successful work behind.

A publication callback for one file must not accidentally trigger on another group's checkpoint. The `name` field therefore matches the complete canonical authority filename, not just a suffix such as `.ckp`. Counters restart when the cut is armed; initialization and earlier successful commands cannot consume the intended occurrence.

Append the following definitions to `schedules/support.rs` after `selected_seeds` from Part 1. `Point` describes observations; `Cut` describes the selected failure. A shared mutex protects only small test bookkeeping, not a file operation or a wait.

```rust title="crates/broker-core/tests/schedules/support.rs"
#[derive(Debug, Clone)]
pub enum Cut {
    SyncBefore(usize),
    Publication { name: String, step: PublicationStep },
    UnlinkAfter(usize),
}
#[derive(Debug)]
enum Point {
    Sync(u64),
    Publication(String, PublicationStep),
    Unlink(u64),
}
#[derive(Default)]
struct FaultState {
    cut: Option<Cut>,
    syncs: usize,
    unlinks: usize,
    hit: bool,
    events: Vec<String>,
}
#[derive(Clone, Default)]
pub struct FaultIo(Arc<Mutex<FaultState>>);
impl FaultIo {
    pub fn arm(&self, cut: Cut) {
        *self.0.lock().unwrap() = FaultState { cut: Some(cut), ..FaultState::default() };
    }
    pub fn hit(&self) -> bool { self.0.lock().unwrap().hit }
    pub fn events(&self) -> Vec<String> { self.0.lock().unwrap().events.clone() }
    fn observe(&self, point: Point) -> io::Result<()> {
        let mut state = self.0.lock().unwrap();
        match &point {
            Point::Sync(base) => { state.syncs += 1; let _ = base; }
            Point::Unlink(base) => { state.unlinks += 1; let _ = base; }
            Point::Publication(_, _) => {},
        }
        if state.events.len() < 128 { state.events.push(format!("{point:?}")); }
        let hit = match (&state.cut, &point) {
            (Some(Cut::SyncBefore(n)), Point::Sync(_)) => state.syncs == *n,
            (Some(Cut::UnlinkAfter(n)), Point::Unlink(_)) => state.unlinks == *n,
            (Some(Cut::Publication { name, step }), Point::Publication(actual, at)) =>
                name == actual && step == at,
            _ => false,
        };
        if hit {
            state.hit = true;
            state.cut = None;
            Err(io::Error::other("scheduled boundary failure"))
        } else { Ok(()) }
    }
}
```

`arm` discards old counters and observations. `observe` increments the appropriate count, retains the first 128 observation descriptions, and then compares the event with the armed cut. Matching consumes the cut, records `hit = true`, and returns one error. The cap bounds diagnostic storage; event counting and matching still continue beyond it. These small fixtures stay below the observation cap.

Every test checks `hit` after the request fails. Without that assertion, an unrelated configuration or admission error could make a fault test appear to pass without reaching the intended operation. A failure predicate must be falsifiable: absence of its target boundary is a failed test, not a successful early exit.

## Successful paths must perform real work

The implementation below delegates every successful data sync to `StorageFile::sync`. Unoverridden methods retain the real default metadata/checkpoint/floor and directory synchronization from `SyncIo`. The publication callback itself performs no rename or sync; the production publisher has already performed the named operation before calling it.

```rust title="crates/broker-core/tests/schedules/support.rs"
impl SyncIo for FaultIo {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        self.observe(Point::Sync(base.get()))?;
        file.sync()
    }
    fn publication_step(&mut self, target: &Path, step: PublicationStep) -> io::Result<()> {
        let name = target.file_name().unwrap().to_string_lossy().into_owned();
        self.observe(Point::Publication(name, step))
    }
    fn remove_segment(&mut self, path: &Path) -> io::Result<()> {
        let parent = path.parent().unwrap();
        assert_eq!(parent.file_name().unwrap().to_str().unwrap(), "partition-0");
        let root = parent.parent().unwrap();
        let base: u64 = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
        let floor = retention::read(root, p()).unwrap().floor.get();
        let safe = (0..2).map(|index| checkpoint::read(root, p(), g(index)).unwrap().next.get())
            .min().unwrap();
        if !(base < floor && floor <= safe) {
            return Err(io::Error::other("attempted unlink without safe published authority"));
        }
        fs::remove_file(path)?;
        self.observe(Point::Unlink(base))
    }
}

```

The unlink adapter also checks a safety precondition at the exact destructive boundary. Let $b$ be the segment base named for removal, $f$ the published floor, and $p_0,p_1$ the authoritative checkpoints read immediately before unlink. It requires:

$$
b<f\le\min(p_0,p_1).
$$

This local check supplements, not replaces, the model's segment-interval comparison. In a valid contiguous layout, the retained-floor file starts after every obsolete segment. Base ordering alone would not diagnose arbitrary overlapping files; recovery's topology checks and the full reference comparison remain necessary.

The adapter intentionally supports the fixture's partition 0 only. Removing a file in another partition is a test failure rather than silently reading the wrong authority. `remove_file` occurs **before** `observe(Unlink)`, so an injected error does not put the file back. Returning the disk reservation or setting the owner to failed is also not rollback.

## Read a recovered view without starting another worker

Complete `schedules/support.rs` with a compact recovered view. Calling it runs the real recovery algorithm and therefore may publish a new epoch; it is not a read-only inspection. It reads the entire retained interval with a limit larger than this fixture, checks both returned count and exclusive end, reads both checkpoint authorities, then drops the logs and directory guard before returning.

```rust title="crates/broker-core/tests/schedules/support.rs"
#[derive(Debug, PartialEq, Eq)]
pub struct View {
    pub epoch: u64,
    pub floor: u64,
    pub end: u64,
    pub fronts: [u64; 2],
    pub payloads: Vec<Vec<u8>>,
    pub repairs: usize,
}
pub fn recovered_view(root: &Path) -> View {
    let session = recovery::recover(root).unwrap();
    let repairs = session.repairs().len();
    let (epoch, mut logs) = session.into_parts();
    let log = &mut logs[0];
    let floor = log.retained_floor();
    let end = log.written_end();
    let batch = log.read(floor, end,
        broker_core::storage::segment::ReadLimits::new(1024, 4096).unwrap()).unwrap();
    assert_eq!(batch.next, end);
    assert_eq!(batch.records.len() as u64, end.get() - floor.get());
    let view = View { epoch: epoch.get(), floor: floor.get(), end: end.get(), repairs,
        fronts: std::array::from_fn(|index| checkpoint::read(root, p(), g(index)).unwrap().next.get()),
        payloads: batch.records.iter().map(|record| record.payload().to_vec()).collect() };
    drop(logs);
    view
}
```

The expected floor, frontiers, and payloads in the next tests are explicit values calculated from the chosen boundary. They are not inferred from whichever authority the recovery implementation happens to accept. A valid authority is an input to recovery, not permission for the test to accept every resulting state.

## Five publication cuts, two authority outcomes

Create `schedules/cuts.rs`. The helpers create four records and accept all their completions for both groups, but do **not** silently checkpoint them. Publication cuts cover candidate creation, writing, synchronization, authority replacement, and directory synchronization.

```rust title="crates/broker-core/tests/schedules/cuts.rs"
use std::fs;
use broker_core::delivery::AckOutcome;
use broker_core::storage::{checkpoint, recovery, retention};
use broker_core::storage::metadata::PublicationStep;
use broker_core::worker::{RequestError, WorkerError};
use super::driver::Rig;
use super::support::*;

const PUBLICATION_CUTS: [PublicationStep; 5] = [
    PublicationStep::TemporaryCreated, PublicationStep::TemporaryWritten,
    PublicationStep::TemporarySynced, PublicationStep::Renamed, PublicationStep::DirectorySynced,
];
fn replaced(step: PublicationStep) -> bool {
    matches!(step, PublicationStep::Renamed | PublicationStep::DirectorySynced)
}
fn completed_four(rig: &mut Rig) {
    for _ in 0..4 { assert!(rig.append()); }
    for group in 0..2 {
        for token in rig.fetch(group, 4, 160) {
            assert_eq!(rig.ack(group, token), AckOutcome::Applied);
        }
    }
}
fn assert_failed<T>(result: Result<T, RequestError>) {
    assert!(matches!(result, Err(RequestError::Failed(_))));
}

```

For this ordinary returned-error experiment, the file namespace is directly observable after the callback returns. The result is exact:

| Callback that returns an error | Checkpoint authority | Candidate |
| --- | --- | --- |
| `TemporaryCreated` | Old | Empty candidate exists |
| `TemporaryWritten` | Old | Fully written candidate exists |
| `TemporarySynced` | Old | Synchronized candidate exists |
| `Renamed` | New | Candidate name no longer names that file |
| `DirectorySynced` | New | Publication operations completed; observer still fails |

`rename` replaces the named destination on the target platform, subject to its documented errors and platform rules; the code uses same-directory candidates. The [Rust rename documentation](https://doc.rust-lang.org/std/fs/fn.rename.html) describes the operation. Its successful return is not a producer or checkpoint reply, nor a substitute for the surrounding synchronization protocol. For a physical power interruption, this table alone would not determine what survives; that is not the fault model being tested.

## Checkpoint failure must not move the other group

The first test runs a fresh directory at each cut. Both volatile groups have completed through offset 3. Only group 0 is checkpointed. Before replacement, recovery must start it at zero; after replacement, it must start at four, even though the request returned failure. Group 1 remains at zero in every case, so all four records remain required.

```rust title="crates/broker-core/tests/schedules/cuts.rs"
#[test]
fn checkpoint_publication_cuts_preserve_the_other_group_and_records() {
    for (index, step) in PUBLICATION_CUTS.into_iter().enumerate() {
        let mut trace = Trace::new(index as u64, "checkpoint_publication_cuts");
        let mut rig = Rig::new();
        completed_four(&mut rig);
        trace.step(format!("checkpoint group0: return error after {step:?}"));
        rig.io.arm(Cut::Publication { name: checkpoint::name(p(), g(0)), step });
        assert_failed(receive(rig.client.try_checkpoint(g(0)).unwrap()));
        assert!(rig.io.hit(), "requested boundary was never reached: {:?}", rig.io.events());
        rig.failed();
        let next = if replaced(step) { 4 } else { 0 };
        assert_eq!(checkpoint::read(&rig.root, p(), g(0)).unwrap().next.get(), next);
        trace.step("recover from authoritative checkpoint, never the candidate");
        let view = recovered_view(&rig.root);
        assert_eq!((view.floor, view.end, view.fronts), (0, 4, [next, 0]));
        assert_eq!(view.payloads, (0..4).map(|n| payload(n).to_vec()).collect::<Vec<_>>());
        assert!(!rig.root.join(checkpoint::pending_name(p(), g(0))).exists());
        let files = record_files(&rig.root);
        let again = recovered_view(&rig.root);
        assert_eq!((again.floor, again.end, again.fronts, again.repairs), (0, 4, [next, 0], 0));
        assert_eq!(again.epoch, view.epoch + 1);
        assert_eq!(record_files(&rig.root), files);
    }
}

```

After successful recovery, an abandoned candidate should be gone. A second recovery must leave record bytes and consumer frontiers unchanged, while publishing another epoch. This distinguishes **record/progress idempotence** from whole-directory byte equality. Treating a changed epoch as non-idempotent repair would misclassify a required fencing action.

## Floor publication fails before the deletion loop starts

Both groups now checkpoint four. Retention synchronizes the empty base-4 replacement, publishes floor 4, and only then reaches deletion. Any failure from a floor-publication callback returns before that deletion loop, including a failure after directory synchronization. Thus base-0 and base-2 files still exist immediately after the owner fails at every publication cut.

```rust title="crates/broker-core/tests/schedules/cuts.rs"
#[test]
fn retained_floor_publication_cuts_do_not_unlink_before_the_callback_returns() {
    for (index, step) in PUBLICATION_CUTS.into_iter().enumerate() {
        let mut trace = Trace::new(index as u64, "floor_publication_cuts");
        let mut rig = Rig::new();
        completed_four(&mut rig);
        for group in 0..2 { rig.checkpoint(group, false); }
        trace.step(format!("retain all four records: error after floor {step:?}"));
        rig.io.arm(Cut::Publication { name: retention::name(p()), step });
        assert_failed(receive(rig.client.try_retention().unwrap()));
        assert!(rig.io.hit());
        rig.failed();
        // Even DirectorySynced is an observer failure before retain reaches its deletion loop.
        assert!(segment(&rig.root, 0).exists() && segment(&rig.root, 2).exists());
        assert!(segment(&rig.root, 4).exists()); // Replacement was synchronized first.
        let floor = if replaced(step) { 4 } else { 0 };
        assert_eq!(retention::read(&rig.root, p()).unwrap().floor.get(), floor);
        trace.step("recover; remove old prefix only if the floor authority advanced");
        let view = recovered_view(&rig.root);
        assert_eq!((view.floor, view.end, view.fronts), (floor, 4, [4, 4]));
        assert_eq!(view.payloads.len() as u64, 4 - floor);
        assert_eq!(segment(&rig.root, 0).exists(), floor == 0);
        assert_eq!(segment(&rig.root, 2).exists(), floor == 0);
        assert!(!rig.root.join(retention::pending_name(p())).exists());
    }
}

```

If replacement did not happen, authority remains floor 0, and recovery keeps all four records plus the already-created empty active file. If replacement happened, authority is floor 4; recovery may remove the obsolete prefix even though the original retention caller never received success. The test asserts both allowed outcomes against the exact selected cut, rather than accepting either result indiscriminately.

A created empty base-4 file with floor 0 is not itself data loss. The existing data prefix remains contiguous and retained. The authority determines whether old files are obsolete; mere existence of the replacement file cannot authorize deletion.

## Interrupt obsolete cleanup a second time

Start with two full segments and both groups persisted at four. Retention publishes floor 4, unlinks base 0, then the adapter returns an error. Recovery sees base 2 below an authoritative floor and removes it. This time a second adapter error returns immediately after that unlink, before startup epoch publication. A third attempt must succeed with the empty base-4 active segment still present.

```rust title="crates/broker-core/tests/schedules/cuts.rs"
#[test]
fn deletion_then_a_second_interrupted_recovery_preserves_the_floor() {
    let mut trace = Trace::new(0x1810, "repeated_deletion_interruption");
    let mut rig = Rig::new();
    completed_four(&mut rig);
    for group in 0..2 { rig.checkpoint(group, false); }
    trace.step("publish floor4, unlink base0, then return error");
    rig.io.arm(Cut::UnlinkAfter(1));
    assert_failed(receive(rig.client.try_retention().unwrap()));
    assert!(rig.io.hit());
    rig.failed();
    assert!(!segment(&rig.root, 0).exists());
    assert!(segment(&rig.root, 2).exists());
    assert_eq!(retention::read(&rig.root, p()).unwrap().floor.get(), 4);
    trace.step("recovery unlinks base2, then returns another error before startup publication");
    let mut io = FaultIo::default();
    io.arm(Cut::UnlinkAfter(1));
    assert!(recovery::recover_with(&rig.root, &mut io, &mut |_| Ok(())).is_err());
    assert!(io.hit());
    assert!(!segment(&rig.root, 2).exists());
    assert!(segment(&rig.root, 4).exists());
    trace.step("third attempt succeeds; fourth attempt changes epoch but no record bytes");
    let view = recovered_view(&rig.root);
    assert_eq!((view.floor, view.end, view.fronts, view.repairs), (4, 4, [4, 4], 0));
    let files = record_files(&rig.root);
    let again = recovered_view(&rig.root);
    assert_eq!((again.floor, again.end, again.fronts), (4, 4, [4, 4]));
    assert_eq!(again.epoch, view.epoch + 1);
    assert_eq!(record_files(&rig.root), files);
}

```

The important invariant is not “every recovery runs once.” It is that each destructive step remains justified by authority that was already validated and synchronized. Missing subsets below the floor are expected after interrupted deletion. Missing the retained-floor file itself is not expected and must not be normalized into an empty log.

Both failed attempts release their directory ownership through error cleanup. The next recovery acquires the lock again. A test that left an old session alive would test lock exclusion instead of recovery, so session lifetimes here are deliberately visible.

## A failed data synchronization does not erase a completed write

The next test fails before the first new data sync. The record write has already completed. The owner must report failed accepted work, keep visible end at zero, stop the disk ledger, and join unsuccessfully. Recovery in this ordinary filesystem experiment sees a complete record and preserves it; that does not retroactively turn the failed request into a confirmed publish.

```rust title="crates/broker-core/tests/schedules/cuts.rs"
#[test]
fn failed_data_sync_keeps_client_success_separate_from_recovered_presence() {
    let mut trace = Trace::new(0x1811, "sync_failure_after_write");
    let mut rig = Rig::new();
    trace.step("write record0; fail the next data synchronization before calling sync");
    rig.io.arm(Cut::SyncBefore(1));
    assert_failed(receive(rig.client.try_publish(payload(91).to_vec()).unwrap()));
    assert!(rig.io.hit());
    let failure = rig.owner.take().unwrap().join().unwrap_err();
    let WorkerError::Failed { state, .. } = failure else { panic!("expected failed owner") };
    assert_eq!((state.written_end.get(), state.visible_end.get()), (1, 0));
    assert!(rig.client.disk_state().unwrap().stopped);
    assert_eq!(rig.client.disk_state().unwrap().reserved_bytes, 0);
    trace.step("ordinary filesystem contents still hold a complete unconfirmed record");
    let view = recovered_view(&rig.root);
    assert_eq!((view.floor, view.end, view.fronts), (0, 1, [0, 0]));
    assert_eq!(view.payloads, vec![payload(91).to_vec()]);
}

```

The distinction is between **confirmed records**, whose successful replies were observed, and **complete unconfirmed records**, which may exist after an uncertain operation. The failure response proves neither absence nor rollback. This fixture checks one exact retained outcome under an injected method error; it is not a list of all outcomes possible under device failure.

## Abandoned replies and sequential group publication

A lost checkpoint reply does not prevent the owner from using the persisted frontier it successfully published. The same-lane observation in the driver establishes that publication completed before retention. Conversely, successfully checkpointing group 0 cannot justify using group 0's frontier for group 1 when group 1's candidate fails before replacement.

```rust title="crates/broker-core/tests/schedules/cuts.rs"
#[test]
fn a_lost_checkpoint_reply_can_still_enable_retention() {
    let mut trace = Trace::new(0x1812, "lost_checkpoint_then_retain");
    let mut rig = Rig::new();
    completed_four(&mut rig);
    trace.step("drop both checkpoint receivers; same-lane queries observe committed authorities");
    for group in 0..2 { rig.checkpoint(group, true); }
    rig.retain();
    rig.check();
    assert_eq!(rig.model.floor(), 4);
    trace.step("joined restart keeps the floor and both checkpointed next offsets");
    rig.restart();
    rig.check();
    rig.stop();
}

#[test]
fn one_group_publication_failure_cannot_inherit_the_other_groups_progress() {
    let mut trace = Trace::new(0x1813, "non_atomic_group_checkpoints");
    let mut rig = Rig::new();
    completed_four(&mut rig);
    rig.checkpoint(0, false);
    trace.step("group0 is at4; group1 candidate syncs but does not replace its zero authority");
    rig.io.arm(Cut::Publication { name: checkpoint::name(p(), g(1)),
        step: PublicationStep::TemporarySynced });
    assert_failed(receive(rig.client.try_checkpoint(g(1)).unwrap()));
    assert!(rig.io.hit());
    rig.failed();
    let view = recovered_view(&rig.root);
    assert_eq!((view.fronts, view.floor, view.end), ([4, 0], 0, 4));
    assert_eq!(view.payloads.len(), 4);
}

```

The second test makes the lack of a multi-group transaction concrete: group 0 remains at four, group 1 remains at zero, and the retained floor remains zero. There is no rollback of group 0 and no fabricated advancement of group 1. The supervisor's final checkpoints have the same sequential, non-atomic publication property.

## Observe ordering on the successful path too

A fault test could miss an ordering regression if it only inspected the final namespace. The last test records the floor's directory-synchronized callback and both actual unlinks, then requires the publication observation to precede the first deletion. At each unlink, the adapter independently reads and checks authority as shown earlier.

```rust title="crates/broker-core/tests/schedules/cuts.rs"
#[test]
fn successful_deletion_follows_floor_directory_synchronization() {
    let mut trace = Trace::new(0x1814, "publication_before_unlink");
    let mut rig = Rig::new();
    completed_four(&mut rig);
    for group in 0..2 { rig.checkpoint(group, false); }
    trace.step("retain; observe publication then every actual deletion");
    rig.retain();
    let events = rig.io.events();
    let published = events.iter().position(|event|
        event.contains("retention-p0.floor") && event.contains("DirectorySynced")).unwrap();
    let removed = events.iter().position(|event| event.starts_with("Unlink")).unwrap();
    assert!(published < removed, "{events:?}");
    assert_eq!(events.iter().filter(|event| event.starts_with("Unlink")).count(), 2);
    assert_eq!(fs::metadata(segment(&rig.root, 4)).unwrap().len(), 32);
    rig.check();
    rig.stop();
}
```

The retained file's 32-byte length is checked explicitly. The broader `rig.check()` then checks frontiers, empty retained data, and the disk ledger. Neither final-state comparison alone nor callback order alone is the whole argument; together they test the observable result and the critical destructive sequencing.

These seven test functions add five checkpoint publication cases, five floor publication cases, and the named compound histories. They are not 7 plus 5 plus 5 separate Cargo tests: each function is one reported test, with its internal case loop supplying additional boundary coverage. Part 4 applies the same discipline to short writes, damaged bytes, and recovery's own failure points.
