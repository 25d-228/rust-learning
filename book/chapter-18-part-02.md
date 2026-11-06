# Chapter 18 — Deterministic failure schedules and recovery checks

## Part 2 — Reproducible owner schedules

> **Learning objectives**
> Drive the real partition owner against a separate reference state; make reply abandonment reproducible without sleeps; distinguish a same-lane fence from a cross-lane query; and combine lease gaps, checkpoints, retention, disk pressure, and restart.

This is Part 2 of the chapter's shared **4.5-hour implementation estimate**. Begin with [Part 1 — A small reference state for composed failures](https://25d-228.github.io/rust-learning/chapter-18.html). The production prerequisite is still the complete Chapter 17 workspace. All source in this part is new test code, not a replacement owner implementation.

## Keep owner cleanup ahead of directory cleanup

Create `schedules/driver.rs`. A `Rig` owns one live worker handle, its client, its manual clock, the reference state, and the directory containing the fixture. Struct fields are declared with the owner before the directory so fallback destruction joins the owner before the directory is removed. The explicit `stop` and `failed` methods will take the handle out of its `Option`, transferring the unique right to join it.

The fault adapter is cloneable; its clones share **test-control state**, not storage handles. Its full implementation appears in Part 3. The initialized session supplies the reference epoch; subsequent expected epochs are checked against that history rather than invented from the next token the implementation returns.

```rust title="crates/broker-core/tests/schedules/driver.rs"
use std::fs;
use std::sync::Arc;
use broker_core::delivery::{AckOutcome, LeaseToken};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::ProgressAck;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::{checkpoint, recovery, retention};
use broker_core::storage::segment::ReadLimits;
use broker_core::types::Offset;
use broker_core::worker::{Client, WorkerHandle};
use crate::common::TestDir;
use super::model::Model;
use super::support::*;

pub struct Rig {
    // Declaration order matters for fallback destruction: join before deleting the directory.
    pub owner: Option<WorkerHandle>,
    pub client: Client,
    pub clock: Arc<ManualClock>,
    pub io: FaultIo,
    pub model: Model,
    pub tokens: [Vec<LeaseToken>; 2],
    pub root: std::path::PathBuf,
    next_id: u64,
    _directory: TestDir,
}
impl Rig {
    pub fn new() -> Self {
        let (directory, root, session) = setup();
        let model = Model::new(session.epoch().get());
        let clock = Arc::new(ManualClock::new(0));
        let io = FaultIo::default();
        let (owner, client) = start(session, io.clone(), Arc::clone(&clock));
        Self { owner: Some(owner), client, clock, io, model,
            tokens: [Vec::new(), Vec::new()], root, next_id: 0, _directory: directory }
    }
```

The raw client can outlive the joined worker, but it retains no open record file. The fixture does not allocate Chapter 15's response pools. Thus this target does not claim to remeasure serving memory or prove that all resource generations have been dropped; the relevant pooled ownership and supervisor tests remain in their existing targets.

## An input ID is not a storage offset

`next_id` advances for every attempted logical input, including a quota refusal. Storage offsets advance only for admitted records. The eight-byte payload is therefore evidence of which input arrived at a particular offset, not a tautological encoding of that offset.

For normal appends, the model predicts admission before invoking the broker. On refusal it checks that the rejected payload returns intact. On success it checks the exact assigned offset. Advancing the model immediately before the real call is safe for this test runner: any mismatch is terminal and unwinds the fixture; the driver does not continue from an unverified model state.

```rust title="crates/broker-core/tests/schedules/driver.rs"
    pub fn append(&mut self) -> bool {
        let bytes = payload(self.next_id);
        self.next_id += 1; // Rejected logical inputs are not assigned storage offsets.
        if !self.model.can_append() {
            let rejected = self.client.try_publish(bytes.to_vec()).err().expect("quota refusal");
            assert_eq!(rejected.reason, AdmissionError::DiskFull);
            assert_eq!(rejected.value, bytes);
            return false;
        }
        let expected = self.model.append(bytes);
        assert_eq!(get(self.client.try_publish(bytes.to_vec())).get(), expected);
        true
    }
    pub fn abandoned_append_then_fence(&mut self) {
        // No control-lane command is used as a data-lane fence.
        assert!(self.model.bytes() + 144 <= QUOTA, "fixture needs two admissions");
        let bytes = payload(self.next_id);
        self.next_id += 1;
        drop(self.client.try_publish(bytes.to_vec()).unwrap());
        self.model.append(bytes);
        assert!(self.append()); // Same data lane; count-one commit precedes this reply.
    }
```

The abandoned append is deliberately different. Its receiver is dropped without reading a result. Then a second append is submitted to the **same FIFO data lane** and its successful reply is awaited. The owner commits one record at a time, so processing and confirming the second append implies that the first append's synchronization already succeeded. That is a causal fence, not a sleep.

The 144-byte precondition reserves enough room for two worst-case 72-byte admissions even if the first reservation has not yet settled. Without it, the second append could be refused while the first still owns its reservation, making the proposed fence invalid.

| Sequence | What the final observation establishes |
| --- | --- |
| Drop append A's receiver; receive append B's success from the same data lane | A was processed and synchronized before B's success |
| Drop append A's receiver; issue a snapshot on the control lane | The snapshot may precede A; it is not a data-lane barrier |
| Drop a request before submitting it | No committed command exists; this is not abandoned accepted work |

A sender can successfully place a reply in a channel before the receiver is dropped. That still is not a **caller-observed** success. This schedule tests missing observation, not necessarily a failed send. The distinction is essential when counting confirmed and ambiguous inputs later.

## Return tokens to an independent model

`fetch` gives the actual batch to the model's validity checker and retains copies of the returned tokens for later acknowledgement attempts. Retaining old tokens is intentional: a generated schedule must be able to submit an expired, superseded, retired, or old-epoch token, not just the latest successful one.

`ack` predicts both the outcome and the number of slots retired. It also checks the API distinction between accepted and nonapplied progress acknowledgements. A duplicate completed follower is accepted without advancing a frontier; an already-retired token is nonapplied. Neither result authorizes a larger checkpoint.

```rust title="crates/broker-core/tests/schedules/driver.rs"
    pub fn fetch(&mut self, index: usize, count: usize, bytes: usize) -> Vec<LeaseToken> {
        let batch = get(self.client.try_lease(g(index), ReadLimits::new(count, bytes).unwrap()));
        self.model.fetch(index, count, bytes, &batch);
        let tokens: Vec<_> = batch.records.iter().map(|record| record.token).collect();
        self.tokens[index].extend_from_slice(&tokens);
        tokens
    }
    pub fn ack(&mut self, index: usize, token: LeaseToken) -> AckOutcome {
        let (expected, retired) = self.model.acknowledge(index, token);
        let result = get(self.client.try_ack_progress(g(index), token));
        let (actual, progress, accepted) = match result {
            ProgressAck::Accepted(value) => (value.ack, value.progress, true),
            ProgressAck::NotApplied { ack, progress } => (ack, progress, false),
        };
        assert_eq!(actual.outcome, expected);
        assert_eq!(actual.retired, retired);
        assert_eq!(accepted, matches!(expected, AckOutcome::Applied | AckOutcome::Duplicate));
        assert_eq!((progress.volatile_next.get(), progress.persisted_next.get()),
            (self.model.groups[index].next, self.model.groups[index].persisted));
        expected
    }
```

The model's map and the actual ring may arrange entries differently. Comparing `AckOutcome`, retired count, and progress gives a public-state comparison without exposing private storage. The following `check` method will independently inspect the entire window after the command.

## Checkpoint observations need their own ordering argument

A manual-clock advance changes lease time only. It does not fast-forward the operating system, the batch timer, or a Tokio clock. The fixture avoids depending on those other timers by committing one record per batch and using explicit checkpoints.

In the abandoned-checkpoint case, this raw `Client` puts both checkpoint and progress query into its **one control lane**. Observing the later query therefore establishes that the earlier checkpoint was processed. Do not transplant this fence unchanged to the bounded client: its checkpoint uses the urgent lane while its progress query uses the ordinary query lane. There, a cross-lane query is not a checkpoint barrier.

```rust title="crates/broker-core/tests/schedules/driver.rs"
    pub fn advance(&mut self, millis: u64) {
        self.model.now += millis;
        assert_eq!(self.clock.advance(millis).unwrap(), self.model.now);
    }
    pub fn checkpoint(&mut self, index: usize, abandon: bool) {
        let changed = self.model.checkpoint(index);
        let ticket = self.client.try_checkpoint(g(index)).unwrap();
        if abandon {
            drop(ticket);
            // Checkpoint and progress query use the same FIFO control lane on this raw client.
            let progress = get(self.client.try_progress(g(index)));
            assert_eq!(progress.persisted_next.get(), self.model.groups[index].persisted);
        } else {
            let result = receive(ticket).unwrap();
            assert_eq!(result.published, changed);
            assert_eq!(result.progress.persisted_next.get(), self.model.groups[index].persisted);
        }
    }
    pub fn retain(&mut self) {
        let before = self.model.floor();
        let (floor, count, bytes, rotated) = self.model.retain();
        let result = get(self.client.try_retention());
        assert_eq!((result.before.get(), result.floor.get()), (before, floor));
        assert_eq!((result.removed_segments, result.removed_bytes, result.rotated), (count, bytes, rotated));
        assert_eq!(result.disk.file_bytes, self.model.bytes());
    }
```

The retention comparison checks more than the final floor. It checks how many whole segments disappeared, how many logical bytes they contained, whether an empty active replacement was needed, and the final ledger charge. For the four-record fixture, the result is two removed segments and 224 removed bytes, followed by a 32-byte surviving file. Those numbers come from the reference geometry, not from recounting the broker's returned answer.

## Join explicitly, and never continue a failed ledger

The ordinary stop expects success and verifies the final visible end. The failed-owner path expects a failed join, no outstanding disk reservation, and no healthy charged upper bound. A failed write may have left bytes whose exact length the in-memory ledger does not know; zero reserved credit does not imply zero file growth.

```rust title="crates/broker-core/tests/schedules/driver.rs"
    pub fn stop(&mut self) {
        let state = self.owner.take().expect("one owner").join().unwrap();
        assert_eq!(state.visible_end.get(), self.model.end());
    }
    pub fn failed(&mut self) {
        assert!(self.owner.take().expect("one owner").join().is_err());
        let disk = self.client.disk_state().unwrap();
        assert!(disk.stopped);
        assert_eq!(disk.reserved_bytes, 0);
        assert_eq!(disk.charged_upper_bound(), None);
    }
    pub fn restart(&mut self) {
        self.stop(); // Plain join, deliberately NOT the supervised final-checkpoint policy.
        let session = recovery::recover(&self.root).unwrap();
        assert_eq!(session.epoch().get(), self.model.epoch + 1);
        self.model.restart(session.epoch().get());
        self.clock = Arc::new(ManualClock::new(0));
        let (owner, client) = start(session, self.io.clone(), Arc::clone(&self.clock));
        self.owner = Some(owner);
        self.client = client;
    }
```

`restart` first joins the previous owner, then acquires a new storage session. It checks the expected next epoch for this successful path, resets the reference's volatile state, and creates the replacement owner with a new manual clock. Old token vectors are **not** cleared. A later fault case will show why an unsuccessfully returned startup can consume an epoch and make the next usable epoch jump by more than one.

## Inspect the whole retained interval after every generated command

The driver now checks owner publication state, both group windows, both checkpoint authorities, retained-floor authority, logical disk accounting, actual file lengths, and every retained payload. File length is named `physical_lengths` only because it is measured from actual files; the comment explicitly excludes allocated filesystem blocks.

```rust title="crates/broker-core/tests/schedules/driver.rs"
    pub fn check(&self) {
        let state = get(self.client.try_snapshot());
        assert_eq!((state.assigned_end.get(), state.written_end.get(), state.visible_end.get()),
            (self.model.end(), self.model.end(), self.model.end()));
        assert_eq!((state.pending_records, state.pending_bytes), (0, 0));
        for index in 0..2 {
            let expected = &self.model.groups[index];
            let progress = get(self.client.try_progress(g(index)));
            let group = get(self.client.try_group_state(g(index)));
            assert_eq!((progress.volatile_next.get(), progress.persisted_next.get()),
                (expected.next, expected.persisted));
            assert_eq!((group.window.completed_end.get(), group.window.issued_end.get()),
                (expected.next, expected.issued));
            assert_eq!((group.window.occupied, group.window.capacity), (expected.attempts.len(), WINDOW));
            assert_eq!(checkpoint::read(&self.root, p(), g(index)).unwrap().next.get(), expected.persisted);
        }
        assert_eq!(retention::read(&self.root, p()).unwrap().floor.get(), self.model.floor());
        let disk = self.client.disk_state().unwrap();
        assert!(!disk.stopped);
        assert_eq!((disk.file_bytes, disk.reserved_bytes), (self.model.bytes(), 0));
        let physical_lengths: u64 = fs::read_dir(self.root.join("partition-0")).unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len()).sum();
        assert_eq!(physical_lengths, disk.file_bytes); // Length, not allocated filesystem blocks.
        let batch = get(self.client.try_fetch(Offset::new(self.model.floor()), ReadLimits::new(1024, 4096).unwrap()));
        assert_eq!(batch.next.get(), self.model.end());
        assert_eq!(batch.records.len() as u64, self.model.end() - self.model.floor());
        for (i, record) in batch.records.iter().enumerate() {
            let offset = self.model.floor() + i as u64;
            assert_eq!(record.offset().get(), offset);
            assert_eq!(record.payload(), &self.model.records[offset as usize]);
        }
    }
}
```

The snapshot must have equal assigned, written, and visible ends because this driver reaches a reply or a defined fence after every data operation. That equality would be an invalid invariant in the middle of a real group-commit batch. Assertions belong to a stated observation boundary, not to an arbitrary instant in the implementation.

The read starts at the model's retained floor, not whatever lower bound the implementation chooses to expose. Its returned count must equal the retained interval length, and each payload must match the input history at that exact offset. This detects omission, duplication, offset reuse, and incorrect retention without reporting legitimate deletion as loss.

## Make the generated operation grammar explicit

Create `schedules/sequences.rs`. First fix the generator's three initial values and exercise the upper-end seed. This test checks reproducibility of the input generator, not uniformity or statistical quality.

```rust title="crates/broker-core/tests/schedules/sequences.rs"
use broker_core::delivery::{AckOutcome, AckRejection};
use super::driver::Rig;
use super::support::*;

#[test]
fn generator_has_fixed_wrapping_vectors() {
    let mut generator = Generator(0);
    assert_eq!(generator.next(), 1442695040888963407);
    assert_eq!(generator.next(), 1876011003808476466);
    assert_eq!(generator.next(), 11166244414315200793);
    let mut a = Generator(u64::MAX);
    let mut b = Generator(u64::MAX);
    for bound in 1..100 { assert_eq!(a.pick(bound), b.pick(bound)); }
}

```

The operation grammar has nine choices. Choices 0 and 1 append, 2 fetches, 3 selects a previously returned token, 4 advances lease time, 5 checkpoints, 6 retains, 7 restarts, and 8 attempts the abandoned-append fence when two admissions fit. Every step also draws a group, even when that particular operation does not use it; this keeps the draw order explicit.

Additional random draws are made inside the selected branch. Therefore the seed reproduces a schedule **for this generator and source version**. Changing branch draw order changes the sequence; record the code state along with a reported seed. A token-dependent branch also makes this a stateful test grammar, not a precomputed list of network packets.

```rust title="crates/broker-core/tests/schedules/sequences.rs"
#[test]
fn seeded_sequences_match_the_reference() {
    for seed in selected_seeds() {
        let mut trace = Trace::new(seed, "seeded_sequences_match_the_reference");
        let mut random = Generator(seed);
        let mut rig = Rig::new();
        for step in 0..96 {
            let operation = random.pick(9);
            let group = random.pick(2);
            trace.step(format!("step={step} operation={operation} group={group} now={} end={} floor={}",
                rig.model.now, rig.model.end(), rig.model.floor()));
            match operation {
                0 | 1 => { rig.append(); }
                2 => {
                    let count = 1 + random.pick(4);
                    let bytes = [1, 39, 40, 79, 80, 160][random.pick(6)];
                    trace.step(format!("fetch count={count} bytes={bytes}"));
                    rig.fetch(group, count, bytes);
                }
                3 if !rig.tokens[group].is_empty() => {
                    let token = rig.tokens[group][random.pick(rig.tokens[group].len())];
                    trace.step(format!("ack {token:?}"));
                    rig.ack(group, token);
                }
                3 => {}
                4 => {
                    let by = [0, 1, 9, 10, 11][random.pick(5)];
                    trace.step(format!("advance={by}"));
                    rig.advance(by);
                }
                5 => rig.checkpoint(group, random.pick(2) == 0),
                6 => rig.retain(),
                7 => rig.restart(),
                8 if rig.model.bytes() + 144 <= QUOTA => rig.abandoned_append_then_fence(),
                8 => { rig.append(); }
                _ => unreachable!(),
            }
            rig.check();
        }
        rig.stop();
    }
}

```

The default six seeds give 576 generated top-level transitions. Each transition also runs the same full-state comparison. The selected byte limits deliberately straddle 40-byte boundaries: 39 cannot fit a record, 40 fits one, 79 still fits one, and 80 fits two. Time increments include exact deadline equality rather than testing only comfortably early or late values.

A finite set of generated schedules does not guarantee every operation was useful in every seed. Empty token histories can make an acknowledgement choice do nothing; lagging groups can prevent retention. The directed tests below force the important combinations rather than assuming random coverage.

## Force the abandoned-reply and completion-gap histories

The first directed test keeps both groups uncheckpointed and proves that the record with no observed publish reply remains present after restart. In this controlled schedule, the same-lane successor establishes that it reached storage. In an arbitrary disconnected client history without that fence, its presence remains ambiguous until independently observed.

```rust title="crates/broker-core/tests/schedules/sequences.rs"
#[test]
fn lost_append_reply_is_not_a_missing_record() {
    let mut trace = Trace::new(0x1801, "lost_append_reply_is_not_a_missing_record");
    let mut rig = Rig::new();
    trace.step("submit id0, drop its receiver, await same-lane id1");
    rig.abandoned_append_then_fence();
    rig.check();
    trace.step("plain join and restart with neither group checkpointed");
    rig.restart();
    rig.check();
    assert_eq!(rig.model.end(), 2);
    assert_eq!(rig.fetch(0, 4, 160).len(), 2);
    rig.stop();
}

```

Now compose the gap, time boundary, retry, checkpoint, and retention. The following table predicts the important frontiers. All offsets are exclusive; `floor` is the retained start.

| Boundary | Group 0 volatile/persisted | Group 1 volatile/persisted | Floor |
| --- | --- | --- | ---: |
| Issue 0–3; complete 2, 1, 3 | 0 / 0 | 0 / 0 | 0 |
| Checkpoint group 0 above the gap | 0 / 0 | 0 / 0 | 0 |
| Expire head; issue and complete its retry | 4 / 0 | 0 / 0 | 0 |
| Publish group 0 checkpoint; abandon reply | 4 / 4 | 0 / 0 | 0 |
| Complete and checkpoint group 1 | 4 / 4 | 4 / 4 | 0 |
| Retain, replace active file, restart | 4 / 4 | 4 / 4 | 4 |

At time 10, the original head's deadline is exactly reached, so it is expired even before a retry is issued. After reissue it is stale by generation instead. The final old token is rejected by epoch before the retired-offset classification can apply.

```rust title="crates/broker-core/tests/schedules/sequences.rs"
#[test]
fn gap_checkpoint_lost_reply_retry_and_retention_compose() {
    let mut trace = Trace::new(0x1802, "gap_checkpoint_lost_reply_retry_and_retention_compose");
    let mut rig = Rig::new();
    for _ in 0..4 { assert!(rig.append()); }
    trace.step("fetch 0..4; complete followers 2,1,3 above the missing head");
    let old = rig.fetch(0, 4, 160);
    for index in [2, 1, 3] { assert_eq!(rig.ack(0, old[index]), AckOutcome::Applied); }
    rig.checkpoint(0, true);
    rig.retain();
    assert_eq!((rig.model.groups[0].persisted, rig.model.floor()), (0, 0));
    trace.step("deadline equality rejects head; retry changes generation; old head is stale");
    rig.advance(10);
    assert_eq!(rig.ack(0, old[0]), AckOutcome::Rejected(AckRejection::Expired));
    let retry = rig.fetch(0, 1, 40)[0];
    assert_eq!(rig.ack(0, old[0]), AckOutcome::Rejected(AckRejection::StaleGeneration));
    assert_eq!(rig.ack(0, retry), AckOutcome::Applied);
    assert_eq!(rig.model.groups[0].next, 4);
    trace.step("checkpoint group0 with lost reply; group1 still protects all records");
    rig.checkpoint(0, true);
    rig.retain();
    assert_eq!(rig.model.floor(), 0);
    trace.step("complete/checkpoint group1; replace consumed active segment and retain");
    for token in rig.fetch(1, 4, 160) { rig.ack(1, token); }
    rig.checkpoint(1, false);
    rig.retain();
    rig.check();
    assert_eq!((rig.model.floor(), rig.model.bytes()), (4, 32));
    trace.step("restart retains floor4; old token is fenced before retired-offset classification");
    rig.restart();
    assert_eq!(rig.ack(0, retry), AckOutcome::Rejected(AckRejection::WrongEpoch));
    rig.check();
    rig.stop();
}

```

This test uses the real checkpoint publication and deletion routines, not a mocked progress file. It forces a fully consumed active segment replacement, verifies the remaining 32-byte file through the reference checker, and then reopens under a different epoch.

## Enumerate a small finite space where it is useful

There are $4\times3\times2\times1=24$ completion orders for four distinct records. Reuse that small space, but add checkpoint, retention, and restart around each completion. This is a new **cross-subsystem** check; it does not rename Chapter 11's existing ring tests as new work.

The four nested loops are intentionally simple. Reject any order containing a repeated index. The remaining orders are the 24 permutations. For each, the slow second group stays at persisted zero and must prevent all reclamation, even after group 0 finishes and checkpoints all four records.

```rust title="crates/broker-core/tests/schedules/sequences.rs"
#[test]
fn all_four_record_completion_orders_checkpoint_only_the_prefix() {
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    let order = [a, b, c, d];
                    if (0..4).any(|i| order[..i].contains(&order[i])) { continue; }
                    let seed = (a * 64 + b * 16 + c * 4 + d) as u64;
                    let mut trace = Trace::new(seed, "completion_order");
                    let mut rig = Rig::new();
                    for _ in 0..4 { assert!(rig.append()); }
                    let tokens = rig.fetch(0, 4, 160);
                    for index in order {
                        trace.step(format!("order={order:?}; complete index={index}; checkpoint; retain"));
                        rig.ack(0, tokens[index]);
                        rig.checkpoint(0, false);
                        rig.retain();
                        rig.check();
                        assert_eq!(rig.model.floor(), 0); // Other group's persisted frontier is zero.
                    }
                    trace.step("restart after final checkpoint");
                    rig.restart();
                    rig.check();
                    assert_eq!((rig.model.groups[0].next, rig.model.groups[1].next), (4, 0));
                    rig.stop();
                }
            }
        }
    }
}

```

## Make stale epochs and byte-limited retries unavoidable

Restart resets the process-local clock and rebuilds empty windows at persisted progress. Thus offset, generation, and numeric deadline can legitimately collide with an old token. The epoch is not redundant decoration. The first test constructs exactly that collision.

The second returns only one of two eligible retries because 79 encoded bytes cannot hold two 40-byte records. The unreturned old token must still be classified as expired, not stale-generation: no new attempt was installed for it. The returned old token must now be stale. These checks connect storage copying limits to attempt installation.

```rust title="crates/broker-core/tests/schedules/sequences.rs"
#[test]
fn an_old_epoch_is_rejected_even_when_attempt_numbers_collide() {
    let mut trace = Trace::new(0x1803, "epoch_collision");
    let mut rig = Rig::new();
    assert!(rig.append());
    let old = rig.fetch(0, 1, 40)[0];
    trace.step("lose volatile window and reset process-local clock");
    rig.restart();
    let new = rig.fetch(0, 1, 40)[0];
    assert_eq!((old.offset, old.generation, old.deadline_ms), (new.offset, new.generation, new.deadline_ms));
    assert_ne!(old.epoch, new.epoch);
    assert_eq!(rig.ack(0, old), AckOutcome::Rejected(AckRejection::WrongEpoch));
    assert_eq!(rig.ack(0, new), AckOutcome::Applied);
    rig.check();
    rig.stop();
}

#[test]
fn a_byte_limited_retry_does_not_install_the_unreturned_attempt() {
    let mut trace = Trace::new(0x1804, "nonfitting_retry");
    let mut rig = Rig::new();
    for _ in 0..2 { assert!(rig.append()); }
    let old = rig.fetch(0, 2, 80);
    trace.step("expire both; one byte below two encoded records returns only one retry");
    rig.advance(10);
    let one = rig.fetch(0, 2, 79);
    assert_eq!(one.len(), 1);
    let skipped = old.iter().find(|token| token.offset != one[0].offset).unwrap();
    assert_eq!(rig.ack(0, *skipped), AckOutcome::Rejected(AckRejection::Expired));
    assert_eq!(rig.ack(0, old[one[0].offset.get() as usize]), AckOutcome::Rejected(AckRejection::StaleGeneration));
    rig.check();
    rig.stop();
}

```

## Finish with pressure that only persisted progress can relieve

The final sequence fills the quota with eight records. Processing and checkpointing group 0 does not help while group 1 remains at zero. Once both groups persist 8, retention can reclaim every data record after creating an empty active file at base 8. The next admitted record still receives offset 8, never an offset recycled from the deleted prefix.

```rust title="crates/broker-core/tests/schedules/sequences.rs"
#[test]
fn a_stalled_group_keeps_disk_full_until_both_checkpoints_advance() {
    let mut trace = Trace::new(0x1805, "disk_pressure_sequence");
    let mut rig = Rig::new();
    trace.step("fill data quota; a refused input does not acquire an offset");
    while rig.append() {}
    assert_eq!(rig.model.end(), 8);
    for group in 0..2 {
        while rig.model.groups[group].next < rig.model.end() {
            for token in rig.fetch(group, 4, 160) { rig.ack(group, token); }
        }
        trace.step(format!("group{group} checkpoint; attempt reclamation"));
        rig.checkpoint(group, false);
        rig.retain();
        if group == 0 { assert!(!rig.append()); }
    }
    assert_eq!((rig.model.floor(), rig.model.bytes()), (8, 32));
    trace.step("admit at offset8 after reclamation; restart and inspect retained range");
    assert!(rig.append());
    rig.restart();
    rig.check();
    assert_eq!((rig.model.floor(), rig.model.end()), (8, 9));
    rig.stop();
}
```

The logical input identifiers include refused attempts, so the newly admitted payload may skip input numbers while its storage offset remains contiguous. That is correct. Confusing logical input IDs with storage offsets would either fabricate missing records or hide retries of the same logical input.

These eight test functions cover deterministic normal/error classifications and composed restart behavior. They do not yet force an I/O operation itself to fail. Part 3 adds that boundary without changing the production implementation or claiming that an error undoes already completed filesystem work.
