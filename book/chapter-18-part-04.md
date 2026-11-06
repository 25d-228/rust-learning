# Chapter 18 — Deterministic failure schedules and recovery checks

## Part 4 — Interrupting recovery again

> **Learning objectives**
> Exercise partial writes through the actual I/O seam; distinguish repairable incomplete tails from complete corrupt records; stop recovery before and after destructive work; and interpret repeatability without demanding that fencing epochs stop advancing.

This is the final part of the chapter's shared **4.5-hour implementation estimate**. Assemble the new source from [Part 1](https://25d-228.github.io/rust-learning/chapter-18.html), [Part 2](https://25d-228.github.io/rust-learning/chapter-18-part-02.html), and [Part 3](https://25d-228.github.io/rust-learning/chapter-18-part-03.html) before the commands below. All destructive fixtures create their own directories. No command in this chapter takes an existing broker directory from the command line.

## Establish a nontrivial persisted prefix first

Create `schedules/recovery.rs`. Its starting state contains three records, group 0 at volatile/persisted 3/2, and group 1 at contiguous volatile/persisted 1/1 with offset 2 completed above the gap. The ordinary joined stop leaves those checkpoint authorities unchanged. On restart, group 0 must repeat from 2 and group 1 from 1; a test that expected both groups at zero would miss checkpoint reconstruction entirely.

With 112-byte segments, offsets 0 and 1 occupy the full base-0 file. Offset 2 occupies the base-2 file, whose length is 72 bytes: 32 for the segment header and 40 for its one record. There is exactly enough room for another 40-byte record without rotation.

```rust title="crates/broker-core/tests/schedules/recovery.rs"
use std::fs;
use std::io;
use broker_core::codec::SegmentHeader;
use broker_core::io::{FaultFile, StorageFile, open_file};
use broker_core::error::FormatError;
use broker_core::storage::{checkpoint, metadata, recovery};
use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::error::StorageError;
use broker_core::storage::metadata::PublicationStep;
use broker_core::storage::recovery::StartupStep;
use broker_core::storage::segment::Segment;
use broker_core::storage::sync::FileSync;
use broker_core::types::{Offset, PartitionId};
use super::driver::Rig;
use super::support::*;

fn stopped_prefix() -> Rig {
    let mut rig = Rig::new();
    for _ in 0..3 { assert!(rig.append()); }
    let zero = rig.fetch(0, 3, 120);
    rig.ack(0, zero[0]); rig.ack(0, zero[1]);
    rig.checkpoint(0, false);
    rig.ack(0, zero[2]); // Volatile3, persisted2.
    let one = rig.fetch(1, 3, 120);
    rig.ack(1, one[0]); rig.checkpoint(1, false);
    rig.ack(1, one[2]); // A completed follower remains behind the gap at1.
    rig.stop();
    rig
}
```

The imported `StorageFile` trait brings its `sync` method into method-call scope. The generic `FaultFile<File>` wrapper is the existing Chapter 7 I/O seam, not a new fake filesystem. The directory lock is acquired while a low-level fixture writes a segment, and released before recovery tries to acquire ownership again.

## Produce a partial tail through short writes, not by slicing a finished file

The helper below makes every low-level write at most three bytes, injects one `Interrupted` result, then refuses further writes after exactly `cut` successful bytes. The production `Segment::append` calls the actual `write_fully` loop through that wrapper. The first error is retried; the later injected error is returned with a real written prefix left in the file.

Rust's [Write contract](https://doc.rust-lang.org/std/io/trait.Write.html#tymethod.write) allows successful short writes. The [write-all operation](https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all) describes the retry-until-complete behavior and treatment of interrupted writes. Our explicit loop was implemented earlier; this fixture composes that behavior with persistent recovery rather than replacing it with a vector manipulation.

```rust title="crates/broker-core/tests/schedules/recovery.rs"
fn partial_append(rig: &Rig, cut: usize) {
    assert!(rig.owner.is_none());
    assert!((1..40).contains(&cut));
    let _guard = DirectoryGuard::acquire(&rig.root).unwrap();
    let mut file = FaultFile::new(open_file(&segment(&rig.root, 2)).unwrap(), 3).unwrap();
    file.interrupt_next_write(); // write_fully must retry Interrupted without spending bytes.
    file.fail_writes_after(cut);
    let mut segment = Segment::open_strict(file,
        SegmentHeader { partition: p(), base: Offset::new(2) }, config()).unwrap();
    assert!(segment.append(&payload(99)).is_err());
    assert!(matches!(segment.append(&payload(100)), Err(StorageError::Poisoned)));
    drop(segment);
    assert_eq!(fs::metadata(super::support::segment(&rig.root, 2)).unwrap().len(), 72 + cut as u64);
}
fn is_payload_checksum(error: &StorageError) -> bool {
    match error {
        StorageError::InFile { source, .. } => is_payload_checksum(source),
        StorageError::Format { source: FormatError::PayloadChecksum { .. }, .. } => true,
        _ => false,
    }
}

```

After the append fails, the same segment object must reject further appends as poisoned. The file length must be exactly $72+k$, where $k$ is the successful prefix length. This verifies that the injected cut actually left the intended bytes, not merely that some error occurred.

The `is_payload_checksum` helper recursively unwraps the contextual `InFile` error and accepts **only** a payload-checksum mismatch. A generic `is_err()` assertion would also pass on a busy directory lock, which is not evidence that the corruption detector worked.

## Stop once after repair, then again after epoch replacement

For every nonempty incomplete prefix, $1\le k<40$, run the following schedule:

| Attempt | Last completed action | Active-file length | Authoritative epoch |
| --- | --- | ---: | ---: |
| Faulty append | Write $k$ bytes of record 3 | $72+k$ | 1 |
| Recovery 1 | Truncate to the last complete record; callback fails before repair sync | 72 | 1 |
| Recovery 2 | No repair needed; replace startup metadata; callback fails | 72 | 2 |
| Recovery 3 | Complete synchronization and startup publication | 72 | 3 |
| Recovery 4 | Repeat a clean startup | 72 | 4 |

For example, $k=35$ leaves a complete 32-byte record header and three of eight payload bytes: total active-file length 107. Recovery must trim to 72, not to 32 and not to 104. A 31-byte prefix lacks a complete record header, but its known fixed fields must still match the expected next offset before it can be called a permitted incomplete tail.

The post-repair error is an ordinary callback error. Closing the file after that error is not itself an observed successful repair sync. The next recovery must reconstruct and synchronize the actual valid prefix before returning a usable session. This is why idempotent rerun matters even when the first attempt already changed the length.

```rust title="crates/broker-core/tests/schedules/recovery.rs"
#[test]
fn every_partial_record_cut_survives_two_interrupted_recovery_attempts() {
    for cut in 1..40 {
        let mut trace = Trace::new(cut as u64, "partial_write_and_two_recovery_cuts");
        let rig = stopped_prefix();
        trace.step(format!("short writes of at most3 bytes, one Interrupted, then error after {cut}"));
        partial_append(&rig, cut);
        trace.step("first recovery truncates the active tail, then returns before repair sync");
        let mut repaired = false;
        let first = recovery::recover_with(&rig.root, &mut FileSync, &mut |point| {
            if let StartupStep::RepairApplied(repair) = point {
                assert_eq!((repair.before, repair.after), (72 + cut as u64, 72));
                repaired = true;
                return Err(io::Error::other("stop after truncation"));
            }
            Ok(())
        });
        assert!(first.is_err() && repaired);
        assert_eq!(fs::metadata(segment(&rig.root, 2)).unwrap().len(), 72);
        trace.step("second recovery needs no repair; return after replacing startup epoch");
        let mut renamed = false;
        let second = recovery::recover_with(&rig.root, &mut FileSync, &mut |point| {
            assert!(!matches!(point, StartupStep::RepairApplied(_)));
            if point == StartupStep::Metadata(PublicationStep::Renamed) {
                renamed = true;
                return Err(io::Error::other("stop after startup rename"));
            }
            Ok(())
        });
        assert!(second.is_err() && renamed);
        assert_eq!(metadata::read(&rig.root).unwrap().epoch().get(), 2);
        trace.step("third recovery publishes epoch3; checkpointed frontiers remain2 and1");
        let view = recovered_view(&rig.root);
        assert_eq!((view.epoch, view.floor, view.end, view.fronts, view.repairs), (3, 0, 3, [2, 1], 0));
        assert_eq!(view.payloads, (0..3).map(|id| payload(id).to_vec()).collect::<Vec<_>>());
        let files = record_files(&rig.root);
        let again = recovered_view(&rig.root);
        assert_eq!((again.epoch, again.fronts, again.repairs), (4, [2, 1], 0));
        assert_eq!(record_files(&rig.root), files);
    }
}

```

Both callbacks assert that they were reached. The second attempt also refuses any new `RepairApplied` event, since the first already removed the tail. The third successful recovery expects no repair and frontiers 2/1. Finally, exact record-file bytes are unchanged by the fourth recovery, while its epoch increases.

The test loops over 39 prefixes, not every possible arbitrary byte sequence. It covers every incomplete length of this fixed record, with its correct encoded prefix. Header corruption, impossible offsets, oversized claims, and other format errors remain in the earlier focused tests and the synthetic fixtures below.

## A complete unconfirmed tail is a different input

This time allow the whole append to finish, then fail the wrapper's synchronization method. `Segment::into_file` transfers its file owner out; no private segment method is exposed just for testing. Calling `StorageFile::sync` exercises the wrapper's failure behavior. Part 3 already tests how the real partition owner reacts to a failed sync; this fixture focuses on the resulting bytes passed into recovery.

```rust title="crates/broker-core/tests/schedules/recovery.rs"
#[test]
fn a_complete_record_after_injected_file_sync_failure_is_not_truncated() {
    let mut trace = Trace::new(0x1820, "complete_but_unconfirmed_tail");
    let rig = stopped_prefix();
    trace.step("append complete offset3 through FaultFile; fail its synchronization");
    {
        let _guard = DirectoryGuard::acquire(&rig.root).unwrap();
        let mut file = FaultFile::new(open_file(&segment(&rig.root, 2)).unwrap(), 3).unwrap();
        file.fail_syncs();
        let mut segment = Segment::open_strict(file,
            SegmentHeader { partition: p(), base: Offset::new(2) }, config()).unwrap();
        assert_eq!(segment.append(&payload(99)).unwrap().get(), 3);
        let mut file = segment.into_file();
        assert!(file.sync().is_err());
    }
    let files = record_files(&rig.root);
    trace.step("recover all complete bytes without claiming producer success");
    let view = recovered_view(&rig.root);
    assert_eq!((view.end, view.fronts, view.repairs), (4, [2, 1], 0));
    assert_eq!(view.payloads.last().unwrap(), &payload(99).to_vec());
    assert_eq!(record_files(&rig.root), files);
}

```

The appended record at offset 3 has input ID 99, making its preservation visible independently of its offset. Recovery retains four complete records, reports no repair, and leaves group progress at 2/1. Again, this ordinary returned-error experiment preserves those file contents; it does not guarantee that unsynchronized bytes survive every external failure.

## Complete bad checksums must not become convenient truncation points

The next fixture deliberately changes byte 64 of the base-2 file. Bytes 0–31 are the segment header and 32–63 are the record header; byte 64 is therefore inside the payload. The stored payload checksum is left unchanged, while the record remains complete and its header remains valid. That should produce a payload-checksum failure, not an incomplete-tail repair.

```rust title="crates/broker-core/tests/schedules/recovery.rs"
#[test]
fn synthetic_complete_payload_corruption_is_repeatedly_refused_unchanged() {
    let mut trace = Trace::new(0x1821, "synthetic_crc_corruption");
    let rig = stopped_prefix();
    trace.step("synthetic fixture: flip a payload byte in a complete active record");
    let path = segment(&rig.root, 2);
    let mut bytes = fs::read(&path).unwrap();
    bytes[64] ^= 0x80; // 32-byte segment header +32-byte record header.
    fs::write(&path, bytes).unwrap();
    let damaged = record_files(&rig.root);
    for attempt in 0..3 {
        trace.step(format!("recovery refusal {attempt}; no truncation, no new epoch"));
        let error = recovery::recover(&rig.root).err().expect("bad checksum must refuse startup");
        assert!(is_payload_checksum(&error), "{error:?}");
        assert_eq!(record_files(&rig.root), damaged);
        assert_eq!(metadata::read(&rig.root).unwrap().epoch().get(), 1);
    }
}

```

The refusal is repeated three times to check that a failed recovery does not silently normalize the damage on an earlier attempt. Record bytes stay identical to the damaged fixture, and no startup epoch is consumed because validation never reaches publication. A caller must diagnose or restore the damaged data deliberately; this chapter does not implement a salvage policy.

## Validate the later partition before repairing the earlier one

Construct a repairable partial record in partition 0 and a complete checksum failure in partition 3. The global recovery plan must reject the latter before changing the former. If startup repaired each partition as soon as it scanned it, the test's exact byte comparison would detect partition 0's unauthorized truncation even though startup ultimately returned an error.

```rust title="crates/broker-core/tests/schedules/recovery.rs"
#[test]
fn a_later_partition_corruption_prevents_an_earlier_tail_repair() {
    let mut trace = Trace::new(0x1822, "all_partitions_before_repair");
    let rig = stopped_prefix();
    partial_append(&rig, 35);
    trace.step("synthesize a complete bad record in partition3 while partition0 has a repairable tail");
    let path = rig.root.join("partition-3").join(broker_core::storage::log::segment_name(Offset::new(0)));
    {
        let _guard = DirectoryGuard::acquire(&rig.root).unwrap();
        let mut segment = Segment::open_strict(open_file(&path).unwrap(),
            SegmentHeader { partition: PartitionId::new(3).unwrap(), base: Offset::new(0) }, config()).unwrap();
        segment.append(&payload(700)).unwrap();
        let mut file = segment.into_file();
        file.sync().unwrap();
    }
    let mut bytes = fs::read(&path).unwrap();
    bytes[64] ^= 1;
    fs::write(&path, bytes).unwrap();
    let before = record_files(&rig.root);
    assert!(recovery::recover(&rig.root).is_err());
    assert_eq!(record_files(&rig.root), before);
    assert_eq!(fs::metadata(segment(&rig.root, 2)).unwrap().len(), 107);
}

```

The partition-3 fixture first writes and synchronizes a valid complete record through the storage API, then changes one payload byte. The corruption step itself is explicitly synthetic. Partition 0's expected length remains 107 bytes after refused startup. This test checks cross-partition planning, not cross-partition transactional append semantics.

## A valid candidate cannot substitute for established authority

A synchronized-looking or checksum-valid candidate is not automatically authoritative. Copy the valid group-0 checkpoint to its candidate name, then damage only the established checkpoint authority while partition 0 also contains a repairable tail. Recovery must refuse before repairing the tail and leave the invalid authority and candidate in place.

```rust title="crates/broker-core/tests/schedules/recovery.rs"
#[test]
fn a_valid_candidate_cannot_replace_a_corrupt_checkpoint_authority() {
    let mut trace = Trace::new(0x1823, "candidate_is_not_authority");
    let rig = stopped_prefix();
    partial_append(&rig, 35);
    trace.step("copy valid checkpoint into candidate, then corrupt only its authority");
    let authority = rig.root.join(checkpoint::name(p(), g(0)));
    let candidate = rig.root.join(checkpoint::pending_name(p(), g(0)));
    let mut bytes = fs::read(&authority).unwrap();
    fs::write(&candidate, &bytes).unwrap();
    bytes[20] ^= 1;
    fs::write(&authority, &bytes).unwrap();
    let before = record_files(&rig.root);
    assert!(recovery::recover(&rig.root).is_err());
    assert_eq!(record_files(&rig.root), before);
    assert_eq!(fs::read(&authority).unwrap(), bytes);
    assert!(candidate.exists());
    assert_eq!(metadata::read(&rig.root).unwrap().epoch().get(), 1);
}

```

The tempting shortcut is to promote the candidate and continue. That would invent an authority rule that the publisher never established, potentially selecting an interrupted future update. The fixture checks the original authority bytes, candidate presence, record bytes, and unchanged epoch rather than treating any error as sufficient evidence.

## Planning and applying are observable separate phases

The `PlanValidated` callback occurs before destructive work. Returning an error there must leave every record file unchanged. Rerunning without the fault should apply exactly one repair and restore the expected persisted frontiers.

```rust title="crates/broker-core/tests/schedules/recovery.rs"
#[test]
fn stopping_after_plan_validation_does_not_apply_the_planned_repair() {
    let mut trace = Trace::new(0x1824, "read_only_plan_boundary");
    let rig = stopped_prefix();
    partial_append(&rig, 20);
    let before = record_files(&rig.root);
    trace.step("observer returns error at PlanValidated before any destructive operation");
    let mut hit = false;
    let result = recovery::recover_with(&rig.root, &mut FileSync, &mut |point| {
        if point == StartupStep::PlanValidated {
            hit = true;
            return Err(io::Error::other("stop after read-only validation"));
        }
        Ok(())
    });
    assert!(result.is_err() && hit);
    assert_eq!(record_files(&rig.root), before);
    trace.step("rerun without fault; exactly one planned repair now applies");
    let view = recovered_view(&rig.root);
    assert_eq!((view.repairs, view.end, view.fronts), (1, 3, [2, 1]));
}

```

A first attempt can also fail after replacing the startup epoch despite performing no record repair at all. Repeat that failure twice. The next usable startup must skip both consumed epochs, while record contents and persisted group frontiers remain unchanged. A startup that never returned a session is not permission to reuse the metadata value it already exposed on disk.

```rust title="crates/broker-core/tests/schedules/recovery.rs"
#[test]
fn repeated_epoch_rename_failures_consume_epochs_without_moving_progress() {
    let mut trace = Trace::new(0x1825, "repeated_epoch_publication_failure");
    let rig = stopped_prefix();
    let files = record_files(&rig.root);
    for expected_epoch in [2, 3] {
        trace.step(format!("return error after publishing startup epoch{expected_epoch}"));
        let mut hit = false;
        let result = recovery::recover_with(&rig.root, &mut FileSync, &mut |point| {
            if point == StartupStep::Metadata(PublicationStep::Renamed) {
                hit = true;
                return Err(io::Error::other("publication outcome is not a usable session"));
            }
            Ok(())
        });
        assert!(result.is_err() && hit);
        assert_eq!(metadata::read(&rig.root).unwrap().epoch().get(), expected_epoch);
        assert_eq!(record_files(&rig.root), files);
    }
    let view = recovered_view(&rig.root);
    assert_eq!((view.epoch, view.end, view.fronts), (4, 3, [2, 1]));
    assert_eq!(record_files(&rig.root), files);
}
```

## Run the target, then replay one seed

From the fully assembled learner workspace:

```bash
cargo check --workspace --all-targets
cargo test -p broker-core --test schedules -- --list
cargo test -p broker-core --test schedules -- --test-threads=4
```

The target supplies **22 test functions**: eight sequence/generator tests, seven publication/deletion tests, and seven recovery tests. Internal loops additionally cover six 96-step schedules by default, 24 completion orders, five checkpoint publication cuts, five retained-floor publication cuts, and 39 incomplete record prefixes. Those loop cases are not counted as extra Cargo tests.

The focused result is expected to report 22 passed and 0 failed. This is an **expected local check, not an observed Rust result**. The authoring environment has no `rustc`, Cargo, or rustfmt; compilation, dependency resolution, actual test outcomes, and native macOS behavior remain unverified. No lockfile has been fabricated. The small independent byte/arithmetic checks used while preparing this text do not execute the Rust implementation.

Replay the generated schedule with one explicit hexadecimal seed:

```bash
BROKER_SCHEDULE_SEED=0x5eed cargo test -p broker-core --test schedules \
  sequences::seeded_sequences_match_the_reference -- --exact --nocapture --test-threads=1
```

The environment variable changes only that generated-sequence test. A failure in a directed publication or recovery case uses its printed case name and loop seed as a diagnostic label; rerun the corresponding named test, which deterministically enumerates its small case set. Do not expect the environment variable to select a partial-write cut in another test.

Cargo options go before `--`; test-harness options go after it. An exact filter that matches no test can succeed with zero tests run, so verify the reported count is one for the command above. The [Rust test-running chapter](https://doc.rust-lang.org/book/ch11-02-running-tests.html) explains filters, output capture, and test-thread control.

The bounded trace has this **format** on an assertion failure, not a claimed observed failure:

```text
schedule case=seeded_sequences_match_the_reference seed=0x0000000000005eed; last 24 transitions:
  sequence-number: operation and state description
```

Actual entries include the operation number, selected group, manual time, record end, retained floor, and any selected token or fetch limits. The original assertion supplies the mismatched values. Preserve the seed and code state; do not erase the failing case by switching seeds until the suite happens to pass.

Then run the cumulative targets and formatting check:

```bash
cargo test --workspace --all-targets -- --test-threads=4
cargo fmt --all -- --check
```

There are now **432 supplied tests** across the required workspace targets: the previous 410 plus this chapter's 22. They are not 432 observed passes. A formatting difference calls for running the formatter and reviewing the diff, not changing a durability assertion. A compile error should be fixed before interpreting any absent test result as success.

The earlier process-kill harnesses remain useful complementary checks. Their complete commands and code are in Chapters 10, 13, and 14. This chapter adds no new process-kill claim and does not count rerunning those harnesses as a new implementation mechanism.

## Diagnose the boundary that failed

| Symptom | Inspect first | Do not substitute |
| --- | --- | --- |
| Expected `DiskFull`, but a ticket was admitted | Worst-case 32-byte rotation reservation and outstanding permits | Treating current active-file room as guaranteed room for every queued append |
| A follower completion frees window capacity | Contiguous retirement and map/ring comparison | Saving only the highest completed offset |
| A nonreturned retry makes an old token stale | Whether attempt installation happened before the byte-limited copy succeeded | Increasing response capacity to hide the boundary |
| Lost-append test intermittently sees no record | Same-data-lane successor reply and count-one commit | A control-lane snapshot or sleep as a barrier |
| A cut test fails but `hit` is false | Exact target filename, selected step, and occurrence counter | Calling every early error a successful injection |
| Error after rename restores an old checkpoint | Which file is authoritative after replacement | Assuming response failure rolled back namespace changes |
| Repeated cleanup loses the retained-floor file | Obsolete-prefix classification and publication-before-delete ordering | Treating all missing segments as acceptable |
| Bad complete payload is trimmed | Incomplete-input classification versus checksum rejection | Calling corruption a partial tail |
| Recovery cannot reacquire the lock | A still-live log/session/owner from the fixture | Deleting the stable lock file |
| Clean replay changes record bytes | Repair plan, obsolete cleanup, and byte comparison scope | Ignoring the difference because epoch growth was expected |

## A required edit-and-restore prediction

Use `a_byte_limited_retry_does_not_install_the_unreturned_attempt`. Before editing, predict the effect of changing its response limit from 79 to 80 encoded bytes. Both 40-byte retries should now be returned, both generations should advance, and both old tokens should be stale-generation rejections. No old token should remain merely expired.

Make that one limit change, replace the single-return/skipped-token assertions with checks for two returned retries and two stale old tokens, and rerun that named test. The existing `Rig::fetch` model computes the new count automatically. Restore the canonical 79-byte boundary test afterward so the nonfitting path remains covered. This exercise is included in the chapter's diagnosis time; it is not a request to invent new broker behavior.

A second useful interpretation exercise needs no code change: after `checkpoint_publication_cuts` fails at `DirectorySynced`, explain why the caller sees failure but the next startup sees group 0 at four. The answer must refer to operation order and authoritative state, not to a generic claim that failures are always ambiguous.

## What these checks establish, and what they leave open

You now have reproducible inputs, a structurally different bounded reference state, exact failure boundaries, and complete destructive fixtures that only touch newly created data. You can distinguish accepted completion, persisted progress, retained data, and externally observed replies across multiple transitions. Recovery can be interrupted after doing real work and then rerun without inventing rollback or recycling an epoch.

The schedules are not an exhaustive state-space exploration, a universal concurrency proof, a power-loss simulator, or a performance measurement. They deliberately reuse focused tests for ring mechanics, clocks, notification races, pooled resource lifetimes, and supervision instead of reimplementing those checks under a new name. A failing reference assertion still needs diagnosis: the broker, the oracle, or the fixture's stated preconditions can each be wrong.

The next chapter builds the external expected-record ledger and end-to-end overload demonstration. That ledger will live outside the broker's failure domain and distinguish confirmed, ambiguous, rejected, delivered, and checkpointed outcomes. The small histories here supply the semantics that make those larger observations interpretable.
