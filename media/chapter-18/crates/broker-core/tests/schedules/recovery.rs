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
