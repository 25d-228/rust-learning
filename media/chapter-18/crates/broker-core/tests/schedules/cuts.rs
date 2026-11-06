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
