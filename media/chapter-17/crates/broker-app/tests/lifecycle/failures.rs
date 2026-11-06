use std::future::pending;
use std::time::Duration;
use tokio::time::{Instant, timeout};
use broker_app::supervisor::{DrainStatus, Phase, StopCause};
use broker_core::mailbox::AdmissionError;
use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::recovery;
use broker_core::worker::{OwnerExit, RequestError};
use super::support::*;

#[test]
fn blocked_sync_keeps_the_lock_and_rejects_new_admission_after_quiesce() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let ticket = rig.clients[0].try_prepare_append(3).unwrap().submit().unwrap();
    runtime.block_on(entered(ready));
    rig.supervisor.request_shutdown();
    assert!(DirectoryGuard::acquire(&rig.root.0).is_err());
    assert!(recovery::recover(&rig.root.0).is_err());
    assert!(matches!(rig.clients[0].try_prepare_append(1), Err(AdmissionError::Closed)));
    assert!(matches!(rig.clients[0].try_checkpoint(g(0)), Err(AdmissionError::Closed)));
    rig.release(); runtime.block_on(reply(ticket));
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn an_owner_failure_is_detected_without_waiting_for_its_abandoned_reply() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Fail);
    drop(rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap());
    runtime.block_on(entered(ready)); rig.release();
    runtime.block_on(async {
        timeout(Duration::from_secs(10), rig.supervisor.serve_until(pending())).await.unwrap().unwrap();
    });
    assert_eq!(rig.supervisor.report().cause, Some(StopCause::OwnerExited));
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Failed);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Failed { .. })));
    assert_eq!(report.pending_owners, 0);
}
#[test]
fn owner_panic_is_fatal_but_the_other_three_owners_are_still_joined() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Panic);
    let ticket = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready)); rig.release();
    runtime.block_on(async {
        timeout(Duration::from_secs(10), rig.supervisor.serve_until(pending())).await.unwrap().unwrap();
        assert!(matches!(ticket.await.unwrap(), Err(RequestError::OwnerStopped)));
    });
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Failed);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Panicked)));
    assert!(report.owners[1..].iter().all(|notice| matches!(notice,
        Some(OwnerExit::Stopped { checkpointed: true, .. }))));
}
#[test]
fn failed_final_checkpoint_does_not_claim_persisted_progress() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        append(&rig.clients[0], b"cat").await;
        let batch = fetch(&rig.clients[0], 0, 1).await;
        ack(&rig.clients[0], batch.records().next().unwrap().token).await;
    });
    rig.fail_checkpoint(1);
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Failed); assert_eq!(rig.saved(0), 0);
    assert!(report.first_failure.unwrap().contains("checkpoint"));
}
#[test]
fn later_checkpoint_failure_does_not_roll_back_an_earlier_group() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        append(&rig.clients[0], b"cat").await;
        for group in 0..2 {
            let batch = fetch(&rig.clients[0], group, 1).await;
            ack(&rig.clients[0], batch.records().next().unwrap().token).await;
        }
    });
    rig.fail_checkpoint(2);
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Failed);
    assert_eq!(rig.saved(0), 1); assert_eq!(rig.saved(1), 0);
}
#[test]
fn requested_shutdown_does_not_hide_a_later_sync_failure() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Fail);
    let ticket = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready));
    rig.supervisor.request_shutdown(); rig.release();
    runtime.block_on(async { assert!(matches!(ticket.await.unwrap(), Err(RequestError::Failed(_)))); });
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.cause, Some(StopCause::Requested)); assert_eq!(report.phase, Phase::Failed);
}
#[test]
fn abandoning_a_ticket_does_not_remove_a_successfully_drained_append() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let mut request = rig.clients[0].try_prepare_append(3).unwrap();
    request.payload_mut().copy_from_slice(b"cat");
    drop(request.submit().unwrap()); runtime.block_on(entered(ready));
    rig.supervisor.request_shutdown(); rig.release();
    let report = rig.stop_and_join(&runtime);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Stopped { state, .. }) if state.visible_end.get() == 1));
    // Drop client-owned pool generations before creating a new service generation.
    drop(rig.clients);
    let session = recovery::recover(&rig.root.0).unwrap();
    let (_, mut logs) = session.into_parts();
    let batch = logs[0].read(broker_core::types::Offset::new(0), broker_core::types::Offset::new(1),
        broker_core::storage::segment::ReadLimits::new(1, 64).unwrap()).unwrap();
    assert_eq!(batch.records[0].payload(), b"cat");
}
#[test]
fn cancelling_a_borrowed_drain_keeps_its_owner_handles_for_resumption() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let ticket = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready)); rig.supervisor.request_shutdown();
    runtime.block_on(async {
        let drain = rig.supervisor.drain_until(Instant::now() + Duration::from_secs(10));
        tokio::pin!(drain); pending_once(drain.as_mut()).await;
        // The owning pinned future ends at this scope; the Supervisor does not.
    });
    assert_eq!(rig.supervisor.phase(), Phase::Draining);
    assert_eq!(runtime.block_on(rig.supervisor.drain_until(Instant::now())).unwrap(), DrainStatus::Pending);
    rig.release(); runtime.block_on(reply(ticket));
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
