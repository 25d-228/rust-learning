use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;
use broker_app::local::LocalBroker;
use broker_app::supervisor::{DrainStatus, Phase, SupervisorConfig};
use broker_core::mailbox::AdmissionError;
use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::segment::ReadLimits;
use broker_core::worker::OwnerExit;
use super::support::*;

#[test]
fn configuration_bounds_both_listeners_and_allows_immediate_network_stop() {
    let mut config = SupervisorConfig { network_grace: Duration::ZERO, ..SupervisorConfig::default() };
    assert!(config.validate().is_ok());
    config.capacity[0] = 0; assert!(config.validate().is_err());
    config.capacity = [64, 64]; assert!(config.validate().is_ok());
    config.capacity[1] = 65; assert!(config.validate().is_err());
    config.capacity = [1, 1]; config.network_grace = Duration::from_secs(61);
    assert!(config.validate().is_err());
}
#[test]
fn starting_can_drain_without_binding_a_listener() {
    let runtime = runtime(); let mut rig = Rig::new();
    assert_eq!(rig.supervisor.phase(), Phase::Starting);
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Stopped);
    assert_eq!(report.active_sessions, 0); assert_eq!(report.pending_owners, 0);
    assert!(report.owners.iter().all(|exit| matches!(exit,
        Some(OwnerExit::Stopped { checkpointed: true, .. }))));
    assert!(DirectoryGuard::acquire(&rig.root.0).is_ok());
}
#[test]
fn reservations_are_not_committed_work_and_cannot_submit_after_quiesce() {
    let runtime = runtime(); let mut rig = Rig::new();
    let append = rig.clients[0].try_prepare_append(3).unwrap();
    let fetch = rig.clients[0].try_prepare_lease(g(0), ReadLimits::new(1, 64).unwrap()).unwrap();
    rig.supervisor.request_shutdown();
    assert!(matches!(append.submit(), Err(AdmissionError::Closed)));
    assert!(matches!(fetch.submit(), Err(AdmissionError::Closed)));
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Stopped);
    assert_eq!(rig.clients[0].memory_state().append.checked_out_bytes, 0);
    assert_eq!(rig.clients[0].memory_state().response.checked_out_bytes, 0);
    assert_eq!(rig.clients[0].disk_state().reserved_bytes, 0);
}
#[test]
fn full_data_query_and_urgent_lanes_need_no_extra_finish_slot() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let first = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready));
    let second = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    let third = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    let queries = [rig.clients[0].try_snapshot().unwrap(), rig.clients[0].try_snapshot().unwrap()];
    let controls = [rig.clients[0].try_checkpoint(g(0)).unwrap(), rig.clients[0].try_checkpoint(g(1)).unwrap()];
    assert!(matches!(rig.clients[0].try_snapshot(), Err(AdmissionError::Full)));
    assert!(matches!(rig.clients[0].try_checkpoint(g(0)), Err(AdmissionError::Full)));
    rig.supervisor.request_shutdown();
    rig.release();
    runtime.block_on(async {
        assert_eq!(reply(first).await.get(), 0);
        assert_eq!(reply(second).await.get(), 1);
        assert_eq!(reply(third).await.get(), 2);
        for ticket in queries { reply(ticket).await; }
        for ticket in controls { reply(ticket).await; }
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn append_admission_waiter_is_notified_and_observes_closed() {
    let runtime = runtime(); let mut rig = Rig::new();
    let client = rig.clients[0].clone();
    let held = [client.try_prepare_append(1).unwrap(), client.try_prepare_append(1).unwrap()];
    runtime.block_on(async {
        let wait = client.prepare_append(1, Instant::now() + Duration::from_secs(10));
        tokio::pin!(wait);
        let count = poll_with_counter(wait.as_mut());
        assert_eq!(client.memory_state().append_waiters, 1);
        rig.supervisor.request_shutdown();
        assert!(count.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(wait.await, Err(AdmissionError::Closed)));
    });
    drop(held); assert_eq!(client.memory_state().append_waiters, 0);
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn response_admission_waiter_is_notified_and_observes_closed() {
    let runtime = runtime(); let mut rig = Rig::new();
    let client = rig.clients[0].clone(); let limits = ReadLimits::new(1, 64).unwrap();
    let held = [client.try_prepare_lease(g(0), limits).unwrap(), client.try_prepare_lease(g(1), limits).unwrap()];
    runtime.block_on(async {
        let wait = client.prepare_lease(g(0), limits, Instant::now() + Duration::from_secs(10));
        tokio::pin!(wait);
        let count = poll_with_counter(wait.as_mut());
        assert_eq!(client.memory_state().response_waiters, 1);
        rig.supervisor.request_shutdown();
        assert!(count.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(wait.await, Err(AdmissionError::Closed)));
    });
    drop(held); assert_eq!(client.memory_state().response_waiters, 0);
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn cancelling_an_admission_future_releases_its_waiter_not_someone_elses_buffers() {
    let runtime = runtime(); let mut rig = Rig::new();
    let client = rig.clients[0].clone();
    let held = [client.try_prepare_append(1).unwrap(), client.try_prepare_append(1).unwrap()];
    runtime.block_on(async {
        {
            let wait = client.prepare_append(1, Instant::now() + Duration::from_secs(10));
            tokio::pin!(wait); pending_once(wait.as_mut()).await;
            assert_eq!(client.memory_state().append_waiters, 1);
        }
        assert_eq!(client.memory_state().append_waiters, 0);
        assert_eq!(client.memory_state().append.free_buffers, 0);
    });
    drop(held); assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn final_checkpoint_stops_at_a_gap_and_does_not_complete_another_group() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        for payload in [b"cat", b"dog", b"owl"] { append(&rig.clients[0], payload).await; }
        let batch = fetch(&rig.clients[0], 0, 3).await;
        let tokens: Vec<_> = batch.records().map(|record| record.token).collect();
        ack(&rig.clients[0], tokens[2]).await; ack(&rig.clients[0], tokens[0]).await;
        assert_eq!(rig.saved(0), 0);
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
    assert_eq!(rig.saved(0), 1); assert_eq!(rig.saved(1), 0);
}
#[test]
fn both_groups_restart_at_their_independent_final_frontiers() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        for payload in [b"cat", b"dog"] { append(&rig.clients[0], payload).await; }
        for group in 0..2 {
            let batch = fetch(&rig.clients[0], group, 2).await;
            for token in batch.records().take(group as usize + 1).map(|record| record.token) {
                ack(&rig.clients[0], token).await;
            }
        }
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
    drop(rig.clients);
    let restarted = LocalBroker::open(&rig.root.0, false).unwrap();
    let clients = restarted.clients();
    runtime.block_on(async {
        assert_eq!(reply(clients[0].try_progress(g(0)).unwrap()).await.persisted_next.get(), 1);
        assert_eq!(reply(clients[0].try_progress(g(1)).unwrap()).await.persisted_next.get(), 2);
    });
    drop(clients); restarted.join().unwrap();
}
#[test]
fn response_bytes_can_outlive_join_without_keeping_the_storage_lock() {
    let runtime = runtime(); let mut rig = Rig::new();
    let batch = runtime.block_on(async {
        append(&rig.clients[0], b"cat").await; fetch(&rig.clients[0], 0, 1).await
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
    assert_eq!(batch.records().next().unwrap().payload(), b"cat");
    assert!(rig.clients[0].memory_state().response.checked_out_bytes >= 4096);
    assert!(DirectoryGuard::acquire(&rig.root.0).is_ok());
    drop(batch);
    assert_eq!(rig.clients[0].memory_state().response.checked_out_bytes, 0);
    assert!(rig.clients[0].memory_state().response.allocated_bytes > 0);
}
#[test]
fn an_observation_timeout_is_pending_not_a_terminal_phase() {
    let runtime = runtime(); let mut rig = Rig::new();
    let ready = rig.gate(AfterGate::Pass);
    let ticket = rig.clients[0].try_prepare_append(1).unwrap().submit().unwrap();
    runtime.block_on(entered(ready)); rig.supervisor.request_shutdown();
    let outcome = runtime.block_on(rig.supervisor.drain_until(Instant::now())).unwrap();
    assert_eq!(outcome, DrainStatus::Pending); assert_eq!(rig.supervisor.phase(), Phase::Draining);
    assert_eq!(rig.supervisor.join().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
    rig.release(); runtime.block_on(reply(ticket));
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
