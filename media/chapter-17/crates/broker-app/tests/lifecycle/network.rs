use std::future::{Future, pending};
use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, timeout};
use broker_app::protocol::{self, Endpoint, Header, Op, Status};
use broker_app::server::{SessionLimits, session_until};
use broker_app::supervisor::{DrainStatus, Phase, StopCause, Supervisor, SupervisorConfig};
use broker_core::worker::OwnerExit;
use super::support::*;

fn append_header() -> Header {
    let mut header = Header::request(Op::Append, p(0), g(0)); header.body = 3; header
}
async fn drive<F: Future>(supervisor: &mut Supervisor, activity: F) -> F::Output {
    let (stop, requested) = oneshot::channel();
    let serving = supervisor.serve_until(async { requested.await.map_err(io::Error::other) });
    let activity = async { let value = activity.await; let _ = stop.send(()); value };
    let (served, value) = tokio::join!(serving, activity);
    served.unwrap(); value
}

#[test]
fn partial_header_is_cancelled_only_with_its_socket() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        let (mut peer, stream) = tokio::io::duplex(128);
        peer.write_all(&append_header().encode()[..20]).await.unwrap();
        let (stop, changed) = watch::channel(false);
        let session = session_until(stream, Endpoint::Producer, rig.clients.clone(), SessionLimits::default(), changed);
        tokio::pin!(session); pending_once(session.as_mut()).await;
        stop.send_replace(true);
        timeout(Duration::from_secs(1), session).await.unwrap().unwrap();
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert_eq!(rig.clients[0].memory_state().append.checked_out_bytes, 0);
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn losing_the_session_stop_sender_closes_an_idle_session() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        let (mut peer, stream) = tokio::io::duplex(128);
        let (stop, changed) = watch::channel(false); drop(stop);
        session_until(stream, Endpoint::Consumer, rig.clients.clone(), SessionLimits::default(), changed).await.unwrap();
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    });
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn forcing_a_partial_body_returns_its_unsubmitted_buffer_and_disk_credit() {
    let runtime = runtime();
    let mut rig = Rig::configured(owners(), SupervisorConfig { network_grace: Duration::ZERO,
        ..SupervisorConfig::default() });
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let client = rig.clients[0].clone();
    let _peer = runtime.block_on(drive(&mut rig.supervisor, async {
        let mut peer = TcpStream::connect(addresses[0]).await.unwrap();
        peer.write_all(&append_header().encode()).await.unwrap(); peer.write_all(b"c").await.unwrap();
        eventually(|| client.memory_state().append.checked_out_bytes > 0).await;
        peer
    }));
    let report = rig.stop_and_join(&runtime);
    assert!(report.network_forced); assert_eq!(report.sessions[0].cancelled, 1);
    assert_eq!(report.phase, Phase::Stopped);
    assert_eq!(client.memory_state().append.checked_out_bytes, 0);
    assert_eq!(client.disk_state().reserved_bytes, 0);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Stopped { state, .. }) if state.visible_end.get() == 0));
}
#[test]
fn accepted_append_can_finish_its_response_during_the_grace_period() {
    let runtime = runtime(); let mut rig = Rig::new();
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Pass);
    let mut peer = runtime.block_on(drive(&mut rig.supervisor, async {
        let mut peer = TcpStream::connect(addresses[0]).await.unwrap();
        peer.write_all(&append_header().encode()).await.unwrap(); peer.write_all(b"cat").await.unwrap();
        entered(ready).await; peer
    }));
    rig.release();
    runtime.block_on(async {
        let (drain, response) = tokio::join!(
            rig.supervisor.drain_until(Instant::now() + Duration::from_secs(10)),
            protocol::read_header(&mut peer, Instant::now() + Duration::from_secs(10)));
        assert_eq!(drain.unwrap(), DrainStatus::ReadyToJoin);
        let response = response.unwrap().unwrap(); assert_eq!(response.status, Status::Ok);
        assert_eq!(response.args[0], 0);
    });
    let report = rig.supervisor.join().unwrap();
    assert_eq!(report.phase, Phase::Stopped); assert!(!report.network_forced);
    assert_eq!(report.sessions[0].completed, 1);
}
#[test]
fn network_deadline_does_not_cancel_or_falsely_confirm_an_owner_append() {
    let runtime = runtime();
    let mut rig = Rig::configured(owners(), SupervisorConfig { network_grace: Duration::ZERO,
        ..SupervisorConfig::default() });
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Pass);
    let mut peer = runtime.block_on(drive(&mut rig.supervisor, async {
        let mut peer = TcpStream::connect(addresses[0]).await.unwrap();
        peer.write_all(&append_header().encode()).await.unwrap(); peer.write_all(b"cat").await.unwrap();
        entered(ready).await; peer
    }));
    runtime.block_on(async {
        timeout(Duration::from_secs(10), async {
            loop {
                let result = rig.supervisor.drain_until(Instant::now() + Duration::from_millis(20)).await.unwrap();
                assert_eq!(result, DrainStatus::Pending); // The owner is still held at the gate.
                if rig.supervisor.report().active_sessions == 0 { break; }
            }
        }).await.expect("cancelled session was not joined");
        let response = protocol::read_header(&mut peer, Instant::now() + Duration::from_secs(1)).await;
        assert!(!matches!(response, Ok(Some(header)) if header.status == Status::Ok));
    });
    assert_eq!(rig.supervisor.report().active_sessions, 0);
    assert_eq!(rig.supervisor.phase(), Phase::Draining);
    rig.release(); let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Stopped); assert!(report.network_forced);
    assert!(matches!(&report.owners[0], Some(OwnerExit::Stopped { state, .. }) if state.visible_end.get() == 1));
}
#[test]
fn cancelling_the_serving_borrow_does_not_drop_its_session_set() {
    let runtime = runtime(); let mut rig = Rig::new();
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    let ready = rig.gate(AfterGate::Pass);
    let _peer = runtime.block_on(async {
        let serving = rig.supervisor.serve_until(pending()); tokio::pin!(serving);
        let activity = async {
            let mut peer = TcpStream::connect(addresses[0]).await.unwrap();
            peer.write_all(&append_header().encode()).await.unwrap(); peer.write_all(b"cat").await.unwrap();
            entered(ready).await; peer
        };
        tokio::select! {
            peer = activity => peer,
            result = &mut serving => panic!("serving ended before cancellation: {result:?}"),
        }
    });
    assert_eq!(rig.supervisor.phase(), Phase::Serving);
    assert_eq!(rig.supervisor.report().active_sessions, 1);
    rig.supervisor.request_shutdown(); rig.release();
    assert_eq!(rig.stop_and_join(&runtime).phase, Phase::Stopped);
}
#[test]
fn bind_failure_enters_cleanup_instead_of_leaking_started_owners() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        let held = TcpListener::bind(address()).await.unwrap();
        assert!(rig.supervisor.bind(address(), held.local_addr().unwrap()).await.is_err());
    });
    assert_eq!(rig.supervisor.phase(), Phase::Draining);
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Failed); assert_eq!(report.cause, Some(StopCause::StartupFailed));
    assert_eq!(report.active_sessions, 0); assert_eq!(report.pending_owners, 0);
}
#[test]
fn malformed_peer_is_a_session_error_not_an_owner_failure() {
    let runtime = runtime(); let mut rig = Rig::new();
    let addresses = runtime.block_on(rig.supervisor.bind(address(), address())).unwrap();
    runtime.block_on(drive(&mut rig.supervisor, async {
        let mut peer = TcpStream::connect(addresses[1]).await.unwrap();
        peer.write_all(&[0; 64]).await.unwrap();
        let _ = timeout(Duration::from_secs(2), peer.read(&mut [0; 1])).await.unwrap();
    }));
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.phase, Phase::Stopped); assert_eq!(report.sessions[1].session_errors, 1);
}
#[test]
fn signal_registration_error_is_failed_only_after_children_are_cleaned_up() {
    let runtime = runtime(); let mut rig = Rig::new();
    runtime.block_on(async {
        rig.supervisor.bind(address(), address()).await.unwrap();
        rig.supervisor.serve_until(async { Err(io::Error::other("signal unavailable")) }).await.unwrap();
    });
    let report = rig.stop_and_join(&runtime);
    assert_eq!(report.cause, Some(StopCause::SignalFailed)); assert_eq!(report.phase, Phase::Failed);
    assert_eq!(report.pending_owners, 0); assert_eq!(report.active_sessions, 0);
}
