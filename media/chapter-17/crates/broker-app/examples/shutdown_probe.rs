use std::io;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{Instant, timeout};
use broker_app::BoxError;
use broker_app::local::LocalBroker;
use broker_app::supervisor::{DrainStatus, Phase, Supervisor, SupervisorConfig};
use broker_core::delivery::progress::ProgressAck;
use broker_core::storage::segment::ReadLimits;
use broker_core::types::GroupId;
use broker_core::worker::AsyncTicket;

async fn receive<T>(ticket: AsyncTicket<T>) -> Result<T, BoxError> {
    let delivered = timeout(Duration::from_secs(10), ticket).await?;
    let result = delivered?;
    Ok(result?)
}
fn main() -> Result<(), BoxError> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: shutdown_probe NEW_DIRECTORY")?);
    if args.next().is_some() { return Err("usage: shutdown_probe NEW_DIRECTORY".into()); }
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let broker = LocalBroker::open(&root, true)?;
    let clients = broker.clients();
    let mut supervisor = Supervisor::new(broker, SupervisorConfig::default())?;
    let g0 = GroupId::new(0)?; let g1 = GroupId::new(1)?;
    let exercise: Result<(), BoxError> = runtime.block_on(async {
        // Collect an exercise error, then still perform the normal shutdown protocol.
        let result: Result<(), BoxError> = async {
            for payload in [b"cat", b"dog"] {
                let mut request = clients[0].try_prepare_append(3)
                    .map_err(|error| io::Error::other(format!("admission: {error:?}")))?;
                request.payload_mut().copy_from_slice(payload);
                receive(request.submit().map_err(|error| io::Error::other(format!("submit: {error:?}")))?).await?;
            }
            let request = clients[0].try_prepare_lease(g0, ReadLimits::new(2, 128)?)
                .map_err(|error| io::Error::other(format!("fetch admission: {error:?}")))?;
            let batch = receive(request.submit().map_err(|error| io::Error::other(format!("fetch submit: {error:?}")))?).await?;
            let tokens: Vec<_> = batch.records().map(|record| record.token).collect();
            if tokens.len() != 2 { return Err("expected two leased records".into()); }
            drop(batch);
            for token in [tokens[1], tokens[0]] {
                let ticket = clients[0].try_ack_progress(g0, token)
                    .map_err(|error| io::Error::other(format!("ack admission: {error:?}")))?;
                if !matches!(receive(ticket).await?, ProgressAck::Accepted(_)) {
                    return Err("acknowledgement was not accepted".into());
                }
            }
            let ticket = clients[0].try_progress(g0)
                .map_err(|error| io::Error::other(format!("progress admission: {error:?}")))?;
            let before = receive(ticket).await?;
            println!("before shutdown: group0 volatile={} persisted={}",
                before.volatile_next.get(), before.persisted_next.get());
            if before.volatile_next.get() != 2 || before.persisted_next.get() != 0 {
                return Err("unexpected pre-shutdown progress".into());
            }
            Ok(())
        }.await;
        supervisor.request_shutdown();
        loop {
            match supervisor.drain_until(Instant::now() + Duration::from_secs(5)).await? {
                DrainStatus::ReadyToJoin => break,
                DrainStatus::Pending => eprintln!("unfinished shutdown: {:?}", supervisor.report()),
            }
        }
        result
    });
    let report = supervisor.join()?;
    println!("shutdown: {:?}; sessions={}; owners_pending={}",
        report.phase, report.active_sessions, report.pending_owners);
    // The new generation below must not overlap this generation's client-owned pools.
    drop(clients); drop(supervisor);
    exercise?;
    if report.phase != Phase::Stopped { return Err("shutdown failed; inspect its report".into()); }
    let restarted = LocalBroker::open(&root, false)?;
    let clients = restarted.clients();
    let verification: Result<(), BoxError> = runtime.block_on(async {
        for group in [g0, g1] {
            let ticket = clients[0].try_progress(group)
                .map_err(|error| io::Error::other(format!("progress admission: {error:?}")))?;
            let progress = receive(ticket).await?;
            let expected = if group == g0 { 2 } else { 0 };
            if progress.volatile_next.get() != expected || progress.persisted_next.get() != expected {
                return Err("restart did not use the saved contiguous frontier".into());
            }
            println!("after restart: group{} volatile={} persisted={}", group.get(),
                progress.volatile_next.get(), progress.persisted_next.get());
        }
        Ok(())
    });
    drop(clients);
    let joined = restarted.join();
    verification?; joined?;
    Ok(())
}
