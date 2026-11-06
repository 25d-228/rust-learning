use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Drop for Directory { fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); } }
fn fixture() -> (Directory, Supervisor, tokio::runtime::Runtime) {
    let directory = Directory(std::env::temp_dir().join(format!("broker-supervisor-unit-{}-{}",
        std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))));
    let broker = LocalBroker::open(&directory.0, true).unwrap();
    let config = SupervisorConfig { capacity: [1, 1], network_grace: Duration::ZERO,
        ..SupervisorConfig::default() };
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    (directory, Supervisor::new(broker, config).unwrap(), runtime)
}
fn track(supervisor: &mut Supervisor, future: impl Future<Output = io::Result<()>> + Send + 'static) {
    let task = supervisor.tasks.spawn(future);
    supervisor.scopes.insert(task.id(), 0);
    supervisor.active[0] += 1;
    supervisor.stats[0].accepted += 1;
}
fn complete(supervisor: &mut Supervisor, runtime: &tokio::runtime::Runtime) -> ShutdownReport {
    supervisor.request_shutdown();
    let result = runtime.block_on(supervisor.drain_until(Instant::now() + Duration::from_secs(10))).unwrap();
    assert_eq!(result, DrainStatus::ReadyToJoin);
    supervisor.join().unwrap()
}
#[test]
fn a_panicking_child_is_counted_and_causes_supervised_failure() {
    let (_directory, mut supervisor, runtime) = fixture();
    runtime.block_on(async {
        let address: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 0).into();
        supervisor.bind(address, address).await.unwrap();
        track(&mut supervisor, async { panic!("injected session panic") });
        tokio::time::timeout(Duration::from_secs(10), supervisor.serve_until(std::future::pending()))
            .await.unwrap().unwrap();
    });
    let report = complete(&mut supervisor, &runtime);
    assert_eq!(report.phase, Phase::Failed); assert_eq!(report.sessions[0].panicked, 1);
    assert_eq!(report.active_sessions, 0); assert!(supervisor.scopes.is_empty());
}
#[test]
fn forced_tasks_are_joined_and_counted_exactly_once() {
    let (_directory, mut supervisor, runtime) = fixture();
    runtime.block_on(async { track(&mut supervisor, std::future::pending()); });
    let report = complete(&mut supervisor, &runtime);
    assert_eq!(report.phase, Phase::Stopped); assert!(report.network_forced);
    assert_eq!(report.sessions[0].accepted, 1); assert_eq!(report.sessions[0].cancelled, 1);
    assert_eq!(supervisor.active, [0, 0]); assert!(supervisor.scopes.is_empty());
}
#[test]
fn repeated_shutdown_cannot_extend_the_original_network_deadline() {
    let (_directory, mut supervisor, runtime) = fixture();
    supervisor.request_shutdown();
    let recorded = Instant::now() + Duration::from_secs(50);
    supervisor.network_deadline = Some(recorded);
    supervisor.request_shutdown();
    assert_eq!(supervisor.network_deadline, Some(recorded));
    assert_eq!(supervisor.cause, Some(StopCause::Requested));
    assert_eq!(complete(&mut supervisor, &runtime).phase, Phase::Stopped);
}
