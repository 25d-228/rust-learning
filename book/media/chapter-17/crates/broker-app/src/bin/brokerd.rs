use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::Instant;
use broker_app::BoxError;
use broker_app::local::LocalBroker;
use broker_app::supervisor::{DrainStatus, Phase, Supervisor, SupervisorConfig};

fn main() -> Result<(), BoxError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 || !matches!(args[0].as_str(), "init" | "open") {
        return Err("usage: brokerd init|open DIRECTORY PRODUCER_PORT CONSUMER_PORT".into());
    }
    let root = PathBuf::from(&args[1]);
    let producer_port: u16 = args[2].parse()?;
    let consumer_port: u16 = args[3].parse()?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let broker = LocalBroker::open(&root, args[0] == "init")?;
    // The supervisor outlives the borrowed asynchronous drive futures.
    let mut supervisor = Supervisor::new(broker, SupervisorConfig::default())?;
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    runtime.block_on(async {
        match supervisor.bind(SocketAddr::new(ip, producer_port), SocketAddr::new(ip, consumer_port)).await {
            Ok(addresses) => {
                println!("LISTEN producer={} consumer={}", addresses[0], addresses[1]);
                supervisor.serve_until(tokio::signal::ctrl_c()).await?;
            }
            Err(error) => eprintln!("listener startup failed: {error}"),
        }
        loop {
            match supervisor.drain_until(Instant::now() + Duration::from_secs(5)).await? {
                DrainStatus::ReadyToJoin => break,
                DrainStatus::Pending => eprintln!("shutdown still unfinished: {:?}", supervisor.report()),
            }
        }
        Ok::<(), io::Error>(())
    })?;
    let report = supervisor.join()?;
    println!("SHUTDOWN {:?}", report);
    if report.phase == Phase::Failed { return Err(io::Error::other("broker shutdown reported failure").into()); }
    Ok(())
}
