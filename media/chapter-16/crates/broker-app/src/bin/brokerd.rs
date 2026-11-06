use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

use broker_app::BoxError;
use broker_app::local::LocalBroker;
use broker_app::protocol::Endpoint;
use broker_app::server::{Clients, SessionLimits, serve_listener};

async fn run(clients: Clients, producer_port: u16, consumer_port: u16) -> io::Result<()> {
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    // Bind both before exposing either accept loop. A bind failure drops the first listener.
    let producer = TcpListener::bind(SocketAddr::new(ip, producer_port)).await?;
    let consumer = TcpListener::bind(SocketAddr::new(ip, consumer_port)).await?;
    println!("LISTEN producer={} consumer={}", producer.local_addr()?, consumer.local_addr()?);
    let (stop, changed) = watch::channel(false);
    let mut listeners = JoinSet::new();
    listeners.spawn(serve_listener(producer, Endpoint::Producer, Arc::clone(&clients),
        SessionLimits::default(), 16, changed.clone()));
    listeners.spawn(serve_listener(consumer, Endpoint::Consumer, clients,
        SessionLimits::default(), 16, changed));
    let mut failure = tokio::select! {
        signal = tokio::signal::ctrl_c() => signal.err(),
        joined = listeners.join_next() => match joined.expect("two listener tasks") {
            Ok(Ok(stats)) => {
                eprintln!("listener stopped: {stats:?}");
                Some(io::Error::other("listener stopped before interrupt"))
            }
            Ok(Err(error)) => Some(error),
            Err(error) => Some(io::Error::other(format!("listener task failed: {error}"))),
        },
    };
    let _ = stop.send(true);
    while let Some(joined) = listeners.join_next().await {
        match joined {
            Ok(Ok(stats)) => eprintln!("listener joined: {stats:?}"),
            Ok(Err(error)) => { if failure.is_none() { failure = Some(error); } }
            Err(error) => { if failure.is_none() { failure = Some(io::Error::other(error)); } }
        }
    }
    match failure { Some(error) => Err(error), None => Ok(()) }
}
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
    let serve_result = runtime.block_on(run(broker.clients(), producer_port, consumer_port));
    // All network tasks have been joined; now join owners outside the async runtime.
    let join_result = broker.join();
    if let Err(error) = &join_result { eprintln!("owner join failed: {error}"); }
    serve_result?;
    join_result?;
    Ok(())
}
