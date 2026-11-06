use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

use broker_core::types::PARTITION_COUNT;
use broker_core::worker::BoundedClient;
use crate::protocol::{self, Endpoint, Header, Status};

mod dispatch;
use dispatch::{CallError, dispatch, send_output};

pub type Clients = Arc<[BoundedClient; PARTITION_COUNT as usize]>;

#[derive(Debug, Clone, Copy)]
pub struct SessionLimits {
    pub header: Duration,
    pub admission: Duration,
    pub body: Duration,
    pub operation: Duration,
    pub write: Duration,
}
impl Default for SessionLimits {
    fn default() -> Self {
        Self { header: Duration::from_secs(10), admission: Duration::from_secs(1),
            body: Duration::from_secs(5), operation: Duration::from_secs(10),
            write: Duration::from_secs(5) }
    }
}
impl SessionLimits {
    pub fn validate(self) -> io::Result<()> {
        if [self.header, self.admission, self.body, self.operation, self.write].iter()
            .any(|value| value.is_zero() || *value > Duration::from_secs(60))
        { return Err(io::Error::new(io::ErrorKind::InvalidInput, "session deadlines must be in (0, 60s]")); }
        Ok(())
    }
}
async fn refuse<W: AsyncWrite + Unpin>(stream: &mut W, request: Header,
    status: Status, limits: SessionLimits) -> io::Result<()>
{
    protocol::write_bytes(stream, &request.response(status, [0; 5], 0).encode(),
        Instant::now() + limits.write).await
}

pub async fn session<S>(stream: S, endpoint: Endpoint, clients: Clients, limits: SessionLimits)
    -> io::Result<()>
where S: AsyncRead + AsyncWrite + Unpin {
    let (_keep_alive, stop) = watch::channel(false);
    session_until(stream, endpoint, clients, limits, stop).await
}

pub async fn session_until<S>(mut stream: S, endpoint: Endpoint, clients: Clients,
    limits: SessionLimits, mut stop: watch::Receiver<bool>) -> io::Result<()>
where S: AsyncRead + AsyncWrite + Unpin {
    limits.validate()?;
    loop {
        if *stop.borrow() { return Ok(()); }
        let incoming = tokio::select! {
            biased;
            _ = stop.changed() => return Ok(()),
            header = protocol::read_header(&mut stream, Instant::now() + limits.header) => header?,
        };
        let Some(header) = incoming else { return Ok(()); };
        let scope = protocol::validate_request(header);
        if scope.is_err() || !endpoint.accepts(header.op) {
            refuse(&mut stream, header, Status::Invalid, limits).await?;
            // In particular, do not try to drain a rejected append's unread body.
            return Ok(());
        }
        let (partition, _) = scope.expect("validated request");
        let client = &clients[usize::from(partition.get())];
        match dispatch(&mut stream, header, client, limits).await {
            Ok(output) => send_output(&mut stream, header, output, limits).await?,
            Err(CallError::Status(status)) => {
                refuse(&mut stream, header, status, limits).await?;
                return Ok(());
            }
            Err(CallError::Transport(error)) => return Err(error),
        }
        // Only after a full response do we read the next request's fixed header.
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ListenerStats {
    pub accepted: u64,
    pub completed: u64,
    pub session_errors: u64,
    pub cancelled: u64,
    pub panicked: u64,
}
fn record_join(result: Result<io::Result<()>, tokio::task::JoinError>, stats: &mut ListenerStats)
    -> io::Result<()>
{
    match result {
        Ok(Ok(())) => stats.completed = stats.completed.saturating_add(1),
        Ok(Err(_)) => stats.session_errors = stats.session_errors.saturating_add(1),
        Err(error) if error.is_cancelled() => stats.cancelled = stats.cancelled.saturating_add(1),
        Err(error) => return Err(io::Error::other(format!("session task panicked: {error}"))),
    }
    Ok(())
}

pub async fn serve_listener(listener: TcpListener, endpoint: Endpoint, clients: Clients,
    limits: SessionLimits, capacity: usize, mut stop: watch::Receiver<bool>)
    -> io::Result<ListenerStats>
{
    limits.validate()?;
    if !(1..=64).contains(&capacity) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "listener capacity must be in 1..=64"));
    }
    let mut tasks = JoinSet::new();
    let mut stats = ListenerStats::default();
    let mut result = loop {
        if *stop.borrow() { break Ok(()); }
        tokio::select! {
            _ = stop.changed() => break Ok(()),
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = record_join(joined.expect("nonempty task set"), &mut stats) {
                    break Err(error);
                }
            }
            accepted = listener.accept(), if tasks.len() < capacity => {
                match accepted {
                    Ok((stream, _)) => {
                        stats.accepted = stats.accepted.saturating_add(1);
                        let clients = Arc::clone(&clients);
                        tasks.spawn(session(stream, endpoint, clients, limits));
                    }
                    Err(error) => break Err(error),
                }
            }
        }
    };
    drop(listener);
    // Chapter 16 stops transport immediately; it does not call this graceful draining.
    tasks.abort_all();
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = record_join(joined, &mut stats) {
            if result.is_ok() { result = Err(error); }
        }
    }
    result?;
    Ok(stats)
}
