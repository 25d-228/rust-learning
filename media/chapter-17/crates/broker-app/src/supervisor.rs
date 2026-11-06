use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{Id, JoinError, JoinSet};
use tokio::time::{Instant, sleep_until};

use broker_core::types::PARTITION_COUNT;
use broker_core::worker::{ExitWatch, OwnerExit};
use crate::local::LocalBroker;
use crate::protocol::Endpoint;
use crate::server::{Clients, ListenerStats, SessionLimits, session_until};

const OWNERS: usize = PARTITION_COUNT as usize;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase { Starting, Serving, Quiescing, Draining, Stopped, Failed }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopCause { Requested, SignalFailed, StartupFailed, OwnerExited, TransportFailed }

#[derive(Debug, Clone, Copy)]
pub struct SupervisorConfig {
    pub sessions: SessionLimits,
    pub capacity: [usize; 2],
    pub network_grace: Duration,
}
impl Default for SupervisorConfig {
    fn default() -> Self {
        Self { sessions: SessionLimits::default(), capacity: [16, 16],
            network_grace: Duration::from_secs(2) }
    }
}
impl SupervisorConfig {
    pub fn validate(self) -> io::Result<()> {
        self.sessions.validate()?;
        if self.capacity.iter().any(|n| !(1..=64).contains(n))
            || self.network_grace > Duration::from_secs(60)
        { return Err(io::Error::new(io::ErrorKind::InvalidInput, "capacity 1..=64; network grace 0..=60s")); }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ShutdownReport {
    pub phase: Phase,
    pub cause: Option<StopCause>,
    pub first_failure: Option<String>,
    pub sessions: [ListenerStats; 2],
    pub active_sessions: usize,
    pub pending_owners: usize,
    pub network_forced: bool,
    pub owners: [Option<OwnerExit>; OWNERS],
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStatus { Pending, ReadyToJoin }

type SessionJoin = Result<(Id, io::Result<()>), JoinError>;
enum Event {
    Stop(io::Result<()>),
    Owner(usize, Option<OwnerExit>),
    Session(SessionJoin),
    Accept(usize, io::Result<(TcpStream, SocketAddr)>),
}

async fn read_exit(receiver: &mut ExitWatch) -> Option<OwnerExit> {
    loop {
        let value = receiver.borrow_and_update().clone();
        if value.is_some() { return value; }
        if receiver.changed().await.is_err() { return None; }
    }
}
async fn next_exit(receivers: &mut [ExitWatch; OWNERS], seen: [bool; OWNERS])
    -> (usize, Option<OwnerExit>)
{
    // The topology is deliberately fixed at four. No monitor tasks or polling timer are needed.
    let [a, b, c, d] = receivers;
    tokio::select! {
        result = read_exit(a), if !seen[0] => (0, result),
        result = read_exit(b), if !seen[1] => (1, result),
        result = read_exit(c), if !seen[2] => (2, result),
        result = read_exit(d), if !seen[3] => (3, result),
        else => std::future::pending().await,
    }
}

pub struct Supervisor {
    // Task ownership is in the object, not in a cancellable drive future.
    tasks: JoinSet<io::Result<()>>,
    scopes: HashMap<Id, usize>,
    broker: LocalBroker,
    clients: Option<Clients>,
    listeners: [Option<TcpListener>; 2],
    stop: watch::Sender<bool>,
    exits: [ExitWatch; OWNERS],
    seen: [bool; OWNERS],
    notices: [Option<OwnerExit>; OWNERS],
    config: SupervisorConfig,
    phase: Phase,
    phase_changed: watch::Sender<Phase>,
    stats: [ListenerStats; 2],
    active: [usize; 2],
    cause: Option<StopCause>,
    first_failure: Option<String>,
    network_deadline: Option<Instant>,
    network_forced: bool,
}

impl Supervisor {
    pub fn new(broker: LocalBroker, config: SupervisorConfig) -> io::Result<Self> {
        config.validate()?;
        let clients = Some(broker.clients());
        let exits = broker.exits();
        let (stop, _) = watch::channel(false);
        let (phase_changed, _) = watch::channel(Phase::Starting);
        Ok(Self { tasks: JoinSet::new(), scopes: HashMap::with_capacity(config.capacity.iter().sum()),
            broker, clients, listeners: [None, None], stop, exits, seen: [false; OWNERS],
            notices: std::array::from_fn(|_| None), config, phase: Phase::Starting, phase_changed,
            stats: [ListenerStats::default(); 2], active: [0, 0], cause: None, first_failure: None,
            network_deadline: None, network_forced: false })
    }
    pub fn phase(&self) -> Phase { self.phase }
    pub fn observe(&self) -> watch::Receiver<Phase> { self.phase_changed.subscribe() }
    pub fn report(&self) -> ShutdownReport {
        ShutdownReport { phase: self.phase, cause: self.cause, first_failure: self.first_failure.clone(),
            sessions: self.stats, active_sessions: self.tasks.len(),
            pending_owners: self.seen.iter().filter(|seen| !**seen).count(),
            network_forced: self.network_forced, owners: self.notices.clone() }
    }
    fn phase_to(&mut self, phase: Phase) {
        self.phase = phase;
        self.phase_changed.send_replace(phase);
    }
    fn fail(&mut self, message: String) {
        if self.first_failure.is_none() { self.first_failure = Some(message); }
    }

    pub async fn bind(&mut self, producer: SocketAddr, consumer: SocketAddr)
        -> io::Result<[SocketAddr; 2]>
    {
        if self.phase != Phase::Starting {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bind requires Starting"));
        }
        let result: io::Result<_> = async {
            let p = TcpListener::bind(producer).await?;
            let c = TcpListener::bind(consumer).await?;
            let addresses = [p.local_addr()?, c.local_addr()?];
            Ok(([Some(p), Some(c)], addresses))
        }.await;
        match result {
            Ok((listeners, addresses)) => {
                self.listeners = listeners;
                self.phase_to(Phase::Serving);
                Ok(addresses)
            }
            Err(error) => {
                self.fail(format!("listener startup: {error}"));
                self.quiesce(StopCause::StartupFailed);
                Err(error)
            }
        }
    }

    fn spawn_session(&mut self, scope: usize, stream: TcpStream) {
        let endpoint = if scope == 0 { Endpoint::Producer } else { Endpoint::Consumer };
        let clients = Arc::clone(self.clients.as_ref().expect("serving clients"));
        let handle = self.tasks.spawn(session_until(stream, endpoint, clients,
            self.config.sessions, self.stop.subscribe()));
        self.scopes.insert(handle.id(), scope);
        self.active[scope] += 1;
        self.stats[scope].accepted = self.stats[scope].accepted.saturating_add(1);
    }
    fn joined_session(&mut self, joined: SessionJoin) {
        let id = match &joined { Ok((id, _)) => *id, Err(error) => error.id() };
        let Some(scope) = self.scopes.remove(&id) else {
            self.fail("joined an untracked session".into());
            return;
        };
        self.active[scope] -= 1;
        let stats = &mut self.stats[scope];
        match joined {
            Ok((_, Ok(()))) => stats.completed = stats.completed.saturating_add(1),
            Ok((_, Err(_))) => stats.session_errors = stats.session_errors.saturating_add(1),
            Err(error) if error.is_cancelled() => {
                stats.cancelled = stats.cancelled.saturating_add(1);
                if !self.network_forced { self.fail("session cancelled outside forced network drain".into()); }
            }
            Err(error) => {
                stats.panicked = stats.panicked.saturating_add(1);
                self.fail(format!("session task panicked: {error}"));
            }
        }
    }
    fn noted_owner(&mut self, index: usize, notice: Option<OwnerExit>, unexpected: bool) {
        self.seen[index] = true;
        let failure = match &notice {
            Some(OwnerExit::Failed { state, cause }) => Some(format!(
                "partition {index} failed at visible {}: {cause:?}", state.visible_end.get())),
            Some(OwnerExit::Panicked) => Some(format!("partition {index} panicked")),
            None => Some(format!("partition {index} lost its exit notice")),
            Some(OwnerExit::Stopped { checkpointed, .. }) if unexpected || !checkpointed =>
                Some(format!("partition {index} exited without the supervised finish barrier")),
            _ => None,
        };
        self.notices[index] = notice;
        if let Some(message) = failure { self.fail(message); }
    }

    pub async fn serve_until<F>(&mut self, stop: F) -> io::Result<()>
    where F: Future<Output = io::Result<()>> {
        if self.phase != Phase::Serving {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "serve requires Serving"));
        }
        tokio::pin!(stop);
        loop {
            let event = tokio::select! {
                result = &mut stop => Event::Stop(result),
                (index, notice) = next_exit(&mut self.exits, self.seen) => Event::Owner(index, notice),
                joined = self.tasks.join_next_with_id(), if !self.tasks.is_empty() =>
                    Event::Session(joined.expect("nonempty session set")),
                accepted = self.listeners[0].as_ref().expect("producer listener").accept(),
                    if self.active[0] < self.config.capacity[0] => Event::Accept(0, accepted),
                accepted = self.listeners[1].as_ref().expect("consumer listener").accept(),
                    if self.active[1] < self.config.capacity[1] => Event::Accept(1, accepted),
            };
            match event {
                Event::Stop(result) => {
                    let cause = match result {
                        Ok(()) => StopCause::Requested,
                        Err(error) => { self.fail(format!("shutdown signal: {error}")); StopCause::SignalFailed }
                    };
                    self.quiesce(cause);
                }
                Event::Owner(index, notice) => {
                    self.noted_owner(index, notice, true);
                    self.quiesce(StopCause::OwnerExited);
                }
                Event::Session(joined) => {
                    self.joined_session(joined);
                    if self.first_failure.is_some() { self.quiesce(StopCause::TransportFailed); }
                }
                Event::Accept(scope, Ok((stream, _))) => self.spawn_session(scope, stream),
                Event::Accept(_, Err(error)) => {
                    self.fail(format!("accept: {error}"));
                    self.quiesce(StopCause::TransportFailed);
                }
            }
            if self.phase != Phase::Serving { return Ok(()); }
        }
    }

    pub fn request_shutdown(&mut self) { self.quiesce(StopCause::Requested); }

    fn quiesce(&mut self, cause: StopCause) {
        if !matches!(self.phase, Phase::Starting | Phase::Serving) { return; }
        self.cause = Some(cause);
        self.phase_to(Phase::Quiescing);
        // No await between listener removal and the four admission-closure boundaries.
        self.listeners = [None, None];
        for (index, recorded) in self.broker.begin_shutdown().into_iter().enumerate() {
            if !recorded { self.fail(format!("partition {index} was already closed before finish was requested")); }
        }
        self.stop.send_replace(true);
        self.network_deadline = Some(Instant::now() + self.config.network_grace);
        self.phase_to(Phase::Draining);
    }

    pub async fn drain_until(&mut self, observation_deadline: Instant) -> io::Result<DrainStatus> {
        if self.phase != Phase::Draining {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "drain requires Draining"));
        }
        loop {
            let network_deadline = self.network_deadline.expect("drain has one absolute deadline");
            if !self.network_forced && !self.tasks.is_empty() && Instant::now() >= network_deadline {
                self.network_forced = true;
                self.tasks.abort_all();
            }
            if self.tasks.is_empty() && self.seen.iter().all(|seen| *seen) {
                // Clients held elsewhere may still retain their own pools; do not restart in-process.
                self.clients.take();
                return Ok(DrainStatus::ReadyToJoin);
            }
            if Instant::now() >= observation_deadline { return Ok(DrainStatus::Pending); }
            tokio::select! {
                joined = self.tasks.join_next_with_id(), if !self.tasks.is_empty() =>
                    self.joined_session(joined.expect("nonempty session set")),
                (index, notice) = next_exit(&mut self.exits, self.seen),
                    if self.seen.iter().any(|seen| !*seen) => self.noted_owner(index, notice, false),
                _ = sleep_until(network_deadline), if !self.network_forced && !self.tasks.is_empty() => {},
                _ = sleep_until(observation_deadline) => return Ok(DrainStatus::Pending),
            }
        }
    }

    // Synchronous and called outside the async runtime. ReadyToJoin is not yet Stopped.
    pub fn join(&mut self) -> io::Result<ShutdownReport> {
        if self.phase != Phase::Draining || !self.tasks.is_empty() || self.seen.iter().any(|seen| !*seen) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "transport or owners remain unjoined"));
        }
        let results = self.broker.join_all();
        for (index, result) in results.into_iter().enumerate() {
            if let Err(error) = result { self.fail(format!("join partition {index}: {error:?}")); }
        }
        self.clients.take();
        self.phase_to(if self.first_failure.is_some() { Phase::Failed } else { Phase::Stopped });
        Ok(self.report())
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if !matches!(self.phase, Phase::Stopped | Phase::Failed) {
            self.request_shutdown();
            self.tasks.abort_all();
            // Drop cannot await. Report misuse; LocalBroker still joins every OS owner.
            use std::io::Write;
            let _ = writeln!(io::stderr(), "supervisor dropped before explicit task/thread joins; no clean shutdown report");
        }
    }
}

#[cfg(test)]
mod tests;
