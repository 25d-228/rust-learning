use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};
use tokio::net::TcpListener;
use tokio::time::Instant;
use broker_core::budget::{DiskQuota, MemoryConfig};
use broker_core::delivery::LeaseToken;
use broker_core::delivery::clock::MonotonicClock;
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::io::StorageFile;
use broker_core::storage::recovery;
use broker_core::storage::segment::StorageConfig;
use broker_core::storage::sync::{FileSync, SyncIo};
use broker_core::types::{Epoch, Offset};
use broker_core::worker::BatchConfig;
use crate::BoxError;
use crate::client::Client;
use crate::local::{LocalBroker, OwnerConfig};
use crate::protocol;
use crate::supervisor::{DrainStatus, Phase, Supervisor, SupervisorConfig};
use super::ledger::Key;
use super::process::publish;
use super::{DATA_QUOTA, FETCH_BYTES, PAYLOAD_BYTES, TIMEOUT, g, p, require};

struct GateSync { root: PathBuf, enabled: bool, fired: bool }
impl SyncIo for GateSync {
    fn sync(&mut self, base: Offset, file: &mut dyn StorageFile) -> io::Result<()> {
        if self.enabled && !self.fired && self.root.join("gate.arm").try_exists()? {
            self.fired = true;
            // The owner has already written its batch. Real synchronization has not run yet.
            publish(&self.root.join("gate.entered"), b"before-record-sync\n")?;
            let deadline = StdInstant::now() + TIMEOUT;
            while !self.root.join("gate.release").try_exists()? {
                if StdInstant::now() >= deadline {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "workload synchronization gate expired"));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        FileSync.sync(base, file)
    }
}

pub fn broker_child(root: &Path, generation: u32) -> Result<(), BoxError> {
    require((1..=3).contains(&generation), "broker generation must be 1, 2, or 3")?;
    let data = root.join("broker");
    let session = if generation == 1 {
        recovery::initialize_budgeted(&data, StorageConfig::new(256, 2, 16)?, DiskQuota::new(DATA_QUOTA, 416)?)?
    } else { recovery::recover(&data)? };
    let epoch = session.epoch().get();
    let owners = OwnerConfig { batch: BatchConfig::new(1, FETCH_BYTES, Duration::from_millis(5))?,
        lanes: [2, 2, 4], window: 4, lease_ms: 30_000, checkpoint: CheckpointPolicy::explicit_only(),
        memory: MemoryConfig::new(PAYLOAD_BYTES, 2, FETCH_BYTES, 2, 4)? };
    let broker = LocalBroker::from_session(session, owners, Arc::new(MonotonicClock::new()), |partition|
        GateSync { root: root.to_path_buf(), enabled: generation == 1 && partition == p(0), fired: false })?;
    let config = SupervisorConfig { capacity: [8, 8], ..SupervisorConfig::default() };
    let mut supervisor = Supervisor::new(broker, config)?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    runtime.block_on(async {
        let control = TcpListener::bind(local).await?;
        let addresses = supervisor.bind(local, local).await?;
        publish(&root.join(format!("broker-{generation}.ready")), format!("{} {} {} {epoch}\n",
            addresses[0], addresses[1], control.local_addr()?).as_bytes())?;
        // This separate loopback listener is harness control, not a broker protocol operation.
        let stop = async {
            let (mut stream, _) = control.accept().await?;
            let mut command = [0; 4];
            protocol::read_body(&mut stream, &mut command, Instant::now() + TIMEOUT).await?;
            require(&command == b"STOP", "invalid workload stop command")
        };
        supervisor.serve_until(stop).await?;
        let drained = supervisor.drain_until(Instant::now() + TIMEOUT).await?;
        require(drained == DrainStatus::ReadyToJoin, "broker drain remains unfinished")?;
        Ok::<(), io::Error>(())
    })?;
    let report = supervisor.join()?;
    require(report.phase == Phase::Stopped, "supervised child exit reported failure")?;
    println!("JOINED {report:?}");
    Ok(())
}

pub(super) fn encode_receipt(token: LeaseToken, payload: &[u8]) -> io::Result<[u8; 64]> {
    require(token.partition == p(0) && token.group == g(1), "victim receipt has the wrong scope")?;
    Key::decode(payload)?;
    let mut bytes = [0; 64];
    bytes[..8].copy_from_slice(b"VICTIM01");
    bytes[8..16].copy_from_slice(&token.epoch.get().to_be_bytes());
    bytes[16..24].copy_from_slice(&token.offset.get().to_be_bytes());
    bytes[24..32].copy_from_slice(&token.generation.to_be_bytes());
    bytes[32..40].copy_from_slice(&token.deadline_ms.to_be_bytes());
    bytes[40..].copy_from_slice(payload);
    Ok(bytes)
}
pub(super) fn decode_receipt(bytes: &[u8]) -> Result<(LeaseToken, [u8; PAYLOAD_BYTES]), BoxError> {
    require(bytes.len() == 64 && &bytes[..8] == b"VICTIM01", "invalid victim receipt")?;
    let number = |start: usize| u64::from_be_bytes(bytes[start..start + 8].try_into().expect("fixed receipt"));
    let token = LeaseToken { epoch: Epoch::new(number(8))?, partition: p(0), group: g(1),
        offset: Offset::new(number(16)), generation: number(24), deadline_ms: number(32) };
    require(token.generation != 0, "victim receipt generation is zero")?;
    let payload: [u8; PAYLOAD_BYTES] = bytes[40..].try_into().expect("fixed receipt payload");
    Key::decode(&payload)?;
    Ok((token, payload))
}

pub fn consumer_child(root: &Path, address: SocketAddr) -> Result<(), BoxError> {
    require(address.ip().is_loopback(), "victim endpoint must be loopback")?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let mut client = Client::connect(address, TIMEOUT).await?;
        let batch = client.fetch(p(0), g(1), 1, FETCH_BYTES).await?;
        let record = batch.records().next().ok_or_else(|| io::Error::other("victim found no record"))?;
        publish(&root.join("victim.ready"), &encode_receipt(record.token, record.payload)?)?;
        // There is deliberately no acknowledgement. The parent will terminate this process.
        std::future::pending::<()>().await;
        drop(batch);
        drop(client);
        Ok::<(), BoxError>(())
    })
}
