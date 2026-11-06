# Chapter 19 — End-to-end coverage and overload demonstration

## Part 2 — Owned child processes and controlled interruption

> **Learning objectives**
> Retain ownership of each child until it is reaped; learn readiness from a completed rendezvous rather than a guessed delay; run the real broker under a small resource profile; and distinguish a process kill from an injected returned error or a power failure.

This is the second part of the same **5.0-hour chapter**. Read [Part 1](https://25d-228.github.io/rust-learning/chapter-19.html) first and continue to [Part 3](https://25d-228.github.io/rust-learning/chapter-19-part-03.html). Source fragments with the same filename concatenate in page order.

## Keep process ownership in a guard

A child process is not a Rust task. Dropping `std::process::Child` does not terminate it or wait for it. On Unix, `Child::kill` requests forced termination with `SIGKILL`; waiting is still needed to observe/reap the exit. See [`Child`](https://doc.rust-lang.org/std/process/struct.Child.html#method.kill). Merely calling `kill` is not evidence that the directory lock is available for restart.

Our guard owns `Option<Child>`. `Some` means cleanup remains the parent's responsibility. A successful reap changes it to `None`. Error-path `Drop` attempts kill and wait, so a failed assertion in the parent does not ordinarily leave a broker running. That synchronous fallback is not a hard real-time shutdown guarantee: an operating-system stall can outlast an application watchdog. Normal execution uses the explicit asynchronous polling paths and checks their results.

Create `workload/process.rs`. `publish` writes a small rendezvous file completely and then renames it into place. The parent watches the final name, not a partial write. These are **ephemeral coordination files**, not durable broker metadata; the function intentionally does not claim synchronized publication or crash recovery for them. A fresh run directory prevents reuse of an old marker.

```rust title="crates/broker-app/src/workload/process.rs"
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};
use crate::protocol;
use super::{TIMEOUT, require};

pub(super) fn publish(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = path.with_extension("writing");
    let mut file = OpenOptions::new().write(true).create_new(true).open(&temporary)?;
    file.write_all(bytes)?;
    drop(file);
    require(!path.try_exists()?, "rendezvous path already exists")?;
    fs::rename(temporary, path)
}

```

The helper takes borrowed paths/bytes and owns its temporary `File`. `drop(file)` closes that handle before rename. A failed write leaves the `.writing` file for diagnosis; the run fails rather than retrying an uncertain rendezvous or promoting it later.

The broker binds port 0 and publishes its actual addresses. This avoids the race in which a parent briefly binds an unused port, closes it, and hopes nobody else takes it before the child starts. The parser requires bound loopback addresses and a positive epoch.

```rust title="crates/broker-app/src/workload/process.rs"
#[derive(Debug, Clone, Copy)]
pub(super) struct Addresses {
    pub producer: SocketAddr, pub consumer: SocketAddr, pub control: SocketAddr, pub epoch: u64,
}
impl Addresses {
    pub fn parse(bytes: &[u8]) -> io::Result<Self> {
        let text = std::str::from_utf8(bytes).map_err(io::Error::other)?;
        let fields: Vec<_> = text.split_whitespace().collect();
        require(fields.len() == 4, "invalid broker rendezvous")?;
        let address = |field: &str| -> io::Result<SocketAddr> {
            let value: SocketAddr = field.parse().map_err(io::Error::other)?;
            require(value.ip().is_loopback() && value.port() != 0, "rendezvous is not bound loopback")?;
            Ok(value)
        };
        let epoch = fields[3].parse().map_err(io::Error::other)?;
        require(epoch != 0, "rendezvous epoch is zero")?;
        Ok(Self { producer: address(fields[0])?, consumer: address(fields[1])?,
            control: address(fields[2])?, epoch })
    }
}

```

The three addresses have different roles: producer traffic, consumer/control operations from the broker protocol, and a private workload stop listener. The last one is not a new broker wire operation. It exists only in the test child, on loopback, with no authentication claim. Do not expose these workload child modes as a remote management service.

## Spawn without a shell and wait for a fact

Add the process type, spawn function, and readiness loop. Arguments are individual arguments passed to `Command`, not shell text. A run-directory name containing spaces therefore remains one argument. The paths are checked as UTF-8 by the caller because this deliberately small command-line interface uses `String` arguments. See [`Command::args`](https://doc.rust-lang.org/std/process/struct.Command.html#method.args).

```rust title="crates/broker-app/src/workload/process.rs"
pub(super) struct Process { child: Option<Child> }
impl Process {
    pub fn spawn(executable: &Path, arguments: &[&str], log: &Path) -> io::Result<Self> {
        let output = OpenOptions::new().write(true).create_new(true).open(log)?;
        let child = Command::new(executable).args(arguments).stdin(Stdio::null())
            .stdout(Stdio::from(output.try_clone()?)).stderr(Stdio::from(output)).spawn()?;
        Ok(Self { child: Some(child) })
    }
    pub async fn ready(&mut self, path: &Path) -> io::Result<Vec<u8>> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let child = self.child.as_mut().ok_or_else(|| io::Error::other("child already reaped"))?;
            if let Some(status) = child.try_wait()? {
                return Err(io::Error::other(format!("child exited before rendezvous: {status}")));
            }
            match fs::read(path) {
                Ok(bytes) => { require(bytes.len() <= 4096, "oversized rendezvous")?; return Ok(bytes); }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {},
                Err(error) => return Err(error),
            }
            require(Instant::now() < deadline, "rendezvous watchdog expired; inspect child log")?;
            sleep(Duration::from_millis(5)).await;
        }
    }
```

Standard output and standard error go to one regular file. We do not leave a child's piped output unread; that would add pipe-buffer backpressure unrelated to the broker. `try_clone` gives another owned handle for redirection, and the parent does not retain a pipe-reader task.

The five-millisecond wait is polling cadence, **not proof of readiness**. The loop checks that the child is still alive and that the final rendezvous file exists. The 30-second deadline is a watchdog that produces a diagnosis opportunity, not a performance target. If the child exits first, inspect its log rather than assuming the listener was merely slow.

## Separate forced termination from graceful exit

Append the rest of the process guard. The ordinary exit path sends four exact control bytes, then requires a successful exit status. The kill path first requires a running child, invokes forced termination, reaps it, and requires a non-success status. Neither path silently substitutes for the other.

```rust title="crates/broker-app/src/workload/process.rs"
    async fn reap(&mut self) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let child = self.child.as_mut().ok_or_else(|| io::Error::other("child already reaped"))?;
            if let Some(status) = child.try_wait()? {
                self.child = None;
                return Ok(status);
            }
            require(Instant::now() < deadline, "child-exit watchdog expired")?;
            sleep(Duration::from_millis(5)).await;
        }
    }
    pub async fn kill(&mut self) -> io::Result<()> {
        let child = self.child.as_mut().ok_or_else(|| io::Error::other("child already reaped"))?;
        require(child.try_wait()?.is_none(), "child exited before the intended kill")?;
        child.kill()?;
        let status = self.reap().await?;
        require(!status.success(), "intended process termination returned success")
    }
    pub async fn stop(&mut self, control: SocketAddr) -> io::Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        let mut stream = protocol::within(deadline, TcpStream::connect(control)).await?;
        protocol::write_bytes(&mut stream, b"STOP", deadline).await?;
        drop(stream);
        let status = self.reap().await?;
        require(status.success(), "graceful broker exit failed; inspect child log")
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Error-path fallback: Child itself neither kills nor reaps on Drop.
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

```

The readiness and exit futures borrow the guard. If one is cancelled, the `Child` remains owned by the guard; dropping the guard performs fallback cleanup. Reaping is the point at which ownership can be discharged. A timeout still leaves a live handle for cleanup, not a fabricated successful stop.

`Process::stop` sends `STOP` through a fresh connection to the workload-only listener. The child feeds that input into Chapter 17's supervisor stop future, drains sessions, joins owners, and exits normally. This lets the same harness distinguish the abrupt first interruption from later healthy shutdowns.

## Start known generations and inspect only quiescent file lengths

Finish `process.rs`. Generation 1 initializes a new broker directory; generations 2 and 3 recover it. Distinct ready/log filenames avoid stale readiness. On failure, the local guard drops and attempts cleanup even if parsing the ready record failed.

```rust title="crates/broker-app/src/workload/process.rs"
pub(super) struct Broker { pub process: Process, pub addresses: Addresses }
impl Broker {
    pub async fn start(executable: &Path, root: &Path, generation: u32) -> io::Result<Self> {
        let path = root.to_str().ok_or_else(|| io::Error::other("run directory must be UTF-8"))?;
        let number = generation.to_string();
        let mut process = Process::spawn(executable, &["broker-child", path, &number],
            &root.join(format!("broker-{generation}.log")))?;
        let bytes = process.ready(&root.join(format!("broker-{generation}.ready"))).await?;
        Ok(Self { process, addresses: Addresses::parse(&bytes)? })
    }
}

pub(super) fn segment_bytes(root: &Path, partition: u16) -> io::Result<u64> {
    let directory: PathBuf = root.join("broker").join(format!("partition-{partition}"));
    let mut total = 0u64;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        require(entry.file_type()?.is_file(), "unexpected partition entry")?;
        require(entry.path().extension().is_some_and(|e| e == "seg"), "unexpected partition filename")?;
        total = total.checked_add(entry.metadata()?.len()).ok_or_else(|| io::Error::other("file length overflow"))?;
    }
    Ok(total)
}
```

`segment_bytes` sums logical `.seg` file lengths only when the scenario has stopped concurrent appends and awaited the relevant operation reply. It does not read internal admission counters to manufacture an expected result, and it does not report physical allocation, filesystem free space, or RSS. The broker's own accounting remains the subject being checked.

The directory and filename checks intentionally reject an unexpected fixture layout. The workload owns a fresh directory; it is not a tolerant scanner for arbitrary user data. None of these helpers removes a pre-existing directory.

## Make the crash boundary observable

Create `workload/children.rs`. `GateSync` is a test adapter at the existing `SyncIo` interface. All successful paths delegate to real `FileSync`; there is no fake successful synchronization. Only generation 1, partition 0 can arm the gate.

```rust title="crates/broker-app/src/workload/children.rs"
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

```

The parent first waits for all normal producer replies, then creates `gate.arm`, then sends one specific append. The owner writes that batch before calling this `sync` method. The adapter publishes `gate.entered` **before** the underlying file synchronization and then waits. The parent kills the broker only after observing that marker. Thus the test names its interruption boundary without depending on a scheduler delay.

The blocking wait runs in the partition's dedicated owner thread, not the async network runtime. Its sleep only avoids a busy loop. `gate.release` is an escape hatch for a manual investigation; the normal scenario kills the process instead. If the watchdog fires, it returns an error rather than pretending synchronization succeeded. The parent treats that as a failed scenario, not a successful crash test.

There can already be complete bytes in the kernel's file cache when this process dies. Recovery is allowed to keep them. This experiment does not disconnect power, emulate a torn device write, or prove which unsynchronized bytes survive such events. The external ledger still labels the append ambiguous until the end of the run.

## Choose a small profile without removing the mechanisms

One workload payload has 24 bytes and one stored record adds a 32-byte header. With segment cap $S=256$, segment-header bytes $H=32$, and record bytes $r=56$, the records per full segment are:

$$
\left\lfloor\frac{S-H}{r}\right\rfloor=\left\lfloor\frac{224}{56}\right\rfloor=4.
$$

Nine records occupy two full segments and a one-record segment:

$$
D_9=9\cdot56+3\cdot32=600\text{ bytes}.
$$

The normal data quota is 640 bytes per partition. Before handoff, admission conservatively reserves one record plus a possible new segment header, or $56+32=88$ bytes. At nine records, $600+88=688>640$, so the next append must be refused. At eight records, the charge is 512 bytes and the reservation fits exactly the next 88 bytes. The additional 416-byte reserve is the existing 384-byte metadata allowance plus 32-byte emergency rotation headroom; it is not available for ordinary producer records.

The profile retains four owners, two groups, real segmented files, two payload buffers per partition, separate data/query/urgent lanes, four-slot windows, real TCP framing, and the supervisor. The owner scratch allocation remains 4 MiB per partition from Chapter 15 even though these payloads are small. Smaller records do not establish a smaller process RSS. The later measurement chapter evaluates such costs separately.

Append the complete child entry point. The supervisor and runtime have the same ownership relationship as `brokerd`: blocking startup precedes serving, and the final OS-thread join occurs outside `block_on`.

```rust title="crates/broker-app/src/workload/children.rs"
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

```

`OwnerConfig` is copied configuration, while the `Arc<MonotonicClock>` shares one clock across the four owners. The closure passed to `from_session` creates a separate sync adapter for each partition and enables the gate only for partition 0 in generation 1. Generation 2 cannot accidentally rearm itself from a leftover `gate.arm` file.

The stop listener is bound before readiness is published. Its future owns the listener and accepted control socket during the wait; there is no detached stdin-reader thread. If the supervisor stops for an owner failure instead, dropping that future closes those resources. A successful child exit still requires `Phase::Stopped` after a real join.

Each lease lasts 30 seconds. That is a fixture timeout allowance, not a guarantee that slow debugging pauses cannot expire it. A rejected acknowledgement stops this scenario with a useful error. Chapter 12's manual-clock tests remain the deterministic place to test exact lease deadlines.

## Kill a consumer after a proven delivery, before any acknowledgement

The victim process fetches exactly one record from partition 0, group 1, publishes its token and payload in a 64-byte receipt, and waits forever. It never acknowledges. The parent reads and validates the receipt, records the delivery, then kills that specific child. A closed consumer connection alone is not used as evidence that a fetch occurred.

The receipt's scope is fixed by this fixture, so partition/group are not duplicated in its bytes. The layout is eight magic bytes, four 64-bit numbers (epoch, offset, generation, deadline), and the 24-byte payload: $8+4\cdot8+24=64$ bytes. This is an internal coordination format, not a new broker record or public protocol. Add both complete codec functions and the consumer entry point.

```rust title="crates/broker-app/src/workload/children.rs"
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
```

The receipt preserves the full lease token for a later stale-epoch test. Its payload is copied into the receipt while the fetched batch still owns the bytes. The pending future retains both batch and client in the victim process; forced termination bypasses their normal Rust cleanup, as intended. The parent retains its independent receipt and ledger.

The first broker kill then discards volatile windows and creates a new startup epoch on recovery. Submitting the victim's old token must fail specifically with `WrongEpoch` and must not advance progress. This composes actual process loss with the already implemented fence; it does not claim that killing a consumer cancels any external side effect.

The next part connects these boundaries into one executable scenario.
