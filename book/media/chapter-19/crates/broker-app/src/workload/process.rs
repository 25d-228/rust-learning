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
