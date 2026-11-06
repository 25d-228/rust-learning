use std::io;
use std::path::Path;
use std::time::Duration;
use broker_core::budget::{DiskQuota, MemoryConfig};
use broker_core::delivery::progress::CheckpointPolicy;
use broker_core::types::GroupId;
use broker_core::worker::BatchConfig;
use crate::BoxError;
use crate::local::OwnerConfig;

mod experiment;
mod load;
mod observed;
mod report;
#[cfg(test)]
mod tests;
pub use experiment::{Summary, execute};

pub const PRODUCERS: usize = 8;
pub const FETCH_RECORDS: usize = 32;
pub const SEGMENT_BYTES: u64 = 8 * 1024 * 1024;
pub const OP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile { Single, Batched, Dense }
impl Profile {
    pub fn parse(value: &str) -> io::Result<Self> {
        match value { "single" => Ok(Self::Single), "batched" => Ok(Self::Batched),
            "dense" => Ok(Self::Dense), _ => Err(invalid("profile must be single, batched, or dense")) }
    }
    pub fn name(self) -> &'static str {
        match self { Self::Single => "single", Self::Batched => "batched", Self::Dense => "dense" }
    }
    pub fn stride(self) -> u64 { if self == Self::Dense { 8 } else { 64 } }
    pub fn batch_records(self) -> usize { if self == Self::Single { 1 } else { 2 } }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode { Closed, Paced }
impl Mode {
    pub fn parse(value: &str) -> io::Result<Self> {
        match value { "closed" => Ok(Self::Closed), "paced" => Ok(Self::Paced),
            _ => Err(invalid("mode must be closed or paced")) }
    }
    pub fn name(self) -> &'static str { if self == Self::Closed { "closed" } else { "paced" } }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub profile: Profile, pub mode: Mode, pub offers_per_producer: usize,
    pub payload_bytes: usize, pub period_us: u64,
}
impl Config {
    pub fn validate(self) -> io::Result<()> {
        require((1..=4096).contains(&self.offers_per_producer), "offers must be in 1..=4096")?;
        require((24..=4096).contains(&self.payload_bytes), "payload bytes must be in 24..=4096")?;
        require(match self.mode { Mode::Closed => self.period_us == 0,
            Mode::Paced => (1000..=10_000).contains(&self.period_us) },
            "closed mode requires period 0; paced mode requires 1000..=10000 microseconds")
    }
    pub fn owners(self) -> Result<OwnerConfig, BoxError> {
        self.validate()?;
        Ok(OwnerConfig {
            batch: BatchConfig::new(self.profile.batch_records(), 2 * 1024 * 1024,
                Duration::from_millis(2))?,
            lanes: [8, 8, 16], window: 64, lease_ms: 60_000,
            checkpoint: CheckpointPolicy::explicit_only(),
            memory: MemoryConfig::new(self.payload_bytes, 4, self.fetch_bytes(), 2, 8)?,
        })
    }
    pub fn fetch_bytes(self) -> usize { FETCH_RECORDS * (32 + self.payload_bytes) }
    pub fn quota(self) -> Result<DiskQuota, BoxError> {
        Ok(DiskQuota::new(64 * 1024 * 1024, 2 * 1024 * 1024)?)
    }
}
fn invalid(message: &str) -> io::Error { io::Error::new(io::ErrorKind::InvalidInput, message) }
fn require(condition: bool, message: &str) -> io::Result<()> {
    if condition { Ok(()) } else { Err(io::Error::other(message.to_owned())) }
}
fn g(index: usize) -> GroupId { GroupId::new(index as u8).expect("fixed group") }
fn phase(root: &Path, name: &str) -> io::Result<()> {
    // An observation label only, not a durable marker or a proof of event order.
    std::fs::write(root.join("phase.tmp"), name)?;
    std::fs::rename(root.join("phase.tmp"), root.join("phase.txt"))
}

// Cost of independent predecessor scans for n contiguous records in ONE segment.
pub fn expected_scans(records: u64, stride: u64) -> u128 {
    assert!((1..=4096).contains(&stride));
    let (records, stride) = (u128::from(records), u128::from(stride));
    let (blocks, tail) = (records / stride, records % stride);
    blocks * stride * (stride + 1) / 2 + tail * (tail + 1) / 2
}
