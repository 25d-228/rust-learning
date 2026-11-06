use std::io;
use std::path::Path;
use std::time::Duration;
use broker_core::types::{GroupId, PartitionId};
use crate::BoxError;

mod children;
mod ledger;
mod process;
mod scenario;
#[cfg(test)]
mod tests;

pub use children::{broker_child, consumer_child};
pub use scenario::run;

pub const PAYLOAD_BYTES: usize = 24;
pub const RECORD_BYTES: usize = 32 + PAYLOAD_BYTES;
pub const FETCH_BYTES: usize = 4 * RECORD_BYTES;
pub const DATA_QUOTA: u64 = 640;
pub const TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_ATTEMPTS: usize = 4096;

fn p(index: u16) -> PartitionId { PartitionId::new(index).expect("fixed partition") }
fn g(index: u8) -> GroupId { GroupId::new(index).expect("fixed group") }
fn require(condition: bool, message: &str) -> io::Result<()> {
    if condition { Ok(()) } else { Err(io::Error::other(message.to_owned())) }
}
pub fn validate_rounds(rounds: u32) -> io::Result<()> {
    require((1..=32).contains(&rounds), "rounds must be in 1..=32")
}

#[derive(Debug, Clone)]
pub struct Report {
    pub confirmed: usize,
    pub ambiguous: usize,
    pub rejected: usize,
    pub observed_ambiguous: usize,
    pub unique_records: usize,
    pub repeated_deliveries: u64,
    pub duplicate_logical_inputs: usize,
    pub rounds: u32,
    pub floor: [u64; 4],
    pub broker_kills: u32,
    pub consumer_kills: u32,
    pub obsolete_token_rejected: bool,
}

pub fn execute(executable: &Path, root: &Path, rounds: u32) -> Result<Report, BoxError> {
    validate_rounds(rounds)?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(run(executable, root, rounds))
}
