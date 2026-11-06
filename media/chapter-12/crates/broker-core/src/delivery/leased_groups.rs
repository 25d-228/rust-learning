use std::sync::Arc;

use crate::storage::error::StorageError;
use crate::storage::log::PartitionLog;
use crate::storage::segment::{OwnedRecord, ReadLimits, ReadStop};
use crate::types::{GROUP_COUNT, GroupId, Offset, PartitionId};
use super::clock::{Clock, ClockError};
use super::groups::GroupState;
use super::lease::{Ack, LeaseConfig, LeaseError, LeaseToken, LeaseWindow};

#[derive(Debug)]
pub struct LeasedRecord {
    pub token: LeaseToken,
    pub record: OwnedRecord,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseStop {
    NoEligible,
    RecordLimit,
    ByteLimit { required: usize },
}
#[derive(Debug)]
pub struct LeaseBatch {
    pub state: GroupState,
    pub records: Vec<LeasedRecord>,
    pub encoded_bytes: usize,
    pub scanned_records: usize,
    pub stop: LeaseStop,
}

pub(crate) enum LeaseReadError {
    Lease(LeaseError),
    Storage(StorageError),
}
impl From<LeaseError> for LeaseReadError {
    fn from(error: LeaseError) -> Self { Self::Lease(error) }
}
impl From<StorageError> for LeaseReadError {
    fn from(error: StorageError) -> Self { Self::Storage(error) }
}

pub(crate) struct LeasedGroups {
    partition: PartitionId,
    windows: [LeaseWindow; GROUP_COUNT as usize],
    clock: Arc<dyn Clock>,
    last_now: Option<u64>,
}
impl LeasedGroups {
    pub(crate) fn new(partition: PartitionId, config: LeaseConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            partition,
            windows: std::array::from_fn(|index| LeaseWindow::new(partition,
                GroupId::new(index as u8).expect("fixed group array"), Offset::new(0), config)),
            clock, last_now: None,
        }
    }

    fn sample(&mut self) -> Result<u64, LeaseError> {
        let now = self.clock.now()?;
        if self.last_now.is_some_and(|previous| now < previous) {
            return Err(ClockError::Regressed.into());
        }
        self.last_now = Some(now);
        Ok(now)
    }

    pub(crate) fn state(&self, group: GroupId) -> GroupState {
        self.windows[usize::from(group.get())].state()
    }

    pub(crate) fn acknowledge(&mut self, group: GroupId, token: LeaseToken) -> Result<Ack, LeaseError> {
        let now = self.sample()?;
        Ok(self.windows[usize::from(group.get())].acknowledge(token, now))
    }

    pub(crate) fn deliver(&mut self, log: &mut PartitionLog, visible: Offset,
        group: GroupId, limits: ReadLimits) -> Result<LeaseBatch, LeaseReadError>
    {
        if log.partition() != self.partition {
            return Err(StorageError::Topology("leases attached to a different partition").into());
        }
        let selected_at = self.sample()?;
        let index = usize::from(group.get());
        let plan = self.windows[index].plan(visible, limits.records(), selected_at)?;
        let mut stop = if plan.len() == limits.records() {
            LeaseStop::RecordLimit
        } else { LeaseStop::NoEligible };
        let mut records = Vec::with_capacity(plan.len());
        let mut encoded_bytes = 0;
        let mut scanned_records = 0;
        for selected in &plan {
            let upper = selected.offset.next().expect("a planned offset has a successor");
            // A one-byte allowance can inspect a nonfitting header even when no bytes remain.
            // Every encoded record is at least 32 bytes; none can slip through that probe.
            let remaining = limits.bytes() - encoded_bytes;
            let one = ReadLimits::new(1, remaining.max(1))?;
            let mut batch = log.read(selected.offset, upper, one)?;
            scanned_records += batch.scanned_records;
            if batch.records.is_empty() {
                if let ReadStop::ByteLimit { required } = batch.stop {
                    stop = LeaseStop::ByteLimit { required };
                    break;
                }
                return Err(StorageError::Topology("planned delivery record was not returned").into());
            }
            if batch.records.len() != 1 || batch.next != upper
                || batch.records[0].offset() != selected.offset || batch.encoded_bytes > remaining
            {
                return Err(StorageError::Topology("single-record lease read is inconsistent").into());
            }
            encoded_bytes += batch.encoded_bytes;
            records.push(batch.records.pop().expect("one validated record"));
        }
        // Do not spend lease time on our own synchronous storage reads.
        // This second observation can fail; until installation no delivery state changed.
        let tokens = if records.is_empty() { Vec::new() } else {
            let issued_at = self.sample()?;
            self.windows[index].install(&plan[..records.len()], visible, issued_at)?
        };
        assert_eq!(records.len(), tokens.len());
        let records = records.into_iter().zip(tokens).map(|(record, token)| {
            LeasedRecord { token, record }
        }).collect();
        Ok(LeaseBatch { state: self.state(group), records,
            encoded_bytes, scanned_records, stop })
    }
}
