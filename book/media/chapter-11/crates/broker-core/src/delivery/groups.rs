use crate::storage::error::StorageError;
use crate::storage::log::PartitionLog;
use crate::storage::segment::{OwnedRecord, ReadLimits, ReadStop};
use crate::types::{Epoch, GROUP_COUNT, GroupId, Offset, PartitionId};
use super::window::{Completion, Window, WindowError, WindowState, validate_width};

#[derive(Debug, Clone, Copy)]
pub struct DeliveryConfig {
    epoch: Epoch,
    width: usize,
}

impl DeliveryConfig {
    pub fn new(epoch: Epoch, width: usize) -> Result<Self, WindowError> {
        validate_width(width)?;
        Ok(Self { epoch, width })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupState {
    pub epoch: Epoch,
    pub partition: PartitionId,
    pub group: GroupId,
    pub window: WindowState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStop {
    VisibleEnd,
    WindowFull,
    RecordLimit,
    ByteLimit { required: usize },
}

#[derive(Debug)]
pub struct DeliveryBatch {
    pub state: GroupState,
    pub records: Vec<OwnedRecord>,
    pub encoded_bytes: usize,
    pub scanned_records: usize,
    pub stop: DeliveryStop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCompletion {
    pub state: GroupState,
    pub completion: Completion,
}

pub(crate) struct Groups {
    epoch: Epoch,
    partition: PartitionId,
    windows: [Window; GROUP_COUNT as usize],
}

impl Groups {
    pub(crate) fn new(partition: PartitionId, config: DeliveryConfig) -> Self {
        Self {
            epoch: config.epoch, partition,
            windows: std::array::from_fn(|_| Window::new(Offset::new(0), config.width)
                .expect("DeliveryConfig validated the width")),
        }
    }

    pub(crate) fn state(&self, group: GroupId) -> GroupState {
        GroupState { epoch: self.epoch, partition: self.partition, group,
            window: self.windows[usize::from(group.get())].state() }
    }

    pub(crate) fn complete(&mut self, group: GroupId, offset: Offset)
        -> Result<GroupCompletion, WindowError>
    {
        let completion = self.windows[usize::from(group.get())].complete(offset)?;
        Ok(GroupCompletion { state: self.state(group), completion })
    }

    pub(crate) fn deliver(&mut self, log: &mut PartitionLog, visible: Offset,
        group: GroupId, limits: ReadLimits) -> Result<DeliveryBatch, StorageError>
    {
        if log.partition() != self.partition {
            return Err(StorageError::Topology("group attached to a different partition"));
        }
        let window = &mut self.windows[usize::from(group.get())];
        let start = window.state().issued_end;
        let upper = window.read_upper(visible)
            .map_err(|_| StorageError::Topology("delivery window exceeds visible prefix"))?;
        if start == upper {
            let stop = if start == visible { DeliveryStop::VisibleEnd }
                else { DeliveryStop::WindowFull };
            return Ok(DeliveryBatch { state: self.state(group), records: Vec::new(),
                encoded_bytes: 0, scanned_records: 0, stop });
        }
        // read owns its copied buffers. No window changes if it returns an error.
        let batch = log.read(start, upper, limits)?;
        let count = batch.records.len();
        let expected_next = start.get().checked_add(count as u64)
            .ok_or(StorageError::Topology("delivery count overflows offset"))?;
        if batch.next != Offset::new(expected_next) || batch.next > upper {
            return Err(StorageError::Topology("read returned an inconsistent delivery interval"));
        }
        // The end promised by the caller is not the number actually copied.
        window.admit(count, visible)
            .map_err(|_| StorageError::Topology("bounded read cannot fit its delivery window"))?;
        let stop = if batch.next == visible { DeliveryStop::VisibleEnd }
            else if window.state().free() == 0 { DeliveryStop::WindowFull }
            else {
                match batch.stop {
                    ReadStop::RecordLimit => DeliveryStop::RecordLimit,
                    ReadStop::ByteLimit { required } => DeliveryStop::ByteLimit { required },
                    ReadStop::End => unreachable!("bounded upper is the visible or window end"),
                }
            };
        Ok(DeliveryBatch { state: self.state(group), records: batch.records,
            encoded_bytes: batch.encoded_bytes, scanned_records: batch.scanned_records, stop })
    }
}
