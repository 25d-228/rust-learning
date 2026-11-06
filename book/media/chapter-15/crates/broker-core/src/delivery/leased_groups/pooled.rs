use std::ops::Range;

use super::*;
use crate::budget::memory::PoolBuffer;
use crate::codec::HEADER_LEN;
use crate::storage::segment::RecordCopy;

#[derive(Debug)]
struct Entry { token: LeaseToken, range: Range<usize> }

#[derive(Debug)]
pub struct PooledLeaseBatch {
    buffer: PoolBuffer,
    entries: Vec<Entry>,
    pub state: GroupState,
    pub encoded_bytes: usize,
    pub scanned_records: usize,
    pub stop: LeaseStop,
}
#[derive(Debug, Clone, Copy)]
pub struct PooledRecord<'a> {
    pub token: LeaseToken,
    pub encoded: &'a [u8],
}
impl<'a> PooledRecord<'a> {
    pub fn payload(&self) -> &'a [u8] { &self.encoded[HEADER_LEN..] }
}
impl PooledLeaseBatch {
    pub fn records(&self) -> impl ExactSizeIterator<Item = PooledRecord<'_>> + '_ {
        self.entries.iter().map(|entry| PooledRecord {
            token: entry.token, encoded: &self.buffer.as_slice()[entry.range.clone()],
        })
    }
}

impl LeasedGroups {
    pub(crate) fn deliver_pooled(&mut self, log: &mut PartitionLog, visible: Offset,
        group: GroupId, limits: ReadLimits, mut buffer: PoolBuffer, scratch: &mut [u8])
        -> Result<PooledLeaseBatch, LeaseReadError>
    {
        if log.partition() != self.partition || limits.bytes() > buffer.as_slice().len() {
            return Err(StorageError::Config("bounded delivery has the wrong scope or output capacity").into());
        }
        let selected_at = self.sample()?;
        let index = usize::from(group.get());
        let plan = self.windows[index].plan(visible, limits.records(), selected_at)?;
        let mut stop = if plan.len() == limits.records() { LeaseStop::RecordLimit } else { LeaseStop::NoEligible };
        let mut ranges = Vec::new();
        ranges.try_reserve_exact(plan.len()).map_err(|source| StorageError::Allocation {
            operation: "bounded delivery ranges", source,
        })?;
        // Allocate result metadata before installing any token. Payload bytes are already reserved.
        let mut entries = Vec::new();
        entries.try_reserve_exact(plan.len()).map_err(|source| StorageError::Allocation {
            operation: "bounded delivery entries", source,
        })?;
        let mut encoded_bytes = 0;
        let mut scanned_records = 0;
        for selected in &plan {
            let result = log.copy_record(selected.offset,
                &mut buffer.as_mut_slice()[encoded_bytes..limits.bytes()], scratch)?;
            match result {
                RecordCopy::TooSmall { required, scanned } => {
                    scanned_records += scanned; stop = LeaseStop::ByteLimit { required }; break;
                }
                RecordCopy::Copied { bytes, scanned } => {
                    scanned_records += scanned;
                    ranges.push(encoded_bytes..encoded_bytes + bytes);
                    encoded_bytes += bytes;
                }
            }
        }
        let tokens = if ranges.is_empty() { Vec::new() } else {
            let issued_at = self.sample()?;
            self.windows[index].install(&plan[..ranges.len()], visible, issued_at)?
        };
        assert_eq!(ranges.len(), tokens.len());
        for (range, token) in ranges.into_iter().zip(tokens) { entries.push(Entry { token, range }); }
        buffer.set_len(encoded_bytes);
        Ok(PooledLeaseBatch { buffer, entries, state: self.state(group), encoded_bytes,
            scanned_records, stop })
    }
}
