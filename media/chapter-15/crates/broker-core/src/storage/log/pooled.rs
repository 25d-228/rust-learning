use super::*;
use crate::storage::segment::RecordCopy;

impl PartitionLog {
    pub(crate) fn append_pooled(&mut self, payload: &[u8], permit: Option<DiskPermit>, scratch: &mut [u8])
        -> Result<Offset, StorageError>
    {
        let size = self.check_append(payload.len())?;
        match (&self.disk, &permit) {
            (Some(disk), Some(permit)) if permit.belongs_to(disk) => {},
            _ => return Err(StorageError::Config("bounded append requires its disk reservation")),
        }
        let before = self.live_bytes();
        let result = (|| {
            let tail = self.segments.last().expect("log has a segment").info();
            if size as u64 > self.config.segment_bytes() - tail.bytes { self.rotate_file(false)?; }
            self.poisoned = true;
            let last = self.segments.len() - 1;
            self.dirty[last] = true;
            self.segments[last].append_using(payload, scratch)
        })();
        match result {
            Ok(offset) => {
                permit.expect("validated disk permit").settle(self.live_bytes() - before);
                self.poisoned = false;
                Ok(offset)
            }
            Err(error) => { self.stop_admission(); Err(error) }
        }
    }

    pub(crate) fn copy_record(&mut self, target: Offset, output: &mut [u8], scratch: &mut [u8])
        -> Result<RecordCopy, StorageError>
    {
        self.healthy()?;
        let floor = self.retained_floor();
        let upper = target.next().map_err(|e| StorageError::format(0, crate::error::FormatError::Identity(e)))?;
        if target < floor || upper > self.written_end() {
            return Err(StorageError::Range { start: target, upper, floor, end: self.written_end() });
        }
        let right = self.segments.partition_point(|segment| segment.info().base <= target);
        let index = right.checked_sub(1).ok_or(StorageError::Topology("no bounded-read segment"))?;
        self.poisoned = true;
        let result = self.segments[index].copy_record(target, output, scratch);
        if result.is_ok() { self.poisoned = false; }
        result
    }
}
