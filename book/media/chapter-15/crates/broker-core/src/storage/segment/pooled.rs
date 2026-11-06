use super::*;

#[derive(Debug, Clone, Copy)]
pub(crate) enum RecordCopy {
    Copied { bytes: usize, scanned: usize },
    TooSmall { required: usize, scanned: usize },
}

impl<F: StorageFile> Segment<F> {
    pub(crate) fn append_using(&mut self, payload: &[u8], scratch: &mut [u8])
        -> Result<Offset, StorageError>
    {
        if self.poisoned { return Err(StorageError::Poisoned); }
        let size = self.config.check_record(payload.len())?;
        let offset = self.end;
        let next = next_offset(offset, self.bytes)?;
        if size as u64 > self.config.segment_bytes - self.bytes { return Err(StorageError::SegmentFull); }
        if size > scratch.len() { return Err(StorageError::Config("owner scratch cannot hold a record")); }
        let header = codec::encode_record_header(offset, payload)
            .map_err(|e| StorageError::format(self.bytes, e))?;
        scratch[..HEADER_LEN].copy_from_slice(&header);
        scratch[HEADER_LEN..size].copy_from_slice(payload);
        self.poisoned = true;
        self.file.seek(SeekFrom::Start(self.bytes))
            .map_err(|e| StorageError::io("seek bounded append", self.bytes, e))?;
        write_fully(&mut self.file, &scratch[..size])
            .map_err(|e| StorageError::io("append bounded record", self.bytes, e))?;
        self.index.record(offset, self.bytes);
        self.bytes += size as u64;
        self.end = next;
        self.poisoned = false;
        Ok(offset)
    }

    pub(crate) fn copy_record(&mut self, target: Offset, output: &mut [u8], scratch: &mut [u8])
        -> Result<RecordCopy, StorageError>
    {
        if self.poisoned { return Err(StorageError::Poisoned); }
        let anchor = self.index.predecessor(target)
            .ok_or(StorageError::Topology("bounded read has no index predecessor"))?;
        let mut position = anchor.position;
        let mut expected = anchor.offset;
        let mut scanned = 0;
        while expected <= target && expected < self.end {
            let (raw, header) = read_header(&mut self.file, position, self.bytes, expected)?;
            let size = header.total_len();
            if expected == target && size > output.len() {
                return Ok(RecordCopy::TooSmall { required: size, scanned });
            }
            let destination = if expected == target { &mut *output } else { &mut *scratch };
            if size > destination.len() {
                return Err(StorageError::Config("owner scratch cannot validate a skipped record"));
            }
            destination[..HEADER_LEN].copy_from_slice(&raw);
            let count = read_prefix(&mut self.file, &mut destination[HEADER_LEN..size])
                .map_err(|e| StorageError::io("copy bounded payload", position + HEADER_LEN as u64, e))?;
            if count != header.payload_len() {
                return Err(StorageError::Incomplete { position, required: size as u64,
                    available: (HEADER_LEN + count) as u64 });
            }
            match codec::decode_record(&destination[..size]).map_err(|e| StorageError::format(position, e))? {
                Probe::Complete { .. } => {},
                Probe::Incomplete { .. } => return Err(StorageError::Topology("filled bounded read is incomplete")),
            }
            scanned += 1;
            if expected == target { return Ok(RecordCopy::Copied { bytes: size, scanned }); }
            position += size as u64;
            expected = next_offset(expected, position)?;
        }
        Err(StorageError::Topology("bounded read ended before the requested record"))
    }
}
