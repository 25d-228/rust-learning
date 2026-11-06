use std::io::SeekFrom;

use crate::codec::{
    self, HEADER_LEN, Probe, RecordHeader, SegmentHeader,
};
use crate::error::FormatError;
use crate::io::{StorageFile, read_prefix, write_fully};
use crate::types::Offset;
use super::error::StorageError;
use super::index::SparseIndex;
use super::sync::SyncIo;

#[derive(Debug, Clone, Copy)]
pub struct StorageConfig {
    segment_bytes: u64,
    index_stride: u64,
    max_segments: usize,
}

impl StorageConfig {
    pub fn new(segment_bytes: u64, index_stride: u64, max_segments: usize)
        -> Result<Self, StorageError>
    {
        if !(64..=64 * 1024 * 1024).contains(&segment_bytes) {
            return Err(StorageError::Config("segment bytes must be in 64..=67108864"));
        }
        if !(1..=4096).contains(&index_stride) {
            return Err(StorageError::Config("index stride must be in 1..=4096"));
        }
        if !(1..=64).contains(&max_segments) {
            return Err(StorageError::Config("segment count must be in 1..=64"));
        }
        Ok(Self { segment_bytes, index_stride, max_segments })
    }

    pub fn segment_bytes(self) -> u64 { self.segment_bytes }
    pub fn index_stride(self) -> u64 { self.index_stride }
    pub fn max_segments(self) -> usize { self.max_segments }

    pub(crate) fn check_record(self, payload_len: usize) -> Result<usize, StorageError> {
        let size = codec::record_len(payload_len).map_err(|e| StorageError::format(0, e))?;
        if size as u64 > self.segment_bytes - HEADER_LEN as u64 {
            return Err(StorageError::RecordTooLarge { encoded: size, segment_cap: self.segment_bytes });
        }
        Ok(size)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReadLimits {
    records: usize,
    bytes: usize,
}

impl ReadLimits {
    pub fn new(records: usize, bytes: usize) -> Result<Self, StorageError> {
        if !(1..=1024).contains(&records) || !(1..=4 * 1024 * 1024).contains(&bytes) {
            return Err(StorageError::Config("read limits: 1..=1024 records, 1..=4194304 bytes"));
        }
        Ok(Self { records, bytes })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct OwnedRecord {
    offset: Offset,
    bytes: Vec<u8>,
}

impl OwnedRecord {
    pub fn offset(&self) -> Offset { self.offset }
    pub fn payload(&self) -> &[u8] { &self.bytes[HEADER_LEN..] }
    pub fn encoded_len(&self) -> usize { self.bytes.len() }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStop {
    End,
    RecordLimit,
    ByteLimit { required: usize },
}

#[derive(Debug)]
pub struct ReadBatch {
    pub records: Vec<OwnedRecord>,
    pub next: Offset,
    pub encoded_bytes: usize,
    pub scanned_records: usize,
    pub stop: ReadStop,
}

impl ReadBatch {
    pub(crate) fn empty(start: Offset, limits: ReadLimits) -> Self {
        Self {
            records: Vec::with_capacity(limits.records), next: start,
            encoded_bytes: 0, scanned_records: 0, stop: ReadStop::End,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentInfo {
    pub base: Offset,
    pub end: Offset,
    pub bytes: u64,
    pub index_entries: usize,
}

pub struct Segment<F: StorageFile> {
    file: F,
    header: SegmentHeader,
    config: StorageConfig,
    end: Offset,
    bytes: u64,
    index: SparseIndex,
    poisoned: bool,
}

fn next_offset(offset: Offset, position: u64) -> Result<Offset, StorageError> {
    offset.next().map_err(|e| StorageError::format(position, FormatError::Identity(e)))
}

fn read_header<F: StorageFile>(file: &mut F, position: u64, end: u64, expected: Offset)
    -> Result<([u8; HEADER_LEN], RecordHeader), StorageError>
{
    let available = end.checked_sub(position)
        .ok_or(StorageError::Topology("read position beyond segment end"))?;
    if available < HEADER_LEN as u64 {
        return Err(StorageError::Incomplete { position, required: HEADER_LEN as u64, available });
    }
    file.seek(SeekFrom::Start(position)).map_err(|e| StorageError::io("seek record", position, e))?;
    let mut raw = [0; HEADER_LEN];
    let count = read_prefix(file, &mut raw).map_err(|e| StorageError::io("read header", position, e))?;
    if count != HEADER_LEN {
        return Err(StorageError::Incomplete {
            position, required: HEADER_LEN as u64, available: count as u64,
        });
    }
    let header = codec::decode_record_header(&raw).map_err(|e| StorageError::format(position, e))?;
    if header.offset() != expected {
        return Err(StorageError::Offset { position, expected, found: header.offset() });
    }
    if header.total_len() as u64 > available {
        return Err(StorageError::Incomplete {
            position, required: header.total_len() as u64, available,
        });
    }
    Ok((raw, header))
}

fn read_body<F: StorageFile>(file: &mut F, position: u64, raw: [u8; HEADER_LEN], header: RecordHeader)
    -> Result<OwnedRecord, StorageError>
{
    // read_header left this exclusively owned cursor immediately after the header.
    let mut bytes = vec![0; header.total_len()];
    bytes[..HEADER_LEN].copy_from_slice(&raw);
    let count = read_prefix(file, &mut bytes[HEADER_LEN..])
        .map_err(|e| StorageError::io("read payload", position + HEADER_LEN as u64, e))?;
    if count != header.payload_len() {
        return Err(StorageError::Incomplete {
            position, required: header.total_len() as u64,
            available: (HEADER_LEN + count) as u64,
        });
    }
    match codec::decode_record(&bytes).map_err(|e| StorageError::format(position, e))? {
        Probe::Complete { .. } => Ok(OwnedRecord { offset: header.offset(), bytes }),
        Probe::Incomplete { .. } => Err(StorageError::Topology("filled record decoded as incomplete")),
    }
}

impl<F: StorageFile> Segment<F> {
    pub fn create(mut file: F, header: SegmentHeader, config: StorageConfig)
        -> Result<Self, StorageError>
    {
        let length = file.file_len().map_err(|e| StorageError::io("inspect new segment", 0, e))?;
        if length != 0 { return Err(StorageError::Topology("new segment file is not empty")); }
        file.seek(SeekFrom::Start(0)).map_err(|e| StorageError::io("seek new segment", 0, e))?;
        write_fully(&mut file, &codec::encode_segment(header))
            .map_err(|e| StorageError::io("write segment header", 0, e))?;
        Ok(Self {
            file, header, config, end: header.base, bytes: HEADER_LEN as u64,
            index: SparseIndex::new(header.base, config.index_stride)?, poisoned: false,
        })
    }

    pub fn open_strict(mut file: F, expected: SegmentHeader, config: StorageConfig)
        -> Result<Self, StorageError>
    {
        let length = file.file_len().map_err(|e| StorageError::io("inspect segment", 0, e))?;
        if length > config.segment_bytes {
            return Err(StorageError::Topology("existing segment exceeds configured cap"));
        }
        file.seek(SeekFrom::Start(0)).map_err(|e| StorageError::io("seek segment header", 0, e))?;
        let mut raw = [0; HEADER_LEN];
        let count = read_prefix(&mut file, &mut raw)
            .map_err(|e| StorageError::io("read segment header", 0, e))?;
        let header = match codec::decode_segment(&raw[..count]).map_err(|e| StorageError::format(0, e))? {
            Probe::Incomplete { required } => return Err(StorageError::Incomplete {
                position: 0, required: required as u64, available: count as u64,
            }),
            Probe::Complete { value, .. } => value,
        };
        if header != expected { return Err(StorageError::Identity { expected, found: header }); }
        let mut segment = Self {
            file, header, config, end: header.base, bytes: HEADER_LEN as u64,
            index: SparseIndex::new(header.base, config.index_stride)?, poisoned: false,
        };
        while segment.bytes < length {
            let position = segment.bytes;
            let (raw, record_header) = read_header(&mut segment.file, position, length, segment.end)?;
            let record = read_body(&mut segment.file, position, raw, record_header)?;
            segment.index.record(record.offset, position);
            segment.end = next_offset(record.offset, position)?;
            segment.bytes += record.encoded_len() as u64;
        }
        Ok(segment)
    }

    pub fn info(&self) -> SegmentInfo {
        SegmentInfo {
            base: self.header.base, end: self.end, bytes: self.bytes,
            index_entries: self.index.entries().len(),
        }
    }

    pub fn index(&self) -> &SparseIndex { &self.index }

    pub fn into_file(self) -> F { self.file }

    pub(crate) fn synchronize<S: SyncIo + ?Sized>(&mut self, io: &mut S)
        -> Result<(), StorageError>
    {
        if self.poisoned { return Err(StorageError::Poisoned); }
        self.poisoned = true;
        io.sync(self.header.base, &mut self.file)
            .map_err(|e| StorageError::io("synchronize segment", self.bytes, e))?;
        self.poisoned = false;
        Ok(())
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<Offset, StorageError> {
        if self.poisoned { return Err(StorageError::Poisoned); }
        let size = self.config.check_record(payload.len())?;
        let offset = self.end;
        let next = next_offset(offset, self.bytes)?;
        if size as u64 > self.config.segment_bytes - self.bytes {
            return Err(StorageError::SegmentFull);
        }
        let bytes = codec::encode_record(offset, payload).map_err(|e| StorageError::format(self.bytes, e))?;
        self.poisoned = true;
        self.file.seek(SeekFrom::Start(self.bytes))
            .map_err(|e| StorageError::io("seek append", self.bytes, e))?;
        write_fully(&mut self.file, &bytes)
            .map_err(|e| StorageError::io("append record", self.bytes, e))?;
        self.index.record(offset, self.bytes);
        self.bytes += size as u64;
        self.end = next;
        self.poisoned = false;
        Ok(offset)
    }

    pub(crate) fn read_into(&mut self, upper: Offset, limits: ReadLimits, batch: &mut ReadBatch)
        -> Result<(), StorageError>
    {
        if self.poisoned { return Err(StorageError::Poisoned); }
        let anchor = self.index.predecessor(batch.next)
            .ok_or(StorageError::Topology("nonempty read has no predecessor"))?;
        let mut position = anchor.position;
        let mut expected = anchor.offset;
        while expected < self.end && batch.next < upper {
            if expected == batch.next && batch.records.len() == limits.records {
                batch.stop = ReadStop::RecordLimit;
                break;
            }
            let (raw, header) = read_header(&mut self.file, position, self.bytes, expected)?;
            if expected == batch.next && header.total_len() > limits.bytes - batch.encoded_bytes {
                batch.stop = ReadStop::ByteLimit { required: header.total_len() };
                break;
            }
            let record = read_body(&mut self.file, position, raw, header)?;
            batch.scanned_records += 1;
            let next = next_offset(expected, position)?;
            position += record.encoded_len() as u64;
            if expected == batch.next {
                batch.encoded_bytes += record.encoded_len();
                batch.next = next;
                batch.records.push(record);
            }
            expected = next;
        }
        Ok(())
    }
}
