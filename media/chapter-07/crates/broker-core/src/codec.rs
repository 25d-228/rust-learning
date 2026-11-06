use crate::error::FormatError;
use crate::types::{Offset, PartitionId};

pub const HEADER_LEN: usize = 32;
pub const MAX_PAYLOAD: usize = 1024 * 1024;
const VERSION: u16 = 1;
const SEGMENT_MAGIC: &[u8; 4] = b"PBLS";
const RECORD_MAGIC: &[u8; 4] = b"PBLR";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probe<T> {
    Incomplete { required: usize },
    Complete { value: T, consumed: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub partition: PartitionId,
    pub base: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    offset: Offset,
    payload_len: usize,
    total_len: usize,
    payload_crc: u32,
}

impl RecordHeader {
    pub fn offset(self) -> Offset { self.offset }
    pub fn payload_len(self) -> usize { self.payload_len }
    pub fn total_len(self) -> usize { self.total_len }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordView<'a> {
    pub offset: Offset,
    pub payload: &'a [u8],
}

fn u16_at(h: &[u8; HEADER_LEN], start: usize) -> u16 {
    u16::from_be_bytes(h[start..start + 2].try_into().expect("fixed u16 field"))
}

fn u32_at(h: &[u8; HEADER_LEN], start: usize) -> u32 {
    u32::from_be_bytes(h[start..start + 4].try_into().expect("fixed u32 field"))
}

fn u64_at(h: &[u8; HEADER_LEN], start: usize) -> u64 {
    u64::from_be_bytes(h[start..start + 8].try_into().expect("fixed u64 field"))
}

fn header_prefix(bytes: &[u8]) -> Option<&[u8; HEADER_LEN]> {
    bytes.get(..HEADER_LEN)?.try_into().ok()
}

fn start_header(magic: &[u8; 4]) -> [u8; HEADER_LEN] {
    let mut h = [0; HEADER_LEN];
    h[0..4].copy_from_slice(magic);
    h[4..6].copy_from_slice(&VERSION.to_be_bytes());
    h[6..8].copy_from_slice(&32_u16.to_be_bytes());
    h
}

fn seal_header(h: &mut [u8; HEADER_LEN]) {
    let crc = crc32fast::hash(&h[..28]);
    h[28..32].copy_from_slice(&crc.to_be_bytes());
}

fn check_header(h: &[u8; HEADER_LEN], magic: &[u8; 4]) -> Result<(), FormatError> {
    let stored = u32_at(h, 28);
    let computed = crc32fast::hash(&h[..28]);
    if stored != computed {
        return Err(FormatError::HeaderChecksum { stored, computed });
    }
    if &h[..4] != magic { return Err(FormatError::Magic); }
    let version = u16_at(h, 4);
    if version != VERSION { return Err(FormatError::Version(version)); }
    let size = u16_at(h, 6);
    if size != 32 { return Err(FormatError::HeaderLength(size)); }
    Ok(())
}

pub fn encode_segment(header: SegmentHeader) -> [u8; HEADER_LEN] {
    let mut h = start_header(SEGMENT_MAGIC);
    h[8..10].copy_from_slice(&header.partition.get().to_be_bytes());
    h[12..20].copy_from_slice(&header.base.get().to_be_bytes());
    seal_header(&mut h);
    h
}

pub fn decode_segment(bytes: &[u8]) -> Result<Probe<SegmentHeader>, FormatError> {
    let Some(h) = header_prefix(bytes) else {
        return Ok(Probe::Incomplete { required: HEADER_LEN });
    };
    check_header(h, SEGMENT_MAGIC)?;
    if h[10..12].iter().chain(h[20..28].iter()).any(|b| *b != 0) {
        return Err(FormatError::Reserved);
    }
    let partition = PartitionId::new(u16_at(h, 8)).map_err(FormatError::Identity)?;
    let base = Offset::new(u64_at(h, 12));
    Ok(Probe::Complete {
        value: SegmentHeader { partition, base }, consumed: HEADER_LEN,
    })
}

pub fn record_len(payload_len: usize) -> Result<usize, FormatError> {
    let total = HEADER_LEN.checked_add(payload_len).ok_or(FormatError::LengthOverflow)?;
    if payload_len > MAX_PAYLOAD {
        return Err(FormatError::TooLarge { length: payload_len, limit: MAX_PAYLOAD });
    }
    Ok(total)
}

pub fn encode_record(offset: Offset, payload: &[u8]) -> Result<Vec<u8>, FormatError> {
    offset.next().map_err(FormatError::Identity)?;
    let total = record_len(payload.len())?;
    let length = u32::try_from(payload.len()).map_err(|_| FormatError::LengthOverflow)?;
    let mut h = start_header(RECORD_MAGIC);
    h[8..16].copy_from_slice(&offset.get().to_be_bytes());
    h[16..20].copy_from_slice(&length.to_be_bytes());
    h[20..24].copy_from_slice(&crc32fast::hash(payload).to_be_bytes());
    seal_header(&mut h);
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&h);
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

pub fn decode_record_header(h: &[u8; HEADER_LEN]) -> Result<RecordHeader, FormatError> {
    check_header(h, RECORD_MAGIC)?;
    if u32_at(h, 24) != 0 { return Err(FormatError::Reserved); }
    let offset = Offset::new(u64_at(h, 8));
    offset.next().map_err(FormatError::Identity)?;
    let payload_len = usize::try_from(u32_at(h, 16)).map_err(|_| FormatError::LengthOverflow)?;
    let total_len = record_len(payload_len)?;
    Ok(RecordHeader { offset, payload_len, total_len, payload_crc: u32_at(h, 20) })
}

pub fn decode_record(bytes: &[u8]) -> Result<Probe<RecordView<'_>>, FormatError> {
    let Some(h) = header_prefix(bytes) else {
        return Ok(Probe::Incomplete { required: HEADER_LEN });
    };
    let header = decode_record_header(h)?;
    if bytes.len() < header.total_len {
        return Ok(Probe::Incomplete { required: header.total_len });
    }
    let payload = &bytes[HEADER_LEN..header.total_len];
    let computed = crc32fast::hash(payload);
    if header.payload_crc != computed {
        return Err(FormatError::PayloadChecksum { stored: header.payload_crc, computed });
    }
    Ok(Probe::Complete {
        value: RecordView { offset: header.offset, payload }, consumed: header.total_len,
    })
}
