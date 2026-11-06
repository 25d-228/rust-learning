use std::future::Future;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Instant, timeout_at};

use broker_core::codec::{HEADER_LEN, MAX_PAYLOAD};
use broker_core::delivery::{AckOutcome, AckRejection, LeaseToken};
use broker_core::types::{Epoch, GroupId, PartitionId};

mod batch;
pub use batch::{FetchedBatch, FetchedRecord};

pub const WIRE_HEADER: usize = 64;
pub const TOKEN_BYTES: usize = 32;
pub const MAX_FETCH_RECORDS: usize = 64;
pub const MAX_ENCODED: usize = 2 * 1024 * 1024;
pub const MAX_WIRE_BODY: usize = MAX_ENCODED + TOKEN_BYTES * MAX_FETCH_RECORDS;

pub fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "absolute socket deadline reached")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op { Append = 1, Fetch = 2, Ack = 3, Checkpoint = 4, Progress = 5, Retain = 6 }
impl Op {
    fn decode(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(Self::Append), 2 => Ok(Self::Fetch), 3 => Ok(Self::Ack),
            4 => Ok(Self::Checkpoint), 5 => Ok(Self::Progress), 6 => Ok(Self::Retain),
            _ => Err(invalid("unknown operation")),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Request = 0, Ok = 1, Invalid = 2, Busy = 3, DiskFull = 4,
    Unavailable = 5, AdmissionDeadline = 6, Refused = 7, Uncertain = 8,
}
impl Status {
    fn decode(value: u8) -> io::Result<Self> {
        match value {
            0 => Ok(Self::Request), 1 => Ok(Self::Ok), 2 => Ok(Self::Invalid),
            3 => Ok(Self::Busy), 4 => Ok(Self::DiskFull), 5 => Ok(Self::Unavailable),
            6 => Ok(Self::AdmissionDeadline), 7 => Ok(Self::Refused), 8 => Ok(Self::Uncertain),
            _ => Err(invalid("unknown status")),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint { Producer, Consumer }
impl Endpoint {
    pub fn accepts(self, op: Op) -> bool {
        match self { Self::Producer => op == Op::Append, Self::Consumer => op != Op::Append }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub op: Op,
    pub status: Status,
    pub partition: u16,
    pub group: u8,
    pub body: u32,
    pub id: u64,
    pub args: [u64; 5],
}
impl Header {
    pub fn request(op: Op, partition: PartitionId, group: GroupId) -> Self {
        Self { op, status: Status::Request, partition: partition.get(), group: group.get(),
            body: 0, id: 1, args: [0; 5] }
    }
    pub fn response(self, status: Status, args: [u64; 5], body: u32) -> Self {
        Self { status, args, body, ..self }
    }
    pub fn encode(self) -> [u8; WIRE_HEADER] {
        let mut bytes = [0; WIRE_HEADER];
        bytes[..4].copy_from_slice(b"PBNW"); bytes[4] = 1;
        bytes[5] = self.op as u8; bytes[6] = self.status as u8;
        bytes[8..10].copy_from_slice(&self.partition.to_be_bytes()); bytes[10] = self.group;
        bytes[12..16].copy_from_slice(&self.body.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.id.to_be_bytes());
        for (index, value) in self.args.iter().enumerate() {
            let start = 24 + 8 * index;
            bytes[start..start + 8].copy_from_slice(&value.to_be_bytes());
        }
        bytes
    }
    pub fn decode(bytes: &[u8; WIRE_HEADER]) -> io::Result<Self> {
        if &bytes[..4] != b"PBNW" || bytes[4] != 1 || bytes[7] != 0 || bytes[11] != 0 {
            return Err(invalid("wrong magic, version, or reserved byte"));
        }
        let mut args = [0; 5];
        for (index, value) in args.iter_mut().enumerate() {
            let start = 24 + 8 * index;
            *value = u64::from_be_bytes(bytes[start..start + 8].try_into().expect("fixed field"));
        }
        let value = Self {
            op: Op::decode(bytes[5])?, status: Status::decode(bytes[6])?,
            partition: u16::from_be_bytes([bytes[8], bytes[9]]), group: bytes[10],
            body: u32::from_be_bytes(bytes[12..16].try_into().expect("fixed field")),
            id: u64::from_be_bytes(bytes[16..24].try_into().expect("fixed field")), args,
        };
        if value.id == 0 { return Err(invalid("request identifier must be nonzero")); }
        Ok(value)
    }
    pub fn scope(self) -> io::Result<(PartitionId, GroupId)> {
        let p = PartitionId::new(self.partition).map_err(|_| invalid("partition out of range"))?;
        let g = GroupId::new(self.group).map_err(|_| invalid("group out of range"))?;
        Ok((p, g))
    }
}

pub fn validate_request(h: Header) -> io::Result<(PartitionId, GroupId)> {
    let scope = h.scope()?;
    if h.id == 0 || h.status != Status::Request { return Err(invalid("not a request")); }
    if h.op != Op::Append && h.body != 0 { return Err(invalid("unexpected request body")); }
    match h.op {
        Op::Append => {
            if h.body as u64 > MAX_PAYLOAD as u64 || h.group != 0 || h.args != [0; 5] {
                return Err(invalid("invalid append shape"));
            }
        }
        Op::Fetch => {
            if !(1..=MAX_FETCH_RECORDS as u64).contains(&h.args[0])
                || !(1..=MAX_ENCODED as u64).contains(&h.args[1]) || h.args[2..] != [0; 3]
            { return Err(invalid("invalid fetch limits")); }
        }
        Op::Ack => {
            if h.args[0] == 0 || h.args[1] == u64::MAX || h.args[2] == 0
                || h.args[3] == 0 || h.args[4] != 0
            { return Err(invalid("invalid acknowledgement token")); }
        }
        Op::Checkpoint | Op::Progress => {
            if h.args != [0; 5] { return Err(invalid("unexpected control arguments")); }
        }
        Op::Retain => {
            if h.group != 0 || h.args != [0; 5] { return Err(invalid("invalid retention request")); }
        }
    }
    Ok(scope)
}

pub fn body_size(records: usize, encoded: usize) -> io::Result<usize> {
    if records > MAX_FETCH_RECORDS || encoded > MAX_ENCODED
        || encoded < records * HEADER_LEN || (records == 0 && encoded != 0)
    { return Err(invalid("invalid fetched body dimensions")); }
    encoded.checked_add(records.checked_mul(TOKEN_BYTES).ok_or_else(|| invalid("body overflow"))?)
        .ok_or_else(|| invalid("body overflow"))
}

pub fn validate_response(request: Header, reply: Header) -> io::Result<()> {
    if reply.id != request.id || reply.op != request.op || reply.partition != request.partition
        || reply.group != request.group || reply.status == Status::Request
    { return Err(invalid("response does not match its request")); }
    reply.scope()?;
    if reply.status != Status::Ok {
        if reply.body != 0 || reply.args != [0; 5] { return Err(invalid("error response must be empty")); }
        return Ok(());
    }
    let [a, b, c, d, e] = reply.args;
    if reply.op != Op::Fetch && reply.body != 0 { return Err(invalid("unexpected response body")); }
    match reply.op {
        Op::Append => {
            if a == u64::MAX || [b, c, d, e] != [0; 4] { return Err(invalid("invalid append response")); }
        }
        Op::Fetch => {
            if a > request.args[0] || b > request.args[1] || e == 0 || c > 2 {
                return Err(invalid("fetch response exceeds request or has no epoch"));
            }
            let count = usize::try_from(a).map_err(|_| invalid("record count overflow"))?;
            let encoded = usize::try_from(b).map_err(|_| invalid("encoded size overflow"))?;
            if reply.body as u64 != body_size(count, encoded)? as u64 {
                return Err(invalid("fetched wire size mismatch"));
            }
            match c {
                0 if d == 0 && a < request.args[0] => {},
                1 if d == 0 && a == request.args[0] => {},
                2 if a < request.args[0] && d >= HEADER_LEN as u64
                    && d <= (HEADER_LEN + MAX_PAYLOAD) as u64 && d > request.args[1] - b => {},
                _ => return Err(invalid("invalid fetch stop reason")),
            }
        }
        Op::Ack => {
            decode_ack(a)?;
            if c > b || d > 4096 || e == 0 { return Err(invalid("invalid acknowledgement progress")); }
        }
        Op::Checkpoint | Op::Progress => {
            if b > a || e == 0 || d != 0 || c > 1 || (reply.op == Op::Progress && c != 0)
                || (reply.op == Op::Checkpoint && a != b)
            { return Err(invalid("invalid progress response")); }
        }
        Op::Retain => {
            if b < a || d > 65 || e > 1 { return Err(invalid("invalid retention response")); }
        }
    }
    Ok(())
}

pub fn encode_token(token: LeaseToken) -> [u8; TOKEN_BYTES] {
    let mut raw = [0; TOKEN_BYTES];
    for (index, value) in [token.epoch.get(), token.offset.get(), token.generation, token.deadline_ms]
        .into_iter().enumerate()
    { raw[index * 8..index * 8 + 8].copy_from_slice(&value.to_be_bytes()); }
    raw
}
pub fn decode_token(raw: &[u8; TOKEN_BYTES], scope: Header) -> io::Result<LeaseToken> {
    let (partition, group) = scope.scope()?;
    let mut fields = [0; 4];
    for (index, value) in fields.iter_mut().enumerate() {
        *value = u64::from_be_bytes(raw[index * 8..index * 8 + 8].try_into().expect("fixed field"));
    }
    if fields[1] == u64::MAX || fields[2] == 0 || fields[3] == 0 {
        return Err(invalid("invalid fetched token"));
    }
    Ok(LeaseToken { epoch: Epoch::new(fields[0]).map_err(|_| invalid("zero token epoch"))?,
        partition, group, offset: broker_core::types::Offset::new(fields[1]),
        generation: fields[2], deadline_ms: fields[3] })
}
pub fn encode_ack(outcome: AckOutcome) -> u64 {
    match outcome {
        AckOutcome::Applied => 0, AckOutcome::Duplicate => 1, AckOutcome::AlreadyRetired => 2,
        AckOutcome::Rejected(reason) => match reason {
            AckRejection::WrongEpoch => 10, AckRejection::WrongPartition => 11,
            AckRejection::WrongGroup => 12, AckRejection::NotIssued => 13,
            AckRejection::StaleGeneration => 14, AckRejection::WrongDeadline => 15,
            AckRejection::Expired => 16,
        },
    }
}
pub fn decode_ack(value: u64) -> io::Result<AckOutcome> {
    Ok(match value {
        0 => AckOutcome::Applied, 1 => AckOutcome::Duplicate, 2 => AckOutcome::AlreadyRetired,
        10 => AckOutcome::Rejected(AckRejection::WrongEpoch),
        11 => AckOutcome::Rejected(AckRejection::WrongPartition),
        12 => AckOutcome::Rejected(AckRejection::WrongGroup),
        13 => AckOutcome::Rejected(AckRejection::NotIssued),
        14 => AckOutcome::Rejected(AckRejection::StaleGeneration),
        15 => AckOutcome::Rejected(AckRejection::WrongDeadline),
        16 => AckOutcome::Rejected(AckRejection::Expired),
        _ => return Err(invalid("unknown acknowledgement outcome")),
    })
}

pub async fn within<F, T>(deadline: Instant, future: F) -> io::Result<T>
where F: Future<Output = io::Result<T>> {
    if Instant::now() >= deadline { return Err(timed_out()); }
    let result = timeout_at(deadline, future).await.map_err(|_| timed_out())?;
    if Instant::now() >= deadline { return Err(timed_out()); }
    result
}
pub async fn read_header<R: AsyncRead + Unpin>(reader: &mut R, deadline: Instant)
    -> io::Result<Option<Header>>
{
    let mut raw = [0; WIRE_HEADER];
    let mut used = 0;
    while used < raw.len() {
        let n = within(deadline, reader.read(&mut raw[used..])).await?;
        if n == 0 {
            return if used == 0 { Ok(None) }
                else { Err(io::Error::new(io::ErrorKind::UnexpectedEof, "partial wire header")) };
        }
        used += n;
    }
    Header::decode(&raw).map(Some)
}
pub async fn read_body<R: AsyncRead + Unpin>(reader: &mut R, body: &mut [u8], deadline: Instant)
    -> io::Result<()>
{
    within(deadline, reader.read_exact(body)).await.map(|_| ())
}
pub async fn write_bytes<W: AsyncWrite + Unpin>(writer: &mut W, bytes: &[u8], deadline: Instant)
    -> io::Result<()>
{
    within(deadline, writer.write_all(bytes)).await
}
