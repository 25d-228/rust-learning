use std::io;
use std::ops::Range;
use broker_core::codec::{self, HEADER_LEN, Probe};
use broker_core::delivery::{LeaseStop, LeaseToken};
use super::{Header, Op, Status, TOKEN_BYTES, body_size, decode_token, invalid};

#[derive(Debug)]
struct Entry { token: LeaseToken, payload: Range<usize> }
#[derive(Debug)]
pub struct FetchedBatch {
    body: Vec<u8>,
    entries: Vec<Entry>,
    pub encoded_bytes: usize,
    pub stop: LeaseStop,
}
#[derive(Debug, Clone, Copy)]
pub struct FetchedRecord<'a> { pub token: LeaseToken, pub payload: &'a [u8] }
impl FetchedBatch {
    // The caller first validates the response against its exact request.
    pub(crate) fn decode(header: Header, body: Vec<u8>) -> io::Result<Self> {
        let [count, encoded, stop, required, epoch] = header.args;
        let count = usize::try_from(count).map_err(|_| invalid("count overflow"))?;
        let encoded = usize::try_from(encoded).map_err(|_| invalid("size overflow"))?;
        if header.op != Op::Fetch || header.status != Status::Ok
            || body.len() != header.body as usize || body.len() != body_size(count, encoded)?
        { return Err(invalid("invalid fetch envelope")); }
        let mut entries: Vec<Entry> = Vec::new();
        entries.try_reserve_exact(count).map_err(io::Error::other)?;
        let mut position = 0usize;
        let mut record_bytes = 0usize;
        for _ in 0..count {
            let raw = body.get(position..position + TOKEN_BYTES)
                .ok_or_else(|| invalid("short token"))?;
            let token = decode_token(raw.try_into().expect("fixed token"), header)?;
            if token.epoch.get() != epoch || entries.iter().any(|e| e.token.offset == token.offset) {
                return Err(invalid("epoch mismatch or duplicate offset in one batch"));
            }
            position += TOKEN_BYTES;
            let (view, consumed) = match codec::decode_record(&body[position..])
                .map_err(|_| invalid("invalid fetched record checksum or header"))?
            {
                Probe::Complete { value, consumed } => (value, consumed),
                Probe::Incomplete { .. } => return Err(invalid("short fetched record")),
            };
            if view.offset != token.offset { return Err(invalid("token and record offsets disagree")); }
            entries.push(Entry { token, payload: position + HEADER_LEN..position + consumed });
            position += consumed;
            record_bytes += consumed;
        }
        if position != body.len() || record_bytes != encoded { return Err(invalid("trailing or miscounted body bytes")); }
        let stop = match stop {
            0 => LeaseStop::NoEligible, 1 => LeaseStop::RecordLimit,
            2 => LeaseStop::ByteLimit { required: usize::try_from(required).map_err(|_| invalid("size overflow"))? },
            _ => return Err(invalid("unknown stop reason")),
        };
        Ok(Self { body, entries, encoded_bytes: encoded, stop })
    }
    pub fn records(&self) -> impl ExactSizeIterator<Item = FetchedRecord<'_>> + '_ {
        self.entries.iter().map(|entry| FetchedRecord {
            token: entry.token, payload: &self.body[entry.payload.clone()],
        })
    }
}
