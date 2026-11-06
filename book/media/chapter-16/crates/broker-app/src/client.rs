use std::error::Error;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::Instant;

use broker_core::delivery::{AckOutcome, LeaseToken};
use broker_core::delivery::progress::{Checkpointed, ProgressState};
use broker_core::types::{Epoch, GroupId, Offset, PartitionId};
use crate::protocol::{self, FetchedBatch, Header, Op, Status};

#[derive(Debug)]
pub enum ClientError { Io(io::Error), Remote(Status) }
impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "transport or protocol: {error}"),
            Self::Remote(status) => write!(f, "broker response: {status:?}"),
        }
    }
}
impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self { Self::Io(error) => Some(error), Self::Remote(_) => None }
    }
}
impl From<io::Error> for ClientError { fn from(error: io::Error) -> Self { Self::Io(error) } }

#[derive(Debug, Clone, Copy)]
pub struct AckReply { pub outcome: AckOutcome, pub progress: ProgressState, pub retired: u64 }
#[derive(Debug, Clone, Copy)]
pub struct RetentionReply {
    pub before: Offset, pub floor: Offset, pub removed_bytes: u64,
    pub removed_segments: u64, pub rotated: bool,
}
enum Reply { Small(Header), Batch(FetchedBatch) }

pub struct Client { stream: Option<TcpStream>, next_id: u64, timeout: Duration }
impl Client {
    pub async fn connect(address: SocketAddr, timeout: Duration) -> Result<Self, ClientError> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "client timeout must be in (0, 60s]").into());
        }
        let stream = protocol::within(Instant::now() + timeout, TcpStream::connect(address)).await?;
        Ok(Self { stream: Some(stream), next_id: 1, timeout })
    }
    pub fn is_usable(&self) -> bool { self.stream.is_some() }

    async fn call(&mut self, mut request: Header, payload: &[u8]) -> Result<Reply, ClientError> {
        request.id = self.next_id;
        protocol::validate_request(request)?;
        if payload.len() != request.body as usize { return Err(protocol::invalid("request body mismatch").into()); }
        let next = self.next_id.checked_add(1).ok_or_else(|| protocol::invalid("request identifier exhausted"))?;
        // Once polled this far, cancellation must close rather than recycle a partial stream.
        let mut stream = self.stream.take().ok_or_else(||
            io::Error::new(io::ErrorKind::NotConnected, "client connection is closed; reconnect explicitly"))?;
        self.next_id = next;
        let deadline = Instant::now() + self.timeout;
        let result: Result<Reply, ClientError> = async {
            protocol::write_bytes(&mut stream, &request.encode(), deadline).await?;
            if !payload.is_empty() { protocol::write_bytes(&mut stream, payload, deadline).await?; }
            let response = protocol::read_header(&mut stream, deadline).await?
                .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing response"))?;
            protocol::validate_response(request, response)?;
            if response.status != Status::Ok { return Err(ClientError::Remote(response.status)); }
            if request.op != Op::Fetch { return Ok(Reply::Small(response)); }
            // Shape and request-specific bounds were checked before this allocation.
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(response.body as usize).map_err(io::Error::other)?;
            bytes.resize(response.body as usize, 0);
            protocol::read_body(&mut stream, &mut bytes, deadline).await?;
            let batch = FetchedBatch::decode(response, bytes)?;
            Ok(Reply::Batch(batch))
        }.await;
        if result.is_ok() { self.stream = Some(stream); }
        // On any error, or if this future is dropped while pending, the local socket is dropped.
        result
    }

    pub async fn append(&mut self, partition: PartitionId, payload: &[u8]) -> Result<Offset, ClientError> {
        let mut request = Header::request(Op::Append, partition, GroupId::new(0).expect("fixed group"));
        request.body = u32::try_from(payload.len()).map_err(|_| protocol::invalid("payload length overflow"))?;
        match self.call(request, payload).await? {
            Reply::Small(header) => Ok(Offset::new(header.args[0])),
            Reply::Batch(_) => unreachable!("append is a validated small response"),
        }
    }
    pub async fn fetch(&mut self, partition: PartitionId, group: GroupId, records: usize, bytes: usize)
        -> Result<FetchedBatch, ClientError>
    {
        let mut request = Header::request(Op::Fetch, partition, group);
        request.args = [records as u64, bytes as u64, 0, 0, 0];
        match self.call(request, &[]).await? {
            Reply::Batch(batch) => Ok(batch), Reply::Small(_) => unreachable!("fetch response is a batch"),
        }
    }
    pub async fn acknowledge(&mut self, token: LeaseToken) -> Result<AckReply, ClientError> {
        let mut request = Header::request(Op::Ack, token.partition, token.group);
        request.args = [token.epoch.get(), token.offset.get(), token.generation, token.deadline_ms, 0];
        match self.call(request, &[]).await? {
            Reply::Small(header) => Ok(AckReply {
                outcome: protocol::decode_ack(header.args[0]).expect("validated acknowledgement"),
                progress: progress(header, 1, 2), retired: header.args[3],
            }),
            Reply::Batch(_) => unreachable!("acknowledgement is a small response"),
        }
    }
    pub async fn checkpoint(&mut self, partition: PartitionId, group: GroupId) -> Result<Checkpointed, ClientError> {
        match self.call(Header::request(Op::Checkpoint, partition, group), &[]).await? {
            Reply::Small(header) => Ok(Checkpointed { progress: progress(header, 0, 1), published: header.args[2] == 1 }),
            Reply::Batch(_) => unreachable!("checkpoint is a small response"),
        }
    }
    pub async fn progress(&mut self, partition: PartitionId, group: GroupId) -> Result<ProgressState, ClientError> {
        match self.call(Header::request(Op::Progress, partition, group), &[]).await? {
            Reply::Small(header) => Ok(progress(header, 0, 1)),
            Reply::Batch(_) => unreachable!("progress is a small response"),
        }
    }
    pub async fn retain(&mut self, partition: PartitionId) -> Result<RetentionReply, ClientError> {
        let request = Header::request(Op::Retain, partition, GroupId::new(0).expect("fixed group"));
        match self.call(request, &[]).await? {
            Reply::Small(header) => Ok(RetentionReply {
                before: Offset::new(header.args[0]), floor: Offset::new(header.args[1]),
                removed_bytes: header.args[2], removed_segments: header.args[3], rotated: header.args[4] == 1,
            }),
            Reply::Batch(_) => unreachable!("retention is a small response"),
        }
    }
}
fn progress(header: Header, volatile: usize, persisted: usize) -> ProgressState {
    let (partition, group) = header.scope().expect("validated scope");
    ProgressState { epoch: Epoch::new(header.args[4]).expect("validated epoch"), partition, group,
        volatile_next: Offset::new(header.args[volatile]), persisted_next: Offset::new(header.args[persisted]) }
}
