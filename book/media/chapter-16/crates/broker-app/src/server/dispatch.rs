use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Instant, timeout_at};

use broker_core::delivery::{LeaseStop, PooledLeaseBatch};
use broker_core::delivery::progress::ProgressAck;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::segment::ReadLimits;
use broker_core::types::{Epoch, Offset};
use broker_core::worker::{AsyncTicket, BoundedClient, RequestError};
use crate::protocol::{self, Header, Op, Status, body_size, encode_ack, encode_token};
use super::SessionLimits;

pub(super) enum Output { Small([u64; 5]), Batch(PooledLeaseBatch) }
pub(super) enum CallError { Status(Status), Transport(io::Error) }
impl From<io::Error> for CallError {
    fn from(error: io::Error) -> Self { Self::Transport(error) }
}
fn admission(error: AdmissionError) -> CallError {
    CallError::Status(match error {
        AdmissionError::Full | AdmissionError::MemoryFull | AdmissionError::WaitersFull => Status::Busy,
        AdmissionError::DiskFull => Status::DiskFull,
        AdmissionError::Closed => Status::Unavailable,
        AdmissionError::InvalidRecord | AdmissionError::TooLarge => Status::Invalid,
        AdmissionError::Deadline => Status::AdmissionDeadline,
    })
}
async fn owner<T>(ticket: AsyncTicket<T>, limits: SessionLimits) -> Result<T, CallError> {
    let deadline = Instant::now() + limits.operation;
    let value = timeout_at(deadline, ticket).await;
    if Instant::now() >= deadline { return Err(CallError::Status(Status::Uncertain)); }
    match value {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(RequestError::Refused(_) | RequestError::Delivery(_)
            | RequestError::Lease(_) | RequestError::DeliveryDisabled))) =>
            Err(CallError::Status(Status::Refused)),
        // Includes a lost channel, panic, accepted I/O failure, and reply timeout.
        _ => Err(CallError::Status(Status::Uncertain)),
    }
}

pub(super) async fn dispatch<S: AsyncRead + Unpin>(stream: &mut S, header: Header,
    client: &BoundedClient, limits: SessionLimits) -> Result<Output, CallError>
{
    let (partition, group) = header.scope()?;
    match header.op {
        Op::Append => {
            let mut prepared = client.prepare_append(header.body as usize,
                Instant::now() + limits.admission).await.map_err(admission)?;
            // The mutable slice belongs to the already charged append pool.
            protocol::read_body(stream, prepared.payload_mut(), Instant::now() + limits.body).await?;
            let offset = owner(prepared.submit().map_err(admission)?, limits).await?;
            Ok(Output::Small([offset.get(), 0, 0, 0, 0]))
        }
        Op::Fetch => {
            let read = ReadLimits::new(header.args[0] as usize, header.args[1] as usize)
                .map_err(|_| CallError::Status(Status::Invalid))?;
            let prepared = client.prepare_lease(group, read,
                Instant::now() + limits.admission).await.map_err(admission)?;
            let batch = owner(prepared.submit().map_err(admission)?, limits).await?;
            Ok(Output::Batch(batch))
        }
        Op::Ack => {
            let token = broker_core::delivery::LeaseToken {
                epoch: Epoch::new(header.args[0]).map_err(|_| CallError::Status(Status::Invalid))?,
                partition, group, offset: Offset::new(header.args[1]),
                generation: header.args[2], deadline_ms: header.args[3],
            };
            let result = owner(client.try_ack_progress(group, token).map_err(admission)?, limits).await?;
            let (ack, progress) = match result {
                ProgressAck::Accepted(value) => (value.ack, value.progress),
                ProgressAck::NotApplied { ack, progress } => (ack, progress),
            };
            Ok(Output::Small([encode_ack(ack.outcome), progress.volatile_next.get(),
                progress.persisted_next.get(), ack.retired as u64, progress.epoch.get()]))
        }
        Op::Checkpoint => {
            let result = owner(client.try_checkpoint(group).map_err(admission)?, limits).await?;
            Ok(Output::Small([result.progress.volatile_next.get(), result.progress.persisted_next.get(),
                u64::from(result.published), 0, result.progress.epoch.get()]))
        }
        Op::Progress => {
            let result = owner(client.try_progress(group).map_err(admission)?, limits).await?;
            Ok(Output::Small([result.volatile_next.get(), result.persisted_next.get(), 0, 0, result.epoch.get()]))
        }
        Op::Retain => {
            let result = owner(client.try_retention().map_err(admission)?, limits).await?;
            Ok(Output::Small([result.before.get(), result.floor.get(), result.removed_bytes,
                result.removed_segments as u64, u64::from(result.rotated)]))
        }
    }
}

pub(super) async fn send_output<W: AsyncWrite + Unpin>(writer: &mut W, request: Header,
    output: Output, limits: SessionLimits) -> io::Result<()>
{
    let deadline = Instant::now() + limits.write;
    match output {
        Output::Small(args) => {
            let reply = request.response(Status::Ok, args, 0);
            protocol::validate_response(request, reply)?;
            protocol::write_bytes(writer, &reply.encode(), deadline).await
        }
        Output::Batch(batch) => {
            let count = batch.records().len();
            let (stop, required) = match batch.stop {
                LeaseStop::NoEligible => (0, 0), LeaseStop::RecordLimit => (1, 0),
                LeaseStop::ByteLimit { required } => (2, required as u64),
            };
            let size = body_size(count, batch.encoded_bytes)?;
            let reply = request.response(Status::Ok, [count as u64, batch.encoded_bytes as u64,
                stop, required, batch.state.epoch.get()], size as u32);
            protocol::validate_response(request, reply)?;
            protocol::write_bytes(writer, &reply.encode(), deadline).await?;
            for record in batch.records() {
                let token = encode_token(record.token);
                protocol::write_bytes(writer, &token, deadline).await?;
                protocol::write_bytes(writer, record.encoded, deadline).await?;
            }
            // `batch` stays alive across every await and returns its buffer on every exit.
            Ok(())
        }
    }
}
