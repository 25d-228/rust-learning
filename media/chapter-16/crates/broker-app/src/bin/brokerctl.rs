use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::Duration;
use broker_app::BoxError;
use broker_app::client::Client;
use broker_app::protocol::MAX_ENCODED;
use broker_core::delivery::AckOutcome;
use broker_core::types::{GroupId, PartitionId};

const USAGE: &str = "usage: brokerctl ADDRESS append PARTITION TEXT | consume PARTITION GROUP | progress PARTITION GROUP | checkpoint PARTITION GROUP | retain PARTITION";

async fn run(args: &[String]) -> Result<(), BoxError> {
    if args.len() < 3 { return Err(USAGE.into()); }
    let address: SocketAddr = args[0].parse()?;
    let partition = PartitionId::new(args[2].parse()?)?;
    let command = args[1].as_str();
    let expected = match command { "append" | "consume" | "progress" | "checkpoint" => 4,
        "retain" => 3, _ => return Err(USAGE.into()) };
    if args.len() != expected { return Err(USAGE.into()); }
    let group = if matches!(command, "consume" | "progress" | "checkpoint") {
        Some(GroupId::new(args[3].parse()?)?)
    } else { None };
    let mut client = Client::connect(address, Duration::from_secs(30)).await?;
    match command {
        "append" => {
            let offset = client.append(partition, args[3].as_bytes()).await?;
            println!("APPENDED partition={} offset={}", partition.get(), offset.get());
        }
        "consume" => {
            let group = group.expect("parsed group");
            let batch = client.fetch(partition, group, 8, MAX_ENCODED).await?;
            for record in batch.records() {
                // The chosen application work is a checksum plus an observed/flushed report.
                let checksum: u64 = record.payload.iter().map(|byte| u64::from(*byte)).sum();
                {
                    let stdout = io::stdout();
                    let mut output = stdout.lock();
                    writeln!(output, "PROCESSED partition={} group={} offset={} bytes={} checksum={}",
                        partition.get(), group.get(), record.token.offset.get(), record.payload.len(), checksum)?;
                    output.flush()?;
                } // No stdout lock is held over the acknowledgement await.
                let reply = client.acknowledge(record.token).await?;
                println!("ACK outcome={:?} volatile_next={} persisted_next={}", reply.outcome,
                    reply.progress.volatile_next.get(), reply.progress.persisted_next.get());
                if !matches!(reply.outcome, AckOutcome::Applied | AckOutcome::Duplicate | AckOutcome::AlreadyRetired) {
                    return Err("acknowledgement rejected after processing; duplicate processing is possible".into());
                }
            }
            println!("FETCH records={} encoded_bytes={} stop={:?}; no checkpoint requested",
                batch.records().len(), batch.encoded_bytes, batch.stop);
        }
        "progress" => {
            let p = client.progress(partition, group.expect("parsed group")).await?;
            println!("PROGRESS epoch={} volatile_next={} persisted_next={}", p.epoch.get(),
                p.volatile_next.get(), p.persisted_next.get());
        }
        "checkpoint" => {
            let p = client.checkpoint(partition, group.expect("parsed group")).await?;
            println!("CHECKPOINT published={} persisted_next={}", p.published, p.progress.persisted_next.get());
        }
        "retain" => {
            let r = client.retain(partition).await?;
            println!("RETAIN before={} floor={} removed_bytes={} removed_segments={} rotated={}",
                r.before.get(), r.floor.get(), r.removed_bytes, r.removed_segments, r.rotated);
        }
        _ => unreachable!("validated command"),
    }
    Ok(())
}
fn main() -> Result<(), BoxError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(run(&args))
}
