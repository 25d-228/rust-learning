use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use broker_core::storage::directory::DirectoryGuard;
use broker_core::storage::log::PartitionLog;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::types::{Offset, PartitionId};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: segment_probe EXISTING_EMPTY_DIRECTORY")?);
    if args.next().is_some() { return Err("expected one directory argument".into()); }
    let owner = Arc::new(DirectoryGuard::acquire(&root)?);
    let partition = PartitionId::new(0)?;
    let config = StorageConfig::new(137, 2, 8)?;
    let mut log = PartitionLog::create(Arc::clone(&owner), partition, config)?;
    for payload in [b"cat".as_slice(), b"dog", b"", b"owl", b"fox", b"hen", b"bee"] {
        log.append(payload)?;
    }
    println!("segments={} written_end={}", log.segment_infos().len(), log.written_end().get());
    let upper = log.written_end();
    let batch = log.read(Offset::new(1), upper, ReadLimits::new(3, 100)?)?;
    let offsets: Vec<_> = batch.records.iter().map(|r| r.offset().get()).collect();
    println!("offsets={offsets:?} bytes={} next={} stop={:?} scanned={}",
        batch.encoded_bytes, batch.next.get(), batch.stop, batch.scanned_records);
    drop(log);
    let mut rebuilt = PartitionLog::open_strict(owner, partition, StorageConfig::new(137, 3, 8)?)?;
    let batch = rebuilt.read(Offset::new(5), Offset::new(6), ReadLimits::new(1, 35)?)?;
    println!("rebuilt stride=3 offset={} scanned={}", batch.records[0].offset().get(), batch.scanned_records);
    println!("writes only; no synchronization or durable publish acknowledgement");
    Ok(())
}
