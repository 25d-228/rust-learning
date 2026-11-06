use std::error::Error;
use std::io::{self, Write};
use std::path::Path;
use std::thread;
use std::time::Duration;

use broker_core::io::StorageFile;
use broker_core::storage::recovery;
use broker_core::storage::segment::{ReadLimits, StorageConfig};
use broker_core::storage::sync::SyncIo;
use broker_core::types::{Epoch, Offset};
use broker_core::worker::{BatchConfig, WorkerHandle};

struct BeforeSync { epoch: Epoch }

impl SyncIo for BeforeSync {
    fn sync(&mut self, base: Offset, _file: &mut dyn StorageFile) -> io::Result<()> {
        // This adapter never reports a successful sync. The process harness kills us here.
        writeln!(io::stdout(), "CUT epoch={} base={} before-data-sync", self.epoch.get(), base.get())?;
        io::stdout().flush()?;
        loop { thread::park(); }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 3 {
        return Err(io::Error::other("usage: recovery_probe init|hold|inspect DIRECTORY").into());
    }
    let mode = args[1].to_str().ok_or_else(|| io::Error::other("mode must be text"))?;
    let path = Path::new(&args[2]);
    match mode {
        "init" => {
            let session = recovery::initialize(path, StorageConfig::new(102, 2, 8)?)?;
            println!("initialized epoch={}", session.epoch().get());
        }
        "hold" => {
            let (epoch, mut logs) = recovery::recover(path)?.into_parts();
            let log = logs.remove(0);
            let owner = WorkerHandle::start(log, BeforeSync { epoch },
                BatchConfig::new(1, 1000, Duration::from_secs(1))?, 4, 4)?;
            let ticket = owner.client().try_publish(b"cat".to_vec())
                .map_err(|_| io::Error::other("unexpected fixture admission failure"))?;
            let result = ticket.recv()?;
            result?;
            owner.join()?;
            return Err(io::Error::other("blocking fault gate unexpectedly returned").into());
            // `logs` keeps the other partitions' guard shares alive until process death.
        }
        "inspect" => {
            let (epoch, mut logs) = recovery::recover(path)?.into_parts();
            println!("epoch={}", epoch.get());
            for log in &mut logs {
                let end = log.written_end();
                let batch = log.read(Offset::new(0), end, ReadLimits::new(8, 1000)?)?;
                let first = batch.records.first().map(|record| {
                    record.payload().iter().map(|b| format!("{b:02x}")).collect::<String>()
                }).unwrap_or_else(|| "-".to_owned());
                println!("partition={} end={} first={first}", log.partition().get(), end.get());
            }
        }
        _ => return Err(io::Error::other("unknown mode; use init, hold, or inspect").into()),
    }
    Ok(())
}
