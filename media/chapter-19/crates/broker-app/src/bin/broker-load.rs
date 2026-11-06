use std::path::Path;
use broker_app::{BoxError, workload};

fn main() -> Result<(), BoxError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("run") if (2..=3).contains(&args.len()) => {
            let rounds = args.get(2).map(|s| s.parse()).transpose()?.unwrap_or(4);
            let executable = std::env::current_exe()?;
            let report = workload::execute(&executable, Path::new(&args[1]), rounds)?;
            println!("PASS {report:#?}");
        }
        Some("broker-child") if args.len() == 3 => {
            workload::broker_child(Path::new(&args[1]), args[2].parse()?)?;
        }
        Some("consumer-child") if args.len() == 3 => {
            workload::consumer_child(Path::new(&args[1]), args[2].parse()?)?;
        }
        _ => return Err("usage: broker-load run NEW_RUN_DIRECTORY [ROUNDS=4]".into()),
    }
    Ok(())
}
