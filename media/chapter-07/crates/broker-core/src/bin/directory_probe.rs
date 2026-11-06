use std::error::Error;
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;

use broker_core::error::DirectoryError;
use broker_core::storage::directory::DirectoryGuard;

fn run() -> Result<ExitCode, Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 3 || (args[2] != "hold" && args[2] != "try") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput, "usage: directory-probe DIRECTORY hold|try",
        ).into());
    }
    let guard = match DirectoryGuard::acquire(Path::new(&args[1])) {
        Ok(guard) => guard,
        Err(DirectoryError::Busy(_)) => {
            println!("busy");
            return Ok(ExitCode::from(2));
        }
        Err(error) => return Err(error.into()),
    };
    println!("locked");
    io::stdout().flush()?;
    if args[2] == "hold" {
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
    }
    drop(guard);
    Ok(ExitCode::SUCCESS)
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            let mut cause = error.source();
            while let Some(source) = cause {
                eprintln!("caused by: {source}");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}
