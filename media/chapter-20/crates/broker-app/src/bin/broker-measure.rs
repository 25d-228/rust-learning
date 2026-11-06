use std::path::Path;
use broker_app::measurement::{self, Config, Mode, Profile};
use broker_app::BoxError;

fn main() -> Result<(), BoxError> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7 {
        return Err("usage: broker-measure NEW_RUN_DIR single|batched|dense closed|paced OFFERS PAYLOAD_BYTES PERIOD_US".into());
    }
    let config = Config { profile: Profile::parse(&args[2])?, mode: Mode::parse(&args[3])?,
        offers_per_producer: args[4].parse()?, payload_bytes: args[5].parse()?, period_us: args[6].parse()? };
    let summary = measurement::execute(Path::new(&args[1]), config)?;
    println!("complete: confirmed={} useful_group_records={} scanned={} sync_groups={}",
        summary.confirmed, summary.useful, summary.scanned, summary.sync_groups);
    println!("results: {}/measured (engine-only, not TCP throughput)", args[1]);
    Ok(())
}
