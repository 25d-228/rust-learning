use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::mem::size_of;
use std::path::Path;
use crate::BoxError;
use crate::metrics::Histogram;
use super::{Config, PRODUCERS, SEGMENT_BYTES, require};
use super::experiment::Details;
use super::load::Appended;
use super::observed::NAMES;

pub(super) fn write_attempts(root: &Path, config: Config, data: &Appended) -> io::Result<()> {
    let mut out = BufWriter::new(File::create(root.join("attempts.tsv"))?);
    for (key, value) in [
        ("offered", (PRODUCERS * config.offers_per_producer) as u64),
        ("started", data.stats.started), ("confirmed", data.stats.confirmed),
        ("refused", data.stats.refused), ("ambiguous", data.stats.ambiguous),
        ("timed_out", data.stats.timed_out), ("missed", data.stats.missed),
    ] { writeln!(out, "{key}\t{value}")?; }
    for (name, count) in ["admission_deadline", "disk", "capacity", "closed", "invalid", "owner_refused"]
        .iter().zip(data.stats.reasons) { writeln!(out, "refused_{name}\t{count}")?; }
    out.flush()
}
fn histogram(name: &str, value: &Histogram, out: &mut impl Write, bins: &mut impl Write)
    -> Result<(), BoxError>
{
    require(value.valid, "invalid histogram: clock or counter overflow; do not compare")?;
    value.write(name, out, bins)?; Ok(())
}
pub(super) fn write(root: &Path, config: Config, data: &Details) -> Result<(), BoxError> {
    let mut distributions = BufWriter::new(File::create(root.join("histograms.tsv"))?);
    let mut bins = BufWriter::new(File::create(root.join("bins.tsv"))?);
    writeln!(distributions, "metric\tvalid\tcount\ttotal_us\tmax_us\tp50_low\tp50_high\tp95_low\tp95_high\tp99_low\tp99_high")?;
    writeln!(bins, "metric\tlow_us\thigh_us\tcount")?;
    let a = &data.appended.stats; let c = &data.consumed.stats;
    for (name, h) in [("append_admission_all", &a.admission), ("append_success", &a.successful),
        ("append_unsuccessful", &a.unsuccessful), ("append_submit_to_reply", &a.submit_to_reply),
        ("append_scheduled_to_finish", &a.scheduled_to_finish), ("fetch_ack_cycle", &c.cycles),
        ("acknowledgement", &c.acknowledgements)] {
        histogram(name, h, &mut distributions, &mut bins)?;
    }
    let mut failures = BufWriter::new(File::create(root.join("owner-failures.tsv"))?);
    writeln!(failures, "partition\tmetric\tfailed_calls")?;
    for (partition, owner) in data.owners.iter().enumerate() {
        require(owner.valid, "owner observer was poisoned or overflowed")?;
        for (index, name) in NAMES.iter().enumerate() {
            histogram(&format!("p{partition}_{name}"), &owner.hist[index], &mut distributions, &mut bins)?;
            writeln!(failures, "{partition}\t{name}\t{}", owner.failures[index])?;
        }
    }
    distributions.flush()?; bins.flush()?; failures.flush()?;
    let mut memory = BufWriter::new(File::create(root.join("memory.tsv"))?);
    writeln!(memory, "partition\tpool\tallocated_capacity\tpeak_checked_out_capacity\tfinal_checked_out_capacity")?;
    let mut allocated = 0usize;
    let mut sum_peaks = 0usize;
    for (partition, state) in data.memory.iter().enumerate() {
        for (name, pool) in [("append", state.append), ("response", state.response)] {
            require(pool.peak_checked_out_bytes <= pool.allocated_bytes, "peak exceeds pool allocations")?;
            writeln!(memory, "{partition}\t{name}\t{}\t{}\t{}", pool.allocated_bytes,
                pool.peak_checked_out_bytes, pool.checked_out_bytes)?;
            allocated += pool.allocated_bytes; sum_peaks += pool.peak_checked_out_bytes;
        }
        allocated += state.scratch_bytes;
        writeln!(memory, "{partition}\tscratch\t{}\tNA\tNA", state.scratch_bytes)?;
    }
    memory.flush()?;
    let mut disk = BufWriter::new(File::create(root.join("disk.tsv"))?);
    writeln!(disk, "partition\tbefore_file_lengths\tafter_file_lengths\tnormal_quota\tmaintenance_allowance")?;
    for partition in 0..4 {
        let (before, after) = (data.disk_before[partition], data.disk_after[partition]);
        require(!before.stopped && !after.stopped && before.reserved_bytes == 0 && after.reserved_bytes == 0,
            "disk accounting is not quiescent")?;
        writeln!(disk, "{partition}\t{}\t{}\t{}\t{}", before.file_bytes, after.file_bytes,
            before.quota.data_bytes(), before.quota.reserve_bytes())?;
    }
    disk.flush()?;
    // Write the successful summary last. Failure before here cannot masquerade as a complete sample.
    let mut summary = BufWriter::new(File::create(root.join("summary.tsv"))?);
    macro_rules! line { ($key:expr, $value:expr) => { writeln!(summary, "{}\t{}", $key, $value)? }; }
    line!("scope", "in_process_bounded_engine_two_phase");
    line!("profile", config.profile.name()); line!("mode", config.mode.name());
    line!("producers", PRODUCERS); line!("offers_per_producer", config.offers_per_producer);
    line!("payload_bytes", config.payload_bytes); line!("period_us", config.period_us);
    line!("batch_records", config.profile.batch_records()); line!("batch_delay_us", 2000);
    line!("index_stride", config.profile.stride()); line!("segment_bytes", SEGMENT_BYTES);
    line!("warmup_offers_per_producer", 16); line!("instrumented_owner", true);
    line!("offered", PRODUCERS * config.offers_per_producer); line!("started", a.started);
    line!("confirmed", a.confirmed); line!("refused", a.refused); line!("missed", a.missed);
    line!("timed_out", a.timed_out); line!("ambiguous", a.ambiguous);
    line!("append_seconds", data.summary.append_seconds);
    line!("confirmed_per_second", a.confirmed as f64 / data.summary.append_seconds);
    line!("consumer_seconds", data.summary.consumer_seconds);
    line!("useful_group_records", c.useful); line!("duplicate_deliveries", c.duplicates);
    line!("useful_group_records_per_second", c.useful as f64 / data.summary.consumer_seconds);
    line!("scanned_records", c.scanned); line!("fetched_records", c.fetched);
    line!("sync_groups", data.summary.sync_groups);
    line!("recovery_seconds", data.recovery.as_secs_f64());
    line!("index_entries_derived", data.summary.index_entries);
    line!("index_entry_size", size_of::<broker_core::storage::index::IndexEntry>());
    line!("pool_plus_scratch_capacity", allocated);
    line!("sum_pool_peaks_not_simultaneous", sum_peaks);
    line!("histogram_size", size_of::<Histogram>());
    line!("retained_p0", data.summary.retained[0]); line!("retained_p1", data.summary.retained[1]);
    line!("retained_p2", data.summary.retained[2]); line!("retained_p3", data.summary.retained[3]);
    line!("valid", true);
    summary.flush()?;
    Ok(())
}
