use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use broker_app::workload;

static NEXT: AtomicU64 = AtomicU64::new(0);
fn directory(label: &str) -> PathBuf {
    let time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("broker-e2e-{label}-{}-{time}-{}",
        std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)))
}
fn executable() -> &'static Path { Path::new(env!("CARGO_BIN_EXE_broker-load")) }

#[test]
fn broker_crash_coverage_overload_and_retained_restart() {
    let root = directory("coverage");
    let report = workload::execute(executable(), &root, 2)
        .unwrap_or_else(|error| panic!("scenario failed; inspect {}: {error}", root.display()));
    assert_eq!((report.broker_kills, report.consumer_kills), (1, 1));
    assert!(report.obsolete_token_rejected);
    assert_eq!(report.ambiguous, 1);
    assert_eq!(report.rejected, 1);
    assert!(report.observed_ambiguous <= 1);
    assert_eq!(report.unique_records, 54);
    assert_eq!(report.confirmed + report.observed_ambiguous, report.unique_records);
    assert_eq!(report.duplicate_logical_inputs, report.observed_ambiguous);
    assert_eq!(report.repeated_deliveries, 17);
    assert_eq!(report.floor, [18, 12, 12, 12]);
    let text = fs::read_to_string(root.join("ledger.tsv")).unwrap();
    assert!(text.contains("RECOVERY-COVERAGE\t"));
    assert!(text.contains("PRESSURE\tack/checkpoint/control progressed"));
    assert_eq!(text.lines().filter(|line| line.starts_with("PASS\t")).count(), 1);
    assert!(!root.join("broker/ledger.tsv").exists());
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn existing_run_is_not_removed_or_reused() {
    let root = directory("existing");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("keep.txt"), b"unrelated content\n").unwrap();
    assert!(workload::execute(executable(), &root, 1).is_err());
    assert_eq!(fs::read(root.join("keep.txt")).unwrap(), b"unrelated content\n");
    assert!(!root.join("broker").exists());
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn invalid_cli_rounds_fail_before_creating_a_run() {
    let root = directory("invalid");
    let output = Command::new(executable()).arg("run").arg(&root).arg("0").output().unwrap();
    assert!(!output.status.success());
    assert!(!root.exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("rounds"));
}
