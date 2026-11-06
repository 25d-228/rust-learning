use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use broker_app::measurement::{self, Config, Mode, Profile};

fn root(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!("broker-measure-{label}-{}-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)))
}
fn config(profile: Profile) -> Config {
    Config { profile, mode: Mode::Closed, offers_per_producer: 32, payload_bytes: 128, period_us: 0 }
}
#[test]
fn all_profiles_cover_the_same_confirmations_and_retained_restart() {
    for profile in [Profile::Single, Profile::Batched, Profile::Dense] {
        let path = root(profile.name());
        let summary = measurement::execute(&path, config(profile))
            .unwrap_or_else(|error| panic!("inspect {}: {error}", path.display()));
        assert_eq!((summary.confirmed, summary.refused, summary.missed, summary.useful), (256, 0, 0, 512));
        assert_eq!(summary.retained, [64; 4]);
        assert_eq!(summary.scanned, if profile == Profile::Dense { 2304 } else { 16640 });
        if profile == Profile::Single { assert_eq!(summary.sync_groups, 256); }
        else { assert!((128..=256).contains(&summary.sync_groups)); }
        assert!(fs::read_to_string(path.join("measured/summary.tsv")).unwrap().contains("valid\ttrue\n"));
        // No assertion compares elapsed time: noisy CI is not a performance oracle.
        fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn paced_offers_are_either_started_or_explicitly_missed() {
    let path = root("paced");
    let config = Config { mode: Mode::Paced, offers_per_producer: 8, period_us: 1000,
        ..config(Profile::Batched) };
    let summary = measurement::execute(&path, config).unwrap();
    assert_eq!(summary.confirmed + summary.refused + summary.missed, 64);
    assert_eq!(summary.useful, 2 * summary.confirmed);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn existing_run_preserves_unrelated_files() {
    let path = root("existing"); fs::create_dir(&path).unwrap();
    fs::write(path.join("keep"), b"unrelated").unwrap();
    assert!(measurement::execute(&path, config(Profile::Batched)).is_err());
    assert_eq!(fs::read(path.join("keep")).unwrap(), b"unrelated");
    assert!(!path.join("warmup").exists()); fs::remove_dir_all(path).unwrap();
}
#[test]
fn invalid_input_fails_before_creating_a_directory() {
    let path = root("invalid");
    assert!(measurement::execute(&path, Config { offers_per_producer: 0, ..config(Profile::Batched) }).is_err());
    assert!(!path.exists());
    let output = Command::new(env!("CARGO_BIN_EXE_broker-measure"))
        .arg(&path).args(["unknown", "closed", "32", "128", "0"]).output().unwrap();
    assert!(!output.status.success()); assert!(!path.exists());
}
