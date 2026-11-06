# Chapter 19 — End-to-end coverage and overload demonstration

## Part 4 — Executable scenarios and checker counterexamples

> **Learning objectives**
> Run the same scenario from a binary and an integration test; read outcome counts without confusing ambiguity with loss; test that the external checker rejects bad histories; and retain useful artifacts from a failed run.

This completes the same **5.0-hour chapter**. Assemble [Part 1](https://25d-228.github.io/rust-learning/chapter-19.html), [Part 2](https://25d-228.github.io/rust-learning/chapter-19-part-02.html), and [Part 3](https://25d-228.github.io/rust-learning/chapter-19-part-03.html) first. These checks use a real local filesystem, loopback sockets, and child processes; a restricted execution environment may refuse them. **Outputs and test outcomes below are expected local checks, not a claim of execution on your Mac.**

## One executable, three explicitly selected roles

Create `src/bin/broker-load.rs`. Cargo discovers the binary from this filename, so no `[[bin]]` manifest entry is needed. The normal command is `run`; the two child modes are invoked by the parent using its own executable path. They share the existing broker library rather than starting `cargo` recursively.

The optional round count is parsed with `Option<Result<...>>::transpose()`: missing input remains `None`, a valid supplied number becomes `Some(number)`, and a parsing error propagates. It then defaults to four and passes through `validate_rounds` before creating the run directory.

```rust title="crates/broker-app/src/bin/broker-load.rs"
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
```

The binary prints `PASS` only when `execute` returns a completed `Report`. A child is a fresh invocation of the same executable with a distinct role, not a forked copy of the parent's async runtime or ledger. Its broker data path is always the `broker/` child of the owned run directory.

## Test the oracle before trusting its green result

Create `workload/tests.rs`. These unit tests do not start a broker. They deliberately feed the external ledger invalid histories so that an accidentally weakened checker fails its own tests. The temporary fixture preserves its directory while unwinding for diagnosis and removes only its own directory on ordinary completion.

The first block contains every import and helper. `add` registers an intent and its explicit outcome; `observe` constructs only the scan map for a delivery that the ledger itself accepted. Tokens use the fixed partition 0 and a named group/epoch. No private production ring or checkpoint code calculates the expected ledger state.

```rust title="crates/broker-app/src/workload/tests.rs"
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use broker_core::delivery::LeaseToken;
use broker_core::types::{Epoch, Offset};
use crate::client::ClientError;
use crate::protocol::Status;
use super::children::{decode_receipt, encode_receipt};
use super::ledger::{Key, Ledger, Outcome, Scan, classify};
use super::process::Addresses;
use super::{g, p, validate_rounds};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("broker-ledger-{}-{time}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for Directory {
    fn drop(&mut self) { if !std::thread::panicking() { let _ = fs::remove_dir_all(&self.0); } }
}
fn fixture() -> (Directory, Ledger) {
    let directory = Directory::new();
    let mut ledger = Ledger::create(&directory.0.join("ledger.tsv")).unwrap();
    ledger.begin_epoch(1).unwrap();
    (directory, ledger)
}
fn key(sequence: u64) -> Key { Key { producer: 0, sequence, attempt: 0 } }
fn token(offset: u64, group: u8, epoch: u64) -> LeaseToken {
    LeaseToken { epoch: Epoch::new(epoch).unwrap(), partition: p(0), group: g(group),
        offset: Offset::new(offset), generation: 1, deadline_ms: 30_000 }
}
fn add(ledger: &mut Ledger, key: Key, outcome: Outcome) {
    ledger.intent(key).unwrap(); ledger.resolve(key, outcome).unwrap();
}
fn observe(ledger: &mut Ledger, group: u8, epoch: u64, offset: u64, key: Key) -> Scan {
    ledger.observe(token(offset, group, epoch), &key.payload()).unwrap();
    Scan::from([((0, offset), key)])
}

```

The first three tests check a literal payload fixture, all short payload lengths plus changed magic/trailing data, and the outcome classifier. The classifier test includes reset, EOF, and timeout errors; none may silently become definite rejection.

```rust title="crates/broker-app/src/workload/tests.rs"
#[test]
fn payload_has_independent_golden_bytes() {
    let value = Key { producer: 5, sequence: 0x0102030405060708, attempt: 2 };
    let expected = [76, 79, 65, 68, 48, 48, 48, 49, 0, 0, 0, 5, 0, 0, 0, 2, 1, 2, 3, 4, 5, 6, 7, 8];
    assert_eq!(value.payload(), expected);
    assert_eq!(Key::decode(&expected).unwrap(), value);
    assert_eq!(value.partition(), 1);
    assert_ne!(value.payload(), Key { attempt: 3, ..value }.payload());
}
#[test]
fn payload_rejects_short_long_and_wrong_magic() {
    let bytes = key(0).payload();
    for cut in 0..bytes.len() { assert!(Key::decode(&bytes[..cut]).is_err()); }
    let mut bad = bytes; bad[0] ^= 1;
    assert!(Key::decode(&bad).is_err());
    let mut long = bytes.to_vec(); long.push(0);
    assert!(Key::decode(&long).is_err());
}
#[test]
fn classification_never_calls_transport_uncertainty_rejection() {
    assert_eq!(classify(&Ok(Offset::new(7))), Outcome::Confirmed(7));
    for status in [Status::Invalid, Status::Busy, Status::DiskFull, Status::Unavailable,
        Status::AdmissionDeadline, Status::Refused] {
        assert_eq!(classify(&Err(ClientError::Remote(status))), Outcome::Rejected);
    }
    assert_eq!(classify(&Err(ClientError::Remote(Status::Uncertain))), Outcome::Ambiguous);
    for kind in [io::ErrorKind::TimedOut, io::ErrorKind::ConnectionReset, io::ErrorKind::UnexpectedEof] {
        assert_eq!(classify(&Err(ClientError::Io(io::Error::new(kind, "fixture")))), Outcome::Ambiguous);
    }
}
```

The next group checks identity and the expected set. Reusing an attempt is different from explicitly incrementing its attempt field. A delivered input cannot register itself as expected, and rejected inputs cannot reappear as accepted records. Confirmation binds the offset **and** partition.

```rust title="crates/broker-app/src/workload/tests.rs"
#[test]
fn intent_and_resolution_cannot_be_reused() {
    let (_directory, mut ledger) = fixture();
    ledger.intent(key(0)).unwrap();
    assert!(ledger.intent(key(0)).is_err());
    assert!(ledger.resolve(key(0), Outcome::Pending).is_err());
    ledger.resolve(key(0), Outcome::Ambiguous).unwrap();
    assert!(ledger.resolve(key(0), Outcome::Confirmed(0)).is_err());
}
#[test]
fn two_inputs_cannot_claim_one_position() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    let other = Key { producer: 4, sequence: 0, attempt: 0 };
    ledger.intent(other).unwrap();
    assert!(ledger.resolve(other, Outcome::Confirmed(0)).is_err());
}
#[test]
fn unknown_input_cannot_become_an_expectation() {
    let (_directory, mut ledger) = fixture();
    assert!(ledger.observe(token(0, 0, 1), &key(0).payload()).is_err());
}
#[test]
fn rejected_input_cannot_be_observed() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Rejected);
    assert!(ledger.observe(token(0, 0, 1), &key(0).payload()).is_err());
}
#[test]
fn confirmation_binds_both_offset_and_partition() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    assert!(ledger.observe(token(1, 0, 1), &key(0).payload()).is_err());
    let wrong = LeaseToken { partition: p(1), ..token(0, 0, 1) };
    assert!(ledger.observe(wrong, &key(0).payload()).is_err());
}
```

The ambiguity test permits either absence or one correctly bound record without changing the original append classification. The next test forbids the same non-retried attempt at two offsets. The recovery test supplies old evidence after an epoch change: it must fail until the same bytes are genuinely observed under the new epoch.

```rust title="crates/broker-app/src/workload/tests.rs"
#[test]
fn ambiguity_allows_absence_or_one_matching_record() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Ambiguous);
    ledger.coverage(0, [0; 4], &Scan::new()).unwrap();
    let scan = observe(&mut ledger, 0, 1, 0, key(0));
    ledger.coverage(0, [1, 0, 0, 0], &scan).unwrap();
    let report = ledger.report(1);
    assert_eq!((report.ambiguous, report.observed_ambiguous, report.confirmed), (1, 1, 0));
}
#[test]
fn one_attempt_cannot_appear_at_two_offsets() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Ambiguous);
    observe(&mut ledger, 0, 1, 0, key(0));
    assert!(ledger.observe(token(1, 0, 1), &key(0).payload()).is_err());
}
#[test]
fn old_delivery_does_not_satisfy_recovery_coverage() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    let old = observe(&mut ledger, 0, 1, 0, key(0));
    ledger.coverage(0, [1, 0, 0, 0], &old).unwrap();
    ledger.begin_epoch(2).unwrap();
    assert!(ledger.coverage(0, [1, 0, 0, 0], &old).is_err());
    assert!(ledger.observe(token(0, 0, 1), &key(0).payload()).is_err());
    let fresh = observe(&mut ledger, 0, 2, 0, key(0));
    ledger.coverage(0, [1, 0, 0, 0], &fresh).unwrap();
}
```

An internally empty scan is not enough when a confirmed record should exist. Nor can group 1 borrow group 0's observations. Gap/extra-offset cases stop count-only checkers. A checkpoint without checked coverage must be refused by the external ledger even if a hypothetical caller asserts that the number was persisted.

```rust title="crates/broker-app/src/workload/tests.rs"
#[test]
fn missing_confirmed_record_is_not_hidden_by_an_empty_end() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    assert!(ledger.coverage(0, [0; 4], &Scan::new()).is_err());
}
#[test]
fn groups_cannot_share_delivery_evidence() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    let scan = observe(&mut ledger, 0, 1, 0, key(0));
    assert!(ledger.coverage(1, [1, 0, 0, 0], &scan).is_err());
}
#[test]
fn interval_gaps_and_extra_offsets_are_rejected() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    add(&mut ledger, key(1), Outcome::Confirmed(2));
    let mut scan = observe(&mut ledger, 0, 1, 0, key(0));
    scan.extend(observe(&mut ledger, 0, 1, 2, key(1)));
    assert!(ledger.coverage(0, [3, 0, 0, 0], &scan).is_err());
    assert!(ledger.coverage(0, [1, 0, 0, 0], &scan).is_err());
}
#[test]
fn checkpoint_cannot_outrun_checked_coverage() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    assert!(ledger.checkpoint(0, 0, 1).is_err());
}
```

The deletion test demonstrates the legal boundary: one checkpoint is insufficient; two checked and checkpointed frontiers permit floor advancement; a later empty retained interval is then valid. The duplicate test constructs two stored attempts for one logical input and repeats one delivery, establishing that the two counters mean different things. An unresolved producer intent blocks success.

```rust title="crates/broker-app/src/workload/tests.rs"
#[test]
fn deletion_needs_both_checkpointed_coverage_frontiers() {
    let (_directory, mut ledger) = fixture();
    add(&mut ledger, key(0), Outcome::Confirmed(0));
    for group in 0..2 {
        let scan = observe(&mut ledger, group, 1, 0, key(0));
        ledger.coverage(group, [1, 0, 0, 0], &scan).unwrap();
    }
    ledger.checkpoint(0, 0, 1).unwrap();
    assert!(ledger.retained(0, 1).is_err());
    ledger.checkpoint(1, 0, 1).unwrap();
    ledger.retained(0, 1).unwrap();
    ledger.begin_epoch(2).unwrap();
    for group in 0..2 { ledger.coverage(group, [1, 0, 0, 0], &Scan::new()).unwrap(); }
}
#[test]
fn delivery_repetition_is_distinct_from_duplicate_logical_input() {
    let (_directory, mut ledger) = fixture();
    let retried = Key { attempt: 1, ..key(0) };
    add(&mut ledger, key(0), Outcome::Ambiguous);
    add(&mut ledger, retried, Outcome::Confirmed(1));
    observe(&mut ledger, 0, 1, 0, key(0));
    observe(&mut ledger, 0, 1, 0, key(0));
    observe(&mut ledger, 0, 1, 1, retried);
    let report = ledger.report(1);
    assert_eq!(report.unique_records, 2);
    assert_eq!(report.repeated_deliveries, 1);
    assert_eq!(report.duplicate_logical_inputs, 1);
}
#[test]
fn unresolved_append_prevents_coverage_success() {
    let (_directory, mut ledger) = fixture();
    ledger.intent(key(0)).unwrap();
    assert!(ledger.coverage(0, [0; 4], &Scan::new()).is_err());
}
```

Finish the unit tests with the receipt codec, readiness constraints, round bounds, and epoch monotonicity. The receipt test tries every shorter prefix of its 64-byte input. The non-loopback example is just a parsed string and is never contacted.

```rust title="crates/broker-app/src/workload/tests.rs"
#[test]
fn victim_receipt_round_trips_and_rejects_wrong_scope() {
    let original = token(3, 1, 2);
    let bytes = encode_receipt(original, &key(0).payload()).unwrap();
    assert_eq!(&bytes[..8], b"VICTIM01");
    let (read, payload) = decode_receipt(&bytes).unwrap();
    assert_eq!(read, original);
    assert_eq!(payload, key(0).payload());
    for cut in 0..64 { assert!(decode_receipt(&bytes[..cut]).is_err()); }
    assert!(encode_receipt(token(0, 0, 1), &key(0).payload()).is_err());
}
#[test]
fn rendezvous_requires_loopback_bound_ports_and_nonzero_epoch() {
    let value = Addresses::parse(b"127.0.0.1:1234 127.0.0.1:1235 127.0.0.1:1236 2\n").unwrap();
    assert_eq!(value.epoch, 2);
    for bad in ["127.0.0.1:0 127.0.0.1:2 127.0.0.1:3 1",
        "192.0.2.1:1 127.0.0.1:2 127.0.0.1:3 1",
        "127.0.0.1:1 127.0.0.1:2 127.0.0.1:3 0", "missing fields"] {
        assert!(Addresses::parse(bad.as_bytes()).is_err());
    }
}
#[test]
fn bounded_rounds_and_monotonic_epochs_are_checked() {
    assert!(validate_rounds(0).is_err());
    assert!(validate_rounds(33).is_err());
    assert!(validate_rounds(1).is_ok());
    assert!(validate_rounds(32).is_ok());
    let (_directory, mut ledger) = fixture();
    assert!(ledger.begin_epoch(1).is_err());
    assert!(ledger.begin_epoch(0).is_err());
}
```

There are **21 supplied workload unit tests**. They test the checker and its coordination formats; they do not prove that the real broker obeys those expectations. That is the role of the following process-level integration target, together with earlier focused storage/concurrency tests.

## Exercise the real binary from Cargo

Create `tests/end_to_end.rs`. Cargo supplies the compiled binary's absolute path through `env!("CARGO_BIN_EXE_broker-load")` when building this integration test. That is a compile-time macro expansion, not a hard-coded `target/debug` assumption and not an environment variable that the learner must invent. See [Cargo's executable-path environment variables](https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-crates).

The real scenario uses two steady rounds to keep the automated run small. Its root is retained on a scenario failure. On success, the test checks the summary and transcript, then deletes only its newly created root. The other two tests protect against accidentally replacing an existing run and against accepting an unbounded/invalid round count.

```rust title="crates/broker-app/tests/end_to_end.rs"
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
```

The integration target has **three supplied tests**. The main test kills one consumer and one broker, uses two further broker generations, verifies stale-token rejection, reaches explicit disk refusal, reclaims after coverage, and checks the final retained restart. The other two fail before starting children. Together with the 21 unit tests, this chapter adds **24 tests**, for **456 cumulative supplied tests**. Test counts describe the supplied source, not observed passes.

## Build and run on the native workspace

Run these commands from the learner workspace reconstructed through this chapter. The existing workspace manifest and compatible Rust toolchain remain sufficient; there is no new dependency or platform requirement beyond the earlier native file-lock API. Keep the source out of the book's `media` directory when building.

```sh
rustc --version
cargo --version
cargo fmt --all
cargo test -p broker-app --lib workload::tests
cargo test -p broker-app --test end_to_end -- --nocapture
cargo test --workspace --all-targets
cargo build -p broker-app --bin broker-load
```

Expected focused results are **21** workload unit tests and **3** end-to-end tests passing. The cumulative command also builds/runs the earlier targets; do not skip an earlier failure merely because the new checker passes. `cargo fmt` is an actual local formatting operation, not a claim that the printed reference sources have already passed rustfmt.

For the manual scenario, use a new run directory:

```sh
mkdir -p runs
cargo run -p broker-app --bin broker-load -- run runs/coverage-small 4
cat runs/coverage-small/report.txt
sed -n '1,30p' runs/coverage-small/ledger.tsv
grep -E '^(KILL|FENCE|RECOVERY-COVERAGE|PRESSURE|RETAIN|ROUND|RESTART|PASS)' runs/coverage-small/ledger.tsv
```

Do not remove an existing run merely to make the command succeed. Choose another name to preserve its evidence. `broker-load` refuses an existing path, even if it is empty. Children bind their actual listeners to ephemeral loopback ports; no prior manual `brokerd` session is required.

The resulting directory has this shape:

```text
coverage-small/
  ledger.tsv                  parent-owned intent/outcome/delivery/checkpoint transcript
  report.txt                  written only after the scenario finishes
  broker/                     only broker record and authoritative metadata files
  broker-1.log                 initialized child, later killed at the sync gate
  broker-2.log                 recovered child, later shut down and joined
  broker-3.log                 retained-state restart, later shut down and joined
  broker-1.ready              actual addresses and epoch for generation 1
  broker-2.ready
  broker-3.ready
  consumer.log                victim process output
  victim.ready                complete delivery receipt, not an acknowledgement
  gate.arm                    deliberate interruption enabled
  gate.entered                owner reached pre-synchronization boundary
```

A failed run may also have `.writing` files and no `report.txt`. A `gate.release` file is absent in the normal kill scenario. The parent does not promote incomplete rendezvous files or use this transcript as broker recovery metadata.

## Calculate the expected summary, including legitimate variation

Let $r$ be the requested number of steady rounds, with $1\le r\le32$. Let $a$ be 1 when the ambiguous original appears after the crash and 0 when it does not. The experiment does not claim control over unsynchronized survival; its checker permits either legal outcome.

The pressure phase ends with nine records in partition 0 and four in each other partition, or $9+3\cdot4=21$ records. Retrying the rejected input after reclamation adds one. Every steady round adds eight producers times two inputs, or 16 records. Thus the number $N$ of unique stored positions observed over the entire run is:

$$
N=21+1+16r=22+16r.
$$

There is exactly one ambiguous append and one explicit rejected attempt. Of the observed positions, $a$ came from the unconfirmed original, so confirmed appends total $N-a$. The original and its explicit retry share a logical input only when both exist, giving $a$ duplicate logical inputs. The rejected attempt itself never appears.

Before the crash, group 0 observed all 16 initial records and the victim observed one record in group 1. Both persisted frontiers were still zero. After recovery those observations repeat, so repeated delivery contributes exactly $16+1=17$ in the controlled scenario. This is counted per group and position, not per producer input.

For four steady rounds, expected report properties are:

| Field | Expected value | Interpretation |
| --- | ---: | --- |
| `unique_records` | 86 | Positions validated before their legitimate deletion |
| `confirmed` | 85 or 86 | Depends on whether the ambiguous original was observed |
| `ambiguous` / `rejected` | 1 / 1 | Original producer-side classifications remain intact |
| `observed_ambiguous` / `duplicate_logical_inputs` | both 0 or both 1 | Recovery observation, not a retroactive producer confirmation |
| `repeated_deliveries` | 17 | Earlier uncheckpointed group observations repeated |
| `floor` | `[26, 20, 20, 20]` | Exclusive offsets; no offset space was reset |
| `broker_kills` / `consumer_kills` | 1 / 1 | Actual forced child terminations in this scenario |
| `obsolete_token_rejected` | `true` | Rejection was specifically `WrongEpoch` |

The floor calculation is partition-specific. Partition 0 ends at $9+1+4r=10+4r$; each other partition ends at $4+4r$. With $r=2$, the integration test expects `[18, 12, 12, 12]` and 54 unique records. Actual interleaving of producer IDs at individual offsets can differ, and the ledger checks the returned bindings rather than hard-coding that interleaving.

These are **calculated expected invariants**, not measured throughput, latency, RSS, or observed results from this authoring environment. A successful process-kill check is also not a power-failure test.

## Diagnose the first failed boundary

| Symptom | Inspect first | Likely distinction to resolve |
| --- | --- | --- |
| Child exits before a ready record | That generation's `.log`; initialization/recovery error | Startup refusal versus slow listener setup |
| `gate.entered` never appears | Generation-1 log, admission result, payload size, selected partition | Owner never reached the sync boundary versus a guessed timing problem |
| Confirmed input is missing | Earlier `APPEND Confirmed`, fresh `DELIVER`, retained floor | True missing/misbound data versus a later legal deletion |
| `WrongEpoch` was not returned | Receipt epoch and generation-2 ready epoch | Stale scope fence versus lease expiry or an incorrectly reinitialized broker |
| Normal drain acknowledgement is rejected | Token deadline and `ACK` ordering | Slow fixture/paused debugger versus missing head completion |
| Refusal probe gets EOF or a timeout | File bytes, conservative reservation, producer header handling | No decisive refusal was received; do not count it as rejected |
| Fast group checkpoint works but deletion does not | Both groups' persisted next values | Expected slow-group pinning versus using volatile progress |
| Reclaim reports unexpected bytes | Before/after lengths and `rotated` | Missing replacement-header accounting versus unsafe deletion |
| Final restart returns records or starts below the floor | Retention authority, group checkpoint files, recovery validation | Resurrected obsolete prefix versus correct empty retained interval |

The transcript records phases in parent observation order. It is not a total ordering of all concurrent events inside four owners. Producer replies can be collected out of order, and networking introduces additional delay. Use the typed offsets and explicit gates rather than interpreting adjacent lines as a global execution order.

## Make a defective checker fail visibly

Temporarily remove **only** `self.current_deliveries.clear();` from `Ledger::begin_epoch` in `crates/broker-app/src/workload/ledger.rs`. Run:

```sh
cargo test -p broker-app --lib workload::tests::old_delivery_does_not_satisfy_recovery_coverage -- --exact
```

Expected failure: the assertion requiring the old scan to be rejected no longer holds, because stale observations satisfy the new epoch's coverage. Restore the exact line immediately after `self.verified = [self.floor; 2];` and rerun. The complete correct function is printed in Part 1 and included in the overlay. Do not leave this deliberate defect in the cumulative workspace.

A second prediction requires no mutation: running 32 rounds should keep each broker partition quiescently reduced to one 32-byte active segment after each reclaim, while the external ledger grows with its bounded history. The final unique-record count is $22+16\cdot32=534$ and the floor is `[138, 132, 132, 132]`. Run under a new directory name:

```sh
cargo run -p broker-app --bin broker-load -- run runs/coverage-32-rounds 32
```

If the broker's record files grow monotonically across these successful reclaim boundaries, investigate retention. If only `ledger.tsv` grows, that is the checker's intentionally retained evidence, not leaked broker disk admission. These commands exercise sustained finite correctness; their elapsed time is not a defensible performance comparison because they include synchronization of the transcript, process startup, control polling, and deliberate interruption.

## What the integrated demonstration now establishes

The same driver crosses real framing, transport ownership, admission, storage, lease windows, checkpoint publication, retention, and supervised exit. Its external expectation survives a broker-only kill. It checks confirmed bytes before any permitted deletion, keeps uncertainty explicit, demonstrates consumer loss and stale fencing, forces disk refusal, proves control progress under that pressure, and observes useful traffic again after safe reclamation.

It does not claim replication, exactly-once effects, exhaustive interleavings, parent/host crash recovery for the ledger, arbitrary power-loss safety, or a performance target. Chapter 20 uses this correctness boundary while adding resource measurement and one measured optimization; the transcript-heavy gate scenario itself must not be mislabeled a benchmark.
