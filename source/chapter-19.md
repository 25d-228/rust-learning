# Chapter 19 — End-to-end coverage and overload demonstration

## Part 1 — External expectations and fresh recovery coverage

> **Learning objectives**
> Preserve expectations outside the broker being killed; distinguish append attempts, stored records, and delivery attempts; check fresh coverage after restart; and authorize deletion only after both groups have verified and persisted their progress.

**Implementation time: 5.0 hours across all four parts**, not per page: 1.0 for the external ledger, 0.75 for child ownership and rendezvous, 1.25 for the composed scenario and commands, and 2.0 for running checks, diagnosing failures, and the edit-and-restore exercises. Reading is excluded. Continue with [Part 2 — Owned child processes and controlled interruption](https://25d-228.github.io/rust-learning/chapter-19-part-02.html), [Part 3 — Coverage, overload, and safe reclamation](https://25d-228.github.io/rust-learning/chapter-19-part-03.html), and [Part 4 — Executable scenarios and checker counterexamples](https://25d-228.github.io/rust-learning/chapter-19-part-04.html).

**Prerequisite code:** the complete workspace through [Chapter 18](https://25d-228.github.io/rust-learning/chapter-18.html). The real TCP `Client`, framed protocol, bounded `LocalBroker`, `Supervisor`, `SyncIo` boundary, recovery, checkpoint, and retention implementations come from Chapters 7–17. Chapter 18 supplies the smaller state/failure checks to run alongside this scenario; we do not replace them with networking.

The **nine-file overlay** at `book/media/chapter-19/` replaces only `broker-app/src/lib.rs` and adds `workload.rs`, five supporting modules, the `broker-load` binary, and `tests/end_to_end.rs`. Apply it after overlays 1–18 in a separate learner workspace. Every new source file is printed in full across explained fragments. Concatenate blocks with the same filename in order; assemble all four parts before compiling. No manifest, dependency, production broker algorithm, persistent encoding, or wire encoding changes. The child modes are a workload-specific way to host the existing broker, not a replacement for `brokerd`.

## A restart check needs an expectation that did not restart

Suppose the producer receives successful offset 3. The broker is killed. Recovery starts, scans only offsets 0–2, and reports that it found three valid records. A checker that constructs its expected set from that recovered scan will accept the loss: the implementation and its oracle have forgotten the same record.

Our parent process records every planned input and every observed append result before the controlled broker termination. That parent, its maps, and `ledger.tsv` remain alive. The broker's files live under `RUN/broker/`; the external transcript, child logs, and rendezvous files live beside that directory. The parent kills only handles it spawned. It does not signal a process group, delete the run, or restart its own ledger.

This separates **process failure domains**, not physical machines. The directories can share a filesystem and storage device. The transcript is useful for diagnosis, but this chapter does not implement replay after a checker crash, a transactional ledger across host failure, or a power-loss experiment. A ledger write failure fails the run; it is never converted into successful coverage.

During crash coverage, no retention request runs and both persisted group frontiers remain zero. Only after a fresh post-restart scan proves coverage do we enter the separate overload/retention phase. There, group 1 stays uncheckpointed until it has caught up. Previously verified, legitimately deleted data is not later counted as a durability failure.

## Three identities, not one counter

A logical input is `(producer, sequence)`. An **append attempt** adds an explicit attempt number. A stored record is identified by `(partition, offset)`. A **delivery attempt** additionally has a group, epoch, lease generation, and deadline. None of these identities is a substitute for another.

| Observation | Meaning | Incorrect inference |
| --- | --- | --- |
| Confirmed append | Valid success reply names a partition/offset | An external consumer effect happened once |
| Ambiguous append | No decisive success/refusal was observed | The record must be absent |
| Rejected append | A defined nonacceptance response was received | Every transport error is a rejection |
| Delivered record | This group observed and validated these bytes | Acknowledgement or checkpoint succeeded |
| Checkpointed progress | A successful checkpoint reply names a persisted contiguous frontier | All out-of-order completions were persisted |

Let $C$ be confirmed attempt identities, $A$ ambiguous identities, and $R$ identities whose records appear in a fresh retained scan. While retention is disabled, the checker requires:

$$
C \subseteq R \subseteq C \cup A.
$$

This is not merely a count comparison. Every confirmed input must retain its exact partition, offset, and payload. Every returned record must correspond to a known confirmed or ambiguous attempt. Rejected and unknown inputs must not appear. Two different inputs cannot claim the same position; one append attempt cannot silently appear at two offsets. A deliberate application retry uses a new attempt identity and can produce a second record for the same logical input.

For example, attempt `(0, 2, 0)` has no reply before the broker dies. After restart, `(0, 2, 1)` succeeds. The scan may find only the retry or both attempts. The latter is a duplicate **logical input**, not an offset collision. Delivering offset 0 twice to group 0 is a different phenomenon: repeated delivery of one stored record.

## Connect the workload module without changing dependencies

Replace the small library root with this complete file. `pub mod workload` makes the driver callable from an integration test; its helper modules remain private to that module. The existing modules and error alias are unchanged.

```rust title="crates/broker-app/src/lib.rs"
pub mod client;
pub mod local;
pub mod protocol;
pub mod server;
pub mod supervisor;
pub mod workload;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
```

Create `workload.rs`. The fixed constants keep the correctness fixture small. Four encoded records fit the response buffer; the byte limit counts encoded records, not wire-token overhead. The public `execute` wrapper deliberately creates a runtime **outside** asynchronous code. Tests call it from ordinary synchronous `#[test]` functions. Code already inside a runtime uses `run(...).await` instead; it must not nest `block_on`.

```rust title="crates/broker-app/src/workload.rs"
use std::io;
use std::path::Path;
use std::time::Duration;
use broker_core::types::{GroupId, PartitionId};
use crate::BoxError;

mod children;
mod ledger;
mod process;
mod scenario;
#[cfg(test)]
mod tests;

pub use children::{broker_child, consumer_child};
pub use scenario::run;

pub const PAYLOAD_BYTES: usize = 24;
pub const RECORD_BYTES: usize = 32 + PAYLOAD_BYTES;
pub const FETCH_BYTES: usize = 4 * RECORD_BYTES;
pub const DATA_QUOTA: u64 = 640;
pub const TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_ATTEMPTS: usize = 4096;

fn p(index: u16) -> PartitionId { PartitionId::new(index).expect("fixed partition") }
fn g(index: u8) -> GroupId { GroupId::new(index).expect("fixed group") }
fn require(condition: bool, message: &str) -> io::Result<()> {
    if condition { Ok(()) } else { Err(io::Error::other(message.to_owned())) }
}
pub fn validate_rounds(rounds: u32) -> io::Result<()> {
    require((1..=32).contains(&rounds), "rounds must be in 1..=32")
}

#[derive(Debug, Clone)]
pub struct Report {
    pub confirmed: usize,
    pub ambiguous: usize,
    pub rejected: usize,
    pub observed_ambiguous: usize,
    pub unique_records: usize,
    pub repeated_deliveries: u64,
    pub duplicate_logical_inputs: usize,
    pub rounds: u32,
    pub floor: [u64; 4],
    pub broker_kills: u32,
    pub consumer_kills: u32,
    pub obsolete_token_rejected: bool,
}

pub fn execute(executable: &Path, root: &Path, rounds: u32) -> Result<Report, BoxError> {
    validate_rounds(rounds)?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(run(executable, root, rounds))
}
```

The report is a summary of checked observations, not a throughput report. Child-kill and fencing fields start false/zero in the ledger's summary and are filled by `run` only after the corresponding operations and final checks succeed. There is no success report on an error path.

The limit of 32 steady rounds and 4,096 registered attempts bounds this checker. Its history grows with its finite input set; it is **not** a model for a forever-running consumer's memory usage. Broker payload pools, owner scratch, socket buffers, and checker maps remain separate accounting domains.

## Put identity in the payload itself

Create `workload/ledger.rs` with the following imports and key type. `Ord` supplies a total order for map keys; the derived comparison follows field declaration order. We use ordered maps for inspectable keys and direct lookups, not the broker's ring or sparse index. See the standard [`BTreeMap` contract](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html).

The exact 24-byte payload is eight magic bytes, a big-endian 32-bit producer ID, a big-endian 32-bit attempt number, and a big-endian 64-bit sequence number. The field order on the wire need not match Rust's struct layout because encoding names every byte range.

```rust title="crates/broker-app/src/workload/ledger.rs"
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use broker_core::delivery::LeaseToken;
use broker_core::types::Offset;
use crate::client::ClientError;
use crate::protocol::Status;
use super::{MAX_ATTEMPTS, PAYLOAD_BYTES, Report, require};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Key { pub producer: u32, pub sequence: u64, pub attempt: u32 }
impl Key {
    pub fn partition(self) -> u16 { (self.producer % 4) as u16 }
    pub fn payload(self) -> [u8; PAYLOAD_BYTES] {
        let mut bytes = [0; PAYLOAD_BYTES];
        bytes[..8].copy_from_slice(b"LOAD0001");
        bytes[8..12].copy_from_slice(&self.producer.to_be_bytes());
        bytes[12..16].copy_from_slice(&self.attempt.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.sequence.to_be_bytes());
        bytes
    }
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        require(bytes.len() == PAYLOAD_BYTES, "workload payload length changed")?;
        require(&bytes[..8] == b"LOAD0001", "workload payload magic changed")?;
        Ok(Self {
            producer: u32::from_be_bytes(bytes[8..12].try_into().expect("checked length")),
            attempt: u32::from_be_bytes(bytes[12..16].try_into().expect("checked length")),
            sequence: u64::from_be_bytes(bytes[16..24].try_into().expect("checked length")),
        })
    }
}

```

For producer 5, attempt 2, sequence `0x0102030405060708`, the bytes are:

```text
4c 4f 41 44 30 30 30 31 | 00 00 00 05 | 00 00 00 02 | 01 02 03 04 05 06 07 08
        LOAD0001        | producer 5  | attempt 2   | sequence
```

The partition rule is `producer % 4`, so this input goes to partition 1. Producer IDs 0 and 4 share partition 0 but remain distinct logical producers. The actual order of their concurrent successful appends is allowed to vary; their returned offsets, not task completion order, establish the stored order.

Decoding first checks the exact length, then the magic, before slicing fields. A changed payload either fails that structure check or names an unknown/differently bound input. The wire client's earlier record-checksum validation remains useful, but the ledger does not accept a checksum as proof that the bytes are the intended input.

## Classify replies at the existing acceptance boundary

Add `Outcome` and `classify`. The status classification follows Chapter 16's `server/dispatch.rs`: admission refusals and the defined owner refusal happen before an append is accepted for mutation; lost channels, uncertain accepted I/O, and reply timeouts use `Uncertain`. `Unavailable` is the existing closed-admission response. This mapping is operation-specific; do not apply it to a different protocol without inspecting that protocol's acceptance boundary.

```rust title="crates/broker-app/src/workload/ledger.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome { Pending, Confirmed(u64), Ambiguous, Rejected }
pub(super) fn classify(result: &Result<Offset, ClientError>) -> Outcome {
    match result {
        Ok(offset) => Outcome::Confirmed(offset.get()),
        Err(ClientError::Remote(Status::Invalid | Status::Busy | Status::DiskFull
            | Status::AdmissionDeadline | Status::Refused | Status::Unavailable)) => Outcome::Rejected,
        // A transport error, Uncertain, or an unrecognized response is not proof of rejection.
        Err(_) => Outcome::Ambiguous,
    }
}

```

A connection failure before sending could sometimes be classified more precisely, but this generic result classifier conservatively treats transport errors as ambiguous. The scenario refuses unexpected producer errors rather than retrying them invisibly. Its one intentional uncertain append is recorded separately at a controlled boundary.

`Pending` means the parent registered the intent but has not classified its attempt yet. It is not a synonym for ambiguous: coverage fails while any intent is unresolved. This catches a producer task panic or a missing result instead of silently shrinking the expected set.

## Own the transcript and the independent maps

Add the state and creation functions. `positions` binds positions to keys; `locations` binds keys back to positions. We keep both directions because the two collision rules are different. `deliveries` counts observations for each group/position across the run. `current_deliveries` records which observations happened in the current broker epoch, so old evidence cannot satisfy a new recovery scan.

```rust title="crates/broker-app/src/workload/ledger.rs"
pub(super) type Scan = BTreeMap<(u16, u64), Key>;
pub(super) struct Ledger {
    file: File,
    entries: BTreeMap<Key, Outcome>,
    positions: BTreeMap<(u16, u64), Key>,
    locations: BTreeMap<Key, (u16, u64)>,
    deliveries: BTreeMap<(u8, u16, u64), u64>,
    current_deliveries: BTreeSet<(u8, u16, u64)>,
    verified: [[u64; 4]; 2],
    checkpointed: [[u64; 4]; 2],
    floor: [u64; 4],
    epoch: u64,
}
impl Ledger {
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut value = Self { file, entries: BTreeMap::new(), positions: BTreeMap::new(),
            locations: BTreeMap::new(), deliveries: BTreeMap::new(), current_deliveries: BTreeSet::new(),
            verified: [[0; 4]; 2],
            checkpointed: [[0; 4]; 2], floor: [0; 4], epoch: 0 };
        value.note("LEDGER\tversion=1; checker survives broker-only termination")?;
        Ok(value)
    }
    pub fn note(&mut self, text: &str) -> io::Result<()> { writeln!(self.file, "{text}") }
    pub fn sync(&self) -> io::Result<()> { self.file.sync_data() }
    pub fn floor(&self) -> [u64; 4] { self.floor }
    pub fn begin_epoch(&mut self, epoch: u64) -> io::Result<()> {
        require(epoch > self.epoch, "restart did not increase epoch")?;
        self.epoch = epoch;
        // Old observations cannot satisfy a new recovery coverage pass.
        self.verified = [self.floor; 2];
        self.current_deliveries.clear();
        self.note(&format!("EPOCH\t{epoch}"))
    }
    pub fn intent(&mut self, key: Key) -> io::Result<()> {
        require(self.entries.len() < MAX_ATTEMPTS, "checker attempt limit reached")?;
        require(!self.entries.contains_key(&key), "attempt ID reused")?;
        self.note(&format!("INTENT\t{key:?}\tp={}", key.partition()))?;
        self.entries.insert(key, Outcome::Pending);
        Ok(())
    }
```

`create_new(true)` refuses an existing transcript. `File` is not a user-space buffered writer; `note` writes directly through its `Write` implementation. `sync` explicitly observes `sync_data` errors at important scenario boundaries. Neither dropping a file nor flushing a higher-level writer is a substitute for the synchronization contract; see [`File`](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_data) and [`Write`](https://doc.rust-lang.org/std/io/trait.Write.html#tymethod.flush).

The `begin_epoch` transition is crucial. It keeps historical confirmed bindings and delivery counts, but clears current-epoch evidence. It also resets verified frontiers to the already authorized retained floor. A fresh scan must prove everything still retained. Data below that floor was already checked for both groups before deletion and is not required to reappear.

## Bind success without trusting arrival order

Add `bind`, `resolve`, and `observe`. A successful append binds its returned position immediately. An ambiguous attempt acquires a position only if a later validated delivery reveals it. That observation does **not** rewrite the original outcome into confirmed: the parent still did not receive a producer success reply.

```rust title="crates/broker-app/src/workload/ledger.rs"
    fn bind(&mut self, key: Key, position: (u16, u64)) -> io::Result<()> {
        require(position.0 == key.partition(), "payload arrived in the wrong partition")?;
        require(self.positions.get(&position).is_none_or(|other| *other == key),
            "two inputs occupy one partition/offset")?;
        require(self.locations.get(&key).is_none_or(|other| *other == position),
            "one non-retried attempt occupies two offsets")?;
        self.positions.insert(position, key);
        self.locations.insert(key, position);
        Ok(())
    }
    pub fn resolve(&mut self, key: Key, outcome: Outcome) -> io::Result<()> {
        require(self.entries.get(&key) == Some(&Outcome::Pending), "resolve requires one pending attempt")?;
        require(outcome != Outcome::Pending, "pending is not a final append observation")?;
        if let Outcome::Confirmed(offset) = outcome { self.bind(key, (key.partition(), offset))?; }
        self.note(&format!("APPEND\t{key:?}\t{outcome:?}"))?;
        self.entries.insert(key, outcome);
        Ok(())
    }
    pub fn observe(&mut self, token: LeaseToken, payload: &[u8]) -> io::Result<Key> {
        require(token.epoch.get() == self.epoch, "delivery has the wrong epoch")?;
        let key = Key::decode(payload)?;
        match self.entries.get(&key) {
            Some(Outcome::Confirmed(offset)) => require(*offset == token.offset.get(),
                "confirmed input changed offset")?,
            Some(Outcome::Ambiguous) => {},
            _ => return Err(io::Error::other("unknown, rejected, or unresolved input was delivered")),
        }
        self.bind(key, (token.partition.get(), token.offset.get()))?;
        let identity = (token.group.get(), token.partition.get(), token.offset.get());
        *self.deliveries.entry(identity).or_default() += 1;
        self.current_deliveries.insert(identity);
        self.note(&format!("DELIVER\tg={}\tp={}\to={}\t{key:?}", identity.0, identity.1, identity.2))?;
        Ok(key)
    }
```

Consider a confirmed key bound to `(0, 7)`. Returning the correct payload at `(0, 8)` fails before updating delivery counts. Returning a different key at `(0, 7)` fails the reverse binding. Returning the same key twice at `(0, 7)` to the same group is allowed and increments the repeated-delivery count. These checks distinguish misdelivery, offset reuse, and expected at-least-once behavior.

The `+= 1` counter is bounded by this finite driver's checks and rounds, not an unbounded public metric. A long-running measurement service should choose an explicit overflow policy; Chapter 20 addresses measurement rather than extending this transcript indefinitely.

## Verify a fresh interval, including absent records

For partition $p$, let $f_p$ be the first retained offset and $e_p$ the exclusive end observed after draining all currently available records. A full scan must contain every integer position in the half-open interval $[f_p,e_p)$ exactly once in its scan map. Across four partitions, its cardinality is:

$$
|R|=\sum_{p=0}^{3}(e_p-f_p).
$$

Cardinality alone is insufficient: offsets 0 and 2 are not a valid replacement for offsets 0 and 1. The loop checks each position, its registered payload binding, and this group's **current-epoch** observation. The subsequent confirmed-input loop catches a falsely shortened end. Otherwise an empty broker could report an empty scan and evade the first loop.

```rust title="crates/broker-app/src/workload/ledger.rs"
    pub fn coverage(&mut self, group: u8, end: [u64; 4], scan: &Scan) -> io::Result<()> {
        require(group < 2, "invalid coverage group")?;
        let mut count = 0u64;
        for partition in 0..4 {
            let start = self.floor[partition];
            require(end[partition] >= start, "scan end precedes retained floor")?;
            require(end[partition] - start <= MAX_ATTEMPTS as u64, "scan interval exceeds checker bound")?;
            count += end[partition] - start;
            for offset in start..end[partition] {
                let position = (partition as u16, offset);
                let key = scan.get(&position).ok_or_else(|| io::Error::other("gap in fresh coverage scan"))?;
                require(self.positions.get(&position) == Some(key), "scan bypassed payload validation")?;
                require(self.current_deliveries.contains(&(group, partition as u16, offset)),
                    "this group did not observe a scanned record")?;
            }
        }
        require(scan.len() as u64 == count, "scan contains an offset outside the retained interval")?;
        for (key, outcome) in &self.entries {
            require(*outcome != Outcome::Pending, "coverage has an unresolved append")?;
            if let Outcome::Confirmed(offset) = outcome {
                let partition = key.partition() as usize;
                if *offset >= self.floor[partition] {
                    require(scan.get(&(partition as u16, *offset)) == Some(key),
                        "confirmed append is missing from the fresh scan")?;
                }
            }
        }
        self.verified[group as usize] = end;
        self.note(&format!("COVERAGE\tg={group}\tstart={:?}\tend={end:?}", self.floor))
    }
```

The input and interval bounds also protect the checker from an accidentally huge end value. `coverage` does not reconstruct expected payloads from recovery output; it compares the scan with earlier producer-side intentions and successful bindings. A missing confirmed record is fatal even when the returned scan is internally contiguous.

The scenario creates new scan maps after restart. It retains those same maps while appending the later pressure records, since no retention has happened yet. After reclamation, the next steady round starts new maps at the advanced floors. This distinction is explicit in Part 3.

## Require coverage and persistence before advancing a floor

Append the remaining methods. `checkpoint` records an observed checkpoint reply only up to verified coverage. `retained` requires both groups' checked and checkpointed frontiers. These are checks on the demo's deletion sequence, independent of the broker's own retention calculation.

```rust title="crates/broker-app/src/workload/ledger.rs"
    pub fn checkpoint(&mut self, group: u8, partition: u16, next: u64) -> io::Result<()> {
        let (g, p) = (group as usize, partition as usize);
        require(g < 2 && p < 4, "invalid checkpoint scope")?;
        require(next >= self.checkpointed[g][p] && next <= self.verified[g][p],
            "checkpoint regressed or outran verified coverage")?;
        self.checkpointed[g][p] = next;
        self.note(&format!("CHECKPOINT\tg={g}\tp={p}\tnext={next}"))
    }
    pub fn retained(&mut self, partition: u16, next: u64) -> io::Result<()> {
        let p = partition as usize;
        require(p < 4 && next >= self.floor[p], "invalid or regressing retained floor")?;
        for group in 0..2 {
            require(next <= self.checkpointed[group][p] && next <= self.verified[group][p],
                "retention precedes both checked, persisted group frontiers")?;
        }
        self.floor[p] = next;
        self.note(&format!("RETAIN\tp={p}\tfloor={next}"))
    }
    pub fn report(&self, rounds: u32) -> Report {
        let confirmed = self.entries.values().filter(|v| matches!(v, Outcome::Confirmed(_))).count();
        let ambiguous = self.entries.values().filter(|v| **v == Outcome::Ambiguous).count();
        let rejected = self.entries.values().filter(|v| **v == Outcome::Rejected).count();
        let observed_ambiguous = self.entries.iter().filter(|(k, v)| **v == Outcome::Ambiguous
            && self.locations.contains_key(*k)).count();
        let logical: BTreeSet<_> = self.deliveries.keys().filter_map(|(_, p, o)|
            self.positions.get(&(*p, *o))).map(|k| (k.producer, k.sequence)).collect();
        let observed: BTreeSet<_> = self.deliveries.keys().map(|(_, p, o)| (*p, *o)).collect();
        Report { confirmed, ambiguous, rejected, observed_ambiguous, unique_records: observed.len(),
            repeated_deliveries: self.deliveries.values().map(|n| n - 1).sum(),
            duplicate_logical_inputs: observed.len() - logical.len(), rounds, floor: self.floor,
            broker_kills: 0, consumer_kills: 0, obsolete_token_rejected: false }
    }
}
```

If group 0 has checked and checkpointed next offset 9 while group 1 is persisted at 0, the external floor must remain 0. Once both are checked and checkpointed at 9, the demo can accept a retention reply advancing to 9. An empty scan after a later restart then describes the legitimate interval `[9, 9)`, not nine missing records.

For a group/position observed $d$ times, repeated delivery contributes $d-1$. We sum that quantity across groups and positions. Separately, the number of extra stored records attributable to logical-input retries is the number of distinct observed positions minus the number of distinct `(producer, sequence)` pairs. An ambiguous original plus its successful retry contributes one there; repeating either offset to another consumer does not create another stored record.

The next part owns the child processes and establishes the exact interruption point. The ledger remains in the parent throughout.
