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
