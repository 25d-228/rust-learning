use std::fs;
use std::sync::Arc;
use broker_core::delivery::{AckOutcome, LeaseToken};
use broker_core::delivery::clock::ManualClock;
use broker_core::delivery::progress::ProgressAck;
use broker_core::mailbox::AdmissionError;
use broker_core::storage::{checkpoint, recovery, retention};
use broker_core::storage::segment::ReadLimits;
use broker_core::types::Offset;
use broker_core::worker::{Client, WorkerHandle};
use crate::common::TestDir;
use super::model::Model;
use super::support::*;

pub struct Rig {
    // Declaration order matters for fallback destruction: join before deleting the directory.
    pub owner: Option<WorkerHandle>,
    pub client: Client,
    pub clock: Arc<ManualClock>,
    pub io: FaultIo,
    pub model: Model,
    pub tokens: [Vec<LeaseToken>; 2],
    pub root: std::path::PathBuf,
    next_id: u64,
    _directory: TestDir,
}
impl Rig {
    pub fn new() -> Self {
        let (directory, root, session) = setup();
        let model = Model::new(session.epoch().get());
        let clock = Arc::new(ManualClock::new(0));
        let io = FaultIo::default();
        let (owner, client) = start(session, io.clone(), Arc::clone(&clock));
        Self { owner: Some(owner), client, clock, io, model,
            tokens: [Vec::new(), Vec::new()], root, next_id: 0, _directory: directory }
    }
    pub fn append(&mut self) -> bool {
        let bytes = payload(self.next_id);
        self.next_id += 1; // Rejected logical inputs are not assigned storage offsets.
        if !self.model.can_append() {
            let rejected = self.client.try_publish(bytes.to_vec()).err().expect("quota refusal");
            assert_eq!(rejected.reason, AdmissionError::DiskFull);
            assert_eq!(rejected.value, bytes);
            return false;
        }
        let expected = self.model.append(bytes);
        assert_eq!(get(self.client.try_publish(bytes.to_vec())).get(), expected);
        true
    }
    pub fn abandoned_append_then_fence(&mut self) {
        // No control-lane command is used as a data-lane fence.
        assert!(self.model.bytes() + 144 <= QUOTA, "fixture needs two admissions");
        let bytes = payload(self.next_id);
        self.next_id += 1;
        drop(self.client.try_publish(bytes.to_vec()).unwrap());
        self.model.append(bytes);
        assert!(self.append()); // Same data lane; count-one commit precedes this reply.
    }
    pub fn fetch(&mut self, index: usize, count: usize, bytes: usize) -> Vec<LeaseToken> {
        let batch = get(self.client.try_lease(g(index), ReadLimits::new(count, bytes).unwrap()));
        self.model.fetch(index, count, bytes, &batch);
        let tokens: Vec<_> = batch.records.iter().map(|record| record.token).collect();
        self.tokens[index].extend_from_slice(&tokens);
        tokens
    }
    pub fn ack(&mut self, index: usize, token: LeaseToken) -> AckOutcome {
        let (expected, retired) = self.model.acknowledge(index, token);
        let result = get(self.client.try_ack_progress(g(index), token));
        let (actual, progress, accepted) = match result {
            ProgressAck::Accepted(value) => (value.ack, value.progress, true),
            ProgressAck::NotApplied { ack, progress } => (ack, progress, false),
        };
        assert_eq!(actual.outcome, expected);
        assert_eq!(actual.retired, retired);
        assert_eq!(accepted, matches!(expected, AckOutcome::Applied | AckOutcome::Duplicate));
        assert_eq!((progress.volatile_next.get(), progress.persisted_next.get()),
            (self.model.groups[index].next, self.model.groups[index].persisted));
        expected
    }
    pub fn advance(&mut self, millis: u64) {
        self.model.now += millis;
        assert_eq!(self.clock.advance(millis).unwrap(), self.model.now);
    }
    pub fn checkpoint(&mut self, index: usize, abandon: bool) {
        let changed = self.model.checkpoint(index);
        let ticket = self.client.try_checkpoint(g(index)).unwrap();
        if abandon {
            drop(ticket);
            // Checkpoint and progress query use the same FIFO control lane on this raw client.
            let progress = get(self.client.try_progress(g(index)));
            assert_eq!(progress.persisted_next.get(), self.model.groups[index].persisted);
        } else {
            let result = receive(ticket).unwrap();
            assert_eq!(result.published, changed);
            assert_eq!(result.progress.persisted_next.get(), self.model.groups[index].persisted);
        }
    }
    pub fn retain(&mut self) {
        let before = self.model.floor();
        let (floor, count, bytes, rotated) = self.model.retain();
        let result = get(self.client.try_retention());
        assert_eq!((result.before.get(), result.floor.get()), (before, floor));
        assert_eq!((result.removed_segments, result.removed_bytes, result.rotated), (count, bytes, rotated));
        assert_eq!(result.disk.file_bytes, self.model.bytes());
    }
    pub fn stop(&mut self) {
        let state = self.owner.take().expect("one owner").join().unwrap();
        assert_eq!(state.visible_end.get(), self.model.end());
    }
    pub fn failed(&mut self) {
        assert!(self.owner.take().expect("one owner").join().is_err());
        let disk = self.client.disk_state().unwrap();
        assert!(disk.stopped);
        assert_eq!(disk.reserved_bytes, 0);
        assert_eq!(disk.charged_upper_bound(), None);
    }
    pub fn restart(&mut self) {
        self.stop(); // Plain join, deliberately NOT the supervised final-checkpoint policy.
        let session = recovery::recover(&self.root).unwrap();
        assert_eq!(session.epoch().get(), self.model.epoch + 1);
        self.model.restart(session.epoch().get());
        self.clock = Arc::new(ManualClock::new(0));
        let (owner, client) = start(session, self.io.clone(), Arc::clone(&self.clock));
        self.owner = Some(owner);
        self.client = client;
    }
    pub fn check(&self) {
        let state = get(self.client.try_snapshot());
        assert_eq!((state.assigned_end.get(), state.written_end.get(), state.visible_end.get()),
            (self.model.end(), self.model.end(), self.model.end()));
        assert_eq!((state.pending_records, state.pending_bytes), (0, 0));
        for index in 0..2 {
            let expected = &self.model.groups[index];
            let progress = get(self.client.try_progress(g(index)));
            let group = get(self.client.try_group_state(g(index)));
            assert_eq!((progress.volatile_next.get(), progress.persisted_next.get()),
                (expected.next, expected.persisted));
            assert_eq!((group.window.completed_end.get(), group.window.issued_end.get()),
                (expected.next, expected.issued));
            assert_eq!((group.window.occupied, group.window.capacity), (expected.attempts.len(), WINDOW));
            assert_eq!(checkpoint::read(&self.root, p(), g(index)).unwrap().next.get(), expected.persisted);
        }
        assert_eq!(retention::read(&self.root, p()).unwrap().floor.get(), self.model.floor());
        let disk = self.client.disk_state().unwrap();
        assert!(!disk.stopped);
        assert_eq!((disk.file_bytes, disk.reserved_bytes), (self.model.bytes(), 0));
        let physical_lengths: u64 = fs::read_dir(self.root.join("partition-0")).unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len()).sum();
        assert_eq!(physical_lengths, disk.file_bytes); // Length, not allocated filesystem blocks.
        let batch = get(self.client.try_fetch(Offset::new(self.model.floor()), ReadLimits::new(1024, 4096).unwrap()));
        assert_eq!(batch.next.get(), self.model.end());
        assert_eq!(batch.records.len() as u64, self.model.end() - self.model.floor());
        for (i, record) in batch.records.iter().enumerate() {
            let offset = self.model.floor() + i as u64;
            assert_eq!(record.offset().get(), offset);
            assert_eq!(record.payload(), &self.model.records[offset as usize]);
        }
    }
}
