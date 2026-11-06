use std::collections::{BTreeMap, BTreeSet};
use broker_core::delivery::{AckOutcome, AckRejection, LeaseBatch, LeaseToken};
use super::support::{LEASE_MS, QUOTA, WINDOW, g, p};

#[derive(Clone, Copy)]
pub struct Attempt {
    pub generation: u64,
    pub deadline: u64,
    pub done: bool,
}
#[derive(Default)]
pub struct Group {
    pub next: u64,
    pub issued: u64,
    pub persisted: u64,
    pub attempts: BTreeMap<u64, Attempt>,
}
pub struct Model {
    pub epoch: u64,
    pub now: u64,
    pub records: Vec<[u8; 8]>,
    pub groups: [Group; 2],
    pub segments: Vec<(u64, u64)>,
}
impl Model {
    pub fn new(epoch: u64) -> Self {
        Self { epoch, now: 0, records: Vec::new(), groups: [Group::default(), Group::default()],
            segments: vec![(0, 0)] }
    }
    pub fn end(&self) -> u64 { self.records.len() as u64 }
    pub fn floor(&self) -> u64 { self.segments[0].0 }
    pub fn bytes(&self) -> u64 {
        32 * self.segments.len() as u64 + 40 * (self.end() - self.floor())
    }
    pub fn can_append(&self) -> bool { self.bytes() + 72 <= QUOTA }
    pub fn append(&mut self, payload: [u8; 8]) -> u64 {
        assert!(self.can_append());
        let offset = self.end();
        let tail = *self.segments.last().unwrap();
        if tail.1 - tail.0 == 2 { self.segments.push((offset, offset)); }
        self.segments.last_mut().unwrap().1 = offset + 1;
        self.records.push(payload);
        offset
    }
    pub fn fetch(&mut self, index: usize, count: usize, bytes: usize, batch: &LeaseBatch) {
        let end = self.end();
        let floor = self.floor();
        let group = &mut self.groups[index];
        let free = WINDOW - group.attempts.len();
        let expired = group.attempts.values().filter(|a| !a.done && a.deadline <= self.now).count();
        let eligible = expired + free.min((end - group.issued) as usize);
        assert_eq!(batch.records.len(), eligible.min(count).min(bytes / 40));
        assert_eq!(batch.encoded_bytes, batch.records.len() * 40);
        let mut distinct = BTreeSet::new();
        for record in &batch.records {
            let token = record.token;
            let offset = token.offset.get();
            assert!(distinct.insert(offset), "one batch repeats an offset");
            assert!(floor <= offset && offset < end);
            assert_eq!(record.record.offset(), token.offset);
            assert_eq!(record.record.payload(), &self.records[offset as usize]);
            assert_eq!((token.epoch.get(), token.partition, token.group), (self.epoch, p(), g(index)));
            assert_eq!(token.deadline_ms, self.now + LEASE_MS);
            if offset == group.issued {
                assert!(group.attempts.len() < WINDOW);
                assert_eq!(token.generation, 1);
                group.issued += 1;
            } else {
                let old = group.attempts.get(&offset).expect("retry must name an occupied offset");
                assert!(!old.done && old.deadline <= self.now);
                assert_eq!(token.generation, old.generation + 1);
            }
            group.attempts.insert(offset, Attempt {
                generation: token.generation, deadline: token.deadline_ms, done: false,
            });
        }
        assert_eq!(batch.state.epoch.get(), self.epoch);
        assert_eq!((batch.state.partition, batch.state.group), (p(), g(index)));
        assert_eq!((batch.state.window.completed_end.get(), batch.state.window.issued_end.get()),
            (group.next, group.issued));
        assert_eq!((batch.state.window.occupied, batch.state.window.capacity),
            (group.attempts.len(), WINDOW));
    }
    pub fn acknowledge(&mut self, index: usize, token: LeaseToken) -> (AckOutcome, usize) {
        use AckOutcome::{AlreadyRetired, Applied, Duplicate, Rejected};
        use AckRejection::*;
        if token.epoch.get() != self.epoch { return (Rejected(WrongEpoch), 0); }
        if token.partition != p() { return (Rejected(WrongPartition), 0); }
        if token.group != g(index) { return (Rejected(WrongGroup), 0); }
        let group = &mut self.groups[index];
        let offset = token.offset.get();
        if offset < group.next { return (AlreadyRetired, 0); }
        let Some(attempt) = group.attempts.get_mut(&offset) else { return (Rejected(NotIssued), 0); };
        if token.generation != attempt.generation { return (Rejected(StaleGeneration), 0); }
        if token.deadline_ms != attempt.deadline { return (Rejected(WrongDeadline), 0); }
        if attempt.done { return (Duplicate, 0); }
        if self.now >= attempt.deadline { return (Rejected(Expired), 0); }
        attempt.done = true;
        let before = group.next;
        while group.attempts.get(&group.next).is_some_and(|entry| entry.done) {
            group.attempts.remove(&group.next);
            group.next += 1;
        }
        (Applied, (group.next - before) as usize)
    }
    pub fn checkpoint(&mut self, index: usize) -> bool {
        let group = &mut self.groups[index];
        let changed = group.persisted != group.next;
        group.persisted = group.next;
        changed
    }
    pub fn retain(&mut self) -> (u64, usize, u64, bool) {
        let before = self.floor();
        let safe = self.groups.iter().map(|group| group.persisted).min().unwrap();
        let end = self.end();
        let tail = *self.segments.last().unwrap();
        let rotated = tail.0 < tail.1 && safe == end;
        if rotated { self.segments.push((end, end)); }
        let floor = self.segments.iter().map(|(base, _)| *base)
            .filter(|base| *base <= safe).max().unwrap();
        let removed = self.segments.iter().filter(|(base, _)| *base < floor).count();
        let bytes = 32 * removed as u64 + 40 * (floor - before);
        self.segments.retain(|(base, _)| *base >= floor);
        (floor, removed, bytes, rotated)
    }
    pub fn restart(&mut self, epoch: u64) {
        assert!(epoch > self.epoch);
        self.epoch = epoch;
        self.now = 0;
        for group in &mut self.groups {
            group.next = group.persisted;
            group.issued = group.persisted;
            group.attempts.clear();
        }
    }
}
