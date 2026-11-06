use std::error::Error;
use std::fmt;

use crate::types::{Epoch, GroupId, Offset, PartitionId};
use super::clock::ClockError;
use super::groups::GroupState;
use super::window::{Slot, Window, WindowError, validate_width};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseError {
    Window(WindowError),
    Clock(ClockError),
    LeaseMillis(u64),
    RecordLimit(usize),
    DeadlineExhausted,
    GenerationExhausted(Offset),
}
impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Window(error) => write!(f, "lease window: {error}"),
            Self::Clock(error) => write!(f, "lease time: {error}"),
            Self::LeaseMillis(value) => write!(f, "lease {value}ms is outside 1..=60000ms"),
            Self::RecordLimit(value) => write!(f, "lease batch {value} is outside 1..=1024"),
            Self::DeadlineExhausted => write!(f, "lease deadline cannot be represented"),
            Self::GenerationExhausted(offset) => write!(f,
                "attempt generation exhausted at offset {}", offset.get()),
        }
    }
}
impl Error for LeaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Window(error) => Some(error),
            Self::Clock(error) => Some(error),
            _ => None,
        }
    }
}
impl From<WindowError> for LeaseError {
    fn from(error: WindowError) -> Self { Self::Window(error) }
}
impl From<ClockError> for LeaseError {
    fn from(error: ClockError) -> Self { Self::Clock(error) }
}

#[derive(Debug, Clone, Copy)]
pub struct LeaseConfig {
    pub(crate) epoch: Epoch,
    width: usize,
    millis: u64,
}
impl LeaseConfig {
    pub fn new(epoch: Epoch, width: usize, millis: u64) -> Result<Self, LeaseError> {
        validate_width(width)?;
        if !(1..=60_000).contains(&millis) { return Err(LeaseError::LeaseMillis(millis)); }
        Ok(Self { epoch, width, millis })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseToken {
    pub epoch: Epoch,
    pub partition: PartitionId,
    pub group: GroupId,
    pub offset: Offset,
    pub generation: u64,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckRejection {
    WrongEpoch,
    WrongPartition,
    WrongGroup,
    NotIssued,
    StaleGeneration,
    WrongDeadline,
    Expired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    Applied,
    Duplicate,
    AlreadyRetired,
    Rejected(AckRejection),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    pub outcome: AckOutcome,
    pub retired: usize,
    pub state: GroupState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Attempt {
    generation: u64,
    deadline_ms: u64,
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct Selection {
    pub(crate) offset: Offset,
    generation: u64,
    retry: bool,
}

pub(crate) struct LeaseWindow {
    epoch: Epoch,
    partition: PartitionId,
    group: GroupId,
    window: Window,
    attempts: Box<[Option<Attempt>]>,
    millis: u64,
    retry_cursor: Offset,
    prefer_retry: bool,
}
impl LeaseWindow {
    pub(crate) fn new(partition: PartitionId, group: GroupId, start: Offset,
        config: LeaseConfig) -> Self
    {
        Self {
            epoch: config.epoch, partition, group,
            window: Window::new(start, config.width).expect("validated lease width"),
            attempts: vec![None; config.width].into_boxed_slice(),
            millis: config.millis, retry_cursor: start, prefer_retry: true,
        }
    }

    pub(crate) fn state(&self) -> GroupState {
        GroupState { epoch: self.epoch, partition: self.partition, group: self.group,
            window: self.window.state() }
    }

    fn position(&self, offset: Offset) -> usize {
        (offset.get() % self.attempts.len() as u64) as usize
    }

    fn attempt(&self, offset: Offset) -> Attempt {
        self.attempts[self.position(offset)].expect("every issued slot retains its current attempt")
    }

    // Read-only selection. The owner may use only a prefix after applying its byte limit.
    pub(crate) fn plan(&self, visible: Offset, count: usize, now: u64)
        -> Result<Vec<Selection>, LeaseError>
    {
        if !(1..=1024).contains(&count) { return Err(LeaseError::RecordLimit(count)); }
        let upper = self.window.read_upper(visible)?;
        let state = self.window.state();
        let start = if self.retry_cursor >= state.completed_end && self.retry_cursor < state.issued_end {
            (self.retry_cursor.get() - state.completed_end.get()) as usize
        } else { 0 };
        let mut retries = Vec::with_capacity(count.min(state.occupied));
        for step in 0..state.occupied {
            let relative = (start + step) % state.occupied;
            let offset = Offset::new(state.completed_end.get() + relative as u64);
            if self.window.status(offset) == Some(Slot::Outstanding)
                && self.attempt(offset).deadline_ms <= now
            {
                retries.push(offset);
                if retries.len() == count { break; }
            }
        }
        let mut retry_index = 0;
        let mut next = state.issued_end;
        let mut prefer_retry = self.prefer_retry;
        let mut selected = Vec::with_capacity(count);
        while selected.len() < count {
            let has_retry = retry_index < retries.len();
            let has_new = next < upper;
            if !has_retry && !has_new { break; }
            let retry = has_retry && (prefer_retry || !has_new);
            let (offset, generation) = if retry {
                let offset = retries[retry_index];
                retry_index += 1;
                let generation = self.attempt(offset).generation.checked_add(1)
                    .ok_or(LeaseError::GenerationExhausted(offset))?;
                (offset, generation)
            } else {
                let offset = next;
                next = next.next().expect("offset is below a representable exclusive upper end");
                (offset, 1)
            };
            selected.push(Selection { offset, generation, retry });
            prefer_retry = !retry;
        }
        Ok(selected)
    }

    // Called synchronously by the same owner, without processing another command after plan.
    pub(crate) fn install(&mut self, selections: &[Selection], visible: Offset, now: u64)
        -> Result<Vec<LeaseToken>, LeaseError>
    {
        if selections.is_empty() { return Ok(Vec::new()); }
        let deadline_ms = now.checked_add(self.millis).ok_or(LeaseError::DeadlineExhausted)?;
        let mut next = self.window.state().issued_end;
        let mut new_count = 0;
        let mut tokens = Vec::with_capacity(selections.len());
        for selected in selections {
            if selected.retry {
                assert_eq!(self.window.status(selected.offset), Some(Slot::Outstanding));
                assert_eq!(self.attempt(selected.offset).generation.checked_add(1), Some(selected.generation));
                assert!(self.attempt(selected.offset).deadline_ms <= now);
            } else {
                assert_eq!(selected.offset, next);
                assert_eq!(self.attempts[self.position(next)], None);
                next = next.next().expect("plan checked the exclusive end");
                new_count += 1;
            }
            tokens.push(LeaseToken { epoch: self.epoch, partition: self.partition,
                group: self.group, offset: selected.offset,
                generation: selected.generation, deadline_ms });
        }
        // All recoverable failures occur before the attempts or scheduling cursors change.
        self.window.admit(new_count, visible)?;
        for (selected, token) in selections.iter().zip(&tokens) {
            let position = self.position(selected.offset);
            self.attempts[position] = Some(Attempt {
                generation: token.generation, deadline_ms: token.deadline_ms,
            });
            if selected.retry {
                self.retry_cursor = selected.offset.next().expect("issued offsets have successors");
            }
            self.prefer_retry = !selected.retry;
        }
        Ok(tokens)
    }

    fn unchanged(&self, outcome: AckOutcome) -> Ack {
        Ack { outcome, retired: 0, state: self.state() }
    }

    pub(crate) fn acknowledge(&mut self, token: LeaseToken, now: u64) -> Ack {
        let reject = |reason| self.unchanged(AckOutcome::Rejected(reason));
        if token.epoch != self.epoch { return reject(AckRejection::WrongEpoch); }
        if token.partition != self.partition { return reject(AckRejection::WrongPartition); }
        if token.group != self.group { return reject(AckRejection::WrongGroup); }
        let before = self.window.state();
        if token.offset < before.completed_end { return self.unchanged(AckOutcome::AlreadyRetired); }
        if token.offset >= before.issued_end { return reject(AckRejection::NotIssued); }
        let attempt = self.attempt(token.offset);
        if token.generation != attempt.generation { return reject(AckRejection::StaleGeneration); }
        if token.deadline_ms != attempt.deadline_ms { return reject(AckRejection::WrongDeadline); }
        // This describes an earlier accepted completion; it does not accept expired work anew.
        if self.window.status(token.offset) == Some(Slot::Completed) {
            return self.unchanged(AckOutcome::Duplicate);
        }
        if now >= attempt.deadline_ms { return reject(AckRejection::Expired); }
        let completion = self.window.complete(token.offset).expect("validated active offset");
        for value in before.completed_end.get()..completion.state.completed_end.get() {
            let position = self.position(Offset::new(value));
            self.attempts[position] = None;
        }
        Ack { outcome: AckOutcome::Applied, retired: completion.retired, state: self.state() }
    }
}

#[cfg(test)]
mod tests;
