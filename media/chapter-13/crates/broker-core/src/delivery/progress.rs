use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::storage::checkpoint::{self, Checkpoint};
use crate::storage::error::StorageError;
use crate::storage::sync::SyncIo;
use crate::types::{Epoch, GROUP_COUNT, GroupId, Offset, PartitionId};
use super::lease::{Ack, AckOutcome};

#[derive(Debug, Clone, Copy)]
pub struct CheckpointPolicy {
    delay: Option<Duration>,
}
impl CheckpointPolicy {
    pub fn explicit_only() -> Self { Self { delay: None } }

    pub fn new(delay: Duration) -> Result<Self, StorageError> {
        if delay.is_zero() || delay > Duration::from_secs(60) {
            return Err(StorageError::Config("checkpoint delay must be in (0, 60s]"));
        }
        Ok(Self { delay: Some(delay) })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgressState {
    pub epoch: Epoch,
    pub partition: PartitionId,
    pub group: GroupId,
    pub volatile_next: Offset,
    pub persisted_next: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckAccepted {
    pub ack: Ack,
    pub progress: ProgressState,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressAck {
    Accepted(AckAccepted),
    NotApplied { ack: Ack, progress: ProgressState },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpointed {
    pub progress: ProgressState,
    pub published: bool,
}

#[derive(Clone, Copy)]
struct Frontier {
    volatile: Offset,
    persisted: Offset,
    due: Option<Instant>,
}

pub(crate) struct Progress {
    root: PathBuf,
    epoch: Epoch,
    partition: PartitionId,
    policy: CheckpointPolicy,
    fronts: [Frontier; GROUP_COUNT as usize],
}
impl Progress {
    pub(crate) fn new(root: PathBuf, epoch: Epoch, partition: PartitionId,
        starts: [Offset; GROUP_COUNT as usize], visible: Offset, policy: CheckpointPolicy)
        -> Result<Self, StorageError>
    {
        if starts.iter().any(|start| *start > visible) {
            return Err(StorageError::Metadata("checkpoint exceeds synchronized visibility"));
        }
        Ok(Self { root, epoch, partition, policy,
            fronts: starts.map(|start| Frontier { volatile: start, persisted: start, due: None }) })
    }

    pub(crate) fn state(&self, group: GroupId) -> ProgressState {
        let f = self.fronts[usize::from(group.get())];
        ProgressState { epoch: self.epoch, partition: self.partition, group,
            volatile_next: f.volatile, persisted_next: f.persisted }
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.fronts.iter().filter_map(|f| f.due).min()
    }

    pub(crate) fn due(&self, group: GroupId, now: Instant) -> bool {
        self.fronts[usize::from(group.get())].due.is_some_and(|end| end <= now)
    }

    pub(crate) fn observe(&mut self, ack: Ack, visible: Offset, now: Instant)
        -> Result<ProgressAck, StorageError>
    {
        if ack.state.epoch != self.epoch || ack.state.partition != self.partition {
            return Err(StorageError::Topology("acknowledgement belongs to another progress owner"));
        }
        let group = ack.state.group;
        let f = &mut self.fronts[usize::from(group.get())];
        let next = ack.state.window.completed_end;
        if next < f.volatile || next > visible || f.persisted > next {
            return Err(StorageError::Topology("invalid contiguous progress observation"));
        }
        // Check the first dirty deadline before changing this progress state.
        let due = if next > f.persisted && f.due.is_none() {
            match self.policy.delay {
                Some(delay) => Some(now.checked_add(delay)
                    .ok_or(StorageError::Config("checkpoint deadline cannot be represented"))?),
                None => None,
            }
        } else { f.due };
        f.volatile = next;
        f.due = due;
        let progress = self.state(group);
        Ok(match ack.outcome {
            AckOutcome::Applied | AckOutcome::Duplicate => ProgressAck::Accepted(AckAccepted { ack, progress }),
            AckOutcome::AlreadyRetired | AckOutcome::Rejected(_) => ProgressAck::NotApplied { ack, progress },
        })
    }

    pub(crate) fn checkpoint<S: SyncIo + ?Sized>(&mut self, group: GroupId,
        visible: Offset, io: &mut S) -> Result<Checkpointed, StorageError>
    {
        let index = usize::from(group.get());
        let f = self.fronts[index];
        if f.persisted > f.volatile || f.volatile > visible {
            return Err(StorageError::Topology("checkpoint would exceed contiguous visible progress"));
        }
        if f.persisted == f.volatile {
            return Ok(Checkpointed { progress: self.state(group), published: false });
        }
        let value = Checkpoint { partition: self.partition, group, epoch: self.epoch, next: f.volatile };
        checkpoint::publish(&self.root, value, io, &mut |_| Ok(()))?;
        // No response and no persisted frontier advance before full publication succeeds.
        self.fronts[index].persisted = f.volatile;
        self.fronts[index].due = None;
        Ok(Checkpointed { progress: self.state(group), published: true })
    }
}

#[cfg(test)]
mod tests;
