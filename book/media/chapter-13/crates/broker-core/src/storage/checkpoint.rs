use std::fs::{self, File};
use std::path::Path;

use crate::io::{open_file, read_prefix};
use crate::types::{Epoch, GROUP_COUNT, GroupId, Offset, PARTITION_COUNT, PartitionId};
use super::error::StorageError;
use super::metadata::{self, PublicationStep};
use super::sync::SyncIo;

pub const CHECKPOINT_LEN: usize = 48;

pub fn name(partition: PartitionId, group: GroupId) -> String {
    format!("progress-p{}-g{}.ckp", partition.get(), group.get())
}
pub fn pending_name(partition: PartitionId, group: GroupId) -> String {
    format!("{}.next", name(partition, group))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    pub partition: PartitionId,
    pub group: GroupId,
    pub epoch: Epoch,
    pub next: Offset,
}
impl Checkpoint {
    pub fn encode(self) -> [u8; CHECKPOINT_LEN] {
        let mut raw = [0; CHECKPOINT_LEN];
        raw[..8].copy_from_slice(b"PBGP\x00\x01\x00\x30");
        raw[8..10].copy_from_slice(&self.partition.get().to_be_bytes());
        raw[10] = self.group.get();
        raw[12..20].copy_from_slice(&self.epoch.get().to_be_bytes());
        raw[20..28].copy_from_slice(&self.next.get().to_be_bytes());
        let crc = crc32fast::hash(&raw[..44]);
        raw[44..48].copy_from_slice(&crc.to_be_bytes());
        raw
    }

    pub fn decode(raw: &[u8]) -> Result<Self, StorageError> {
        if raw.len() != CHECKPOINT_LEN {
            return Err(StorageError::Metadata("checkpoint length is not 48"));
        }
        let stored = u32::from_be_bytes(raw[44..48].try_into().expect("fixed CRC field"));
        if stored != crc32fast::hash(&raw[..44]) {
            return Err(StorageError::Metadata("checkpoint checksum mismatch"));
        }
        if &raw[..8] != b"PBGP\x00\x01\x00\x30" || raw[11] != 0
            || raw[28..44].iter().any(|b| *b != 0)
        {
            return Err(StorageError::Metadata("checkpoint header or reserved fields are invalid"));
        }
        let partition = PartitionId::new(u16::from_be_bytes(raw[8..10].try_into().expect("fixed partition")))
            .map_err(|_| StorageError::Metadata("checkpoint partition is out of range"))?;
        let group = GroupId::new(raw[10])
            .map_err(|_| StorageError::Metadata("checkpoint group is out of range"))?;
        let epoch = Epoch::new(u64::from_be_bytes(raw[12..20].try_into().expect("fixed epoch")))
            .map_err(|_| StorageError::Metadata("checkpoint epoch is zero"))?;
        let next = Offset::new(u64::from_be_bytes(raw[20..28].try_into().expect("fixed next offset")));
        Ok(Self { partition, group, epoch, next })
    }

    pub fn validate(self, partition: PartitionId, group: GroupId, epoch: Epoch,
        floor: Offset, end: Offset) -> Result<Offset, StorageError>
    {
        if self.partition != partition || self.group != group {
            return Err(StorageError::Metadata("checkpoint scope differs from its filename"));
        }
        if self.epoch > epoch {
            return Err(StorageError::Metadata("checkpoint comes from a future startup epoch"));
        }
        if floor > end || self.next < floor || self.next > end {
            return Err(StorageError::Metadata("checkpoint is outside the recovered record interval"));
        }
        Ok(self.next)
    }
}

pub fn read(root: &Path, partition: PartitionId, group: GroupId) -> Result<Checkpoint, StorageError> {
    let path = root.join(name(partition, group));
    let info = fs::symlink_metadata(&path)
        .map_err(|e| StorageError::path("inspect required checkpoint; never default to zero", &path, e))?;
    if !info.file_type().is_file() || info.len() != CHECKPOINT_LEN as u64 {
        return Err(StorageError::Metadata("checkpoint must be a regular 48-byte file"));
    }
    let mut file = File::open(&path).map_err(|e| StorageError::path("open checkpoint", &path, e))?;
    let mut raw = [0; CHECKPOINT_LEN];
    let count = read_prefix(&mut file, &mut raw)
        .map_err(|e| StorageError::path("read checkpoint", &path, e))?;
    Checkpoint::decode(&raw[..count])
}

pub(crate) fn is_name(candidate: &str) -> bool {
    (0..PARTITION_COUNT).any(|p| (0..GROUP_COUNT).any(|g| {
        let partition = PartitionId::new(p).expect("fixed partitions");
        let group = GroupId::new(g).expect("fixed groups");
        candidate == name(partition, group) || candidate == pending_name(partition, group)
    }))
}

pub(crate) fn publish<S, H>(root: &Path, value: Checkpoint, io: &mut S, after: &mut H)
    -> Result<(), StorageError>
where S: SyncIo + ?Sized, H: FnMut(PublicationStep) -> std::io::Result<()> {
    metadata::publish_bytes(root, &name(value.partition, value.group),
        &pending_name(value.partition, value.group), &value.encode(), io, after)
}

pub(crate) fn synchronize<S: SyncIo + ?Sized>(root: &Path, partition: PartitionId,
    group: GroupId, io: &mut S) -> Result<(), StorageError>
{
    let path = root.join(name(partition, group));
    let mut file = open_file(&path).map_err(|e| StorageError::path("reopen checkpoint for sync", &path, e))?;
    io.sync_checkpoint(&mut file)
        .map_err(|e| StorageError::path("synchronize recovered checkpoint", &path, e))
}

pub(crate) fn discard_pending(root: &Path, partition: PartitionId, group: GroupId)
    -> Result<(), StorageError>
{
    metadata::discard_candidate(root, &pending_name(partition, group))
}

// Not Clone: startup attaches this permission to exactly one owned log.
pub(crate) struct RecoveredProgress {
    pub(crate) epoch: Epoch,
    pub(crate) next: [Offset; GROUP_COUNT as usize],
}
