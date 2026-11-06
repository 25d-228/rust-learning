use std::fs::{self, File};
use std::path::Path;

use crate::budget::{DiskQuota, DiskState};
use crate::io::{open_file, read_prefix};
use crate::types::{Epoch, Offset, PARTITION_COUNT, PartitionId};
use super::error::StorageError;
use super::metadata;
use super::sync::SyncIo;

pub const FLOOR_LEN: usize = 48;
pub fn name(partition: PartitionId) -> String { format!("retention-p{}.floor", partition.get()) }
pub fn pending_name(partition: PartitionId) -> String { format!("{}.next", name(partition)) }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub partition: PartitionId,
    pub epoch: Epoch,
    pub floor: Offset,
    pub quota: DiskQuota,
}
impl Retention {
    pub fn encode(self) -> [u8; FLOOR_LEN] {
        let mut raw = [0; FLOOR_LEN];
        raw[..8].copy_from_slice(b"PBRF\x00\x01\x00\x30");
        raw[8..10].copy_from_slice(&self.partition.get().to_be_bytes());
        raw[12..20].copy_from_slice(&self.epoch.get().to_be_bytes());
        raw[20..28].copy_from_slice(&self.floor.get().to_be_bytes());
        raw[28..36].copy_from_slice(&self.quota.data_bytes().to_be_bytes());
        raw[36..44].copy_from_slice(&self.quota.reserve_bytes().to_be_bytes());
        let crc = crc32fast::hash(&raw[..44]);
        raw[44..].copy_from_slice(&crc.to_be_bytes());
        raw
    }

    pub fn decode(raw: &[u8]) -> Result<Self, StorageError> {
        if raw.len() != FLOOR_LEN { return Err(StorageError::Metadata("retained floor length is not 48")); }
        let crc = u32::from_be_bytes(raw[44..].try_into().expect("fixed CRC field"));
        if crc != crc32fast::hash(&raw[..44]) {
            return Err(StorageError::Metadata("retained floor checksum mismatch"));
        }
        if &raw[..8] != b"PBRF\x00\x01\x00\x30" || raw[10..12] != [0, 0] {
            return Err(StorageError::Metadata("invalid retained floor header"));
        }
        let partition = PartitionId::new(u16::from_be_bytes(raw[8..10].try_into().expect("fixed partition")))
            .map_err(|_| StorageError::Metadata("retained floor partition is out of range"))?;
        let epoch = Epoch::new(u64::from_be_bytes(raw[12..20].try_into().expect("fixed epoch")))
            .map_err(|_| StorageError::Metadata("retained floor epoch is zero"))?;
        let floor = Offset::new(u64::from_be_bytes(raw[20..28].try_into().expect("fixed floor")));
        let data = u64::from_be_bytes(raw[28..36].try_into().expect("fixed data quota"));
        let reserve = u64::from_be_bytes(raw[36..44].try_into().expect("fixed reserve quota"));
        Ok(Self { partition, epoch, floor, quota: DiskQuota::new(data, reserve)? })
    }

    pub(crate) fn validate(self, partition: PartitionId, epoch: Epoch) -> Result<Self, StorageError> {
        if self.partition != partition || self.epoch > epoch {
            return Err(StorageError::Metadata("retained floor has a different partition or a future epoch"));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retained {
    pub before: Offset,
    pub floor: Offset,
    pub removed_segments: usize,
    pub removed_bytes: u64,
    pub rotated: bool,
    pub disk: DiskState,
}

pub fn read(root: &Path, partition: PartitionId) -> Result<Retention, StorageError> {
    let path = root.join(name(partition));
    let info = fs::symlink_metadata(&path)
        .map_err(|e| StorageError::path("inspect required retained floor", &path, e))?;
    if !info.file_type().is_file() || info.len() != FLOOR_LEN as u64 {
        return Err(StorageError::Metadata("retained floor must be a regular 48-byte file"));
    }
    let mut file = File::open(&path).map_err(|e| StorageError::path("open retained floor", &path, e))?;
    let mut raw = [0; FLOOR_LEN];
    let count = read_prefix(&mut file, &mut raw)
        .map_err(|e| StorageError::path("read retained floor", &path, e))?;
    Retention::decode(&raw[..count])
}

pub(crate) fn is_name(candidate: &str) -> bool {
    (0..PARTITION_COUNT).any(|p| {
        let p = PartitionId::new(p).expect("fixed partitions");
        candidate == name(p) || candidate == pending_name(p)
    })
}

pub(crate) fn publish<S: SyncIo + ?Sized>(root: &Path, value: Retention, io: &mut S)
    -> Result<(), StorageError>
{
    metadata::publish_bytes(root, &name(value.partition), &pending_name(value.partition),
        &value.encode(), io, &mut |_| Ok(()))
}

pub(crate) fn synchronize<S: SyncIo + ?Sized>(root: &Path, partition: PartitionId, io: &mut S)
    -> Result<(), StorageError>
{
    let path = root.join(name(partition));
    let mut file = open_file(&path).map_err(|e| StorageError::path("reopen retained floor", &path, e))?;
    io.sync_retention(&mut file).map_err(|e| StorageError::path("synchronize retained floor", &path, e))
}

pub(crate) fn discard_pending(root: &Path, partition: PartitionId) -> Result<(), StorageError> {
    metadata::discard_candidate(root, &pending_name(partition))
}
