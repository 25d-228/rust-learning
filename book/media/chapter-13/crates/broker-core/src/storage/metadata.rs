use std::fs::{self, File};
use std::io;
use std::path::Path;

use crate::io::{create_file, read_prefix, write_fully};
use crate::types::{Epoch, GROUP_COUNT, PARTITION_COUNT};
use super::error::StorageError;
use super::segment::StorageConfig;
use super::sync::SyncIo;

pub const META_NAME: &str = "broker.meta";
pub const TEMP_NAME: &str = "broker.meta.next";
pub const META_LEN: usize = 48;

#[derive(Debug, Clone, Copy)]
pub struct Metadata {
    epoch: Epoch,
    config: StorageConfig,
}

impl Metadata {
    pub fn new(epoch: Epoch, config: StorageConfig) -> Self { Self { epoch, config } }
    pub fn epoch(self) -> Epoch { self.epoch }
    pub fn config(self) -> StorageConfig { self.config }

    pub fn encode(self) -> [u8; META_LEN] {
        let mut raw = [0; META_LEN];
        raw[..4].copy_from_slice(b"PBMD");
        raw[4..6].copy_from_slice(&1_u16.to_be_bytes());
        raw[6..8].copy_from_slice(&48_u16.to_be_bytes());
        raw[8..16].copy_from_slice(&self.epoch.get().to_be_bytes());
        raw[16..18].copy_from_slice(&PARTITION_COUNT.to_be_bytes());
        raw[18] = GROUP_COUNT;
        raw[20..28].copy_from_slice(&self.config.segment_bytes().to_be_bytes());
        raw[28..36].copy_from_slice(&self.config.index_stride().to_be_bytes());
        raw[36..40].copy_from_slice(&(self.config.max_segments() as u32).to_be_bytes());
        let crc = crc32fast::hash(&raw[..44]);
        raw[44..48].copy_from_slice(&crc.to_be_bytes());
        raw
    }

    pub fn decode(raw: &[u8]) -> Result<Self, StorageError> {
        if raw.len() != META_LEN { return Err(StorageError::Metadata("length is not 48")); }
        let stored = u32::from_be_bytes(raw[44..48].try_into().expect("fixed CRC field"));
        if stored != crc32fast::hash(&raw[..44]) {
            return Err(StorageError::Metadata("checksum mismatch"));
        }
        if &raw[..8] != b"PBMD\x00\x01\x00\x30" {
            return Err(StorageError::Metadata("unsupported magic, version, or header length"));
        }
        let partitions = u16::from_be_bytes(raw[16..18].try_into().expect("fixed partition count"));
        if partitions != PARTITION_COUNT || raw[18] != GROUP_COUNT {
            return Err(StorageError::Metadata("partition or group count differs"));
        }
        if raw[19] != 0 || raw[40..44].iter().any(|b| *b != 0) {
            return Err(StorageError::Metadata("reserved fields are nonzero"));
        }
        let epoch = Epoch::new(u64::from_be_bytes(raw[8..16].try_into().expect("fixed epoch")))
            .map_err(StorageError::Epoch)?;
        let segment_bytes = u64::from_be_bytes(raw[20..28].try_into().expect("fixed segment cap"));
        let stride = u64::from_be_bytes(raw[28..36].try_into().expect("fixed stride"));
        let count = u32::from_be_bytes(raw[36..40].try_into().expect("fixed segment count"));
        let count = usize::try_from(count)
            .map_err(|_| StorageError::Metadata("segment count is not representable"))?;
        let config = StorageConfig::new(segment_bytes, stride, count)?;
        Ok(Self { epoch, config })
    }
}

pub fn read(root: &Path) -> Result<Metadata, StorageError> {
    let path = root.join(META_NAME);
    let info = fs::symlink_metadata(&path)
        .map_err(|e| StorageError::path("inspect authoritative metadata; do not initialize on failure", &path, e))?;
    if !info.file_type().is_file() || info.len() != META_LEN as u64 {
        return Err(StorageError::Metadata("expected a regular 48-byte file"));
    }
    let mut file = File::open(&path).map_err(|e| StorageError::path("open metadata", &path, e))?;
    let mut raw = [0; META_LEN];
    let count = read_prefix(&mut file, &mut raw)
        .map_err(|e| StorageError::path("read metadata", &path, e))?;
    Metadata::decode(&raw[..count])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationStep {
    TemporaryCreated,
    TemporaryWritten,
    TemporarySynced,
    Renamed,
    DirectorySynced,
}

pub(crate) fn discard_pending(root: &Path) -> Result<(), StorageError> {
    discard_candidate(root, TEMP_NAME)
}

pub(crate) fn discard_candidate(root: &Path, name: &str) -> Result<(), StorageError> {
    let path = root.join(name);
    match fs::symlink_metadata(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StorageError::path("inspect pending metadata", &path, e)),
        Ok(info) if info.file_type().is_file() => fs::remove_file(&path)
            .map_err(|e| StorageError::path("remove abandoned metadata candidate", &path, e)),
        Ok(_) => Err(StorageError::Metadata("pending metadata is not a regular file")),
    }
}

pub(crate) fn publish<S, H>(root: &Path, value: Metadata, io: &mut S, after: &mut H)
    -> Result<(), StorageError>
where S: SyncIo + ?Sized, H: FnMut(PublicationStep) -> io::Result<()> {
    publish_bytes(root, META_NAME, TEMP_NAME, &value.encode(), io, after)
}

fn mark<S, H>(io: &mut S, after: &mut H, target: &Path, step: PublicationStep)
    -> Result<(), StorageError>
where S: SyncIo + ?Sized, H: FnMut(PublicationStep) -> io::Result<()> {
    io.publication_step(target, step)
        .map_err(|e| StorageError::path("publication observer failed", target, e))?;
    after(step).map_err(|e| StorageError::path("interrupted metadata publication", target, e))
}

// Names are crate-owned constants or generated from validated fixed-range IDs.
pub(crate) fn publish_bytes<S, H>(root: &Path, current_name: &str, temp_name: &str,
    bytes: &[u8], io: &mut S, after: &mut H) -> Result<(), StorageError>
where S: SyncIo + ?Sized, H: FnMut(PublicationStep) -> io::Result<()> {
    let temp = root.join(temp_name);
    let current = root.join(current_name);
    let mut file = create_file(&temp)
        .map_err(|e| StorageError::path("create metadata candidate", &temp, e))?;
    mark(io, after, &current, PublicationStep::TemporaryCreated)?;
    write_fully(&mut file, bytes)
        .map_err(|e| StorageError::path("write metadata candidate", &temp, e))?;
    mark(io, after, &current, PublicationStep::TemporaryWritten)?;
    io.sync_metadata(&mut file)
        .map_err(|e| StorageError::path("synchronize metadata candidate", &temp, e))?;
    mark(io, after, &current, PublicationStep::TemporarySynced)?;
    drop(file);
    fs::rename(&temp, &current)
        .map_err(|e| StorageError::path("replace authoritative metadata", &current, e))?;
    mark(io, after, &current, PublicationStep::Renamed)?;
    io.sync_directory(root)
        .map_err(|e| StorageError::path("synchronize metadata directory", root, e))?;
    mark(io, after, &current, PublicationStep::DirectorySynced)?;
    Ok(())
}
