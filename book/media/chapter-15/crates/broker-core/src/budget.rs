use std::sync::{Arc, Mutex};

use crate::codec::HEADER_LEN;
use crate::storage::error::StorageError;

// Two checkpoint authorities/candidates, one floor pair, and one broker-meta pair.
// Charging the entire broker-meta pair to EACH partition is conservative.
pub const METADATA_HEADROOM: u64 = 8 * 48;
pub const ROTATION_HEADROOM: u64 = HEADER_LEN as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskQuota {
    data: u64,
    reserve: u64,
}
impl DiskQuota {
    pub fn new(data: u64, reserve: u64) -> Result<Self, StorageError> {
        if data < 3 * HEADER_LEN as u64 || reserve < METADATA_HEADROOM + ROTATION_HEADROOM
            || data.checked_add(reserve).is_none()
        {
            return Err(StorageError::Config("disk quota: data >= 96, reserve >= 416, representable total"));
        }
        Ok(Self { data, reserve })
    }
    pub fn data_bytes(self) -> u64 { self.data }
    pub fn reserve_bytes(self) -> u64 { self.reserve }
    pub fn total_bytes(self) -> u64 { self.data + self.reserve }
}
impl Default for DiskQuota {
    fn default() -> Self { Self { data: 126 * 1024 * 1024, reserve: 2 * 1024 * 1024 } }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskState {
    pub quota: DiskQuota,
    pub file_bytes: u64,
    pub reserved_bytes: u64,
    pub stopped: bool,
}
impl DiskState {
    pub fn charged_upper_bound(self) -> Option<u64> {
        // A failed write can leave an unmeasured prefix. Only recovery re-establishes exact lengths.
        if self.stopped { None }
        else { Some(self.file_bytes + self.reserved_bytes + METADATA_HEADROOM) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiskRefusal { Full, Stopped }

#[derive(Clone)]
pub(crate) struct DiskBudget(Arc<Mutex<DiskState>>);
impl DiskBudget {
    pub(crate) fn new(quota: DiskQuota, live: u64) -> Result<Self, StorageError> {
        if live > quota.data + ROTATION_HEADROOM {
            return Err(StorageError::Metadata("segment lengths exceed data quota plus one rotation header"));
        }
        Ok(Self(Arc::new(Mutex::new(DiskState {
            quota, file_bytes: live, reserved_bytes: 0, stopped: false,
        }))))
    }

    pub(crate) fn state(&self) -> DiskState {
        *self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn stop(&self) {
        self.0.lock().unwrap_or_else(|error| error.into_inner()).stopped = true;
    }

    fn reserve(&self, bytes: u64, maintenance: bool) -> Result<DiskPermit, DiskRefusal> {
        let mut state = self.0.lock().expect("disk ledger mutex poisoned");
        if state.stopped { return Err(DiskRefusal::Stopped); }
        let limit = state.quota.data + if maintenance { ROTATION_HEADROOM } else { 0 };
        let used = state.file_bytes + state.reserved_bytes;
        if used > limit || bytes > limit - used { return Err(DiskRefusal::Full); }
        state.reserved_bytes += bytes;
        Ok(DiskPermit { budget: self.clone(), bytes, active: true })
    }

    pub(crate) fn reserve_append(&self, encoded: usize) -> Result<DiskPermit, DiskRefusal> {
        let bytes = u64::try_from(encoded).ok()
            .and_then(|n| n.checked_add(HEADER_LEN as u64)).ok_or(DiskRefusal::Full)?;
        // The owner may rotate before this queued request; reserve its worst case now.
        self.reserve(bytes, false)
    }

    pub(crate) fn reserve_rotation(&self, maintenance: bool) -> Result<DiskPermit, DiskRefusal> {
        self.reserve(HEADER_LEN as u64, maintenance)
    }

    pub(crate) fn reclaimed(&self, bytes: u64) {
        let mut state = self.0.lock().expect("disk ledger mutex poisoned");
        state.file_bytes = state.file_bytes.checked_sub(bytes).expect("only owned live files reclaimed");
    }
}

// Non-Clone. Ownership follows the queued command, not its reply receiver.
pub(crate) struct DiskPermit {
    budget: DiskBudget,
    bytes: u64,
    active: bool,
}
impl DiskPermit {
    pub(crate) fn belongs_to(&self, budget: &DiskBudget) -> bool {
        self.active && Arc::ptr_eq(&self.budget.0, &budget.0)
    }

    pub(crate) fn settle(mut self, growth: u64) {
        assert!(growth <= self.bytes, "file growth exceeded its reservation");
        let mut state = self.budget.0.lock().expect("disk ledger mutex poisoned");
        state.reserved_bytes -= self.bytes;
        state.file_bytes += growth;
        self.active = false;
    }
}
impl Drop for DiskPermit {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.budget.0.lock().unwrap_or_else(|error| error.into_inner());
            state.reserved_bytes -= self.bytes;
        }
    }
}

#[cfg(test)]
mod tests;

pub(crate) mod memory;
pub use memory::{APPEND_BYTES, RESPONSE_BYTES, OWNER_SCRATCH_BYTES, MemoryConfig, PoolState};
