use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::codec::{HEADER_LEN, MAX_PAYLOAD};
use crate::mailbox::AdmissionError;
use crate::storage::error::StorageError;

pub const APPEND_BYTES: usize = 8 * 1024 * 1024;
pub const RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const OWNER_SCRATCH_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct MemoryConfig {
    pub(crate) append_size: usize,
    pub(crate) append_count: usize,
    pub(crate) response_size: usize,
    pub(crate) response_count: usize,
    pub(crate) waiters: usize,
}
impl MemoryConfig {
    pub fn new(append_size: usize, append_count: usize, response_size: usize,
        response_count: usize, waiters: usize) -> Result<Self, StorageError>
    {
        if !(1..=MAX_PAYLOAD).contains(&append_size) || !(1..=4096).contains(&append_count)
            || !(HEADER_LEN..=RESPONSE_BYTES).contains(&response_size)
            || !(1..=4096).contains(&response_count) || !(1..=64).contains(&waiters)
            || append_size.checked_mul(append_count).is_none_or(|n| n > APPEND_BYTES)
            || response_size.checked_mul(response_count).is_none_or(|n| n > RESPONSE_BYTES)
        {
            return Err(StorageError::Config("memory pool sizes, counts, or waiter limit are invalid"));
        }
        Ok(Self { append_size, append_count, response_size, response_count, waiters })
    }
}
impl Default for MemoryConfig {
    fn default() -> Self {
        Self { append_size: MAX_PAYLOAD, append_count: 8,
            response_size: 2 * 1024 * 1024, response_count: 2, waiters: 8 }
    }
}

// Startup only. No payload allocation occurs in checkout, resize, or return.
pub(crate) fn allocate_bytes(size: usize) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(|source| StorageError::Allocation {
        operation: "preallocate payload buffer", source,
    })?;
    bytes.resize(size, 0);
    Ok(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolState {
    pub allocated_bytes: usize,
    pub checked_out_bytes: usize,
    pub peak_checked_out_bytes: usize,
    pub free_buffers: usize,
    pub block_bytes: usize,
}
struct PoolInner {
    free: Vec<Vec<u8>>,
    allocated: usize,
    checked_out: usize,
    peak: usize,
    block: usize,
}
#[derive(Clone)]
pub(crate) struct BufferPool {
    inner: Arc<Mutex<PoolInner>>,
    changed: Arc<Notify>,
}
impl BufferPool {
    pub(crate) fn new(size: usize, count: usize, ceiling: usize, changed: Arc<Notify>)
        -> Result<Self, StorageError>
    {
        let mut free = Vec::new();
        free.try_reserve_exact(count).map_err(|source| StorageError::Allocation {
            operation: "preallocate free-buffer entries", source,
        })?;
        let mut allocated = 0usize;
        for _ in 0..count {
            let bytes = allocate_bytes(size)?;
            allocated = allocated.checked_add(bytes.capacity())
                .filter(|n| *n <= ceiling)
                .ok_or(StorageError::Config("allocator-reported capacities exceed the pool ceiling"))?;
            free.push(bytes);
        }
        Ok(Self { inner: Arc::new(Mutex::new(PoolInner {
            free, allocated, checked_out: 0, peak: 0, block: size,
        })), changed })
    }
    pub(crate) fn state(&self) -> PoolState {
        let p = self.inner.lock().expect("buffer-pool mutex poisoned");
        PoolState { allocated_bytes: p.allocated, checked_out_bytes: p.checked_out,
            peak_checked_out_bytes: p.peak,
            free_buffers: p.free.len(), block_bytes: p.block }
    }
    pub(crate) fn take(&self, len: usize) -> Result<PoolBuffer, AdmissionError> {
        let mut p = self.inner.lock().expect("buffer-pool mutex poisoned");
        if len > p.block { return Err(AdmissionError::TooLarge); }
        let bytes = p.free.pop().ok_or(AdmissionError::MemoryFull)?;
        p.checked_out += bytes.capacity();
        p.peak = p.peak.max(p.checked_out);
        drop(p);
        let mut value = PoolBuffer { pool: self.clone(), bytes: Some(bytes), used: len };
        // A caller never sees an earlier user's contents, even on a partially filled request.
        value.as_mut_slice().fill(0);
        Ok(value)
    }
}

// No Clone, DerefMut<Vec<u8>>, or into_vec: a payload cannot escape its lifetime owner.
pub(crate) struct PoolBuffer {
    pool: BufferPool,
    bytes: Option<Vec<u8>>,
    used: usize,
}
impl PoolBuffer {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes.as_ref().expect("live pooled buffer")[..self.used]
    }
    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes.as_mut().expect("live pooled buffer")[..self.used]
    }
    pub(crate) fn set_len(&mut self, len: usize) {
        assert!(len <= self.bytes.as_ref().expect("live pooled buffer").len());
        self.used = len;
    }
}
impl std::fmt::Debug for PoolBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolBuffer").field("used", &self.used).finish_non_exhaustive()
    }
}
impl Drop for PoolBuffer {
    fn drop(&mut self) {
        if let Some(bytes) = self.bytes.take() {
            let mut p = self.pool.inner.lock().unwrap_or_else(|e| e.into_inner());
            p.checked_out -= bytes.capacity();
            p.free.push(bytes); // At most the original count; no growth allocation.
            drop(p);
            self.pool.changed.notify_waiters();
        }
    }
}

#[derive(Clone)]
pub(crate) struct WaitGate(Arc<Mutex<(usize, usize)>>);
impl WaitGate {
    pub(crate) fn new(limit: usize) -> Self { Self(Arc::new(Mutex::new((0, limit)))) }
    pub(crate) fn enter(&self) -> Result<WaitGuard, AdmissionError> {
        let mut state = self.0.lock().expect("waiter counter poisoned");
        if state.0 == state.1 { return Err(AdmissionError::WaitersFull); }
        state.0 += 1;
        Ok(WaitGuard(self.clone()))
    }
    pub(crate) fn used(&self) -> usize { self.0.lock().expect("waiter counter poisoned").0 }
}
pub(crate) struct WaitGuard(WaitGate);
impl Drop for WaitGuard {
    fn drop(&mut self) {
        self.0.0.lock().unwrap_or_else(|e| e.into_inner()).0 -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_not_payload_length_is_charged_and_returned() {
        let pool = BufferPool::new(64, 1, 128, Arc::new(Notify::new())).unwrap();
        let allocated = pool.state().allocated_bytes;
        let mut first = pool.take(3).unwrap();
        assert_eq!(pool.state().checked_out_bytes, allocated);
        let pointer = first.as_slice().as_ptr();
        first.as_mut_slice().copy_from_slice(b"cat");
        assert_eq!(pool.take(0).err(), Some(AdmissionError::MemoryFull));
        drop(first);
        let second = pool.take(3).unwrap();
        assert_eq!(second.as_slice().as_ptr(), pointer);
        assert_eq!(second.as_slice(), &[0, 0, 0]);
        drop(second);
        assert_eq!(pool.state().checked_out_bytes, 0);
        assert_eq!(pool.state().allocated_bytes, allocated);
    }
    #[test]
    fn peak_counts_capacity_and_survives_return() {
        let pool = BufferPool::new(64, 2, 256, Arc::new(Notify::new())).unwrap();
        let a = pool.take(1).unwrap();
        let first = pool.state().checked_out_bytes;
        assert_eq!(pool.state().peak_checked_out_bytes, first);
        let b = pool.take(0).unwrap();
        let peak = pool.state().allocated_bytes;
        assert_eq!(pool.state().peak_checked_out_bytes, peak);
        drop(a); drop(b);
        assert_eq!(pool.state().checked_out_bytes, 0);
        assert_eq!(pool.state().peak_checked_out_bytes, peak);
        let c = pool.take(2).unwrap();
        assert_eq!(pool.state().peak_checked_out_bytes, peak);
        drop(c);
    }
    #[test]
    fn impossible_buffer_requests_do_not_change_the_pool() {
        let pool = BufferPool::new(64, 1, 128, Arc::new(Notify::new())).unwrap();
        let before = pool.state();
        assert_eq!(pool.take(65).err(), Some(AdmissionError::TooLarge));
        assert_eq!(pool.state(), before);
    }
    #[test]
    fn startup_reports_capacity_overflow_and_refuses_over_budget_capacity() {
        assert!(matches!(allocate_bytes(usize::MAX), Err(StorageError::Allocation { .. })));
        assert!(BufferPool::new(64, 1, 63, Arc::new(Notify::new())).is_err());
        assert!(MemoryConfig::new(1024 * 1024, 9, 32, 1, 1).is_err());
        assert!(MemoryConfig::new(1, 1, 4 * 1024 * 1024, 2, 1).is_err());
        assert!(MemoryConfig::new(1, 1, 32, 1, 0).is_err());
    }
    #[test]
    fn ordinary_unwind_returns_the_owned_buffer() {
        let pool = BufferPool::new(64, 1, 128, Arc::new(Notify::new())).unwrap();
        let handle = pool.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _buffer = handle.take(64).unwrap();
            panic!("controlled unwind outside the pool mutex");
        }));
        assert!(result.is_err());
        assert_eq!(pool.state().checked_out_bytes, 0);
        assert_eq!(pool.state().free_buffers, 1);
    }
    #[test]
    fn waiter_limit_counts_live_guards_not_notifications() {
        let gate = WaitGate::new(1);
        let first = gate.enter().unwrap();
        assert_eq!(gate.enter().err().map(|_| AdmissionError::WaitersFull), Some(AdmissionError::WaitersFull));
        drop(first);
        let second = gate.enter().unwrap(); assert_eq!(gate.used(), 1); drop(second);
        assert_eq!(gate.used(), 0);
    }
}
