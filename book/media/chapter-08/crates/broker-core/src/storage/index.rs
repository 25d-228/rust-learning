use crate::types::Offset;
use super::error::StorageError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub offset: Offset,
    pub position: u64,
}

pub struct SparseIndex {
    base: Offset,
    stride: u64,
    entries: Vec<IndexEntry>,
}

impl SparseIndex {
    pub fn new(base: Offset, stride: u64) -> Result<Self, StorageError> {
        if !(1..=4096).contains(&stride) {
            return Err(StorageError::Config("index stride must be in 1..=4096"));
        }
        Ok(Self { base, stride, entries: Vec::new() })
    }

    // Only the validated, contiguous segment scanner/appender calls this.
    pub(crate) fn record(&mut self, offset: Offset, position: u64) {
        assert!(offset >= self.base);
        if (offset.get() - self.base.get()) % self.stride == 0 {
            if let Some(previous) = self.entries.last() {
                assert!(previous.offset < offset && previous.position < position);
            }
            self.entries.push(IndexEntry { offset, position });
        }
    }

    pub fn entries(&self) -> &[IndexEntry] { &self.entries }

    pub fn predecessor(&self, target: Offset) -> Option<IndexEntry> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            let middle = low + (high - low) / 2;
            if self.entries[middle].offset <= target {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low.checked_sub(1).map(|i| self.entries[i])
    }
}
