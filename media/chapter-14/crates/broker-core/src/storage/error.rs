use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::codec::SegmentHeader;
use crate::error::{DirectoryError, FormatError, IdError};
use crate::types::Offset;

#[derive(Debug)]
pub enum StorageError {
    Config(&'static str),
    Metadata(&'static str),
    InFile { path: PathBuf, source: Box<StorageError> },
    Epoch(IdError),
    Directory(DirectoryError),
    Topology(&'static str),
    Name(PathBuf),
    Identity { expected: SegmentHeader, found: SegmentHeader },
    Offset { position: u64, expected: Offset, found: Offset },
    Incomplete { position: u64, required: u64, available: u64 },
    Format { position: u64, source: FormatError },
    Io { operation: &'static str, position: u64, source: io::Error },
    PathIo { operation: &'static str, path: PathBuf, source: io::Error },
    RecordTooLarge { encoded: usize, segment_cap: u64 },
    SegmentFull,
    SegmentLimit,
    DiskFull,
    Range { start: Offset, upper: Offset, floor: Offset, end: Offset },
    Poisoned,
}

impl StorageError {
    pub(crate) fn io(operation: &'static str, position: u64, source: io::Error) -> Self {
        Self::Io { operation, position, source }
    }

    pub(crate) fn path(operation: &'static str, path: &std::path::Path, source: io::Error) -> Self {
        Self::PathIo { operation, path: path.to_path_buf(), source }
    }

    pub(crate) fn format(position: u64, source: FormatError) -> Self {
        Self::Format { position, source }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InFile { path, .. } => write!(f, "recovery scan: {}", path.display()),
            Self::Metadata(message) => write!(f, "authoritative metadata: {message}"),
            Self::Epoch(_) => write!(f, "cannot allocate a restart epoch"),
            Self::Directory(_) => write!(f, "cannot own the data directory"),
            Self::Config(message) => write!(f, "storage configuration: {message}"),
            Self::Topology(message) => write!(f, "segment topology: {message}"),
            Self::Name(path) => write!(f, "unexpected segment entry: {}", path.display()),
            Self::Identity { expected, found } => write!(f,
                "segment identity {found:?} differs from {expected:?}"),
            Self::Offset { position, expected, found } => write!(f,
                "byte {position}: expected offset {}, found {}", expected.get(), found.get()),
            Self::Incomplete { position, required, available } => write!(f,
                "byte {position}: need {required} bytes, have {available}"),
            Self::Format { position, .. } => write!(f, "invalid format at byte {position}"),
            Self::Io { operation, position, .. } => write!(f, "{operation} at byte {position}"),
            Self::PathIo { operation, path, .. } => write!(f, "{operation}: {}", path.display()),
            Self::RecordTooLarge { encoded, segment_cap } => write!(f,
                "{encoded}-byte record plus segment header exceeds cap {segment_cap}"),
            Self::SegmentFull => write!(f, "active segment has insufficient room; rotate first"),
            Self::DiskFull => write!(f, "logical disk admission is full"),
            Self::SegmentLimit => write!(f, "segment-count limit reached"),
            Self::Range { start, upper, floor, end } => write!(f,
                "requested [{}, {}) outside [{}, {})", start.get(), upper.get(), floor.get(), end.get()),
            Self::Poisoned => write!(f, "storage owner stopped after an uncertain operation"),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InFile { source, .. } => Some(source.as_ref()),
            Self::Epoch(source) => Some(source),
            Self::Directory(source) => Some(source),
            Self::Format { source, .. } => Some(source),
            Self::Io { source, .. } | Self::PathIo { source, .. } => Some(source),
            _ => None,
        }
    }
}
