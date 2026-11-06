use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdError {
    OutOfRange { kind: &'static str, value: u64, limit: u64 },
    ZeroEpoch,
    Exhausted(&'static str),
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfRange { kind, value, limit } => {
                write!(f, "{kind} {value} must be below {limit}")
            }
            Self::ZeroEpoch => write!(f, "epoch must be nonzero"),
            Self::Exhausted(kind) => write!(f, "{kind} has no representable successor"),
        }
    }
}

impl Error for IdError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    HeaderChecksum { stored: u32, computed: u32 },
    Magic,
    Version(u16),
    HeaderLength(u16),
    Reserved,
    LengthOverflow,
    TooLarge { length: usize, limit: usize },
    Identity(IdError),
    PayloadChecksum { stored: u32, computed: u32 },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeaderChecksum { stored, computed } => {
                write!(f, "header CRC {stored:08x} differs from {computed:08x}")
            }
            Self::Magic => write!(f, "wrong format magic"),
            Self::Version(v) => write!(f, "unsupported format version {v}"),
            Self::HeaderLength(n) => write!(f, "unsupported header length {n}"),
            Self::Reserved => write!(f, "reserved bits must be zero"),
            Self::LengthOverflow => write!(f, "record length is not representable"),
            Self::TooLarge { length, limit } => {
                write!(f, "payload length {length} exceeds {limit}")
            }
            Self::Identity(_) => write!(f, "invalid stored identity"),
            Self::PayloadChecksum { stored, computed } => {
                write!(f, "payload CRC {stored:08x} differs from {computed:08x}")
            }
        }
    }
}

impl Error for FormatError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(source) => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum DirectoryError {
    Busy(PathBuf),
    Io { operation: &'static str, path: PathBuf, source: io::Error },
}

impl fmt::Display for DirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy(path) => write!(f, "directory already owned: {}", path.display()),
            Self::Io { operation, path, .. } => {
                write!(f, "{operation}: {}", path.display())
            }
        }
    }
}

impl Error for DirectoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Busy(_) => None,
        }
    }
}
