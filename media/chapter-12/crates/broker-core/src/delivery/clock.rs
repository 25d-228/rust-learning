use std::error::Error;
use std::fmt;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockError {
    Regressed,
    Exhausted,
    Poisoned,
}

impl fmt::Display for ClockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Regressed => write!(f, "lease clock moved backwards"),
            Self::Exhausted => write!(f, "lease clock milliseconds cannot be represented"),
            Self::Poisoned => write!(f, "manual clock mutex is poisoned"),
        }
    }
}
impl Error for ClockError {}

// Milliseconds in one process-local domain, never a Unix timestamp.
// Successful observations must be nondecreasing.
pub trait Clock: Send + Sync {
    fn now(&self) -> Result<u64, ClockError>;
}

pub struct MonotonicClock {
    origin: Instant,
}

impl MonotonicClock {
    pub fn new() -> Self { Self { origin: Instant::now() } }
}
impl Default for MonotonicClock {
    fn default() -> Self { Self::new() }
}
impl Clock for MonotonicClock {
    fn now(&self) -> Result<u64, ClockError> {
        let elapsed = Instant::now().checked_duration_since(self.origin)
            .ok_or(ClockError::Regressed)?;
        u64::try_from(elapsed.as_millis()).map_err(|_| ClockError::Exhausted)
    }
}

pub struct ManualClock {
    millis: Mutex<u64>,
}

impl ManualClock {
    pub fn new(millis: u64) -> Self { Self { millis: Mutex::new(millis) } }

    pub fn advance(&self, by: u64) -> Result<u64, ClockError> {
        let mut millis = self.millis.lock().map_err(|_| ClockError::Poisoned)?;
        let next = millis.checked_add(by).ok_or(ClockError::Exhausted)?;
        *millis = next;
        Ok(next)
    }
}
impl Clock for ManualClock {
    fn now(&self) -> Result<u64, ClockError> {
        Ok(*self.millis.lock().map_err(|_| ClockError::Poisoned)?)
    }
}
