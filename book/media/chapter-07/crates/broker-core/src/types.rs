use crate::error::IdError;

pub const PARTITION_COUNT: u16 = 4;
pub const GROUP_COUNT: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(u16);

impl PartitionId {
    pub fn new(value: u16) -> Result<Self, IdError> {
        if value >= PARTITION_COUNT {
            return Err(IdError::OutOfRange {
                kind: "partition", value: u64::from(value),
                limit: u64::from(PARTITION_COUNT),
            });
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u16 { self.0 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u8);

impl GroupId {
    pub fn new(value: u8) -> Result<Self, IdError> {
        if value >= GROUP_COUNT {
            return Err(IdError::OutOfRange {
                kind: "group", value: u64::from(value),
                limit: u64::from(GROUP_COUNT),
            });
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u8 { self.0 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(u64);

impl Offset {
    pub fn new(value: u64) -> Self { Self(value) }
    pub fn get(self) -> u64 { self.0 }

    pub fn next(self) -> Result<Self, IdError> {
        self.0.checked_add(1).map(Self).ok_or(IdError::Exhausted("offset"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(u64);

impl Epoch {
    pub fn new(value: u64) -> Result<Self, IdError> {
        if value == 0 { return Err(IdError::ZeroEpoch); }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 { self.0 }

    pub fn next(self) -> Result<Self, IdError> {
        self.0.checked_add(1).map(Self).ok_or(IdError::Exhausted("epoch"))
    }
}
