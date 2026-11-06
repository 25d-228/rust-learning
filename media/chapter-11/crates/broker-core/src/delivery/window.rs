use std::error::Error;
use std::fmt;

use crate::types::Offset;

pub const MAX_WINDOW: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    Empty,
    Outstanding,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowError {
    Capacity(usize),
    Full { requested: usize, free: usize },
    RegressedVisible { issued: Offset, visible: Offset },
    BeyondVisible { end: Offset, visible: Offset },
    OffsetExhausted,
    NotIssued { offset: Offset, issued: Offset },
}

impl fmt::Display for WindowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity(width) => write!(f, "window width {width} is outside 1..={MAX_WINDOW}"),
            Self::Full { requested, free } => write!(f, "need {requested} window slots; only {free} free"),
            Self::RegressedVisible { issued, visible } => write!(f,
                "visible end {} is below issued end {}", visible.get(), issued.get()),
            Self::BeyondVisible { end, visible } => write!(f,
                "delivery end {} exceeds visible end {}", end.get(), visible.get()),
            Self::OffsetExhausted => write!(f, "delivery offset successor cannot be represented"),
            Self::NotIssued { offset, issued } => write!(f,
                "offset {} has not been issued; issued end is {}", offset.get(), issued.get()),
        }
    }
}

impl Error for WindowError {}

pub(crate) fn validate_width(width: usize) -> Result<(), WindowError> {
    if (1..=MAX_WINDOW).contains(&width) { Ok(()) }
    else { Err(WindowError::Capacity(width)) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowState {
    pub completed_end: Offset,
    pub issued_end: Offset,
    pub occupied: usize,
    pub capacity: usize,
}

impl WindowState {
    pub fn free(self) -> usize { self.capacity - self.occupied }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionKind {
    Applied,
    Duplicate,
    AlreadyRetired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completion {
    pub kind: CompletionKind,
    pub retired: usize,
    pub state: WindowState,
}

#[derive(Debug)]
pub struct Window {
    slots: Box<[Slot]>,
    head: usize,
    len: usize,
    completed_end: Offset,
    issued_end: Offset,
}

impl Window {
    pub fn new(start: Offset, width: usize) -> Result<Self, WindowError> {
        validate_width(width)?;
        Ok(Self {
            slots: vec![Slot::Empty; width].into_boxed_slice(),
            head: 0, len: 0, completed_end: start, issued_end: start,
        })
    }

    pub fn state(&self) -> WindowState {
        WindowState {
            completed_end: self.completed_end, issued_end: self.issued_end,
            occupied: self.len, capacity: self.slots.len(),
        }
    }

    fn physical(&self, relative: usize) -> usize {
        assert!(relative < self.slots.len());
        let until_wrap = self.slots.len() - self.head;
        if relative < until_wrap { self.head + relative }
        else { relative - until_wrap }
    }

    pub fn status(&self, offset: Offset) -> Option<Slot> {
        if offset < self.completed_end || offset >= self.issued_end { return None; }
        let relative = usize::try_from(offset.get() - self.completed_end.get())
            .expect("an active distance is at most the window width");
        Some(self.slots[self.physical(relative)])
    }

    fn check_visible(&self, visible: Offset) -> Result<(), WindowError> {
        if visible < self.issued_end {
            Err(WindowError::RegressedVisible { issued: self.issued_end, visible })
        } else { Ok(()) }
    }

    pub fn read_upper(&self, visible: Offset) -> Result<Offset, WindowError> {
        self.check_visible(visible)?;
        let available = visible.get() - self.issued_end.get();
        let count = available.min(self.state().free() as u64);
        let upper = self.issued_end.get().checked_add(count)
            .ok_or(WindowError::OffsetExhausted)?;
        Ok(Offset::new(upper))
    }

    pub fn admit(&mut self, count: usize, visible: Offset) -> Result<(), WindowError> {
        self.check_visible(visible)?;
        let free = self.state().free();
        if count > free { return Err(WindowError::Full { requested: count, free }); }
        let count64 = u64::try_from(count).map_err(|_| WindowError::OffsetExhausted)?;
        let end = Offset::new(self.issued_end.get().checked_add(count64)
            .ok_or(WindowError::OffsetExhausted)?);
        if end > visible { return Err(WindowError::BeyondVisible { end, visible }); }
        // Every fallible preflight is above this line. No payloads are stored here.
        for relative in self.len..self.len + count {
            let position = self.physical(relative);
            assert_eq!(self.slots[position], Slot::Empty);
            self.slots[position] = Slot::Outstanding;
        }
        self.len += count;
        self.issued_end = end;
        Ok(())
    }

    pub fn complete(&mut self, offset: Offset) -> Result<Completion, WindowError> {
        if offset < self.completed_end {
            return Ok(Completion { kind: CompletionKind::AlreadyRetired,
                retired: 0, state: self.state() });
        }
        if offset >= self.issued_end {
            return Err(WindowError::NotIssued { offset, issued: self.issued_end });
        }
        let relative = usize::try_from(offset.get() - self.completed_end.get())
            .expect("an active distance is at most the window width");
        let position = self.physical(relative);
        match self.slots[position] {
            Slot::Completed => return Ok(Completion { kind: CompletionKind::Duplicate,
                retired: 0, state: self.state() }),
            Slot::Outstanding => self.slots[position] = Slot::Completed,
            Slot::Empty => unreachable!("every offset in the issued interval is occupied"),
        }
        let mut retired = 0;
        while self.len != 0 && self.slots[self.head] == Slot::Completed {
            self.slots[self.head] = Slot::Empty;
            self.head = if self.head + 1 == self.slots.len() { 0 } else { self.head + 1 };
            self.len -= 1;
            self.completed_end = self.completed_end.next()
                .expect("each admitted record has a representable exclusive end");
            retired += 1;
        }
        Ok(Completion { kind: CompletionKind::Applied, retired, state: self.state() })
    }
}
