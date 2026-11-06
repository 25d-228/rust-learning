# Chapter 11 — Bounded delivery and out-of-order completion
## Part 1 — A window that cannot advance through a gap

> **Learning objectives**
> Represent delivered work separately from completed progress. Build a fixed-width state machine that tolerates completion in any order, retires only a contiguous prefix, and never mistakes a wrapped memory position for a logical offset.

**Implementation time:** 5.0 hours for all three parts together: 3.5 for implementation and 1.5 for state-machine checks, integration, and diagnosis. Reading is separate. [Part 2 — Delivery on the partition owner](https://25d-228.github.io/rust-learning/chapter-11-part-02.html) connects the window to storage and commands. [Part 3 — Completion orders, bounded reads, and restart checks](https://25d-228.github.io/rust-learning/chapter-11-part-03.html) supplies the complete tests and executable demonstration.

**Prerequisite code:** finish all three parts of [Chapter 10](https://25d-228.github.io/rust-learning/chapter-10.html). Keep its recovered `StorageSession`, partition logs, typed identities, and file formats, together with Chapter 9's owner, bounded mailbox, synchronized visible end, and reply tickets. The foundation programs remain in the workspace. Chapter 4's ring is a useful conceptual prerequisite, but its private example types are not imported. We write a new ring whose policy preserves unfinished work instead of evicting samples.

This chapter's seven-file overlay is under `book/media/chapter-11/`. It replaces `crates/broker-core/src/{lib,worker}.rs` and adds `src/delivery/{mod,window,groups}.rs`, `tests/delivery.rs`, and `examples/delivery_probe.rs`. There are no new dependencies, manifest changes, or disk-format changes. All paths in listings are relative to the learner workspace. For each exact filename, concatenate its Rust listings in order; they give the complete resulting file. Finish all three parts before building the integrated crate.

## Completion is not the largest number observed

Four records have been handed to a consumer group at offsets 0, 1, 2, and 3. Processing offset 2 finishes first. Recording “next offset = 3” would skip unfinished offsets 0 and 1 after a restart. Recording only “one record completed” would lose which record it was. Neither answer identifies the safe contiguous prefix.

Instead, keep one state for every issued position not yet retired. A completion changes that position from **outstanding** to **completed**. Retirement changes a completed position at the head to **empty**, then repeats while the next position is also completed. A completed position beyond a gap cannot be reused.

| Action, without refilling | Completed end | Issued end | Occupied slots | Slots retired by this action |
| --- | ---: | ---: | ---: | ---: |
| Issue 0, 1, 2, 3 | 0 | 4 | 4 | 0 |
| Complete 2 | 0 | 4 | 4 | 0 |
| Complete 0 | 1 | 4 | 3 | 1 |
| Complete 3 | 1 | 4 | 3 | 0 |
| Complete 1 | 4 | 4 | 0 | 3 |

The last operation retires three positions, not just the newly completed one. Offsets 2 and 3 were already complete but could not be retired while 1 remained outstanding. “Retire” here means reuse a metadata slot; it does **not** mean delete a disk record or persist consumer progress.

Each group gets its own window. Group 0 completing a record says nothing about group 1. The groups share the partition's stored records, not a completion cursor.

## Three frontiers, one fixed capacity

Use exclusive ends throughout. Let $C$ be `completed_end`, the next offset not in the retired prefix. Let $D$ be `issued_end`, the first offset never issued by this window. Let $V$ be the owner's synchronized `visible_end`. Let $W$ be the chosen window width and $L$ its occupied-slot count. The invariants are:

$$
C \le D \le V
$$

$$
L = D - C, \qquad 0 \le L \le W
$$

The occupied interval is `[C, D)`. Every offset in it has exactly one outstanding or completed slot; positions outside it have no active slot. If $F$ is the number of free slots, then:

$$
F = W - L
$$

Completing a non-head position changes a state but none of these counters. Retiring a prefix of $r$ positions advances $C$ by $r$, decreases $L$ by $r$, and leaves $D$ unchanged. Issuing $k$ new positions advances $D$ and $L$ by $k$, leaving $C$ unchanged. These two transitions explain the arithmetic instead of merely asserting it.

The window holds metadata only. It does not own the consumers' payload buffers, a filesystem handle, a thread, or a timer. A single partition owner will mutate both group windows; no internal mutex is needed.

## Give every slot and refusal a name

Create `crates/broker-core/src/delivery/window.rs`. `Slot` is a small, copyable enum. `WindowError` describes rejected transitions, not storage failures. An out-of-window completion must not panic merely because a caller supplied an offset that was never issued. Exhaustive matching, introduced in Chapter 3, keeps each state explicit; see Rust's [match chapter](https://doc.rust-lang.org/book/ch06-02-match.html).

The 4,096-slot upper bound is a course policy, not a property of Rust or a claim about an optimal production setting. It bounds construction and the longest retirement loop. Invalid widths are rejected before allocation.

```rust title="crates/broker-core/src/delivery/window.rs"
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

```

`BeyondVisible` means the requested new end would expose unsynchronized work. `RegressedVisible` means the supplied visible end is already below issued work: that is an inconsistent caller state, not ordinary “no new data.” `OffsetExhausted` means an exclusive successor cannot be represented. `Full` does not mean the disk is full.

The error implements the standard traits directly. The validator is visible inside this crate so the delivery configuration can use the same width rule without allocating and discarding a temporary window.

```rust title="crates/broker-core/src/delivery/window.rs"
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

```

## Snapshots are not ownership of a window

A `WindowState` is a copyable observation. Copying it does not duplicate completion authority or issue any records. The actual `Window` will not implement `Clone`. Its private fields prevent callers from setting a counter independently of its slots.

`CompletionKind::Applied` means this call changed an outstanding slot. `Duplicate` means the slot is still occupied but was already completed. `AlreadyRetired` means the offset lies below the current completed end; no slot is changed. It is **not proof** that this process delivered that offset. A window constructed at a nonzero start assumes the earlier prefix. Later checkpoint loading must justify that start.

```rust title="crates/broker-core/src/delivery/window.rs"
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

```

The `retired` count reports how many slots became reusable, which may be zero, one, or several. The returned state is after the operation. A duplicate or already-retired response always reports zero newly retired slots.

## Allocate once and keep logical ends separate from memory positions

The `Box<[Slot]>` owns a fixed-length heap slice. Construction fills a vector, then transfers its elements into that slice. No normal transition pushes, resizes, or reallocates it. Rust's [boxed allocation documentation](https://doc.rust-lang.org/std/boxed/index.html) describes ownership and destruction; [`Vec::into_boxed_slice`](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.into_boxed_slice) describes the conversion. Neither makes allocator failure a `WindowError`; allocation failure follows the allocator/runtime behavior, not a recoverable admission refusal in this example.

`head` identifies the physical slot for `completed_end`. `len` counts occupied slots, including completed positions above a gap. A fresh window is empty even when its starting offset is nonzero.

```rust title="crates/broker-core/src/delivery/window.rs"
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

```

Let $H$ denote `head`, and let $j$ be an offset's distance from the completed end. For an active offset $o$:

$$
j = o - C, \qquad 0 \le j < L
$$

The physical position $p$ is conceptually $(H+j) \bmod W$. The implementation uses subtraction and a branch, avoiding a potentially overflowing addition before the remainder:

$$
p =
\begin{cases}
H+j & j < W-H \\
j-(W-H) & j \ge W-H
\end{cases}
$$

With width 4, head 1, and offset distance 3, there are `4 - 1 = 3` slots until wrap. Since 3 is not less than 3, the physical position is `3 - 3 = 0`. This is where offset 4 goes after offset 0 retires from a window that still holds 1, 2, and 3.

`status` first checks the logical interval. Only then is subtracting offsets safe, and only then do we know the distance fits `usize`: it is less than a width bounded by 4,096. A stale offset cannot alias a newly occupied physical slot just because both have the same remainder.

```rust title="crates/broker-core/src/delivery/window.rs"
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

```

`status` returns `None` for retired and never-issued offsets. Active offsets can return only `Outstanding` or `Completed`; `Empty` is the physical state used outside the active interval. The public observation method is useful for tests without exporting a mutable reference to the ring.

## Bound the next read without pretending it will fill the window

A delivery request may read at most the free slots and at most the visible suffix. Let $N$ be that maximum record count and $U$ the exclusive upper bound passed to storage:

$$
N = \min(V-D, F), \qquad U = D+N
$$

When `C=1`, `D=4`, `V=9`, and `W=4`, the occupied count is 3 and only one slot is free. Therefore `N=min(5,1)=1` and `U=5`. The reader may attempt `[4,5)`, not `[4,9)`.

Do not compute `C + W` unconditionally and clamp afterward. Near the maximum offset, that addition can overflow even when the actual visible range is small. Subtract within the validated interval first, choose the bounded distance, and use checked addition. Rust's [`u64::checked_add`](https://doc.rust-lang.org/std/primitive.u64.html#method.checked_add) returns `None` on overflow instead of silently wrapping.

```rust title="crates/broker-core/src/delivery/window.rs"
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

```

`read_upper` does not reserve anything. Storage may return fewer records because of its independent record or encoded-byte limit. Only the count actually returned will be admitted. A full window and an exhausted visible suffix both produce `upper == issued_end`; Part 2 will distinguish their response reasons without performing a file read.

## Commit issuance only after every preflight succeeds

`admit` records a successfully copied contiguous suffix beginning at `issued_end`. Its caller must not use it to reserve an arbitrary offset or skip a record. The checks precede all slot changes: visible end consistency, free capacity, representable successor, and the visible upper bound.

The loop spans relative positions `len..len+count`. These are exactly the currently empty positions following the occupied interval. Since `count <= capacity - len`, the loop endpoint cannot exceed capacity. The assertions defend internal invariants; ordinary full/invalid requests return an error before reaching them.

```rust title="crates/broker-core/src/delivery/window.rs"
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

```

For example, a window with three free slots and `issued_end=10` may permit a read up to 13. If the reader copies only offsets 10 and 11, call `admit(2, visible)`. The new issued end is 12, not 13, and the remaining slot is still available. Reserving the requested upper bound would invent an outstanding record whose bytes were never returned.

There is no fallible user-input operation after the mutation begins. An internal assertion failure can still unwind; the owner lifecycle handles that as owner failure, not a rollback guarantee. The state machine uses safe Rust throughout.

## Mark one completion and retire as far as the prefix permits

First classify the logical offset. A retired offset is a no-op. An offset at or above the issued end is rejected. For an active offset, distinguish a duplicate from a new completion, then inspect only the head.

Every loop iteration clears exactly one completed slot, moves the head by one with wrap, reduces `len`, and advances the completed end by one. The successor exists because a record was admitted only when its exclusive end could be represented. `u64::MAX` may be an exclusive end; it can never be issued as a record offset by this window.

```rust title="crates/broker-core/src/delivery/window.rs"
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
```

The loop stops at an outstanding slot or an empty window. It never searches for a larger completed offset and jumps to it. With completion order 2, 0, 3, 1, the final call sees the completed run 1, 2, 3 and clears all three.

A useful second trace includes refilling. Use `O` for outstanding, `C` for completed, and `-` for empty. Numbers in cells are logical offsets, not array indices.

| Action | Physical slots 0, 1, 2, 3 | Head | Completed end | Issued end |
| --- | --- | ---: | ---: | ---: |
| Issue four | `O0 O1 O2 O3` | 0 | 0 | 4 |
| Complete 2 | `O0 O1 C2 O3` | 0 | 0 | 4 |
| Complete 0 | `- O1 C2 O3` | 1 | 1 | 4 |
| Issue offset 4 | `O4 O1 C2 O3` | 1 | 1 | 5 |
| Complete 3 | `O4 O1 C2 C3` | 1 | 1 | 5 |
| Complete 1 | `O4 - - -` | 0 | 4 | 5 |

Offset 4 survives because its slot is outstanding. A mistaken implementation that clears every physical position up to the largest completed array index could erase it.

## What is bounded, and what is not

Admission takes time proportional to the number of newly issued positions. Status lookup is constant time. A completion can retire at most a window's width in one call. Across a sequence of successful admissions and retirements, each admitted position is cleared once, so the total retirement-loop work is proportional to the number of retired positions, not their completion order.

For four partitions and two groups per partition, let $s$ be `size_of::<Slot>()` in the actual build. The slot-array element storage is:

$$
M_{slots} = 4 \times 2 \times W \times s
$$

At width 256 this is `2,048 * s` bytes. Do not assume an enum's layout from its source spelling; inspect it when measuring. This equation excludes the window structs, allocator metadata, payload copies, sparse indexes, reply queues, and other runtime state. It is not a process resident-memory estimate.

Most importantly, completion does not destroy a payload that a client still owns. A client can retain old buffers after completing work and request more. This chapter bounds tracking slots and each read, not the total memory held by all clients. Later byte permits will connect those lifetimes to a global budget.

The pure window is now complete. Continue with [Part 2](https://25d-228.github.io/rust-learning/chapter-11-part-02.html) to give it two group identities and a real storage owner; Part 3 provides runnable checks rather than requiring an invented test harness.
