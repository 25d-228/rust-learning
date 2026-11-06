# Chapter 12 — Lease expiration, retries, and stale-token fencing
## Part 1 — Attempts, deadlines, and a bounded retry window

> **Learning objectives**
> Reissue unfinished records without allocating a second delivery slot. Distinguish a record from an attempt to process it. Make acknowledgement authority depend on a complete token, an owner-observed deadline, and a persisted startup epoch.

**Implementation time:** 4.5 hours for all three parts together: 3.0 for mechanisms and integration, 1.5 for deadline/interleaving checks and diagnosis. Reading is separate. [Part 2 — Clocked reads and owner integration](https://25d-228.github.io/rust-learning/chapter-12-part-02.html) connects the state machine to actual record copies and commands. [Part 3 — Deterministic expiry and restart checks](https://25d-228.github.io/rust-learning/chapter-12-part-03.html) provides complete tests and an executable demonstration.

**Prerequisite code:** complete all three parts of [Chapter 11](https://25d-228.github.io/rust-learning/chapter-11.html), including `delivery/window.rs`, `delivery/groups.rs`, `worker.rs`, and `tests/delivery.rs`. Keep Chapter 10's `StorageSession` and startup epoch. A window has exclusive completed and issued ends, a fixed width, and no payload ownership. A partition owner alone changes its windows and reads only through its synchronized visible end. Those rules remain unchanged.

This chapter supplies a nine-file overlay under `book/media/chapter-12/`. New files are `src/delivery/clock.rs`, `src/delivery/lease.rs`, `src/delivery/leased_groups.rs`, `src/delivery/lease/tests.rs`, and `examples/lease_probe.rs`, all under `crates/broker-core/`. Replace `src/delivery/mod.rs` and `src/worker.rs` with their complete listings. In `src/storage/segment.rs`, replace only the `ReadLimits` implementation with the complete small implementation in Part 2. Append Part 3's additions to `tests/delivery.rs`; keep its earlier tests and helpers. The overlay contains full resulting files for reference. No dependencies, manifests, record bytes, or metadata format change.

For newly created or fully replaced files, concatenate the listings bearing the same filename in order. Some fragments continue an open `impl` or `match`; none are separate alternative implementations. Finish all three parts before building the integrated crate.

## A lost reply cannot be repaired by forgetting issuance

A group receives offset 0. The owner records it as outstanding and sends a reply containing the payload. The consumer disappears. Perhaps the reply never arrived; perhaps the consumer performed its external effect and disappeared before acknowledging. The broker cannot distinguish those situations from silence.

Chapter 11 deliberately left the slot occupied. Freeing it merely because the receiver vanished would erase the unfinished gap and permit later offsets to crowd it out. Keeping it forever, however, would leave a finite window permanently stalled. We need another **attempt** at the same logical offset, not another slot and not an invented completion.

A lease gives an attempt a deadline. An outstanding attempt becomes eligible for another delivery when the owner's clock reaches that deadline. Reissuing increases the attempt's generation and replaces its deadline. The stored record and its logical offset do not change. A token from the previous attempt can no longer complete the replacement.

This is a pull design. There is no timer thread moving expired positions to another queue. A delivery request scans the bounded window for eligible retries; an acknowledgement checks expiry directly. With no consumer polling, expired records simply remain outstanding. Expiration does not run consumer code, undo its effects, revoke its memory, or prove that it stopped.

| Owner time in milliseconds | Operation | Current offset-0 attempt | Completed end |
| --- | --- | --- | ---: |
| 0 | Deliver offset 0 | Generation 1, deadline 10 | 0 |
| 9 | No acknowledgement yet | Generation 1, still usable | 0 |
| 10 | Acknowledge generation 1 | Refuse: expired | 0 |
| 10 | Poll and reissue offset 0 | Generation 2, deadline 20 | 0 |
| 10 | Acknowledge generation 1 again | Refuse: stale generation | 0 |
| 11 | Acknowledge generation 2 | Complete and retire offset 0 | 1 |

The order of the last two acknowledgements matters only to their classification, not to whether an old attempt can complete the new one. A lost successful acknowledgement can also produce a retry; the broker's fencing is not exactly-once execution of an external side effect.

## Two independent kinds of progress

Retain the previous window notation. Let $C$ be the completed exclusive end, $D$ the issued exclusive end, $V$ the synchronized visible end, and $W$ the window width. The number of occupied positions is $N$:

$$
C \le D \le V, \qquad N = D-C \le W.
$$

Let $a_o$ be the current attempt generation for active offset $o$, and $d_o$ its deadline in process-local integer milliseconds. A new offset begins at generation 1. A retry changes the attempt but not the record interval:

$$
a'_o = a_o + 1, \qquad C'=C,\quad D'=D,\quad N'=N.
$$

Both additions that create authority are checked. If $t_i$ is the clock sample at installation and $\ell$ the configured lease length, then:

$$
d_o = t_i + \ell.
$$

For an outstanding attempt with an exactly matching token, a new acknowledgement is admissible only when $t < d_o$. Retry eligibility uses the complementary boundary $t \ge d_o$. Equality therefore cannot simultaneously authorize the old attempt and a replacement. Neither comparison depends on whether a background expiry pass has happened.

Generation saturation is not a solution to overflow: repeatedly issuing generation `u64::MAX` would make different attempts indistinguishable. Wrapping to zero would reuse old authority. Return an explicit error instead. [Checked integer addition](https://doc.rust-lang.org/std/primitive.u64.html#method.checked_add) returns `None` at overflow; the implementation uses that result instead of silently choosing either policy.

## Give the refusal paths concrete types

Create `crates/broker-core/src/delivery/lease.rs`. A window error is still a window error, and a clock error still retains its cause. `From` implementations allow `?` to preserve those distinctions. Deadline exhaustion and attempt exhaustion are not storage corruption and do not mean that an already published record was rolled back.

```rust title="crates/broker-core/src/delivery/lease.rs"
use std::error::Error;
use std::fmt;

use crate::types::{Epoch, GroupId, Offset, PartitionId};
use super::clock::ClockError;
use super::groups::GroupState;
use super::window::{Slot, Window, WindowError, validate_width};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseError {
    Window(WindowError),
    Clock(ClockError),
    LeaseMillis(u64),
    RecordLimit(usize),
    DeadlineExhausted,
    GenerationExhausted(Offset),
}
impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Window(error) => write!(f, "lease window: {error}"),
            Self::Clock(error) => write!(f, "lease time: {error}"),
            Self::LeaseMillis(value) => write!(f, "lease {value}ms is outside 1..=60000ms"),
            Self::RecordLimit(value) => write!(f, "lease batch {value} is outside 1..=1024"),
            Self::DeadlineExhausted => write!(f, "lease deadline cannot be represented"),
            Self::GenerationExhausted(offset) => write!(f,
                "attempt generation exhausted at offset {}", offset.get()),
        }
    }
}
impl Error for LeaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Window(error) => Some(error),
            Self::Clock(error) => Some(error),
            _ => None,
        }
    }
}
impl From<WindowError> for LeaseError {
    fn from(error: WindowError) -> Self { Self::Window(error) }
}
impl From<ClockError> for LeaseError {
    fn from(error: ClockError) -> Self { Self::Clock(error) }
}

```

`LeaseError::RecordLimit` protects the internal planner independently of the storage reader. The public path already receives a validated `ReadLimits`, but the state machine's own contract remains explicit and testable. Configuration errors are checked before allocation.

The configured duration is a positive integer number of milliseconds, capped here at 60,000. This is a bounded course policy, not a requirement of lease algorithms. The clock adapter and the effect of rounding to millisecond ticks are explained in Part 2. The persisted `Epoch` is supplied by the startup session, not by the consumer.

```rust title="crates/broker-core/src/delivery/lease.rs"
#[derive(Debug, Clone, Copy)]
pub struct LeaseConfig {
    pub(crate) epoch: Epoch,
    width: usize,
    millis: u64,
}
impl LeaseConfig {
    pub fn new(epoch: Epoch, width: usize, millis: u64) -> Result<Self, LeaseError> {
        validate_width(width)?;
        if !(1..=60_000).contains(&millis) { return Err(LeaseError::LeaseMillis(millis)); }
        Ok(Self { epoch, width, millis })
    }
}

```

## A token names one scoped attempt

Represent a token by the tuple $(e,p,q,o,a,d)$: startup epoch, partition, consumer group, record offset, attempt generation, and deadline. The token carries copied identifiers and integers; it neither borrows a payload nor keeps the owner alive.

| Field | Confusion it prevents |
| --- | --- |
| Epoch | An earlier process lifetime matching a newly reconstructed window |
| Partition | Offset 0 in one independent log being mistaken for offset 0 in another |
| Group | One group's completion advancing another group's window |
| Offset | A physical ring slot being mistaken for the record now occupying it |
| Generation | A previous delivery of the same record completing its replacement |
| Deadline | A caller extending the current attempt by changing only its expiry field |

Fields are public so a future protocol parser can reconstruct the value. Public construction does not make the token authoritative. The owner compares it against private state. Conversely, these integers are not a signature or a consumer identity: this course's trusted local clients could guess or copy them. Scope fencing is not authentication or authorization.

`AckOutcome::Applied` means this owner changed volatile completion state. `Duplicate` means the same accepted attempt remains represented above a gap. `AlreadyRetired` is only an interval classification: its detailed attempt history has already been forgotten. Neither classification causes a second completion, proves a particular external effect occurred, or reports a durable consumer checkpoint.

```rust title="crates/broker-core/src/delivery/lease.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseToken {
    pub epoch: Epoch,
    pub partition: PartitionId,
    pub group: GroupId,
    pub offset: Offset,
    pub generation: u64,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckRejection {
    WrongEpoch,
    WrongPartition,
    WrongGroup,
    NotIssued,
    StaleGeneration,
    WrongDeadline,
    Expired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    Applied,
    Duplicate,
    AlreadyRetired,
    Rejected(AckRejection),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    pub outcome: AckOutcome,
    pub retired: usize,
    pub state: GroupState,
}

```

## Store one attempt per occupied record, not one per retry

Keep Chapter 11's window for outstanding/completed status and prefix retirement. Add a separate fixed-size attempt array. Its physical position is determined by the logical offset:

$$
\operatorname{position}(o) = o \bmod W.
$$

This is **not** the previous window's private head-relative index. Each array has its own addressing rule. We never pass an attempt-array position into `Window`.

Why is the new addressing safe? All active offsets lie in a contiguous interval of at most $W$ positions. Two different active offsets differ by at least 1 and at most $W-1$. Equal remainders would require their difference to be a multiple of $W$, which is impossible in that range. Retired entries are cleared before their positions can be reused.

For width 3 with active interval `[5, 8)`, offsets 5, 6, and 7 use attempt positions 2, 0, and 1. Completing 7 does not clear position 1 while 5 remains unfinished. Reissuing 5 replaces the entry at position 2 without inserting another entry. A later new offset 8 may use position 2 only after the prefix has advanced enough to admit it.

The optional attempt entry is retained for completed followers too. This lets us distinguish a matching duplicate from an older generation while the record is still above a gap. Clearing that entry immediately on completion would lose the evidence needed for this distinction.

```rust title="crates/broker-core/src/delivery/lease.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Attempt {
    generation: u64,
    deadline_ms: u64,
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct Selection {
    pub(crate) offset: Offset,
    generation: u64,
    retry: bool,
}

pub(crate) struct LeaseWindow {
    epoch: Epoch,
    partition: PartitionId,
    group: GroupId,
    window: Window,
    attempts: Box<[Option<Attempt>]>,
    millis: u64,
    retry_cursor: Offset,
    prefer_retry: bool,
}
impl LeaseWindow {
    pub(crate) fn new(partition: PartitionId, group: GroupId, start: Offset,
        config: LeaseConfig) -> Self
    {
        Self {
            epoch: config.epoch, partition, group,
            window: Window::new(start, config.width).expect("validated lease width"),
            attempts: vec![None; config.width].into_boxed_slice(),
            millis: config.millis, retry_cursor: start, prefer_retry: true,
        }
    }

    pub(crate) fn state(&self) -> GroupState {
        GroupState { epoch: self.epoch, partition: self.partition, group: self.group,
            window: self.window.state() }
    }

    fn position(&self, offset: Offset) -> usize {
        (offset.get() % self.attempts.len() as u64) as usize
    }

    fn attempt(&self, offset: Offset) -> Attempt {
        self.attempts[self.position(offset)].expect("every issued slot retains its current attempt")
    }

```

`LeaseWindow` is crate-visible, not a public state handle. The single partition owner contains it. `GroupState` is a copied observation of its frontiers, not a mutation capability. `Attempt` and `Selection` are small `Copy` values; copying a plan does not copy record bytes or create permission to bypass installation.

At the maximum width, four partitions and two groups contain $4 \times 2 \times 4096 = 32768$ logical positions. Each has a status slot and an optional attempt slot. This is a count, not a portable byte-size claim: enum layout and padding are not fixed by this source. Payload vectors, indexes, returned responses, clock ownership, allocator overhead, and file caches are outside this calculation.

## Select retries once, then interleave new work

The planner is read-only. It first obtains the same bounded new-record upper end that Chapter 11 used. Then it scans at most the currently occupied width, beginning at a rotating retry cursor. Completed positions are skipped. An outstanding position is eligible when its stored deadline is at or before the sampled time.

Collect at most the requested number of eligible retries. Merge those offsets with the contiguous new-record suffix, alternating whenever both categories are available. A local preference variable calculates the plan; the real preference is not changed yet. Generation increments are checked while planning, so exhaustion returns an error before any part of this plan is installed.

```rust title="crates/broker-core/src/delivery/lease.rs"
    // Read-only selection. The owner may use only a prefix after applying its byte limit.
    pub(crate) fn plan(&self, visible: Offset, count: usize, now: u64)
        -> Result<Vec<Selection>, LeaseError>
    {
        if !(1..=1024).contains(&count) { return Err(LeaseError::RecordLimit(count)); }
        let upper = self.window.read_upper(visible)?;
        let state = self.window.state();
        let start = if self.retry_cursor >= state.completed_end && self.retry_cursor < state.issued_end {
            (self.retry_cursor.get() - state.completed_end.get()) as usize
        } else { 0 };
        let mut retries = Vec::with_capacity(count.min(state.occupied));
        for step in 0..state.occupied {
            let relative = (start + step) % state.occupied;
            let offset = Offset::new(state.completed_end.get() + relative as u64);
            if self.window.status(offset) == Some(Slot::Outstanding)
                && self.attempt(offset).deadline_ms <= now
            {
                retries.push(offset);
                if retries.len() == count { break; }
            }
        }
        let mut retry_index = 0;
        let mut next = state.issued_end;
        let mut prefer_retry = self.prefer_retry;
        let mut selected = Vec::with_capacity(count);
        while selected.len() < count {
            let has_retry = retry_index < retries.len();
            let has_new = next < upper;
            if !has_retry && !has_new { break; }
            let retry = has_retry && (prefer_retry || !has_new);
            let (offset, generation) = if retry {
                let offset = retries[retry_index];
                retry_index += 1;
                let generation = self.attempt(offset).generation.checked_add(1)
                    .ok_or(LeaseError::GenerationExhausted(offset))?;
                (offset, generation)
            } else {
                let offset = next;
                next = next.next().expect("offset is below a representable exclusive upper end");
                (offset, 1)
            };
            selected.push(Selection { offset, generation, retry });
            prefer_retry = !retry;
        }
        Ok(selected)
    }

```

The loop containing `% state.occupied` executes only when that count is positive, so the empty window does not divide by zero. `start + step` is less than twice the maximum width. The offset addition stays inside the already represented issued interval. The casts are therefore bounded by window invariants rather than an assumption that arbitrary `u64` values fit `usize`.

The retry cursor matters even with alternating categories. Suppose a full three-slot window holds expired offsets 0, 1, and 2, and a client requests one record per poll. After reissuing 0, a slow client may wait long enough for 0 to expire again. Always scanning from the head would return 0 forever. Advancing the retry cursor gives the sequence 0, 1, 2, 0 instead.

The category preference also persists across successful single-record polls. With width 4, initially issue 0 and 1 while 2 and 3 are visible. Let both old attempts expire. The following service is possible without completing any record:

| Poll | Category | Offset | Occupied after installation | Next preference |
| --- | --- | ---: | ---: | --- |
| 1 | Retry | 0 | 2 | New |
| 2 | New | 2 | 3 | Retry |
| 3 | Retry | 1 | 3 | New |
| 4 | New | 3 | 4 | Retry |

No category can demand a new slot beyond capacity. Once full, only eligible retries can be served until a contiguous completed prefix retires. The preference is a scheduling rule, not a promise about elapsed latency or client fairness. It does not force consumers to poll, complete work, or supply a byte allowance large enough for the selected record.

A plan of four selections for this example is `[retry 0, new 2, retry 1, new 3]`. A storage byte limit may allow only the first selection. The unread suffix must not advance generations, issued end, or scheduling cursors. That is why planning has not mutated the window.

## Install only a successfully copied prefix

The same owner performs planning, reads, and installation synchronously. It does not handle another acknowledgement between those phases. This makes a borrowed prefix of the plan sufficient; there is no public plan object that another task can install later.

Before changing state, calculate the common deadline and construct every returned token. Validate the assumptions connecting each planned retry to its current attempt and each new offset to the contiguous suffix. These assertions describe internal owner invariants, not validation of arbitrary client tokens. A future asynchronous storage refactor would need to preserve exclusivity or add explicit plan revalidation; it must not carry this assumption over silently.

`window.admit` counts only newly issued offsets. Retrying an existing offset does not call completion, clear a slot, or advance the issued end. Its record stays outstanding while its attempt metadata is replaced.

```rust title="crates/broker-core/src/delivery/lease.rs"
    // Called synchronously by the same owner, without processing another command after plan.
    pub(crate) fn install(&mut self, selections: &[Selection], visible: Offset, now: u64)
        -> Result<Vec<LeaseToken>, LeaseError>
    {
        if selections.is_empty() { return Ok(Vec::new()); }
        let deadline_ms = now.checked_add(self.millis).ok_or(LeaseError::DeadlineExhausted)?;
        let mut next = self.window.state().issued_end;
        let mut new_count = 0;
        let mut tokens = Vec::with_capacity(selections.len());
        for selected in selections {
            if selected.retry {
                assert_eq!(self.window.status(selected.offset), Some(Slot::Outstanding));
                assert_eq!(self.attempt(selected.offset).generation.checked_add(1), Some(selected.generation));
                assert!(self.attempt(selected.offset).deadline_ms <= now);
            } else {
                assert_eq!(selected.offset, next);
                assert_eq!(self.attempts[self.position(next)], None);
                next = next.next().expect("plan checked the exclusive end");
                new_count += 1;
            }
            tokens.push(LeaseToken { epoch: self.epoch, partition: self.partition,
                group: self.group, offset: selected.offset,
                generation: selected.generation, deadline_ms });
        }
        // All recoverable failures occur before the attempts or scheduling cursors change.
        self.window.admit(new_count, visible)?;
        for (selected, token) in selections.iter().zip(&tokens) {
            let position = self.position(selected.offset);
            self.attempts[position] = Some(Attempt {
                generation: token.generation, deadline_ms: token.deadline_ms,
            });
            if selected.retry {
                self.retry_cursor = selected.offset.next().expect("issued offsets have successors");
            }
            self.prefer_retry = !selected.retry;
        }
        Ok(tokens)
    }

```

The deadline and token allocation happen before `Window::admit`. That method performs all its recoverable checks before mutating. After successful admission, the remaining operations are bounded assignments and checked-invariant assertions. Ordinary error returns cannot install half a plan. Normal allocator failure behavior is unchanged; this is not a recoverable out-of-memory API.

Installing an empty prefix returns no tokens and does not invent a deadline. In particular, a nonfitting first record cannot increment its generation merely because the owner considered it. The real retry cursor and category preference advance only for returned selections. A nonfitting chosen record is not silently skipped; increasing the byte allowance or deliberately changing the scheduler is a separate decision.

All tokens in one returned batch share the installation-time sample. That does not make the lease last until the consumer reads the reply. Channel delay and later network delay consume usable time, and a caller can receive a token that has already expired.

## Validate acknowledgement authority before touching the window

Validation follows a deliberate order. Check all three scope fields before even classifying an offset as retired. Then reject unissued positions. For active positions, require exact equality of generation and deadline with the stored attempt.

A matching completed follower yields `Duplicate`, even if time has since passed its deadline: the completion was already accepted, and this response does not accept it again. An outstanding matching attempt must still satisfy the strict time bound. The code never uses a deadline supplied in isolation by the caller.

After an applied acknowledgement, clear exactly the attempt entries whose corresponding window positions retired. A completed follower whose predecessor is still outstanding keeps its entry.

```rust title="crates/broker-core/src/delivery/lease.rs"
    fn unchanged(&self, outcome: AckOutcome) -> Ack {
        Ack { outcome, retired: 0, state: self.state() }
    }

    pub(crate) fn acknowledge(&mut self, token: LeaseToken, now: u64) -> Ack {
        let reject = |reason| self.unchanged(AckOutcome::Rejected(reason));
        if token.epoch != self.epoch { return reject(AckRejection::WrongEpoch); }
        if token.partition != self.partition { return reject(AckRejection::WrongPartition); }
        if token.group != self.group { return reject(AckRejection::WrongGroup); }
        let before = self.window.state();
        if token.offset < before.completed_end { return self.unchanged(AckOutcome::AlreadyRetired); }
        if token.offset >= before.issued_end { return reject(AckRejection::NotIssued); }
        let attempt = self.attempt(token.offset);
        if token.generation != attempt.generation { return reject(AckRejection::StaleGeneration); }
        if token.deadline_ms != attempt.deadline_ms { return reject(AckRejection::WrongDeadline); }
        // This describes an earlier accepted completion; it does not accept expired work anew.
        if self.window.status(token.offset) == Some(Slot::Completed) {
            return self.unchanged(AckOutcome::Duplicate);
        }
        if now >= attempt.deadline_ms { return reject(AckRejection::Expired); }
        let completion = self.window.complete(token.offset).expect("validated active offset");
        for value in before.completed_end.get()..completion.state.completed_end.get() {
            let position = self.position(Offset::new(value));
            self.attempts[position] = None;
        }
        Ack { outcome: AckOutcome::Applied, retired: completion.retired, state: self.state() }
    }
}

```

The small `reject` closure borrows `self` immutably to create an unchanged snapshot. Its last use precedes the mutable call to `window.complete`, so the borrow does not span the mutation. This is the last-use borrowing rule from Chapter 2 in a state-machine method, not an extra lock or a clone of the window.

The owner distinguishes these cases:

| Token and current state | Outcome | Completion or attempt mutation |
| --- | --- | --- |
| Wrong epoch, partition, or group | Rejected with the corresponding scope reason | None |
| Correct scope, offset below completed end | `AlreadyRetired` | None; old attempt history is no longer present |
| Offset at or beyond issued end | `Rejected(NotIssued)` | None |
| Active offset, generation differs | `Rejected(StaleGeneration)` | None |
| Active generation, deadline differs | `Rejected(WrongDeadline)` | None |
| Exact current token, completed follower | `Duplicate` | None |
| Exact current token, outstanding, time at/after deadline | `Rejected(Expired)` | None |
| Exact current token, outstanding, before deadline | `Applied` | Complete it; retire only a contiguous prefix |

The early retired classification cannot validate a forgotten generation or deadline. It is intentionally **not** an accepted acknowledgement. Retaining every old token to answer that question would make memory grow with history rather than window width. Wrong-scope tokens are still rejected before this classification.

At the exact deadline, acknowledgement-first returns `Expired` and leaves the slot eligible for retry. Retry-first installs a new generation, so the old acknowledgement returns `StaleGeneration`. Both orders preserve the unfinished record. Just before the deadline, a valid acknowledgement can complete it, preventing the subsequent retry scan from selecting it.

Add the test-module declaration. Part 3 supplies the entire child file; it can inspect the parent's private state without exposing test mutation hooks through the public library API.

```rust title="crates/broker-core/src/delivery/lease.rs"
#[cfg(test)]
mod tests;
```

## What survives and what must not be reused

The record remains in the existing segmented log. Attempt generation, deadline, retry cursor, and completion state remain volatile. Restart reconstructs an empty window at zero in this chapter and may repeat completed records. Chapter 13 will introduce persisted consumer progress without pretending that a timer can be restored across clock domains.

The startup epoch makes old tokens unusable after restart, even when offset, generation, and deadline happen to match a new attempt numerically. This requires a real lifecycle rule: do not destroy an owner and reconstruct its lease windows under the same epoch. For this course, owner failure stops the broker session; recovery publishes a new epoch before replacement owners are exposed. A future supervisor must preserve that rule, not reset attempts inside a still-live epoch.

The next part adds the clock and the read path. The mathematical boundary will stay the same; the important new decision is **where** the owner samples time and **when** it turns a copied record into an issued attempt.
