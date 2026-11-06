# Chapter 4 — Traits, generics, iterators, and bounded collections

**Implementation time:** 2.5 hours: 1.75 for the program and 0.75 for comparison tests, compiler experiments, and boundary diagnosis. Reading is separate. Keep the existing workspace and compatible toolchain; this chapter adds no dependency or edition requirement.

**Prerequisites:** [Chapter 1](https://25d-228.github.io/rust-learning/chapter-01.html) supplies the Cargo workspace; [Chapter 2](https://25d-228.github.io/rust-learning/chapter-02.html) supplies moves, borrowing, methods, and lifetime-bound views; [Chapter 3](https://25d-228.github.io/rust-learning/chapter-03.html) supplies enums, `Option`, `Result`, checked arithmetic, and error propagation. Retain those examples unchanged. Their private types are not library exports, and this program does not import their parser implementation.

> **Learning objectives**
> Write one scanner that accepts different concrete sources without copying the whole input. Implement a generic ring that returns rejected ownership, search a logically sorted index across physical wraparound, and distinguish a bound on stored entries from a bound on work or bytes. Explain where static dispatch, a trait object, a closure, and an owned allocation each enter the program.

A sparse index need not store every record. It can retain selected offset-to-position pairs, then answer “which stored offset is the greatest one no larger than this request?” The storage chapters will use that operation to choose a starting point for a validated forward read. Here we make the language and collection mechanics visible with small deterministic data. There are no files, sockets, threads, or durable records yet.

## Add a scanner without adding a crate

Create `crates/language-lab/examples/generic_scan.rs` in the workspace from Chapter 1. Every Rust panel labelled with that exact path belongs to this one file, in page order, with a blank line between panels. Some panels continue the same test module; the text identifies those boundaries. Other filenames denote separate diagnostic programs, not additions to the example. All new functions and fixtures appear here.

The assembled [reference example](https://25d-228.github.io/rust-learning/media/chapter-04/crates/language-lab/examples/generic_scan.rs) is also included under `book/media/chapter-04/`. It is a one-file overlay, not another Cargo project. Build it in your learner workspace or a scratch copy, not inside the book's media directory. No change to `src/lib.rs`, either Cargo manifest, or the previous three examples is needed.

Our six input entries have offsets `10, 12, 14, 16, 18, 20`. Their positions are `0, 32, 64, 96, 128, 160`. A position is a hypothetical byte location, not an offset in the Chapter 3 frame format. The scanner selects offsets whose remainder on division by four is two, retaining only the newest two selected entries. It should finish with offsets `14, 18`; offset `10` was selected and explicitly evicted. This deliberately lossy summary is **not** a reliable delivery queue.

## Give two sources one narrow interface

A **trait** names operations that an implementing type provides. A generic caller can require that trait without knowing the implementer's fields. This is more precise than accepting “anything with a method of this name”: an implementation must explicitly satisfy the trait. See the Rust Book's [trait definitions and implementations](https://doc.rust-lang.org/book/ch10-02-traits.html).

Begin with a small value and the source contract:

```rust title="crates/language-lab/examples/generic_scan.rs"
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    offset: u64,
    position: u64,
}

trait EntrySource {
    fn next_entry(&mut self) -> Option<Entry>;
}
```

`Entry` contains two integers and owns no payload allocation. Deriving `Copy` is appropriate for this metadata value; deriving `Clone` is also required by `Copy`. Copying it does not copy a record's bytes. `Debug`, `PartialEq`, and `Eq` serve the same diagnostic and assertion roles as in Chapter 3. The ring will be generic over other types, including non-`Copy` values.

`next_entry` takes `&mut self` because advancing a cursor changes source state. It returns an owned `Entry`, not a reference tied to that mutable borrow. The scanner can therefore ask for another entry while keeping the previous metadata. The trait's method declaration ends in a semicolon; each source supplies its body through `impl EntrySource for ...`.

For this exercise, `Some(entry)` means one item has been consumed; `None` means permanent exhaustion. An implementation must remain exhausted after its first `None`. The scanner checks increasing offsets itself. The Rust type system checks the method signature, but it does not prove those behavioral contracts. This interface has no “temporarily unavailable” or I/O-error state. A future file source needs an explicit fallible interface, not an invented convention that `None` also means a read failure.

### Borrow existing entries without borrowing each result

The first source holds a slice and a cursor:

```rust title="crates/language-lab/examples/generic_scan.rs"
struct SliceSource<'a> {
    entries: &'a [Entry],
    next: usize,
}

impl<'a> SliceSource<'a> {
    fn new(entries: &'a [Entry]) -> Self {
        Self { entries, next: 0 }
    }
}

impl EntrySource for SliceSource<'_> {
    fn next_entry(&mut self) -> Option<Entry> {
        let entry = self.entries.get(self.next).copied()?;
        self.next += 1;
        Some(entry)
    }
}
```

`SliceSource<'a>` does not own the array; `'a` connects the field to storage owned elsewhere. The constructor borrows that storage. `Self` in its implementation means `SliceSource<'a>`. In the trait implementation, `SliceSource<'_>` means the implementation works for the lifetime carried by each value; it is not a request to make a reference live forever.

`get(self.next)` returns `Option<&Entry>`. The [`copied` method on that option](https://doc.rust-lang.org/std/option/enum.Option.html#method.copied) turns a present reference into a copied `Entry`. The `?` operates on `Option` here: it returns `None` from this method when the cursor is at the end. That return occurs before incrementing `next`. After exhaustion, the cursor consequently stays at the same out-of-range position.

On a successful lookup, `next` was less than the slice length. Incrementing it reaches at most that length, so this increment cannot overflow `usize`. No new allocation occurs. A call returns a separate metadata value, while the source continues borrowing the original array for future calls.

### Generate equivalent entries without allocating an input array

The other source calculates one entry at a time:

```rust title="crates/language-lab/examples/generic_scan.rs"
struct GeneratedSource {
    next: u32,
    end: u32,
}

impl GeneratedSource {
    fn new(count: u32) -> Self {
        Self { next: 0, end: count }
    }
}

impl EntrySource for GeneratedSource {
    fn next_entry(&mut self) -> Option<Entry> {
        if self.next == self.end {
            return None;
        }
        let index = u64::from(self.next);
        self.next += 1;
        Some(Entry {
            offset: 10 + 2 * index,
            position: 32 * index,
        })
    }
}
```

`GeneratedSource::new(6)` starts with `next = 0` and `end = 6`; the end is exclusive. Let $i$ be the current index, a whole number from zero through five in this fixture. Its offset $o_i$ and position $p_i$ are:

$$
o_i = 10 + 2i, \qquad p_i = 32i.
$$

For $i=2$, the generated entry is offset 14 at position 64. These arithmetic rules describe the fixture, not a storage format. The generator stores only two counters regardless of the requested count.

The counters are `u32`, but arithmetic producing metadata uses `u64`. The largest emitted index is below `u32::MAX`; widening before multiplication keeps both formulas within `u64`. The increment is guarded by `next < end`: emitting the final permitted item can make `next == end`, but it cannot increment an already exhausted counter. This is a concrete bounded-arithmetic argument, not a claim that arbitrary offset addition is safe.

Both implementations return the same values for the six-entry fixture. They differ in ownership and materialization: one borrows an already resident array, while the other computes metadata on demand. Our scan should not need two versions of its loop.

## Read closures and iterators as ownership operations

A **closure** is a callable value written with vertical bars around its parameters. In `|word| word.len()`, `word` is the parameter and the expression after the second bar is the result. Braces introduce a multi-statement body. A closure may capture names from its surrounding scope; the mutable `mapped` counter below is one such capture. The Rust Book explains [closure capture and callable traits](https://doc.rust-lang.org/book/ch13-01-closures.html).

An **iterator** yields items through successive calls to `next`. Its associated type, written `Iterator::Item` in explanations or `Self::Item` inside the trait, identifies the yielded type for that implementation. The methods we use come from the standard prelude. `map` transforms yielded items, `take(n)` limits how many are requested through that adapter, and `collect` consumes the iterator to build a collection. Building the `map` adapter alone does not execute its mapping closure. See the [iterator introduction](https://doc.rust-lang.org/book/ch13-02-iterators.html) and the [`Iterator` API](https://doc.rust-lang.org/std/iter/trait.Iterator.html).

Add this complete check function; `main` will call it later:

```rust title="crates/language-lab/examples/generic_scan.rs"
fn iterator_ownership_checks() {
    let mut words = vec![String::from("red"), String::from("blue")];
    let lengths: Vec<usize> = words.iter().map(|word| word.len()).collect();
    assert_eq!(lengths, vec![3, 4]);
    assert_eq!(words.len(), 2);

    for word in words.iter_mut() {
        word.push('!');
    }
    let first: Vec<String> = words.into_iter().take(1).collect();
    assert_eq!(first, vec![String::from("red!")]);

    let mut mapped = 0;
    let prefix: Vec<u32> = (0..1000)
        .map(|value| {
            mapped += 1;
            value * 2
        })
        .take(3)
        .collect();
    assert_eq!(prefix, vec![0, 2, 4]);
    assert_eq!(mapped, 3);
}
```

Follow the types rather than reading the dots as an opaque pipeline:

| Expression | Item type | What happens to `words`? |
| --- | --- | --- |
| `words.iter()` | `&String` | It remains the owner and is shared-borrowed during iteration. |
| `words.iter_mut()` | `&mut String` | Each yielded reference allows exclusive access to that element. |
| `words.into_iter()` | `String` | The vector is consumed; its iterator owns the remaining elements. |

The first `collect` builds a **new** vector of two lengths. The annotation `Vec<usize>` identifies the destination collection, not a cast applied afterward. Once that iterator is consumed, its shared borrow is finished, so the next loop can mutate the words. `push('!')` appends a character to a string; this is not a ring push.

The last operation on `words` moves it into an owning iterator. `take(1)` yields the first `String`; it does not clone it. After collecting that one string, dropping the owning iterator drops the unconsumed `"blue!"` as well. `words` cannot be used afterward. The one-element result is bounded in length, but the original two-element input was already resident. Limiting output does not retroactively bound the input's allocation.

The integer range `0..1000` is different: it need not own a thousand-element vector. Mapping and taking three values yields `0, 2, 4`, and the counter ends at three. The `mapped` borrow is held by the closure until the chain finishes; inspect the counter afterward. A test will reverse the collection order and show why “collect everything, then take three” performs different work despite producing the same prefix.

### Choose the callable requirement that the loop needs

The scanner's selector will have bound `F: FnMut(&Entry) -> bool`. Read this as “`F` can be called repeatedly with a shared entry reference, returns a Boolean, and may mutate its captured state.” `Fn` permits repeated calls without mutable access to the closure's captured state. `FnOnce` permits consuming that state and consequently does not by itself permit a second call. A closure can implement more than one of these traits. `move` controls how values are captured; it does **not** by itself imply a closure can only run once. These distinctions follow the [closure trait rules](https://doc.rust-lang.org/reference/types/closure.html#call-traits-and-coercions).

Our counter selector changes a captured number, so requiring `Fn` would be needlessly restrictive. Accepting only `FnOnce` would not justify a loop that calls the selector again. The scanner takes `keep` by value and binds it as `mut` because calling an `FnMut` value may modify its state. Nothing requires a heap-allocated closure or a dynamic callback registry.

## Reserve space, or enforce a limit?

A vector's capacity is allocation space, not an admission policy. `Vec::with_capacity(3)` starts with length zero; it does not create three usable elements, and ordinary `push` can grow beyond three. A deque reservation is not a fixed bound either. The distinction is documented in [`Vec` capacity and reallocation](https://doc.rust-lang.org/std/vec/struct.Vec.html#capacity-and-reallocation) and [`VecDeque::with_capacity`](https://doc.rust-lang.org/std/collections/struct.VecDeque.html#method.with_capacity).

Choose collections by the operations and guarantees you need:

| Collection | Useful behavior here | Additional policy still required |
| --- | --- | --- |
| `Vec<Entry>` | Contiguous metadata; indexed access; appending to a sorted index. | Reject or evict at a logical bound; removing the first entry shifts later entries. |
| `VecDeque<Entry>` | Efficient front removal and back insertion; logical indexed access across a ring. | Reject or evict before growth; storage can occupy two physical slices. |
| `BTreeMap<u64, u64>` | Ordered keys and predecessor/range queries without a sorted input requirement. | Bound the number of entries; decide how duplicate keys and deletion behave. |
| The `Ring<T>` below | Exactly the configured number of slots after construction, with visible push/pop mechanics. | Decide whether a full push should block, fail, or trigger a separate eviction. |

The standard [collection selection guide](https://doc.rust-lang.org/std/collections/index.html#when-should-you-use-which-collection) is useful background, not a replacement for deciding the policy. We will use a deque and ordered map as independent comparisons in small tests. They do not implement our ring or binary search for us.

## Own fixed slots without requiring the elements to be cloneable

`Box<T>` owns a heap allocation and drops its contents on destruction. `Box<[Option<T>]>` therefore owns a slice of optional values. The slice length can be selected at runtime, but the box has no vector-style `push` that silently grows it. This is useful owned indirection, not shared ownership: moving the box transfers its one ownership responsibility. See [`Box`](https://doc.rust-lang.org/std/boxed/index.html).

The type parameter `T` names the stored element type. `Ring<Entry>` and `Ring<String>` are different concrete types generated from one definition. `impl<T> Ring<T>` supplies methods for any such element type; it does not require `T: Clone`, `Copy`, `Default`, or `Debug`. The derived debugging implementation is available when `T` supports debugging, but normal ring operations do not acquire that bound.

Add the storage, constructor, and read methods:

```rust title="crates/language-lab/examples/generic_scan.rs"
#[derive(Debug, PartialEq, Eq)]
struct ZeroCapacity;

#[derive(Debug)]
struct Ring<T> {
    slots: Box<[Option<T>]>,
    head: usize,
    len: usize,
}

impl<T> Ring<T> {
    fn new(capacity: usize) -> Result<Self, ZeroCapacity> {
        if capacity == 0 {
            return Err(ZeroCapacity);
        }
        let slots: Vec<Option<T>> = (0..capacity).map(|_| None).collect();
        Ok(Self {
            slots: slots.into_boxed_slice(),
            head: 0,
            len: 0,
        })
    }

    fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn len(&self) -> usize {
        self.len
    }

    fn physical(&self, logical: usize) -> usize {
        assert!(logical < self.capacity());
        let before_wrap = self.capacity() - self.head;
        if logical < before_wrap {
            self.head + logical
        } else {
            logical - before_wrap
        }
    }

    fn get(&self, logical: usize) -> Option<&T> {
        if logical >= self.len {
            return None;
        }
        self.slots[self.physical(logical)].as_ref()
    }
}
```

A unit-like struct such as `ZeroCapacity` has no fields. It still distinguishes this failure from an arbitrary string message. `Ring::new(0)` rejects the request before allocating. All positive capacities proceed through the ordinary allocator; allocation failure is **not** turned into `ZeroCapacity`. Like Chapter 3's `to_vec`, this constructor is not a recoverable-allocation interface.

`(0..capacity).map(|_| None)` creates one empty option per slot. The underscore ignores the range value; the iterator still executes the closure once per requested slot. The annotation tells `collect` to make `Vec<Option<T>>`. [`into_boxed_slice`](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.into_boxed_slice) consumes the vector and discards its excess vector capacity. The temporary vector may reserve more storage during construction; our fixed-slot promise describes the constructed ring, not an exact peak allocator-byte budget for that conversion.

Why not `vec![None; capacity]`? That repeated-value form requires the element type to implement `Clone`; `Option<T>` implements `Clone` only when `T` does. We do not need to clone a value to make empty slots. Calling the closure separately avoids imposing an unrelated restriction on the ring. A compiler exercise below demonstrates the difference with a non-cloneable element.

### Derive the mapping from logical order to physical slots

Let $C$ be the positive capacity, $h$ the physical index of the oldest occupied slot, and $n$ the number of occupied slots. Maintain:

$$
0 \le h < C, \qquad 0 \le n \le C.
$$

Logical index $i=0$ means the oldest value, regardless of where it sits in the allocation. For a present value, $0\le i<n$. Its physical slot is conceptually:

$$
p(i) = (h+i) \bmod C.
$$

Directly calculating `h + i` before the remainder can overflow for enormous integer operands. Our implementation instead computes $b=C-h$, the number of slots before the physical end, and uses:

$$
p(i) =
\begin{cases}
h+i, & i<b,\\
i-b, & i\ge b.
\end{cases}
$$

In the first branch the sum is less than `capacity`, so it fits. The second subtracts only when `logical >= before_wrap`. `physical` accepts only indexes below capacity; push calls it only after proving a free slot exists. The method is private because callers should use **logical** indexing, not depend on internal layout.

For $C=3$ and $h=1$, `before_wrap` is two. Logical indexes `0, 1, 2` map to physical slots `1, 2, 0`. A physically stored array `[22, 14, 18]` therefore represents the logical sequence `[14, 18, 22]`. Numeric labels here stand for entries with those offsets.

`get` first rejects indexes outside `len`. For an in-range index, [`as_ref`](https://doc.rust-lang.org/std/option/enum.Option.html#method.as_ref) views a `Some(T)` as `Some(&T)` without transferring ownership. The returned reference borrows the ring. It cannot remain in use across a mutation of the ring, even though the allocation never grows. Exclusive mutation, not only reallocation, is relevant to borrowing.

## Return rejected ownership; remove accepted ownership once

Add another implementation block for the same generic type:

```rust title="crates/language-lab/examples/generic_scan.rs"
impl<T> Ring<T> {
    fn try_push(&mut self, value: T) -> Result<(), T> {
        if self.len == self.capacity() {
            return Err(value);
        }
        let tail = self.physical(self.len);
        assert!(self.slots[tail].is_none());
        self.slots[tail] = Some(value);
        self.len += 1;
        Ok(())
    }

    fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let value = self.slots[self.head].take().expect("occupied head");
        self.head += 1;
        if self.head == self.capacity() {
            self.head = 0;
        }
        self.len -= 1;
        Some(value)
    }
}
```

`Result<(), T>` is intentional. A successful push returns the unit value `()` because the ring now owns the element. A full push returns `Err(value)`, transferring it back to the caller. The caller can retry, keep it elsewhere, or deliberately drop it; the ring must not silently destroy an unaccepted message. The full check precedes all mutation.

The insertion slot is the physical image of logical index `len`. Because a non-full ring has `len < capacity`, this is a valid free slot. Setting `Some(value)` moves the value there, and only then does the method increase occupancy. The assertion expresses an internal invariant: overwriting a nonempty tail would be an implementation error, not ordinary backpressure.

`pop` uses [`Option::take`](https://doc.rust-lang.org/std/option/enum.Option.html#method.take) to move the oldest value out while replacing its slot with `None`. It then advances the head, wraps at the physical end, and reduces occupancy. Merely decrementing `len` would leave an owned value in a supposedly free slot. Merely reading through a reference would not return ownership of a non-`Copy` element. `take` performs the necessary transition.

No custom destructor is needed. When a ring is dropped, its boxed slice drops every remaining `Some(T)`. Previously popped slots contain `None`, so they do not drop those values again. A returned rejection is owned by the caller; it was never added to the ring.

For capacity three, trace the following calls. The full push does **not** cause the subsequent pop; the caller requests that separately.

| Operation | Physical slots | Head | Length | Returned value |
| --- | --- | ---: | ---: | --- |
| construct | `[None, None, None]` | 0 | 0 | ring |
| push 10, 14, 18 | `[10, 14, 18]` | 0 | 3 | three successes |
| try to push 22 | `[10, 14, 18]` | 0 | 3 | rejected 22 |
| pop | `[None, 14, 18]` | 1 | 2 | owned 10 |
| retry 22 | `[22, 14, 18]` | 1 | 3 | success |

The logical contents after the retry are `[14, 18, 22]`. The storage address need not move, and the retained entries need not shift toward slot zero. Full and empty are unambiguous because `len` is explicit; a head index alone would not distinguish them.

## Make the scan generic, but make its policy concrete

The scanner's output will report how much it consumed and what it retained:

```rust title="crates/language-lab/examples/generic_scan.rs"
#[derive(Debug, PartialEq, Eq)]
enum ScanError {
    ZeroCapacity,
    OutOfOrder { previous: u64, next: u64 },
}

#[derive(Debug)]
struct ScanReport {
    visited: usize,
    selected: usize,
    evicted: usize,
    retained: Ring<Entry>,
}
```

`ScanError::ZeroCapacity` reports configuration failure. `OutOfOrder` records the previous and offending offsets without borrowing the input. These example errors need equality and diagnostic formatting for tests; they do not implement `std::error::Error` or a user-facing message chain. Chapter 3 supplied that pattern when a boundary needed it. Avoid implementing unused traits merely because another example did.

The `where` clause below places requirements after the parameter list. `S: EntrySource` permits source methods; `F: FnMut(&Entry) -> bool` permits repeated selector calls. Generic parameters ordinarily have an implicit `Sized` requirement, meaning the concrete size is known at compilation. `?Sized` relaxes that requirement for `S`, while the parameter remains `&mut S`, a reference. It will let the same function accept a `dyn EntrySource` later; it does not make the trait optional. See the [reference on `?Sized`](https://doc.rust-lang.org/reference/trait-bounds.html#sized).

Add the complete scan:

```rust title="crates/language-lab/examples/generic_scan.rs"
fn scan<S, F>(
    source: &mut S,
    max_items: usize,
    capacity: usize,
    mut keep: F,
) -> Result<ScanReport, ScanError>
where
    S: EntrySource + ?Sized,
    F: FnMut(&Entry) -> bool,
{
    let mut retained = Ring::new(capacity).map_err(|_| ScanError::ZeroCapacity)?;
    let mut previous = None;
    let mut visited = 0;
    let mut selected = 0;
    let mut evicted = 0;

    while visited < max_items {
        let entry = match source.next_entry() {
            Some(entry) => entry,
            None => break,
        };
        if let Some(previous) = previous {
            if entry.offset <= previous {
                return Err(ScanError::OutOfOrder {
                    previous,
                    next: entry.offset,
                });
            }
        }
        previous = Some(entry.offset);
        visited += 1;
        if keep(&entry) {
            selected += 1;
            if let Err(entry) = retained.try_push(entry) {
                retained.pop().expect("a full ring has an oldest entry");
                retained.try_push(entry).expect("one slot was released");
                evicted += 1;
            }
        }
    }
    Ok(ScanReport {
        visited,
        selected,
        evicted,
        retained,
    })
}
```

The input limit `max_items` counts entries visited, **not** entries selected. A predicate that rejects everything therefore cannot make the scanner walk indefinitely looking for an acceptable result. The loop also stops on exhaustion and does not ask for a lookahead item after reaching its limit. A zero work limit consumes no input; a zero capacity is an error even when the work limit is zero, and it is rejected before pulling anything.

`if let Some(previous) = previous` enters only when there is a preceding offset. The inner name shadows the outer optional value for that block; the outer option is still assigned afterward. Checking `<=`, rather than just `<`, rejects duplicates as well as descending offsets. The check precedes the predicate, so filtering cannot hide malformed input order. The validation applies to the entries visited by **this call**. It does not inspect an unread suffix or remember a prior call's final offset.

When `try_push` rejects an entry, `if let Err(entry)` extracts the returned value. The scanner then deliberately pops and drops the oldest selected entry and retries the push. There must be one free slot after that pop, so the retry failure would violate our own invariant. This is why the `expect` belongs here while a normal full push is a returned value, not a panic.

The `Copy` entry makes this policy cheap, but the ring's move contract remains valid for strings and other owners. Eviction is appropriate for this newest-samples demonstration; copying the same loop into a reliable work queue would lose accepted work. The later delivery window will retain occupied positions until its own completion rules permit reuse.

### Account for selection, retention, and work separately

Let $v$ be visited entries, $s$ selected entries, $r$ retained entries, and $e$ evicted entries in a successful report. With positive capacity $C$:

$$
s\le v\le \text{max\_items},\qquad r=\min(s,C),\qquad e=s-r.
$$

The code's `visited`, `selected`, and `evicted` fields correspond to $v$, $s$, and $e$; `retained.len()` is $r$. Counter increments remain bounded by `max_items`, which is a `usize`. For our six-entry input and capacity two:

| Offset visited | Selected? | Logical retained offsets | Total evicted |
| --- | --- | --- | ---: |
| 10 | yes | `[10]` | 0 |
| 12 | no | `[10]` | 0 |
| 14 | yes | `[10, 14]` | 0 |
| 16 | no | `[10, 14]` | 0 |
| 18 | yes | `[14, 18]` | 1 |
| 20 | no | `[14, 18]` | 1 |

The final report is $v=6$, $s=3$, $r=2$, $e=1$. Physically, capacity two now holds `[18, 14]` with head one. The selector is called six times even though only three entries are selected.

A bad third offset is a different path. The first two entries and the offending third entry have already been pulled when the error is returned. The selector runs only for the first two. The local ring is dropped, no partial report is returned, and the caller's source remains advanced past the bad entry. This function is **not transactional**, and retrying it on the same advanced source does not repeat the failed prefix.

A source or selector could itself be slow, allocate, or panic. Our item bound does not bound their internal work or wall-clock time. The scan allocates the ring before iterating, so construction also costs work proportional to capacity even when `max_items` is zero. These limitations are part of the interface, not reasons to replace the loop with an unbounded collection.

## Search logical order, not the allocation's order

For a requested key, a **predecessor** is the retained entry with the greatest offset less than or equal to that key. A missing exact key is normal: a sparse index is intentionally incomplete. The search below assumes strictly increasing offsets in logical ring order. `scan` establishes that for its result; `Ring<T>` alone does not enforce sortedness for arbitrary manually inserted values.

We search for the boundary after all offsets that are at most the key. Let $n$ be the logical length, and let `low` and `high` delimit the half-open candidate interval `[low, high)`. Initially that interval is `[0, n)`. During the search, entries before `low` satisfy the comparison and entries at or after `high` do not. The midpoint is:

$$
m = \text{low} + \left\lfloor\frac{\text{high}-\text{low}}{2}\right\rfloor.
$$

Subtracting before adding keeps the midpoint within the interval without overflowing `low + high`. If the midpoint offset is at most the key, the boundary lies after it; otherwise the boundary is no later than it. When `low == high`, `low` is the number of retained entries eligible for the predecessor. Zero means there is no predecessor; otherwise the answer is logical index `low - 1`.

Add the binary search, a linear reference, and a bounded diagnostic snapshot:

```rust title="crates/language-lab/examples/generic_scan.rs"
fn predecessor(entries: &Ring<Entry>, key: u64) -> Option<Entry> {
    let mut low = 0;
    let mut high = entries.len();
    while low < high {
        let middle = low + (high - low) / 2;
        let entry = entries.get(middle).expect("logical index is occupied");
        if entry.offset <= key {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    match low.checked_sub(1) {
        Some(index) => entries.get(index).copied(),
        None => None,
    }
}

fn linear_predecessor(entries: &Ring<Entry>, key: u64) -> Option<Entry> {
    let mut found = None;
    for logical in 0..entries.len() {
        let entry = *entries.get(logical).expect("logical index is occupied");
        if entry.offset <= key {
            found = Some(entry);
        }
    }
    found
}

fn snapshot(entries: &Ring<Entry>) -> Vec<Entry> {
    (0..entries.len())
        .map(|logical| *entries.get(logical).expect("logical index is occupied"))
        .collect()
}
```

`middle + 1` cannot exceed the original length because the midpoint is always inside a nonempty interval. `checked_sub(1)` handles the zero boundary without unsigned underflow. The search compares `entry.offset <= key` directly; it never computes `key + 1`, so `u64::MAX` is a valid request. A returned entry is copied metadata and no longer borrows the ring.

The linear reference deliberately visits every logical entry and remembers the latest qualifying one. It is slower but follows a visibly different loop. If both algorithms agree only on nonwrapped storage, that is not enough: they could still be comparing the wrong physical positions. The deque comparison later tests the mapping itself.

For logical entries `[14, 18, 22]` and key 19, the search evolves as follows:

| Before comparison | Middle | Offset read | Next interval |
| --- | ---: | ---: | --- |
| `[0, 3)` | 1 | 18 | `[2, 3)` |
| `[2, 3)` | 2 | 22 | `[2, 2)` |

The boundary is two, so logical index one gives offset 18. With head one, the two comparisons read physical slots two and zero. With key 13 the boundary becomes zero and returns `None`; with key 22 or a larger key it becomes three and returns offset 22. An empty ring skips the loop and returns `None` as well.

The interval shrinks by roughly half per iteration. For $n$ entries, a convenient upper bound on comparisons is $\lceil\log_2(n+1)\rceil$, including zero comparisons for an empty ring. The linear reference performs $n$ entry comparisons. These are algorithmic bounds, not measured timing results; small indexes may not benefit perceptibly from binary search.

`snapshot` collects at most `entries.len()` metadata values, never the complete source. It is useful for assertions and printing; the binary search does not allocate it. Collecting an already bounded result can be reasonable. In the demo, producing a vector of offsets from a snapshot is deliberately extra diagnostic allocation, not part of the scanner's steady-state storage guarantee.

> **Note**
> A `None` result below the oldest retained sample means **this retained index has no usable predecessor**. It does not prove a record is absent from the original input. Our demo evicted offset 10; querying key 13 cannot rediscover it. A production index needs a defined retained range or a fallback to the segment start. Do not turn “no retained sample” into “record does not exist.”

## Keep the fixtures independent of the two sources

Write the six entries explicitly rather than generating the expected array with the same arithmetic as the generator. That allows a generator mistake to disagree with the fixture. Add a small repeated-wrap check used by both the executable and a test:

```rust title="crates/language-lab/examples/generic_scan.rs"
fn fixture() -> [Entry; 6] {
    [
        Entry {
            offset: 10,
            position: 0,
        },
        Entry {
            offset: 12,
            position: 32,
        },
        Entry {
            offset: 14,
            position: 64,
        },
        Entry {
            offset: 16,
            position: 96,
        },
        Entry {
            offset: 18,
            position: 128,
        },
        Entry {
            offset: 20,
            position: 160,
        },
    ]
}

fn wrapped_lookup_checks() {
    let mut entries = Ring::new(3).unwrap();
    assert_eq!(predecessor(&entries, 0), None);
    for index in 0..40u64 {
        let entry = Entry {
            offset: 2 * index,
            position: 32 * index,
        };
        if let Err(entry) = entries.try_push(entry) {
            entries.pop().unwrap();
            entries.try_push(entry).unwrap();
        }
        for key in 0..=80 {
            assert_eq!(
                predecessor(&entries, key),
                linear_predecessor(&entries, key)
            );
        }
        assert_eq!(
            predecessor(&entries, u64::MAX),
            entries.get(entries.len() - 1).copied()
        );
    }
}
```

The repeated-wrap check stores forty even offsets with capacity three: three initial insertions and 37 evictions. It queries each state at all 81 keys from zero through 80 and at `u64::MAX`. These finite checks complement the invariant derivation; they are not a proof of every possible history.

The suffix `u64` on `40u64` determines the range's element type for the fixture arithmetic. `0..=80` includes 80, unlike `0..80`. Multiplication here is safe because the loop is explicitly bounded to forty small values.

## Erase a source type only where mixed ownership needs it

When `scan` is called with `&mut SliceSource`, the compiler knows that concrete source type. With `&mut GeneratedSource`, it knows a different type. Rust specializes generic code for its concrete types, called **monomorphization**; such calls use static dispatch and can be optimized using those known implementations. There is no requirement to choose a source at runtime. See the Rust Book's [generic-code compilation explanation](https://doc.rust-lang.org/book/ch10-01-syntax.html#performance-of-code-using-generics).

A trait object solves a different problem: holding different source types behind one interface. `dyn EntrySource` denotes a value whose concrete type is hidden behind that interface. A pointer to it carries access to the value and its method-dispatch information. `Box<dyn EntrySource>` additionally owns the concrete value. It does not clone that value or provide shared ownership. See the [trait-object reference](https://doc.rust-lang.org/reference/types/trait-object.html).

Our trait supports this use because its method has a reference receiver, no method type parameter, and a concrete `Option<Entry>` return. It does not require knowing the hidden implementer's size to return a `Self` value. These are the relevant parts of [dyn compatibility](https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility); not every possible trait can be used this way.

Add a narrow heterogeneous-source check:

```rust title="crates/language-lab/examples/generic_scan.rs"
fn boxed_fixture_checks() {
    let rows = fixture();
    let mut sources: [Box<dyn EntrySource + '_>; 2] = [
        Box::new(SliceSource::new(&rows)),
        Box::new(GeneratedSource::new(6)),
    ];
    for source in &mut sources {
        let report = scan(source.as_mut(), 6, 2, |entry| entry.offset % 4 == 2).unwrap();
        assert_eq!((report.visited, report.selected, report.evicted), (6, 3, 1));
        assert_eq!(snapshot(&report.retained), vec![rows[2], rows[4]]);
    }
}
```

The fixed array has two elements of the same **pointer type**, even though one allocation contains a `SliceSource` and the other a `GeneratedSource`. `Box::new` moves each wrapper into owned heap storage. The slice wrapper still only borrows `rows`; boxing the wrapper does not copy or prolong the array's storage.

The explicit `+ '_` allows the trait-object lifetime to be inferred from this local borrowing relationship. It does not assert `'static`. `rows` must outlive the boxed slice source, so it is declared first and remains in scope throughout the loop. Trait-object lifetime defaults differ by context; see the [lifetime-elision reference](https://doc.rust-lang.org/reference/lifetime-elision.html#default-trait-object-lifetimes). Do not add `'static` merely to suppress a borrowing error.

`source.as_mut()` reborrows the box's value as `&mut dyn EntrySource`; it does not move the box. `S: ?Sized` allows this instantiation of `scan`. Calls to `next_entry` then go through the trait object rather than assuming one concrete wrapper. The selector closure remains its own concrete generic type. Static and dynamic source dispatch both execute the same scan body and must produce the same retained data.

There is no measured claim here that one dispatch mechanism is faster. The tradeoff is knowledge and ownership: a generic call keeps concrete type information, while a trait object permits a mixed collection and an indirect method call. Box allocation is not required merely to call through a trait object; a borrowed `&mut dyn EntrySource` can also do that. The boxes here exist to own the heterogeneous wrappers in one container.

## Connect the complete executable

Add `main` after the helpers:

```rust title="crates/language-lab/examples/generic_scan.rs"
fn main() {
    let rows = fixture();
    let mut slice = SliceSource::new(&rows);
    let mut generated = GeneratedSource::new(6);
    let first = scan(&mut slice, 6, 2, |entry| entry.offset % 4 == 2).unwrap();
    let second = scan(&mut generated, 6, 2, |entry| entry.offset % 4 == 2).unwrap();

    for (label, report) in [("slice", &first), ("generated", &second)] {
        assert_eq!((report.visited, report.selected, report.evicted), (6, 3, 1));
        assert_eq!(snapshot(&report.retained), vec![rows[2], rows[4]]);
        let offsets: Vec<u64> = snapshot(&report.retained)
            .into_iter()
            .map(|entry| entry.offset)
            .collect();
        println!(
            "{label}: visited={} selected={} evicted={} retained={offsets:?}",
            report.visited, report.selected, report.evicted
        );
    }
    for key in [13, 14, 17, 18, 99] {
        let found = predecessor(&first.retained, key);
        assert_eq!(found, linear_predecessor(&first.retained, key));
        println!("lookup {key} -> {found:?}");
    }
    boxed_fixture_checks();
    println!("boxed sources agree with the static scans");
    iterator_ownership_checks();
    println!("iterator ownership checks passed");
    wrapped_lookup_checks();
    println!("ring wraparound and predecessor boundary checks passed");
}
```

The `(label, report)` pairs borrow the two reports rather than moving their rings. The loop destructures each pair. Mapping the snapshot consumes entries; the scanner's selector instead borrows each entry. Similar closure syntax does not imply the same ownership boundary.

With a correct implementation, the expected output is:

```text
slice: visited=6 selected=3 evicted=1 retained=[14, 18]
generated: visited=6 selected=3 evicted=1 retained=[14, 18]
lookup 13 -> None
lookup 14 -> Some(Entry { offset: 14, position: 64 })
lookup 17 -> Some(Entry { offset: 14, position: 64 })
lookup 18 -> Some(Entry { offset: 18, position: 128 })
lookup 99 -> Some(Entry { offset: 18, position: 128 })
boxed sources agree with the static scans
iterator ownership checks passed
ring wraparound and predecessor boundary checks passed
```

These are expected fixture results, not file reads or timing measurements. `cargo run` executes these assertions; test-only functions require the test target below.

## Test storage policy before trusting the index

Begin the test module with zero capacity, rejected ownership, and capacity-one wraparound. `Ring::<u64>` explicitly supplies the generic type where no stored value could otherwise infer it. The `::<...>` spelling at this expression position is commonly called the turbofish; it provides a type argument, not a runtime capacity.

```rust title="crates/language-lab/examples/generic_scan.rs"
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};

    #[test]
    fn zero_capacity_is_rejected() {
        assert_eq!(Ring::<u64>::new(0).unwrap_err(), ZeroCapacity);
    }

    #[test]
    fn full_ring_returns_the_owned_value_without_changing_slots() {
        let mut ring = Ring::new(1).unwrap();
        ring.try_push(String::from("first")).unwrap();
        let rejected = ring.try_push(String::from("second")).unwrap_err();
        assert_eq!(rejected, "second");
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.get(0).map(|word| word.as_str()), Some("first"));
        assert_eq!(ring.get(1), None);
        assert_eq!(ring.pop(), Some(String::from("first")));
        ring.try_push(rejected).unwrap();
        assert_eq!(ring.pop(), Some(String::from("second")));
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn one_slot_can_wrap_repeatedly() {
        let mut ring = Ring::new(1).unwrap();
        for value in 0..20 {
            ring.try_push(value).unwrap();
            assert_eq!(ring.pop(), Some(value));
            assert_eq!(ring.len(), 0);
            assert_eq!(ring.capacity(), 1);
            assert_eq!(ring.get(0), None);
        }
    }
```

Keep the module open. The string test establishes that the public ring operations do not require `Copy`: it recovers the rejected owner, pops the accepted owner, and retries the rejection. A `String` happens to support cloning, but the ring never requests a clone. The separate non-cloneable compiler exercise checks that restriction directly.

Now compare long mixed histories with a deque. The reference explicitly refuses to grow when full; a plain unbounded `VecDeque` would not represent our ring contract.

```rust title="crates/language-lab/examples/generic_scan.rs"
    #[test]
    fn mixed_ring_operations_match_a_bounded_deque() {
        for capacity in 1..=5 {
            let mut ring = Ring::new(capacity).unwrap();
            let mut reference = VecDeque::new();
            for step in 0..200 {
                if step % 3 == 2 {
                    assert_eq!(ring.pop(), reference.pop_front());
                } else if reference.len() == capacity {
                    assert_eq!(ring.try_push(step), Err(step));
                } else {
                    ring.try_push(step).unwrap();
                    reference.push_back(step);
                }
                assert_eq!(ring.len(), reference.len());
                assert_eq!(ring.capacity(), capacity);
                for logical in 0..reference.len() {
                    assert_eq!(ring.get(logical), reference.get(logical));
                }
                assert_eq!(ring.get(ring.len()), None);
                assert_eq!(
                    ring.slots.iter().filter(|slot| slot.is_some()).count(),
                    ring.len()
                );
            }
            while let Some(expected) = reference.pop_front() {
                assert_eq!(ring.pop(), Some(expected));
            }
            assert_eq!(ring.pop(), None);
        }
    }
```

Each capacity from one through five receives 200 operations. Two out of every three operations attempt a push and the third attempts a pop; many pushes occur while full. Compare the returned values, logical order, fixed capacity, and the count of `Some` slots after every transition. Checking only the final contents could miss a temporary overwrite or stale owner. Access to private `slots` is permitted from this child test module; it is not part of the ring's caller API.

`while let Some(expected)` repeats while the deque returns an element. The iterator predicate `|slot| slot.is_some()` borrows an option and checks occupancy; it does not take or move the stored value. `count()` consumes that iterator to calculate the physical occupancy for this test.

### Compare predecessor queries at gaps and wrap boundaries

Continue inside the same module:

```rust title="crates/language-lab/examples/generic_scan.rs"
    #[test]
    fn predecessor_handles_empty_exact_gap_and_extreme_keys() {
        let mut ring = Ring::new(3).unwrap();
        assert_eq!(predecessor(&ring, u64::MAX), None);
        let rows = fixture();
        for index in [0, 2, 4] {
            ring.try_push(rows[index]).unwrap();
        }
        for (key, expected) in [
            (0, None),
            (9, None),
            (10, Some(rows[0])),
            (13, Some(rows[0])),
            (14, Some(rows[2])),
            (17, Some(rows[2])),
            (18, Some(rows[4])),
            (u64::MAX, Some(rows[4])),
        ] {
            assert_eq!(predecessor(&ring, key), expected);
        }
    }

    #[test]
    fn predecessor_matches_linear_search_after_repeated_wraps() {
        wrapped_lookup_checks();
    }

    #[test]
    fn ordered_map_agrees_on_the_same_retained_entries() {
        let mut source = GeneratedSource::new(20);
        let report = scan(&mut source, 20, 3, |_| true).unwrap();
        let map: BTreeMap<u64, u64> = snapshot(&report.retained)
            .into_iter()
            .map(|entry| (entry.offset, entry.position))
            .collect();
        for key in 0..=60 {
            let expected = map
                .range(..=key)
                .next_back()
                .map(|(&offset, &position)| Entry { offset, position });
            assert_eq!(predecessor(&report.retained, key), expected);
        }
    }
```

The first test states exact expectations instead of relying exclusively on an oracle. The second exercises repeated wrapping. The third builds an ordered map from the **same retained entries**. Comparing against the full original input would be wrong after eviction: the map would know about entries the ring intentionally discarded.

The map's key and value types are both `u64`. The mapping closure produces `(offset, position)` tuples, which `collect` inserts into `BTreeMap`. The [`range(..=key)` operation](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html#method.range) restricts keys to those no greater than the request; `next_back()` chooses the last item of that ordered range. Its item is a pair of references. The pattern `|(&offset, &position)|` destructures those references and copies the two integers to construct an owned `Entry`. There is no cloned map or collected query range.

A map insertion at an already present key would replace a value rather than preserve two same-key entries. That is **not** our scanner's duplicate policy; duplicates are rejected before retention. This comparison is valid only because the retained input has already satisfied the scanner's strict ordering rule.

## Count pulls, not only retained results

A small test source records calls while delegating value generation. Append it and the source-boundary checks, still inside `mod tests`:

```rust title="crates/language-lab/examples/generic_scan.rs"
    struct CountingSource {
        inner: GeneratedSource,
        calls: usize,
    }

    impl EntrySource for CountingSource {
        fn next_entry(&mut self) -> Option<Entry> {
            self.calls += 1;
            self.inner.next_entry()
        }
    }

    #[test]
    fn both_sources_match_the_fixture_and_remain_exhausted() {
        let rows = fixture();
        let mut slice = SliceSource::new(&rows);
        let mut generated = GeneratedSource::new(6);
        for expected in rows {
            assert_eq!(slice.next_entry(), Some(expected));
            assert_eq!(generated.next_entry(), Some(expected));
        }
        for _ in 0..2 {
            assert_eq!(slice.next_entry(), None);
            assert_eq!(generated.next_entry(), None);
        }
    }

    #[test]
    fn empty_source_produces_an_empty_report() {
        let mut source = SliceSource::new(&[]);
        let report = scan(&mut source, 10, 2, |_| panic!("no entry to select")).unwrap();
        assert_eq!((report.visited, report.selected, report.evicted), (0, 0, 0));
        assert_eq!(report.retained.len(), 0);
    }
```

The wrapper needs no testing-only branch in `scan`. Returning `None` also counts as a request. The empty-input predicate panics if called: exhaustion must be detected before trying to select a nonexistent entry.

Add checks for configuration failure, zero work, and a selector that rejects everything:

```rust title="crates/language-lab/examples/generic_scan.rs"
    #[test]
    fn zero_limit_and_invalid_capacity_do_not_pull() {
        let mut source = CountingSource {
            inner: GeneratedSource::new(10),
            calls: 0,
        };
        let report = scan(&mut source, 0, 2, |_| panic!("zero work budget")).unwrap();
        assert_eq!(report.visited, 0);
        assert_eq!(source.calls, 0);
        assert_eq!(
            scan(&mut source, 3, 0, |_| true).unwrap_err(),
            ScanError::ZeroCapacity
        );
        assert_eq!(source.calls, 0);
    }

    #[test]
    fn rejected_entries_still_consume_the_work_budget() {
        let mut source = CountingSource {
            inner: GeneratedSource::new(100),
            calls: 0,
        };
        let mut predicate_calls = 0;
        let report = scan(&mut source, 4, 2, |_| {
            predicate_calls += 1;
            false
        })
        .unwrap();
        assert_eq!((report.visited, report.selected, report.evicted), (4, 0, 0));
        assert_eq!(source.calls, 4);
        assert_eq!(predicate_calls, 4);
        assert_eq!(source.inner.next, 4);
        assert_eq!(report.retained.len(), 0);
    }
```

The rejecting-selector test consumes exactly four items from a source capable of producing a hundred. Both `source.calls` and the mutable closure counter must equal four. Testing only the empty retained ring would not catch a scanner that visited all hundred entries before returning. This is an operation-count check, not a profiler or a measurement of elapsed time.

Next verify explicit eviction, malformed ordering, and dynamic dispatch:

```rust title="crates/language-lab/examples/generic_scan.rs"
    #[test]
    fn selected_entries_are_retained_or_explicitly_evicted() {
        let mut source = GeneratedSource::new(6);
        let report = scan(&mut source, 6, 2, |entry| entry.offset % 4 == 2).unwrap();
        let rows = fixture();
        assert_eq!((report.visited, report.selected, report.evicted), (6, 3, 1));
        assert_eq!(snapshot(&report.retained), vec![rows[2], rows[4]]);
        assert_eq!(predecessor(&report.retained, 13), None);
    }

    #[test]
    fn duplicate_and_decreasing_offsets_fail_before_filtering() {
        for bad_offset in [12, 11] {
            let mut rows = fixture();
            rows[2].offset = bad_offset;
            let mut source = SliceSource::new(&rows);
            let mut predicate_calls = 0;
            let error = scan(&mut source, 6, 2, |_| {
                predicate_calls += 1;
                false
            })
            .unwrap_err();
            assert_eq!(
                error,
                ScanError::OutOfOrder {
                    previous: 12,
                    next: bad_offset,
                }
            );
            assert_eq!(predicate_calls, 2);
            assert_eq!(source.next, 3);
        }
    }

    #[test]
    fn boxed_sources_use_the_same_scan() {
        boxed_fixture_checks();
    }
```

For the invalid third entry, both duplicate offset 12 and descending offset 11 must fail after previous offset 12. The source cursor is three and the predicate count is two. Those assertions record the nontransactional failure behavior rather than silently assuming the rejected entry can be read again. Selecting nothing cannot bypass validation.

Finish the test module with ownership and collection-order checks:

```rust title="crates/language-lab/examples/generic_scan.rs"
    #[test]
    fn iterator_ownership_and_lazy_prefix_are_checked() {
        iterator_ownership_checks();
    }

    #[test]
    fn collecting_before_take_visits_the_whole_input() {
        let mut mapped = 0;
        let whole: Vec<u32> = (0..8)
            .map(|value| {
                mapped += 1;
                value * 2
            })
            .collect();
        assert_eq!(mapped, 8);
        assert_eq!(whole.len(), 8);
        let prefix: Vec<u32> = whole.into_iter().take(3).collect();
        assert_eq!(prefix, vec![0, 2, 4]);
    }
}
```

The last brace closes `mod tests`. There are **16 required tests** in this example. The final test deliberately collects a tiny eight-item source before truncating it: all eight mapping calls have already happened. Its output prefix matches the lazy-prefix check, but its work and resident intermediate vector differ. Using eight items keeps this demonstration safe; replacing it with a huge allocation is unnecessary.

## Run the example and the complete foundation suite

From the learner workspace root:

```sh
cargo check -p language-lab --all-targets
cargo run -p language-lab --example generic_scan
cargo test -p language-lab --example generic_scan
cargo test -p language-lab --all-targets
cargo fmt --all
cargo fmt --all -- --check
```

The targeted example test run should report **16 passed**. With the required code from Chapters 1–3, the cumulative total is **43 tests**: six library tests, eight ownership tests, thirteen parser tests, and these sixteen. Cargo may display several target summaries rather than one combined total. Optional tests you added can increase the count. `cargo test -p language-lab` alone is not a substitute for explicitly selecting example tests or `--all-targets`.

Run the formatter before its check-only form. Treat the displayed results as expected local checks, not a claim that compilation, tests, formatting, or native macOS execution has already succeeded.

To run the earlier examples' ordinary assertions as well:

```sh
cargo run -p language-lab --example workspace_probe
cargo run -p language-lab --example borrowed_record
cargo run -p language-lab --example fallible_frame
```

Each ordinary `main` is separate from its test harness. Preserve the earlier examples.

## Diagnose bounds that the algorithm does not need

Two short experiments clarify what the type requirements actually buy. Keep intentional compiler failures outside `examples/` so `--all-targets` stays buildable. From the workspace root, create a temporary scratch directory:

```sh
mkdir -p scratch/chapter-04
```

### Repeating `None` can accidentally require `Clone`

Create this complete intentionally rejected program:

```rust title="scratch/chapter-04/repeated_none.rs"
struct NotClone;

fn main() {
    let slots: Vec<Option<NotClone>> = vec![None; 3];
    assert_eq!(slots.len(), 3);
}
```

Compile it separately:

```sh
rustc --edition=2024 scratch/chapter-04/repeated_none.rs -o scratch/chapter-04/repeated_none
```

Expect a trait-bound diagnostic: the repeated vector element must implement `Clone`, and `Option<NotClone>` does not. Exact diagnostic wording and locations can vary with the installed compiler. No actual `NotClone` value was requested, but the repetition API still imposes its type-level cloning requirement. Do not add a blanket `T: Clone` bound to a generic ring merely to accommodate this constructor shorthand.

Replace the scratch file with this complete repair:

```rust title="scratch/chapter-04/repeated_none.rs"
struct NotClone;

fn main() {
    let slots: Vec<Option<NotClone>> = (0..3).map(|_| None).collect();
    assert_eq!(slots.len(), 3);
    assert!(slots.iter().all(|slot| slot.is_none()));
}
```

Recompile with the same command and run `./scratch/chapter-04/repeated_none`. Expected behavior is successful, silent completion. The [`vec!` documentation](https://doc.rust-lang.org/std/macro.vec.html) specifies the repetition form; `all` consumes an iterator and stops if its predicate fails. The repair constructs distinct empty options without cloning an element.

### An immutable callable bound excludes a stateful selector

In `generic_scan.rs`, temporarily change only `F: FnMut(&Entry) -> bool` to `F: Fn(&Entry) -> bool`, leaving the tests present. Run:

```sh
cargo test -p language-lab --example generic_scan
```

The mutable predicate-counter closures should be rejected because they need mutable access to their captured counters. The pure selector in `main` may still compile, so testing only `cargo run` would miss this unnecessary restriction. Restore `FnMut` and rerun the targeted tests. Do not change the counters to shared mutable state or remove their assertions to appease the wrong bound. The selector's intended contract requires local mutation, not synchronization.

The same distinction explains why `keep` is a mutable binding inside `scan`. It is the owned callable value that may change state; the entry passed to it is still only a shared reference. Neither property contradicts the other.

## Distinguish resource bounds from timing claims

For a constructed ring with capacity $C$, let $s_T$ be the byte size reported by `std::mem::size_of::<Option<T>>()`. The slot region contains:

$$
B_{\text{slots}} = C s_T
$$

bytes, excluding the ring's own metadata and allocator overhead. This is a layout calculation, not a statement that the entire process uses exactly that much memory. A measured slot size of 24 bytes with capacity three, for example, would give 72 bytes of slot storage. Do not assume 24 is a portable enum-layout guarantee. Temporarily insert the following complete statements at the start of this example's `main` to inspect your build, then remove them to recover the displayed output:

```rust title="Temporary statements inside generic_scan.rs main"
let slot_bytes = std::mem::size_of::<Option<Entry>>();
println!("one optional entry occupies {slot_bytes} bytes in this build");
```

[`size_of`](https://doc.rust-lang.org/std/mem/fn.size_of.html) reports a type's in-memory size including padding, not the size of allocations an instance may own. For `Ring<String>`, each slot contains a string handle; the string's separately allocated contents are not bounded by the number of slots. For `Ring<Entry>`, no entry owns another allocation. A future byte budget must charge owned buffers explicitly rather than equating a two-item queue with two small messages.

Within this implementation, constructing the ring performs work proportional to capacity. Each ring push, pop, and logical access does a constant number of operations without growing the slot allocation. For a successful scan visiting $v$ items, the loop performs work proportional to $v$, **assuming constant-cost source and selector calls**. Retention uses at most $C$ entries, while each predecessor query uses logarithmic comparisons and no extra collection. Those claims describe the code's operation structure. They are not throughput numbers, allocation-failure guarantees, or conclusions about which collection wins on your machine.

### Diagnose the observed symptom

| Symptom | Likely violated relationship | Specific repair |
| --- | --- | --- |
| A full push loses the new string | The rejection path dropped ownership instead of returning it. | Return `Err(value)` before changing a slot; retry the returned value in the string test. |
| Results fail only after wrapping | Search or snapshot used physical order. | Route every logical lookup through `get`; compare each transition with the deque. |
| Empty lookup panics | The code calculated `low - 1` without handling zero. | Retain the checked subtraction and test an empty ring. |
| A maximal key wraps | The search compared with `key + 1`. | Compare offsets directly with `<= key`. |
| A rejecting filter scans all input | The loop limit counted selected items. | Limit `visited`; assert both source and predicate call counts. |
| A duplicate survives when filtered out | Ordering was validated only after selection. | Validate every visited offset before calling the selector. |
| A trait-object call demands a known size | The source type parameter retained its implicit `Sized` bound. | Keep `S: EntrySource + ?Sized` and pass `source.as_mut()`. |
| A zero-length vector is indexed despite a reservation | Capacity was mistaken for initialized length. | Build initialized `None` slots; do not use unsafe length manipulation. |

## Leave a reusable mechanism, not an accidental broker

The completed workspace has one new example and no new dependency. Its source trait is narrow, its two input implementations are explicit, its scanner bounds visited work and retained metadata independently, and its predecessor lookup works through a logical ring view. The lossy newest-samples policy belongs to the scanner; the ring itself only accepts, rejects, and removes ownership.

For a runnable variation, change only `selected_entries_are_retained_or_explicitly_evicted`: use capacity three, expect counters `(6, 3, 0)`, and expect retained entries `[rows[0], rows[2], rows[4]]`. Predict its key-13 result before running: it becomes `Some(rows[0])`, offset 10 at position zero. Run `cargo test -p language-lab --example generic_scan selected_entries_are_retained_or_explicitly_evicted`, then restore the original test. This checks retained coverage without changing the standard demonstration.

[Chapter 5 in the roadmap](https://25d-228.github.io/rust-learning/index.html#chapter-5-shared-ownership-threads-and-synchronization) will put bounded ownership transfer behind synchronization. The present ring is not thread-safe by itself, and rejecting a full push is not yet waiting for capacity. Keep that distinction: a mutex and a condition variable will need an explicit predicate, closure state, notification rule, and cleanup path rather than a new name for this single-threaded loop.
