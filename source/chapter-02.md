# Chapter 2 — Ownership, borrowing, and lifetime-bound views

A storage or network buffer has two jobs that eventually conflict: keep bytes available to a reader, and become reusable for the next operation. A reader that merely points into the buffer cannot continue using the old contents after the owner overwrites them. Copying everything avoids that conflict but hides the resource decision. This chapter makes the decision explicit: inspect a borrowed record, copy only the payload that must survive, and then reuse the original storage.

The program uses one known-valid, four-byte record. It is an ownership exercise, not the broker's disk format or a parser for untrusted input. There is no network, asynchronous runtime, storage file, or external crate.

**Starting point:** [Chapter 1's workspace](https://25d-228.github.io/rust-learning/chapter-01.html), including both Cargo manifests, `src/lib.rs`, `src/byte_summary.rs`, and `examples/workspace_probe.rs`. Keep all five files unchanged. The library still exports `summarize(&[u8]) -> (usize, usize, usize)` and `sum_pair(u8, u8) -> u16`; this chapter does not change or require additional library interfaces.

**Implementation time:** 2.5 hours: about 1.75 for the program and focused modifications, and 0.75 for tests and compiler experiments. Reading is separate. Record the time actually spent implementing and debugging.

> **Learning objectives**
> Distinguish moving an owner from copying data; choose between a shared view, an exclusive mutable view, and an owned result; define a struct and its borrowing or consuming methods; express an input/output lifetime relationship; and diagnose three invalid ownership relationships without adding arbitrary clones or lifetime annotations.

## Extend the existing example workspace

Create one file:

```text title="New file — relative to the Chapter 1 workspace root"
crates/language-lab/examples/borrowed_record.rs
```

All Rust blocks labelled **`crates/language-lab/examples/borrowed_record.rs`**, in order, form that file. Each block contains complete functions or declarations. Other code blocks are explicitly labelled as explanations, experiments, or optional additions; do not concatenate them into the program.

Cargo discovers this example from its location, just as it discovered `workspace_probe`. The manifests and edition remain unchanged. `Vec`, `String`, and `drop` are available through Rust's standard prelude; no `use` declarations or dependencies are missing from the main program. The tests have their own imports, supplied below. The [prelude documentation](https://doc.rust-lang.org/std/prelude/index.html) describes these automatically available names.

We will assemble the example before running it. It will have two short introductory functions, an owning record type, a borrowing view type, a view constructor, `main`, and eight tests. The compiler-failure experiments use separate scratch files so that the working Cargo targets remain buildable.

## A move transfers responsibility, not the byte contents

For an ordinary owning value, Rust tracks which binding is responsible for its eventual cleanup. Passing that value by value, returning it, or assigning it to another binding can **move** it. After a move, the old binding is not another usable owner. On normal scope exit, the value is dropped through its current owner. These are language rules, not a runtime reference counter; see the Rust Book's [ownership discussion](https://doc.rust-lang.org/book/ch04-01-what-is-ownership.html#ownership-rules).

Consider a `Vec<u8>`, a growable sequence of byte values. `Vec` is a generic type; the angle brackets select `u8` as the element type. We will define our own generic interfaces in Chapter 4. Here, using the standard container needs no generic implementation of our own.

A useful conceptual picture separates the vector's handle from its elements:

```text title="Conceptual storage — not a promised field order or stack address"
local vector handle: pointer, length, capacity
                            |
                            v
heap allocation:          [99, 0, 255, spare storage ...]
                           three initialized elements
```

The standard vector owns a contiguous heap buffer when it has a nonzero byte allocation. Its handle has a fixed size, but that does not make the handle freely duplicable: it carries ownership of that allocation. The handle might be in a stack slot, another object, or optimized into registers. Do not infer ownership from whether a value is described as “on the stack.” Rust does not guarantee the field order shown in this diagram. The [vector guarantees](https://doc.rust-lang.org/std/vec/struct.Vec.html#guarantees) distinguish the handle, initialized elements, and spare capacity.

In the following trace, `A` names one conceptual allocation, not a real address:

| Operation | Owner of allocation A | Valid access through the old name? | Payload bytes duplicated? |
| --- | --- | --- | --- |
| Create `original` | `original` | Yes | Initial bytes are written once |
| `let moved = original;` | `moved` | No | No |
| Borrow a slice from `moved` | Still `moved` | `original` remains moved | No |
| Drop `moved` after all borrows end | None | No | No; its allocation is released |

A move need not copy the heap elements, even though moving the handle may involve machine instructions. “Move” describes the ownership change, not an instruction count or a guarantee that no machine word moves.

## Copy small values; clone owned bytes deliberately

Some types implement **`Copy`**, a standard trait that permits implicit duplication when a value would otherwise move. A trait describes behavior a type supports; detailed trait definitions come later. Integers such as `u8` and fixed arrays of `Copy` elements support this behavior. Ordinary shared references are also `Copy`: copying a reference makes another reference to the same data, not another allocation. A mutable reference is not `Copy`. See the [`Copy` contract](https://doc.rust-lang.org/std/marker/trait.Copy.html).

`Vec<u8>` does not implement `Copy`. It does implement **`Clone`**: `vector.clone()` explicitly creates another vector with its own byte contents. That cost is intentional and visible. Do not generalize this into “all cloning deeply copies everything.” The [`Clone` contract](https://doc.rust-lang.org/std/clone/trait.Clone.html) is type-specific; later shared-ownership types clone handles to shared data instead.

Start `borrowed_record.rs` with this complete function:

```rust title="crates/language-lab/examples/borrowed_record.rs"
fn demonstrate_moves() {
    let tag = 7u8;
    let copied_tag = tag;
    assert_eq!(tag, copied_tag);
    println!("copy: tag={tag} copied={copied_tag}");

    let original = vec![b'c', 0, 255];
    let mut moved = original;
    let cloned = moved.clone();
    moved[0] = b'd';

    assert_eq!(moved.as_slice(), &[100, 0, 255]);
    assert_eq!(cloned.as_slice(), &[99, 0, 255]);
    println!("move/clone: moved={moved:?} cloned={cloned:?}");
}
```

The suffix in `7u8` selects an unsigned byte rather than relying on integer inference. `vec!` is a macro that creates a vector from its listed elements; `b'c'` is the byte value 99. `let mut moved` permits mutation through that binding. `original` did not need `mut`: permission to mutate is not a permanent property attached to its allocation. The new owner can be bound mutably.

The assignment to `moved[0]` changes the original allocation's first element to 100. The cloned vector remains `[99, 0, 255]`. `as_slice()` borrows the initialized elements for the assertions. The `:?` formatting displays a vector's byte values for diagnosis; it does not consume the vector. Both vectors are still valid through their respective bindings until the function ends.

At normal function return, the two remaining vector owners are dropped. The moved-from binding `original` does not release the allocation a second time. This is why replacing the move with an untracked, bitwise copy of an owning handle would be wrong: two independently dropped handles would claim one allocation.

## Text is a stronger promise than bytes

`String` owns UTF-8 text. `&str` borrows UTF-8 text. `Vec<u8>` and `&[u8]` carry bytes without claiming those bytes are text. In particular, an embedded zero is an ordinary byte, and 255 is allowed in a byte vector even though it is not a valid byte by itself in UTF-8. The [`String` documentation](https://doc.rust-lang.org/std/string/struct.String.html#utf-8) explains this validity constraint and why a string's length is a byte length.

Append a second function:

```rust title="crates/language-lab/examples/borrowed_record.rs"
fn demonstrate_text() {
    let label = String::from("é");
    let bytes = label.as_bytes();
    assert_eq!(bytes, &[0xc3, 0xa9]);
    assert_eq!(label.len(), 2);
    println!("text: label={label} bytes={bytes:?} length={}", label.len());
}
```

`String::from` creates owned text from the string literal. `::` selects the associated function on the type, whereas `label.as_bytes()` calls a method on a value. The `0x` prefix writes a hexadecimal integer: `0xc3` is 195 and `0xa9` is 169. The literal here is the single precomposed character `é`, encoded in two bytes; `len()` is not a count of displayed characters.

`as_bytes()` only borrows. This function's `bytes` points into the storage owned by `label`; it is not a separately allocated conversion. Indexing a byte slice at a valid byte position does not promise a whole text character. A string slice must additionally end on UTF-8 boundaries, so selecting the first byte of this text is not a valid `&str`. The [slice explanation](https://doc.rust-lang.org/book/ch04-03-slices.html#string-slices) makes that distinction concrete.

Our record payload `[99, 0, 255]` intentionally is not text. We will inspect it numerically and will not decode it lossily just to print it. Doing so would change the diagnostic representation of the payload before we had decided whether text was part of its contract.

## Borrowing specifies access, not a second owner

A shared borrow of our plain byte data permits reading. An exclusive mutable borrow permits reading and writing. A borrow does not take responsibility for freeing the owner's storage. While a shared slice is needed, the owner cannot be used to perform a conflicting mutation; while an exclusive borrow is needed, other overlapping accesses must not conflict with it. The Rust Book's [borrowing rules](https://doc.rust-lang.org/book/ch04-02-references-and-borrowing.html) supply the language model behind these restrictions.

The signatures describe different capabilities:

| Parameter or result | Capability in this chapter | Owns the byte allocation? |
| --- | --- | --- |
| `Vec<u8>` | Receive or return the whole growable buffer by value | Yes |
| `&[u8]` | Read a contiguous byte range | No |
| `&mut [u8]` | Exclusively read or overwrite that range; not resize its owner | No |
| `&mut Vec<u8>` | Exclusively access the vector, including length-changing operations | No |

A slice reference contains a starting location and a length. It does not scan for a terminator, and it does not retain the allocation after its owner is destroyed. `&bytes[1..4]` borrows indexes 1, 2, and 3; the endpoint 4 is excluded. `&mut bytes[1..4]` requests mutable access to the same range. The owner remains where it was.

For these simple references, the borrow checker can usually stop requiring a borrow after its last use, even before the surrounding braces close. The binding's textual scope and the region over which its borrow is required are not identical. A later use can extend that required region and make an intervening mutation invalid. We will exercise exactly that change, rather than treating braces as a magical lifetime repair.

## Define a small record with a visible invariant

The exercise record is exactly four bytes: a one-byte **tag** followed by three payload bytes. The tag is not a length. There are no hidden lengths, alignment padding, checksum, or terminator in this byte layout.

```text title="The complete first record — hexadecimal values"
byte index:        0       1       2       3
contents:         07      63      00      ff
meaning:         tag     payload bytes
```

Let $H$ be the header size in bytes, $P$ the payload size in bytes, and $T$ the total record size. For this fixture:

$$
T = H + P = 1 + 3 = 4.
$$

Let $b_i$ denote the byte at record index $i$, and $p_j$ the byte at payload index $j$. The range operation implements:

$$
p_j = b_{H+j}, \qquad 0 \leq j < P.
$$

Thus payload index zero reads record index one, whose value is 99; payload index two reads record index three, whose value is 255. No byte-swapping or integer-width conversion is needed because the header contains only one byte.

A **struct** groups named fields. Our owning struct contains a vector; our view struct contains a copied tag and a borrowed slice. The Rust Book's [struct syntax](https://doc.rust-lang.org/book/ch05-01-defining-structs.html) explains declarations, construction, and field access.

Append these declarations:

```rust title="crates/language-lab/examples/borrowed_record.rs"
const HEADER_LEN: usize = 1;
const PAYLOAD_LEN: usize = 3;
const RECORD_LEN: usize = HEADER_LEN + PAYLOAD_LEN;

struct OwnedRecord {
    bytes: Vec<u8>,
}

struct RecordView<'a> {
    tag: u8,
    payload: &'a [u8],
}
```

`const` declares a compile-time constant with an explicit type. `[u8; PAYLOAD_LEN]`, used shortly, is an array type with exactly three elements; the semicolon separates element type from length. `[u8]` without that length describes a slice, which we access behind a reference.

`OwnedRecord` needs no lifetime parameter because its vector owns the bytes. `RecordView<'a>` does need one: the field `payload` refers to bytes owned elsewhere. The apostrophe distinguishes a **lifetime parameter** from a type parameter. Read `&'a [u8]` as “a shared byte slice valid for the borrow region named `'a`.” It does not mean allocate an object named `a`, attach a timeout, or extend the owner's life.

There is deliberately no automatic `Copy` or `Clone` derivation on either struct. Reading `view.tag` copies a `u8`; reading `view.payload` copies a shared reference. Those facts do not automatically make the whole `RecordView` type `Copy`. We only need the field operations in this exercise.

## Give the owner borrowing and consuming methods

An `impl Type` block defines operations associated with that type. Within it, `Self` means the type itself. A parameter named `self` receives an existing instance: `&self` borrows it, `&mut self` borrows it exclusively, and `self` takes it by value. The [method syntax reference in the Rust Book](https://doc.rust-lang.org/book/ch05-03-method-syntax.html) explains those receiver choices.

Our invariant is simple: every usable `OwnedRecord` constructed here contains exactly four initialized bytes. Construction and replacement accept a three-element array. The methods do not expose vector resizing while the wrapper remains usable.

Append the entire implementation:

```rust title="crates/language-lab/examples/borrowed_record.rs"
impl OwnedRecord {
    fn new(tag: u8, payload: [u8; PAYLOAD_LEN]) -> Self {
        let mut bytes = Vec::with_capacity(RECORD_LEN);
        bytes.push(tag);
        bytes.extend_from_slice(&payload);
        Self { bytes }
    }

    fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[HEADER_LEN..RECORD_LEN]
    }

    fn replace(&mut self, tag: u8, payload: [u8; PAYLOAD_LEN]) {
        self.bytes[0] = tag;
        self.bytes[HEADER_LEN..RECORD_LEN].copy_from_slice(&payload);
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}
```

`new` has no `self` parameter because it constructs the instance rather than receiving one. Call it with `OwnedRecord::new(...)`. `Self { bytes }` is field-init shorthand for `OwnedRecord { bytes: bytes }`; it moves the local vector into the returned struct. The payload array contains `Copy` bytes, so passing that array by value does not transfer a separate heap allocation.

`Vec::with_capacity(RECORD_LEN)` creates an **empty** vector with room for at least four elements; it does not create four initialized bytes. `push` adds the tag and `extend_from_slice` copies the three payload values into the vector. The constructor's length therefore progresses from zero, to one, to four. Capacity is permission to grow without necessarily allocating again, not permission to read uninitialized elements. See [`with_capacity`](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.with_capacity).

`as_bytes` exposes no mutation. `payload_mut` exposes only the payload positions, not the tag or the vector's length. An exclusive borrow of a slice permits assigning to its elements; it does not provide `push` or `clear` on the owning vector.

`replace` overwrites the tag and then the existing three-byte payload. The destination slice and the input array have equal lengths by construction. [`copy_from_slice`](https://doc.rust-lang.org/std/primitive.slice.html#method.copy_from_slice) requires equal lengths; it copies elements into existing storage rather than allocating a new vector. No runtime length field is being trusted here.

`into_bytes` consumes the wrapper and returns its vector by value. It moves the allocation's ownership to the caller rather than cloning it. After the call, the old `OwnedRecord` cannot be used. The caller may resize the returned vector because it no longer claims to be a valid fixed record. This is a useful API distinction: a borrowed view preserves an owner's contract, while a consuming conversion can deliberately leave that contract behind.

The fields are private to this example's module, not inaccessible to code in the same module. The invariant therefore depends on the supplied code using these methods rather than editing `record.bytes` directly. Later library modules will give this boundary an actual external caller. We are not claiming that a name containing “Owned” enforces an invariant by itself.

## Tie the returned view to the input borrow

The view constructor accepts already assembled bytes, checks our fixed-shape programming invariant, and returns a tag plus a payload slice. Append it:

```rust title="crates/language-lab/examples/borrowed_record.rs"
fn view_record<'a>(bytes: &'a [u8]) -> RecordView<'a> {
    assert_eq!(
        bytes.len(),
        RECORD_LEN,
        "fixed record requires exactly four bytes"
    );
    RecordView {
        tag: bytes[0],
        payload: &bytes[HEADER_LEN..RECORD_LEN],
    }
}
```

The assertion precedes indexing. Its purpose is to catch a caller that violates the exercise's “exactly one fixed record” precondition. It is not an acceptable policy for arbitrary network input: Chapter 3 will distinguish incomplete data from invalid data using explicit return values. Both a three-byte input and a five-byte input violate this function's current contract.

Read the signature in three parts. `<'a>` declares a lifetime parameter for this function. `bytes: &'a [u8]` accepts a borrow for that region. `-> RecordView<'a>` carries the same relationship into the returned struct's slice field. The function returns a view by value, but that view still contains a reference; returning a struct does not make its referenced bytes owned.

The caller supplies storage whose borrow remains valid for the uses it makes of the view. The function cannot manufacture a longer lifetime or make a borrowed vector survive its own destruction. Lifetimes express constraints checked at compile time; they are not reference counts, hidden copies, or runtime timers. The Rust Book's [lifetime discussion](https://doc.rust-lang.org/book/ch10-03-lifetime-syntax.html#lifetime-annotations-in-function-signatures) explains this input/output relationship.

The lifetime chain in this example is:

```text title="Where the view's bytes come from"
record owns its Vec allocation
  record.as_bytes() borrows that allocation
    view_record(...) returns a RecordView
      view.payload refers to bytes 1 through 3 of that same allocation
```

The two methods `as_bytes` and `payload_mut` omit explicit lifetime names. Rust's [lifetime-elision rules](https://doc.rust-lang.org/reference/lifetime-elision.html#lifetime-elision-in-functions) associate a method's elided output-reference lifetime with the borrowed receiver. Here `fn as_bytes(&self) -> &[u8]` has the same relationship as `fn as_bytes<'a>(&'a self) -> &'a [u8]`. Omitting the annotation does not omit the constraint.

For the same reason, the free function could use `fn view_record(bytes: &[u8]) -> RecordView<'_>`: one input lifetime makes the relationship unambiguous. We retain the explicit `'a` to make that relationship visible. Do not add `'static` in an attempt to escape it. A local owned buffer is not made permanent by changing a signature.

## Copy only what must survive the reuse boundary

The main operation has three distinct phases: borrow, detach, mutate. **Detach** here means creating independent owned payload bytes, not just copying the view's reference. [`to_vec`](https://doc.rust-lang.org/std/primitive.slice.html#method.to_vec) performs that conversion for a slice. On our byte payload, it copies exactly the three initialized payload values into a new vector.

`let saved = first.payload;` would only copy a shared reference. The buffer would still have to remain unchanged for the later uses of `saved`. By contrast, `let saved = first.payload.to_vec();` creates a `Vec<u8>` that is no longer tied to the source borrow. The choice is observable in the type, not just the variable name.

Append the complete executable entry point:

```rust title="crates/language-lab/examples/borrowed_record.rs"
fn main() {
    demonstrate_moves();
    demonstrate_text();

    let mut record = OwnedRecord::new(7, [b'c', 0, 255]);
    let saved = {
        let first = view_record(record.as_bytes());
        let same_payload = first.payload;
        assert_eq!(first.tag, 7);
        assert_eq!(same_payload, &[99, 0, 255]);
        println!("view: tag={} payload={:?}", first.tag, first.payload);
        first.payload.to_vec()
    };
    println!("saved: {saved:?}");

    record.replace(8, [b'd', b'o', b'g']);
    {
        let reused = view_record(record.as_bytes());
        assert_eq!(reused.tag, 8);
        assert_eq!(reused.payload, &[100, 111, 103]);
        println!("reused: tag={} payload={:?}", reused.tag, reused.payload);
    }
    assert_eq!(saved.as_slice(), &[99, 0, 255]);

    {
        let payload = record.payload_mut();
        payload[0] = b'D';
        println!("mutable view: {payload:?}");
    }

    let mut raw = record.into_bytes();
    assert_eq!(raw.as_slice(), &[8, 68, 111, 103]);
    println!("transferred bytes: {raw:?}");
    let capacity_before_clear = raw.capacity();
    raw.clear();
    assert!(raw.is_empty());
    assert_eq!(raw.capacity(), capacity_before_clear);
    drop(raw);

    assert_eq!(saved.as_slice(), &[99, 0, 255]);
    println!("saved after owner drop: {saved:?}");
}
```

A block is an expression. The final line `first.payload.to_vec()` has no semicolon, so its owned vector becomes the block's value and is bound to `saved`. Adding a semicolon would discard that value and make the block evaluate to `()`, the unit type. The borrowed `first` does not escape this block; only independent bytes do.

`same_payload` demonstrates the difference: reading `first.payload` copies a shared slice reference and leaves the field usable, as the next print shows. It does not detach anything. The actual detachment occurs on the following line that calls `to_vec`.

The later `payload` binding is not declared `mut`, yet `payload[0] = b'D'` is allowed because its value is an exclusive `&mut [u8]`. The permission is in the reference type. A `mut` binding to a shared `&[u8]` would instead permit rebinding the variable, not writing through that shared reference.

There are no simultaneous conflicting accesses in this sequence. `replace` runs after the old shared view is no longer used. The mutable slice is no longer used when `into_bytes` consumes the owner. Braces make the phases easy to see; the crucial property is which borrows are still required, not merely where we inserted punctuation.

`drop(raw)` consumes the vector and lets it be destroyed before the final assertion. It is an ordinary function, documented by [`std::mem::drop`](https://doc.rust-lang.org/std/mem/fn.drop.html), not a special lifetime escape. Calling `drop` on a shared reference would dispose of a copied reference, not the owner. Our final assertion specifically tests data in the *other*, independently owned vector.

## Trace the allocations and the copy cost

Name the original record's allocation `A` and the saved payload's allocation `B`. These names describe ownership; the program does not rely on their numeric addresses.

| Moment | Allocation A: owner and contents | Allocation B: owner and contents | Reference still needed into A? |
| --- | --- | --- | --- |
| After construction | `record`, `[7, 99, 0, 255]` | Not created | No |
| While inspecting `first` | `record`, unchanged | Not created | Yes: `first.payload` and its copied reference |
| After the `saved` block | `record`, unchanged | `saved`, `[99, 0, 255]` | No |
| After `replace` | `record`, `[8, 100, 111, 103]` | `saved`, unchanged | No old view remains |
| During mutable payload access | `record`, then `[8, 68, 111, 103]` | `saved`, unchanged | One exclusive payload borrow |
| After `into_bytes` | `raw`, same four bytes | `saved`, unchanged | No |
| After `raw.clear()` | `raw`, zero initialized elements; capacity retained | `saved`, unchanged | No |
| After `drop(raw)` | Released by its owner | `saved`, still valid | No |

Let $L$ be a vector's initialized length and $C$ its reported capacity, both in elements. For our byte vectors, each element occupies one byte. The usable-index condition and spare-capacity count are:

$$
0 \leq i < L \leq C, \qquad \text{spare elements} = C - L.
$$

With $L=4$, indexes zero through three exist; an allocation with capacity 16 would still not permit reading index four. After `clear`, the length is zero even though capacity is retained. The program captures and compares actual capacities rather than assuming the allocator returns a particular number. [`Vec::clear`](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.clear) removes the elements without changing the capacity; it is not a deallocation or a secure erase.

The copy decision is similarly explicit. For a buffer containing $N$ initialized bytes and a selected payload of $P$ bytes, copying the entire buffer duplicates $N$ content bytes, whereas `payload.to_vec()` duplicates $P$. Moving its vector or borrowing a view duplicates no payload bytes. That does **not** say those operations execute zero instructions or that allocation overhead is zero.

In this example, the choices are four bytes versus three. For a hypothetical 64 KiB input buffer with a selected 1 KiB payload, the content-copy ratio would be:

$$
\frac{65{,}536}{1{,}024} = 64.
$$

One KiB is 1,024 bytes. The ratio describes copied content, not a measured 64-fold latency improvement. Allocator overhead, metadata, cache behavior, and how long the source must be retained affect the actual tradeoff. A borrowed view avoids the content copy but constrains reuse of its owner. An owned payload pays for independence. Neither choice is universally best.

At normal scope exit, a struct's fields are dropped recursively. We need no custom destructor to free the `Vec` inside `OwnedRecord`; and after `into_bytes` moves that field out, it is the returned vector that owns cleanup. Rust's [destructor rules](https://doc.rust-lang.org/reference/destructors.html#destructors) explain this behavior. Do not confuse it with durability: process termination or abort need not execute destructors, and releasing an in-memory owner does not synchronize a storage file. Recovery will require explicit mechanisms later.

## Test views, mutation, and ownership independently

`main` provides the visible trace, but small tests isolate mistakes. The following six tests belong to a nested module enabled only when compiling this example as a test target. `super` names the parent module, so the import reaches the types and function defined above without making them public.

Append the complete module:

```rust title="crates/language-lab/examples/borrowed_record.rs"
#[cfg(test)]
mod tests {
    use super::{OwnedRecord, view_record};

    #[test]
    fn reads_tag_and_binary_payload() {
        let record = OwnedRecord::new(7, [b'c', 0, 255]);
        let first = view_record(record.as_bytes());
        let second = view_record(record.as_bytes());
        assert_eq!(first.tag, 7);
        assert_eq!(first.payload, &[99, 0, 255]);
        assert_eq!(second.payload, first.payload);
    }

    #[test]
    fn mutable_payload_preserves_the_tag() {
        let mut record = OwnedRecord::new(7, [b'c', 0, 255]);
        let payload = record.payload_mut();
        payload[0] = b'C';
        payload[2] = 128;
        assert_eq!(record.as_bytes(), &[7, 67, 0, 128]);
    }

    #[test]
    fn replacement_does_not_change_saved_payload() {
        let mut record = OwnedRecord::new(7, [b'c', 0, 255]);
        let saved = view_record(record.as_bytes()).payload.to_vec();
        record.replace(8, [b'd', b'o', b'g']);
        assert_eq!(record.as_bytes(), &[8, 100, 111, 103]);
        assert_eq!(saved.as_slice(), &[99, 0, 255]);
    }

    #[test]
    fn copied_payload_outlives_its_source() {
        let saved = {
            let record = OwnedRecord::new(7, [b'c', 0, 255]);
            view_record(record.as_bytes()).payload.to_vec()
        };
        assert_eq!(saved.as_slice(), &[99, 0, 255]);
    }

    #[test]
    fn consuming_conversion_returns_the_whole_buffer() {
        let record = OwnedRecord::new(9, [0, 128, 255]);
        let mut raw = record.into_bytes();
        assert_eq!(raw.as_slice(), &[9, 0, 128, 255]);
        let capacity = raw.capacity();
        raw.clear();
        assert!(raw.is_empty());
        assert_eq!(raw.capacity(), capacity);
    }

    #[test]
    fn cloned_byte_vectors_can_change_independently() {
        let mut first = vec![99u8, 0, 255];
        let mut second = first.clone();
        first[0] = 100;
        second[2] = 128;
        assert_eq!(first.as_slice(), &[100, 0, 255]);
        assert_eq!(second.as_slice(), &[99, 0, 128]);
    }
}
```

The first test permits two simultaneous shared views, not two owners. The second test deliberately omits an inner scope: the mutable `payload` is last used before `record.as_bytes()` creates its shared view, so the access phases do not overlap. Adding a later use of `payload` after that assertion would change the borrow relationship.

The replacement test distinguishes a real payload copy from a copied reference. The scope-exit test goes further: its source record is destroyed before the assertion. The conversion test checks that the header travels with the consumed buffer and that clearing does not discard capacity. The clone test changes both copies, so an accidentally shared buffer would violate at least one expectation. None of these tests requires an allocation address or exact allocator capacity.

Finally, two tests check the fixed-shape precondition. `#[should_panic(expected = "...")]` tells the test harness to expect a panic containing the supplied text; an ordinary return makes the test fail. This is a targeted check of a programming assertion, not the error-handling strategy for incoming records. See the [test attribute reference](https://doc.rust-lang.org/reference/attributes/testing.html#the-should_panic-attribute).

Append this second complete test module:

```rust title="crates/language-lab/examples/borrowed_record.rs"
#[cfg(test)]
mod shape_tests {
    use super::view_record;

    #[test]
    #[should_panic(expected = "fixed record requires exactly four bytes")]
    fn rejects_a_short_fixed_record() {
        view_record(&[7, 99, 0]);
    }

    #[test]
    #[should_panic(expected = "fixed record requires exactly four bytes")]
    fn rejects_extra_bytes() {
        view_record(&[7, 99, 0, 255, 9]);
    }
}
```

The five-byte case matters: this function does not accept a prefix and return a consumed length. Treating two records or trailing bytes as acceptable would silently change the interface. That capability belongs to the fallible framing exercise in Chapter 3, not a hidden exception in this one.

## Run the completed example and its own tests

Run from the existing workspace root:

```sh title="Terminal — format, check, and execute the Chapter 2 target"
cargo fmt --all
cargo check -p language-lab --all-targets
cargo run -p language-lab --example borrowed_record
cargo test -p language-lab --example borrowed_record
```

Expected program output, excluding Cargo's build messages:

```text title="Expected stdout — borrowed_record"
copy: tag=7 copied=7
move/clone: moved=[100, 0, 255] cloned=[99, 0, 255]
text: label=é bytes=[195, 169] length=2
view: tag=7 payload=[99, 0, 255]
saved: [99, 0, 255]
reused: tag=8 payload=[100, 111, 103]
mutable view: [68, 111, 103]
transferred bytes: [8, 68, 111, 103]
saved after owner drop: [99, 0, 255]
```

The example's test command should report **eight passed tests**: six ownership/data checks and two expected-panic checks. Test order and timing are not part of the expected result. The output shown here specifies expected behavior; compare it with your local run.

Do not substitute a plain `cargo test -p language-lab` and assume that it ran the new example's unit tests. Cargo normally builds examples as a compilation check; running their test harnesses requires selecting them. [`cargo test` target selection](https://doc.rust-lang.org/cargo/commands/cargo-test.html#target-selection) documents `--example` and `--all-targets`. Running the example's test harness also does not run its ordinary `main`; that is why both commands appear above.

## Read a compiler rejection as an ownership trace

The next three programs are deliberately invalid. Keep them outside `crates/language-lab/examples/` so that checking a failure does not break the working package. From the workspace root, create a scratch directory:

```sh title="Terminal — scratch files for intentional compiler failures"
mkdir -p runs/ownership
```

Use only the named files created for these experiments, and do not overwrite unrelated work. Their code is independent of the Cargo example. After each rejection, replace that file with its complete repaired version before moving on. Do not leave a broken example in the package, suppress the error, or use `unsafe` to bypass it. Compiler wording and line numbers vary by version; the ownership conflict is the important result.

### A moved-from name cannot inspect the new owner's vector

Create the following scratch file:

```rust title="runs/ownership/use_after_move.rs — intentional failure"
fn main() {
    let bytes = vec![7u8, 99, 0, 255];
    let owner = bytes;
    println!("{bytes:?}");
    println!("{owner:?}");
}
```

Check it independently:

```sh title="Terminal — expect a nonzero compiler exit"
rustc --edition=2024 runs/ownership/use_after_move.rs -o runs/ownership/use_after_move
```

Expect a use/borrow-of-moved-value diagnostic, normally [E0382](https://doc.rust-lang.org/error_codes/E0382.html), pointing from the later `bytes` use back to `let owner = bytes`. The vector was not destroyed at that assignment; it moved. The error prevents the old name from being treated as a second owner.

Here the intention is one owner, so the repair is to use the current owner, not allocate a clone. Replace the entire file with:

```rust title="runs/ownership/use_after_move.rs — repaired complete file"
fn main() {
    let bytes = vec![7u8, 99, 0, 255];
    let owner = bytes;
    println!("{owner:?}");
}
```

Repeat the compilation command; only after it succeeds, run `./runs/ownership/use_after_move`. Expect `[7, 99, 0, 255]`. When both callers genuinely need independent mutable bytes, cloning can be correct; when a second reader only needs temporary access, borrowing can be correct. Neither is automatically the right repair for every move error.

### A lifetime name cannot rescue a local allocation

Create a function that wrongly tries to return a slice into its own local vector:

```rust title="runs/ownership/local_reference.rs — intentional failure"
fn make_payload<'a>() -> &'a [u8] {
    let bytes = vec![7u8, 99, 0, 255];
    &bytes[1..4]
}

fn main() {
    println!("{:?}", make_payload());
}
```

Check it:

```sh title="Terminal — expect a nonzero compiler exit"
rustc --edition=2024 runs/ownership/local_reference.rs -o runs/ownership/local_reference
```

Expect a diagnostic about returning a reference to locally owned data, normally [E0515](https://doc.rust-lang.org/error_codes/E0515.html). The local `bytes` owner would be dropped when `make_payload` returns. The unconstrained `'a` in the signature cannot create storage for the caller's later use. Substituting `'static` would only make a stronger unsatisfied promise.

This function is supposed to manufacture data for a caller that supplied no owner. Return ownership. Replace the file with:

```rust title="runs/ownership/local_reference.rs — repaired complete file"
fn make_payload() -> Vec<u8> {
    vec![99, 0, 255]
}

fn main() {
    let payload = make_payload();
    println!("{payload:?}");
}
```

Repeat the compilation command; after success, run `./runs/ownership/local_reference`. Expect `[99, 0, 255]`. The returned vector moves into `payload`; its bytes are not copied merely because the function returns. When the purpose is instead to view caller-owned storage, accept a borrowed input and tie the result to it, as `view_record` already does. These are two different interfaces with different owners, not alternative spellings of the same lifetime annotation.

### Spare capacity does not permit mutation during a live shared borrow

Create this third file:

```rust title="runs/ownership/live_borrow.rs — intentional failure"
fn main() {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&[7u8, 99, 0, 255]);
    let payload = &bytes[1..4];
    bytes.push(9);
    println!("{payload:?}");
}
```

Check it:

```sh title="Terminal — expect a nonzero compiler exit"
rustc --edition=2024 runs/ownership/live_borrow.rs -o runs/ownership/live_borrow
```

Expect a conflicting mutable/immutable borrow diagnostic, normally [E0502](https://doc.rust-lang.org/error_codes/E0502.html). Trace all three points: the slice creates a shared borrow, `push` needs exclusive access to the vector, and the final print keeps the shared borrow needed across that mutation.

This fixture intentionally has spare capacity. Even though this particular push can fit, `push` still requires `&mut Vec<u8>`; the compiler does not replace that interface with a special permission based on a capacity calculation. With a full vector, growth can also relocate storage and invalidate an address. The borrowing API prevents that entire class of overlapping use rather than asking the caller to guess whether this allocation will move.

The desired result is to preserve the old payload while allowing mutation, so detach it. Replace the entire file with:

```rust title="runs/ownership/live_borrow.rs — repaired complete file"
fn main() {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&[7u8, 99, 0, 255]);
    let saved = bytes[1..4].to_vec();
    bytes.push(9);
    println!("{saved:?}");
    println!("{bytes:?}");
}
```

Repeat the compilation command; after success, run `./runs/ownership/live_borrow`. Expect:

```text title="Expected stdout — repaired live_borrow"
[99, 0, 255]
[7, 99, 0, 255, 9]
```

There is also a no-copy design when the payload is only needed **before** the push: move its print before the mutation and never use that reference afterward. The source owner remains alive, but its shared borrow is no longer required. Predict that result from the last use, not from whether the `payload` binding is still textually in scope.

Do not diagnose a failed compilation by executing an older binary at the same output path. Compilation failures can leave a previous successful executable there. Check the compiler's exit status and run only after a successful build. The three repaired scratch files can remain in `runs/ownership` for comparison; they are not Cargo targets or part of the required book source assets.

## Diagnose the smallest incorrect relationship

| Symptom | Likely cause | Focused correction |
| --- | --- | --- |
| `Vec` has capacity four but indexing at zero panics | Capacity was mistaken for initialized length. | Use `push`/`extend_from_slice` to initialize the four bytes. |
| The saved value still prevents mutation | `saved` is a slice reference, not `Vec<u8>`. | Inspect the type; use `to_vec()` at the intended ownership boundary. |
| Adding `'a` or `'static` does not fix a return | The returned reference still points into a local owner. | Return owned bytes, or borrow storage supplied by the caller. |
| A payload write fails despite `let mut` | The value is `&[u8]`, so only the binding is mutable. | Obtain `&mut [u8]` through a legitimately exclusive borrow. |
| A later print makes earlier mutation stop compiling | That print extends the required borrow across the mutation. | Reorder use, narrow the borrow, or detach the needed bytes. |
| A view unexpectedly includes the tag | The range starts at index zero. | Use `HEADER_LEN..RECORD_LEN`; the payload has length three. |
| The text example expects length one | A byte count was treated as a character count. | Inspect the two UTF-8 bytes; keep binary payloads separate from text. |
| Only six tests run | The command selected the library but not the example's tests. | Select `--example borrowed_record` or `--all-targets`. |

The intended repairs change ownership, access timing, or the interface. They do not silence errors with arbitrary clones, remove inconvenient assertions, or expose raw pointers. A smaller borrow and a clearer owner are often more important than a more elaborate type annotation.

## Leave a runnable foundation for fallible parsing

As a focused variation, separate the copied scalar header from the borrowed payload. Add this **optional ninth test** inside the existing `mod tests` block:

```rust title="Optional addition — inside mod tests, not a new module"
#[test]
fn a_copied_tag_survives_replacement() {
    let mut record = OwnedRecord::new(7, [b'c', 0, 255]);
    let old_tag = view_record(record.as_bytes()).tag;
    record.replace(8, [b'd', b'o', b'g']);
    assert_eq!(old_tag, 7);
    assert_eq!(view_record(record.as_bytes()).tag, 8);
}
```

Predict why this compiles without copying a payload: `old_tag` is a `u8`, not a reference. Its use does not retain a borrow of the original storage. In contrast, saving `.payload` without `to_vec()` and using it after `replace` would recreate the live-borrow conflict. Run the optional test when added; the example then has nine tests instead of eight. The reference file contains only the eight required tests.

Finish with the original workspace and the new example intact:

```sh title="Terminal — final cumulative checks"
cargo fmt --all -- --check
cargo check -p language-lab --all-targets
cargo test -p language-lab --all-targets
cargo test -p language-lab --all-targets --release
cargo run -p language-lab --example workspace_probe
cargo run -p language-lab --example borrowed_record
```

Without the optional addition, expect the six Chapter 1 library tests plus eight Chapter 2 example tests, **14 in total across separate test executables**. `workspace_probe` has no unit-test functions of its own; its normal executable still performs the Chapter 1 assertions. Its four output lines are unchanged. `borrowed_record` produces the nine lines above. A successful test harness run is not evidence that either example's ordinary `main` ran, so retain both `cargo run` commands.

The complete new file is available as [the Chapter 2 reference example](https://25d-228.github.io/rust-learning/media/chapter-02/crates/language-lab/examples/borrowed_record.rs). Place it at the matching `examples/` path in the Chapter 1 workspace; it is not a second Cargo workspace. The exact code is already present in the filename-labelled blocks above. Build a scratch copy or the learner workspace, not the book's media directory.

The result is deliberately modest in format and precise in ownership. A view cannot outlive its source borrow; changing a buffer requires ending conflicting borrows; and an owned copy can survive both mutation and destruction of the old owner. [Chapter 3's planned capability](https://25d-228.github.io/rust-learning/index.html#chapter-3-enums-fallible-parsing-and-composed-errors) adds explicit incomplete/invalid outcomes, checked lengths, and composed errors to those same boundaries. No fallible parser, storage module, or future helper is required to run this chapter.
