# Chapter 7, Part 1 — Record bytes and typed identities

A restart has no access to the objects that were in memory before the process stopped. It has files. This chapter gives those files an explicit byte format, separates incomplete bytes from invalid bytes, and establishes who may open the data directory for mutation. It also gives later storage algorithms one narrow interface through which short writes and failures can be reproduced deliberately.

**Prerequisites:** Chapters 1–6 and their complete `language-lab` workspace, including Chapter 6's Tokio manifest update. None of the private types inside earlier examples is imported into the broker. Their ownership, parsing, trait, and cleanup techniques are reused through complete new definitions below.

**Implementation time for all of Chapter 7:** 3.5 hours: 2.5 for the crate, format, I/O seam, and directory guard; 1.0 for the byte fixtures, boundary checks, and process-lock experiment. Reading is excluded. Record actual implementation and diagnosis time separately; do not stretch a finished task to match the estimate.

> **Learning objectives**
> Distinguish a typed logical offset from a byte position; encode a stable binary format without serializing Rust object memory; validate a fixed header before trusting its payload length; preserve the distinction between an incomplete input and corruption; loop correctly over short I/O; and keep exclusive directory ownership attached to a live file handle.

By the end, `record_probe` decodes a record and reports a partial body correctly. Three integration-test targets check the codec, real-file I/O boundary, and directory guard. A separate process harness demonstrates exclusion and reacquisition after both normal exit and termination. There is no append log, sparse index, batching owner, recovery repair, network service, or durability acknowledgement yet. Those mechanisms will use these interfaces rather than being hidden inside them.

Chapter 7 has two connected parts and one implementation milestone. This part defines identities and the byte format; [Part 2 — File I/O and directory ownership](https://25d-228.github.io/rust-learning/chapter-07-part-02.html) completes the I/O seam, directory guard, and runnable integration. Finish both parts before running Cargo against the new crate; its module declarations name files supplied in Part 2.

## Add the core crate without disturbing the foundations

Work in the same learner workspace, not inside the book's media directory. Replace only the root manifest with the following. Keep `crates/language-lab/` exactly as it stands after Chapter 6.

```toml title="Cargo.toml"
[workspace]
members = ["crates/language-lab", "crates/broker-core"]
resolver = "3"
```

Create `crates/broker-core/Cargo.toml`:

```toml title="crates/broker-core/Cargo.toml"
[package]
name = "broker-core"
version = "0.1.0"
edition = "2024"
rust-version = "1.89"

[dependencies]
crc32fast = "1"

[[bin]]
name = "directory-probe"
path = "src/bin/directory_probe.rs"
```

The genuine new compiler requirement is `File::try_lock`, stabilized in Rust 1.89. The [`File` documentation](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock) identifies that API boundary. Use a compatible maintained stable compiler, not specifically 1.89; the number is a minimum needed by this implementation, not an LTS designation or a request to downgrade. Cargo's [`rust-version` field](https://doc.rust-lang.org/cargo/reference/manifest.html#the-rust-version-field) records the requirement. Dependencies must also support the selected compiler. Keep the resolved `Cargo.lock` in the learner workspace; this chapter does not fabricate one without resolving dependencies.

`crc32fast = "1"` selects a compatible 1.x checksum implementation. It is a normal dependency because the library uses it, unlike Tokio's development-only use in the foundation examples. The crate computes [IEEE CRC32](https://docs.rs/crc32fast/latest/crc32fast/); it does not choose our layout, validate identities, decide what may be repaired, or store anything. `broker-core` does not need Tokio yet. The diagnostic `directory-probe` binary is not `brokerd` and does not create broker metadata.

Create the module roots:

```rust title="crates/broker-core/src/lib.rs"
pub mod codec;
pub mod error;
pub mod io;
pub mod storage;
pub mod types;
```

```rust title="crates/broker-core/src/storage/mod.rs"
pub mod directory;
```

All later listings name their destination. For a repeated filename, append the next listing in chapter order; do not replace earlier pieces. No omitted helper is required. Complete matching reference files are under `book/media/chapter-07/`; the [root manifest](https://25d-228.github.io/rust-learning/media/chapter-07/Cargo.toml), [crate manifest](https://25d-228.github.io/rust-learning/media/chapter-07/crates/broker-core/Cargo.toml), and [module root](https://25d-228.github.io/rust-learning/media/chapter-07/crates/broker-core/src/lib.rs) can help check file placement. This is an overlay on the prerequisite workspace, not a standalone workspace containing a second copy of `language-lab`.

The five library modules separate decisions: `types` constrains identities, `error` describes failures, `codec` maps bytes to values, `io` exposes file operations, and `storage::directory` owns the process-exclusion resource. A `pub mod` makes the module reachable; public items inside it still choose which fields callers can access. See Rust's [visibility rules](https://doc.rust-lang.org/reference/visibility-and-privacy.html).

## Give unrelated integers different types

A partition number, consumer-group number, logical offset, and restart epoch may all be small integers in a demonstration. Substituting one for another is still a bug. A type alias such as `type Offset = u64` would merely rename `u64`; a tuple struct such as `struct Offset(u64)` creates a distinct type. This is the [newtype pattern](https://doc.rust-lang.org/book/ch20-03-advanced-types.html#type-safety-and-abstraction-with-the-newtype-pattern), applied to the operations we actually need.

First create the identity errors. The imports for `io` and `PathBuf` will also be used by directory errors appended later to this same file.

```rust title="crates/broker-core/src/error.rs"
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
```

`limit` is exclusive: partition 3 is permitted, partition 4 is not. `Exhausted` is different from a bad initial range: a valid counter can reach its last representable value and then have no successor. `Display` is for a human; callers still match the structured variants. The `&'static str` fields hold fixed labels, not borrowed input or heap-allocated error messages.

Now create the partition and group types:

```rust title="crates/broker-core/src/types.rs"
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
```

The tuple field is private even though the type is public. Outside `types.rs`, `PartitionId(9)` is not an escape hatch; callers use `new`, which checks the range. `Self(value)` constructs the enclosing type inside its implementation, and `.0` reads its first tuple field. `get(self)` copies a small value because these types implement `Copy`; it does not destroy a unique resource.

The derived comparison traits support sorting and ordered collections, and `Hash` supports hash-based collections. They do not permit comparing a `GroupId` directly with a `PartitionId`. No arithmetic operators or implicit dereference to integers are added: raw-number conversion stays visible at a byte or indexing boundary.

Append the two counters:

```rust title="crates/broker-core/src/types.rs"
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
```

`map(Self)` means “if the checked addition produced a number, wrap that number in this tuple struct.” `ok_or` turns the missing successor into an explicit error. No release-build wrapping or saturating counter silently reuses an old identity.

An `Offset` may equal `u64::MAX` because it can denote an **exclusive end** after the final representable record. A record at that value cannot be accepted: its next offset would be unrepresentable. Thus encoding and decoding a record both require `offset.next()` to succeed. The last permitted record offset is `u64::MAX - 1`. An empty exhausted segment may have base `u64::MAX`; it cannot accept another record.

An `Epoch` starts at 1 and never wraps. It will fence delivery identities after restart; it is **not** a per-record sequence or part of the segment header. Persistently allocating epochs belongs to Chapter 10. An `Offset` alone also does not identify its partition. Comparing offsets from different partitions still requires the correct enclosing state; this newtype does not encode a partition into the Rust type system.

## Specify bytes rather than Rust memory

The persistent format is version 1. All multibyte integers use **big-endian** order: the most significant byte comes first. This is a format decision, not a claim about the M4's native order. The byte sequence for integer 3 in a four-byte field is `00 00 00 03`.

A segment begins with one 32-byte segment header, followed immediately by zero or more records. Each record consists of a 32-byte record header followed immediately by its payload. There is no padding, alignment gap, compression, key field, timestamp, or terminal marker in version 1. Arbitrary bytes, including zero bytes and non-UTF-8 values, are valid payload contents. An empty payload is valid.

### The segment header

Byte ranges below are half-open: `8..10` includes bytes 8 and 9, not byte 10.

| Range | Width | Meaning |
| --- | ---: | --- |
| `0..4` | 4 | ASCII magic `PBLS` |
| `4..6` | 2 | Version: `1` |
| `6..8` | 2 | Header size: `32` |
| `8..10` | 2 | Partition number: `0..4`, exclusive at 4 |
| `10..12` | 2 | Reserved; all zero |
| `12..20` | 8 | Base logical offset |
| `20..28` | 8 | Reserved; all zero |
| `28..32` | 4 | IEEE CRC32 of bytes `0..28` |

The base is the offset of the first record if one exists. Segment identity lives here rather than being repeated in every record. The future segment reader must compare the header's partition/base with the requested partition and filename, and verify consecutive record offsets. The codec alone cannot know those external expectations.

### The record header

| Range | Width | Meaning |
| --- | ---: | --- |
| `0..4` | 4 | ASCII magic `PBLR` |
| `4..6` | 2 | Version: `1` |
| `6..8` | 2 | Header size: `32` |
| `8..16` | 8 | Logical offset; a successor must be representable |
| `16..20` | 4 | Payload length in bytes, at most 1,048,576 |
| `20..24` | 4 | IEEE CRC32 of payload bytes only |
| `24..28` | 4 | Reserved; all zero |
| `28..32` | 4 | IEEE CRC32 of bytes `0..28` |
| `32..32 + length` | variable | Payload bytes |

Both headers have their own checksum. A corrupted length must not make a reader allocate a huge buffer or interpret a complete damaged record as an indefinitely incomplete body. The header checksum also covers the expected payload checksum; changing that expectation alone invalidates the header.

A checksum detects accidental changes; it is not authentication. A party that can rewrite the bytes can also recompute the checksum, and checksums have collisions. The [CRC documentation](https://docs.python.org/3/library/zlib.html#zlib.crc32) explicitly excludes cryptographic authentication. Our tests intentionally recompute header CRCs to reach semantic checks such as an unsupported version or forbidden length. Passing the checksum is necessary, not sufficient, for validity.

Do not dump a `SegmentHeader` struct to disk, use `transmute`, or derive offsets from `size_of`. Rust's [type-layout rules](https://doc.rust-lang.org/reference/type-layout.html) do not make a default struct layout a portable file format. Even a chosen C-compatible representation would not settle integer byte order or checksum coverage. Every field below is written with a visible slice range.

## Separate malformed bytes from unavailable bytes

Append the format errors to `error.rs`:

```rust title="crates/broker-core/src/error.rs"
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
```

There is deliberately no `Incomplete` error variant here. A parser can have too few bytes without having evidence that the bytes are invalid. Whether that condition means “read more,” “clean end,” “eligible active-tail repair,” or “startup failure” depends on the file's role and the recovery protocol. This codec reports facts and never truncates a file.

`FormatError::Identity` preserves the original `IdError` as its cause, using the error-composition technique from Chapter 3. Header and payload checksum errors are distinct because they occur before and after trusting the validated body length. `{stored:08x}` formats a checksum as eight hexadecimal digits with leading zeros; it is diagnostic text, not serialization.

Create the codec's types and constants:

```rust title="crates/broker-core/src/codec.rs"
use crate::error::FormatError;
use crate::types::{Offset, PartitionId};

pub const HEADER_LEN: usize = 32;
pub const MAX_PAYLOAD: usize = 1024 * 1024;
const VERSION: u16 = 1;
const SEGMENT_MAGIC: &[u8; 4] = b"PBLS";
const RECORD_MAGIC: &[u8; 4] = b"PBLR";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probe<T> {
    Incomplete { required: usize },
    Complete { value: T, consumed: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub partition: PartitionId,
    pub base: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    offset: Offset,
    payload_len: usize,
    total_len: usize,
    payload_crc: u32,
}

impl RecordHeader {
    pub fn offset(self) -> Offset { self.offset }
    pub fn payload_len(self) -> usize { self.payload_len }
    pub fn total_len(self) -> usize { self.total_len }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordView<'a> {
    pub offset: Offset,
    pub payload: &'a [u8],
}
```

`Probe<T>` generalizes Chapter 3's inspection result without changing its consumption rule. A successful inspection either needs a specified **total** number of input bytes or returns a value plus the byte count belonging to it. Extra coalesced bytes remain the caller's responsibility.

`RecordHeader` has private fields: only the validating decoder constructs it. Its getters provide a checked body size to Chapter 8's bounded file reader. A public `RecordView` is only a view of supplied bytes, not a durability certificate; its lifetime ties the payload to the input slice, as in Chapter 2. Copying a view copies metadata and a reference, not payload ownership.

Let $H$ be the header size, $L$ the payload size, $T$ the total record size, and $M$ the maximum payload size, all in bytes. The required arithmetic is:

$$
H=32,\qquad M=1{,}048{,}576,\qquad 0\le L\le M,\qquad T=H+L.
$$

The maximum encoded record is therefore 1,048,608 bytes. With a three-byte `cat` payload, the total is 35. If the segment header starts at file byte 0, the first record starts at byte 32, its payload occupies `64..67`, and the next record starts at byte 67. Its logical offset can be 7; offset 7 does **not** mean file byte 7.

More generally, for record index $i$ counted from zero, let $P_i$ be its starting file byte and $L_j$ the payload length of each preceding record $j$. Then:

$$
P_i=32+\sum_{j=0}^{i-1}(32+L_j).
$$

For the first record the sum is empty, so the position is 32. This sum becomes repeated checked position advancement in the segment reader, not a giant allocation or a conversion from logical offset to byte position.

## Read fixed-width integers without reinterpreting memory

Append the private helpers:

```rust title="crates/broker-core/src/codec.rs"
fn u16_at(h: &[u8; HEADER_LEN], start: usize) -> u16 {
    u16::from_be_bytes(h[start..start + 2].try_into().expect("fixed u16 field"))
}

fn u32_at(h: &[u8; HEADER_LEN], start: usize) -> u32 {
    u32::from_be_bytes(h[start..start + 4].try_into().expect("fixed u32 field"))
}

fn u64_at(h: &[u8; HEADER_LEN], start: usize) -> u64 {
    u64::from_be_bytes(h[start..start + 8].try_into().expect("fixed u64 field"))
}

fn header_prefix(bytes: &[u8]) -> Option<&[u8; HEADER_LEN]> {
    bytes.get(..HEADER_LEN)?.try_into().ok()
}
```

`&[u8; HEADER_LEN]` borrows an array whose length is known at compile time. It is different from `&[u8]`, which borrows a slice with a runtime length. In `header_prefix`, `get(..32)` returns `None` rather than panicking on a short input. `?` propagates that `None` from this `Option`-returning helper. The subsequent slice-to-array-reference conversion borrows the same 32 bytes; `.ok()` converts its `Result` into an `Option`. See [`TryInto`](https://doc.rust-lang.org/std/convert/trait.TryInto.html).

Inside the integer helpers, `try_into()` instead creates a small fixed array of two, four, or eight bytes. Every caller passes a fixed field start from the tables, and every source is already a 32-byte array. The `expect` statements document an internal bounds fact; they never validate a caller-declared payload size. A panic there would indicate a programming error in the layout constants, not a corrupt-file outcome. No unaligned pointer cast is used.

[`from_be_bytes` and `to_be_bytes`](https://doc.rust-lang.org/std/primitive.u64.html#method.to_be_bytes) keep native integer representation out of the format. For example, four input bytes $b_0,b_1,b_2,b_3$ represent:

$$
256^3b_0+256^2b_1+256b_2+b_3.
$$

The bytes `00 00 01 00` therefore mean 256, not 1. These byte weights explain both the decoding operation and why a native-endian memory dump would be the wrong implementation.

Append the shared header construction and validation:

```rust title="crates/broker-core/src/codec.rs"
fn start_header(magic: &[u8; 4]) -> [u8; HEADER_LEN] {
    let mut h = [0; HEADER_LEN];
    h[0..4].copy_from_slice(magic);
    h[4..6].copy_from_slice(&VERSION.to_be_bytes());
    h[6..8].copy_from_slice(&32_u16.to_be_bytes());
    h
}

fn seal_header(h: &mut [u8; HEADER_LEN]) {
    let crc = crc32fast::hash(&h[..28]);
    h[28..32].copy_from_slice(&crc.to_be_bytes());
}

fn check_header(h: &[u8; HEADER_LEN], magic: &[u8; 4]) -> Result<(), FormatError> {
    let stored = u32_at(h, 28);
    let computed = crc32fast::hash(&h[..28]);
    if stored != computed {
        return Err(FormatError::HeaderChecksum { stored, computed });
    }
    if &h[..4] != magic { return Err(FormatError::Magic); }
    let version = u16_at(h, 4);
    if version != VERSION { return Err(FormatError::Version(version)); }
    let size = u16_at(h, 6);
    if size != 32 { return Err(FormatError::HeaderLength(size)); }
    Ok(())
}
```

`start_header` begins with zeros, so every reserved byte has a defined value. It fills only the common prefix. `seal_header` runs **after** all format-specific fields have been set; it excludes its own checksum field. Computing the checksum before setting the length or payload checksum would produce a self-inconsistent record.

The validator reads 28 fixed bytes and the fixed checksum location first. Only after they agree does it interpret magic, version, and header size. An unsupported future format is rejected; the current decoder does not guess a different layout. A changed version byte without an updated CRC produces `HeaderChecksum`; a self-consistent header claiming version 2 produces `Version(2)`. That error precedence is deliberate.

The fixed literal `32_u16` is the wire field's integer value, whereas `HEADER_LEN` is a `usize` for indexing memory. Keeping the conversion explicit avoids pretending that a machine-sized integer is a persistent field. A new header size requires a new, fully specified format, not changing one constant in isolation.

## Encode and inspect a segment header

Append the segment functions:

```rust title="crates/broker-core/src/codec.rs"
pub fn encode_segment(header: SegmentHeader) -> [u8; HEADER_LEN] {
    let mut h = start_header(SEGMENT_MAGIC);
    h[8..10].copy_from_slice(&header.partition.get().to_be_bytes());
    h[12..20].copy_from_slice(&header.base.get().to_be_bytes());
    seal_header(&mut h);
    h
}

pub fn decode_segment(bytes: &[u8]) -> Result<Probe<SegmentHeader>, FormatError> {
    let Some(h) = header_prefix(bytes) else {
        return Ok(Probe::Incomplete { required: HEADER_LEN });
    };
    check_header(h, SEGMENT_MAGIC)?;
    if h[10..12].iter().chain(h[20..28].iter()).any(|b| *b != 0) {
        return Err(FormatError::Reserved);
    }
    let partition = PartitionId::new(u16_at(h, 8)).map_err(FormatError::Identity)?;
    let base = Offset::new(u64_at(h, 12));
    Ok(Probe::Complete {
        value: SegmentHeader { partition, base }, consumed: HEADER_LEN,
    })
}
```

`let Some(h) = ... else { return ...; };` either binds the complete header or returns immediately. The `else` branch must leave this control-flow path, so `h` is available afterward. This is the [let-else form](https://doc.rust-lang.org/book/ch06-03-if-let.html#staying-on-the-happy-path-with-letelse), not an unchecked extraction.

The chained iterators visit the two reserved ranges without copying them into a temporary vector. `any` returns as soon as it sees a nonzero byte. The partition constructor then checks the decoded integer; the base accepts any representable `u64` because an exhausted empty segment can still be described. The parser consumes exactly 32 bytes even if a record follows in the same input slice.

For partition 2 and base offset 7, the complete header is:

```text title="Segment header: 32 bytes, hexadecimal"
50 42 4c 53 00 01 00 20 00 02 00 00 00 00 00 00
00 00 00 07 00 00 00 00 00 00 00 00 f8 35 0e 5f
```

The final four bytes encode checksum `0xf8350e5f`. That value covers the preceding 28 bytes, including both reserved ranges. It is a fixture calculated from the specified bytes, not a value obtained by round-tripping this Rust encoder.

## Validate lengths before allocating an encoded record

Append the sizing and encoding functions:

```rust title="crates/broker-core/src/codec.rs"
pub fn record_len(payload_len: usize) -> Result<usize, FormatError> {
    let total = HEADER_LEN.checked_add(payload_len).ok_or(FormatError::LengthOverflow)?;
    if payload_len > MAX_PAYLOAD {
        return Err(FormatError::TooLarge { length: payload_len, limit: MAX_PAYLOAD });
    }
    Ok(total)
}

pub fn encode_record(offset: Offset, payload: &[u8]) -> Result<Vec<u8>, FormatError> {
    offset.next().map_err(FormatError::Identity)?;
    let total = record_len(payload.len())?;
    let length = u32::try_from(payload.len()).map_err(|_| FormatError::LengthOverflow)?;
    let mut h = start_header(RECORD_MAGIC);
    h[8..16].copy_from_slice(&offset.get().to_be_bytes());
    h[16..20].copy_from_slice(&length.to_be_bytes());
    h[20..24].copy_from_slice(&crc32fast::hash(payload).to_be_bytes());
    seal_header(&mut h);
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&h);
    bytes.extend_from_slice(payload);
    Ok(bytes)
}
```

`record_len` first checks addition and then applies the inclusive format maximum. Testing `usize::MAX` exercises overflow without attempting to allocate that many bytes. A normal length of 1,048,577 fits the machine integer but violates the format policy and yields `TooLarge`. The later `u32::try_from` makes the wire conversion fallible explicitly, even though the chosen maximum already guarantees it on the target machine.

`encode_record` rejects an exhausted record offset and an invalid length **before** building a heap vector. It reads the caller's payload, calculates its CRC, seals the header, then copies the header and payload into the returned owner. The caller's slice is unchanged. The vector contains exactly the record bytes, not spare-capacity bytes.

The one-record bound is not a broker-wide memory budget. During encoding, both the caller's payload and the new encoded vector can exist. For a maximum-size payload their combined initialized byte lengths are 2,097,184, plus the 32-byte local header and ordinary object overhead. Allocator capacity may be larger; [`Vec::with_capacity`](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.with_capacity) is not a global memory limit. Allocation failure is not represented as `FormatError` here. Chapter 15 will account for live owners and admission separately.

The CRC for `cat` is `0x9e5e43a8`. At logical offset 7, the complete record is:

```text title="Record header and cat payload: 35 bytes, hexadecimal"
50 42 4c 52 00 01 00 20 00 00 00 00 00 00 00 07
00 00 00 03 9e 5e 43 a8 00 00 00 00 89 ee 57 48
63 61 74
```

The payload CRC occupies `20..24`; `0x89ee5748` at `28..32` is the header CRC. They cover different byte ranges. Substituting CRC32C for IEEE CRC32 would change the format even though both results fit four bytes.

## Make header validation usable before the body read

Append the final codec functions:

```rust title="crates/broker-core/src/codec.rs"
pub fn decode_record_header(h: &[u8; HEADER_LEN]) -> Result<RecordHeader, FormatError> {
    check_header(h, RECORD_MAGIC)?;
    if u32_at(h, 24) != 0 { return Err(FormatError::Reserved); }
    let offset = Offset::new(u64_at(h, 8));
    offset.next().map_err(FormatError::Identity)?;
    let payload_len = usize::try_from(u32_at(h, 16)).map_err(|_| FormatError::LengthOverflow)?;
    let total_len = record_len(payload_len)?;
    Ok(RecordHeader { offset, payload_len, total_len, payload_crc: u32_at(h, 20) })
}

pub fn decode_record(bytes: &[u8]) -> Result<Probe<RecordView<'_>>, FormatError> {
    let Some(h) = header_prefix(bytes) else {
        return Ok(Probe::Incomplete { required: HEADER_LEN });
    };
    let header = decode_record_header(h)?;
    if bytes.len() < header.total_len {
        return Ok(Probe::Incomplete { required: header.total_len });
    }
    let payload = &bytes[HEADER_LEN..header.total_len];
    let computed = crc32fast::hash(payload);
    if header.payload_crc != computed {
        return Err(FormatError::PayloadChecksum { stored: header.payload_crc, computed });
    }
    Ok(Probe::Complete {
        value: RecordView { offset: header.offset, payload }, consumed: header.total_len,
    })
}
```

`decode_record_header` accepts only a fixed-size header. It validates the common fields, reserved bits, offset successor, payload-length conversion, and total size before returning a `RecordHeader`. Chapter 8 can read into a 32-byte stack array, call this function, and only then allocate a body buffer of the checked size. The API does not require allocating the untrusted body just to validate its header.

`decode_record` itself never allocates or consumes input. Its returned payload borrows only `32..total_len`. Additional bytes are neither checksummed as this record nor discarded. Before full body availability, it returns the total required length; after full availability, a bad payload checksum is corruption, never “need more.”

Trace the same record as bytes become available:

| Available bytes | Inspection result | What has been trusted? |
| ---: | --- | --- |
| 0 | `Incomplete { required: 32 }` | Nothing about a header |
| 31 | `Incomplete { required: 32 }` | Still no complete header |
| 32 | `Incomplete { required: 35 }` | Validated length, offset, and expected payload CRC |
| 34 | `Incomplete { required: 35 }` | The body is still short |
| 35 | `Complete`, consumed 35 | Header and payload checks both succeeded |
| 70, with a second record attached | First `Complete`, consumed 35 | Only the first record is inspected |

Now change the four length bytes to `ff ff ff ff` without changing the header CRC. The result is `HeaderChecksum`, not a request for roughly four gigabytes. Recompute the header CRC while keeping that length: the result becomes `TooLarge`, still before waiting for or allocating a body. Finally change the last byte of a complete `cat` record: the result is `PayloadChecksum`.

At end-of-file, an empty record-start read can be a clean end. A nonempty short record in the active segment is only a **candidate** for later repair. The codec cannot tell whether the bytes are in the active segment, whether an append was interrupted, or whether earlier offsets are contiguous. A short immutable segment and a complete bad checksum are not repairable by simply discarding data. Do not scan forward for the next magic sequence: those bytes can occur inside a payload, and skipping damage would destroy the ordering contract.

## Run the byte-level demonstration

Create the complete example:

```rust title="crates/broker-core/examples/record_probe.rs"
use std::error::Error;

use broker_core::codec::{Probe, SegmentHeader, decode_record, encode_record, encode_segment};
use broker_core::types::{Offset, PartitionId};

fn main() -> Result<(), Box<dyn Error>> {
    let segment = encode_segment(SegmentHeader {
        partition: PartitionId::new(2)?, base: Offset::new(7),
    });
    let record = encode_record(Offset::new(7), b"cat")?;
    let short = decode_record(&record[..34])?;
    assert!(matches!(short, Probe::Incomplete { required: 35 }));
    if let Probe::Complete { value, consumed } = decode_record(&record)? {
        assert_eq!(value.payload, b"cat");
        println!("segment={} record={} offset={} payload={} consumed={}",
            segment.len(), record.len(), value.offset.get(), value.payload.len(), consumed);
    } else {
        panic!("complete fixture was not decoded");
    }
    println!("34 bytes: incomplete; need 35 total");
    Ok(())
}
```

After completing the remaining library files in [Part 2](https://25d-228.github.io/rust-learning/chapter-07-part-02.html), run:

```sh
cargo run -p broker-core --example record_probe
```

Expected standard output:

```text
segment=32 record=35 offset=7 payload=3 consumed=35
34 bytes: incomplete; need 35 total
```

The program deliberately uses a payload slice rather than interpreting arbitrary payloads as text. Its `Result<(), Box<dyn Error>>` composes the concrete format and identity errors for an executable boundary, while the library retains structured errors. This is the same distinction taught in Chapter 3.

## Use independent golden bytes, then break one rule at a time

An encoder and decoder can agree on the same wrong layout. A round-trip test alone would not notice. Create `tests/codec.rs` with literal expected bytes calculated from the format tables, plus a helper that deliberately updates a damaged header's CRC when a test needs to reach deeper validation.

```rust title="crates/broker-core/tests/codec.rs"
use broker_core::codec::{
    HEADER_LEN, MAX_PAYLOAD, Probe, RecordView, SegmentHeader, decode_record,
    decode_record_header, decode_segment, encode_record, encode_segment, record_len,
};
use broker_core::error::{FormatError, IdError};
use broker_core::types::{Epoch, GroupId, Offset, PartitionId};

const SEGMENT: [u8; 32] = [
    0x50, 0x42, 0x4c, 0x53, 0x00, 0x01, 0x00, 0x20,
    0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0xf8, 0x35, 0x0e, 0x5f,
];
const RECORD: [u8; 35] = [
    0x50, 0x42, 0x4c, 0x52, 0x00, 0x01, 0x00, 0x20,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
    0x00, 0x00, 0x00, 0x03, 0x9e, 0x5e, 0x43, 0xa8,
    0x00, 0x00, 0x00, 0x00, 0x89, 0xee, 0x57, 0x48,
    0x63, 0x61, 0x74,
];

fn reseal(bytes: &mut [u8]) {
    let crc = crc32fast::hash(&bytes[..28]);
    bytes[28..32].copy_from_slice(&crc.to_be_bytes());
}
```

The golden arrays are not generated by `encode_record` during the test. Their CRCs can be independently checked with Python's standard `zlib` implementation:

```sh
python3 - <<'PYCODE'
import struct
import zlib
segment_prefix = struct.pack('>4sHHHHQQ', b'PBLS', 1, 32, 2, 0, 7, 0)
record_prefix = struct.pack('>4sHHQIII', b'PBLR', 1, 32, 7, 3, zlib.crc32(b'cat'), 0)
assert len(segment_prefix) == len(record_prefix) == 28
assert zlib.crc32(segment_prefix) == 0xf8350e5f
assert zlib.crc32(record_prefix) == 0x89ee5748
assert zlib.crc32(b'cat') == 0x9e5e43a8
print('segment=f8350e5f record=89ee5748 payload=9e5e43a8')
PYCODE
```

The `>` prefix requests big-endian standard field sizes; `4s`, `H`, `I`, and `Q` contribute four raw bytes and two-, four-, and eight-byte integers respectively. This small independent calculation checks fixture construction; it is not execution of the Rust decoder.

Append identity, checksum-variant, and golden-layout checks:

```rust title="crates/broker-core/tests/codec.rs"
#[test]
fn identifiers_check_ranges_and_successors() {
    assert_eq!(PartitionId::new(3).unwrap().get(), 3);
    assert!(PartitionId::new(4).is_err());
    assert_eq!(GroupId::new(1).unwrap().get(), 1);
    assert!(GroupId::new(2).is_err());
    assert_eq!(Epoch::new(0), Err(IdError::ZeroEpoch));
    assert_eq!(Epoch::new(1).unwrap().next().unwrap().get(), 2);
    assert_eq!(Epoch::new(u64::MAX).unwrap().next(), Err(IdError::Exhausted("epoch")));
    assert_eq!(Offset::new(u64::MAX).next(), Err(IdError::Exhausted("offset")));
}

#[test]
fn crc_variant_is_ieee_not_crc32c() {
    assert_eq!(crc32fast::hash(b"123456789"), 0xcbf4_3926);
    assert_eq!(crc32fast::hash(b""), 0);
}

#[test]
fn segment_matches_independent_golden_bytes() {
    let header = SegmentHeader { partition: PartitionId::new(2).unwrap(), base: Offset::new(7) };
    assert_eq!(encode_segment(header), SEGMENT);
    assert_eq!(decode_segment(&SEGMENT).unwrap(), Probe::Complete { value: header, consumed: 32 });
}

#[test]
fn record_matches_independent_golden_bytes() {
    assert_eq!(encode_record(Offset::new(7), b"cat").unwrap(), RECORD.to_vec());
    assert_eq!(decode_record(&RECORD).unwrap(), Probe::Complete {
        value: RecordView { offset: Offset::new(7), payload: b"cat" }, consumed: 35,
    });
}
```

The `123456789` check distinguishes the intended CRC variant. The two golden tests constrain encoding and decoding separately. Notice that the types permit the exclusive end at the integer limit while `next` refuses another increment. These are observable domain rules, not accidental overflow behavior.

Append availability and size boundaries:

```rust title="crates/broker-core/tests/codec.rs"
#[test]
fn every_short_header_and_body_is_incomplete() {
    for end in 0..HEADER_LEN {
        assert_eq!(decode_segment(&SEGMENT[..end]).unwrap(), Probe::Incomplete { required: 32 });
        assert_eq!(decode_record(&RECORD[..end]).unwrap(), Probe::Incomplete { required: 32 });
    }
    for end in HEADER_LEN..RECORD.len() {
        assert_eq!(decode_record(&RECORD[..end]).unwrap(), Probe::Incomplete { required: 35 });
    }
}

#[test]
fn empty_payload_is_a_complete_record() {
    let bytes = encode_record(Offset::new(0), b"").unwrap();
    assert_eq!(bytes.len(), HEADER_LEN);
    assert_eq!(decode_record(&bytes).unwrap(), Probe::Complete {
        value: RecordView { offset: Offset::new(0), payload: b"" }, consumed: HEADER_LEN,
    });
}

#[test]
fn limit_is_inclusive_and_checked_before_allocation() {
    let payload = vec![0xa5; MAX_PAYLOAD];
    let bytes = encode_record(Offset::new(0), &payload).unwrap();
    assert_eq!(bytes.len(), HEADER_LEN + MAX_PAYLOAD);
    let Probe::Complete { value, .. } = decode_record(&bytes).unwrap() else { panic!("incomplete") };
    assert_eq!(value.payload, payload.as_slice());
    assert_eq!(record_len(MAX_PAYLOAD + 1), Err(FormatError::TooLarge {
        length: MAX_PAYLOAD + 1, limit: MAX_PAYLOAD,
    }));
    let mut h: [u8; HEADER_LEN] = RECORD[..HEADER_LEN].try_into().unwrap();
    h[16..20].copy_from_slice(&1_048_577_u32.to_be_bytes());
    reseal(&mut h);
    assert!(matches!(decode_record(&h), Err(FormatError::TooLarge { .. })));
}

#[test]
fn length_arithmetic_cannot_wrap() {
    assert_eq!(record_len(usize::MAX), Err(FormatError::LengthOverflow));
}
```

The prefix loop tests every cut before a full header and every cut in the three-byte body. A valid zero-length payload must not be confused with “no record.” The maximum payload test owns about two MiB of initialized input/output data and verifies the inclusive boundary. Its separate oversized-header case supplies only 32 bytes: it must fail immediately, without demanding an oversized body. This test observes the result; the no-allocation conclusion also relies on inspecting the header-decoder code, which contains no allocation operation.

Append checks that distinguish integrity from semantics:

```rust title="crates/broker-core/tests/codec.rs"
#[test]
fn untrusted_length_fails_header_crc_before_requesting_a_body() {
    let mut h: [u8; HEADER_LEN] = RECORD[..HEADER_LEN].try_into().unwrap();
    h[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(decode_record(&h), Err(FormatError::HeaderChecksum { .. })));
    reseal(&mut h);
    assert!(matches!(decode_record(&h), Err(FormatError::TooLarge { .. })));
}

#[test]
fn valid_crc_does_not_make_unsupported_fields_valid() {
    for (index, value, expected) in [
        (0, b'X', FormatError::Magic),
        (5, 2, FormatError::Version(2)),
        (7, 33, FormatError::HeaderLength(33)),
        (24, 1, FormatError::Reserved),
    ] {
        let mut bytes = RECORD;
        bytes[index] = value;
        reseal(&mut bytes);
        assert_eq!(decode_record(&bytes), Err(expected));
    }
}

#[test]
fn segment_identity_and_reserved_bytes_are_checked() {
    let mut bytes = SEGMENT;
    bytes[8..10].copy_from_slice(&4_u16.to_be_bytes());
    reseal(&mut bytes);
    assert!(matches!(decode_segment(&bytes), Err(FormatError::Identity(_))));
    for index in [10, 11, 20, 27] {
        let mut bytes = SEGMENT;
        bytes[index] = 1;
        reseal(&mut bytes);
        assert_eq!(decode_segment(&bytes), Err(FormatError::Reserved));
    }
}

#[test]
fn a_record_requires_a_representable_exclusive_end() {
    assert!(encode_record(Offset::new(u64::MAX - 1), b"").is_ok());
    assert!(matches!(encode_record(Offset::new(u64::MAX), b""), Err(FormatError::Identity(_))));
    let mut bytes = RECORD;
    bytes[8..16].copy_from_slice(&u64::MAX.to_be_bytes());
    reseal(&mut bytes);
    assert!(matches!(decode_record(&bytes), Err(FormatError::Identity(_))));
}
```

The first malformed-length test deliberately leaves the old CRC in place and expects a header-checksum error. After `reseal`, the same length reaches the maximum-size check. The table-driven test changes one semantic field, reseals, and checks the corresponding error. A decoder that only verifies CRCs would fail these tests.

The segment test also reseals the header after changing partition or reserved fields. Its job is not to prove that a checksum detects those modifications; it proves those fields remain constrained even when checksum verification succeeds. The exhausted-offset test performs the same check on both the encoder and decoder.

Finish the codec tests:

```rust title="crates/broker-core/tests/codec.rs"
#[test]
fn every_single_bit_change_in_a_complete_record_is_detected() {
    for byte in 0..RECORD.len() {
        for bit in 0..8 {
            let mut damaged = RECORD;
            damaged[byte] ^= 1_u8 << bit;
            assert!(decode_record(&damaged).is_err(), "byte {byte}, bit {bit}");
        }
    }
}

#[test]
fn payload_corruption_is_not_an_incomplete_tail() {
    let mut damaged = RECORD;
    damaged[34] ^= 1;
    assert!(matches!(decode_record(&damaged), Err(FormatError::PayloadChecksum { .. })));
}

#[test]
fn coalesced_input_consumes_one_record_and_borrows_its_bytes() {
    let mut bytes = RECORD.to_vec();
    bytes.extend_from_slice(&encode_record(Offset::new(8), b"dog").unwrap());
    let Probe::Complete { value, consumed } = decode_record(&bytes).unwrap() else { panic!("incomplete") };
    assert_eq!(consumed, 35);
    assert_eq!(value.payload.as_ptr(), bytes[HEADER_LEN..].as_ptr());
    let Probe::Complete { value: second, consumed: n } = decode_record(&bytes[consumed..]).unwrap()
        else { panic!("second record incomplete") };
    assert_eq!(second.offset, Offset::new(8));
    assert_eq!(second.payload, b"dog");
    assert_eq!(n + consumed, bytes.len());
}

#[test]
fn header_only_validation_exposes_a_bounded_read_size() {
    let h: &[u8; HEADER_LEN] = RECORD[..HEADER_LEN].try_into().unwrap();
    let header = decode_record_header(h).unwrap();
    assert_eq!(header.offset(), Offset::new(7));
    assert_eq!(header.payload_len(), 3);
    assert_eq!(header.total_len(), 35);
}

#[test]
fn rejected_input_is_not_modified() {
    let mut bytes = RECORD;
    bytes[0] ^= 1;
    let before = bytes;
    assert!(decode_record(&bytes).is_err());
    assert_eq!(bytes, before);
}
```

The bit-flip loop tries all 280 single-bit changes in the 35-byte record. This finite fixture is not a proof against all possible corruptions or CRC collisions. A changed payload is explicitly checked as `PayloadChecksum`, not incomplete input. The coalesced-record test validates the consumed byte count and compares slice pointers to show that decoding borrows the original payload storage rather than copying it.

`let Probe::Complete { ... } = ... else { panic!(...); };` is appropriate in these tests because the fixtures are intentionally complete. The production parser returns structured outcomes instead. The header-only test exercises the API a bounded reader will use, and the final test checks that inspection has not mutated rejected input.

## Continue with the I/O owner

The byte format, codec, golden tests, example, and identity types are now specified. Continue with [Chapter 7, Part 2 — File I/O and directory ownership](https://25d-228.github.io/rust-learning/chapter-07-part-02.html) to complete the declared library modules and run the whole chapter. The 3.5-hour estimate covers both parts, not each part separately.
