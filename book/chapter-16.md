# Chapter 16 — Framed TCP sessions and clients

## Part 1 — Wire bytes and bounded frame decoding

> **Learning objectives**
> Define an exact binary interface rather than relying on TCP packet boundaries; validate lengths before resource admission; retain the core's durability and lease meanings over a socket; and make a cancelled partial exchange unusable instead of accidentally resuming it.

**Implementation time: 4.5 hours for all four parts**, not for each page: 0.75 for framing and validation, 1.0 for server integration and bounded sessions, 1.25 for the client/commands, and 1.5 for wire checks and debugging. Reading is excluded. [Part 2 — Sessions that retain resource ownership](https://25d-228.github.io/rust-learning/chapter-16-part-02.html), [Part 3 — Cancellation-safe clients and loopback commands](https://25d-228.github.io/rust-learning/chapter-16-part-03.html), and [Part 4 — Fragmentation, slow peers, and ambiguity checks](https://25d-228.github.io/rust-learning/chapter-16-part-04.html) together contain the complete implementation.

**Prerequisite code:** the workspace through [Chapter 15](https://25d-228.github.io/rust-learning/chapter-15.html), including `BoundedClient`, prepared appends, pooled lease replies, reserved urgent commands, and initialized/recovered storage. Chapter 3 supplies incomplete-frame reasoning; Chapter 6 supplies future ownership and cancellation. All earlier storage, checkpoint, retained-floor, and epoch encodings stay unchanged.

The **16-file overlay** at `book/media/chapter-16/` replaces only the root `Cargo.toml` and adds fifteen files under the new `crates/broker-app/`. Apply it after overlays 1–15 in a separate implementation workspace. Every new source file appears below or in the other three parts; blocks with the same filename concatenate in their displayed order. Build after assembling all four parts. There are no new `broker-core` replacements.

The runnable result is `brokerd`, listening on two loopback ports, and `brokerctl`, which appends, fetches/processes/acknowledges, explicitly checkpoints, reads progress, and requests safe retention. This is a trusted-local-machine protocol: no authentication, TLS, compression, replication, or client-id deduplication is implied. A valid lease token is a state-machine fence, **not a cryptographic credential**.

## A successful write is still not a successful request

Chapter 15 ended with an in-process request owner and an asynchronous reply receiver. TCP adds another uncertain boundary. The client can send all request bytes and lose its connection before learning whether the partition accepted them. Conversely, the broker can synchronize an append, write the response into a kernel socket buffer, and still not know whether the application read it.

Our baseline has exactly **one outstanding operation per connection**. A client sends a complete request and waits for its complete response before sending the next. The server nevertheless handles coalesced input correctly: bytes for the next request may already exist in the kernel, but the application reads only the current fixed header and its declared body. It does not collect an arbitrary stream into an ever-growing vector. TCP is a byte stream, so a read can end anywhere; the distinction between transport bytes and complete frames is also illustrated in Tokio's [framing tutorial](https://tokio.rs/tokio/tutorial/framing).

A three-byte append occupies a 64-byte header followed by `63 61 74`, the bytes of `cat`. The stream may arrive as 2 bytes, then 63 bytes, then 2 bytes. The first two reads have not yet supplied the complete body. The header remains in a fixed array; after validation, the body goes directly into the prepared append buffer.

```text
client buffer --socket--> fixed header
                              |
                         validate shape
                              |
                   slot + pool buffer + disk permit
                              |
                    read exactly the payload length
                              |
                      submit to partition owner
                              |
                    synchronized offset --socket--> client
```

Dropping resources above `submit` cancels unsubmitted work. Dropping a reply below it does not undo the command. We will preserve this distinction in both status codes and ownership.

## Add transport, not a second storage engine

Replace the root manifest. Create the app manifest and library module exactly as follows. Tokio's `net` feature supplies sockets, `io-util` supplies the extension traits used for reads and writes, and `signal` supplies the CLI interrupt notification. The core keeps its existing feature set; Cargo combines compatible features within the build. `test-util` is used only by transport-clock tests. The existing compiler declaration is retained from the native file-lock prerequisite; use a compatible maintained toolchain rather than downgrading to this minimum.

```toml title="Cargo.toml"
[workspace]
members = ["crates/language-lab", "crates/broker-core", "crates/broker-app"]
resolver = "3"
```

```toml title="crates/broker-app/Cargo.toml"
[package]
name = "broker-app"
version = "0.1.0"
edition = "2024"
rust-version = "1.89"

[dependencies]
broker-core = { path = "../broker-core" }
tokio = { version = "1", default-features = false, features = ["rt", "macros", "sync", "time", "net", "io-util", "signal"] }

[dev-dependencies]
tokio = { version = "1", default-features = false, features = ["test-util"] }
```

```rust title="crates/broker-app/src/lib.rs"
pub mod client;
pub mod local;
pub mod protocol;
pub mod server;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
```

`protocol` has no ownership of a broker thread. `server` translates validated messages into bounded core requests. `client` owns a socket and parses replies. `local` prepares the four persistent owners before serving. The two binaries are ordinary command-line entry points, not hidden services.

## The 64-byte envelope

All multibyte numbers use **big-endian** order. Positions below are zero-based, with the end excluded. The body length counts only bytes **after** this header. There is no implicit padding on the wire and no serialization of a Rust struct's memory representation.

| Bytes | Meaning | Rule |
| --- | --- | --- |
| `0..4` | Magic | ASCII `PBNW` |
| `4` | Version | `1` |
| `5` | Operation | `1` append, `2` fetch, `3` acknowledge, `4` checkpoint, `5` progress, `6` retain |
| `6` | Status | `0` request; other values are responses |
| `7` | Reserved | Zero |
| `8..10` | Partition | `0..4`, excluding 4 |
| `10` | Group | `0` or `1`; append and retain require `0` |
| `11` | Reserved | Zero |
| `12..16` | Body byte count | Unsigned 32-bit length, further limited by operation |
| `16..24` | Request identifier | Nonzero unsigned 64-bit value |
| `24..64` | Five arguments | Five unsigned 64-bit fields, interpreted by operation |

The identifier is echoed in the response. It is **not persisted, not a producer sequence number, and not a deduplication key**. The client increases it within one connection to catch mismatched responses. A different connection may reuse it. The server does not silently manufacture an exactly-once contract from this field.

Create `protocol.rs` with its imports, constants, error constructors, and small enums. `#[repr(u8)]` specifies each enum's numeric representation for our explicit casts; it does not authorize reading arbitrary network bytes as enum memory. The decoding matches still reject unknown values.

```rust title="crates/broker-app/src/protocol.rs"
use std::future::Future;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Instant, timeout_at};

use broker_core::codec::{HEADER_LEN, MAX_PAYLOAD};
use broker_core::delivery::{AckOutcome, AckRejection, LeaseToken};
use broker_core::types::{Epoch, GroupId, PartitionId};

mod batch;
pub use batch::{FetchedBatch, FetchedRecord};

pub const WIRE_HEADER: usize = 64;
pub const TOKEN_BYTES: usize = 32;
pub const MAX_FETCH_RECORDS: usize = 64;
pub const MAX_ENCODED: usize = 2 * 1024 * 1024;
pub const MAX_WIRE_BODY: usize = MAX_ENCODED + TOKEN_BYTES * MAX_FETCH_RECORDS;

pub fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "absolute socket deadline reached")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op { Append = 1, Fetch = 2, Ack = 3, Checkpoint = 4, Progress = 5, Retain = 6 }
impl Op {
    fn decode(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(Self::Append), 2 => Ok(Self::Fetch), 3 => Ok(Self::Ack),
            4 => Ok(Self::Checkpoint), 5 => Ok(Self::Progress), 6 => Ok(Self::Retain),
            _ => Err(invalid("unknown operation")),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Request = 0, Ok = 1, Invalid = 2, Busy = 3, DiskFull = 4,
    Unavailable = 5, AdmissionDeadline = 6, Refused = 7, Uncertain = 8,
}
impl Status {
    fn decode(value: u8) -> io::Result<Self> {
        match value {
            0 => Ok(Self::Request), 1 => Ok(Self::Ok), 2 => Ok(Self::Invalid),
            3 => Ok(Self::Busy), 4 => Ok(Self::DiskFull), 5 => Ok(Self::Unavailable),
            6 => Ok(Self::AdmissionDeadline), 7 => Ok(Self::Refused), 8 => Ok(Self::Uncertain),
            _ => Err(invalid("unknown status")),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint { Producer, Consumer }
impl Endpoint {
    pub fn accepts(self, op: Op) -> bool {
        match self { Self::Producer => op == Op::Append, Self::Consumer => op != Op::Append }
    }
}

```

`Endpoint::accepts` separates producer traffic from consumer/control traffic. It is routing policy, not access control. Both listeners are bound to loopback by the executable. The header type keeps unvalidated numeric scope fields so decoding can be separated from semantic validation.

The following implementation emits fields into known byte ranges. `scope` constructs the typed partition and group only after the integers are available. The `expect` calls protect conversions from already-fixed slices, not assumptions about untrusted values; illegal partition and group values return errors.

```rust title="crates/broker-app/src/protocol.rs"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub op: Op,
    pub status: Status,
    pub partition: u16,
    pub group: u8,
    pub body: u32,
    pub id: u64,
    pub args: [u64; 5],
}
impl Header {
    pub fn request(op: Op, partition: PartitionId, group: GroupId) -> Self {
        Self { op, status: Status::Request, partition: partition.get(), group: group.get(),
            body: 0, id: 1, args: [0; 5] }
    }
    pub fn response(self, status: Status, args: [u64; 5], body: u32) -> Self {
        Self { status, args, body, ..self }
    }
    pub fn encode(self) -> [u8; WIRE_HEADER] {
        let mut bytes = [0; WIRE_HEADER];
        bytes[..4].copy_from_slice(b"PBNW"); bytes[4] = 1;
        bytes[5] = self.op as u8; bytes[6] = self.status as u8;
        bytes[8..10].copy_from_slice(&self.partition.to_be_bytes()); bytes[10] = self.group;
        bytes[12..16].copy_from_slice(&self.body.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.id.to_be_bytes());
        for (index, value) in self.args.iter().enumerate() {
            let start = 24 + 8 * index;
            bytes[start..start + 8].copy_from_slice(&value.to_be_bytes());
        }
        bytes
    }
    pub fn decode(bytes: &[u8; WIRE_HEADER]) -> io::Result<Self> {
        if &bytes[..4] != b"PBNW" || bytes[4] != 1 || bytes[7] != 0 || bytes[11] != 0 {
            return Err(invalid("wrong magic, version, or reserved byte"));
        }
        let mut args = [0; 5];
        for (index, value) in args.iter_mut().enumerate() {
            let start = 24 + 8 * index;
            *value = u64::from_be_bytes(bytes[start..start + 8].try_into().expect("fixed field"));
        }
        let value = Self {
            op: Op::decode(bytes[5])?, status: Status::decode(bytes[6])?,
            partition: u16::from_be_bytes([bytes[8], bytes[9]]), group: bytes[10],
            body: u32::from_be_bytes(bytes[12..16].try_into().expect("fixed field")),
            id: u64::from_be_bytes(bytes[16..24].try_into().expect("fixed field")), args,
        };
        if value.id == 0 { return Err(invalid("request identifier must be nonzero")); }
        Ok(value)
    }
    pub fn scope(self) -> io::Result<(PartitionId, GroupId)> {
        let p = PartitionId::new(self.partition).map_err(|_| invalid("partition out of range"))?;
        let g = GroupId::new(self.group).map_err(|_| invalid("group out of range"))?;
        Ok((p, g))
    }
}

```

For a `cat` append to partition 2 with identifier `0x0102030405060708`, the first 24 bytes are:

```text
50 42 4e 57  01 01 00 00  00 02 00 00  00 00 00 03
01 02 03 04  05 06 07 08
```

They are followed by 40 zero argument bytes, then `63 61 74`. The total is $64+3=67$ bytes. Partition 2 does not make the identifier 2; byte ranges keep those concepts distinct. Part 4 compares this independently written prefix with the encoder, rather than checking only that the encoder and decoder agree with each other.

## Specify every operation before writing its handler

Call the argument slots `a`, `b`, `c`, `d`, and `e`, in byte order. A dash in the table means **zero**, not “ignored.” An append may have an empty payload. Every other request has body length zero. Successful responses other than fetch also have body length zero.

| Operation | Request arguments | Successful response arguments |
| --- | --- | --- |
| Append | All zero; body is raw payload | `a=assigned offset`; remaining fields zero |
| Fetch | `a=record limit`, `b=encoded-byte limit`; remaining fields zero | `a=record count`, `b=encoded record bytes`, `c=stop reason`, `d=required bytes if stopped by size`, `e=current epoch` |
| Acknowledge | `a=epoch`, `b=offset`, `c=generation`, `d=deadline milliseconds`, `e=0` | `a=ack outcome`, `b=volatile next`, `c=persisted next`, `d=retired count`, `e=current epoch` |
| Checkpoint | All zero | `a=volatile next`, `b=persisted next`, `c=published flag`, `d=0`, `e=current epoch` |
| Progress | All zero | `a=volatile next`, `b=persisted next`, `c=d=0`, `e=current epoch` |
| Retain | All zero | `a=previous floor`, `b=current floor`, `c=removed bytes`, `d=removed segments`, `e=rotated flag` |

A fetch accepts 1–64 records and 1–2,097,152 encoded bytes. The smaller record-count limit is a transport profile over the core's larger interface, keeping token and response metadata bounded. The byte limit includes each stored record's 32-byte header, not its lease token or the outer envelope. A budget smaller than one record is a legal request: the reply is empty with a byte-limit stop and the required encoded record size.

Fetch stop codes are `0` for no eligible record, `1` for the record limit, and `2` for a byte limit. Under this implementation, record-limit means the requested count was returned. A byte-limit reply has room for fewer records than requested and names a record larger than the **remaining** byte allowance, not necessarily larger than the original allowance.

Checkpoint success reports equal volatile and persisted next offsets at that owner's snapshot. `published=false` means the requested frontier was already persisted; it does not mean failure. A subsequent completion can make a later progress reply dirty again. Retention never accepts a client-selected floor: the owner still derives it from both persisted consumer frontiers.

Add the complete request validator. In particular, `body` is checked before an app handler can reserve a buffer or start reading it.

```rust title="crates/broker-app/src/protocol.rs"
pub fn validate_request(h: Header) -> io::Result<(PartitionId, GroupId)> {
    let scope = h.scope()?;
    if h.id == 0 || h.status != Status::Request { return Err(invalid("not a request")); }
    if h.op != Op::Append && h.body != 0 { return Err(invalid("unexpected request body")); }
    match h.op {
        Op::Append => {
            if h.body as u64 > MAX_PAYLOAD as u64 || h.group != 0 || h.args != [0; 5] {
                return Err(invalid("invalid append shape"));
            }
        }
        Op::Fetch => {
            if !(1..=MAX_FETCH_RECORDS as u64).contains(&h.args[0])
                || !(1..=MAX_ENCODED as u64).contains(&h.args[1]) || h.args[2..] != [0; 3]
            { return Err(invalid("invalid fetch limits")); }
        }
        Op::Ack => {
            if h.args[0] == 0 || h.args[1] == u64::MAX || h.args[2] == 0
                || h.args[3] == 0 || h.args[4] != 0
            { return Err(invalid("invalid acknowledgement token")); }
        }
        Op::Checkpoint | Op::Progress => {
            if h.args != [0; 5] { return Err(invalid("unexpected control arguments")); }
        }
        Op::Retain => {
            if h.group != 0 || h.args != [0; 5] { return Err(invalid("invalid retention request")); }
        }
    }
    Ok(scope)
}

```

An acknowledgement's offset cannot be `u64::MAX`, because that value may represent an exclusive end but cannot be issued. Epoch, generation, and deadline are nonzero. Exact epoch/generation/deadline equality and expiration remain the owner state machine's job, not a transport guess. The client must return the deadline unchanged: it is in the broker's process-local clock domain, not Unix time or the client's clock.

## Distinguish refusal from uncertainty

Every error response contains a zero body and five zero arguments. After sending one, the server closes the connection. For an invalid common header—wrong magic, version, opcode, or reserved bytes—it closes without trusting the request enough to reply. A recognized header with an invalid request shape may receive `Invalid`.

| Status | Code | Meaning for this request |
| --- | ---: | --- |
| `Ok` | 1 | Operation-specific result; an acknowledgement still has its own outcome |
| `Invalid` | 2 | Shape, endpoint, or configured request size refused before submission |
| `Busy` | 3 | Bounded lane, pool, or waiter admission refused |
| `DiskFull` | 4 | Disk reservation refused before handoff |
| `Unavailable` | 5 | Owner admission already closed; no new handoff occurred |
| `AdmissionDeadline` | 6 | Admission wait ended before submission |
| `Refused` | 7 | Owner explicitly rejected the operation without applying it |
| `Uncertain` | 8 | Submitted operation failed, lost its reply, or exceeded the reply-wait budget; no rollback is promised |

If the connection disappears without a complete valid response, the client usually cannot distinguish these outcomes. Do not turn that ambiguity into a fake `Busy` response or automatically replay an append. A repeat can create another durable record. An application that needs deduplication must define and implement it separately; this course's broker contract is unchanged.

## Count the fetch bytes, not just the payload

Each fetched item is a **32-byte token**, followed by the already encoded record: its 32-byte storage header and payload. The token contains four unsigned 64-bit values: epoch, offset, generation, and deadline. Partition/group come from the outer response header. The storage header also contains the offset; the client checks that both copies agree.

Let $n$ be the returned record count, $x_i$ the payload size of returned item $i$, and $E$ the sum of encoded record sizes. These are byte counts, except $n$. Then

$$
E=\sum_{i=0}^{n-1}(32+x_i),\qquad B=32n+E,\qquad F=64+B.
$$

Here $B$ is the declared body length, and $F$ is the complete response length. Two three-byte records give $E=70$, $B=134$, and $F=198$. Their token ranges start at body positions 0 and 67. The first payload begins at body position 64; the second begins at 131. These are **body-relative** positions; add 64 to find them in the complete wire response.

With $n\le64$ and $E\le2{,}097{,}152$, the global wire body ceiling is

$$
B\le2{,}097{,}152+32\cdot64=2{,}099{,}200.
$$

The ceiling includes token overhead even though the response pool stores only encoded records. Tokens are generated into one fixed array at a time; no second server-side body vector is created. Two maximum-size records still do not fit a 2 MiB response pool: their encoded size exceeds it by 64 bytes.

The client checks both global dimensions and the **specific request's limits** before allocating receive storage. A response below the global ceiling can still violate a small request. Add the dimension helper and full response validator:

```rust title="crates/broker-app/src/protocol.rs"
pub fn body_size(records: usize, encoded: usize) -> io::Result<usize> {
    if records > MAX_FETCH_RECORDS || encoded > MAX_ENCODED
        || encoded < records * HEADER_LEN || (records == 0 && encoded != 0)
    { return Err(invalid("invalid fetched body dimensions")); }
    encoded.checked_add(records.checked_mul(TOKEN_BYTES).ok_or_else(|| invalid("body overflow"))?)
        .ok_or_else(|| invalid("body overflow"))
}

pub fn validate_response(request: Header, reply: Header) -> io::Result<()> {
    if reply.id != request.id || reply.op != request.op || reply.partition != request.partition
        || reply.group != request.group || reply.status == Status::Request
    { return Err(invalid("response does not match its request")); }
    reply.scope()?;
    if reply.status != Status::Ok {
        if reply.body != 0 || reply.args != [0; 5] { return Err(invalid("error response must be empty")); }
        return Ok(());
    }
    let [a, b, c, d, e] = reply.args;
    if reply.op != Op::Fetch && reply.body != 0 { return Err(invalid("unexpected response body")); }
    match reply.op {
        Op::Append => {
            if a == u64::MAX || [b, c, d, e] != [0; 4] { return Err(invalid("invalid append response")); }
        }
        Op::Fetch => {
            if a > request.args[0] || b > request.args[1] || e == 0 || c > 2 {
                return Err(invalid("fetch response exceeds request or has no epoch"));
            }
            let count = usize::try_from(a).map_err(|_| invalid("record count overflow"))?;
            let encoded = usize::try_from(b).map_err(|_| invalid("encoded size overflow"))?;
            if reply.body as u64 != body_size(count, encoded)? as u64 {
                return Err(invalid("fetched wire size mismatch"));
            }
            match c {
                0 if d == 0 && a < request.args[0] => {},
                1 if d == 0 && a == request.args[0] => {},
                2 if a < request.args[0] && d >= HEADER_LEN as u64
                    && d <= (HEADER_LEN + MAX_PAYLOAD) as u64 && d > request.args[1] - b => {},
                _ => return Err(invalid("invalid fetch stop reason")),
            }
        }
        Op::Ack => {
            decode_ack(a)?;
            if c > b || d > 4096 || e == 0 { return Err(invalid("invalid acknowledgement progress")); }
        }
        Op::Checkpoint | Op::Progress => {
            if b > a || e == 0 || d != 0 || c > 1 || (reply.op == Op::Progress && c != 0)
                || (reply.op == Op::Checkpoint && a != b)
            { return Err(invalid("invalid progress response")); }
        }
        Op::Retain => {
            if b < a || d > 65 || e > 1 { return Err(invalid("invalid retention response")); }
        }
    }
    Ok(())
}

```

The checks precede the arithmetic that uses their bounds. `body_size` first limits count and encoded size; it then uses checked addition and multiplication. `validate_response` checks that the two echoed scope fields, opcode, and identifier match. It also rejects a body attached to a small reply or an error. The tests intentionally present a globally valid large fetch response to a one-record request; that response must be refused from the header alone.

For progress, persisted next must not exceed volatile next. For retention, the floor cannot move backward. These checks do not replace trusted storage recovery, but they stop a malformed peer response from becoming an apparently valid client object.

## Preserve acknowledgement outcomes separately from transport success

The acknowledgement result codes are `0` applied, `1` duplicate, and `2` already retired. Rejections are `10` wrong epoch, `11` wrong partition, `12` wrong group, `13` not issued, `14` stale generation, `15` wrong deadline, and `16` expired. A correctly handled stale acknowledgement is a successful **protocol exchange**, not an accepted completion. Thus it carries status `Ok` with a rejection outcome.

`AlreadyRetired` says the contiguous frontier is beyond that position. It does not attest that this particular attempt performed an external effect. Likewise, `Duplicate` does not add another completion. Neither outcome creates exactly-once processing.

Add the token and outcome conversions. They map every core outcome without flattening expiry and stale-generation cases into an undifferentiated error.

```rust title="crates/broker-app/src/protocol.rs"
pub fn encode_token(token: LeaseToken) -> [u8; TOKEN_BYTES] {
    let mut raw = [0; TOKEN_BYTES];
    for (index, value) in [token.epoch.get(), token.offset.get(), token.generation, token.deadline_ms]
        .into_iter().enumerate()
    { raw[index * 8..index * 8 + 8].copy_from_slice(&value.to_be_bytes()); }
    raw
}
pub fn decode_token(raw: &[u8; TOKEN_BYTES], scope: Header) -> io::Result<LeaseToken> {
    let (partition, group) = scope.scope()?;
    let mut fields = [0; 4];
    for (index, value) in fields.iter_mut().enumerate() {
        *value = u64::from_be_bytes(raw[index * 8..index * 8 + 8].try_into().expect("fixed field"));
    }
    if fields[1] == u64::MAX || fields[2] == 0 || fields[3] == 0 {
        return Err(invalid("invalid fetched token"));
    }
    Ok(LeaseToken { epoch: Epoch::new(fields[0]).map_err(|_| invalid("zero token epoch"))?,
        partition, group, offset: broker_core::types::Offset::new(fields[1]),
        generation: fields[2], deadline_ms: fields[3] })
}
pub fn encode_ack(outcome: AckOutcome) -> u64 {
    match outcome {
        AckOutcome::Applied => 0, AckOutcome::Duplicate => 1, AckOutcome::AlreadyRetired => 2,
        AckOutcome::Rejected(reason) => match reason {
            AckRejection::WrongEpoch => 10, AckRejection::WrongPartition => 11,
            AckRejection::WrongGroup => 12, AckRejection::NotIssued => 13,
            AckRejection::StaleGeneration => 14, AckRejection::WrongDeadline => 15,
            AckRejection::Expired => 16,
        },
    }
}
pub fn decode_ack(value: u64) -> io::Result<AckOutcome> {
    Ok(match value {
        0 => AckOutcome::Applied, 1 => AckOutcome::Duplicate, 2 => AckOutcome::AlreadyRetired,
        10 => AckOutcome::Rejected(AckRejection::WrongEpoch),
        11 => AckOutcome::Rejected(AckRejection::WrongPartition),
        12 => AckOutcome::Rejected(AckRejection::WrongGroup),
        13 => AckOutcome::Rejected(AckRejection::NotIssued),
        14 => AckOutcome::Rejected(AckRejection::StaleGeneration),
        15 => AckOutcome::Rejected(AckRejection::WrongDeadline),
        16 => AckOutcome::Rejected(AckRejection::Expired),
        _ => return Err(invalid("unknown acknowledgement outcome")),
    })
}

```

## Finish or abandon a partial I/O operation

We need three small I/O operations: read one fixed header, read an already-sized body, and write a borrowed byte slice. `AsyncReadExt` and `AsyncWriteExt` add methods to implementors of the base traits; importing only `AsyncRead` does not import the extension methods ([read extension API](https://docs.rs/tokio/latest/tokio/io/trait.AsyncReadExt.html), [write extension API](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWriteExt.html)).

The deadline is absolute for one phase. `within` checks it before polling and after completion because Tokio documents that an immediately ready future can win even when its deadline has passed ([`timeout_at`](https://docs.rs/tokio/latest/tokio/time/fn.timeout_at.html)). The wrapper is not a preemptive scheduler; it cannot interrupt a long synchronous poll. A completion rejected by the post-check may already have moved bytes, which is another reason to close rather than replay the exchange.

`read_header` tracks `used`. A zero-byte read with `used == 0` is clean end-of-stream between requests. A zero-byte read after a prefix is truncation. Each read uses the original deadline, so a peer cannot extend it forever by periodically sending one more byte.

```rust title="crates/broker-app/src/protocol.rs"
pub async fn within<F, T>(deadline: Instant, future: F) -> io::Result<T>
where F: Future<Output = io::Result<T>> {
    if Instant::now() >= deadline { return Err(timed_out()); }
    let result = timeout_at(deadline, future).await.map_err(|_| timed_out())?;
    if Instant::now() >= deadline { return Err(timed_out()); }
    result
}
pub async fn read_header<R: AsyncRead + Unpin>(reader: &mut R, deadline: Instant)
    -> io::Result<Option<Header>>
{
    let mut raw = [0; WIRE_HEADER];
    let mut used = 0;
    while used < raw.len() {
        let n = within(deadline, reader.read(&mut raw[used..])).await?;
        if n == 0 {
            return if used == 0 { Ok(None) }
                else { Err(io::Error::new(io::ErrorKind::UnexpectedEof, "partial wire header")) };
        }
        used += n;
    }
    Header::decode(&raw).map(Some)
}
pub async fn read_body<R: AsyncRead + Unpin>(reader: &mut R, body: &mut [u8], deadline: Instant)
    -> io::Result<()>
{
    within(deadline, reader.read_exact(body)).await.map(|_| ())
}
pub async fn write_bytes<W: AsyncWrite + Unpin>(writer: &mut W, bytes: &[u8], deadline: Instant)
    -> io::Result<()>
{
    within(deadline, writer.write_all(bytes)).await
}
```

The header array is part of the future's owned state; do not assume every async local physically remains on a thread stack. `read_body` borrows a prepared pool buffer. `write_bytes` borrows either a small array or a view into an owned response.

Tokio's `read_exact` and `write_all` may have consumed or written a prefix when cancelled. Retrying the same whole buffer on the same stream can corrupt frame boundaries; `write_all` would start from the beginning. Our callers therefore **drop the socket on timeout, I/O error, or cancellation of a partial exchange**, rather than resume these helpers after interruption. See the explicit cancellation notes for [`read_exact`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncReadExt.html#method.read_exact) and [`write_all`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWriteExt.html#method.write_all).

A failed body read drops a prepared append before handoff. A failed response write drops only the response owner: the earlier append or lease may already be effective. The helper signatures alone cannot encode that distinction; the session state machine in Part 2 does.

## Own a received batch, lend its payload views

The server does not need to collect a wire body, but the standalone client needs somewhere to retain bytes after the socket read. It allocates one bounded body vector only after checking the response envelope. That allocation belongs to the **client process**, not the broker's 64 MiB payload-pool claim. A caller retaining many client batches must bound that retention separately.

Create `protocol/batch.rs`. Each entry contains a copied token and a range into the body vector. There is no self-referential stored slice. `records()` manufactures borrowed views tied to `&self`, just as Chapter 2's record views were tied to their owner.

```rust title="crates/broker-app/src/protocol/batch.rs"
use std::io;
use std::ops::Range;
use broker_core::codec::{self, HEADER_LEN, Probe};
use broker_core::delivery::{LeaseStop, LeaseToken};
use super::{Header, Op, Status, TOKEN_BYTES, body_size, decode_token, invalid};

#[derive(Debug)]
struct Entry { token: LeaseToken, payload: Range<usize> }
#[derive(Debug)]
pub struct FetchedBatch {
    body: Vec<u8>,
    entries: Vec<Entry>,
    pub encoded_bytes: usize,
    pub stop: LeaseStop,
}
#[derive(Debug, Clone, Copy)]
pub struct FetchedRecord<'a> { pub token: LeaseToken, pub payload: &'a [u8] }
```

The decoder walks `token -> encoded record -> token -> encoded record`. It reuses the core record decoder to validate the header and payload checksums, checks the token's epoch against the envelope, and requires the duplicated offsets to agree. At the end, both the byte cursor and the encoded-byte total must match the declared dimensions. There can be no silently ignored suffix.

Retry scheduling can interleave older offsets with new ones, so the decoder does **not** insist on ascending offsets. It does reject the same offset twice in one response. The straightforward scan of previous entries costs at most $64\cdot63/2=2016$ comparisons, with no extra map allocation. This is a bounded baseline, not a claim that quadratic scanning is desirable at arbitrary batch sizes.

```rust title="crates/broker-app/src/protocol/batch.rs"
impl FetchedBatch {
    // The caller first validates the response against its exact request.
    pub(crate) fn decode(header: Header, body: Vec<u8>) -> io::Result<Self> {
        let [count, encoded, stop, required, epoch] = header.args;
        let count = usize::try_from(count).map_err(|_| invalid("count overflow"))?;
        let encoded = usize::try_from(encoded).map_err(|_| invalid("size overflow"))?;
        if header.op != Op::Fetch || header.status != Status::Ok
            || body.len() != header.body as usize || body.len() != body_size(count, encoded)?
        { return Err(invalid("invalid fetch envelope")); }
        let mut entries: Vec<Entry> = Vec::new();
        entries.try_reserve_exact(count).map_err(io::Error::other)?;
        let mut position = 0usize;
        let mut record_bytes = 0usize;
        for _ in 0..count {
            let raw = body.get(position..position + TOKEN_BYTES)
                .ok_or_else(|| invalid("short token"))?;
            let token = decode_token(raw.try_into().expect("fixed token"), header)?;
            if token.epoch.get() != epoch || entries.iter().any(|e| e.token.offset == token.offset) {
                return Err(invalid("epoch mismatch or duplicate offset in one batch"));
            }
            position += TOKEN_BYTES;
            let (view, consumed) = match codec::decode_record(&body[position..])
                .map_err(|_| invalid("invalid fetched record checksum or header"))?
            {
                Probe::Complete { value, consumed } => (value, consumed),
                Probe::Incomplete { .. } => return Err(invalid("short fetched record")),
            };
            if view.offset != token.offset { return Err(invalid("token and record offsets disagree")); }
            entries.push(Entry { token, payload: position + HEADER_LEN..position + consumed });
            position += consumed;
            record_bytes += consumed;
        }
        if position != body.len() || record_bytes != encoded { return Err(invalid("trailing or miscounted body bytes")); }
        let stop = match stop {
            0 => LeaseStop::NoEligible, 1 => LeaseStop::RecordLimit,
            2 => LeaseStop::ByteLimit { required: usize::try_from(required).map_err(|_| invalid("size overflow"))? },
            _ => return Err(invalid("unknown stop reason")),
        };
        Ok(Self { body, entries, encoded_bytes: encoded, stop })
    }
    pub fn records(&self) -> impl ExactSizeIterator<Item = FetchedRecord<'_>> + '_ {
        self.entries.iter().map(|entry| FetchedRecord {
            token: entry.token, payload: &self.body[entry.payload.clone()],
        })
    }
}
```

For the two-record body above, `position` starts at 0, advances to 32 after the first token, and to 67 after the first encoded record. The stored payload range is `64..67`. The second iteration advances through 99 to 134 and stores `131..134`. Those ranges remain valid while the vector is owned by the batch; there is no payload clone per record.

Malformed response bytes are a protocol error and the client will not restore that socket for another request. Checksums detect accidental record corruption, not a malicious peer that can recompute them. The explicit loopback/trusted-peer boundary still matters.

Continue to [Part 2](https://25d-228.github.io/rust-learning/chapter-16-part-02.html) to connect these bytes to the bounded owner APIs without inserting an uncharged staging buffer.

