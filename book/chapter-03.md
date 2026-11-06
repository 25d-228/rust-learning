# Chapter 3 — Enums, fallible parsing, and composed errors

A byte buffer can be valid without containing a complete message. It can also contain a complete header that will never be acceptable, however many bytes arrive afterward. Treating both cases as “try again” makes an invalid sender indistinguishable from a slow one. Removing a header before its payload arrives creates a different failure: the next call interprets payload bytes as a new length.

This chapter builds a parser that first inspects borrowed bytes, then consumes exactly one complete frame. Its return types distinguish ordinary incompleteness, a usable payload, and a reported error. An explicit end-of-input decision turns an unfinished frame into a truncation error. A second error type adds the frame's byte position without discarding the original cause.

**Starting point:** the workspace from [Chapter 1](https://25d-228.github.io/rust-learning/chapter-01.html) and the ownership example from [Chapter 2](https://25d-228.github.io/rust-learning/chapter-02.html). Keep both manifests, the library, `workspace_probe.rs`, and `borrowed_record.rs` unchanged. This example uses Chapter 2's slice borrowing, owned vectors, and lifetime relationships, but does not call its private `OwnedRecord` or `view_record` functions. The known-valid four-byte ownership record remains a separate exercise; today's length-prefixed frame is not the broker's eventual disk format.

**Implementation time:** 2.0 hours: approximately 1.25 for the parser, adapters, and demonstrations, and 0.75 for boundary tests and diagnosis. Reading is separate. Use actual implementation time to adjust later estimates rather than extending this task to fill the estimate.

> **Learning objectives**
> Model alternatives with enums; inspect them with exhaustive patterns; use `Option` and `Result` for different questions; check binary lengths before indexing or allocating; transfer a borrowed payload into an owned result before mutating its buffer; and preserve a typed cause while adding error context.

## Add one example, not a new crate

Create this file in the existing learner workspace:

```text title="New file — relative to the workspace root"
crates/language-lab/examples/fallible_frame.rs
```

The **12 Rust blocks whose exact label is `crates/language-lab/examples/fallible_frame.rs`**, in order, form the complete source file. Each contains complete declarations, functions, or test modules. Other listings are explicitly labelled as explanatory replacements or scratch exercises; do not concatenate those into the example. The reference copy at the end contains these same 12 blocks.

No manifest changes or external crates are needed. Keep the existing edition and compatible toolchain from Chapter 1; this chapter introduces no additional toolchain requirement. Cargo discovers the example from its filename. All new types are private to this example target, and the existing library exports remain unchanged.

The code separates four responsibilities:

| Layer | Input and result | Mutation policy |
| --- | --- | --- |
| `probe_frame` | Borrow bytes; report a complete view, a required total length, or a length error. | Never mutates or allocates payload storage. |
| `take_frame` | Borrow a vector mutably; return an owned payload, no payload, or an error. | Removes one complete frame only after copying its payload. |
| `count_finished_frames` | Examine a small, finished fixture; count frames or report the first failure's position. | Works on its own copy; never changes the caller's slice. |
| `report_error` | Borrow an error and follow its causes. | Prints descriptions; does not decide parsing or recovery policy. |

There is no socket, file reader, runtime, or unseen parser helper. The fixture driver is not a bounded network receive loop; its deliberately small inputs let us isolate parsing and error behavior first.

## A frame boundary is not a read boundary

The exercise format is a four-byte, unsigned, **big-endian payload length**, followed immediately by that many arbitrary bytes. Big-endian puts the most significant byte first. The length excludes the header. The frame for the three bytes `cat` is:

```text title="One frame — hexadecimal byte values and zero-based positions"
position       0  1  2  3   4  5  6
bytes         00 00 00 03  63 61 74
              ----------- --------
              length = 3   c  a  t
```

Let $b_0,b_1,b_2,b_3$ denote the four header bytes interpreted as nonnegative integers. Each is between 0 and 255. The declared payload length $L$ is:

$$
L = b_0 256^3 + b_1 256^2 + b_2 256 + b_3.
$$

For `00 00 00 03`, only the last term is nonzero, so $L=3$. For `00 00 01 00`, the third term is $1\times256$, so $L=256$, not 1. In code, `u32::from_be_bytes` performs this fixed-order interpretation; it does not use the machine's native byte order as the wire format. Its input is exactly a four-element array. See the [integer conversion documentation](https://doc.rust-lang.org/std/primitive.u32.html#method.from_be_bytes).

Let $H=4$ be the header length, $N$ the number of initialized input bytes, and $M$ the configured maximum payload length. After accepting the declaration, the total length of the first frame is:

$$
T = H + L.
$$

A complete frame requires $N\ge T$. Its payload is the half-open range `[H..T]`, and the next frame, if any, starts at `T`. An empty payload has $L=0$ and still occupies four header bytes. It must not be confused with no frame at all.

Our demonstration sets $M=8$. A partial header of two bytes needs **four total bytes**, not four additional bytes. A header declaring three payload bytes with only six total bytes present needs seven total bytes, so one more byte is missing. The parser's `total_required` field always refers to this total at the current buffer start. Before the header is complete it names the next decision boundary, not necessarily the final frame size.

## Use an enum when the states have different data

A struct groups fields that exist together. An **enum** chooses one variant at a time, and each variant can carry different fields. An incomplete probe needs a required length but has no payload. A ready probe needs a slice and a consumed length but does not need a fictitious “missing bytes” field. Putting those alternatives in an enum avoids combinations such as “ready, but payload absent.” Rust's [enum explanation](https://doc.rust-lang.org/book/ch06-01-defining-an-enum.html) describes this distinction.

Begin the source file with the imports, constant, and three enums:

```rust title="crates/language-lab/examples/fallible_frame.rs"
use std::error::Error;
use std::fmt;

const HEADER_LEN: usize = 4;

#[derive(Debug, PartialEq, Eq)]
enum FrameError {
    LengthOverflow,
    TooLarge { length: usize, limit: usize },
    Truncated { available: usize, total_required: usize },
}

#[derive(Debug, PartialEq, Eq)]
enum Probe<'a> {
    NeedMore { total_required: usize },
    Ready { payload: &'a [u8], consumed: usize },
}

enum InputState {
    Open,
    Ended,
}
```

`FrameError::LengthOverflow` is a variant with no fields. `FrameError::TooLarge { length: 256, limit: 8 }` is a variant with named fields; these braces construct a value, not a new type. `Probe::Ready` carries a borrowed payload, whereas neither error variant with numeric fields needs to borrow the input. The `Truncated` variant belongs to the end-of-input adapter, not to the pure probe.

`#[derive(Debug, PartialEq, Eq)]` asks the compiler to implement standard traits for these types. `Debug` provides diagnostic formatting such as `{:?}`. `PartialEq` provides equality comparisons, and `Eq` marks that equality as an equivalence relation. Here the derived implementations compare the active variant and its fields; byte slices compare contents. They are useful for precise assertions. They do not serialize the enum or make its memory layout our protocol. We will implement user-facing formatting separately.

The lifetime in `Probe<'a>` has the same job as in Chapter 2: the `Ready` payload refers to bytes supplied by the caller. Constructing the enum does not allocate or extend that storage's lifetime. The other variant holds only a number. We keep the lifetime because **a possible returned variant** can borrow data.

`InputState::Open` means that more input may arrive. `InputState::Ended` means the caller has established that no more bytes will arrive for this stream. This enum makes the caller's decision visible at the call site; a bare Boolean would not explain whether `true` means “ended,” “wait,” or “strict.” It does not detect end-of-input itself, and the caller must not resume appending after declaring the stream ended.

The imports bring two standard interfaces into scope: the `Error` trait and the `fmt` module. We will introduce their implementations before using them. `Option`, `Result`, `Vec`, and their commonly used variants already have names available through the standard prelude.

## `Option` answers “is there a value?”; `Result` answers “did this succeed?”

`Option<T>` has two variants: `Some(T)` contains a value, and `None` does not. `Result<T, E>` has two variants: `Ok(T)` contains a successful result, and `Err(E)` contains an error. The letters stand for types, not runtime variables. For example, `Option<usize>` optionally holds an unsigned size, whereas `Result<usize, FrameError>` either holds a size or explains why computing it failed. See the standard [`Option`](https://doc.rust-lang.org/std/option/enum.Option.html) and [`Result`](https://doc.rust-lang.org/std/result/enum.Result.html) definitions.

The probe returns `Result<Probe<'_>, FrameError>`. The outer `Result` describes validation success; the inner `Probe` describes availability. Therefore both a complete frame and ordinary partial input are `Ok`. The adapter returns `Result<Option<Vec<u8>>, FrameError>` because it no longer exposes a borrowed view or required length on its normal waiting path.

Read that nested type from the outside inward:

| Value | Meaning at the adapter boundary |
| --- | --- |
| `Err(error)` | The declaration is invalid, or input ended inside a frame. |
| `Ok(None)` while open | No complete frame is available yet. |
| `Ok(None)` after input ended | The remaining buffer is empty: a clean boundary. |
| `Ok(Some(payload))` | One complete frame was consumed; the vector owns its payload, possibly zero bytes. |

`Some(Vec::new())` is not `None`: it is a real empty payload. Likewise `Ok(None)` is not an error disguised as success. It is a documented ordinary outcome. An application that needs to distinguish “waiting” from “finished” already has `InputState`; this adapter does not invent a second hidden state variable.

### Match every possibility before extracting a value

A `match` expression examines a value and executes an arm whose pattern fits it. A pattern such as `Some(total)` binds the contained number to `total`; it is not calling a function named `Some`. Each arm has `pattern => expression`, and the whole match is an expression. The compiler checks exhaustiveness. Do not hide an expected case behind an arbitrary default just to satisfy it; see the Rust Book's [match discussion](https://doc.rust-lang.org/book/ch06-02-match.html).

Our first complete helper combines that control flow with checked arithmetic:

```rust title="crates/language-lab/examples/fallible_frame.rs"
fn checked_frame_len(length: usize) -> Result<usize, FrameError> {
    match HEADER_LEN.checked_add(length) {
        Some(total) => Ok(total),
        None => Err(FrameError::LengthOverflow),
    }
}
```

`checked_add` returns `Some(sum)` when the sum fits in `usize`, and `None` on overflow. The `Some` arm converts a successful optional number into `Ok(total)`. The `None` arm assigns a concrete reason, `LengthOverflow`. Both arms now have the same result type, so the match can be the function's final expression. No semicolon follows the match because its value is returned. The [checked addition API](https://doc.rust-lang.org/std/primitive.usize.html#method.checked_add) defines this behavior independently of the build's overflow-check settings.

The shorter expression `HEADER_LEN.checked_add(length).ok_or(FrameError::LengthOverflow)` has the same behavior. [`ok_or`](https://doc.rust-lang.org/std/option/enum.Option.html#method.ok_or) turns `Some(value)` into `Ok(value)` and replaces `None` with the supplied error. Keep the explicit match in the reference program; it makes the two decisions visible before using compact combinators elsewhere.

This helper checks arithmetic, not whether a vector of that length can exist or be allocated. On a 64-bit target the greatest `u32` payload length plus four fits in `usize`, but a public helper accepting arbitrary `usize` still needs its stated behavior. The tests exercise overflow by passing `usize::MAX` directly to this helper, without allocating a huge buffer or pretending a four-byte wire header can encode that value on the target.

## Validate lengths before borrowing a payload

Three short Rust operations let the probe propagate failures without nesting every branch.

**A fallible conversion** such as `usize::try_from(encoded)` returns a `Result`. Unlike a narrowing cast, it checks whether the number is representable. The [`TryFrom` contract](https://doc.rust-lang.org/std/convert/trait.TryFrom.html) makes this distinct from an always-successful `From` conversion. On the native 64-bit target a `u32` fits, but the signature expresses the assumption without truncation.

**`map_err`** changes only the error value. In `map_err(|_| FrameError::LengthOverflow)`, the bars delimit a small unnamed function called a closure. Its `_` parameter deliberately ignores the conversion library's particular error value and constructs our parser error instead. Success passes through unchanged. This is a deliberate loss of low-level conversion detail, not a general rule to erase causes. Later, a different closure will preserve the actual parser error while adding its location. See [`Result::map_err`](https://doc.rust-lang.org/std/result/enum.Result.html#method.map_err).

**`?`** extracts the successful value of a `Result` or returns an error from the current function. In these calls, `map_err` has already made the error type match the function's return type. More generally, `?` can use a suitable `From` conversion between error types; it does not fabricate one or automatically attach a byte position. It is propagation, not a panic, retry, or rollback mechanism. The Rust Book's [error-propagation section](https://doc.rust-lang.org/book/ch09-02-recoverable-errors-with-result.html#propagating-errors) explains this distinction.

Append the complete probe:

```rust title="crates/language-lab/examples/fallible_frame.rs"
fn probe_frame(input: &[u8], max_payload: usize) -> Result<Probe<'_>, FrameError> {
    if input.len() < HEADER_LEN {
        return Ok(Probe::NeedMore {
            total_required: HEADER_LEN,
        });
    }

    let encoded = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    let length = usize::try_from(encoded).map_err(|_| FrameError::LengthOverflow)?;
    if length > max_payload {
        return Err(FrameError::TooLarge {
            length,
            limit: max_payload,
        });
    }

    let total = checked_frame_len(length)?;
    if input.len() < total {
        return Ok(Probe::NeedMore {
            total_required: total,
        });
    }

    Ok(Probe::Ready {
        payload: &input[HEADER_LEN..total],
        consumed: total,
    })
}
```

The first length check protects **all four header indexes**. Only after it succeeds do we form the `[u8; 4]` array for `from_be_bytes`. Reading the first four bytes speculatively and checking the length afterward would already be too late.

Next, the declared payload is compared with `max_payload` before checking whether its body is present. With limit eight, `00 00 01 00` returns `TooLarge` immediately. Waiting for 260 total bytes first would incorrectly treat a permanently unacceptable declaration as ordinary incompleteness. The limit applies to each payload, not to the total number of currently buffered bytes: two coalesced valid frames can together exceed a single frame's length.

`checked_frame_len(length)?` computes the total without unchecked addition. Only after proving that the input contains that total can we form `&input[HEADER_LEN..total]`. The `Ready` result reports the total **including the header**, not just the payload length. Additional bytes remain outside this slice; they are not an error.

The return lifetime `Probe<'_>` is inferred from the sole borrowed input, `input`. It expresses the same relationship as an explicitly named input/output lifetime from Chapter 2. The numeric `max_payload` parameter does not contribute a borrowed lifetime. The [reference's lifetime-elision rules](https://doc.rust-lang.org/reference/lifetime-elision.html#lifetime-elision-in-functions) cover this case.

### Follow every branch with concrete bytes

The probe's decision order is part of its contract:

| Input at the current frame start; limit eight | Decision | Reason |
| --- | --- | --- |
| No bytes, or `00 00` | `NeedMore { total_required: 4 }` | No complete length field exists. |
| `00 00 00 03 63 61` | `NeedMore { total_required: 7 }` | Length three is acceptable, but only two payload bytes exist. |
| `00 00 00 03 63 61 74` | `Ready`, payload `cat`, consumed seven | Header and payload are complete. |
| `cat` frame followed by a `dog` frame | The same first `Ready` result | Coalescing does not change the first boundary. |
| `00 00 00 00` | `Ready`, empty payload, consumed four | Zero payload bytes are permitted. |
| `00 00 01 00` | `TooLarge { length: 256, limit: 8 }` | Policy fails before any body is needed. |

None of these calls removes a byte. An application can probe twice and get the same result for unchanged input. The probe has no knowledge of end-of-input, so even a final partial header is still `NeedMore` here. The next layer interprets that result using information the probe does not have.

## End-of-input changes the decision, not the format

The same six bytes can mean “wait for one more byte” while the stream is open and “one byte is missing forever” after it ends. Do not infer that distinction from `buffer.is_empty()` alone: an open empty buffer may simply have received nothing yet. Conversely, an ended nonempty buffer can contain complete frames that still need to be delivered.

The adapter combines these facts:

| Probe result | Open input | Ended input |
| --- | --- | --- |
| Invalid length | Return the original error; preserve bytes. | Return the original error; preserve bytes. |
| Incomplete, buffer empty | Return `Ok(None)`. | Return `Ok(None)` as clean completion. |
| Incomplete, buffer nonempty | Return `Ok(None)` and wait. | Return `Truncated`; preserve bytes. |
| Complete frame | Copy and consume one frame. | Copy and consume one frame; check the remainder on the next call. |

This is why “end-of-input” is not itself an error and does not mean “discard whatever remains.” A final complete zero-length frame produces `Some(empty)` first and only then clean `None` on the next call.

Append the adapter:

```rust title="crates/language-lab/examples/fallible_frame.rs"
fn take_frame(
    buffer: &mut Vec<u8>,
    max_payload: usize,
    state: InputState,
) -> Result<Option<Vec<u8>>, FrameError> {
    let (owned, consumed) = match probe_frame(buffer.as_slice(), max_payload)? {
        Probe::NeedMore { total_required } => {
            return match state {
                InputState::Open => Ok(None),
                InputState::Ended => {
                    if buffer.is_empty() {
                        Ok(None)
                    } else {
                        Err(FrameError::Truncated {
                            available: buffer.len(),
                            total_required,
                        })
                    }
                }
            };
        }
        Probe::Ready { payload, consumed } => (payload.to_vec(), consumed),
    };

    drop(buffer.drain(..consumed));
    Ok(Some(owned))
}
```

The outer match inspects the successful `Probe` after `?` propagates any invalid-length error. In the `NeedMore` arm, `return match state { ... };` returns the result of the inner match from `take_frame`. The `Ended` branch uses `if` as an expression whose two branches produce results of the same type. No path through this arm reaches the drain.

The pattern `Probe::Ready { payload, consumed }` binds both named fields. The arm evaluates to a tuple containing independent payload storage and the number of original bytes to remove. The tuple binding outside the match receives those two values. There is only one mutation point, after the match has finished.

### Why the copy precedes the drain

Before `to_vec`, `payload` is a slice into the accumulation vector. Draining its prefix can relocate the remaining bytes and invalidate the meaning of that view. `payload.to_vec()` gives the result a separate owner. Once the borrowed payload's last use ends, the adapter can mutably borrow the buffer to remove the consumed prefix. Replacing `to_vec()` with the slice itself would retain the conflict from Chapter 2, not create a zero-copy consume operation.

`Vec::drain(..consumed)` selects the half-open prefix from zero up to `consumed`. It returns an iterator that owns a mutable borrow of the vector; `drop(...)` immediately disposes of it and its remaining removed elements. We do not need to inspect the removed bytes. The documented [drain behavior](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.drain) also makes an out-of-range drain a programming error, so the earlier `Ready` proof matters. Chapter 4 will examine iterators in detail; here the returned drain is only used for its removal and cleanup behavior.

For the coalesced `cat` and `dog` input, the vector has 14 initialized bytes. Copying the first payload creates an owned three-byte vector. Removing seven bytes leaves the seven-byte `dog` frame at the front. The returned `cat` vector does not refer to those moved bytes. On the next call, removing the second seven-byte frame leaves length zero.

### What the failure guarantee does and does not cover

Every ordinary `Err` return and every incomplete `Ok(None)` return leaves the adapter's buffer unchanged. This follows from the control flow: those returns occur before `drain`. It is not a guarantee supplied by `Result` or `?`; moving a mutation earlier would break it while remaining valid Rust.

The payload allocation uses ordinary `to_vec` behavior. Allocation failure is **not** converted into `FrameError`. This code therefore does not promise recoverable out-of-memory handling. Nor does draining promise that vector capacity shrinks or resident memory returns to the operating system. Keep logical consumption, allocation capacity, and process memory as separate quantities.

The limit eight bounds a single accepted payload, not the accumulation vector or the number of outstanding returned vectors. A receive loop that appends forever without parsing can still grow without bound. Chapter 15's budgets and Chapter 16's transport loop must enforce their own admission and read limits; this parser is one mechanism they will use, not a substitute for them.

## Give errors a readable form without erasing their structure

`FrameError` already contains useful fields. Converting it permanently into a string would make tests and callers depend on parsing prose to recover those fields. Instead, implement the standard formatting interface while retaining the enum.

A **trait implementation** uses `impl Trait for Type`. It supplies the methods required by that trait for that particular type. This differs from Chapter 2's inherent `impl OwnedRecord`, which introduced methods directly on the type. We only need two concrete standard traits here; defining our own traits and choosing generic bounds comes in Chapter 4.

`fmt::Display` is the formatting interface used by `{}`. Its required `fmt` method borrows the error and writes through a mutable `Formatter`. `fmt::Result` is a shorthand for `Result<(), fmt::Error>`: `()` is a success with no additional value, and the error belongs to the **formatting operation**, not to frame parsing. `write!` writes to that formatter rather than printing directly to standard output. The [Display contract](https://doc.rust-lang.org/std/fmt/trait.Display.html) defines this signature and its relationship to user-facing text.

Append the implementation:

```rust title="crates/language-lab/examples/fallible_frame.rs"
impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthOverflow => write!(f, "frame length does not fit usize"),
            Self::TooLarge { length, limit } => {
                write!(f, "payload length {length} exceeds limit {limit}")
            }
            Self::Truncated {
                available,
                total_required,
            } => write!(
                f,
                "truncated frame: have {available} bytes, need {total_required} total"
            ),
        }
    }
}

impl Error for FrameError {}
```

`Self` names the implementing type, so `Self::TooLarge` means `FrameError::TooLarge`. Because `self` is a shared reference, matching it borrows the fields rather than moving data out of the error. Formatting does not consume the error; tests and later reporting can still inspect it. Each arm returns the `write!` result directly. It does not invent a `fmt::Error` merely because the object being displayed describes a parsing error.

`Debug` remains useful for an assertion such as `assert_eq!`, while `Display` gives a concise explanation to a human. Implementing `Display` also supplies the usual `.to_string()` conversion. We will use that only in tests of the displayed message, not as our error representation.

`std::error::Error` is a trait for describable error values. It requires `Debug` and `Display`, both of which are now provided. An empty `impl Error for FrameError {}` accepts the default methods; this enum has no lower-level retained cause. Nothing about implementing `Error` makes an error fatal or throws an exception. `Result` still returns an ordinary value to its caller. See the standard [Error interface](https://doc.rust-lang.org/std/error/trait.Error.html).

## Add context at the layer that knows the position

The probe sees only the current prefix of a buffer. It does not know how many earlier frames an application already consumed. Adding an offset inside it would either invent a value or force every low-level caller to carry an unrelated stream history.

A fixture driver does know that history. Give it a contextual error that owns the original `FrameError`:

```rust title="crates/language-lab/examples/fallible_frame.rs"
#[derive(Debug, PartialEq, Eq)]
struct DecodeError {
    offset: usize,
    source: FrameError,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cannot decode frame starting at byte {}", self.offset)
    }
}

impl Error for DecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
```

This wrapper's fields answer two separate questions: **where** the operation failed and **why** the parser rejected it. `offset` is a zero-based byte offset from the start of the supplied fixture, not a record count, broker offset, or position inside the payload. The `source` field owns the concrete cause. No input bytes are borrowed, so returning this error does not keep the input buffer alive.

The wrapper's `Display` prints only its own context. Its `source()` returns the lower-level error. A reporter can then print each layer once. If the wrapper's display also appended the source text, a reporter that followed the chain would repeat the cause. This follows the standard library's [error-source guidance](https://doc.rust-lang.org/std/error/trait.Error.html#method.source).

### Read the unfamiliar return type one boundary at a time

`Option<&(dyn Error + 'static)>` contains a **borrowed trait object**. `&dyn Error` lets a caller use the error interface without knowing the concrete cause type. Returning `Some(&self.source)` converts a reference to our concrete `FrameError` into that interface; it does not allocate a box or clone the error. The reporter below needs only formatting and `.source()`, so the erased concrete type is sufficient. This narrow use of dynamic dispatch prepares the more deliberate interface choices in Chapter 4.

The `+ 'static` bounds the hidden concrete type: it cannot depend on a shorter borrowed input lifetime. It does **not** make the returned reference immortal. The reference's lifetime is still tied to `&self`, so it cannot outlive the wrapper that owns the cause. Here `FrameError` stores only integers, making that requirement straightforward. The [lifetime-elision rules for methods and trait objects](https://doc.rust-lang.org/reference/lifetime-elision.html) distinguish these two lifetime roles.

This is different from declaring `&'static FrameError`, which would promise a reference valid for the entire program. Our signature makes no such promise.

## Propagate a cause after attaching the available context

Append a small driver over a **finished fixture**:

```rust title="crates/language-lab/examples/fallible_frame.rs"
fn count_finished_frames(input: &[u8], max_payload: usize) -> Result<usize, DecodeError> {
    let mut buffer = input.to_vec();
    let mut count = 0;

    loop {
        let offset = input.len() - buffer.len();
        let next = take_frame(&mut buffer, max_payload, InputState::Ended)
            .map_err(|source| DecodeError { offset, source })?;
        match next {
            Some(_) => count += 1,
            None => return Ok(count),
        }
    }
}
```

The driver copies the supplied bytes into its own mutable vector so that the exact `take_frame` adapter is exercised. It does not retain all decoded payloads: `Some(_)` counts a frame and discards that owned payload. This is deliberately a fixture counter, not a delivery API or a memory-bounded reader. Do not use this whole-input copy when the later network loop is supposed to limit incoming storage.

The `loop` repeats until an arm returns. Before each attempt it calculates the current frame start. Let $N_0$ be the original fixture length and $N_r$ the current remaining length. Because this driver only removes complete prefixes and never appends bytes, $0\le N_r\le N_0$. Its zero-based byte offset $O$ is therefore:

$$
O = N_0 - N_r.
$$

At the beginning of a 13-byte truncated fixture, $O=13-13=0$. After consuming the complete seven-byte `cat` frame, six bytes remain, so $O=13-6=7$. This calculation uses lengths rather than adding to a separate counter that could accidentally drift away from what was consumed.

The closure `|source| DecodeError { offset, source }` receives the parser error by value and moves it into the wrapper. The short field notation uses the local variable with the same name for each field. It also captures the already computed `offset`. If `take_frame` succeeds, `map_err` does not call the closure and the successful option passes through unchanged. If it fails, `?` now propagates a **`DecodeError`**, not a bare parser error.

There is intentionally no `From<FrameError> for DecodeError` implementation. A `FrameError` alone has no fixture position from which such a conversion could construct an honest offset. An automatic conversion that silently set it to zero would compile but lie about later frames. Context belongs at this call site, where it is available.

`InputState::Ended` is used on every attempt because all fixture bytes are already present. Thus `None` can mean clean completion here, never “wait for another read.” Even a zero-length payload removes four bytes, so successful iterations make progress. The count cannot overflow for an actually supplied slice: it is at most the original length divided by four. Returned errors drop the driver's local vector during ordinary return; completed earlier iterations are not rewound. No external processing effect has been performed by this counter.

### Traverse the error chain without assuming its concrete type

`while let` repeats while a pattern matches. `while let Some(cause) = source` extracts a cause when one exists and stops when the value is `None`. Unlike a full `match`, it intentionally needs no body for the stopping case. We use it for a linear error chain, not to ignore parser errors.

Append the reporter:

```rust title="crates/language-lab/examples/fallible_frame.rs"
fn report_error(error: &dyn Error) {
    println!("error: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        println!("caused by: {cause}");
        source = cause.source();
    }
}
```

The parameter is borrowed, so the caller retains ownership of the entire error. The top-level display is printed once, then each `.source()` supplies the next borrowed cause. Our `DecodeError` has one source and `FrameError` has none, so this loop prints exactly one cause line. `println!` is suitable for this demonstration's output, not a mandatory logging policy for the broker.

In a real application the outermost caller would also decide whether to close a session, refuse startup, retry, or stop a worker. Formatting the cause is not that policy. In particular, retrying this example's `TooLarge` result against unchanged bytes cannot make progress.

## Run a complete fragmented-input trace

The entry point constructs every input it needs. Its first frame arrives in three additions: two header bytes; the rest of the header plus `ca`; then `t` together with a whole second frame. Assertions check both results and preserved bytes.

The byte-string literal `b"cat"` has type `&[u8; 3]`: a reference to three fixed byte values. Where a slice is expected it can be borrowed as `&[u8]`. It is a compact fixture notation, not a conversion of arbitrary payload bytes into text. It complements the single-byte literals such as `b'c'` used earlier.

The `expect` calls below are assertions about **known fixtures**. `Result::expect` extracts an `Ok` value or panics when that fixture violates the stated expectation. `Option::expect` extracts a `Some` value; the second call is necessary because the successful result itself is optional. `expect_err` asserts that a deliberately invalid fixture produces `Err` and extracts that error. These helpers do not appear in the parsing or consuming routines, where arbitrary input must be reported through return values.

Append the complete entry point:

```rust title="crates/language-lab/examples/fallible_frame.rs"
fn main() {
    let mut buffer = vec![0, 0];
    assert_eq!(take_frame(&mut buffer, 8, InputState::Open), Ok(None));
    assert_eq!(buffer, vec![0, 0]);
    println!("partial header: kept {} bytes", buffer.len());

    buffer.extend_from_slice(&[0, 3, b'c', b'a']);
    assert_eq!(take_frame(&mut buffer, 8, InputState::Open), Ok(None));
    assert_eq!(buffer, vec![0, 0, 0, 3, b'c', b'a']);
    println!("partial body: kept {} bytes", buffer.len());

    buffer.extend_from_slice(&[b't', 0, 0, 0, 3, b'd', b'o', b'g']);
    let first = take_frame(&mut buffer, 8, InputState::Open)
        .expect("valid first frame")
        .expect("complete first frame");
    assert_eq!(first.as_slice(), b"cat");
    assert_eq!(buffer, vec![0, 0, 0, 3, b'd', b'o', b'g']);
    println!("first: {first:?}; remaining={}", buffer.len());

    let second = take_frame(&mut buffer, 8, InputState::Ended)
        .expect("valid second frame")
        .expect("complete second frame");
    assert_eq!(second.as_slice(), b"dog");
    assert_eq!(take_frame(&mut buffer, 8, InputState::Ended), Ok(None));
    println!("second: {second:?}; clean end={}", buffer.is_empty());
    drop(buffer);
    assert_eq!(first.as_slice(), b"cat");

    let mut empty_frame = vec![0, 0, 0, 0];
    let empty = take_frame(&mut empty_frame, 0, InputState::Ended)
        .expect("zero is within the limit")
        .expect("an empty payload is still a frame");
    assert!(empty.is_empty());
    assert!(empty_frame.is_empty());
    println!("empty payload: {} bytes; consumed {HEADER_LEN}", empty.len());

    let mut invalid = vec![0, 0, 1, 0];
    let before = invalid.clone();
    let error = take_frame(&mut invalid, 8, InputState::Open)
        .expect_err("length 256 exceeds the limit");
    assert_eq!(error, FrameError::TooLarge { length: 256, limit: 8 });
    assert_eq!(invalid, before);
    println!("rejected: {error}; kept {} bytes", invalid.len());

    let complete = [0, 0, 0, 3, b'c', b'a', b't', 0, 0, 0, 3, b'd', b'o', b'g'];
    assert_eq!(count_finished_frames(&complete, 8), Ok(2));
    let truncated = &complete[..complete.len() - 1];
    let error = count_finished_frames(truncated, 8)
        .expect_err("the second frame is missing its last byte");
    assert_eq!(error.offset, 7);
    report_error(&error);
}
```

The saved `first` payload survives both consuming the next frame and dropping the accumulation buffer, repeating the ownership boundary from Chapter 2 with an actual parser. The empty-frame fixture proves that a zero payload limit still permits a zero-length payload. The oversized-header fixture needs no payload bytes to be rejected and keeps all four original bytes.

The final `complete` array has two seven-byte frames. The half-open slice ending at `complete.len() - 1` removes only the last byte, `g`. Its second frame therefore has a complete length field but only `do` of the declared three payload bytes:

```text title="The truncated fixture — original byte offsets"
0                      7                 13
| complete cat frame  | header + d o     | end of available input
                       ^ frame start      ^ missing g would be here

error context: frame starts at byte 7
local cause:   six bytes available; seven total required
```

Byte 7 is the **frame start**, not the missing byte's position. The first unavailable byte is at offset 13; the expected exclusive end of that frame is 14. Keeping these quantities distinct makes later file-format diagnostics substantially more useful than “unexpected end” alone.

## Test availability, mutation, and end-of-input independently

The assertions in `main` provide a readable trace. Separate tests make it easier to identify which contract failed. The following three test modules contain **13 tests**. None expects a panic: malformed or incomplete frame input has a returned outcome. This differs from Chapter 2's two expected-panic tests for its intentionally stricter known-valid record view; leave those earlier tests unchanged.

### Probe boundaries and arithmetic

Append the first test module:

```rust title="crates/language-lab/examples/fallible_frame.rs"
#[cfg(test)]
mod probe_tests {
    use super::{FrameError, HEADER_LEN, Probe, checked_frame_len, probe_frame};

    #[test]
    fn short_headers_request_four_total_bytes() {
        for available in 0..HEADER_LEN {
            let input = vec![0; available];
            assert_eq!(
                probe_frame(&input, 8),
                Ok(Probe::NeedMore { total_required: 4 })
            );
        }
    }

    #[test]
    fn partial_bodies_request_the_whole_frame_length() {
        let frame = [0, 0, 0, 3, b'c', b'a', b't'];
        for available in HEADER_LEN..frame.len() {
            assert_eq!(
                probe_frame(&frame[..available], 8),
                Ok(Probe::NeedMore { total_required: 7 })
            );
        }
    }

    #[test]
    fn ready_exposes_only_the_first_payload() {
        let input = [0, 0, 0, 3, b'c', b'a', b't', 0, 0, 0, 0];
        assert_eq!(
            probe_frame(&input, 3),
            Ok(Probe::Ready { payload: b"cat", consumed: 7 })
        );
    }

    #[test]
    fn checked_length_rejects_overflow_without_allocating() {
        assert_eq!(checked_frame_len(0), Ok(4));
        assert_eq!(checked_frame_len(3), Ok(7));
        assert_eq!(checked_frame_len(usize::MAX), Err(FrameError::LengthOverflow));
        assert_eq!(checked_frame_len(usize::MAX - HEADER_LEN), Ok(usize::MAX));
    }
}
```

The first loop visits lengths zero through three. The second visits lengths four, five, and six of the seven-byte frame. These are all incomplete prefixes, but they require different next totals. The ready case sets the limit to exactly three and includes another frame afterward, checking both inclusive acceptance at the limit and the first-frame boundary.

The arithmetic test passes extreme **numbers**, not huge allocations. `usize::MAX - HEADER_LEN` plus the header fits exactly; `usize::MAX` plus the header does not. The test does not establish that a vector of the former total can be created. Arithmetic validity and a resource policy are separate checks.

### The consume boundary

Append the second module:

```rust title="crates/language-lab/examples/fallible_frame.rs"
#[cfg(test)]
mod take_tests {
    use super::{FrameError, InputState, take_frame};

    #[test]
    fn incomplete_input_is_not_consumed() {
        let frame = [0, 0, 0, 3, b'c', b'a', b't'];
        for available in 0..frame.len() {
            let mut buffer = frame[..available].to_vec();
            let before = buffer.clone();
            assert_eq!(take_frame(&mut buffer, 8, InputState::Open), Ok(None));
            assert_eq!(buffer, before);
        }
    }

    #[test]
    fn oversized_input_is_rejected_before_waiting_for_its_body() {
        let mut buffer = vec![0, 0, 1, 0];
        let before = buffer.clone();
        assert_eq!(
            take_frame(&mut buffer, 8, InputState::Open),
            Err(FrameError::TooLarge { length: 256, limit: 8 })
        );
        assert_eq!(buffer, before);
        assert_eq!(
            take_frame(&mut buffer, 8, InputState::Ended),
            Err(FrameError::TooLarge { length: 256, limit: 8 })
        );
        assert_eq!(buffer, before);
    }

    #[test]
    fn empty_payload_is_a_frame_even_at_end_of_input() {
        let mut buffer = vec![0, 0, 0, 0];
        assert_eq!(
            take_frame(&mut buffer, 0, InputState::Ended),
            Ok(Some(Vec::new()))
        );
        assert!(buffer.is_empty());
        assert_eq!(take_frame(&mut buffer, 0, InputState::Ended), Ok(None));
    }

    #[test]
    fn exact_limit_and_arbitrary_bytes_are_accepted() {
        let mut buffer = vec![0, 0, 0, 3, 0, 128, 255];
        assert_eq!(
            take_frame(&mut buffer, 3, InputState::Ended),
            Ok(Some(vec![0, 128, 255]))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn owned_payload_survives_reuse_and_destruction() {
        let mut buffer = vec![0, 0, 0, 3, b'c', b'a', b't'];
        let saved = take_frame(&mut buffer, 8, InputState::Open).unwrap().unwrap();
        buffer.extend_from_slice(&[255, 0, 1]);
        drop(buffer);
        assert_eq!(saved.as_slice(), b"cat");
    }
}
```

`incomplete_input_is_not_consumed` snapshots every incomplete prefix before calling the mutable adapter. A result-only test would miss a parser that returned `None` correctly but had already removed the header. The oversized test also runs after input is declared ended: invalid length still wins over a truncation diagnosis.

`Vec::new()` in the empty-frame assertion is an actual payload value wrapped in `Some`. The subsequent call expects `None`. The binary fixture `[0, 128, 255]` confirms that parsing does not silently require UTF-8. The reuse test deliberately appends unrelated bytes after consumption and destroys the buffer, then checks the detached payload.

### End-of-input and split invariance

Append the final module:

```rust title="crates/language-lab/examples/fallible_frame.rs"
#[cfg(test)]
mod finish_tests {
    use super::{DecodeError, FrameError, InputState, count_finished_frames, take_frame};
    use std::error::Error;

    #[test]
    fn ended_partial_input_is_truncated_and_unchanged() {
        let frame = [0, 0, 0, 3, b'c', b'a', b't'];
        for available in 1..frame.len() {
            let mut buffer = frame[..available].to_vec();
            let before = buffer.clone();
            let total_required = if available < 4 { 4 } else { 7 };
            assert_eq!(
                take_frame(&mut buffer, 8, InputState::Ended),
                Err(FrameError::Truncated { available, total_required })
            );
            assert_eq!(buffer, before);
        }
    }

    #[test]
    fn finished_count_handles_clean_boundaries() {
        assert_eq!(count_finished_frames(&[], 8), Ok(0));
        assert_eq!(count_finished_frames(&[0, 0, 0, 0], 0), Ok(1));
        assert_eq!(count_finished_frames(&[0, 0, 0, 0, 0, 0, 0, 0], 0), Ok(2));
    }

    #[test]
    fn context_keeps_the_second_frame_offset_and_cause() {
        let input = [0, 0, 0, 3, b'c', b'a', b't', 0, 0, 0, 3, b'd', b'o'];
        let error = count_finished_frames(&input, 8).unwrap_err();
        assert_eq!(
            error,
            DecodeError {
                offset: 7,
                source: FrameError::Truncated { available: 6, total_required: 7 },
            }
        );
        assert_eq!(error.to_string(), "cannot decode frame starting at byte 7");
        let cause = error.source().expect("the wrapper keeps its cause");
        assert_eq!(cause.to_string(), "truncated frame: have 6 bytes, need 7 total");
        assert!(cause.source().is_none());
    }

    #[test]
    fn every_two_chunk_split_preserves_both_frames() {
        let input = [0, 0, 0, 3, b'c', b'a', b't', 0, 0, 0, 3, b'd', b'o', b'g'];
        for split in 0..=input.len() {
            let mut buffer = input[..split].to_vec();
            let mut decoded = Vec::new();
            loop {
                match take_frame(&mut buffer, 8, InputState::Open).unwrap() {
                    Some(payload) => decoded.push(payload),
                    None => break,
                }
            }
            buffer.extend_from_slice(&input[split..]);
            loop {
                match take_frame(&mut buffer, 8, InputState::Ended).unwrap() {
                    Some(payload) => decoded.push(payload),
                    None => break,
                }
            }
            assert_eq!(decoded, vec![b"cat".to_vec(), b"dog".to_vec()]);
            assert!(buffer.is_empty());
        }
    }
}
```

The truncation test includes partial headers and partial bodies. Its `if` expression chooses a required total of four before the header exists and seven after the header declares the payload. Every failure must leave the original prefix unchanged.

The context test checks the typed wrapper first and its two display layers second. `error.source()` borrows the cause through the standard trait; that trait is imported in this test module because method lookup needs it in scope. The cause's own `.source()` is `None`, so reporting terminates rather than duplicating the wrapper.

The last test moves the boundary between two input chunks through **all 15 positions** of the 14-byte `cat`/`dog` sequence. The inclusive range `0..=input.len()` includes an empty first chunk and an empty last chunk. It consumes everything currently available while open, appends the remainder, then consumes with end-of-input known. `break` exits the current loop when no complete frame remains. Every split must produce the same two payloads in the same order and no leftover bytes.

This test is stronger than checking only the two frame boundaries, but it is not a proof for every possible input or every multi-chunk schedule. Its purpose is specific: splitting this fixture at a header or body position must not change the decoded result. Later protocol tests will reuse the same idea with their own fields and limits.

## Execute the program and select its tests explicitly

Run these commands from the learner workspace root after assembling all 12 source blocks:

```sh title="Terminal — Chapter 3 checks"
cargo fmt --all
cargo check -p language-lab --all-targets
cargo run -p language-lab --example fallible_frame
cargo test -p language-lab --example fallible_frame
cargo test -p language-lab --example fallible_frame --release
```

Expected standard output from the ordinary example, excluding Cargo build messages:

```text title="Expected stdout — fallible_frame"
partial header: kept 2 bytes
partial body: kept 6 bytes
first: [99, 97, 116]; remaining=7
second: [100, 111, 103]; clean end=true
empty payload: 0 bytes; consumed 4
rejected: payload length 256 exceeds limit 8; kept 4 bytes
error: cannot decode frame starting at byte 7
caused by: truncated frame: have 6 bytes, need 7 total
```

The last two lines are an **expected handled error demonstration**, not an unexpected program failure. `main` asserted that the fixture was truncated, reported its error, and can finish normally. Both example test commands should report 13 passed tests. Test order and elapsed time are not part of the expected output.

These are expected local results, not a claim that this program has run on your Mac. Compare the exact values with your own execution. A test harness run does not execute an example's ordinary `main`; keep the `cargo run` command. Likewise, plain `cargo test -p language-lab` is not evidence that this example's unit tests ran. Explicit example selection or `--all-targets` is required, as documented by [Cargo's target selection](https://doc.rust-lang.org/cargo/commands/cargo-test.html#target-selection).

## Diagnose an invalid use of `?`

A common first error is assuming that `?` can turn absence into any requested error automatically. Check this once in an isolated scratch program, not in a broken Cargo example. Create the directory from the workspace root:

```sh title="Terminal — scratch directory for the error-conversion exercise"
mkdir -p runs/fallible
```

Create this complete, deliberately invalid file:

```rust title="runs/fallible/option_question.rs — intentional compiler failure"
fn total(length: usize) -> Result<usize, &'static str> {
    let value = 4usize.checked_add(length)?;
    Ok(value)
}

fn main() {
    assert_eq!(total(3), Ok(7));
}
```

Compile it independently:

```sh title="Terminal — expect a nonzero compiler exit"
rustc --edition=2024 runs/fallible/option_question.rs -o runs/fallible/option_question
```

`checked_add` produces `Option<usize>`, while the function returns `Result<usize, &'static str>`. There is no explanation attached to `None` for the compiler to return as the string error. Expect a diagnostic about using `?` on an `Option` in a function returning `Result`; exact wording and line numbers depend on the compiler. The Rust Book discusses the [compatible return types for `?`](https://doc.rust-lang.org/book/ch09-02-recoverable-errors-with-result.html#where-the--operator-can-be-used).

Repair the same scratch file by explicitly choosing the meaning of absence:

```rust title="runs/fallible/option_question.rs — complete replacement"
fn total(length: usize) -> Result<usize, &'static str> {
    let value = 4usize.checked_add(length).ok_or("length overflow")?;
    Ok(value)
}

fn main() {
    assert_eq!(total(3), Ok(7));
    assert_eq!(total(usize::MAX), Err("length overflow"));
    println!("7; overflow returned as an error");
}
```

Now compile and run:

```sh title="Terminal — repaired standalone exercise"
rustc --edition=2024 runs/fallible/option_question.rs -o runs/fallible/option_question
./runs/fallible/option_question
```

Expect `7; overflow returned as an error`. The string literal used here has a program-long lifetime; it is a deliberately small scratch error type. The real example keeps a structured enum and its fields. Do not repair the scratch error with `unwrap`: that would turn the tested overflow into a panic instead of preserving the intended return contract.

## Separate a reported input error from a broken implementation

The probe and adapter accept arbitrary byte slices subject to the documented format policy. Incompleteness, oversize declarations, and end-of-input inside a frame are ordinary input outcomes. They must not trigger assertions. An out-of-range slice or drain would instead indicate that our implementation violated its own validated bounds. The Rust Book's [discussion of panic versus returned errors](https://doc.rust-lang.org/book/ch09-03-to-panic-or-not-to-panic.html) supports treating these differently.

The test code uses `unwrap` or `expect` because its fixtures specify which branch must occur. Such a panic means the test expectation failed. It is not a production recovery strategy. In particular, `?` is not an alternative spelling of `unwrap`: it returns an error to the caller rather than panicking when that branch occurs.

| Symptom | Likely mistake | Repair or diagnostic check |
| --- | --- | --- |
| A short header panics | Indexing happened before checking for four bytes. | Move all header indexing after the first guard. |
| The oversized fixture waits for its body | Availability was checked before the maximum length. | Reject a known invalid declaration as soon as its header exists. |
| The next call sees a nonsensical length | A previous partial frame lost its header. | Keep every mutation after the `Ready` decision. |
| `dog` disappears after taking `cat` | The adapter drained the whole vector rather than `consumed`. | Verify the first remaining length is seven. |
| The remaining bytes begin in the payload | The consumed count omitted the header. | Remove `HEADER_LEN + payload_length`, not just the payload. |
| An empty payload looks like no frame | `Some(empty)` was collapsed into `None`. | Preserve the outer option independently of the vector's length. |
| EOF silently accepts a suffix | `NeedMore` was treated as clean completion for nonempty input. | Combine it with `InputState` and the remaining length. |
| A later-frame error says byte zero | Context was attached without tracking prior consumption. | Calculate the offset at the driver boundary before taking the frame. |
| The cause is printed twice | The wrapper both formatted and exposed the same source. | Keep context in the wrapper's display and cause text in the source. |
| A probe error becomes “wait” | Error information was discarded, for example with `.ok()`. | Preserve `Result`; match or propagate its error branch. |

Do not make these checks pass by weakening the input limit, changing an expected payload, or removing the error branch. Their purpose is to protect the distinction between availability, validation, and consumption.

## See the cost before optimizing it

The pure probe inspects four header bytes and constructs a slice view; it never scans the payload. The owned adapter copies the payload and may shift the remaining tail toward the front of the vector. Let $L$ be the current payload length and $R$ the number of bytes after its frame. A useful source-level model for the byte work of a successful take is:

$$
\text{payload copy} = L, \qquad \text{tail relocation} = R.
$$

For the first take from `cat` followed by `dog`, $L=3$ and $R=7$. For the second, $L=3$ and $R=0$. This model counts logical byte movement, not measured processor instructions, allocator overhead, or a throughput result.

Repeated prefix drains can amplify the tail work. Suppose a buffer contains $k$ equal-length frames, each occupying $F$ bytes including its header. The first removal shifts $(k-1)F$ bytes, the next shifts $(k-2)F$, and so on. Adding those tail lengths gives:

$$
\text{total relocated bytes} = F\frac{k(k-1)}{2}.
$$

With three seven-byte frames, the tail work is $14+7+0=21$ bytes, in addition to copying the three payloads. A cursor-based design could defer compaction, but it must preserve the same “no consumption before a complete valid frame” rule and account for the retained allocation. That design choice belongs to a measured optimization, not an unexplained replacement for the working adapter.

The fixture driver also makes one whole-input copy. It is useful for this chapter's reproducible error locations, not a recommended ingestion path for large input. Neither this exercise nor its arithmetic model establishes a bounded-memory service. Keeping those limitations explicit is part of knowing what a mechanism actually guarantees.

## Leave a complete parser foundation

As a focused variation, make a valid first frame followed by an oversized second header. Predict whether the error is `Truncated` or `TooLarge`, and which frame position it reports. This **optional fourteenth test** supplies the complete check; add it inside the existing `finish_tests` module, not as a second module with the same name:

```rust title="Optional addition — inside finish_tests"
#[test]
fn second_frame_policy_failure_keeps_its_position() {
    let input = [0, 0, 0, 3, b'c', b'a', b't', 0, 0, 1, 0];
    assert_eq!(
        count_finished_frames(&input, 8),
        Err(DecodeError {
            offset: 7,
            source: FrameError::TooLarge { length: 256, limit: 8 },
        })
    );
}
```

The answer follows the same guard order as before: the second header already proves that length 256 violates the limit, so no body is needed for a policy error. The fixture start is still seven bytes into the input. The reference source retains the original 13 tests; adding this test makes 14 for this example.

Finish by checking all completed foundation targets:

```sh title="Terminal — cumulative checks after Chapter 3"
cargo fmt --all -- --check
cargo check -p language-lab --all-targets
cargo test -p language-lab --all-targets
cargo test -p language-lab --all-targets --release
cargo run -p language-lab --example workspace_probe
cargo run -p language-lab --example borrowed_record
cargo run -p language-lab --example fallible_frame
```

Without optional additions, expect **27 unit tests across separate executables**: six from the Chapter 1 library, eight from Chapter 2's example, and thirteen here. Each optional test actually added increases that total by one. The earlier example outputs remain unchanged, and the new example emits the eight lines shown above. Keep the mains' checks separate from the test harnesses.

The complete new source is available as [the Chapter 3 reference example](https://25d-228.github.io/rust-learning/media/chapter-03/crates/language-lab/examples/fallible_frame.rs). Place it at the matching `examples/` path in the existing Chapter 1 workspace; it is a one-file overlay, not a standalone Cargo project. Build the learner workspace or a scratch copy rather than the book's media directories.

The resulting foundation has a precise boundary: a probe borrows without consuming; the adapter copies before removing one complete frame; end-of-input changes incomplete data into an explicit error; and the driver adds position while preserving cause. [Chapter 4's planned capability](https://25d-228.github.io/rust-learning/index.html#chapter-4-traits-generics-iterators-and-bounded-collections) builds on these enum and error skills to define useful traits, compare generic and dynamic dispatch, and implement bounded scans and collections. No future broker code is required to run the program completed here.
