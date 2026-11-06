# A Durable Partitioned Event Broker

This book builds a Rust broker that accepts records, stores them in partitioned logs, and delivers them independently to two consumer groups. Its central problem is not forwarding messages. It is keeping storage order, delivery attempts, consumer progress, resource ownership, and restart behavior consistent when operations overlap or fail.

The finished system has one process, one data directory, one topic, and four fixed partitions. Each partition has a dedicated owner thread. Asynchronous connections communicate with those owners through bounded mailboxes. Consumers may finish work out of order, disappear while holding deliveries, or reconnect after a broker restart. The broker must preserve confirmed records until they are safe to reclaim, reject obsolete delivery tokens, and stop accepting data rather than exhaust its configured budgets.

> **Learning objectives**
> Implement the mechanisms that connect Rust's ownership model to persistent and concurrent state: checked binary formats, segmented storage and indexes, synchronization boundaries, bounded delivery windows, lease fencing, crash-safe progress, retention, admission, and shutdown. Explain the behavior with executable traces and measurements, not just a successful happy-path run.

## The software and its boundaries

The project consists of `brokerd`, a small client library, command-line producer and consumer programs, and a failure/load harness. All belong to one Cargo workspace. The harness is not a second independent project: it exercises the broker's actual protocol and persistent state.

You implement record encoding and decoding, sparse indexing, segment rotation, batching policy, recovery decisions, delivery state machines, progress publication, deletion eligibility, resource accounting, and supervision. Tokio supplies execution, sockets, timers, and synchronization primitives. The standard library supplies files and threads. A checksum library supplies the CRC calculation, not the record format, recovery policy, or storage engine. No database, durable queue, broker, actor framework, or workflow engine replaces these mechanisms.

The service binds only to loopback. Separate producer and consumer/control listeners reserve connection capacity for consuming and acknowledging when publishers are saturated. Both feed the same four owners; this is not a distributed topology. Consumer workers explicitly choose a group and partition. Multiple workers can share a group without implementing membership discovery or partition rebalancing.

There is no replication, consensus, multi-node failover, SQL, Kafka wire compatibility, dynamic topic creation, partition-count change, group creation/deletion, authentication, TLS, dashboard, or mandatory unsafe code. A permanently failing record can block a group's progress; there is no automatic skip or dead-letter policy. Smaller scale makes the state transitions observable without removing the difficult mechanisms.

### Ordering, success, and ambiguous outcomes

Each stored record has a `(partition, offset)` identity. Offsets increase within a partition; they do not order records across partitions. A producer selects its partition explicitly, and concurrent append order is the order assigned by that partition's owner—not the order in which remote calls began. First attempts are offered in offset order, but concurrent completion and retries are not an ordered execution guarantee.

| Operation | What a successful response means | What it does not mean |
| --- | --- | --- |
| Publish | The owner wrote the record, completed the selected file-synchronization boundary, and made the offset visible. | A reply lost in transit does not imply that the append failed. Retrying can append the same logical input again. |
| Fetch | The group received a bounded delivery attempt with a lease token. | The external work is complete, or another attempt can never occur. |
| Acknowledge | A current token was accepted into the in-memory completion window. The response reports volatile and persisted progress separately. | That this acknowledgement has necessarily reached a durable checkpoint. |
| Checkpoint | The group's contiguous next offset was published successfully to persistent metadata. | External side effects were committed atomically with the checkpoint. |
| Retain/delete | Only whole closed segments below both groups' persisted progress became eligible for deletion. | An active segment, an unread record, or a stalled group's work may be discarded to make room. |

The contract is **at-least-once delivery**, conditional on eventual consumer progress and available storage, not exactly-once external processing. Consumers acknowledge only after completing their work. A process failure after that work but before its checkpoint can repeat the work. Producer retries that create different offsets and delivery retries of one offset are counted separately. The distinction between publishing, consumption, and external effects is also discussed in Apache Kafka's versioned [message-delivery design](https://kafka.apache.org/41/design/design/#message-delivery-semantics); this broker does not implement Kafka's additional distributed guarantees.

### Failure model

The mandatory recovery model is termination and restart of the broker while the operating system and filesystem remain running. Tests also inject short writes, write/sync errors, incomplete trailing records, and complete corrupt records. They do not establish survival of sudden physical power loss, a lost disk, arbitrary file replacement, or corrupted external consumer state.

Use explicit synchronization and handle its errors. Rust's [`File::sync_all`](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all) is distinct from a successful write; file-drop errors are not reported to the caller. The course does not turn that API into an unsupported physical-storage guarantee.

A complete record with an invalid checksum, an unexplained offset gap, or corruption in an immutable segment is a startup error. Only an incomplete final append in the active segment is eligible for tail repair under the append protocol. A valid full header must be checked before trusting its declared body length. After a write or sync error, the owner refuses further mutations instead of continuing from an uncertain cursor. The supervisor stops the broker; restart performs recovery before reopening admission.

## Architecture: ownership follows the state

A partition owner holds the log files, sparse index, pending append batch, two group windows, and progress metadata. It serializes mutations to that partition. Network tasks never hold a file cursor or a mutable reference into an owner's buffers. Requests carry owned data and a reply channel; replies carry owned, accounted-for buffers.

```text title="The completed broker"
Producer sessions --> producer admission ----> data lane -----+
                                                              |
Consumer sessions --> response reservation --> fetch lane ----+--> partition owner
Acks/checkpoints ---> reserved control lane -------------------+     (one of four)
                                                                     |
                    supervisor <---- worker result                   +--> log + index
                         |                                           +--> group windows
                         +--> stop admission, drain, join             +--> checkpoints
                                                                     +--> retention
```

The data, fetch, and acknowledgement/control paths have separate limits. Scheduling gives control work recurring service without starving append work. An owner must not wait for payload capacity that only its own pending acknowledgements can release. Blocking file operations run on the dedicated threads, not on async runtime workers. Tokio's documentation distinguishes finite [`spawn_blocking` work from dedicated threads](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html); already-running blocking work cannot simply be aborted.

Reads and deletion are serialized by the same owner. A fetch copies a bounded response while the source segment is still owned; the response no longer depends on the file when deletion is considered. This deliberately avoids an initial memory-mapped or shared-file-pin design. The cost of the copy remains visible in measurements.

### Four different offsets

All progress values are **exclusive next offsets**. For partition $p$ and group $g$, let $W_p$ be the next append offset, $V_p$ the end of the synchronized visible prefix, $A_{g,p}$ the first offset not yet contiguously acknowledged in memory, and $D_{g,p}$ its persisted checkpoint. The implementation maintains:

$$
0 \le D_{g,p} \le A_{g,p} \le V_p \le W_p.
$$

For example, a new group has $A=0$. It finishes offsets 0, 2, and 3 while offset 1 remains outstanding. Its contiguous progress is 1, not 4. Once offset 1 completes, progress becomes 4. Until that value is checkpointed, its persisted progress may still be 0 and recovery must allow repeats from 0.

Let $R_p$ be the first retained offset. With two groups, safe retention also requires:

$$
R_p \le \min(D_{0,p}, D_{1,p}).
$$

A closed segment ending at exclusive offset $E$ is eligible only when $E \le \min(D_{0,p}, D_{1,p})$. If a segment covers offsets 0 through 3 and the group checkpoints are 6 and 3, it stays. It becomes eligible when the second group reaches 4. The active segment always stays. Retention publishes its new floor before removing eligible files, so restart can finish an interrupted deletion without interpreting it as an unexplained gap.

These symbols map to integer fields and comparisons, not to a separate mathematical subsystem. The chapters derive the window, lookup, and accounting calculations with small traces before implementing their loops.

### Leases, restart epochs, and progress

A delivery token contains the restart epoch, group, partition, offset, and attempt generation. Within one run, a retry increments its generation. Before serving any client after restart, the broker persists a new epoch under the exclusive data-directory lock. Both counters use checked arithmetic; exhaustion fails rather than wrapping into a previously valid token.

Lease time is monotonic within a run and is not restored from wall-clock timestamps. At or beyond its deadline, a token is expired even if a periodic retry scan has not visited it yet. The owner checks that rule when processing an acknowledgement. A previous attempt or epoch cannot advance progress. This fencing protects broker state; it cannot stop a stale consumer from making an external side effect.

The completion window holds at most a configured number of offsets from its contiguous base. Completed entries above a gap still occupy the window. Lease state and completion bits are volatile; only the contiguous checkpoint is authoritative across restart. Published metadata uses a same-directory temporary file, explicit synchronization, replacement, and the platform-appropriate directory handling. [`rename`](https://doc.rust-lang.org/std/fs/fn.rename.html) has platform and filesystem boundaries that the implementation must respect. A corrupt authoritative checkpoint is not silently replaced by an older one after records may already have been deleted.

### Resource limits are part of the algorithm

The starting demonstration configuration uses a 64 KiB payload limit, 8 MiB segment target, 256-offset window per group/partition, and 256 KiB encoded fetch-response limit. These are tunable workload limits, not measured performance claims. Eight windows give at most $4 \times 2 \times 256 = 2048$ tracked delivery positions. A retry timer may not accumulate an unbounded history of obsolete attempts; the first implementation scans the bounded windows.

The payload-buffer allowance is 64 MiB: 8 MiB of append capacity and 4 MiB of fetch-response capacity per partition, plus 16 MiB of fixed owner scratch capacity. Thus $4 \times 8 + 4 \times 4 + 16 = 64$ MiB. Reserved capacity follows the owning buffer through a mailbox, batch, and socket write, including abandoned replies. Actual allocation capacity and simultaneous encoding copies count; payload length alone is not sufficient. Small fixed headers, connection state, indexes, window entries, thread stacks, allocator overhead, and operating-system caches are described and measured separately. **64 MiB is not a process-RSS promise.**

The default logical data-directory allowance is 512 MiB. Reserve 8 MiB for bounded metadata and rotation overhead, leaving four 126 MiB partition quotas for segment files. Derived indexes initially live only in memory. Account for existing file lengths and outstanding append reservations before admitting writes; the logical cap has no intentional overshoot. Physical filesystem allocation is observed separately. A hot partition can exhaust its quota while another has space: cross-partition borrowing is deliberately absent.

A full disk budget returns bounded overload rather than holding unlimited producers. A healthy consumer/control connection must still acknowledge, checkpoint, and release eligible space. The design assumes file operations eventually return; a blocked device cannot be repaired by queue scheduling. Tokio's [weighted semaphore behavior](https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html) supplies useful ownership primitives, but not the broker's pool separation, admission policy, or deadlock argument.

## Starting knowledge and native environment

Basic programming in C or Python is sufficient. No prior Rust, asynchronous Rust, broker implementation, or storage-engine experience is assumed. Chapters 1–6 are actual implementation chapters with small complete programs. They establish the language tools before the broker is built, rather than asking the reader to learn Rust while debugging an unexplained storage system.

Use native macOS on an Apple M4 Mac mini with 16 GB of memory. No virtual machine, Linux-only tracing baseline, x86 assembly, container environment, or cloud account is required. Inspect the actual machine rather than assuming its macOS release:

```sh title="Observe the local toolchain; these are not installation commands"
uname -m
sw_vers
xcrun clang --version
rustc -vV
cargo --version
```

A maintained stable Rust toolchain, Cargo, and working native linker are the suggested baseline. The manifests use edition 2024, introduced in Rust 1.85; the directory-lock implementation uses `File::try_lock`, stabilized in Rust 1.89. These are [edition](https://doc.rust-lang.org/edition-guide/rust-2024/index.html) and [API](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock) compatibility facts, not instructions to install either old release. Dependencies can impose a higher actual minimum. Compatible newer installations are acceptable; an equivalent locking adapter can support a different toolchain without changing the storage contract. Rust's upstream [security policy](https://rust-lang.org/policies/security/#receiving-security-updates) supports the current stable release rather than an upstream LTS channel.

Introduce Tokio only in Chapter 6, using its maintained 1.x line with the features actually used: runtime, macros, synchronization, time, and later networking, I/O, and signals. Introduce [`crc32fast`](https://docs.rs/crc32fast/latest/crc32fast/) in Chapter 7 for IEEE CRC32 calculation. The learner retains `Cargo.lock` for reproducible application builds; dependency versions are recorded when resolved rather than prescribed as permanent gates. The Cargo [workspace model](https://doc.rust-lang.org/cargo/reference/workspaces.html) provides a common lockfile, build directory, and workspace-wide checks. Optional formatting, linting, debugger, and platform profiling tools supplement—not replace—the executable checks.

## The implementation workspace

The following is the layout reached during the course, not a requirement to create empty modules in advance. `language-lab` is the only member needed at the beginning. The library and application members appear when their first working capability is introduced.

```text title="Learner workspace on work/partition-broker"
Cargo.toml
Cargo.lock
crates/
  language-lab/
    Cargo.toml
    src/lib.rs
    examples/
      workspace_probe.rs
      borrowed_record.rs
      fallible_frame.rs
      generic_scan.rs
      thread_handoff.rs
      cancel_permits.rs
  broker-core/
    Cargo.toml
    src/
      lib.rs
      types.rs
      error.rs
      codec.rs
      io.rs
      storage/
        mod.rs
        directory.rs
        segment.rs
        index.rs
        metadata.rs
        recovery.rs
        retention.rs
      delivery/
        mod.rs
        window.rs
        lease.rs
        progress.rs
      worker.rs
      mailbox.rs
      budget.rs
    tests/
      codec.rs
      storage.rs
      delivery.rs
      recovery.rs
      pressure.rs
  broker-app/
    Cargo.toml
    src/
      lib.rs
      protocol.rs
      client.rs
      server.rs
      supervisor.rs
      workload.rs
      metrics.rs
      bin/
        brokerd.rs
        brokerctl.rs
        broker-load.rs
    tests/
      wire.rs
      lifecycle.rs
      end_to_end.rs
runs/                         # disposable fixtures and measured outputs; ignored
```

`broker-core` owns the algorithms and file state. It does not depend on socket parsing or command-line flags. `broker-app` owns transport, process supervision, clients, and the final workload. Tests use temporary directories they create themselves; destructive failure cases never target unrelated data. A data directory contains a stable lock file, broker metadata with the epoch, and four partition directories with segments, two group checkpoints, and a retention floor. Sparse indexes are reconstructed from segments and are not trusted persistent state.

## Implementation plan and hours

Follow the chapters in order; each starts from the preceding runnable state and names the interfaces it extends. Foundation programs remain independently runnable. Commands in the outlines below describe checks available **after implementing that chapter**, not binaries already supplied with this roadmap.

The plan contains **80.0 hours of scheduled implementation** plus **8.5 hours of unallocated debugging/integration contingency**, for **88.5 hours total**. Estimates assume a fast implementer following complete explanations and reference routines. They count setup, language exercises, code, tests, diagnosis, integration, and measurement. Reading, textbook writing, and publication are excluded. Focused tests are included in their mechanism chapters; later chapters add cross-subsystem checks rather than charging for those tests twice.

| Chapter | Capability | Implementation hours |
| --- | --- | ---: |
| 1 | Native workspace, modules, and executable checks | 1.5 |
| 2 | Ownership, borrowing, and lifetime-bound views | 2.5 |
| 3 | Enums, fallible parsing, and composed errors | 2.0 |
| 4 | Traits, generics, iterators, and bounded collections | 2.5 |
| 5 | Shared ownership, threads, and synchronization | 2.5 |
| 6 | Futures, async ownership, and cancellation | 2.5 |
| 7 | Record format, typed offsets, and the I/O boundary | 3.5 |
| 8 | Segments, sparse indexing, and bounded reads | 4.5 |
| 9 | Partition owners and synchronized group commit | 6.0 |
| 10 | Recovery, metadata publication, and restart epochs | 6.0 |
| 11 | Bounded delivery and out-of-order completion | 5.0 |
| 12 | Lease expiration, retries, and stale-token fencing | 4.5 |
| 13 | Durable consumer progress | 4.5 |
| 14 | Safe retention and disk admission | 4.5 |
| 15 | Byte budgets and pressure-safe admission | 5.0 |
| 16 | Framed TCP sessions and clients | 4.5 |
| 17 | Cancellation boundaries and supervised shutdown | 4.5 |
| 18 | Deterministic failure schedules and recovery checks | 4.5 |
| 19 | End-to-end coverage and overload demonstration | 5.0 |
| 20 | Resource measurement and one justified optimization | 4.5 |
| | **Scheduled implementation** | **80.0** |
| | **Unallocated contingency** | **8.5** |
| | **Total** | **88.5** |

The phase totals are 13.5 hours for foundations, 20.0 for storage, 18.5 for delivery/progress/retention, 14.0 for admission/network/lifecycle integration, and 14.0 for integrated failure work and measurement. Record actual implementation time by chapter and revise remaining estimates when actuals diverge. The contingency is not another feature list. Keep the failure and ownership mechanisms; reduce optional workload scale or peripheral tooling first.

## Part 1 — Rust before the broker

### Chapter 1 — Native workspace, modules, and executable checks

**Prerequisites:** basic functions, loops, arrays, and command-line use; no Rust code. **Time:** 1.5 hours: 1.0 for setup/programming and 0.5 for checks and diagnosis.

**Sections.** Establish native compiler/linker observations and create a workspace containing only `language-lab`. Distinguish a package, crate, module, library target, example target, and test. Introduce bindings, integer types, function returns, expressions, basic control flow, paths, and the macros used in the example. Separate a small byte-summary function into the library and call it from an executable. Use `cargo check`, `cargo run`, `cargo test`, formatting, and one deliberately failed assertion to inspect a real diagnostic.

**Runnable outcome:** `examples/workspace_probe.rs` summarizes a fixed byte fixture through library functions and verifies its counts and integer arithmetic. `cargo run -p language-lab --example workspace_probe` and `cargo test -p language-lab` need no other workspace member. Correct a module-path error and a numerical boundary error without changing the intended result. No broker files or networking are needed.

### Chapter 2 — Ownership, borrowing, and lifetime-bound views

**Prerequisites:** Chapter 1's workspace and test commands. **Time:** 2.5 hours: 1.75 programming and 0.75 compiler experiments/tests.

**Sections.** Follow values through moves, `Copy`, deliberate cloning, scope exit, and stack/heap ownership. Introduce structs and methods around an owned byte vector. Compare immutable/mutable borrows and slices; show why reallocation cannot overlap a live view. Build a record view with an explicit lifetime tied to its input, then convert a selected view to owned bytes when it must survive buffer mutation. Explain `String` versus byte storage and use byte fixtures without assuming arbitrary payloads are UTF-8.

**Runnable outcome:** `borrowed_record.rs` views a known-valid fixed record, copies one selected payload, ends the borrow, and mutates the original buffer while the independent copy remains valid. It does not yet require a fallible parser. Run `cargo run -p language-lab --example borrowed_record`. Investigate use-after-move, a returned reference to local storage, and mutation during a live borrow as intentional compiler failures, then keep the working example buildable. The [lifetime reference](https://doc.rust-lang.org/book/ch10-03-lifetime-syntax.html) supports the distinction between a borrow's validity and the storage owner's lifetime.

### Chapter 3 — Enums, fallible parsing, and composed errors

**Prerequisites:** Chapter 2's byte ownership and slice views. **Time:** 2.0 hours: 1.25 programming and 0.75 boundary/error tests.

**Sections.** Introduce `enum`, `match`, `Option`, and `Result` before using them to distinguish incomplete input from invalid input. Implement checked length arithmetic and an owned adapter around a borrowed frame probe. Teach `?`, `map_err`, conversions, and error context; implement `Display` and `std::error::Error` for a concrete parser/application error. Explain when a panic is a programming assertion and when an error must be returned to a caller.

**Runnable outcome:** `fallible_frame.rs` handles partial headers, partial bodies, two coalesced frames, an empty frame, an over-limit declaration, and end-of-input. `cargo run -p language-lab --example fallible_frame` verifies that incomplete/invalid input is not consumed. A deliberately truncated fixture reports its location rather than panicking. The complete probe, consume adapter, error helpers, and fixtures are all part of this program; no network decoder is assumed.

### Chapter 4 — Traits, generics, iterators, and bounded collections

**Prerequisites:** Chapter 3's parser and error types. **Time:** 2.5 hours: 1.75 programming and 0.75 comparison/boundary checks.

**Sections.** Define a small trait and a generic record scanner with useful bounds, then compare static dispatch with a narrowly used trait object. Introduce closures and iterator ownership: consuming items, borrowing items, and collecting only when necessary. Choose between `Vec`, `VecDeque`, and an ordered map for concrete lookup/window needs. Implement a fixed-capacity ring and a predecessor search, including empty input and wraparound. Introduce `Box` where owned indirection is genuinely useful; do not build a framework of generic policies.

**Runnable outcome:** `generic_scan.rs` runs the same scan over two simple sources and compares the bounded predecessor lookup with a linear reference. `cargo run -p language-lab --example generic_scan` checks boundary keys and repeated ring wraparound. The tests expose capacity versus length and accidental full-input collection. These are language exercises, not the production log or consumer window; later chapters give those structures their own invariants.

### Chapter 5 — Shared ownership, threads, and synchronization

**Prerequisites:** Chapters 1–4, including the bounded ring. **Time:** 2.5 hours: 1.5 programming and 1.0 synchronization/cleanup checks.

**Sections.** Contrast `Rc`, `RefCell`, `Arc`, and `Mutex` by ownership and permitted mutation. Explain `Send` and `Sync` before moving work to another thread; neither means that arbitrary compound operations are automatically correct. Implement a small bounded two-lane handoff with `Mutex` and `Condvar`, a predicate loop, and explicit closed state. Use an RAII guard to return a reservation and join every worker. Compare a lock-protected metric with an atomic observation counter while keeping correctness-critical decisions under the state owner.

**Runnable outcome:** `thread_handoff.rs` transfers owned messages, demonstrates a full queue, closes admission, drains accepted work, and joins the thread. `cargo run -p language-lab --example thread_handoff` verifies that control work is not permanently starved and reservations return on error. Diagnose a lost-notification pattern and premature closure. Explain rejected `Rc` transfer using Rust's [`Send`/`Sync` model](https://doc.rust-lang.org/book/ch16-04-extensible-concurrency-sync-and-send.html), without introducing manual unsafe implementations.

### Chapter 6 — Futures, async ownership, and cancellation

**Prerequisites:** Chapter 5's ownership, synchronization, and cleanup program. **Time:** 2.5 hours: 1.5 programming and 1.0 cancellation experiments.

**Sections.** Introduce `async`, `.await`, `Future::poll`, `Pending`/`Ready`, and waking with a small safe example; use Tokio rather than writing an executor. Explain what state survives suspension, why spawning needs owned data and appropriate `Send`/`'static` bounds, and what those bounds do not mean. Introduce `Pin`/`Unpin` at the point a future is pinned without unsafe self-referential structures. Trace owned permits through `select!`, early return, task completion, dropping a future, and dropping a join handle.

**Runnable outcome:** `cancel_permits.rs` uses explicit barriers/notifications rather than timing luck to cancel before and after a resource handoff. `cargo run -p language-lab --example cancel_permits` demonstrates restored capacity and distinguishes abandoning a reply from cancelling transferred work. No lock guard crosses an await accidentally. Read Tokio's [spawn bounds](https://tokio.rs/tokio/tutorial/spawning), [`select!` cancellation rules](https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety), and Rust's [`Pin` contract](https://doc.rust-lang.org/std/pin/index.html) alongside the actual state trace. This is the last standalone foundation chapter.

## Part 2 — Authoritative storage

### Chapter 7 — Record format, typed offsets, and the I/O boundary

**Prerequisites:** all six foundation programs. **Time:** 3.5 hours: 2.5 implementation and 1.0 format/error tests.

**Sections.** Create `broker-core` and introduce newtypes for partitions, groups, offsets, and epochs so unrelated integers are not interchangeable. Specify the segment and record byte layouts with explicit endianness, versioning, checked lengths, header integrity, body checksum, and maximum sizes. Implement full encode/decode routines and enumerate valid, incomplete, and corrupt outcomes. Define the small storage-I/O interface used by real files and a short-write/failure adapter. Hold a non-replaced lock-file handle for exclusive data-directory ownership; a leftover lock filename is not itself a live lock.

**Implementation outcome:** `types.rs`, `error.rs`, `codec.rs`, `io.rs`, and `storage/directory.rs` can round-trip records and reject malformed headers before allocating their declared bodies. `cargo test -p broker-core --test codec` exercises golden bytes, exact limits, overflow, and checksum failures. A two-process lock check rejects a second broker owner and succeeds after the first process exits. The CRC dependency computes checksums only; the learner writes every layout and validation decision.

### Chapter 8 — Segments, sparse indexing, and bounded reads

**Prerequisites:** Chapter 7's codec, directory guard, and file adapter. **Time:** 4.5 hours: 3.25 implementation and 1.25 storage tests/diagnosis.

**Sections.** Append whole records to a bounded active segment and rotate before its size limit would be exceeded. Name segments by base offset and handle an empty new tail without reusing offsets. Build an in-memory sparse offset-to-byte-position index, use predecessor lookup, and scan forward with validated record boundaries. Bound record count and response bytes independently. Explain file-position ownership and ensure reads restore or explicitly set the next append position.

**Implementation outcome:** `storage/segment.rs` and `storage/index.rs` support append, rotation, and offset reads in a single-threaded fixture. `cargo test -p broker-core --test storage` compares indexed reads with a full-scan oracle at every boundary of several small segments. Deleting the in-memory index and rebuilding it preserves the result. An over-large record is rejected, not placed in an over-large segment. This chapter does not yet expose a durable network publish operation; synchronization and replies arrive in Chapter 9.

### Chapter 9 — Partition owners and synchronized group commit

**Prerequisites:** Chapter 8's log/index and Chapters 5–6's handoff/cancellation programs. **Time:** 6.0 hours: 4.25 implementation and 1.75 interleaving/failure checks.

**Sections.** Give each of four dedicated threads one partition's mutable state and route owned commands through bounded lanes. Adapt the small handoff into `mailbox.rs`, with a timed wait for batch deadlines, explicit closure, and recurring control service. Accumulate batches under count, byte, and time limits; explain the difference between assigned, written, visible, and replied offsets. Complete synchronization before advancing visibility or sending successful publish replies. Poison the owner on uncertain file I/O, fail outstanding requests, and report the failure through a joined result path.

**Implementation outcome:** `worker.rs` and an in-process driver concurrently append to four partitions while preserving each owner's order. Tests with a controllable I/O adapter hold a sync call and prove that no earlier reply or fetch visibility escapes. A failed sync produces no success for that batch. Small bounded fixtures operate without the final network server or full asynchronous budget manager; no production memory-limit claim is made yet. Shutdown at this stage joins owners explicitly and never silently detaches them.

### Chapter 10 — Recovery, metadata publication, and restart epochs

**Prerequisites:** Chapter 9's synchronized owner and Chapter 7's fault-capable I/O boundary. **Time:** 6.0 hours: 4.0 implementation and 2.0 recovery fixtures/debugging.

**Sections.** Scan segments in offset order, rebuild sparse indexes, and derive append/visible ends without trusting old in-memory state. Distinguish incomplete active-tail bytes from complete checksum failure, invalid headers, immutable-segment corruption, and gaps. Repair only eligible tails, reset the append position, and synchronize recovery changes before admitting work. Implement same-directory metadata replacement and a first-creation protocol that does not mistake a partially initialized directory for a healthy broker. Allocate and publish a checked restart epoch before opening service.

**Implementation outcome:** `storage/recovery.rs` and `storage/metadata.rs` reopen valid fixtures, retain valid complete but previously unconfirmed records, repair an interrupted last append, and reject unexplained damage. `cargo test -p broker-core --test recovery` includes an empty rotated tail and interruptions around metadata publication. Old delivery identities cannot share the new epoch. A complete record found after a lost reply is allowed; recovery does not invent knowledge of which producer received a response. Existing directories with missing authoritative metadata fail closed rather than resetting counters.

## Part 3 — Delivery, progress, and reclamation

### Chapter 11 — Bounded delivery and out-of-order completion

**Prerequisites:** Chapter 10's recovered visible prefix. **Time:** 5.0 hours: 3.5 implementation and 1.5 state-machine/reference checks.

**Sections.** Define independent group state over one shared partition log. Turn the foundation ring into a fixed-width delivery window, with explicit empty, outstanding, and completed states. Fill new positions only within the window and visible prefix; completed positions above a gap remain occupied. Mark completions and advance only over a contiguous prefix, using checked logical offsets and wrapped physical slots. Integrate bounded file reads while leaving storage ownership on the partition thread.

**Implementation outcome:** `delivery/window.rs` supports two groups progressing differently without duplicating the log. `cargo test -p broker-core --test delivery` exercises acknowledgement orders such as 2, 0, 3, 1, a stalled first offset, and several ring wraps against a simple set-based reference. The chapter's in-process completion interface is not exposed to untrusted clients; full token validation is added next. A fetch limited by bytes may return fewer records than the window has space for, and an empty visible suffix is not an error.

### Chapter 12 — Lease expiration, retries, and stale-token fencing

**Prerequisites:** Chapter 11's window and Chapter 10's persisted epoch. **Time:** 4.5 hours: 3.0 implementation and 1.5 deadline/interleaving tests.

**Sections.** Introduce a monotonic clock interface with a manually advanced test clock. Attach deadlines and checked attempt generations to outstanding positions. Select expired retries without growing an unbounded timer structure or allocating a new window slot for the same offset. Validate epoch, group, partition, offset, generation, and deadline on every acknowledgement. Define no-mutation responses for stale or already-retired tokens and bound retry/new-delivery scheduling.

**Implementation outcome:** `delivery/lease.rs` redelivers expired work under a fresh token, rejects the previous token, and rejects an old token after restart. Exact-deadline tests require no sleep and cover acknowledgement-versus-expiry ordering. `cargo test -p broker-core --test delivery` confirms that duplicates do not double-count completion or advance through a gap. The lease does not cancel external consumer work, and a process restart intentionally forgets volatile attempts rather than pretending to resume a timer.

### Chapter 13 — Durable consumer progress

**Prerequisites:** Chapter 12's valid acknowledgements and Chapter 10's publication helper. **Time:** 4.5 hours: 3.0 implementation and 1.5 checkpoint/failure tests.

**Sections.** Separate volatile next offset from persisted next offset in both state and responses. Encode checksummed, versioned checkpoints and validate them against the recovered log and group identity. Publish only contiguous progress, on a bounded periodic policy or explicit checkpoint request. Explain what happens before replacement, after replacement, and after a failure that leaves publication outcome uncertain. On restart, rebuild the empty delivery window at persisted progress and allow repeated side effects rather than skipping uncheckpointed work.

**Implementation outcome:** `delivery/progress.rs` implements `AckAccepted` and `Checkpointed` with distinguishable meanings. Tests crash between completion and checkpoint, then between checkpoint-publication steps, and verify the correct repeat range. A checkpoint cannot exceed the synchronized visible end. Missing initial checkpoints are allowed only during defined initialization; missing or corrupt authoritative checkpoints in an established directory are errors. No old-file fallback may claim records are available after retention has removed them.

### Chapter 14 — Safe retention and disk admission

**Prerequisites:** Chapter 13's persisted group progress and Chapter 8's segment boundaries. **Time:** 4.5 hours: 3.0 implementation and 1.5 retention/restart/pressure tests.

**Sections.** Compute the minimum persisted group progress and select only whole eligible closed segments. Publish a monotonic retained floor before unlinking files, validate its relationship to checkpoints, and complete interrupted deletion during recovery. Serialize fetch copying and deletion through the owner; do not reclaim a file while a read still needs it. Account for file lengths, pending append reservations, and reserved metadata/rotation space under fixed partition quotas. Keep failure handling conservative when deletion or metadata publication fails.

**Implementation outcome:** `storage/retention.rs` and the disk-admission portion of `budget.rs` allow a stalled group to fill its partition quota without dropping its records. Acknowledging and checkpointing that group makes safe space reusable. `cargo test -p broker-core --test pressure` checks no-delete before persistence, exact segment boundaries, interrupted cleanup, and no intentional cap overshoot. The active segment is preserved even when fully consumed; rotation policy and reserved headroom prevent an accidental need to delete it to admit the next record.

## Part 4 — Bounded asynchronous service

### Chapter 15 — Byte budgets and pressure-safe admission

**Prerequisites:** Chapter 14's disk policy, Chapter 9's lanes, and Chapter 6's async resource ownership. **Time:** 5.0 hours: 3.5 implementation and 1.5 saturation/cancellation checks.

**Sections.** Distinguish item count, encoded length, allocation capacity, and reserved bytes. Implement separate per-partition append and response pools plus fixed owner scratch, including all simultaneous copies. Carry RAII reservations with owned commands and replies; acquire response capacity before requesting a fetch. Combine mailbox-slot and byte admission without holding a mutex across an await, losing a notification, or waiting inside the owner on capacity it must itself release. Bound waiting callers through connection/operation limits and use explicit overload/deadline responses rather than an unbounded waiting list.

**Implementation outcome:** the complete `budget.rs` and asynchronous mailbox adapter handle cancellation before admission, after capacity acquisition, after handoff, and after reply abandonment. `cargo test -p broker-core --test pressure` saturates append pools while a healthy consumer still acknowledges and checkpoints. A request larger than its pool is rejected immediately. Allocation failure, failed admission, and ordinary overload leave consistent reservations. Bounded count alone is never used as evidence of the 64 MiB payload allowance.

### Chapter 16 — Framed TCP sessions and clients

**Prerequisites:** Chapter 15's bounded core APIs and Chapter 3's partial-frame rules. **Time:** 4.5 hours: 3.0 implementation and 1.5 wire/integration checks.

**Sections.** Create `broker-app` and a versioned length-prefixed protocol for publish, fetch, acknowledge, checkpoint, and small status queries. Specify the request/response fields and error meanings, including ambiguity after a disconnect. Decode a fixed header before reserving or reading the body; bound frame sizes, partial-input storage, connections, and output buffers. Give producer and consumer/control listeners independent connection capacity, enforce socket deadlines, and allow only one outstanding request per connection in the baseline. Implement a small client and command-line producer/consumer without hiding framing in a broker library.

**Implementation outcome:** `protocol.rs`, `client.rs`, `server.rs`, `brokerd`, and `brokerctl` exchange real records over loopback. `cargo test -p broker-app --test wire` splits a frame at every header/body boundary, coalesces frames, disconnects a slow peer, and rejects oversized input before its body is allocated. Consumer work runs before acknowledgement; explicit checkpointing is visible. Socket closure releases response memory without rolling back an append already owned by a partition.

### Chapter 17 — Cancellation boundaries and supervised shutdown

**Prerequisites:** Chapter 16's network service and all previous state owners. **Time:** 4.5 hours: 3.0 implementation and 1.5 lifecycle/fatal-error tests.

**Sections.** Define the lifecycle from starting through serving, quiescing, draining, and stopped or failed. Stop new admission, close listeners, resolve already accepted commands, flush batches, checkpoint eligible progress, and join workers in an order that preserves resources. Distinguish a client's cancelled wait from cancellation of an accepted operation. Track every task/thread, report panics and fatal storage errors, and wake blocked admission waiters. Bound network draining while reporting honestly when a blocking file operation has not completed; a timeout is not successful termination.

**Implementation outcome:** `supervisor.rs` coordinates normal shutdown and a fatal partition error. `cargo test -p broker-app --test lifecycle` stops the service with full queues, a partial frame, a pending sync, and an abandoned reply. No success is emitted for unconfirmed work and no owner is quietly detached. A process can restart only after the previous owner exits or its directory lock is released. Tokio's [shutdown discussion](https://tokio.rs/tokio/topics/shutdown) provides the notification/waiting primitives; the dependency order and accepted-work policy are implemented here.

## Part 5 — Failure evidence and measured behavior

### Chapter 18 — Deterministic failure schedules and recovery checks

**Prerequisites:** Chapter 17's complete lifecycle and the earlier I/O/clock seams. **Time:** 4.5 hours: 2.75 implementation and 1.75 failure diagnosis.

**Sections.** Extend the focused fixtures into short, deterministic multi-step schedules across append, completion, checkpoint, and retention. Enumerate meaningful interruption points without a general model-checking framework. Compare the optimized structures with small reference states and a seeded schedule generator. Inject partial writes and sync failures, and separately construct damaged-file fixtures to avoid confusing synthetic corruption with a physical power event. Check cleanup and restart idempotence when an interrupted recovery is rerun.

**Implementation outcome:** cross-subsystem tests in `broker-core/tests` cover a lost publish reply, a progress gap during checkpoint, stale acknowledgement after retry, checkpoint/floor publication before deletion, and recovery interrupted a second time. Each failing schedule prints a reproducible seed and short transition trace. Existing unit tests are reused, not counted as new work. All destructive tests operate on fresh fixture directories, and a complete bad checksum is consistently refused rather than silently repaired.

### Chapter 19 — End-to-end coverage and overload demonstration

**Prerequisites:** Chapter 18's state/failure checks and Chapter 16's real clients. **Time:** 5.0 hours: 3.0 harness/integration and 2.0 scenario diagnosis.

**Sections.** Build `broker-load` and an external expected-record ledger keyed by producer logical ID and returned `(partition, offset)`. Track confirmed, ambiguous, rejected, delivered, and checkpointed outcomes separately. Drive several producers and both groups, deliberately stall or kill a consumer, and submit an obsolete token. Kill only the broker child process while the checker remains alive, restart it on the same directory, and compare retained data with the appropriate expected set. Exercise an aggressive small-budget configuration before a sustained bounded run.

**Implementation outcome:** `workload.rs` and `tests/end_to_end.rs` execute one reproducible scenario covering normal traffic, group lag, overload, consumer loss, broker restart, duplicates, and safe reclamation. The durable-coverage phase disables retention or deliberately keeps group progress low so confirmed records remain inspectable. A separate retention phase checks group coverage before deletion; deleted processed records are not reported as loss. The ledger remains outside the broker's directory and failure domain. `cargo test -p broker-app --test end_to_end` supplies a small automated run; the chapter also gives the complete manual demonstration commands.

### Chapter 20 — Resource measurement and one justified optimization

**Prerequisites:** Chapter 19's reproducible workload, correctness checker, and complete broker. **Time:** 4.5 hours: 2.0 measurement/analysis, 1.5 focused optimization, and 1.0 regression/repeat runs.

**Sections.** Measure confirmed append throughput, useful consumer throughput, latency distributions, admission wait, owner service time, recovery duration, rejection/timeouts, application buffer high-water marks, RSS, and logical/physical disk observations. Implement bounded histograms or bounded samples with stated precision and handle rejected requests rather than reporting only successes. Compare per-record synchronization with bounded batching under the same success contract and fixed workload settings. Inspect one measured bottleneck—such as copies, index scan distance, or batch flush frequency—make a focused change, and rerun correctness plus comparable measurements.

**Implementation outcome:** `metrics.rs`, workload reporting, and repeatable result files under `runs/` produce a before/after comparison with toolchain, configuration, record sizes, warmup, duration, and sample counts. Closed-loop concurrency and rate-controlled offered load are labelled separately; a rate-controlled generator bounds its own queue and reports missed schedule/deferred requests. No throughput target is assumed. A negative optimization result is valid when diagnosed; retain or revert it explicitly. The final run demonstrates the same delivery, recovery, retention, and memory-accounting contracts after the change.

## The final demonstration

Start with a small fixture that can be inspected record by record. Publish uniquely labelled inputs into all four partitions, let two groups advance at different rates, and retain the producer's confirmed-offset ledger outside the broker. Stop one consumer after it performs work but before it checkpoints. On retry, the same offset may repeat, while an obsolete acknowledgement must not change progress.

Next interrupt the broker process without its graceful-shutdown path. After recovery, verify that confirmed records still required by either group are present, that complete unconfirmed records are allowed, and that partial active-tail bytes are treated according to the format. Validate corrupt-file refusal and injected I/O errors in separate disposable fixtures. A process-kill result is not relabelled a power-loss experiment.

Finally fill a reduced partition quota with a stalled group. Publishers must receive bounded overload while healthy acknowledgement/control traffic remains usable. Resume the stalled group, checkpoint its contiguous progress, and observe safe whole-segment reclamation and renewed admission. Report duplicates and legitimate retention separately from missing required records.

Run the bounded performance comparison only after those checks. Preserve the configuration and measurement output needed to reproduce the observed tradeoff. The end product is a working broker whose limits and failure behavior can be explained from its code, together with evidence that its interacting mechanisms behave as specified on the actual machine.
