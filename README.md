# Rust Systems Engineering

Build a **Durable Partitioned Event Broker** in Rust: segmented storage, sparse indexes, synchronized batching, crash recovery, bounded delivery windows, lease fencing, durable consumer progress, safe retention, and pressure-aware asynchronous I/O.

The implementation targets native macOS on Apple silicon and is bounded to one process, four partitions, and two consumer groups. The contract is per-partition append order and at-least-once delivery, not replication or exactly-once external processing. Libraries provide runtime and transport plumbing; the storage, delivery, admission, and lifecycle mechanisms are implemented in the course.

## Read the book

<!-- course-links:start -->
- [Roadmap — A Durable Partitioned Event Broker](https://25d-228.github.io/rust-learning/index.html) · [Issue](https://github.com/25d-228/rust-learning/issues/1)
- [Chapter 1 — Native workspace, modules, and executable checks](https://25d-228.github.io/rust-learning/chapter-01.html) · [Issue](https://github.com/25d-228/rust-learning/issues/2)
- [Chapter 2 — Ownership, borrowing, and lifetime-bound views](https://25d-228.github.io/rust-learning/chapter-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/3)
- [Chapter 3 — Enums, fallible parsing, and composed errors](https://25d-228.github.io/rust-learning/chapter-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/4)
- [Chapter 4 — Traits, generics, iterators, and bounded collections](https://25d-228.github.io/rust-learning/chapter-04.html) · [Issue](https://github.com/25d-228/rust-learning/issues/5)
- [Chapter 5 — Shared ownership, threads, and synchronization](https://25d-228.github.io/rust-learning/chapter-05.html) · [Issue](https://github.com/25d-228/rust-learning/issues/6)
- [Chapter 6 — Futures, async ownership, and cancellation](https://25d-228.github.io/rust-learning/chapter-06.html) · [Issue](https://github.com/25d-228/rust-learning/issues/7)
- [Chapter 7, Part 1 — Record bytes and typed identities](https://25d-228.github.io/rust-learning/chapter-07.html) · [Issue](https://github.com/25d-228/rust-learning/issues/8)
- [Chapter 7, Part 2 — File I/O and directory ownership](https://25d-228.github.io/rust-learning/chapter-07-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/9)
- [Chapter 8, Part 1 — Segments and sparse record lookup](https://25d-228.github.io/rust-learning/chapter-08.html) · [Issue](https://github.com/25d-228/rust-learning/issues/10)
- [Chapter 8, Part 2 — Rotation, bounded reads, and storage checks](https://25d-228.github.io/rust-learning/chapter-08-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/11)
- [Chapter 9, Part 1 — Dirty-file synchronization and timed mailboxes](https://25d-228.github.io/rust-learning/chapter-09.html) · [Issue](https://github.com/25d-228/rust-learning/issues/12)
- [Chapter 9, Part 2 — Owned commands and synchronized publication](https://25d-228.github.io/rust-learning/chapter-09-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/13)
- [Chapter 9, Part 3 — Controlled failures and four-partition checks](https://25d-228.github.io/rust-learning/chapter-09-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/14)
- [Chapter 10, Part 1 — Validate before repairing](https://25d-228.github.io/rust-learning/chapter-10.html) · [Issue](https://github.com/25d-228/rust-learning/issues/15)
- [Chapter 10, Part 2 — Publish the startup epoch](https://25d-228.github.io/rust-learning/chapter-10-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/16)
- [Chapter 10, Part 3 — Recovery fixtures and a process interruption](https://25d-228.github.io/rust-learning/chapter-10-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/17)
- [Chapter 11, Part 1 — A window that cannot advance through a gap](https://25d-228.github.io/rust-learning/chapter-11.html) · [Issue](https://github.com/25d-228/rust-learning/issues/18)
- [Chapter 11, Part 2 — Delivery on the partition owner](https://25d-228.github.io/rust-learning/chapter-11-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/19)
- [Chapter 11, Part 3 — Completion orders, bounded reads, and restart checks](https://25d-228.github.io/rust-learning/chapter-11-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/20)
- [Chapter 12, Part 1 — Attempts, deadlines, and a bounded retry window](https://25d-228.github.io/rust-learning/chapter-12.html) · [Issue](https://github.com/25d-228/rust-learning/issues/21)
- [Chapter 12, Part 2 — Clocked reads and owner integration](https://25d-228.github.io/rust-learning/chapter-12-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/22)
- [Chapter 12, Part 3 — Deterministic expiry and restart checks](https://25d-228.github.io/rust-learning/chapter-12-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/23)
- [Chapter 13, Part 1 — Checkpoint bytes and validated startup](https://25d-228.github.io/rust-learning/chapter-13.html) · [Issue](https://github.com/25d-228/rust-learning/issues/24)
- [Chapter 13, Part 2 — Accepted completion and checkpointed progress](https://25d-228.github.io/rust-learning/chapter-13-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/25)
- [Chapter 13, Part 3 — Publication cuts and repeat ranges](https://25d-228.github.io/rust-learning/chapter-13-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/26)
- [Chapter 14, Part 1 — Disk reservations and retained-floor metadata](https://25d-228.github.io/rust-learning/chapter-14.html) · [Issue](https://github.com/25d-228/rust-learning/issues/27)
- [Chapter 14, Part 2 — Segment reclamation and restart reconstruction](https://25d-228.github.io/rust-learning/chapter-14-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/28)
- [Chapter 14, Part 3 — Owner integration and a pressure trace](https://25d-228.github.io/rust-learning/chapter-14-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/29)
- [Chapter 14, Part 4 — Failure cuts and recovery checks](https://25d-228.github.io/rust-learning/chapter-14-part-04.html) · [Issue](https://github.com/25d-228/rust-learning/issues/30)
- [Chapter 15, Part 1 — Capacity-owned buffers and reserved lanes](https://25d-228.github.io/rust-learning/chapter-15.html) · [Issue](https://github.com/25d-228/rust-learning/issues/31)
- [Chapter 15, Part 2 — Bounded storage copies and owner integration](https://25d-228.github.io/rust-learning/chapter-15-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/32)
- [Chapter 15, Part 3 — Asynchronous admission and an owned response](https://25d-228.github.io/rust-learning/chapter-15-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/33)
- [Chapter 15, Part 4 — Pressure, cancellation, and executable checks](https://25d-228.github.io/rust-learning/chapter-15-part-04.html) · [Issue](https://github.com/25d-228/rust-learning/issues/34)
- [Chapter 16, Part 1 — Wire bytes and bounded frame decoding](https://25d-228.github.io/rust-learning/chapter-16.html) · [Issue](https://github.com/25d-228/rust-learning/issues/35)
- [Chapter 16, Part 2 — Sessions that retain resource ownership](https://25d-228.github.io/rust-learning/chapter-16-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/36)
- [Chapter 16, Part 3 — Cancellation-safe clients and loopback commands](https://25d-228.github.io/rust-learning/chapter-16-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/37)
- [Chapter 16, Part 4 — Fragmentation, slow peers, and ambiguity checks](https://25d-228.github.io/rust-learning/chapter-16-part-04.html) · [Issue](https://github.com/25d-228/rust-learning/issues/38)
- [Chapter 17, Part 1 — Admission closure and the final owner barrier](https://25d-228.github.io/rust-learning/chapter-17.html) · [Issue](https://github.com/25d-228/rust-learning/issues/39)
- [Chapter 17, Part 2 — A supervisor that retains its children](https://25d-228.github.io/rust-learning/chapter-17-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/40)
- [Chapter 17, Part 3 — Sessions, commands, and a joined restart](https://25d-228.github.io/rust-learning/chapter-17-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/41)
- [Chapter 17, Part 4 — Controlled shutdown and failure checks](https://25d-228.github.io/rust-learning/chapter-17-part-04.html) · [Issue](https://github.com/25d-228/rust-learning/issues/42)
- [Chapter 18, Part 1 — A small reference state for composed failures](https://25d-228.github.io/rust-learning/chapter-18.html) · [Issue](https://github.com/25d-228/rust-learning/issues/43)
- [Chapter 18, Part 2 — Reproducible owner schedules](https://25d-228.github.io/rust-learning/chapter-18-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/44)
- [Chapter 18, Part 3 — Publication and deletion cuts](https://25d-228.github.io/rust-learning/chapter-18-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/45)
- [Chapter 18, Part 4 — Interrupting recovery again](https://25d-228.github.io/rust-learning/chapter-18-part-04.html) · [Issue](https://github.com/25d-228/rust-learning/issues/46)
- [Chapter 19, Part 1 — External expectations and fresh recovery coverage](https://25d-228.github.io/rust-learning/chapter-19.html) · [Issue](https://github.com/25d-228/rust-learning/issues/47)
- [Chapter 19, Part 2 — Owned child processes and controlled interruption](https://25d-228.github.io/rust-learning/chapter-19-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/48)
- [Chapter 19, Part 3 — Coverage, overload, and safe reclamation](https://25d-228.github.io/rust-learning/chapter-19-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/49)
- [Chapter 19, Part 4 — Executable scenarios and checker counterexamples](https://25d-228.github.io/rust-learning/chapter-19-part-04.html) · [Issue](https://github.com/25d-228/rust-learning/issues/50)
- [Chapter 20, Part 1 — Bounded distributions and explicit observation boundaries](https://25d-228.github.io/rust-learning/chapter-20.html) · [Issue](https://github.com/25d-228/rust-learning/issues/51)
- [Chapter 20, Part 2 — Finite workloads and the index-density candidate](https://25d-228.github.io/rust-learning/chapter-20-part-02.html) · [Issue](https://github.com/25d-228/rust-learning/issues/52)
- [Chapter 20, Part 3 — Recovery, resource sampling, and comparable runs](https://25d-228.github.io/rust-learning/chapter-20-part-03.html) · [Issue](https://github.com/25d-228/rust-learning/issues/53)
- [Chapter 20, Part 4 — Regression checks and the retain-or-revert decision](https://25d-228.github.io/rust-learning/chapter-20-part-04.html) · [Issue](https://github.com/25d-228/rust-learning/issues/54)
<!-- course-links:end -->

## Course at a glance

| Chapters | Implementation focus | Hours |
| --- | --- | ---: |
| 1–6 | Runnable Rust foundations: ownership, lifetimes, errors, traits, collections, threads, futures, and cancellation | 13.5 |
| 7–10 | Record format, segments/indexes, group commit, and recovery | 20.0 |
| 11–14 | Delivery windows, leases, persisted progress, and retention | 18.5 |
| 15–17 | Byte budgets, framed TCP sessions, and supervised shutdown | 14.0 |
| 18–20 | Integrated failure checks, end-to-end demonstration, and measured optimization | 14.0 |
| | Unallocated debugging/integration contingency | 8.5 |
| | **Total implementation estimate** | **88.5** |

Reading is excluded. Chapters name their prerequisite code and executable outcomes. The final demonstration covers consumer loss, broker termination/restart, stale acknowledgements, overload, safe reclamation, and a measured resource/performance tradeoff.

## Working branches

`main` is the concise entry point. `gh-pages` contains the complete book and generated website. Keep learner code on `work/partition-broker` from `main`; do not merge the website branch into it. Read [AGENTS.md](AGENTS.md) before changing implementation or documentation.
