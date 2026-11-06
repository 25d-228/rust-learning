# Repository working rules

This repository contains the Rust Systems Engineering book and its learner workspace.

- `main` holds the concise course README/index and this guidance.
- `gh-pages` holds canonical `book/` Markdown, generated root-level website files, and book build tools, not learner source.
- Implement the Durable Partitioned Event Broker on persistent `work/partition-broker` from `main`. Keep the language exercises, broker, clients, and harness in that one workspace; no branch-per-function ceremony is needed.

Read the relevant complete chapter and its prerequisite code before implementing. Explain, review, and diagnose by default. Implement only the scope explicitly delegated by the user. Passing a check or publishing a lesson does not itself permit closing its issue; ask before closing a whole issue. Do not add an approval ceremony between functions when the implementation scope is already delegated.

The technical baseline is one process, one topic, four fixed partitions, two preconfigured groups, native macOS, and safe Rust. Preserve the stated append-success, acknowledgement, checkpoint, recovery, and resource-ownership boundaries. Runtime/transport libraries must not replace the core storage, delivery, admission, or supervision algorithms. Do not silently expand into clustering, Kafka compatibility, or exactly-once claims.

Preserve user edits and existing branch history. Do not merge `gh-pages` into code branches to acquire documentation. Read the site/issues or fetch book source separately. Documentation updates on `main` may be brought into the work branch with a normal merge only after inspecting local changes; never reset learner work.

Edit each lesson once in canonical `book/` Markdown. The same full body supplies the issue and webpage. Use the supplied Scholar build tools and theme; do not maintain a summary-only website or a second lesson body. Stable chapter IDs are not issue numbers. Publication must use actual returned issue URLs, match existing open and closed issues, and preserve unrelated work. Only an authenticated local publisher performs those operations.

The textbook, not private chat context, defines the implementation. Point out missing requirements rather than inventing them silently. Keep destructive tests in fresh disposable directories. Report which checks actually ran and on which platform; do not turn expected output into a claimed result. Do not publish private notes, credentials, secret-bearing logs, or the complete authoring input package.
