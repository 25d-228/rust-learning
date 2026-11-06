# Chapter 1 — Native workspace, modules, and executable checks

A broker eventually interprets bytes from files and sockets. Before introducing either, we need a smaller capability: put byte-processing code in a library, call it from a separate executable, and distinguish a successful compilation from a correct result. This chapter establishes that workflow with no third-party dependencies.

The executable examines six bytes. It reports the input length, counts zero bytes, and counts bytes in the range 128 through 255. A second library function adds two byte values without losing the carry. Empty input, the 127/128 boundary, the final element, and the 255/256 arithmetic boundary make useful checks even in this small program.

**Starting point:** basic functions, loops, arrays, and terminal use; no previous Rust code. Create only the `language-lab` package. The broker and its asynchronous runtime are not prerequisites.

**Implementation time:** 1.5 hours: about 1.0 for environment setup and writing the program, then 0.5 for tests and deliberate diagnostic experiments. Reading is separate. Record the time actually spent implementing and debugging rather than treating this estimate as a requirement.

> **Learning objectives**
> Build a native Cargo workspace; distinguish packages, crates, modules, and executable targets; read a small Rust function; reason about a loop's index and integer widths; and use a compiler error, an assertion failure, and an overflow failure to diagnose three different mistakes.

## Observe the native toolchain

Run these commands in a macOS terminal. They inspect the existing installation; they do not change it.

```sh title="Terminal — inspect the Mac"
uname -m
sw_vers
xcrun clang --version
rustc -vV
cargo --version
```

For the intended native Apple-silicon setup, `uname -m` should report `arm64`, and the `host` line in `rustc -vV` should be `aarch64-apple-darwin`. The latter is Rust's [Apple-silicon macOS target](https://doc.rust-lang.org/rustc/platform-support/apple-darwin.html), not a Linux target. An `x86_64` result deserves investigation before collecting performance measurements: check whether the terminal or compiler is running through translation. Do not infer the macOS release from the hardware; `sw_vers` reports the actual installation.

Cargo coordinates package builds, tests, and executables; `rustc` compiles Rust. A complete executable also needs the platform's linker and system libraries. A later `cargo run` is a more useful installation check than merely seeing a compiler version, because it reaches linking and execution.

An existing compatible toolchain is sufficient. These manifests select **edition 2024**, which first became available in Rust 1.85. That is a [language-mode compatibility boundary](https://doc.rust-lang.org/edition-guide/rust-2024/index.html), not a recommendation to install that old release. Prefer a maintained stable installation. Rust's [security policy](https://rust-lang.org/policies/security/#receiving-security-updates) supports the current stable release; the course does not invent an upstream LTS channel or require nightly features.

When Rust is absent, the official [installation instructions](https://rust-lang.org/tools/install/) provide the `rustup` installer. On macOS, the documented command is:

```sh title="Terminal — install Rust only when it is missing"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

This downloads and executes an installer; review the official instructions before running it. Follow its prompts, reopen the terminal, and repeat `rustc -vV` and `cargo --version`. Do not layer another installation over a working one merely to reproduce an example version. When using rustup, `rustup show active-toolchain` identifies the toolchain selected for the current directory.

When the Apple tools are absent, `xcode-select --install` requests Apple's Command Line Tools installation. An existing suitable Xcode installation can already supply them. Use Apple's [installation guidance](https://developer.apple.com/documentation/xcode/installing-the-command-line-tools/) rather than guessing a developer-directory path or reinstalling tools after an unrelated Rust error.

## Give the code one workspace and two targets

Use the root of the implementation checkout on `work/partition-broker`, or a new ordinary directory for this exercise. No remote repository access is needed. All file paths and Cargo commands below are relative to that workspace root. Do not create the learner workspace inside a book-source directory, and do not overwrite an existing unrelated `Cargo.toml`.

Create the directories:

```sh title="Terminal — from the chosen workspace root"
mkdir -p crates/language-lab/src crates/language-lab/examples
```

The complete source layout for this chapter will be:

```text title="Chapter 1 workspace"
Cargo.toml
crates/
  language-lab/
    Cargo.toml
    src/
      lib.rs
      byte_summary.rs
    examples/
      workspace_probe.rs
```

Cargo generates `Cargo.lock` and `target/` after a build command. They are not missing source files. Keep the lockfile with the application workspace; ignore build output under `/target/` in the implementation checkout's `.gitignore`. Do not add `Cargo.lock` to the ignore rules. The distinction is that the manifest describes requested dependencies, while Cargo records exact resolved dependencies in the [lockfile](https://doc.rust-lang.org/cargo/guide/cargo-toml-vs-cargo-lock.html). There are no external dependencies to resolve yet.

Create the root manifest:

```toml title="Cargo.toml"
[workspace]
members = ["crates/language-lab"]
resolver = "3"
```

A **workspace** groups packages that share a lockfile and build directory. This root is a **virtual manifest**: it has `[workspace]` but no `[package]`, so the root itself does not define an executable or library. `members` names directories containing package manifests. The explicit `resolver` selects Cargo's dependency-resolution rules; a virtual workspace has no root package edition from which to infer them. See Cargo's [workspace reference](https://doc.rust-lang.org/cargo/reference/workspaces.html#virtual-workspace).

Now create the member manifest:

```toml title="crates/language-lab/Cargo.toml"
[package]
name = "language-lab"
version = "0.1.0"
edition = "2024"
publish = false
```

This is a **package**: a named collection of build targets described by one manifest. `version` describes this package, not the compiler. `publish = false` prevents publishing the package to a registry; it does not prevent local builds. These fields are defined in the [manifest reference](https://doc.rust-lang.org/cargo/reference/manifest.html).

A **crate** is a Rust compilation unit. Our package supplies a library crate rooted at `src/lib.rs` and an executable example crate rooted at `examples/workspace_probe.rs`. A **module** is a namespace inside a crate, not another Cargo package. Cargo discovers these targets from the filenames, so neither a `[lib]` section nor an `[[example]]` section is necessary. The package name uses a hyphen in Cargo commands; the library's default Rust name uses an underscore: `language_lab`. These conventions come from Cargo's [target rules](https://doc.rust-lang.org/cargo/reference/cargo-targets.html).

The separation will matter immediately. The executable is a client of the library; it does not get access to every private item merely because both targets belong to one package.

## Declare a module before importing its functions

A module declaration tells the compiler which source belongs to a crate. At this location, `mod byte_summary;` would load `src/byte_summary.rs`. Adding `pub` makes that module available through the library's public interface. A file lying in `src/` is not automatically imported just because it exists.

Create the entire library root as follows:

```rust title="crates/language-lab/src/lib.rs"
pub mod byte_summary;
```

The module's functions will also need `pub`. Making the module public does not automatically make its contents public. This two-level choice is explained by Rust's [module and privacy rules](https://doc.rust-lang.org/book/ch07-02-defining-modules-to-control-scope-and-privacy.html).

Once the next section creates `byte_summary.rs`, the library's paths will be:

```text title="Names in the library crate"
language_lab
  byte_summary
    summarize
    sum_pair
```

Within that library, `crate::byte_summary::summarize` starts at the current crate's root. Within the **example** crate, the same `crate::` prefix starts at the example, not at the package's library. The example instead uses `language_lab::byte_summary::summarize`. `::` separates path components; it is not a filesystem slash. Rust's [path rules](https://doc.rust-lang.org/book/ch07-03-paths-for-referring-to-an-item-in-the-module-tree.html) make this distinction explicit.

## Read the function signature before writing the loop

We will use this interface:

```text title="The summary interface"
summarize(bytes: &[u8]) -> (usize, usize, usize)
                           length  zeros  high bytes
```

`u8` is an unsigned eight-bit integer, so one value represents one byte from 0 through 255. `usize` is the unsigned integer type used for lengths and indexing; its width depends on the target. `->` introduces the return type. Parentheses with three comma-separated types describe a **tuple**: three values returned together in a fixed order. Arrays, tuples, and integer widths are introduced in the Rust Book's [data-type discussion](https://doc.rust-lang.org/book/ch03-02-data-types.html).

`&[u8]` is a shared reference to a byte slice. A slice describes a contiguous sequence with a length; it does not use a zero byte as a terminator. The caller retains its data, and this function only reads it. Passing `&fixture` lets an array be viewed as a slice without allocating or copying the array. This is the small amount of borrowing needed here; Chapter 2 will examine moves, mutation, and reference lifetimes explicitly. See the official [slice introduction](https://doc.rust-lang.org/book/ch04-03-slices.html#other-slices).

For the implementation, `let` introduces a binding. A binding cannot normally be reassigned unless declared with `mut`; the counters change, so they will use `let mut`. `let byte` does not need `mut`, because each loop iteration introduces a new local value rather than reassigning an old one. An annotation such as `: usize` fixes the type instead of leaving it to inference. These are the standard [binding and mutability rules](https://doc.rust-lang.org/book/ch03-01-variables-and-mutability.html).

A `while` loop repeats while its condition is `true`. `if` runs its body only when its condition is `true`; integer values are not implicitly treated as Booleans. `+= 1` updates a counter. We deliberately begin with an explicit index rather than an iterator adapter, so the valid indexing range is visible. Iterators will be taught before they become part of the main implementation. The relevant language rules are in [control flow](https://doc.rust-lang.org/book/ch03-05-control-flow.html).

A function definition starts with `fn`; `pub fn` makes that function public. Braces delimit its body. `.len()` calls the slice's length method, and `bytes[index]` selects one element. A line starting with `///` documents the following item rather than executing.

Create `byte_summary.rs` with this complete first function. Subsequent blocks marked with this same filename are appended to it in order.

```rust title="crates/language-lab/src/byte_summary.rs"
/// Returns (input length, zero-byte count, count of bytes at least 128).
pub fn summarize(bytes: &[u8]) -> (usize, usize, usize) {
    let mut index: usize = 0;
    let mut zeros: usize = 0;
    let mut high_bytes: usize = 0;

    while index < bytes.len() {
        let byte = bytes[index];
        if byte == 0 {
            zeros += 1;
        }
        if byte >= 128 {
            high_bytes += 1;
        }
        index += 1;
    }

    (bytes.len(), zeros, high_bytes)
}
```

`bytes[index]` reads one `u8` value; copying this integer does not copy the whole slice. The function creates no heap buffer and changes no caller-owned byte.

The last tuple has **no semicolon**. Rust functions can return the value of their final expression. A semicolon would discard that value and leave the body returning the unit value, written `()`, which would disagree with the declared tuple return type. `return (bytes.len(), zeros, high_bytes);` would also work, but the final-expression form makes this straight-line ending simpler. This distinction is covered by the Rust Book's [function and expression explanation](https://doc.rust-lang.org/book/ch03-03-how-functions-work.html#statements-and-expressions).

### Trace the six bytes, including the last one

Our fixture is `[0, 65, 127, 128, 255, 0]`. Its length is six, so the valid indexes are zero through five. A zero byte is ordinary input, not an instruction to stop. The high-byte count uses the **inclusive** threshold 128.

| Index read | Value | Zeros after this iteration | High bytes after this iteration | Next index |
| --- | ---: | ---: | ---: | ---: |
| 0 | 0 | 1 | 0 | 1 |
| 1 | 65 | 1 | 0 | 2 |
| 2 | 127 | 1 | 0 | 3 |
| 3 | 128 | 1 | 1 | 4 |
| 4 | 255 | 1 | 2 | 5 |
| 5 | 0 | 2 | 2 | 6 |

At index six, `index < bytes.len()` is false. The function returns `(6, 2, 2)` without reading `bytes[6]`. Safe slice indexing would panic on an out-of-bounds index; it is not a request to read adjacent memory. The [slice API](https://doc.rust-lang.org/std/primitive.slice.html) documents length and indexing behavior.

Let $n$ mean the slice length and $i$ the next index to read. Let $z_i$ and $h_i$ be the zero and high-byte counts after the first $i$ bytes have been examined. Because no byte can be both zero and at least 128, the loop maintains:

$$
0 \le z_i + h_i \le i \le n.
$$

Initially all three counters are zero. An iteration is entered only when $i<n$, reads exactly element $i$, increments at most one classification count, and then increases $i$ by one. At exit $i=n$. For an empty slice, $n=0$ and the loop is never entered; the return value is `(0, 0, 0)`.

This also explains why the counters fit: none exceeds a length already represented as `usize`. The implementation scans each byte once and uses a fixed number of local counters, with no allocation proportional to input length. This describes the algorithm, not a measured throughput result. Counting high bytes is not UTF-8 validation, and the summary is not a storage checksum.

## Widen the operands, not just the destination

A byte can hold 255, but the sum of two bytes can exceed 255. Our second function returns the exact mathematical sum as `u16`, an unsigned sixteen-bit integer. For byte inputs $a$ and $b$:

$$
0 \le a,b \le 255,
\qquad
0 \le a+b \le 510 < 2^{16}.
$$

Thus converting both inputs to `u16` **before** adding makes every possible result representable. `u16::from(value)` is an associated conversion function: the type before `::` names the destination. The standard library supplies this lossless conversion from `u8`. It uses the [`From` conversion interface](https://doc.rust-lang.org/std/convert/trait.From.html); we are calling an existing implementation, not defining a trait yet.

Append this complete function to `byte_summary.rs`:

```rust title="crates/language-lab/src/byte_summary.rs"
/// Returns the exact sum of two byte values, including a possible carry.
pub fn sum_pair(left: u8, right: u8) -> u16 {
    u16::from(left) + u16::from(right)
}
```

For inputs 255 and 1, the conversions produce two `u16` operands, 255 and 1; their addition produces 256. This adds **values**. It does not decode two adjacent bytes into a little-endian or big-endian integer.

A wider return type does not retroactively widen an earlier operation. In `u16::from(left + right)`, the addition still happens on the two `u8` operands first. The diagnostic experiment below will expose that difference. For general record lengths later in the book, widening alone is not a complete overflow strategy: the permitted input ranges and the destination width must still justify the arithmetic.

## Call the library from an executable

The example needs the library's two functions. A `use` declaration brings their paths into scope, allowing the short names in later calls. The braces in `use language_lab::byte_summary::{summarize, sum_pair};` group two imports from the same module. This does not paste source text into the example or create another copy of the module.

`fn main()` is this executable's entry point. The array type `[u8; 6]` means six `u8` elements, with the length known at compile time. `b'A'` is a single-byte literal whose value is 65; it is not the Unicode character literal `'A'`, which has type `char`.

A binding such as `let (length, zeros, high_bytes) = summarize(&fixture);` **destructures** the returned tuple, giving a name to each component. The `&` at the call site creates the shared reference accepted by the function. The array remains usable afterward.

Three macros appear below. The `!` distinguishes a macro invocation from an ordinary function call. `assert_eq!` checks two expressions for equality and reports their values when they differ. `assert!` requires a Boolean condition to be true. These checks remain active in release builds; they are not `debug_assert!` checks. `println!` writes formatted text followed by a newline; `{length}` substitutes the named value. See the standard [`assert_eq!`](https://doc.rust-lang.org/std/macro.assert_eq.html) and [`println!`](https://doc.rust-lang.org/std/macro.println.html) documentation.

Create the entire example file:

```rust title="crates/language-lab/examples/workspace_probe.rs"
use language_lab::byte_summary::{sum_pair, summarize};

fn main() {
    let fixture: [u8; 6] = [0, b'A', 127, 128, 255, 0];
    let (length, zeros, high_bytes) = summarize(&fixture);

    assert_eq!(length, 6);
    assert_eq!(zeros, 2);
    assert_eq!(high_bytes, 2);
    assert!(zeros + high_bytes <= length);
    assert_eq!(fixture[1], b'A');

    let carry = sum_pair(255, 1);
    let maximum = sum_pair(255, 255);
    assert_eq!(carry, 256);
    assert_eq!(maximum, 510);

    println!("bytes={length} zero={zeros} high={high_bytes}");
    println!("sum_pair(255, 1)={carry}");
    println!("sum_pair(255, 255)={maximum}");
    println!("workspace probe: ok");
}
```

The actual import orders `sum_pair` before `summarize`, which is the order rustfmt normally chooses. The order has no effect on the meaning of the paths.

The fixture lives in `main`'s scope. During `summarize`, the library reads through a temporary shared reference and returns only integer counts. It stores no reference to the fixture. By the later `fixture[1]` assertion, that function call has finished and the array is still present. There is no file, socket, background worker, or manually allocated buffer to clean up. The operating system still owns the process's standard output; printing is not evidence of a disk synchronization boundary.

The assertions intentionally precede the output. A wrong count should stop this diagnostic program rather than print the final success line. A **panic** signals a violated assertion and terminates this simple executable with a nonzero status; the test harness introduced next can instead record a test's panic as a failure. Returned errors for malformed external input will be introduced in Chapter 3. Do not use this demonstration's assertions as a substitute for fallible protocol parsing.

Run the executable from the workspace root:

```sh title="Terminal — build, link, and run the example"
cargo run -p language-lab --example workspace_probe
```

`-p` selects the package by its manifest name. `--example` selects the example target by the filename stem, without `.rs`. A plain `cargo run -p language-lab` is not equivalent: this package has no default binary target in `src/main.rs`.

Expected program output, excluding Cargo's build messages:

```text title="Expected standard output"
bytes=6 zero=2 high=2
sum_pair(255, 1)=256
sum_pair(255, 255)=510
workspace probe: ok
```

Do not compare compilation durations or temporary build paths with somebody else's output. The four program lines, assertion results, and successful exit are the meaningful checks here.

## Make boundaries repeatable with unit tests

The example checks one mixed fixture and two sums. Library tests make individual boundaries independently repeatable, without depending on the example's `main` function.

An **attribute** is metadata beginning with `#[...]`. `#[cfg(test)]` includes the following module when the library is compiled for tests, and leaves it out of an ordinary library build. `#[test]` identifies a function that the test runner should execute. Inside the nested `tests` module, `super` refers to its parent module, `byte_summary`; the grouped `use` imports the two functions being tested. Rust's [test organization](https://doc.rust-lang.org/book/ch11-03-test-organization.html#unit-tests) and [test-function introduction](https://doc.rust-lang.org/book/ch11-01-writing-tests.html#structuring-test-functions) explain these mechanisms.

The tests introduce one additional array notation: `[0; 4]` constructs four zero bytes. In contrast, `[u8; 4]` in a type annotation describes an array's element type and length. In `summarize(&[])`, the parameter type supplies enough context to infer an empty byte array.

Append this whole test module after `sum_pair` in `byte_summary.rs`. This is the third and final block for that file.

```rust title="crates/language-lab/src/byte_summary.rs"
#[cfg(test)]
mod tests {
    use super::{sum_pair, summarize};

    #[test]
    fn empty_input_is_zero() {
        assert_eq!(summarize(&[]), (0, 0, 0));
    }

    #[test]
    fn counts_mixed_fixture() {
        let fixture: [u8; 6] = [0, b'A', 127, 128, 255, 0];
        assert_eq!(summarize(&fixture), (6, 2, 2));
    }

    #[test]
    fn classifies_threshold_and_extremes() {
        assert_eq!(summarize(&[0]), (1, 1, 0));
        assert_eq!(summarize(&[127]), (1, 0, 0));
        assert_eq!(summarize(&[128]), (1, 0, 1));
        assert_eq!(summarize(&[255]), (1, 0, 1));
    }

    #[test]
    fn counts_repeated_values() {
        assert_eq!(summarize(&[0; 4]), (4, 4, 0));
        assert_eq!(summarize(&[255; 4]), (4, 0, 4));
    }

    #[test]
    fn visits_final_byte() {
        assert_eq!(summarize(&[65, 0]), (2, 1, 0));
        assert_eq!(summarize(&[65, 255]), (2, 0, 1));
    }

    #[test]
    fn widens_before_addition() {
        assert_eq!(sum_pair(0, 0), 0);
        assert_eq!(sum_pair(255, 0), 255);
        assert_eq!(sum_pair(0, 255), 255);
        assert_eq!(sum_pair(254, 1), 255);
        assert_eq!(sum_pair(255, 1), 256);
        assert_eq!(sum_pair(1, 255), 256);
        assert_eq!(sum_pair(255, 255), 510);
    }
}
```

The mixed fixture alone would not tell us which rule failed. The singleton 127 and 128 cases isolate the threshold. The empty case tests the loop entry condition. The two-element cases put the only counted byte at the very end. Repeated values ensure counters increase more than once. The addition tests cross the boundary in both operand orders rather than testing only sums below 256.

Run the tests:

```sh title="Terminal — execute the library tests"
cargo test -p language-lab
```

Expected: six unit tests pass, none fail. With these files there are no documentation code examples to test, so the documentation-test phase has zero tests. Test order, durations, and Cargo's executable paths can vary. The unit-test count refers to the six `#[test]` functions, not to the number of `assert_eq!` calls.

> **Note**
> With these default target settings, `cargo test` compiles the example to check that it still builds, but does not execute its `main`. A passing test run is not a substitute for `cargo run -p language-lab --example workspace_probe`. Cargo documents this distinction in its [test target selection](https://doc.rust-lang.org/cargo/commands/cargo-test.html#target-selection).

## Know what each development command establishes

Use the following sequence after the files are complete:

```sh title="Terminal — routine development checks"
cargo fmt --all
cargo fmt --all -- --check
cargo check -p language-lab --all-targets
cargo test -p language-lab
cargo run -p language-lab --example workspace_probe
```

`cargo fmt --all` edits Rust source formatting. The following `--check` form reports whether formatting changes are still needed, without applying them. The `--` separator passes the following option to rustfmt. Formatting does not validate behavior. If the formatter is missing from a rustup-managed installation, add its component with `rustup component add rustfmt`; another installation method may provide the component differently. See the [formatter command](https://doc.rust-lang.org/cargo/commands/cargo-fmt.html) and [rustup components](https://rust-lang.github.io/rustup/concepts/components.html).

`cargo check` performs compilation checks without the final code-generation work needed to produce an executable. `--all-targets` includes the example and test targets, rather than only the ordinary library. It neither links a runnable program nor executes assertions; some errors only appear during later code generation or linking. These limits are described in the [`cargo check` reference](https://doc.rust-lang.org/cargo/commands/cargo-check.html).

For this workspace, the distinctions are:

| Command | What to learn from success | What remains unchecked |
| --- | --- | --- |
| `cargo fmt --all -- --check` | The source satisfies the installed formatter. | Types, linking, and results. |
| `cargo check -p language-lab --all-targets` | The selected Rust targets pass compiler checking. | Final executable generation and execution. |
| `cargo test -p language-lab` | The six library tests run successfully. | The example's `main` is not run under these settings. |
| `cargo run -p language-lab --example workspace_probe` | The example links and its assertions execute. | Inputs and behaviors not covered by that example. |

A clean check is useful because it is fast feedback, not because it proves every property. The next experiments deliberately separate these outcomes.

## Learn three different diagnostics

Make only one temporary change at a time. Run the named command, understand the result, and restore the correct code before proceeding. Do not leave intentionally broken examples in the workspace or disable tests to make the output green.

### A path can name the wrong crate

Temporarily replace the import at the top of `workspace_probe.rs` with:

```rust title="Temporary incorrect import — in workspace_probe.rs"
use crate::byte_summary::{sum_pair, summarize};
```

Run:

```sh title="Terminal — check the example's paths"
cargo check -p language-lab --example workspace_probe
```

Expected: an unresolved-import diagnostic points at `crate::byte_summary`. Here `crate` is the example crate, which has no such module. The filesystem location of the library inside the same package does not change the meaning of `crate`.

Restore the import to `use language_lab::byte_summary::{sum_pair, summarize};` and run the same command again. Do not fix this by copying `byte_summary.rs` into `examples/` or by adding a second `mod byte_summary;` declaration there. That would create another module rather than use the library's public API.

A different diagnostic, “file not found for module,” points to the declaration in `src/lib.rs`: verify the exact filename `src/byte_summary.rs`. An “item is private” diagnostic instead means the module or function is missing the intended `pub`. These errors arise at different boundaries even when all three prevent a call from compiling.

### An incorrect expectation still type-checks

Inside `widens_before_addition`, temporarily change just this test assertion:

```rust title="Temporary incorrect expectation — inside widens_before_addition"
assert_eq!(sum_pair(255, 1), 255);
```

Run the compiler check, then the single selected test:

```sh title="Terminal — compile first, execute second"
cargo check -p language-lab --all-targets
cargo test -p language-lab byte_summary::tests::widens_before_addition -- --exact
```

The check is expected to succeed: the call and comparison have compatible types. The test is expected to fail, with the important values:

```text title="Expected assertion diagnostic — excerpt"
assertion `left == right` failed
  left: 256
 right: 255
```

“Left” means the assertion's first expression, our computed result; “right” means the second, our stated expectation. The arithmetic is correct in this experiment. Changing the library to return 255 would make the deliberately wrong test pass while breaking its contract. Restore the expected value to 256.

The filter before `--` selects a test name. `--exact` is passed to the test runner and requires the full name to match. This selected run should execute one unit test, with five filtered out. A report of zero matching tests is not a successful run of the intended test.

For a failing test with more calls to inspect, the same command can request a backtrace:

```sh title="Terminal — request a backtrace for a failure"
RUST_BACKTRACE=1 cargo test -p language-lab byte_summary::tests::widens_before_addition -- --exact
```

Read the first frame in your own source and the failed assertion before following library internals. Backtrace paths and formatting depend on the compiler and platform; do not expect a particular address or source-line number.

### A wide return type cannot rescue narrow arithmetic

With all test expectations restored, temporarily replace only the implementation of `sum_pair` with:

```rust title="Temporary incorrect function — replace sum_pair only"
pub fn sum_pair(left: u8, right: u8) -> u16 {
    u16::from(left + right)
}
```

Run:

```sh title="Terminal — exercise the arithmetic boundary"
cargo check -p language-lab --all-targets
cargo test -p language-lab byte_summary::tests::widens_before_addition -- --exact
cargo test -p language-lab --release byte_summary::tests::widens_before_addition -- --exact
```

The compiler check can accept this function: a runtime argument may be any `u8`, and some pairs fit. Under Cargo's ordinary development/test settings, the 255-plus-1 call triggers an overflow panic at the addition. Under the default release settings, overflow checking is disabled; the eight-bit addition wraps to zero, and the later widening produces zero as `u16`. The equality assertion still executes and rejects zero instead of 256. Cargo's [profile settings](https://doc.rust-lang.org/cargo/reference/profiles.html#overflow-checks) and Rust's [overflow rules](https://doc.rust-lang.org/reference/expressions/operator-expr.html#overflow) explain the distinction. Configuration overrides can enable overflow checks in release too; then that run also panics at the addition.

For the default unchecked case, the mistaken operation computes:

$$
(255+1) \bmod 256 = 0,
$$

where $\bmod 256$ means taking the remainder after division by 256. Converting that zero cannot reconstruct the lost carry. With 255 and 255, the narrow result would be 254 rather than 510. Neither value satisfies this function's exact-sum contract.

A source expression containing only overflowing constants may instead be rejected during compilation. That does not contradict the runtime experiment, whose addition operates on function parameters. Also, Rust's fixed-width arithmetic should not be read as C's integer-promotion rules, and it is not Python's arbitrary-precision integer arithmetic.

Restore `u16::from(left) + u16::from(right)` as the function body. Do not weaken the assertions, use saturating arithmetic, or intentionally wrap the sum. Those choices would implement different contracts. Rerun the normal suite and then `cargo test -p language-lab --release`; the correct function's results do not depend on the overflow-check setting.

## Diagnose the symptom at the correct layer

| Symptom | Likely cause | Focused correction |
| --- | --- | --- |
| `rustc` or `cargo` is not found | Installation is missing or its executable directory is not on `PATH`. | Inspect the installation and reopen the shell after following its setup instructions. |
| Edition 2024 or resolver 3 is rejected | The selected Cargo/toolchain is too old for these manifests. | Check `cargo --version` and the active toolchain; choose a compatible maintained installation. |
| A check passes but linking fails | The compiler check did not need a final native executable. | Inspect the linker error and Apple tool selection; do not rewrite correct byte-processing code. |
| No binary target is available for `cargo run` | The executable lives under `examples/`, not `src/main.rs`. | Use `--example workspace_probe`. |
| Missing module or unresolved import | A declaration, filename, or crate-root path disagrees. | Compare `lib.rs`, `byte_summary.rs`, and the library-name import separately. |
| Function returns `()` instead of a tuple | A semicolon discarded the final tuple. | Restore the tail expression or use an explicit `return`. |
| Empty input panics | The loop indexes before checking length, or subtracts one from an empty length. | Keep the loop guard as `index < bytes.len()`. |
| `[128]` has no high byte | The threshold is `> 128` rather than `>= 128`. | Include the boundary value, then rerun the singleton test. |
| Final byte is absent from the counts | The loop stops at `len - 1`. | Process indexes strictly below `len`, including `len - 1` when nonempty. |
| A sum panics in development or is wrong in release | Addition happened before widening. | Convert both inputs first; keep the 256 and 510 expectations. |

These are local experiments, not reasons to delete a build directory, reinstall every tool, or alter unrelated code. Start with the first meaningful diagnostic and the smallest command that reproduces it.

## Leave a working foundation for Chapter 2

Restore both production functions and every original expected value after the diagnostic experiments. The workspace should contain the two manifests, the library root, the completed `byte_summary.rs` with six tests, and the executable example. Cargo owns its generated lockfile and build output. There are no broker modules or hidden helper files.

As a small variation, predict the summary when the fixture's 127 becomes 128. Length stays six, zeros stay two, and high bytes increase to three. Verify the prediction by adding this assertion inside the existing `counts_mixed_fixture` test; it needs no new helper:

```rust title="Optional extra assertion — inside counts_mixed_fixture"
assert_eq!(summarize(&[0, b'A', 128, 128, 255, 0]), (6, 2, 3));
```

The number of test functions remains six. The original fixture and executable output need not change. This variation checks the meaning of one threshold rather than merely rerunning the same input.

The final runnable check is:

```sh title="Terminal — final Chapter 1 checks"
cargo fmt --all -- --check
cargo check -p language-lab --all-targets
cargo test -p language-lab
cargo test -p language-lab --release
cargo run -p language-lab --example workspace_probe
```

Expect six passing unit tests in both test profiles and the four program lines shown earlier. Do not record these as executed until they have actually run on the local machine.

A file-for-file reference copy of the required listings is available with the book: [workspace manifest](https://25d-228.github.io/rust-learning/media/chapter-01/Cargo.toml), [package manifest](https://25d-228.github.io/rust-learning/media/chapter-01/crates/language-lab/Cargo.toml), [library root](https://25d-228.github.io/rust-learning/media/chapter-01/crates/language-lab/src/lib.rs), [byte functions and tests](https://25d-228.github.io/rust-learning/media/chapter-01/crates/language-lab/src/byte_summary.rs), and [executable](https://25d-228.github.io/rust-learning/media/chapter-01/crates/language-lab/examples/workspace_probe.rs). These contain the correct program, not the temporary defects or the optional extra assertion. The complete listings above are sufficient to write it without downloading those files.

Chapter 2 starts from this workspace and keeps this example working. It adds an owned record and a borrowed view, making the ownership and lifetime relationships explicit before a parser, storage file, or asynchronous task depends on them. See the [course roadmap](https://25d-228.github.io/rust-learning/index.html#chapter-2-ownership-borrowing-and-lifetime-bound-views) for that next capability.
