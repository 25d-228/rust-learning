# Chapter 7, Part 2 — File I/O and directory ownership

[Part 1 — Record bytes and typed identities](https://25d-228.github.io/rust-learning/chapter-07.html) supplied the manifests, module roots, typed identifiers, identity/format errors, complete codec, golden tests, and `record_probe` example. Start from those exact files plus Chapters 1–6's `language-lab` workspace. This part completes `io.rs`, appends `DirectoryError` to the existing `error.rs`, and creates the directory guard and its diagnostic executable. It then adds the real-file tests and a two-process harness.

**Implementation time:** part of Chapter 7's single **3.5-hour** estimate, not an additional allocation. The chapter ends only after the checks in this part. There is still no segment manager or recovery implementation.

> **Learning objectives**
> Preserve actual progress through short writes and errors; distinguish writes, flushes, and synchronization; keep a file cursor explicit; inject named failures through a narrow trait; and attach cooperative directory ownership to a live, non-replaced file handle.

Append repeated filename listings in their displayed order. In particular, keep the imports and error definitions already created in Part 1. Complete matching files remain under `book/media/chapter-07/`.

## Give storage algorithms an I/O seam, not a database

Create `io.rs`. The trait uses **supertraits**: implementing `StorageFile` also requires the byte-reading, byte-writing, and cursor operations named after the colon. This is not object inheritance or a hidden implementation. Each concrete type still provides those behaviors, while generic storage code can ask for one combined capability.

```rust title="crates/broker-core/src/io.rs"
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub trait StorageFile: Read + Write + Seek {
    fn file_len(&mut self) -> io::Result<u64>;
    fn truncate(&mut self, length: u64) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;
}

impl StorageFile for File {
    fn file_len(&mut self) -> io::Result<u64> { Ok(self.metadata()?.len()) }
    fn truncate(&mut self, length: u64) -> io::Result<()> { self.set_len(length) }
    fn sync(&mut self) -> io::Result<()> { self.sync_all() }
}

pub fn create_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).create_new(true).open(path)
}

pub fn open_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
}
```

The real implementation is a `std::fs::File`. `create_file` atomically refuses an existing path; `open_file` refuses a missing path and does not truncate anything. Keeping creation and reopening separate prevents a recovery path from quietly replacing missing authoritative data with an empty file. These are the [`OpenOptions` contracts](https://doc.rust-lang.org/std/fs/struct.OpenOptions.html#method.create_new), not an existence check followed by a race-prone create.

`sync` means the selected `sync_all` boundary, not a buffer flush or a successful write. `truncate` changes file length but does **not** reset the cursor. Future repair must explicitly seek to the repaired end before appending. These details are specified by [`sync_all`](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all) and [`set_len`](https://doc.rust-lang.org/std/fs/struct.File.html#method.set_len).

The trait requires `&mut self` for its additional operations even though some `File` methods accept `&self`. That matches the future partition owner's serial access discipline. It does not prove exclusive ownership of a filesystem object: another open handle can still exist. The directory guard and the process architecture supply that cooperative rule. `Send` is not a supertrait here because a synchronous codec test need not cross threads; Chapter 9 will add the required thread-transfer bounds at its ownership boundary.

This seam intentionally does not yet wrap directory rename, directory synchronization, or metadata publication. Those operations need their own explicit protocol in Chapter 10. Nor is a `Cursor<Vec<u8>>` automatically a durability model: an in-memory byte buffer has no real synchronization operation. Tests below wrap actual files and inject only the named failures.

## Loop over short I/O without inventing rollback

Append the write loop:

```rust title="crates/broker-core/src/io.rs"
pub fn write_fully<F: StorageFile + ?Sized>(file: &mut F, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        match file.write(bytes) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "write made no progress")),
            Ok(n) => bytes = &bytes[n..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
```

A [`Write::write`](https://doc.rust-lang.org/std/io/trait.Write.html#tymethod.write) call may accept only a prefix. A positive count advances the remaining slice; `Interrupted` retries the same remaining slice; zero progress on nonempty input becomes `WriteZero`. An ordinary error returns immediately. The standard `write_all` implements the same broad loop policy, but writing this small loop makes the state transition visible.

`F: StorageFile + ?Sized` permits a concrete file or a dynamically sized trait object behind a reference, the same bound used in Chapter 4. The loop relies on implementors honoring the `Write` contract that a successful count cannot exceed the supplied slice length. Rust's type system does not prove that semantic rule for every custom implementation.

Let $N$ be the original number of bytes and $k$ the count successfully accepted so far. The remaining slice is the original range `k..N`. Each positive result $n$ changes the state to $k+n$ and retries only the unaccepted suffix. For eight bytes and a maximum three-byte write, the successful calls accept `abc`, `def`, and `gh`; the remaining lengths are 8, 5, 2, and 0.

If the file instead fails after five bytes, the sequence is `abc`, `de`, then error. The file contains `abcde` and its cursor is 5. Returning `Err` does **not** undo the first two writes. Replaying the whole record at the current cursor would append another prefix; continuing blindly would leave an ambiguous log. Later partition owners stop mutating after an uncertain write or synchronization error and reopen through recovery. This helper does not implement that owner state machine.

Append the read loop:

```rust title="crates/broker-core/src/io.rs"
pub fn read_prefix<F: StorageFile + ?Sized>(file: &mut F, bytes: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < bytes.len() {
        match file.read(&mut bytes[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}
```

`read_prefix` fills as much of the supplied buffer as possible, stopping at the current end-of-file and returning the byte count. Short positive reads keep filling; an interruption retries. Only `bytes[..filled]` is newly read input. The unused suffix may retain old contents and must not be parsed as if it came from the file. If an error follows earlier successful reads, the helper returns the error without undoing buffer or cursor changes.

The [`Read` contract](https://doc.rust-lang.org/std/io/trait.Read.html#method.read) allows a file read to return zero at its current end even if a future append makes more bytes available. Our future partition owner serializes reads and appends; this function alone does not establish a permanent end-of-stream or a stable snapshot. [`Seek`](https://doc.rust-lang.org/std/io/trait.Seek.html) changes the byte cursor, which is unrelated to the logical `Offset` type.

## Inject partial progress through the same interface

Append the fault adapter's state and controls:

```rust title="crates/broker-core/src/io.rs"
pub struct FaultFile<F> {
    inner: F,
    max_write: usize,
    write_allowance: Option<usize>,
    interrupt_next: bool,
    zero_next: bool,
    fail_sync: bool,
}

impl<F> FaultFile<F> {
    pub fn new(inner: F, max_write: usize) -> io::Result<Self> {
        if max_write == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "max_write must be positive"));
        }
        Ok(Self {
            inner, max_write, write_allowance: None,
            interrupt_next: false, zero_next: false, fail_sync: false,
        })
    }

    pub fn fail_writes_after(&mut self, bytes: usize) { self.write_allowance = Some(bytes); }
    pub fn interrupt_next_write(&mut self) { self.interrupt_next = true; }
    pub fn zero_next_write(&mut self) { self.zero_next = true; }
    pub fn fail_syncs(&mut self) { self.fail_sync = true; }
    pub fn into_inner(self) -> F { self.inner }
}
```

`FaultFile<F>` **owns** its inner file; wrapping moves the file into it. `into_inner(self)` later consumes the adapter and returns that owner without cloning a handle. In normal tests, dropping the adapter also drops the file.

`max_write` limits bytes accepted by one call, not total disk usage. `fail_writes_after(n)` permits at most `n` additional successfully written bytes from the moment it is armed, then makes nonempty writes fail. It is not a simulated power-loss boundary. The single-use interrupt and zero-progress flags expose separate loop branches. `fail_syncs` makes subsequent synchronization attempts fail before calling the inner synchronization operation; a real failed sync can have more ambiguous effects.

Append forwarding for ordinary reads and cursor movements:

```rust title="crates/broker-core/src/io.rs"
impl<F: Read> Read for FaultFile<F> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> { self.inner.read(bytes) }
}

impl<F: Seek> Seek for FaultFile<F> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> { self.inner.seek(position) }
}
```

Each implementation declares only the bound it uses: reading needs `F: Read`, seeking needs `F: Seek`. This lets the wrapper compose with the underlying capabilities rather than requiring unrelated operations at every method. It also makes the deliberately limited fault surface clear: this version forwards read and seek errors but does not synthesize them.

Append the write implementation:

```rust title="crates/broker-core/src/io.rs"
impl<F: Write> Write for FaultFile<F> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() { return Ok(0); }
        if std::mem::take(&mut self.interrupt_next) {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        if std::mem::take(&mut self.zero_next) { return Ok(0); }
        let mut limit = self.max_write.min(bytes.len());
        if let Some(remaining) = self.write_allowance {
            if remaining == 0 { return Err(io::Error::other("injected write failure")); }
            limit = limit.min(remaining);
        }
        let n = self.inner.write(&bytes[..limit])?;
        if let Some(remaining) = &mut self.write_allowance { *remaining -= n; }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> { self.inner.flush() }
}
```

[`std::mem::take`](https://doc.rust-lang.org/std/mem/fn.take.html) replaces a field with its type's default and returns the previous value. For a Boolean that means “read the armed flag and reset it to false.” Thus an interrupted write is retried once without an infinite stream of injected interruptions. Empty writes return zero without consuming any armed fault.

After those flags, the permitted call size is the minimum of the input length, `max_write`, and any remaining allowance. The inner file may still write less than that size. Only the actual successful count is subtracted from the allowance. On an inner error, `?` returns without subtracting a speculative count. The slice bound also makes that subtraction safe for a conforming `Write` implementation.

`flush` is forwarded, but it never bypasses the explicit sync failure. This adapter's sync behavior completes the file seam:

```rust title="crates/broker-core/src/io.rs"
impl<F: StorageFile> StorageFile for FaultFile<F> {
    fn file_len(&mut self) -> io::Result<u64> { self.inner.file_len() }
    fn truncate(&mut self, length: u64) -> io::Result<()> { self.inner.truncate(length) }
    fn sync(&mut self) -> io::Result<()> {
        if self.fail_sync { return Err(io::Error::other("injected sync failure")); }
        self.inner.sync()
    }
}
```

A test can now wrap the same real-file operations used by the eventual log. It can observe five persisted-in-the-running-filesystem bytes after a failed eight-byte write, or a fully written buffer followed by a reported sync failure. Neither observation proves survival of physical power loss. No producer response is emitted by this layer, and a completed codec decode says nothing about whether a synchronization boundary occurred.

## Hold directory ownership in a resource, not a filename

Before opening mutable storage, the eventual broker must become the sole cooperating owner of its data directory. A test such as “does `.broker.lock` exist?” cannot distinguish a live owner from a file left by a crashed process. Deleting an allegedly stale lock filename is also wrong: another process may still hold a lock on the original file while a new process locks a replacement file at the same path.

Append the remaining error type to `error.rs`:

```rust title="crates/broker-core/src/error.rs"
#[derive(Debug)]
pub enum DirectoryError {
    Busy(PathBuf),
    Io { operation: &'static str, path: PathBuf, source: io::Error },
}

impl fmt::Display for DirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy(path) => write!(f, "directory already owned: {}", path.display()),
            Self::Io { operation, path, .. } => {
                write!(f, "{operation}: {}", path.display())
            }
        }
    }
}

impl Error for DirectoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Busy(_) => None,
        }
    }
}
```

`Busy` is expected contention and remains distinct from permissions, unsupported locking, a nonexistent root, or another I/O failure. For those errors, the operation and path provide context while `source()` retains the original system error. The caller must not convert every failed lock attempt into “another broker is running.”

Create the guard:

```rust title="crates/broker-core/src/storage/directory.rs"
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use crate::error::DirectoryError;

pub struct DirectoryGuard {
    root: PathBuf,
    _lock: File,
}

impl DirectoryGuard {
    pub fn acquire(path: &Path) -> Result<Self, DirectoryError> {
        let root = path.canonicalize().map_err(|source| DirectoryError::Io {
            operation: "resolve data directory", path: path.to_path_buf(), source,
        })?;
        let lock_path = root.join(".broker.lock");
        let file = OpenOptions::new().read(true).write(true)
            .create(true).truncate(false).open(&lock_path)
            .map_err(|source| DirectoryError::Io {
                operation: "open directory lock", path: lock_path.clone(), source,
            })?;
        match file.try_lock() {
            Ok(()) => Ok(Self { root, _lock: file }),
            Err(TryLockError::WouldBlock) => Err(DirectoryError::Busy(root)),
            Err(TryLockError::Error(source)) => Err(DirectoryError::Io {
                operation: "lock data directory", path: lock_path, source,
            }),
        }
    }

    pub fn root(&self) -> &Path { &self.root }
}
```

The caller supplies an **existing** directory. Canonicalization resolves the path for this acquisition; it does not initialize broker metadata, make an untrusted path safe, or defend against concurrent namespace replacement. Opening a child lock path will fail when the resolved root is not a usable directory. Chapter 10 will distinguish a new directory from a partially initialized broker; the presence of `.broker.lock` is never sufficient evidence of a healthy initialization.

The lock file is opened for reading and writing, created only when absent, and explicitly not truncated when present. `try_lock` distinguishes the standard library's [`WouldBlock` and I/O error variants](https://doc.rust-lang.org/std/fs/enum.TryLockError.html). On success, the file is moved into `_lock`. The underscore suppresses an unused-field warning; it does **not** discard the resource. Writing `let _ = acquired_guard;` and dropping the owner would be a different lifetime.

No custom `Drop` is necessary. Closing the final lock handle releases the lock; the filename remains. The guard does not expose or duplicate its handle. It must outlive every mutable partition owner in the eventual broker. The `root()` accessor borrows only the path and does not transfer lock ownership. Returning a path does not tie subsequently opened files to the guard at the type level, so the future supervisor must preserve the lifetime explicitly.

This is cooperative exclusion on the native filesystem, not a security boundary. Every broker process must use the same stable lock file. Do not remove, rename, replace, or truncate that file during service; do not move or replace the root directory. Use a local trusted data directory for the course, not a network-mounted share with unverified locking semantics. A noncooperating program can still alter files. These limits are separate from Rust memory safety.

## Observe acquisition with a small executable

Create the diagnostic binary. It takes a directory plus `try` or `hold`. The latter reports readiness only after locking and then waits for a line or end-of-input on standard input.

```rust title="crates/broker-core/src/bin/directory_probe.rs"
use std::error::Error;
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;

use broker_core::error::DirectoryError;
use broker_core::storage::directory::DirectoryGuard;

fn run() -> Result<ExitCode, Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 3 || (args[2] != "hold" && args[2] != "try") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput, "usage: directory-probe DIRECTORY hold|try",
        ).into());
    }
    let guard = match DirectoryGuard::acquire(Path::new(&args[1])) {
        Ok(guard) => guard,
        Err(DirectoryError::Busy(_)) => {
            println!("busy");
            return Ok(ExitCode::from(2));
        }
        Err(error) => return Err(error.into()),
    };
    println!("locked");
    io::stdout().flush()?;
    if args[2] == "hold" {
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
    }
    drop(guard);
    Ok(ExitCode::SUCCESS)
}
```

`args_os` keeps the directory path as an operating-system string rather than requiring UTF-8. `Path::new` borrows it without copying its contents. The two modes are fixed text; paths may still contain non-UTF-8 bytes on platforms that permit them. `ExitCode` makes the three outcomes explicit: 0 for acquisition, 2 for contention, and 1 for other failures.

The guard stays alive through the `hold` gate. Flushing **standard output** ensures the readiness message reaches a pipe; this has nothing to do with storage durability. The explicit `drop(guard)` documents the normal release point. An earlier `?` error also destroys the guard during return. Forced process termination does not run Rust destructors; the operating system closes the process's handles instead.

Complete the binary with error-chain reporting:

```rust title="crates/broker-core/src/bin/directory_probe.rs"
fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            let mut cause = error.source();
            while let Some(source) = cause {
                eprintln!("caused by: {source}");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}
```

Build it once, then use two terminals on the Mac:

```sh
cargo build -p broker-core --bin directory-probe
LOCK_DEMO=$(mktemp -d)
printf '%s\n' "$LOCK_DEMO"
target/debug/directory-probe "$LOCK_DEMO" hold
```

The first terminal prints `locked` and waits. Copy its exact temporary path into the second terminal:

```sh
LOCK_DEMO='/the/exact/temporary/path/printed/in/the/first/terminal'
target/debug/directory-probe "$LOCK_DEMO" try
printf 'status=%s\n' "$?"
```

While the first owner is alive, the expected output is `busy` and `status=2`. Press Return in the first terminal, then repeat the second terminal's probe: it should print `locked` with status 0. The `.broker.lock` file remains. Use only the disposable directory created for this experiment. Do not delete the lock file to make a failed check pass.

The automated harness below also tests termination, with timeouts and cleanup. It avoids leaving a background owner behind when an assertion fails.

## Keep destructive tests inside directories they own

Create the common integration-test helper. Cargo treats `tests/common/mod.rs` as a shared module rather than an independent test target; each test file imports it with `mod common`, following the [integration-test structure](https://doc.rust-lang.org/cargo/reference/cargo-targets.html#integration-tests).

```rust title="crates/broker-core/tests/common/mod.rs"
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct TestDir(PathBuf);

impl TestDir {
    pub fn new() -> io::Result<Self> {
        for _ in 0..100 {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("partition-broker-{}-{id}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "temporary directory names occupied"))
    }

    pub fn path(&self) -> &Path { &self.0 }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!("temporary directory cleanup {}: {error}", self.0.display());
        }
    }
}
```

The atomic counter generates candidate names within a test process; relaxed ordering is sufficient because it does not publish any other data. `create_dir`, not the counter, makes directory creation exclusive. A reused process ID or leftover directory yields `AlreadyExists` and another candidate, never deletion of somebody else's fixture. The retry loop is bounded.

`TestDir` is declared before the files and guards using it. Reverse local destruction order normally closes those resources before the directory cleanup runs. Tests also explicitly drop owners when they need to inspect closure behavior. The helper removes only the directory it successfully created and reports cleanup errors instead of disguising them as successful assertions. A killed test process can leave a fixture behind; no automatic stale-path deletion is used to conceal that fact.

## Check short writes, visible prefixes, and the cursor

Create `tests/io_boundary.rs` and its first two tests:

```rust title="crates/broker-core/tests/io_boundary.rs"
mod common;

use std::fs;
use std::io::{Seek, SeekFrom, Write};
use broker_core::io::{FaultFile, StorageFile, create_file, open_file, read_prefix, write_fully};
use common::TestDir;

#[test]
fn short_writes_and_interruption_preserve_the_exact_suffix() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("short.log");
    let mut file = FaultFile::new(create_file(&path).unwrap(), 3).unwrap();
    file.interrupt_next_write();
    write_fully(&mut file, b"abcdefgh").unwrap();
    assert_eq!(file.file_len().unwrap(), 8);
    file.sync().unwrap();
    drop(file);
    assert_eq!(fs::read(path).unwrap(), b"abcdefgh");
}

#[test]
fn write_error_keeps_a_prefix_instead_of_rolling_back() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("partial.log");
    let mut file = FaultFile::new(create_file(&path).unwrap(), 3).unwrap();
    file.fail_writes_after(5);
    assert!(write_fully(&mut file, b"abcdefgh").is_err());
    assert_eq!(file.stream_position().unwrap(), 5);
    drop(file);
    assert_eq!(fs::read(path).unwrap(), b"abcde");
}
```

Both tests use the actual filesystem. The first combines a one-shot interruption with a three-byte maximum write and checks the exact final bytes. The second allows only five further bytes and checks the cursor and surviving prefix after failure. It does not retry the append or pretend the prefix was rolled back.

Append zero-progress and synchronization-failure checks:

```rust title="crates/broker-core/tests/io_boundary.rs"
#[test]
fn zero_progress_is_an_error_not_an_infinite_loop() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("zero.log");
    let mut file = FaultFile::new(create_file(&path).unwrap(), 4).unwrap();
    file.zero_next_write();
    assert_eq!(write_fully(&mut file, b"x").unwrap_err().kind(), std::io::ErrorKind::WriteZero);
    assert_eq!(file.file_len().unwrap(), 0);
}

#[test]
fn completed_writes_do_not_hide_sync_failure() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("sync.log");
    let mut file = FaultFile::new(create_file(&path).unwrap(), 8).unwrap();
    write_fully(&mut file, b"written").unwrap();
    file.flush().unwrap();
    file.fail_syncs();
    assert!(file.sync().is_err());
    drop(file);
    assert_eq!(fs::read(path).unwrap(), b"written");
}
```

`WriteZero` must terminate the loop; repeatedly accepting `Ok(0)` would hang it. The synchronization test deliberately succeeds at both writing and flushing, then fails at the separate `sync` call. Reading those bytes again demonstrates visibility in the running filesystem, not durable acceptance by a broker or survival of power loss. In a later owner test, this same error must prevent a successful producer reply.

Finish the I/O checks:

```rust title="crates/broker-core/tests/io_boundary.rs"
#[test]
fn prefix_read_reports_partial_eof_then_clean_eof() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("read.log");
    fs::write(&path, b"cat").unwrap();
    let mut file = open_file(&path).unwrap();
    let mut bytes = [0; 5];
    assert_eq!(read_prefix(&mut file, &mut bytes).unwrap(), 3);
    assert_eq!(&bytes[..3], b"cat");
    assert_eq!(read_prefix(&mut file, &mut bytes).unwrap(), 0);
}

#[test]
fn truncation_does_not_reposition_the_cursor() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("truncate.log");
    let mut file = create_file(&path).unwrap();
    write_fully(&mut file, b"abcdef").unwrap();
    file.truncate(2).unwrap();
    assert_eq!(file.file_len().unwrap(), 2);
    assert_eq!(file.stream_position().unwrap(), 6);
    file.seek(SeekFrom::Start(2)).unwrap();
    write_fully(&mut file, b"Z").unwrap();
    file.sync().unwrap();
    drop(file);
    assert_eq!(fs::read(path).unwrap(), b"abZ");
}

#[test]
fn create_does_not_overwrite_and_open_does_not_create() {
    let dir = TestDir::new().unwrap();
    let path = dir.path().join("exclusive.log");
    assert!(open_file(&path).is_err());
    let mut file = create_file(&path).unwrap();
    write_fully(&mut file, b"keep").unwrap();
    drop(file);
    assert_eq!(create_file(&path).unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
    let file = open_file(&path).unwrap();
    assert!(FaultFile::new(file, 0).is_err());
    assert_eq!(fs::read(path).unwrap(), b"keep");
}
```

A read into five bytes returns 3 for `cat` and then 0 at the current end. The truncation test makes the hidden cursor issue concrete: after writing six bytes and shrinking to two, the cursor is still 6. It explicitly seeks to 2 before writing `Z`, yielding `abZ`. Forgetting that seek can create an unintended gap. Finally, reopening must not create a missing file, and exclusive creation must preserve an existing file's `keep` contents.

These tests do not use arbitrary user paths, require a VM, or measure storage performance. They are small state-and-byte checks. Short reads and injected read/seek/truncate failures can be added when their recovery branches are introduced; the current adapter's implemented fault controls are exactly the ones exercised here.

## Check handle lifetime locally, then cross a process boundary

Create the directory tests:

```rust title="crates/broker-core/tests/directory.rs"
mod common;

use std::fs;
use broker_core::error::DirectoryError;
use broker_core::storage::directory::DirectoryGuard;
use common::TestDir;

#[test]
fn a_live_guard_excludes_a_separately_opened_handle() {
    let dir = TestDir::new().unwrap();
    let guard = DirectoryGuard::acquire(dir.path()).unwrap();
    assert_eq!(guard.root(), dir.path().canonicalize().unwrap().as_path());
    assert!(matches!(DirectoryGuard::acquire(dir.path()), Err(DirectoryError::Busy(_))));
    drop(guard);
}

#[test]
fn leftover_filename_is_not_ownership_and_is_never_truncated() {
    let dir = TestDir::new().unwrap();
    let lock = dir.path().join(".broker.lock");
    fs::write(&lock, b"sentinel").unwrap();
    let guard = DirectoryGuard::acquire(dir.path()).unwrap();
    drop(guard);
    assert_eq!(fs::read(&lock).unwrap(), b"sentinel");
    let guard = DirectoryGuard::acquire(dir.path()).unwrap();
    drop(guard);
    assert!(lock.exists());
}

#[test]
fn missing_root_is_not_silently_initialized() {
    let dir = TestDir::new().unwrap();
    let absent = dir.path().join("absent");
    assert!(matches!(DirectoryGuard::acquire(&absent), Err(DirectoryError::Io { .. })));
    assert!(!absent.exists());
}
```

The first test separately opens the same lock file while its guard is alive. The second precreates a sentinel filename, acquires it, releases it, and reacquires it without altering its contents. Thus neither existence nor file contents determine ownership. The third makes sure a missing root is not silently promoted into an initialized broker.

Same-process checks are not a substitute for two-process exclusion. Create the following Python harness as `checks/check_directory.py` at the workspace root. It supplies process control only; the Rust binary owns the lock and all the implementation decisions. It uses Unix pipe readiness, which applies to the native macOS target; Python's [`select` documentation](https://docs.python.org/3/library/select.html#select.select) distinguishes that from Windows socket-only support.

```python title="checks/check_directory.py"
#!/usr/bin/env python3
"""Drive the Rust lock probe on macOS/Unix; no storage logic is implemented here."""
import argparse
import os
from pathlib import Path
import select
import subprocess
import tempfile
import time


def ready_line(process: subprocess.Popen, seconds: float = 5.0) -> bytes:
    deadline = time.monotonic() + seconds
    data = bytearray()
    while b"\n" not in data:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([process.stdout], [], [], remaining)[0]:
            raise TimeoutError("owner did not report readiness")
        chunk = os.read(process.stdout.fileno(), 256)
        if not chunk:
            raise RuntimeError("owner exited before reporting readiness")
        data.extend(chunk)
        if len(data) > 256:
            raise RuntimeError("unexpected readiness output")
    return bytes(data)


def contender(binary: Path, root: Path, expected_code: int, expected: bytes) -> None:
    result = subprocess.run([str(binary), str(root), "try"],
                            capture_output=True, timeout=5, check=False)
    assert result.returncode == expected_code, (result.returncode, result.stderr)
    assert result.stdout == expected, result.stdout
```

`ready_line` waits for the child's flushed readiness line rather than sleeping and hoping acquisition already occurred. It reads raw bytes so an incomplete line remains subject to the deadline. The 256-byte limit bounds unexpected readiness output. `contender` uses [`subprocess.run`](https://docs.python.org/3/library/subprocess.html#subprocess.run) with a timeout; the argument list avoids shell interpolation of the directory path.

Append the lifecycle exercise:

```python title="checks/check_directory.py"
def exercise(binary: Path, terminate: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="broker-lock-") as name:
        root = Path(name)
        lock = root / ".broker.lock"
        lock.write_bytes(b"sentinel")
        owner = subprocess.Popen([str(binary), str(root), "hold"],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, bufsize=0)
        try:
            assert ready_line(owner) == b"locked\n"
            contender(binary, root, 2, b"busy\n")
            if terminate:
                owner.kill()
            else:
                owner.stdin.close()  # EOF releases the gate, then Rust drops the guard.
            status = owner.wait(timeout=5)
            assert (status < 0) if terminate else (status == 0), status
            contender(binary, root, 0, b"locked\n")
            assert lock.read_bytes() == b"sentinel"
        finally:
            if owner.poll() is None:
                owner.kill()
            owner.wait(timeout=5)
            for stream in (owner.stdin, owner.stdout, owner.stderr):
                stream.close()
    print("kill: busy, exited, reacquired" if terminate else "normal: busy, exited, reacquired")
```

The first contender is launched only after the owner reports `locked`. For normal exit, closing the parent's input pipe delivers EOF to the owner's `read_line`; for termination, `kill` bypasses the Rust cleanup path. In both cases `wait` observes process exit before the reacquisition check. A negative exit status identifies signal termination on this Unix harness. The retained sentinel checks that acquisition did not truncate or replace the lock file.

The `finally` block kills and waits for any still-running owner and closes every pipe, including on assertion failures. The five-second deadlines are failure guards, not timing evidence for correct exclusion. Run this script without Python's `-O` option, which would disable its assertions. It is intended for a small local diagnostic, not as the broker's supervisor implementation.

Finish the script:

```python title="checks/check_directory.py"
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    exercise(binary, terminate=False)
    exercise(binary, terminate=True)


if __name__ == "__main__":
    main()
```

Build the Rust binary before running it:

```sh
cargo build -p broker-core --bin directory-probe
python3 checks/check_directory.py target/debug/directory-probe
```

Expected output:

```text
normal: busy, exited, reacquired
kill: busy, exited, reacquired
```

A successful termination check demonstrates process-exit release of this lock on the tested filesystem. It does not test a broker restart, segment recovery, filesystem integrity, or physical power loss. Those are different claims with different fixtures.

## Make a type confusion fail before it reaches the disk

A short compiler exercise reinforces the purpose of the newtypes. Temporarily create this example; it is deliberately **not** part of the reference overlay:

```rust title="crates/broker-core/examples/type_mix.rs (intentional compiler failure)"
use broker_core::types::{Epoch, Offset};

fn accepts_offset(offset: Offset) -> u64 { offset.get() }

fn main() {
    let epoch = Epoch::new(7).unwrap();
    println!("{}", accepts_offset(epoch));
}
```

Run `cargo check -p broker-core --example type_mix`. The expected diagnostic reports an `Epoch` where an `Offset` was required; exact diagnostic formatting depends on the compiler. Changing both types to aliases of `u64` would erase this protection.

Repair the meaning, not just the type checker. A restart epoch is not a record position, even when the two numbers happen to be equal. The complete repair supplies an actual offset:

```rust title="crates/broker-core/examples/type_mix.rs (repair)"
use broker_core::types::{Epoch, Offset};

fn accepts_offset(offset: Offset) -> u64 { offset.get() }

fn main() {
    let epoch = Epoch::new(7).unwrap();
    let next_offset = Offset::new(42);
    assert_eq!(accepts_offset(next_offset), 42);
    println!("epoch={}, next offset={}", epoch.get(), next_offset.get());
}
```

Run the repaired example, then remove the scratch `type_mix.rs` before the cumulative suite. Converting `epoch.get()` into `Offset::new(...)` merely to satisfy the signature would reintroduce the semantic bug through an explicit conversion. Types protect boundaries that the program actually preserves.

## Run focused checks and preserve the foundation suite

Once all named files are present, use these commands from the workspace root:

```sh
cargo check --workspace --all-targets
cargo test -p broker-core --test codec
cargo test -p broker-core --test io_boundary
cargo test -p broker-core --test directory
cargo run -p broker-core --example record_probe
cargo build -p broker-core --bin directory-probe
python3 checks/check_directory.py target/debug/directory-probe
cargo test --workspace --all-targets
cargo fmt --all
```

Expected required test counts are **17 codec tests, 7 I/O-boundary tests, and 3 directory tests**, all passing on a supported local filesystem. These 27 new tests bring the required supplied Rust suite to **102**, including the previous 75 foundation tests. The two-process harness is a separate command and is not counted as a Rust test. Ordinary example mains are also separate from test functions; the earlier six foundation mains remain available unchanged.

`cargo fmt` may reflow the compact listings; formatting does not establish correctness. Keep all earlier tests and examples rather than replacing `language-lab` with the new crate. A compiler or dependency-resolution failure must be diagnosed before interpreting any test count. No test output here is a substitute for executing the commands in the actual learner workspace.

The reference overlay contains the root manifest, the new crate, its three integration-test targets and common helper, the `record_probe` example, the diagnostic binary, and the Python harness. The [codec source](https://25d-228.github.io/rust-learning/media/chapter-07/crates/broker-core/src/codec.rs), [I/O source](https://25d-228.github.io/rust-learning/media/chapter-07/crates/broker-core/src/io.rs), [directory guard](https://25d-228.github.io/rust-learning/media/chapter-07/crates/broker-core/src/storage/directory.rs), [golden tests](https://25d-228.github.io/rust-learning/media/chapter-07/crates/broker-core/tests/codec.rs), and [process harness](https://25d-228.github.io/rust-learning/media/chapter-07/checks/check_directory.py) correspond exactly to the explained listings. There are no future storage modules or empty recovery tests to fill in by guesswork.

## Diagnose the first violated boundary

| Symptom | Likely cause | Focused check or correction |
| --- | --- | --- |
| `try_lock` or `TryLockError` is unavailable | Compiler predates the used standard API | Check `rustc --version`; use a compatible maintained toolchain supporting the 1.89 API boundary |
| A golden record differs before byte 28 | Wrong field range or byte order | Compare the two layout tables; inspect each integer's `to_be_bytes` output |
| Header CRC always fails | Header was sealed too early, or its own checksum bytes were included | Hash precisely `0..28` after populating format-specific fields |
| A changed length causes “incomplete body” instead of an error | Length was trusted before header integrity and size checks | Call `decode_record_header` before using the length |
| `cat` is followed by bytes from the next record | Payload slice extends to the input end | Use `HEADER_LEN..header.total_len`, and advance by `consumed` only |
| A full damaged record is silently discarded | Incomplete and corrupt were merged | Preserve `Err(FormatError)`; do not classify it as a repairable tail |
| A short write duplicates a prefix | The full input was retried after positive progress | Retry only the suffix after the successful count |
| A write loop never finishes | `Ok(0)` on nonempty input was treated as progress | Return `WriteZero` |
| Append after truncation leaves a gap | File length changed but cursor did not | Seek explicitly to the repaired byte end |
| Two owners both report `locked` | Guard dropped too soon, paths differ, or lock file was replaced | Retain the guard, compare roots, and never unlink or replace the lock file |
| Lock contention is reported for a permissions error | All lock errors were collapsed into “busy” | Match `TryLockError::WouldBlock` separately and report the I/O source |
| Process harness times out before readiness | Binary was not built, child errored, or stdout was not flushed | Run the diagnostic directly; inspect its error chain rather than adding a sleep |

A focused variation is to arm `fail_writes_after` for every prefix of the 35-byte golden record, using a fresh file each time. Predict the exact file length and decoder outcome before running it. Prefixes shorter than 32 require a header; prefixes 32–34 require a 35-byte record. At an allowance of 35, the record write finishes successfully because the loop has no remaining bytes and makes no failing extra call. The next nonempty write would fail. This is a useful distinction between “fail the next call after a threshold” and “retroactively fail a completed write.” Do not relabel this variation a recovery implementation.

## Carry exact interfaces into the segment layer

This chapter leaves a checked, versioned codec; distinct identity types; an I/O trait implemented by real files and a deterministic fault adapter; and a live directory guard. The codec borrows validated payload bytes without allocating them. The encoder owns its returned record bytes. The fault adapter owns its underlying file. The directory guard owns the exclusion handle and leaves the lock filename in place after release.

The next chapter, **Chapter 8 — Segments, sparse indexing, and bounded reads**, will connect these pieces into segment creation, append positioning, rotation, index maintenance, and limited reads. It must compare segment identity with caller expectations, check offset continuity, and seek using byte positions rather than logical offsets. It must not treat a valid CRC as proof of synchronization, a filename as proof of ownership, or an I/O error as proof that no bytes changed.
