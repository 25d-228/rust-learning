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

impl<F: Read> Read for FaultFile<F> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> { self.inner.read(bytes) }
}

impl<F: Seek> Seek for FaultFile<F> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> { self.inner.seek(position) }
}

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

impl<F: StorageFile> StorageFile for FaultFile<F> {
    fn file_len(&mut self) -> io::Result<u64> { self.inner.file_len() }
    fn truncate(&mut self, length: u64) -> io::Result<()> { self.inner.truncate(length) }
    fn sync(&mut self) -> io::Result<()> {
        if self.fail_sync { return Err(io::Error::other("injected sync failure")); }
        self.inner.sync()
    }
}
