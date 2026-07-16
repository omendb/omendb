//! Device abstraction for file I/O.
//!
//! On Linux, supports O_DIRECT for bypassing the page cache.
//! On macOS and other platforms, falls back to buffered I/O.

use crate::btree::node::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
#[cfg(not(any(unix, windows)))]
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsRawFd;
#[cfg(any(test, feature = "fault-injection"))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Options for opening a device.
#[derive(Debug, Clone)]
pub struct DeviceOptions {
    /// Use O_DIRECT on Linux (bypass page cache).
    pub use_odirect: bool,
    /// Use fsync after writes.
    pub sync_writes: bool,
    /// Create the file if it doesn't exist.
    pub create: bool,
}

impl Default for DeviceOptions {
    fn default() -> Self {
        Self {
            use_odirect: true,
            sync_writes: false,
            create: true,
        }
    }
}

/// A device (file) for storing pages.
///
/// Handles page-aligned I/O and optionally uses O_DIRECT on Linux.
pub struct Device {
    /// The underlying file.
    file: File,
    /// Whether to use O_DIRECT.
    use_odirect: bool,
    /// Whether to sync after writes.
    sync_writes: bool,
    /// Test-only deterministic sync failure hook.
    #[cfg(any(test, feature = "fault-injection"))]
    fail_next_sync: AtomicBool,
    /// Test-only deterministic write failure hook.
    #[cfg(any(test, feature = "fault-injection"))]
    fail_next_write: AtomicBool,
    /// Test-only deterministic disk-full hook.
    #[cfg(any(test, feature = "fault-injection"))]
    fail_next_disk_full: AtomicBool,
    /// Test-only persistent capacity limit in bytes.
    #[cfg(any(test, feature = "fault-injection"))]
    capacity_limit: AtomicU64,
}

impl Device {
    /// Open a device file.
    pub fn open<P: AsRef<Path>>(path: P, options: &DeviceOptions) -> io::Result<Self> {
        Self::open_with_mode(path, options, true)
    }

    /// Open an existing device without write permissions.
    pub fn open_read_only<P: AsRef<Path>>(
        path: P,
        options: &DeviceOptions,
    ) -> io::Result<Self> {
        Self::open_with_mode(path, options, false)
    }

    fn open_with_mode<P: AsRef<Path>>(
        path: P,
        options: &DeviceOptions,
        writable: bool,
    ) -> io::Result<Self> {
        let mut open_options = OpenOptions::new();
        open_options.read(true).write(writable);

        if options.create && writable {
            open_options.create(true);
        }

        #[cfg(target_os = "linux")]
        if options.use_odirect {
            use std::os::unix::fs::OpenOptionsExt;
            open_options.custom_flags(libc::O_DIRECT);
        }

        let file = open_options.open(path)?;

        Ok(Self {
            file,
            use_odirect: options.use_odirect,
            sync_writes: options.sync_writes,
            #[cfg(any(test, feature = "fault-injection"))]
            fail_next_sync: AtomicBool::new(false),
            #[cfg(any(test, feature = "fault-injection"))]
            fail_next_write: AtomicBool::new(false),
            #[cfg(any(test, feature = "fault-injection"))]
            fail_next_disk_full: AtomicBool::new(false),
            #[cfg(any(test, feature = "fault-injection"))]
            capacity_limit: AtomicU64::new(u64::MAX),
        })
    }

    /// Read a page at the given offset.
    ///
    /// The buffer must be page-aligned for O_DIRECT.
    pub fn read_page(&self, offset: u64, buf: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        #[cfg(unix)]
        {
            let mut filled = 0;
            while filled < buf.len() {
                let count = self.file.read_at(&mut buf[filled..], offset + filled as u64)?;
                if count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "page read reached end of file",
                    ));
                }
                filled += count;
            }
            Ok(())
        }

        #[cfg(windows)]
        {
            let mut filled = 0;
            while filled < buf.len() {
                let count = self
                    .file
                    .seek_read(&mut buf[filled..], offset + filled as u64)?;
                if count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "page read reached end of file",
                    ));
                }
                filled += count;
            }
            Ok(())
        }

        #[cfg(not(any(unix, windows)))]
        {
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(buf)
        }
    }

    /// Write a page at the given offset.
    ///
    /// The buffer must be page-aligned for O_DIRECT.
    pub fn write_page(&mut self, offset: u64, buf: &[u8; PAGE_SIZE]) -> io::Result<()> {
        self.check_write_capacity(offset)?;
        #[cfg(any(test, feature = "fault-injection"))]
        if self.fail_next_disk_full.swap(false, Ordering::AcqRel) {
            return Err(io::Error::from(io::ErrorKind::StorageFull));
        }
        #[cfg(any(test, feature = "fault-injection"))]
        if self.fail_next_write.swap(false, Ordering::AcqRel) {
            return Err(io::Error::other("injected device write failure"));
        }
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;

        if self.sync_writes {
            self.file.sync_data()?;
        }

        Ok(())
    }

    /// Check whether a page can be written at the requested offset.
    ///
    /// Production filesystems perform the final admission check during the
    /// write itself. The deterministic capacity hook is checked separately so
    /// StorageEngine can preflight an entire generation before issuing its
    /// first page write.
    pub fn check_write_capacity(&self, _offset: u64) -> io::Result<()> {
        let end = _offset
            .checked_add(PAGE_SIZE as u64)
            .ok_or_else(|| io::Error::from(io::ErrorKind::StorageFull))?;
        self.check_capacity(end)?;
        #[cfg(any(test, feature = "fault-injection"))]
        if self.fail_next_disk_full.load(Ordering::Acquire) {
            return Err(io::Error::from(io::ErrorKind::StorageFull));
        }
        Ok(())
    }

    /// Check a deterministic absolute artifact end offset without consuming
    /// the one-shot disk-full fault used by actual page writes.
    pub fn check_capacity(&self, end: u64) -> io::Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        if end > self.capacity_limit.load(Ordering::Acquire)
        {
            return Err(io::Error::from(io::ErrorKind::StorageFull));
        }
        #[cfg(not(any(test, feature = "fault-injection")))]
        let _ = end;
        Ok(())
    }

    /// Sync all data to disk.
    pub fn sync(&self) -> io::Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        if self.fail_next_sync.swap(false, Ordering::AcqRel) {
            return Err(io::Error::other("injected device sync failure"));
        }
        self.file.sync_data()
    }

    /// Inject one deterministic sync failure for recovery tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_sync_failure(&self) {
        self.fail_next_sync.store(true, Ordering::Release);
    }

    /// Inject one deterministic page-write failure for recovery tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_write_failure(&self) {
        self.fail_next_write.store(true, Ordering::Release);
    }

    /// Inject one deterministic disk-full result for recovery tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_disk_full(&self) {
        self.fail_next_disk_full.store(true, Ordering::Release);
    }

    /// Set a persistent capacity limit for recovery tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_capacity_limit(&self, capacity: u64) {
        self.capacity_limit.store(capacity, Ordering::Release);
    }

    /// Get the file size.
    pub fn size(&self) -> io::Result<u64> {
        self.file.metadata().map(|m| m.len())
    }

    /// Physically reserve space through `length` where the host exposes a
    /// filesystem preallocation primitive, then make that extent visible as
    /// the file's logical length.
    ///
    /// Linux uses `fallocate` and macOS uses `F_PREALLOCATE` with a contiguous
    /// allocation attempt followed by the filesystem-wide allocation mode.
    /// Other platforms retain the portable `set_len` behavior, which grows a
    /// logical file but does not promise physical block reservation.
    pub fn preallocate(&self, length: u64) -> io::Result<()> {
        preallocate_file(&self.file, length)
    }

    /// Reserve physical blocks through `length` without changing the logical
    /// file length where the host supports a keep-size primitive.
    ///
    /// Returns `false` on platforms where only portable logical growth is
    /// available. The caller can still perform its normal write and handle a
    /// final filesystem `StorageFull` result there.
    pub fn reserve(&self, length: u64) -> io::Result<bool> {
        reserve_file(&self.file, length)
    }

    /// Truncate the page file to a durable, page-aligned length.
    pub fn truncate(&mut self, length: u64) -> io::Result<()> {
        if !length.is_multiple_of(PAGE_SIZE as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device length must be page aligned",
            ));
        }
        let current = self.file.metadata()?.len();
        if length > current {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device truncation cannot grow the file",
            ));
        }
        self.file.set_len(length)?;
        self.sync()
    }

    /// Whether O_DIRECT is being used.
    pub fn uses_odirect(&self) -> bool {
        self.use_odirect
    }
}

/// Reserve a file extent before a durability-critical write.
pub(crate) fn preallocate_file(file: &File, length: u64) -> io::Result<()> {
    let _ = reserve_file(file, length)?;
    // Keep the logical length contract explicit for the WAL reservation and
    // for callers that intentionally want a visible preallocated extent.
    file.set_len(length)
}

/// Reserve physical blocks through `length` while preserving the current
/// logical file length.
pub(crate) fn reserve_file(file: &File, length: u64) -> io::Result<bool> {
    let current = file.metadata()?.len();
    if current >= length {
        return Ok(true);
    }

    #[cfg(target_os = "linux")]
    {
        let offset = libc::off_t::try_from(current).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "preallocation offset overflows")
        })?;
        let size = libc::off_t::try_from(length - current).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "preallocation length overflows")
        })?;
        // SAFETY: the descriptor is borrowed from a live `File`, and the
        // checked offsets/lengths are valid `off_t` values.
        let result = unsafe {
            libc::fallocate(
                file.as_raw_fd(),
                libc::FALLOC_FL_KEEP_SIZE,
                offset,
                size,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(true)
    }

    #[cfg(target_os = "macos")]
    {
        let size = libc::off_t::try_from(length - current).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "preallocation length overflows")
        })?;
        let mut store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATECONTIG,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: size,
            fst_bytesalloc: 0,
        };
        // SAFETY: the descriptor is borrowed from a live `File` and `store`
        // is the platform-defined `fstore_t` passed to `F_PREALLOCATE`.
        let mut result = unsafe {
            libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store)
        };
        if result == -1 {
            store.fst_flags = libc::F_ALLOCATEALL;
            store.fst_bytesalloc = 0;
            // SAFETY: the same live descriptor and initialized `fstore_t` are
            // reused for the documented non-contiguous fallback.
            result = unsafe {
                libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store)
            };
        }
        if result == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(true)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (file, length);
        Ok(false)
    }
}

/// Allocate a page-aligned buffer for O_DIRECT I/O.
///
/// Returns a buffer of the given size, aligned to PAGE_SIZE.
#[cfg(target_os = "linux")]
#[allow(dead_code)]
pub fn alloc_aligned_buffer(size: usize) -> Vec<u8> {
    use std::alloc::{Layout, alloc_zeroed};

    let layout = Layout::from_size_align(size, PAGE_SIZE).expect("invalid layout");
    let ptr = unsafe { alloc_zeroed(layout) };
    if ptr.is_null() {
        panic!("failed to allocate aligned buffer");
    }
    unsafe { Vec::from_raw_parts(ptr, size, size) }
}

/// Allocate a page-aligned buffer (non-Linux fallback).
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
pub fn alloc_aligned_buffer(size: usize) -> Vec<u8> {
    // On non-Linux, we don't need strict alignment, but provide it for consistency.
    vec![0u8; size]
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    use std::os::unix::fs::MetadataExt;
    use tempfile::tempdir;

    #[test]
    fn test_device_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let options = DeviceOptions::default();

        let device = Device::open(&path, &options);
        assert!(device.is_ok());
    }

    #[test]
    fn test_device_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let options = DeviceOptions {
            use_odirect: false, // disable O_DIRECT for testing
            sync_writes: false,
            create: true,
        };

        let mut device = Device::open(&path, &options).unwrap();

        let write_buf = [42u8; PAGE_SIZE];
        device.write_page(0, &write_buf).unwrap();

        let mut read_buf = [0u8; PAGE_SIZE];
        device.read_page(0, &mut read_buf).unwrap();

        assert_eq!(read_buf, write_buf);
    }

    #[test]
    fn test_device_multiple_pages() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let options = DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        };

        let mut device = Device::open(&path, &options).unwrap();

        // Write multiple pages.
        for i in 0..10 {
            let buf = [i as u8; PAGE_SIZE];
            device
                .write_page(i as u64 * PAGE_SIZE as u64, &buf)
                .unwrap();
        }

        // Read them back.
        for i in 0..10 {
            let mut buf = [0u8; PAGE_SIZE];
            device
                .read_page(i as u64 * PAGE_SIZE as u64, &mut buf)
                .unwrap();
            assert_eq!(buf, [i as u8; PAGE_SIZE]);
        }
    }

    #[test]
    fn test_device_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let options = DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        };

        let mut device = Device::open(&path, &options).unwrap();
        assert_eq!(device.size().unwrap(), 0);

        let buf = [0u8; PAGE_SIZE];
        device.write_page(0, &buf).unwrap();
        assert_eq!(device.size().unwrap(), PAGE_SIZE as u64);
    }

    #[test]
    fn test_device_preallocate_is_idempotent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let options = DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        };

        let device = Device::open(&path, &options).unwrap();
        let length = (PAGE_SIZE * 2) as u64;
        device.preallocate(length).unwrap();
        device.preallocate(length).unwrap();
        assert_eq!(device.size().unwrap(), length);
    }

    #[test]
    fn test_device_reserve_keeps_logical_length() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let options = DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        };

        let device = Device::open(&path, &options).unwrap();
        let length = (PAGE_SIZE * 2) as u64;
        let physically_reserved = device.reserve(length).unwrap();
        assert_eq!(device.size().unwrap(), 0);
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            assert!(physically_reserved);
            assert!(std::fs::metadata(&path).unwrap().blocks() > 0);
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        assert!(!physically_reserved);
    }

    #[test]
    fn test_device_truncate_is_page_aligned_and_durable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let options = DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        };
        let mut device = Device::open(&path, &options).unwrap();
        let page = [0xA5u8; PAGE_SIZE];
        device.write_page(0, &page).unwrap();
        device.write_page(PAGE_SIZE as u64, &page).unwrap();

        device.truncate(PAGE_SIZE as u64).unwrap();
        assert_eq!(device.size().unwrap(), PAGE_SIZE as u64);
        assert!(device.truncate(1).is_err());
        assert!(device.truncate((PAGE_SIZE * 2) as u64).is_err());
    }

    #[test]
    fn test_aligned_buffer() {
        let buf = alloc_aligned_buffer(PAGE_SIZE);
        assert_eq!(buf.len(), PAGE_SIZE);
        // On Linux with O_DIRECT, alignment is guaranteed.
        // On other platforms, we just check the size.
        #[cfg(target_os = "linux")]
        assert_eq!(buf.as_ptr() as usize % PAGE_SIZE, 0);
    }
}
