//! Platform-specific file extent reservation.
//!
//! This module owns the physical allocation effects used by publication and
//! WAL admission. [`super::Device`] owns page I/O and logical file lifecycle;
//! callers use these helpers when they need a keep-size reservation before a
//! durability-critical write.

use std::fs::File;
use std::io;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsRawFd;

/// Reserve a file extent before a durability-critical write.
pub(crate) fn preallocate_file(file: &File, length: u64) -> io::Result<()> {
    let _ = reserve_file(file, length)?;
    // Keep the logical length contract explicit for callers that intentionally
    // want a visible preallocated extent. Keep-size WAL admission uses
    // `reserve_file` directly so the logical WAL length remains its true
    // append frontier.
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
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "preallocation offset overflows",
            )
        })?;
        let size = libc::off_t::try_from(length - current).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "preallocation length overflows",
            )
        })?;
        // SAFETY: the descriptor is borrowed from a live `File`, and the
        // checked offsets/lengths are valid `off_t` values.
        let result =
            unsafe { libc::fallocate(file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, offset, size) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(true)
    }

    #[cfg(target_os = "macos")]
    {
        let size = libc::off_t::try_from(length - current).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "preallocation length overflows",
            )
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
        let mut result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
        if result == -1 || (result == 0 && store.fst_bytesalloc > 0 && store.fst_bytesalloc < size)
        {
            store.fst_flags = libc::F_ALLOCATEALL;
            store.fst_bytesalloc = 0;
            // SAFETY: the same live descriptor and initialized `fstore_t` are
            // reused for the documented non-contiguous fallback.
            result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
        }
        if result == -1 {
            return Err(io::Error::last_os_error());
        }
        // F_PREALLOCATE can report success after allocating less than the
        // requested extent. Treat that as capacity failure instead of
        // claiming a reservation that the later write is not protected by.
        // Some APFS-backed descriptors report zero here even though the
        // reservation is visible in `st_blocks`; zero therefore means the
        // filesystem did not provide accounting, not that no bytes were
        // reserved. Reject only a positive, short report.
        if store.fst_bytesalloc > 0 && store.fst_bytesalloc < size {
            return Err(io::Error::from(io::ErrorKind::StorageFull));
        }
        Ok(true)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (file, length);
        Ok(false)
    }
}
