//! The durability sync primitive, class-selected.
//!
//! One place decides what a "sync" means. On Linux the two classes
//! coincide (fsync/fdatasync flush through to the device or its volatile
//! cache the same way); on macOS they are different operations with a
//! ~100x latency gap (measured on this host, 2026-09-06):
//!
//! - `F_FULLFSYNC` (Rust std's `sync_data`/`sync_all`): ~4.2 ms; a device
//!   barrier that survives power loss even on consumer SSDs.
//! - plain `fsync(2)`: ~0.03 ms; flushes kernel pages to the disk's
//!   volatile cache — safe against process and kernel crash, not power
//!   loss on devices that acknowledge flushes early.
//!
//! PostgreSQL's macOS builds use plain fsync / `open_datasync`; its
//! measured single-client commit latency on this host (0.267 ms) is only
//! consistent with the kernel-barrier class. OmenDB defaults to the
//! device barrier (correctness first) and lets a deployment opt into the
//! kernel-barrier class explicitly through `Options::sync_class`.

use std::fs::File;
use std::io;
#[cfg(target_os = "macos")]
use std::os::unix::io::AsRawFd;

use crate::db::SyncClass;

/// Sync the file's data (and enough metadata to read it back) under the
/// selected durability class.
pub(crate) fn sync_file_data(file: &File, class: SyncClass) -> io::Result<()> {
    match class {
        SyncClass::DeviceBarrier => file.sync_data(),
        SyncClass::KernelBarrier => {
            #[cfg(target_os = "macos")]
            {
                // Rust std maps sync_data to F_FULLFSYNC on macOS; the
                // kernel-barrier class calls fsync(2) directly.
                let outcome = unsafe { libc::fsync(file.as_raw_fd()) };
                if outcome != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            }
            #[cfg(not(target_os = "macos"))]
            {
                file.sync_data()
            }
        }
    }
}

/// Sync the file's data and all its metadata under the selected class.
pub(crate) fn sync_file_all(file: &File, class: SyncClass) -> io::Result<()> {
    match class {
        SyncClass::DeviceBarrier => file.sync_all(),
        SyncClass::KernelBarrier => {
            #[cfg(target_os = "macos")]
            {
                let outcome = unsafe { libc::fsync(file.as_raw_fd()) };
                if outcome != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            }
            #[cfg(not(target_os = "macos"))]
            {
                file.sync_all()
            }
        }
    }
}
