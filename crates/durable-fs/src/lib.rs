//! Class-selected durability primitives.
//!
//! One place decides what a "sync" means, what a directory flush means,
//! and what an atomic publication means — for every OmenDB-family engine
//! (seerdb, olap, vector). The crate is deliberately tiny: no fault
//! seams, no policy, no buffering. Consumers wrap it with their own
//! failure injection and group-commit policy.
//!
//! # Sync classes
//!
//! On Linux the two classes coincide (fsync/fdatasync flush through to
//! the device or its volatile cache the same way); on macOS they are
//! different operations with a ~100x latency gap (measured 2026-09-06 on
//! the development host):
//!
//! - `F_FULLFSYNC` (Rust std's `sync_data`/`sync_all`): ~4.2 ms; a device
//!   barrier that survives power loss even on consumer SSDs.
//! - plain `fsync(2)`: ~0.03 ms; flushes kernel pages to the disk's
//!   volatile cache — safe against process and kernel crash, not power
//!   loss on devices that acknowledge flushes early.
//!
//! PostgreSQL's macOS builds use plain fsync / `open_datasync`; its
//! measured single-client commit latency on the same host (0.267 ms) is
//! only consistent with the kernel-barrier class. Engines default to the
//! device barrier (correctness first) and let a deployment opt into the
//! kernel-barrier class explicitly.
//!
//! A note on `sync_data` and file size: `fdatasync(2)` explicitly flushes
//! size changes ("file size is metadata needed to access the data"), and
//! Rust's `sync_data` maps to `F_FULLFSYNC` (a full barrier) on macOS and
//! to `FlushFileBuffers` on Windows. There is no platform where a
//! successful `sync_data` leaves a length change undurable, so either
//! class-selected sync is correct for append-shaped WAL paths.
//!
//! # Directory durability
//!
//! Creating, renaming, or unlinking a file is only durable once the
//! containing directory entry is synced. [`fsync_dir`] does that for one
//! directory; [`fsync_dir_chain`] walks to the root for a `create_dir_all`
//! that may have created several ancestors. Directory syncs always use
//! the strongest barrier: they sit on create/publish paths (rare,
//! correctness-critical), never per-append.
//!
//! # Atomic publication
//!
//! [`atomic_write`] is the buffered shape: write a unique temporary sibling, sync
//! it, rename over the target, sync the parent directory. A crash leaves
//! either the old or the new content, never a partial mix. Streaming
//! publishers (large segments, compacted logs) compose the same steps
//! from [`sync_file_all`], [`std::fs::rename`], and [`fsync_dir`] instead.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(missing_docs)]

use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// What a "sync" means on this platform.
///
/// The default keeps the stronger barrier: a database engine should never
/// trade correctness, and a deployment that can accept kernel-crash-only
/// durability (battery-backed storage, containers on managed hosts, CI)
/// opts in explicitly and sees the ~100x sync-latency difference.
///
/// The two classes are also a durability *ladder* engines can expose
/// directly: a "strict / never lose an acknowledged write" safety tier is
/// `DeviceBarrier`-class syncs, and a "process-crash-safe with a
/// power-loss window" tier (the SQLite `synchronous=NORMAL`-in-WAL
/// analogy) is exactly what `KernelBarrier` buys on macOS. Consumers
/// should name their tier in terms of crash survival, not syscalls.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncClass {
    /// Device barrier (macOS `F_FULLFSYNC`): survives power loss even on
    /// consumer SSDs. The strongest available class. Default.
    #[default]
    DeviceBarrier,
    /// Kernel-page-cache barrier (plain `fsync(2)`): survives process
    /// and kernel crash; on power loss the disk cache may lose the last
    /// writes. PostgreSQL's installed macOS default class.
    KernelBarrier,
}

/// Sync the file's data (and the metadata needed to read it back, which
/// POSIX defines to include the file size) under the selected class.
///
/// The natural choice for append-shaped paths (WAL/group-commit barriers)
/// where the name and inode are already durable.
pub fn sync_file_data(file: &File, class: SyncClass) -> io::Result<()> {
    match class {
        SyncClass::DeviceBarrier => file.sync_data(),
        SyncClass::KernelBarrier => kernel_fsync(file),
    }
}

/// Sync the file's data and all its metadata under the selected class.
///
/// The natural choice for publication paths and for syncs that must
/// cover a `set_len` (recovery truncation, compaction rewrites).
pub fn sync_file_all(file: &File, class: SyncClass) -> io::Result<()> {
    match class {
        SyncClass::DeviceBarrier => file.sync_all(),
        SyncClass::KernelBarrier => {
            #[cfg(target_os = "macos")]
            {
                kernel_fsync(file)
            }
            #[cfg(not(target_os = "macos"))]
            {
                file.sync_all()
            }
        }
    }
}

/// On non-macOS platforms the two classes coincide, so the kernel-barrier
/// class is the std sync itself; on macOS it is a direct `fsync(2)` call,
/// because std maps both `sync_data` and `sync_all` to the much stronger
/// `F_FULLFSYNC`.
#[cfg(target_os = "macos")]
fn kernel_fsync(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let outcome = unsafe { libc::fsync(file.as_raw_fd()) };
    if outcome == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "macos"))]
fn kernel_fsync(file: &File) -> io::Result<()> {
    file.sync_data()
}

/// Make one directory's entry changes (create/rename/unlink of its
/// children) durable. Always the strongest barrier: directory syncs sit
/// on publication paths, never per-append.
///
/// Unix-shaped: on other platforms directory-entry durability uses
/// different mechanisms and this call is a no-op. Every current consumer
/// runs on macOS or Linux, including in CI.
pub fn fsync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    Ok(())
}

/// Make a newly created directory and each newly reachable ancestor
/// durable.
///
/// `create_dir_all` can create more than one ancestor. Syncing only the
/// immediate parent would leave an outer directory entry vulnerable to
/// being lost after an acknowledged create on filesystems that honor
/// directory durability separately from file durability.
pub fn fsync_dir_chain(path: &Path) -> io::Result<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let mut current = path;
    loop {
        fsync_dir(current)?;
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current {
            break;
        }
        if parent.as_os_str().is_empty() {
            fsync_dir(Path::new("."))?;
            break;
        }
        current = parent;
    }
    Ok(())
}

/// Atomically replace `path` with `data`: write a unique temporary sibling,
/// sync it, rename over the target, sync the parent directory.
///
/// A crash leaves either the old or the new content, never a partial
/// mix. Temporary files left by a crash are not reused. Failures before
/// rename leave the target unchanged and attempt to remove the temporary
/// file. A directory-sync error after rename may leave the new content
/// visible without confirming its durability.
///
/// For payloads too large to buffer (streaming segments, compacted
/// logs), compose [`sync_file_all`], [`std::fs::rename`], and
/// [`fsync_dir`] directly instead.
pub fn atomic_write(path: &Path, data: &[u8]) -> io::Result<()> {
    let (temporary, file) = create_temporary_sibling(path)?;
    let publish = || -> io::Result<()> {
        let mut file = file;
        file.write_all(data)?;
        file.flush()?;
        // Always the device barrier: this is a publication sync, not an
        // append-shaped one, and the class knob exists for the hot paths.
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)
    };
    if let Err(error) = publish() {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    fsync_dir(publication_parent(path))
}

fn publication_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn create_temporary_sibling(path: &Path) -> io::Result<(PathBuf, File)> {
    static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);
    let filename = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "publication requires a filename",
        )
    })?;
    for _ in 0..128 {
        let sequence = NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed);
        let mut temporary_name = filename.to_os_string();
        temporary_name.push(format!(".tmp.{}.{sequence}", std::process::id()));
        let temporary = publication_parent(path).join(temporary_name);
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique publication temporary file",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn sync_classes_do_not_error_on_real_files() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("data");
        fs::write(&path, b"payload").expect("write");
        let file = File::open(&path).expect("open");
        sync_file_data(&file, SyncClass::DeviceBarrier).expect("data device");
        sync_file_data(&file, SyncClass::KernelBarrier).expect("data kernel");
        sync_file_all(&file, SyncClass::DeviceBarrier).expect("all device");
        sync_file_all(&file, SyncClass::KernelBarrier).expect("all kernel");
    }

    #[test]
    fn fsync_dir_accepts_existing_directory() {
        let directory = tempfile::tempdir().expect("tempdir");
        fsync_dir(directory.path()).expect("fsync dir");
    }

    #[test]
    fn fsync_dir_chain_walks_created_ancestors() {
        let directory = tempfile::tempdir().expect("tempdir");
        let nested = directory.path().join("a/b/c");
        fs::create_dir_all(&nested).expect("create");
        fsync_dir_chain(&nested).expect("chain");
    }

    #[test]
    fn atomic_write_replaces_content_and_cleans_tmp() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("artifact");
        fs::write(&path, b"old").expect("old");
        atomic_write(&path, b"new content").expect("publish");
        assert_eq!(fs::read(&path).expect("read"), b"new content");
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn atomic_write_failure_leaves_old_content_and_no_tmp() {
        let directory = tempfile::tempdir().expect("tempdir");
        // Target inside a missing directory: the write fails, the old
        // content (none) stays, and no tmp litters the parent.
        let path = directory.path().join("missing/artifact");
        assert!(atomic_write(&path, b"new").is_err());
        assert!(!path.with_extension("tmp").exists());
    }

    #[test]
    fn atomic_write_tmp_destination_preserves_open_old_file() {
        use std::io::Read;
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("artifact.tmp");
        fs::write(&path, b"old").expect("old");
        let mut old_file = File::open(&path).expect("open old inode");
        atomic_write(&path, b"new content").expect("publish");
        let mut old_contents = Vec::new();
        old_file
            .read_to_end(&mut old_contents)
            .expect("read old inode");
        assert_eq!(
            old_contents, b"old",
            "publication must not modify the old inode"
        );
        assert_eq!(fs::read(path).expect("read new inode"), b"new content");
    }

    #[test]
    fn atomic_write_concurrent_sibling_destinations_do_not_collide() {
        let directory = tempfile::tempdir().expect("tempdir");
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for index in 0..8 {
                let barrier = &barrier;
                let path = directory.path().join(format!("artifact.{index}"));
                scope.spawn(move || {
                    let contents = vec![index as u8; 4096];
                    barrier.wait();
                    for _ in 0..8 {
                        atomic_write(&path, &contents).expect("publish distinct destination");
                        assert_eq!(fs::read(&path).expect("read own destination"), contents);
                    }
                });
            }
        });
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 8);
    }

    #[test]
    fn atomic_write_rename_failure_removes_temporary_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("existing-directory");
        fs::create_dir(&destination).expect("create directory");
        assert!(atomic_write(&destination, b"new").is_err());
        assert!(destination.is_dir());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn atomic_write_accepts_bare_filename() {
        const CHILD: &str = "DURABLE_FS_BARE_FILENAME_TEST";
        if std::env::var_os(CHILD).is_some() {
            atomic_write(Path::new("artifact"), b"new").expect("publish relative filename");
            assert_eq!(fs::read("artifact").unwrap(), b"new");
            return;
        }
        let directory = tempfile::tempdir().expect("tempdir");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::atomic_write_accepts_bare_filename"])
            .current_dir(directory.path())
            .env(CHILD, "1")
            .output()
            .expect("run with isolated current directory");
        assert!(output.status.success(), "{output:?}");
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn fsync_dir_reports_missing_directory() {
        let directory = tempfile::tempdir().expect("tempdir");
        let missing = directory.path().join("no-such-dir");
        assert!(fsync_dir(&missing).is_err());
    }
}
