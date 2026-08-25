//! Low-level durable artifact I/O and filesystem-barrier helpers.
//!
//! This module owns temporary-file publication, reserved artifact writes,
//! directory durability barriers, and cleanup of non-authoritative temporary
//! and reservation files. `DB` remains the mutable state owner;
//! `publication.rs` owns ordering; and `durability.rs` owns admission policy.

#[cfg(any(test, feature = "fault-injection"))]
use super::BLOB_FILE;
use super::{BLOB_RESERVATION_FILE, Error, Result, WAL_RESERVATION_FILE};
#[cfg(any(test, feature = "fault-injection"))]
use super::{
    FAIL_NEXT_ATOMIC_RENAME, FAIL_NEXT_ATOMIC_SHORT_WRITE, FAIL_NEXT_ATOMIC_TORN_WRITE,
    FAIL_NEXT_BLOB_SEGMENT_CATALOG_AFTER_WRITE, FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME,
    FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE, FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC,
    FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE, FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC,
};
use crate::space::{preallocate_file, reserve_file};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

pub(super) fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    atomic_write_with_fault_injection(path, data, true)
}

/// Persist the reuse reservation without consuming a fault intended for a
/// PMT/blob checkpoint. The ledger has its own real I/O error path; the
/// publication-fault harness targets the artifact under test explicitly.
pub(super) fn atomic_write_without_fault_injection(path: &Path, data: &[u8]) -> Result<()> {
    atomic_write_with_fault_injection(path, data, false)
}

fn atomic_write_with_fault_injection(path: &Path, data: &[u8], inject_faults: bool) -> Result<()> {
    atomic_write_with_options(path, data, inject_faults, true)
}

pub(super) fn atomic_write_without_directory_sync(path: &Path, data: &[u8]) -> Result<()> {
    atomic_write_with_options(path, data, true, false)
}

fn atomic_write_with_options(
    path: &Path,
    data: &[u8],
    inject_faults: bool,
    sync_parent: bool,
) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    if let Err(error) = preallocate_file(&file, data.len() as u64) {
        drop(file);
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    #[cfg(any(test, feature = "fault-injection"))]
    let short_write = inject_faults
        && ((path.file_name().is_some_and(|name| name == BLOB_FILE)
            && FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE.with(|failure| failure.replace(false)))
            || FAIL_NEXT_ATOMIC_SHORT_WRITE.with(|failure| failure.replace(false)));
    #[cfg(not(any(test, feature = "fault-injection")))]
    let short_write = false;
    #[cfg(not(any(test, feature = "fault-injection")))]
    let _ = inject_faults;
    #[cfg(any(test, feature = "fault-injection"))]
    let torn_write = inject_faults
        && ((path.file_name().is_some_and(|name| name == BLOB_FILE)
            && FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE.with(|failure| failure.replace(false)))
            || FAIL_NEXT_ATOMIC_TORN_WRITE.with(|failure| failure.replace(false)));
    #[cfg(not(any(test, feature = "fault-injection")))]
    let torn_write = false;

    if short_write {
        let prefix_len = data.len() / 2;
        file.set_len(prefix_len as u64)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&data[..prefix_len])?;
    } else {
        file.write_all(data)?;
        if torn_write && !data.is_empty() {
            let offset = data.len() / 2;
            file.seek(SeekFrom::Start(offset as u64))?;
            file.write_all(&[data[offset] ^ 0xA5])?;
        }
    }
    file.flush()?;

    #[cfg(any(test, feature = "fault-injection"))]
    if inject_faults
        && path.file_name().is_some_and(|name| name == BLOB_FILE)
        && FAIL_NEXT_BLOB_SEGMENT_CATALOG_AFTER_WRITE.with(|failure| failure.replace(false))
    {
        return Err(std::io::Error::other("injected failure after blob catalog write").into());
    }

    #[cfg(any(test, feature = "fault-injection"))]
    if inject_faults
        && path.file_name().is_some_and(|name| name == BLOB_FILE)
        && FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC.with(|failure| failure.replace(false))
    {
        return Err(std::io::Error::other("injected blob catalog sync failure").into());
    }
    file.sync_all()?;
    crate::storage::record_durability_sync();
    drop(file);

    #[cfg(any(test, feature = "fault-injection"))]
    let blob_catalog_rename_failure = inject_faults
        && path.file_name().is_some_and(|name| name == BLOB_FILE)
        && FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME.with(|failure| failure.replace(false));
    #[cfg(any(test, feature = "fault-injection"))]
    if blob_catalog_rename_failure {
        return Err(std::io::Error::other("injected blob catalog rename failure").into());
    }

    #[cfg(any(test, feature = "fault-injection"))]
    let injected = inject_faults && FAIL_NEXT_ATOMIC_RENAME.with(|failure| failure.replace(false));
    #[cfg(any(test, feature = "fault-injection"))]
    if injected {
        return Err(std::io::Error::other("injected atomic rename failure").into());
    }

    fs::rename(&temporary, path)?;

    #[cfg(any(test, feature = "fault-injection"))]
    if short_write || torn_write {
        return Err(std::io::Error::other("injected atomic artifact write failure").into());
    }

    if sync_parent {
        sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
    } else {
        Ok(())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn atomic_write_reserved(
    path: &Path,
    reservation: &Path,
    data: &[u8],
    sync_parent: bool,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(reservation)?;
    if !reserve_file(&file, data.len() as u64)? {
        return Err(Error::DiskFull);
    }
    file.set_len(data.len() as u64)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(data)?;
    file.flush()?;
    file.sync_all()?;
    crate::storage::record_durability_sync();
    drop(file);

    #[cfg(any(test, feature = "fault-injection"))]
    let injected = FAIL_NEXT_ATOMIC_RENAME.with(|failure| failure.replace(false));
    #[cfg(any(test, feature = "fault-injection"))]
    if injected {
        return Err(std::io::Error::other("injected atomic rename failure").into());
    }

    fs::rename(reservation, path)?;
    if sync_parent {
        sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
    } else {
        Ok(())
    }
}

#[cfg(any(test, feature = "fault-injection"))]
#[allow(dead_code)]
pub(super) fn inject_atomic_rename_failure() {
    FAIL_NEXT_ATOMIC_RENAME.with(|failure| failure.set(true));
}

pub(super) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
        crate::storage::record_durability_sync();
    }
    Ok(())
}

pub(super) fn sync_publication_directory(path: &Path) -> Result<()> {
    #[cfg(any(test, feature = "fault-injection"))]
    if FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC.with(|failure| failure.replace(false)) {
        return Err(std::io::Error::other("injected publication directory sync failure").into());
    }
    sync_directory(path)
}

/// Sync a newly created directory and each newly reachable parent directory.
///
/// `create_dir_all` can create more than one ancestor. Syncing only the
/// immediate parent would leave an outer directory entry vulnerable to being
/// lost after an acknowledged create on filesystems that honor directory
/// durability separately from file durability.
pub(super) fn sync_directory_chain(path: &Path) -> Result<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let mut current = path;
    loop {
        sync_directory(current)?;
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current {
            break;
        }
        if parent.as_os_str().is_empty() {
            sync_directory(Path::new("."))?;
            break;
        }
        current = parent;
    }
    Ok(())
}

/// Remove non-authoritative atomic-publication temporary files left by an
/// interrupted write. They are safe to discard because every authoritative
/// artifact is selected by the manifest or its catalog, never by a `.tmp`
/// name. Read-only/check handles deliberately leave them untouched.
pub(super) fn cleanup_orphaned_temporary_artifacts(path: &Path) -> Result<()> {
    let mut removed = false;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.file_name().to_string_lossy().ends_with(".tmp") {
            fs::remove_file(entry.path())?;
            removed = true;
        }
    }
    if removed {
        sync_directory(path)?;
    }
    Ok(())
}

pub(super) fn clear_blob_reservation(path: &Path) -> Result<()> {
    let reservation = path.join(BLOB_RESERVATION_FILE);
    if reservation.exists() {
        fs::remove_file(reservation)?;
        sync_directory(path)?;
    }
    Ok(())
}

pub(super) fn clear_wal_reservation(path: &Path) -> Result<()> {
    let reservation = path.join(WAL_RESERVATION_FILE);
    if reservation.exists() {
        fs::remove_file(reservation)?;
        sync_directory(path)?;
    }
    Ok(())
}
