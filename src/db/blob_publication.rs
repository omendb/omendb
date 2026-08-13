//! Durable whole-image blob publication and rewrite-backup recovery.
//!
//! Segmented blob effects live in `segmented_blob_publication.rs`. `DB` remains
//! the owner of mutable blob state and publication ordering; these methods only
//! translate whole-image state into durable artifacts and reconcile interrupted
//! artifact publication.

use super::*;

impl DB {
    pub(super) fn write_blob_image(&self, path: &Path, data: &[u8]) -> Result<u64> {
        self.write_blob_image_with_directory_sync(path, data, true)
    }

    /// Write a blob image while deferring the containing-directory sync to the
    /// publication barrier. The caller must sync the directory before making
    /// a new manifest authoritative.
    pub(super) fn write_blob_image_without_directory_sync(
        &self,
        path: &Path,
        data: &[u8],
    ) -> Result<u64> {
        self.write_blob_image_with_directory_sync(path, data, false)
    }

    fn write_blob_image_with_directory_sync(
        &self,
        path: &Path,
        data: &[u8],
        sync_parent: bool,
    ) -> Result<u64> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let reservation = self.path.join(BLOB_RESERVATION_FILE);
            if reservation.is_file() {
                atomic_write_reserved(path, &reservation, data, sync_parent)?;
                return Ok(data.len() as u64);
            }
        }

        if sync_parent {
            atomic_write(path, data)?;
        } else {
            atomic_write_without_directory_sync(path, data)?;
        }
        Ok(data.len() as u64)
    }

    /// Reconcile a blob image or segmented catalog kept aside while a blob
    /// publication crosses the blob-image/manifest boundary.
    ///
    /// The backup has the generation selected by the prior manifest. If that
    /// generation is still authoritative, the publication did not complete
    /// and the backup must be restored. If the manifest advanced, publication
    /// completed and the backup is only stale cleanup state.
    pub(super) fn recover_blob_rewrite_backup(
        path: &Path,
        current_manifest: Option<Manifest>,
        read_only: bool,
    ) -> Result<Option<Vec<u8>>> {
        let backup_path = path.join(BLOB_REWRITE_BACKUP_FILE);
        if !backup_path.is_file() {
            return Ok(None);
        }

        let manifest_generation = current_manifest.map(|manifest| manifest.generation_id.get());
        let blob_path = path.join(BLOB_FILE);
        if let Some(manifest_generation) = manifest_generation
            && blob_path.is_file()
            && let Some(blobs) = fs::read(&blob_path).ok().and_then(|bytes| {
                parse_blob_catalog(path, &bytes, Some(manifest_generation))
                    .ok()
                    .flatten()
            })
            && blobs.generation_id() == manifest_generation
        {
            if !read_only {
                fs::remove_file(&backup_path)?;
                if blobs.is_segmented() {
                    let delta_path = path.join(BLOB_DELTA_FILE);
                    if delta_path.exists() {
                        fs::remove_file(delta_path)?;
                    }
                }
                sync_directory(path)?;
            }
            return Ok(None);
        }

        let backup_bytes = fs::read(&backup_path)?;
        let backup_blobs = parse_blob_catalog(path, &backup_bytes, manifest_generation)?
            .ok_or_else(|| {
                Error::Corruption("interrupted blob rewrite backup is invalid".into())
            })?;
        let Some(manifest_generation) = manifest_generation else {
            if read_only {
                return Ok(Some(backup_bytes));
            }
            if blob_path.exists() {
                fs::remove_file(&blob_path)?;
            }
            fs::rename(&backup_path, &blob_path)?;
            sync_directory(path)?;
            return Ok(None);
        };

        let backup_generation = backup_blobs.generation_id();
        if backup_generation > manifest_generation {
            return Err(Error::Corruption(format!(
                "blob rewrite backup generation {} is newer than manifest {}",
                backup_generation, manifest_generation
            )));
        }
        if backup_generation < manifest_generation {
            if !read_only {
                fs::remove_file(&backup_path)?;
                sync_directory(path)?;
            }
            return Ok(None);
        }

        let current_blobs = if blob_path.is_file() {
            fs::read(&blob_path)
                .ok()
                .and_then(|bytes| parse_blob_catalog(path, &bytes, None).ok().flatten())
        } else {
            None
        };
        let needs_restore = current_blobs
            .as_ref()
            .is_none_or(|blobs| blobs.generation_id() != manifest_generation);
        if !needs_restore {
            if !read_only {
                fs::remove_file(&backup_path)?;
                sync_directory(path)?;
            }
            return Ok(None);
        }
        if read_only {
            return Ok(Some(backup_bytes));
        }

        if blob_path.exists() {
            fs::remove_file(&blob_path)?;
        }
        fs::rename(&backup_path, &blob_path)?;
        sync_directory(path)?;
        Ok(None)
    }
}
