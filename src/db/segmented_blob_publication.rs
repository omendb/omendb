//! Segmented blob artifact publication and post-manifest cleanup.
//!
//! This module owns the segmented layout's filesystem effects: catalog
//! backups and deltas, append-only segment suffixes, catalog visibility, and
//! stale-segment cleanup. `DB` remains the owner of mutable blob state and
//! publication ordering; the segmented methods preserve the existing
//! manifest-selected catalog frontier and recovery protocol.

use super::*;
use crate::space::reserve_file;
use std::io::{Seek, SeekFrom, Write};

impl DB {
    /// Preserve the catalog selected by the current manifest before a full
    /// segmented catalog consolidation.
    fn prepare_segment_catalog_backup(&self, consolidate: bool) -> Result<()> {
        if !consolidate {
            return Ok(());
        }
        let backup_path = self.path.join(BLOB_REWRITE_BACKUP_FILE);
        if backup_path.exists() {
            return Ok(());
        }

        let catalog_path = self.path.join(BLOB_FILE);
        if catalog_path.exists() {
            fs::rename(&catalog_path, &backup_path)?;
            sync_directory(&self.path)?;
            return Ok(());
        }

        // A first segmented publication has no catalog to rename. Keep a
        // valid empty catalog for the generation selected by the current
        // manifest so a failed first catalog publication can recover to the
        // empty database just like later publications recover to their old
        // catalog.
        let generation_id = self
            .manifest_history
            .latest()
            .map_or(0, |manifest| manifest.generation_id.get());
        let mut empty = BlobManager::with_threshold_and_mode(self.blobs.threshold(), true);
        empty.set_generation(generation_id);
        let bytes = empty.to_segment_catalog_bytes();
        atomic_write_without_fault_injection(&backup_path, &bytes)
    }

    pub(super) fn finish_segment_catalog_backup(&self) -> Result<()> {
        let backup_path = self.path.join(BLOB_REWRITE_BACKUP_FILE);
        let consolidated = backup_path.exists();
        if consolidated {
            fs::remove_file(backup_path)?;
            let delta_path = self.path.join(BLOB_DELTA_FILE);
            if delta_path.exists() {
                fs::remove_file(delta_path)?;
            }
            sync_directory(&self.path)?;
        }
        Ok(())
    }

    fn append_segment_catalog_delta(&self, delta: &[u8]) -> Result<u64> {
        let delta_path = self.path.join(BLOB_DELTA_FILE);
        let existing = match fs::read(&delta_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let prefix = BlobManager::segment_catalog_delta_prefix_len_through_generation(
            &existing,
            self.blobs.persisted_segment_catalog_generation(),
        )
        .ok_or_else(|| Error::Corruption("segmented catalog delta log is invalid".into()))?;
        let prefix = u64::try_from(prefix).map_err(|_| Error::DiskFull)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&delta_path)?;
        if prefix != file.metadata()?.len() {
            file.set_len(prefix)?;
        }
        file.seek(SeekFrom::Start(prefix))?;

        #[cfg(any(test, feature = "fault-injection"))]
        let short_write =
            FAIL_NEXT_BLOB_SEGMENT_CATALOG_DELTA_SHORT_WRITE.with(|failure| failure.replace(false));
        #[cfg(not(any(test, feature = "fault-injection")))]
        let short_write = false;

        #[cfg(any(test, feature = "fault-injection"))]
        let torn_write =
            FAIL_NEXT_BLOB_SEGMENT_CATALOG_DELTA_TORN_WRITE.with(|failure| failure.replace(false));
        #[cfg(not(any(test, feature = "fault-injection")))]
        let torn_write = false;

        if short_write {
            let partial_len = (delta.len() / 2).max(1).min(delta.len());
            file.write_all(&delta[..partial_len])?;
            file.flush()?;
            return Err(std::io::Error::other("injected short blob catalog delta write").into());
        }

        file.write_all(delta)?;

        if torn_write {
            if !delta.is_empty() {
                let offset = delta.len() / 2;
                file.seek(SeekFrom::Start(prefix + offset as u64))?;
                file.write_all(&[delta[offset] ^ 0xA5])?;
            }
            file.flush()?;
            return Err(std::io::Error::other("injected torn blob catalog delta write").into());
        }

        file.flush()?;

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_BLOB_SEGMENT_CATALOG_AFTER_WRITE.with(|failure| failure.replace(false)) {
            return Err(
                std::io::Error::other("injected failure after blob catalog delta write").into(),
            );
        }

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected blob catalog sync failure").into());
        }
        file.sync_all()?;
        Ok(delta.len() as u64)
    }

    fn write_segment_suffixes(&self) -> Result<u64> {
        let mut bytes_written = 0u64;
        for file_id in self.blobs.segment_file_ids() {
            let data = self
                .blobs
                .segment_bytes(file_id)
                .ok_or_else(|| Error::Corruption("blob segment disappeared from catalog".into()))?;
            let persisted = self.blobs.persisted_segment_length(file_id);
            let persisted_usize = usize::try_from(persisted)
                .map_err(|_| Error::Corruption("blob segment length overflows usize".into()))?;
            if data.len() < persisted_usize {
                return Err(Error::Corruption(
                    "blob segment shrank below its catalog frontier".into(),
                ));
            }
            if data.len() == persisted_usize {
                continue;
            }

            let segment_path = blob_segment_path(&self.path, file_id);
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&segment_path)?;
            let physical_len = file.metadata()?.len();
            if physical_len < persisted {
                return Err(Error::Corruption(
                    "blob segment is shorter than its catalog frontier".into(),
                ));
            }
            if physical_len != persisted {
                file.set_len(persisted)?;
            }
            let new_len = data.len() as u64;
            reserve_file(&file, new_len)?;
            file.set_len(new_len)?;
            file.seek(SeekFrom::Start(persisted))?;
            let suffix = &data[persisted_usize..];

            #[cfg(any(test, feature = "fault-injection"))]
            let short_write =
                FAIL_NEXT_BLOB_SEGMENT_SHORT_WRITE.with(|failure| failure.replace(false));
            #[cfg(not(any(test, feature = "fault-injection")))]
            let short_write = false;

            #[cfg(any(test, feature = "fault-injection"))]
            let torn_write =
                FAIL_NEXT_BLOB_SEGMENT_TORN_WRITE.with(|failure| failure.replace(false));
            #[cfg(not(any(test, feature = "fault-injection")))]
            let torn_write = false;

            if short_write {
                let partial_len = (suffix.len() / 2).max(1);
                file.write_all(&suffix[..partial_len])?;
                file.flush()?;
                return Err(std::io::Error::other("injected short blob segment write").into());
            }

            file.write_all(suffix)?;

            if torn_write {
                let offset = suffix.len() / 2;
                file.seek(SeekFrom::Start(persisted + offset as u64))?;
                file.write_all(&[suffix[offset] ^ 0xA5])?;
                file.flush()?;
                return Err(std::io::Error::other("injected torn blob segment write").into());
            }

            file.flush()?;

            #[cfg(any(test, feature = "fault-injection"))]
            if FAIL_NEXT_BLOB_SEGMENT_SYNC.with(|failure| failure.replace(false)) {
                return Err(std::io::Error::other("injected blob segment sync failure").into());
            }
            file.sync_all()?;
            bytes_written = bytes_written.saturating_add(new_len - persisted);

            #[cfg(any(test, feature = "fault-injection"))]
            if FAIL_NEXT_BLOB_SEGMENT_AFTER_WRITE.with(|failure| failure.replace(false)) {
                return Err(
                    std::io::Error::other("injected failure after blob segment write").into(),
                );
            }
        }

        Ok(bytes_written)
    }

    fn publish_segment_catalog(&mut self, consolidate: bool) -> Result<u64> {
        let catalog_path = self.path.join(BLOB_FILE);
        if consolidate {
            let catalog = self.blobs.to_segment_catalog_bytes();
            atomic_write_without_directory_sync(&catalog_path, &catalog)?;
            self.blobs.mark_segment_catalog_consolidated();
            Ok(catalog.len() as u64)
        } else {
            let delta = self
                .blobs
                .to_segment_catalog_delta_bytes()
                .ok_or_else(|| Error::Corruption("segmented catalog delta overflows".into()))?;
            let bytes_written = self.append_segment_catalog_delta(&delta)?;
            self.blobs.mark_segment_delta_persisted();
            Ok(bytes_written)
        }
    }

    pub(super) fn write_blob_segments(&mut self) -> Result<u64> {
        let catalog_path = self.path.join(BLOB_FILE);
        let consolidate = !catalog_path.exists() || self.blobs.catalog_needs_consolidation();
        self.prepare_segment_catalog_backup(consolidate)?;
        let segment_bytes = self.write_segment_suffixes()?;
        let catalog_bytes = self.publish_segment_catalog(consolidate)?;
        Ok(segment_bytes.saturating_add(catalog_bytes))
    }

    /// Remove segment files no longer named by the authoritative active
    /// catalog. This runs only after the manifest publication barrier, so an
    /// interrupted rewrite leaves the old segments available for catalog
    /// recovery.
    pub(super) fn prune_unreferenced_blob_segments(&self) -> Result<()> {
        let live = self
            .blobs
            .segment_file_ids()
            .into_iter()
            .collect::<HashSet<_>>();
        let mut removed = false;
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(file_id) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix(BLOB_SEGMENT_PREFIX))
                .and_then(|suffix| suffix.parse::<u32>().ok())
            else {
                continue;
            };
            if !live.contains(&file_id) {
                fs::remove_file(entry.path())?;
                removed = true;

                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_BLOB_SEGMENT_PRUNE_AFTER_REMOVE.with(|failure| failure.replace(false))
                {
                    return Err(std::io::Error::other("injected blob segment prune failure").into());
                }
            }
        }
        if removed {
            sync_directory(&self.path)?;
        }
        Ok(())
    }

    /// Finish the post-manifest cleanup for segmented blob publication.
    ///
    /// The manifest is the publication barrier. After it is durable, stale
    /// segments and the previous catalog backup are derived artifacts that
    /// can be removed. A failure may leave either artifact behind; reopening
    /// and the next publication reconcile them through the authoritative
    /// catalog and manifest history.
    pub(super) fn finish_segmented_blob_publication_cleanup(&self) -> Result<()> {
        if !self.blobs.is_segmented() {
            return Ok(());
        }
        self.prune_unreferenced_blob_segments()?;
        self.finish_segment_catalog_backup()?;
        Ok(())
    }
}
