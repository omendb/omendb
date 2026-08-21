//! Two-slot manifest publication and recovery selection.

use super::format::{MANIFEST_SLOT_SIZE, Manifest};
use crate::error::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(any(test, feature = "fault-injection"))]
use std::cell::Cell;

#[cfg(any(test, feature = "fault-injection"))]
thread_local! {
    static FAIL_NEXT_MANIFEST_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_MIRROR_MANIFEST_SYNC: Cell<bool> = const { Cell::new(false) };
}

const MANIFEST_SLOT_COUNT: usize = 2;
const MANIFEST_FILE_SIZE: u64 = (MANIFEST_SLOT_SIZE * MANIFEST_SLOT_COUNT) as u64;

/// A two-slot manifest file with fail-closed publication.
pub struct ManifestStore {
    file: File,
}

impl ManifestStore {
    /// Open or create a two-slot manifest file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        let length = file.metadata()?.len();
        match length {
            0 => file.set_len(MANIFEST_FILE_SIZE)?,
            MANIFEST_FILE_SIZE => {}
            _ => {
                return Err(Error::Corruption(format!(
                    "manifest has invalid length {length}"
                )));
            }
        }
        file.seek(SeekFrom::Start(0))?;
        Ok(Self { file })
    }

    /// Open an existing manifest without write permissions.
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let length = file.metadata()?.len();
        if length != MANIFEST_FILE_SIZE {
            return Err(Error::Corruption(format!(
                "manifest has invalid length {length}"
            )));
        }
        Ok(Self { file })
    }

    /// Load the newest valid manifest generation.
    pub fn load_latest(&mut self) -> Result<Option<Manifest>> {
        let manifests = self.load_valid_manifests()?;
        Ok(manifests
            .into_iter()
            .max_by_key(|manifest| (manifest.generation_id, manifest.commit_id)))
    }

    /// Load every independently valid manifest slot.
    ///
    /// Maintenance must treat both slots as recovery roots. The newest slot
    /// is the normal authority, but the older slot is still the fallback until
    /// the next publication has made it safe to overwrite or remove its
    /// artifacts.
    pub fn load_valid_manifests(&mut self) -> Result<Vec<Manifest>> {
        let mut manifests = Vec::with_capacity(MANIFEST_SLOT_COUNT);
        let mut saw_invalid = false;

        for slot in 0..MANIFEST_SLOT_COUNT {
            let bytes = self.read_slot(slot)?;
            match Manifest::from_bytes(&bytes) {
                Ok(Some(manifest)) => manifests.push(manifest),
                Ok(None) => {}
                Err(_) => saw_invalid = true,
            }
        }

        if manifests.is_empty() && saw_invalid {
            return Err(Error::Corruption("no valid manifest generation".into()));
        }
        Ok(manifests)
    }

    /// Publish a new manifest into the inactive slot and sync it.
    pub fn publish(&mut self, manifest: Manifest) -> Result<()> {
        let current_slot = self.current_slot()?;
        let target_slot = current_slot.map_or(0, |slot| 1 - slot);
        let bytes = manifest.to_bytes();
        self.write_slot(target_slot, &bytes)?;
        self.file.flush()?;

        self.sync_manifest(false)
    }

    /// Copy the current manifest into the inactive slot before a maintenance
    /// or user generation may reuse pages named by the older slot.
    pub fn publish_mirrored(&mut self, manifest: Manifest) -> Result<()> {
        let current_slot = self.current_slot()?;
        let target_slot = current_slot.map_or(0, |slot| 1 - slot);
        let bytes = manifest.to_bytes();
        self.write_slot(target_slot, &bytes)?;
        self.file.flush()?;

        self.sync_manifest(true)
    }

    fn sync_manifest(&mut self, mirror: bool) -> Result<()> {
        #[cfg(not(any(test, feature = "fault-injection")))]
        let _ = mirror;

        #[cfg(any(test, feature = "fault-injection"))]
        if mirror {
            if FAIL_NEXT_MIRROR_MANIFEST_SYNC.with(|failure| failure.replace(false)) {
                return Err(std::io::Error::other("injected mirror manifest sync failure").into());
            }
        } else if FAIL_NEXT_MANIFEST_SYNC.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected manifest sync failure").into());
        }

        self.file.sync_all()?;
        crate::storage::record_durability_sync();
        Ok(())
    }

    /// Publish identical metadata into both slots.
    ///
    /// This is used when a copied archive becomes a new history. Equal
    /// generation/commit identities otherwise make the normal alternating
    /// publisher continue selecting the same slot.
    pub fn publish_replicated(&mut self, manifest: Manifest) -> Result<()> {
        let bytes = manifest.to_bytes();
        self.write_slot(0, &bytes)?;
        self.write_slot(1, &bytes)?;
        self.file.flush()?;

        self.sync_manifest(false)
    }

    /// Inject one failure at the next manifest sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_sync_failure(&self) {
        FAIL_NEXT_MANIFEST_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure at the next safety-mirror sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_mirror_sync_failure(&self) {
        FAIL_NEXT_MIRROR_MANIFEST_SYNC.with(|failure| failure.set(true));
    }

    fn write_slot(&mut self, slot: usize, bytes: &[u8; MANIFEST_SLOT_SIZE]) -> Result<()> {
        self.file
            .seek(SeekFrom::Start((slot * MANIFEST_SLOT_SIZE) as u64))?;
        self.file.write_all(bytes)?;
        Ok(())
    }

    fn current_slot(&mut self) -> Result<Option<usize>> {
        let mut newest = None;
        let mut saw_invalid = false;

        for slot in 0..MANIFEST_SLOT_COUNT {
            let bytes = self.read_slot(slot)?;
            match Manifest::from_bytes(&bytes) {
                Ok(Some(manifest)) => {
                    if newest.is_none_or(|(_, current)| manifest.is_newer_than(current)) {
                        newest = Some((slot, manifest));
                    }
                }
                Ok(None) => {}
                Err(_) => saw_invalid = true,
            }
        }

        if newest.is_none() && saw_invalid {
            return Err(Error::Corruption("no valid manifest generation".into()));
        }
        Ok(newest.map(|(slot, _)| slot))
    }

    fn read_slot(&mut self, slot: usize) -> Result<[u8; MANIFEST_SLOT_SIZE]> {
        let mut bytes = [0; MANIFEST_SLOT_SIZE];
        self.file
            .seek(SeekFrom::Start((slot * MANIFEST_SLOT_SIZE) as u64))?;
        self.file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}
