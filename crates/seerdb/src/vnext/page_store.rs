//! Synchronous out-of-place page-image store for the vNext kernel.
//!
//! One append-only arena holds checksummed page images; immutable map files
//! record which physical image currently serves each logical page. Rewrites
//! always append, so a prior image and every prior map remain intact.
//!
//! This establishes persistent **placement** authority, not database recovery
//! authority. Reopening a spilled working map does not make uncheckpointed pages
//! restart authority: only a validated complete checkpoint may select a map for
//! recovery. In particular this module never discovers a newest map, never
//! repairs a referenced image, and never derives the append position from map
//! references instead of the validated complete arena prefix.
//!
//! The caller owns the directory exclusively. File-level mutexes and
//! `create_new` protect one process; they are not a cross-process write lock.

use super::page_image::{
    IMAGE_FILE_HEADER_SIZE, ImageLocation, PAGE_IMAGE_HEADER_SIZE, PageImageMetadata,
    decode_file_header, decode_image, decode_image_header, encode_file_header, encode_image,
    is_image_offset, page_image_bytes,
};
use super::page_map::{self, PageMap, PageMapEntry, PageMapId, PageMapRef};
use super::{PageDependencyTable, PageIo, PageKey, StoreIncarnation};
use durable_fs::{SyncClass, fsync_dir, sync_file_all};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const IMAGE_FILE_NAME: &str = "page-images.dat";

/// Which page-store operation failed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PageStoreOperation {
    Create,
    Open,
    Read,
    AppendImage,
    SyncImages,
    WriteMap,
    SyncMap,
    SyncDirectory,
    RepairTail,
}

/// Failure from the persistent page-image store.
#[derive(Debug, thiserror::Error)]
pub enum PageStoreError {
    #[error("invalid page-store input: {0}")]
    InvalidInput(&'static str),
    #[error("page store is corrupt: {0}")]
    Corruption(&'static str),
    #[error("missing logical page {0:?}")]
    MissingPage(PageKey),
    #[error("page dependencies are not durable for {0:?}")]
    DependenciesNotDurable(PageKey),
    #[error("page map {0:?} already exists")]
    MapExists(PageMapId),
    #[error("page-store identity or offset space exhausted")]
    Exhausted,
    #[error("page store is fenced until reopen")]
    Fenced,
    #[error("page-store operation lock is poisoned")]
    Poisoned,
    #[error("page-store {operation:?} failed: {source}")]
    Io {
        operation: PageStoreOperation,
        #[source]
        source: io::Error,
    },
}

struct PageStoreState {
    images: File,
    end_offset: u64,
    working: BTreeMap<PageKey, ImageLocation>,
    fenced: bool,
}

/// Append-only page-image arena plus an explicit working placement overlay.
pub struct PersistentPageIo {
    directory: PathBuf,
    store: StoreIncarnation,
    page_bytes: u32,
    sync_class: SyncClass,
    dependencies: Arc<PageDependencyTable>,
    state: Mutex<PageStoreState>,
}

impl PersistentPageIo {
    /// Create a new empty page store.
    ///
    /// An existing arena is never adopted as a new store: the arena header is
    /// what binds images to a store incarnation.
    pub fn create(
        directory: &Path,
        store: StoreIncarnation,
        page_bytes: u32,
        sync_class: SyncClass,
    ) -> Result<Self, PageStoreError> {
        validate_page_bytes(page_bytes)?;
        std::fs::create_dir_all(directory).map_err(|source| PageStoreError::Io {
            operation: PageStoreOperation::Create,
            source,
        })?;
        let path = directory.join(IMAGE_FILE_NAME);
        let mut images = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|source| PageStoreError::Io {
                operation: PageStoreOperation::Create,
                source,
            })?;
        let header = encode_file_header(store, page_bytes);
        images
            .write_all(&header)
            .and_then(|()| images.sync_all())
            .and_then(|()| fsync_dir(directory))
            .map_err(|source| PageStoreError::Io {
                operation: PageStoreOperation::Create,
                source,
            })?;
        Ok(Self {
            directory: directory.to_path_buf(),
            store,
            page_bytes,
            sync_class,
            dependencies: Arc::new(PageDependencyTable::new()),
            state: Mutex::new(PageStoreState {
                images,
                end_offset: IMAGE_FILE_HEADER_SIZE as u64,
                working: BTreeMap::new(),
                fenced: false,
            }),
        })
    }

    /// Reopen one explicitly selected immutable map.
    ///
    /// The map reference must come from outside the file being validated (a
    /// caller now, a validated manifest later). Missing components are never
    /// created and no newest map is searched for.
    pub fn open(
        directory: &Path,
        expected_store: StoreIncarnation,
        selected: PageMapRef,
        sync_class: SyncClass,
    ) -> Result<(Self, PageMap), PageStoreError> {
        let map_path = map_path(directory, selected.id());
        let map_bytes = read_exact_file(&map_path, selected.file_bytes())?;
        let map = page_map::decode(&map_bytes, selected, expected_store)?;
        let page_bytes = map.page_bytes();

        let images_path = directory.join(IMAGE_FILE_NAME);
        let mut images = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&images_path)
            .map_err(|source| PageStoreError::Io {
                operation: PageStoreOperation::Open,
                source,
            })?;
        let file_len = images
            .metadata()
            .map_err(|source| PageStoreError::Io {
                operation: PageStoreOperation::Open,
                source,
            })?
            .len();
        let mut header = [0u8; IMAGE_FILE_HEADER_SIZE];
        images
            .read_exact(&mut header)
            .map_err(|source| PageStoreError::Io {
                operation: PageStoreOperation::Open,
                source,
            })?;
        let arena = decode_file_header(&header)?;
        if arena.store() != expected_store {
            return Err(PageStoreError::Corruption(
                "page-image arena store incarnation mismatch",
            ));
        }
        if arena.page_bytes() != page_bytes {
            return Err(PageStoreError::Corruption(
                "page-image arena page size mismatch",
            ));
        }

        // Validate every complete image in the arena and locate the first
        // incomplete final append. Complete corruption fails closed even in an
        // unselected image.
        let slot = page_image_bytes(page_bytes).ok_or(PageStoreError::Exhausted)?;
        let mut end_offset = IMAGE_FILE_HEADER_SIZE as u64;
        let mut tail = None;
        while end_offset < file_len {
            let remaining = file_len - end_offset;
            if remaining < PAGE_IMAGE_HEADER_SIZE as u64 {
                tail = Some(end_offset);
                break;
            }
            if remaining < slot {
                let mut header = [0u8; PAGE_IMAGE_HEADER_SIZE];
                read_at(&mut images, end_offset, &mut header)?;
                // A well-framed final header whose payload or trailer is missing
                // is the one repairable append case; any framing failure is
                // complete corruption and fails closed.
                decode_image_header(&header, expected_store, page_bytes)?;
                tail = Some(end_offset);
                break;
            }
            let mut frame =
                vec![0u8; usize::try_from(slot).map_err(|_| PageStoreError::Exhausted)?];
            read_at(&mut images, end_offset, &mut frame)?;
            let (metadata, _) = decode_image(&frame, expected_store, page_bytes)?;
            if metadata.location().offset() != end_offset {
                return Err(PageStoreError::Corruption(
                    "page-image self-offset disagrees with its arena position",
                ));
            }
            end_offset += slot;
        }

        // Selected references must resolve inside the validated complete prefix.
        let mut restored = BTreeMap::new();
        for entry in map.entries() {
            let location = entry.image();
            if !is_image_offset(location.offset(), page_bytes) {
                return Err(PageStoreError::Corruption(
                    "page-map reference is not an image slot boundary",
                ));
            }
            if location.offset() + slot > end_offset {
                return Err(PageStoreError::Corruption(
                    "page-map reference lies beyond the validated arena prefix",
                ));
            }
            let mut frame =
                vec![0u8; usize::try_from(slot).map_err(|_| PageStoreError::Exhausted)?];
            read_at(&mut images, location.offset(), &mut frame)?;
            let (metadata, _) = decode_image(&frame, expected_store, page_bytes)?;
            if metadata.key() != entry.key() {
                return Err(PageStoreError::Corruption(
                    "page-map reference names the wrong logical page",
                ));
            }
            if metadata.location().checksum() != location.checksum() {
                return Err(PageStoreError::Corruption(
                    "page-map reference expected checksum mismatch",
                ));
            }
            restored.insert(entry.key(), location);
        }

        if map.image_file_end() != IMAGE_FILE_HEADER_SIZE as u64
            && !is_image_offset(map.image_file_end(), page_bytes)
        {
            return Err(PageStoreError::Corruption(
                "page-map image boundary is not a slot boundary",
            ));
        }
        if map.image_file_end() > end_offset {
            return Err(PageStoreError::Corruption(
                "page-map image boundary exceeds the validated arena prefix",
            ));
        }

        // Only now may an unreferenced incomplete final append be repaired.
        if let Some(offset) = tail {
            images
                .set_len(offset)
                .and_then(|()| images.sync_all())
                .map_err(|source| PageStoreError::Io {
                    operation: PageStoreOperation::RepairTail,
                    source,
                })?;
        }
        sync_file_all(&images, sync_class).map_err(|source| PageStoreError::Io {
            operation: PageStoreOperation::Open,
            source,
        })?;
        fsync_dir(directory).map_err(|source| PageStoreError::Io {
            operation: PageStoreOperation::SyncDirectory,
            source,
        })?;

        let dependencies = Arc::new(PageDependencyTable::new());
        for (key, location) in &restored {
            let mut frame =
                vec![0u8; usize::try_from(slot).map_err(|_| PageStoreError::Exhausted)?];
            read_at(&mut images, location.offset(), &mut frame)?;
            let (metadata, _) = decode_image(&frame, expected_store, page_bytes)?;
            dependencies
                .merge(*key, metadata.dependencies())
                .map_err(|_| PageStoreError::Poisoned)?;
        }

        let io = Self {
            directory: directory.to_path_buf(),
            store: expected_store,
            page_bytes,
            sync_class,
            dependencies,
            state: Mutex::new(PageStoreState {
                images,
                end_offset,
                working: restored,
                fenced: false,
            }),
        };
        Ok((io, map))
    }

    /// Dependency table this store gates writeback against.
    #[must_use]
    pub fn dependencies(&self) -> &Arc<PageDependencyTable> {
        &self.dependencies
    }

    /// Store incarnation bound to this handle.
    #[must_use]
    pub const fn store(&self) -> StoreIncarnation {
        self.store
    }

    /// Configured logical page size.
    #[must_use]
    pub const fn page_bytes(&self) -> u32 {
        self.page_bytes
    }

    /// Append one image out-of-place and return its physical placement.
    ///
    /// Success means the bytes and working placement were updated. It is not
    /// durability or restart publication: only a published map makes an image
    /// placement authority.
    pub fn write_image(
        &self,
        key: PageKey,
        source: &[u8],
    ) -> Result<ImageLocation, PageStoreError> {
        if source.len() != self.page_bytes as usize {
            return Err(PageStoreError::InvalidInput(
                "page-image payload size does not match the configured page size",
            ));
        }
        let required = self
            .dependencies
            .checked_requirements(key)
            .map_err(|error| match error.kind() {
                io::ErrorKind::WouldBlock => PageStoreError::DependenciesNotDurable(key),
                _ => PageStoreError::Poisoned,
            })?;
        let mut state = self.state.lock().map_err(|_| PageStoreError::Poisoned)?;
        if state.fenced {
            return Err(PageStoreError::Fenced);
        }
        let offset = state.end_offset;
        let frame = encode_image(self.store, key, offset, self.page_bytes, required, source)?;
        let location = ImageLocation::new(offset, image_frame_checksum(&frame));
        let seek = state.images.seek(SeekFrom::Start(offset));
        let written = seek.and_then(|_| state.images.write_all(&frame));
        if let Err(source) = written {
            state.fenced = true;
            return Err(PageStoreError::Io {
                operation: PageStoreOperation::AppendImage,
                source,
            });
        }
        state.end_offset = offset + frame.len() as u64;
        state.working.insert(key, location);
        Ok(location)
    }

    /// Current working placement of one logical page.
    pub fn placement(&self, key: PageKey) -> Result<ImageLocation, PageStoreError> {
        let state = self.state.lock().map_err(|_| PageStoreError::Poisoned)?;
        state
            .working
            .get(&key)
            .copied()
            .ok_or(PageStoreError::MissingPage(key))
    }

    /// Durably publish one immutable map over an explicitly selected entry set.
    ///
    /// Every referenced image is re-validated and re-checked for durability
    /// before the images are synchronized, so a published map never names an
    /// image whose own requirements were not covered.
    pub fn write_map(
        &self,
        id: PageMapId,
        entries: &[PageMapEntry],
    ) -> Result<PageMap, PageStoreError> {
        let mut state = self.state.lock().map_err(|_| PageStoreError::Poisoned)?;
        if state.fenced {
            return Err(PageStoreError::Fenced);
        }
        let slot = page_image_bytes(self.page_bytes).ok_or(PageStoreError::Exhausted)?;
        let mut placements = Vec::with_capacity(entries.len());
        let mut selected = BTreeMap::new();
        let mut previous: Option<PageKey> = None;
        for entry in entries {
            if previous.is_some_and(|key| key >= entry.key()) {
                return Err(PageStoreError::InvalidInput(
                    "page-map entries must be in strictly increasing key order",
                ));
            }
            previous = Some(entry.key());
            if placements.contains(&entry.image()) {
                return Err(PageStoreError::InvalidInput(
                    "page-map entries must not name the same physical image twice",
                ));
            }
            placements.push(entry.image());
            let location = entry.image();
            if !is_image_offset(location.offset(), self.page_bytes) {
                return Err(PageStoreError::InvalidInput(
                    "page-map reference is not an image slot boundary",
                ));
            }
            if location.offset() + slot > state.end_offset {
                return Err(PageStoreError::Corruption(
                    "page-map reference lies beyond the complete arena prefix",
                ));
            }
            let mut frame =
                vec![0u8; usize::try_from(slot).map_err(|_| PageStoreError::Exhausted)?];
            read_at(&mut state.images, location.offset(), &mut frame)?;
            let (metadata, _) = decode_image(&frame, self.store, self.page_bytes)?;
            if metadata.key() != entry.key() {
                return Err(PageStoreError::Corruption(
                    "page-map reference names the wrong logical page",
                ));
            }
            if metadata.location().checksum() != location.checksum() {
                return Err(PageStoreError::Corruption(
                    "page-map reference expected checksum mismatch",
                ));
            }
            if !self.dependencies.frontiers_cover(metadata.dependencies()) {
                return Err(PageStoreError::DependenciesNotDurable(entry.key()));
            }
            selected.insert(entry.key(), location);
        }

        let image_sync = sync_file_all(&state.images, self.sync_class);
        if let Err(source) = image_sync {
            state.fenced = true;
            return Err(PageStoreError::Io {
                operation: PageStoreOperation::SyncImages,
                source,
            });
        }

        let image_file_end = state.end_offset;
        let (bytes, reference) =
            page_map::encode(id, self.store, self.page_bytes, image_file_end, entries)?;
        let path = map_path(&self.directory, id);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|source| {
                if source.kind() == io::ErrorKind::AlreadyExists {
                    PageStoreError::MapExists(id)
                } else {
                    state.fenced = true;
                    PageStoreError::Io {
                        operation: PageStoreOperation::WriteMap,
                        source,
                    }
                }
            })?;
        let map_write = file
            .write_all(&bytes)
            .and_then(|()| file.sync_all())
            .and_then(|()| fsync_dir(&self.directory));
        if let Err(source) = map_write {
            state.fenced = true;
            return Err(PageStoreError::Io {
                operation: PageStoreOperation::SyncMap,
                source,
            });
        }
        Ok(PageMap::from_validated(
            reference,
            self.store,
            self.page_bytes,
            image_file_end,
            selected,
        ))
    }

    /// Read one page through an immutable selected map.
    ///
    /// The working overlay is deliberately ignored so a caller can prove what a
    /// published map resolves to.
    pub fn read_mapped_page(
        &self,
        map: &PageMap,
        key: PageKey,
        destination: &mut [u8],
    ) -> Result<PageImageMetadata, PageStoreError> {
        if map.page_bytes() != self.page_bytes {
            return Err(PageStoreError::Corruption(
                "page-map page size disagrees with the page store",
            ));
        }
        let location = map.require(key)?;
        let (metadata, payload) = self.read_frame(location.offset())?;
        if metadata.key() != key {
            return Err(PageStoreError::Corruption(
                "page-map reference names the wrong logical page",
            ));
        }
        if metadata.location().checksum() != location.checksum() {
            return Err(PageStoreError::Corruption(
                "page-map reference expected checksum mismatch",
            ));
        }
        copy_payload(payload.as_slice(), destination)?;
        Ok(metadata)
    }

    fn read_frame(&self, offset: u64) -> Result<(PageImageMetadata, Vec<u8>), PageStoreError> {
        let slot = page_image_bytes(self.page_bytes).ok_or(PageStoreError::Exhausted)?;
        let mut state = self.state.lock().map_err(|_| PageStoreError::Poisoned)?;
        let mut frame = vec![0u8; usize::try_from(slot).map_err(|_| PageStoreError::Exhausted)?];
        read_at(&mut state.images, offset, &mut frame)?;
        let (metadata, payload) = decode_image(&frame, self.store, self.page_bytes)?;
        Ok((metadata, payload.to_vec()))
    }
}

impl PageIo for PersistentPageIo {
    fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()> {
        let offset = self.placement(key).map_err(page_store_to_io)?.offset();
        let (metadata, payload) = self.read_frame(offset).map_err(page_store_to_io)?;
        if metadata.key() != key {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "page image does not belong to the requested logical page",
            ));
        }
        copy_payload(&payload, destination).map_err(page_store_to_io)
    }

    fn write_page(&self, key: PageKey, source: &[u8]) -> io::Result<()> {
        self.write_image(key, source)
            .map(|_| ())
            .map_err(page_store_to_io)
    }
}

fn validate_page_bytes(page_bytes: u32) -> Result<(), PageStoreError> {
    if page_bytes == 0 {
        return Err(PageStoreError::InvalidInput(
            "page-store page size must be nonzero",
        ));
    }
    let slot = page_image_bytes(page_bytes).ok_or(PageStoreError::Exhausted)?;
    if usize::try_from(slot).is_err() {
        return Err(PageStoreError::Exhausted);
    }
    Ok(())
}

fn map_path(directory: &Path, id: PageMapId) -> PathBuf {
    directory.join(format!("page-map-{:016x}.map", id.get()))
}

fn read_exact_file(path: &Path, expected_len: u64) -> Result<Vec<u8>, PageStoreError> {
    let mut file = File::open(path).map_err(|source| PageStoreError::Io {
        operation: PageStoreOperation::Open,
        source,
    })?;
    let actual = file
        .metadata()
        .map_err(|source| PageStoreError::Io {
            operation: PageStoreOperation::Open,
            source,
        })?
        .len();
    if actual != expected_len {
        return Err(PageStoreError::Corruption(
            "page-map file length does not match its reference",
        ));
    }
    let mut bytes =
        vec![0u8; usize::try_from(expected_len).map_err(|_| PageStoreError::Exhausted)?];
    file.read_exact(&mut bytes)
        .map_err(|source| PageStoreError::Io {
            operation: PageStoreOperation::Read,
            source,
        })?;
    Ok(bytes)
}

fn read_at(file: &mut File, offset: u64, destination: &mut [u8]) -> Result<(), PageStoreError> {
    file.seek(SeekFrom::Start(offset))
        .and_then(|_| file.read_exact(destination))
        .map_err(|source| PageStoreError::Io {
            operation: PageStoreOperation::Read,
            source,
        })
}

fn copy_payload(payload: &[u8], destination: &mut [u8]) -> Result<(), PageStoreError> {
    if destination.len() != payload.len() {
        return Err(PageStoreError::InvalidInput(
            "page-image destination length does not match the configured page size",
        ));
    }
    destination.copy_from_slice(payload);
    Ok(())
}

/// Frame checksum is the trailer of an encoded image.
fn image_frame_checksum(frame: &[u8]) -> u32 {
    let trailer = frame.len() - 4;
    u32::from_le_bytes(
        frame[trailer..]
            .try_into()
            .expect("encoded page-image frame has a trailer"),
    )
}

fn page_store_to_io(error: PageStoreError) -> io::Error {
    let kind = match &error {
        PageStoreError::MissingPage(_) => io::ErrorKind::NotFound,
        PageStoreError::DependenciesNotDurable(_) => io::ErrorKind::WouldBlock,
        PageStoreError::Corruption(_) => io::ErrorKind::InvalidData,
        PageStoreError::Fenced | PageStoreError::Poisoned => io::ErrorKind::Other,
        PageStoreError::InvalidInput(_)
        | PageStoreError::MapExists(_)
        | PageStoreError::Exhausted
        | PageStoreError::Io { .. } => io::ErrorKind::Other,
    };
    io::Error::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::ids::test_incarnation;
    use crate::vnext::{BufferPool, Lsn, PageDependencies, PageId, StorageObjectId, VersionId};

    const N: u32 = 64;

    fn key(page: u64) -> PageKey {
        PageKey::new(StorageObjectId::new(1), PageId::new(page))
    }

    fn page(value: u8) -> Vec<u8> {
        vec![value; N as usize]
    }

    fn map_id(value: u64) -> PageMapId {
        PageMapId::new(value).expect("nonzero map identity")
    }

    fn create(directory: &Path) -> PersistentPageIo {
        PersistentPageIo::create(directory, test_incarnation(1), N, SyncClass::KernelBarrier)
            .expect("creates")
    }

    fn arena_len(directory: &Path) -> u64 {
        std::fs::metadata(directory.join(IMAGE_FILE_NAME))
            .expect("arena metadata")
            .len()
    }

    fn append_bytes(directory: &Path, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(directory.join(IMAGE_FILE_NAME))
            .expect("opens arena");
        file.write_all(bytes).expect("appends");
        file.sync_all().expect("syncs");
    }

    #[test]
    fn out_of_place_rewrite_keeps_prior_maps_resolving_their_own_image() {
        let directory = tempfile::tempdir().expect("tempdir");
        let io = create(directory.path());
        let first = io.write_image(key(1), &page(0x11)).expect("writes");
        let map1 = io
            .write_map(map_id(1), &[PageMapEntry::new(key(1), first)])
            .expect("map 1");
        let second = io.write_image(key(1), &page(0x22)).expect("rewrites");
        assert_ne!(first.offset(), second.offset());
        let map2 = io
            .write_map(map_id(2), &[PageMapEntry::new(key(1), second)])
            .expect("map 2");

        let mut buffer = [0u8; N as usize];
        io.read_mapped_page(&map1, key(1), &mut buffer)
            .expect("map 1 read");
        assert_eq!(buffer.as_slice(), page(0x11));
        io.read_mapped_page(&map2, key(1), &mut buffer)
            .expect("map 2 read");
        assert_eq!(buffer.as_slice(), page(0x22));
        assert_eq!(io.placement(key(1)).expect("placement"), second);
        assert!(matches!(
            io.read_mapped_page(&map1, key(2), &mut buffer),
            Err(PageStoreError::MissingPage(_))
        ));
    }

    #[test]
    fn two_reopens_preserve_placement_and_continue_after_the_complete_prefix() {
        let directory = tempfile::tempdir().expect("tempdir");
        let first;
        let second;
        let mut reference;
        {
            let io = create(directory.path());
            first = io.write_image(key(1), &page(0x11)).expect("writes");
            second = io.write_image(key(2), &page(0x22)).expect("writes");
            reference = io
                .write_map(
                    map_id(1),
                    &[
                        PageMapEntry::new(key(1), first),
                        PageMapEntry::new(key(2), second),
                    ],
                )
                .expect("map")
                .reference();
        }
        let complete = arena_len(directory.path());

        // First reopen: read, then keep using the store and publish another map.
        {
            let (io, map) = PersistentPageIo::open(
                directory.path(),
                test_incarnation(1),
                reference,
                SyncClass::KernelBarrier,
            )
            .expect("first open");
            let mut buffer = [0u8; N as usize];
            io.read_mapped_page(&map, key(1), &mut buffer)
                .expect("read");
            assert_eq!(buffer.as_slice(), page(0x11));
            assert_eq!(io.placement(key(1)).expect("placement"), first);
            assert_eq!(io.placement(key(2)).expect("placement"), second);
            let third = io.write_image(key(3), &page(0x33)).expect("appends");
            assert_eq!(third.offset(), complete);
            reference = io
                .write_map(
                    map_id(2),
                    &[
                        PageMapEntry::new(key(1), first),
                        PageMapEntry::new(key(2), second),
                        PageMapEntry::new(key(3), third),
                    ],
                )
                .expect("second map")
                .reference();
        }

        // Two further reopens of the recovered image stay valid.
        let mut buffer = [0u8; N as usize];
        for _ in 0..2 {
            let (io, map) = PersistentPageIo::open(
                directory.path(),
                test_incarnation(1),
                reference,
                SyncClass::KernelBarrier,
            )
            .expect("reopen");
            assert_eq!(map.len(), 3);
            io.read_mapped_page(&map, key(3), &mut buffer)
                .expect("third page");
            assert_eq!(buffer.as_slice(), page(0x33));
            assert_eq!(io.placement(key(3)).expect("placement").offset(), complete);
        }

        // An older selected map still resolves its own images.
        let (io, old) = PersistentPageIo::open(
            directory.path(),
            test_incarnation(1),
            PageMapRef::new(
                map_id(1),
                std::fs::metadata(
                    directory
                        .path()
                        .join(format!("page-map-{:016x}.map", map_id(1).get())),
                )
                .expect("map metadata")
                .len(),
                map_checksum(directory.path(), map_id(1)),
            ),
            SyncClass::KernelBarrier,
        )
        .expect("older map reopens");
        assert_eq!(old.len(), 2);
        io.read_mapped_page(&old, key(2), &mut buffer)
            .expect("second page");
        assert_eq!(buffer.as_slice(), page(0x22));
    }

    fn map_checksum(directory: &Path, id: PageMapId) -> u32 {
        let bytes = std::fs::read(directory.join(format!("page-map-{:016x}.map", id.get())))
            .expect("map file");
        let trailer = bytes.len() - 4;
        u32::from_le_bytes(bytes[trailer..].try_into().expect("trailer"))
    }

    #[test]
    fn unreferenced_incomplete_final_append_is_repaired_on_open() {
        let directory = tempfile::tempdir().expect("tempdir");
        let reference;
        {
            let io = create(directory.path());
            let location = io.write_image(key(1), &page(0x11)).expect("writes");
            reference = io
                .write_map(map_id(1), &[PageMapEntry::new(key(1), location)])
                .expect("map")
                .reference();
        }
        let complete = arena_len(directory.path());
        // A torn append shorter than even a header.
        append_bytes(directory.path(), &[0u8; 10]);
        assert_eq!(arena_len(directory.path()), complete + 10);
        {
            let (io, map) = PersistentPageIo::open(
                directory.path(),
                test_incarnation(1),
                reference,
                SyncClass::KernelBarrier,
            )
            .expect("reopen repairs the torn tail");
            assert_eq!(arena_len(directory.path()), complete);
            let mut buffer = [0u8; N as usize];
            io.read_mapped_page(&map, key(1), &mut buffer)
                .expect("read");
            assert_eq!(buffer.as_slice(), page(0x11));
        }

        // A well-framed header whose payload and trailer never landed is the
        // other repairable shape.
        let slot = crate::vnext::page_image::page_image_bytes(N).expect("slot");
        let frame = crate::vnext::page_image::encode_image(
            test_incarnation(1),
            key(9),
            complete,
            N,
            PageDependencies::none(),
            &page(0x99),
        )
        .expect("encodes");
        append_bytes(directory.path(), &frame[..100]);
        assert!(arena_len(directory.path()) < complete + slot);
        let (io, map) = PersistentPageIo::open(
            directory.path(),
            test_incarnation(1),
            reference,
            SyncClass::KernelBarrier,
        )
        .expect("reopen repairs the incomplete frame");
        assert_eq!(arena_len(directory.path()), complete);
        let mut buffer = [0u8; N as usize];
        io.read_mapped_page(&map, key(1), &mut buffer)
            .expect("read");
        assert_eq!(buffer.as_slice(), page(0x11));
        let appended = io.write_image(key(9), &page(0x99)).expect("appends");
        assert_eq!(appended.offset(), complete);
    }

    #[test]
    fn dangling_selected_reference_fails_closed_without_truncating_the_arena() {
        let directory = tempfile::tempdir().expect("tempdir");
        let complete = {
            let io = create(directory.path());
            let location = io.write_image(key(1), &page(0x11)).expect("writes");
            io.write_map(map_id(1), &[PageMapEntry::new(key(1), location)])
                .expect("map");
            arena_len(directory.path())
        };
        let frame = crate::vnext::page_image::encode_image(
            test_incarnation(1),
            key(2),
            complete,
            N,
            PageDependencies::none(),
            &page(0x22),
        )
        .expect("encodes");
        append_bytes(directory.path(), &frame[..100]);

        // A map that names the incomplete slot cannot come from write_map, which
        // validates references; craft it directly to prove open refuses it.
        let (bytes, reference) = crate::vnext::page_map::encode(
            map_id(7),
            test_incarnation(1),
            N,
            complete,
            &[PageMapEntry::new(
                key(2),
                super::ImageLocation::new(complete, 0),
            )],
        )
        .expect("crafted map encodes");
        std::fs::write(
            directory
                .path()
                .join(format!("page-map-{:016x}.map", map_id(7).get())),
            &bytes,
        )
        .expect("writes map");
        let corrupted = arena_len(directory.path());
        assert!(matches!(
            PersistentPageIo::open(
                directory.path(),
                test_incarnation(1),
                reference,
                SyncClass::KernelBarrier,
            ),
            Err(PageStoreError::Corruption(_))
        ));
        assert_eq!(
            arena_len(directory.path()),
            corrupted,
            "a selected dangling reference must not be repaired by truncation"
        );
    }

    #[test]
    fn foreign_store_and_unmatched_image_references_fail_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let location;
        let reference;
        {
            let io = create(directory.path());
            location = io.write_image(key(1), &page(0x11)).expect("writes");
            reference = io
                .write_map(map_id(1), &[PageMapEntry::new(key(1), location)])
                .expect("map")
                .reference();
        }
        // Foreign incarnation.
        assert!(matches!(
            PersistentPageIo::open(
                directory.path(),
                test_incarnation(2),
                reference,
                SyncClass::KernelBarrier,
            ),
            Err(PageStoreError::Corruption(_))
        ));
        // A map that names a real slot with a different expected checksum.
        let (bytes, foreign_reference) = crate::vnext::page_map::encode(
            map_id(2),
            test_incarnation(1),
            N,
            arena_len(directory.path()),
            &[PageMapEntry::new(
                key(1),
                super::ImageLocation::new(location.offset(), location.checksum() ^ 0xffff),
            )],
        )
        .expect("crafted map encodes");
        std::fs::write(
            directory
                .path()
                .join(format!("page-map-{:016x}.map", map_id(2).get())),
            &bytes,
        )
        .expect("writes map");
        assert!(matches!(
            PersistentPageIo::open(
                directory.path(),
                test_incarnation(1),
                foreign_reference,
                SyncClass::KernelBarrier,
            ),
            Err(PageStoreError::Corruption(_))
        ));
    }

    #[test]
    fn publication_rejects_an_image_that_belongs_to_another_key() {
        let directory = tempfile::tempdir().expect("tempdir");
        let io = create(directory.path());
        let location = io.write_image(key(1), &page(0x11)).expect("writes");
        // Control: the matching key and checksum publishes.
        io.write_map(map_id(1), &[PageMapEntry::new(key(1), location)])
            .expect("matching placement publishes");
        assert!(matches!(
            io.write_map(map_id(2), &[PageMapEntry::new(key(2), location)]),
            Err(PageStoreError::Corruption(_))
        ));
        assert!(matches!(
            io.write_map(
                map_id(3),
                &[PageMapEntry::new(
                    key(1),
                    super::ImageLocation::new(location.offset(), location.checksum() ^ 1),
                )],
            ),
            Err(PageStoreError::Corruption(_))
        ));
    }

    #[test]
    fn map_identity_reuse_is_refused() {
        let directory = tempfile::tempdir().expect("tempdir");
        let io = create(directory.path());
        let location = io.write_image(key(1), &page(0x11)).expect("writes");
        io.write_map(map_id(1), &[PageMapEntry::new(key(1), location)])
            .expect("first map");
        assert!(matches!(
            io.write_map(map_id(1), &[PageMapEntry::new(key(1), location)]),
            Err(PageStoreError::MapExists(_))
        ));
    }

    #[test]
    fn dependencies_gate_publication_and_reopen_restores_requirements() {
        let directory = tempfile::tempdir().expect("tempdir");
        let required = PageDependencies::new(Lsn::new(5), Some(VersionId::new(3)));
        let location;
        let reference;
        {
            let io = create(directory.path());
            io.dependencies()
                .merge(key(1), required)
                .expect("requirement recorded");
            assert!(matches!(
                io.write_image(key(1), &page(0x11)),
                Err(PageStoreError::DependenciesNotDurable(_))
            ));
            io.dependencies().advance_wal(Lsn::new(5));
            assert!(matches!(
                io.write_image(key(1), &page(0x11)),
                Err(PageStoreError::DependenciesNotDurable(_))
            ));
            io.dependencies().advance_undo(VersionId::new(3));
            location = io.write_image(key(1), &page(0x11)).expect("writes");
            reference = io
                .write_map(map_id(1), &[PageMapEntry::new(key(1), location)])
                .expect("map")
                .reference();
        }
        let (io, map) = PersistentPageIo::open(
            directory.path(),
            test_incarnation(1),
            reference,
            SyncClass::KernelBarrier,
        )
        .expect("reopen");
        // Requirements travel with the image, and no durable frontier is
        // advanced by reopening page metadata.
        assert_eq!(
            io.dependencies()
                .requirements(key(1))
                .expect("requirements"),
            required
        );
        assert_eq!(io.dependencies().durable_wal(), Lsn::new(0));
        assert_eq!(io.dependencies().durable_undo(), None);
        assert!(matches!(
            io.write_image(key(1), &page(0x22)),
            Err(PageStoreError::DependenciesNotDurable(_))
        ));
        assert!(matches!(
            io.write_map(map_id(2), &[PageMapEntry::new(key(1), location)]),
            Err(PageStoreError::DependenciesNotDurable(_))
        ));
        io.dependencies().advance_wal(Lsn::new(5));
        io.dependencies().advance_undo(VersionId::new(3));
        let appended = io.write_image(key(1), &page(0x22)).expect("writes");
        io.write_map(map_id(2), &[PageMapEntry::new(key(1), appended)])
            .expect("map");
        let mut buffer = [0u8; N as usize];
        io.read_mapped_page(&map, key(1), &mut buffer)
            .expect("read");
        assert_eq!(buffer.as_slice(), page(0x11));
    }

    #[test]
    fn page_io_surface_supports_dirty_eviction_and_reload() {
        let directory = tempfile::tempdir().expect("tempdir");
        let io = Arc::new(create(directory.path()));
        let pool = BufferPool::new(2, N as usize, io.clone()).expect("pool");

        // Three pages through a two-frame pool force eviction, which is what
        // routes a dirty buffer page through the persistent store.
        for (index, value) in [(1u64, 0x11u8), (2, 0x22), (3, 0x33)] {
            let guard = pool
                .create_page(key(index), &page(value))
                .expect("creates page");
            drop(guard);
        }
        assert!(
            arena_len(directory.path()) > IMAGE_FILE_HEADER_SIZE as u64,
            "dirty eviction must materialize at least one image"
        );

        // Every page must read back intact, loading evicted images from the arena.
        let mut buffer = [0u8; N as usize];
        for (index, value) in [(1u64, 0x11u8), (2, 0x22), (3, 0x33)] {
            let guard = pool.pin(key(index)).expect("reloads page");
            {
                let read = guard.read().expect("read latch");
                buffer.copy_from_slice(&read);
            }
            assert_eq!(
                buffer.as_slice(),
                page(value),
                "page {index} reloads intact"
            );
        }

        // A committed map over the materialized placements resolves them.
        let entries: Vec<PageMapEntry> = (1..=3)
            .map(|index| {
                PageMapEntry::new(key(index), io.placement(key(index)).expect("placement"))
            })
            .collect();
        let map = io.write_map(map_id(1), &entries).expect("map over arena");
        assert_eq!(map.len(), 3);
        io.read_mapped_page(&map, key(2), &mut buffer)
            .expect("mapped read");
        assert_eq!(buffer.as_slice(), page(0x22));
    }
}
