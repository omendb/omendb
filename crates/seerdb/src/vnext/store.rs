//! Durable store identity and exclusive writable directory ownership.
//!
//! Every persistent vNext component is bound to one [`StoreDirectory`]. The
//! directory owns two things that no component handle can provide on its own:
//!
//! - an OS-held exclusive lock, so a second process (or a second independent
//!   handle in this process) cannot write the same store; and
//! - an immutable, checksummed incarnation that binds WAL, undo and page
//!   components to the same physical store, so a foreign file cannot be adopted
//!   merely because its own framing is valid.
//!
//! This is ownership and binding only. It is not recovery authority, it does not
//! select a checkpoint, and a successfully opened store is not a certificate
//! that every component exists or that current pages may be trusted.

use super::StoreIncarnation;
use durable_fs::{atomic_write, fsync_dir_chain, sync_file_all};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

pub(super) const LOCK_FILE_NAME: &str = "seerdb.lock";
pub(super) const IDENTITY_FILE_NAME: &str = "store.identity";
pub(super) const WAL_DIRECTORY_NAME: &str = "wal";
pub(super) const UNDO_FILE_NAME: &str = "undo.log";
pub(super) const IMAGE_FILE_NAME: &str = "page-images.dat";

const STORE_IDENTITY_MAGIC: [u8; 4] = *b"OMSI";

/// Common size of every 32-byte incarnation-bound container header.
pub(super) const BOUND_HEADER_BYTES: usize = 32;
/// Version byte shared by every 32-byte incarnation-bound container header.
pub(super) const BOUND_HEADER_VERSION: u8 = 1;
const BOUND_HEADER_CHECKSUM_OFFSET: usize = 28;

/// One mutable persistent component under store ownership.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StoreComponent {
    Wal,
    Undo,
    Pages,
}

impl StoreComponent {
    const fn claim_bit(self) -> u8 {
        match self {
            Self::Wal => 0b001,
            Self::Undo => 0b010,
            Self::Pages => 0b100,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Wal => "WAL",
            Self::Undo => "undo",
            Self::Pages => "page",
        }
    }
}

/// Failure while establishing or validating store ownership.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("store directory is already owned by another writer")]
    Busy,
    #[error("another {} component handle already owns mutable state", .0.name())]
    ComponentBusy(StoreComponent),
    #[error("store directory has no {LOCK_FILE_NAME}")]
    MissingLock,
    #[error("store directory has no {IDENTITY_FILE_NAME}")]
    MissingIdentity,
    #[error("store directory already contains a store identity")]
    AlreadyExists,
    #[error("store directory contains foreign contents")]
    NotEmpty,
    #[error("durable store ownership is not supported on this platform")]
    UnsupportedPlatform,
    #[error("invalid store path: {0}")]
    InvalidPath(&'static str),
    #[error("store component {path:?} is corrupt: {reason}")]
    Corruption { path: PathBuf, reason: &'static str },
    #[error("store component {path:?} belongs to incarnation {actual:?}, expected {expected:?}")]
    ForeignIncarnation {
        path: PathBuf,
        expected: StoreIncarnation,
        actual: StoreIncarnation,
    },
    #[error("generated store incarnation was all zeroes")]
    InvalidRandomIncarnation,
    #[error("failed to generate a store incarnation: {0}")]
    Random(#[source] io::Error),
    #[error("store {operation} failed at {path:?}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Shared exclusive ownership of one store directory.
struct StoreOwnership {
    directory: PathBuf,
    incarnation: StoreIncarnation,
    claimed: AtomicU8,
    /// Held for the lifetime of the ownership capability. Never cloned,
    /// exposed, or replaced while owned.
    _lock: File,
}

/// Exclusive writable ownership of one durable store directory.
///
/// The capability is not `Clone`. Dropping it releases the writer lock only
/// after every component lease that shares it has been dropped.
pub struct StoreDirectory {
    inner: Arc<StoreOwnership>,
}
/// Exclusive claim on one mutable component under an owned directory.
///
/// A second constructor for the same component fails rather than letting two
/// independent indexes or overlays diverge, and holding a lease keeps the
/// directory lock alive after the caller drops the root capability.
pub(super) struct ComponentLease {
    owner: Arc<StoreOwnership>,
    component: StoreComponent,
}

impl ComponentLease {
    pub(super) fn directory(&self) -> &Path {
        &self.owner.directory
    }

    pub(super) fn incarnation(&self) -> StoreIncarnation {
        self.owner.incarnation
    }

    pub(super) fn wal_directory(&self) -> PathBuf {
        self.owner.directory.join(WAL_DIRECTORY_NAME)
    }

    pub(super) fn undo_path(&self) -> PathBuf {
        self.owner.directory.join(UNDO_FILE_NAME)
    }

    pub(super) fn image_path(&self) -> PathBuf {
        self.owner.directory.join(IMAGE_FILE_NAME)
    }

    pub(super) fn map_path(&self, id: u64) -> PathBuf {
        self.owner.directory.join(format!("page-map-{id:016x}.map"))
    }
}

impl Drop for ComponentLease {
    fn drop(&mut self) {
        self.owner
            .claimed
            .fetch_and(!self.component.claim_bit(), Ordering::AcqRel);
    }
}

impl StoreDirectory {
    /// Create a new store directory and publish its immutable identity.
    ///
    /// An existing directory is accepted only when it contains nothing but the
    /// stable lock file, so a concurrent creator cannot overwrite an identity.
    /// This never replaces an existing identity, even a corrupt one. A second
    /// creator contending for the same lock reports [`StoreError::Busy`].
    pub fn create(directory: &Path) -> Result<Self, StoreError> {
        require_supported_platform()?;
        if directory.as_os_str().is_empty() {
            return Err(StoreError::InvalidPath("store directory is empty"));
        }
        if !directory.exists() {
            std::fs::create_dir_all(directory).map_err(|source| StoreError::Io {
                operation: "create store directory",
                path: directory.to_path_buf(),
                source,
            })?;
        }
        let directory = canonical_directory(directory)?;
        let lock = open_lock_file(&directory)?;
        acquire_lock(&lock, &directory)?;
        // Recheck under the lock: a concurrent creator may have published an
        // identity between the existence check and ownership, and foreign
        // contents must never be adopted.
        require_only_lock_file(&directory)?;

        let incarnation = generate_incarnation()?;
        let identity = encode_identity(incarnation);
        let path = directory.join(IDENTITY_FILE_NAME);
        atomic_write(&path, &identity).map_err(|source| StoreError::Io {
            operation: "publish store identity",
            path: path.clone(),
            source,
        })?;
        fsync_dir_chain(&directory).map_err(|source| StoreError::Io {
            operation: "synchronize store directory chain",
            path: directory.clone(),
            source,
        })?;
        Ok(Self {
            inner: Arc::new(StoreOwnership {
                directory,
                incarnation,
                claimed: AtomicU8::new(0),
                _lock: lock,
            }),
        })
    }

    /// Acquire exclusive ownership of an existing store and validate its identity.
    ///
    /// Returns directory ownership and a validated incarnation, not a recovered
    /// database and not a statement that any component exists. Missing lock and
    /// missing identity fail closed; neither is created here.
    pub fn open(directory: &Path, expected: Option<StoreIncarnation>) -> Result<Self, StoreError> {
        require_supported_platform()?;
        let directory = canonical_directory(directory)?;
        let lock_path = directory.join(LOCK_FILE_NAME);
        if !lock_path.is_file() {
            return Err(StoreError::MissingLock);
        }
        let lock = open_lock_file(&directory)?;
        acquire_lock(&lock, &directory)?;

        let identity_path = directory.join(IDENTITY_FILE_NAME);
        if !identity_path.is_file() {
            return Err(StoreError::MissingIdentity);
        }
        let bytes = std::fs::read(&identity_path).map_err(|source| StoreError::Io {
            operation: "read store identity",
            path: identity_path.clone(),
            source,
        })?;
        let incarnation = decode_identity(&bytes, &identity_path)?;
        if let Some(expected) = expected
            && expected != incarnation
        {
            return Err(StoreError::ForeignIncarnation {
                path: identity_path,
                expected,
                actual: incarnation,
            });
        }
        let file = File::open(&identity_path).map_err(|source| StoreError::Io {
            operation: "open store identity",
            path: identity_path.clone(),
            source,
        })?;
        sync_file_all(&file, durable_fs::SyncClass::DeviceBarrier).map_err(|source| {
            StoreError::Io {
                operation: "synchronize store identity",
                path: identity_path,
                source,
            }
        })?;
        fsync_dir_chain(&directory).map_err(|source| StoreError::Io {
            operation: "synchronize store directory chain",
            path: directory.clone(),
            source,
        })?;
        Ok(Self {
            inner: Arc::new(StoreOwnership {
                directory,
                incarnation,
                claimed: AtomicU8::new(0),
                _lock: lock,
            }),
        })
    }

    /// Return the canonical owned directory.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.inner.directory
    }

    /// Return the validated store incarnation.
    #[must_use]
    pub fn incarnation(&self) -> StoreIncarnation {
        self.inner.incarnation
    }

    /// Claim exclusive mutable state for one persistent component.
    pub(super) fn claim(&self, component: StoreComponent) -> Result<ComponentLease, StoreError> {
        let bit = component.claim_bit();
        let mut observed = self.inner.claimed.load(Ordering::Acquire);
        loop {
            if observed & bit != 0 {
                return Err(StoreError::ComponentBusy(component));
            }
            match self.inner.claimed.compare_exchange_weak(
                observed,
                observed | bit,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ComponentLease {
                        owner: Arc::clone(&self.inner),
                        component,
                    });
                }
                Err(actual) => observed = actual,
            }
        }
    }
}

/// Encode the common 32-byte incarnation-bound container header.
///
/// The layout is shared by `store.identity` and the undo container:
/// magic, version, flags, size, 16-byte incarnation, reserved u32, CRC32C.
pub(super) fn encode_bound_header(
    magic: [u8; 4],
    incarnation: StoreIncarnation,
) -> [u8; BOUND_HEADER_BYTES] {
    let mut bytes = [0u8; BOUND_HEADER_BYTES];
    bytes[..4].copy_from_slice(&magic);
    bytes[4] = BOUND_HEADER_VERSION;
    bytes[6..8].copy_from_slice(&(BOUND_HEADER_BYTES as u16).to_le_bytes());
    bytes[8..24].copy_from_slice(incarnation.as_bytes());
    let checksum = crc32c::crc32c(&bytes[..BOUND_HEADER_CHECKSUM_OFFSET]);
    bytes[BOUND_HEADER_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

/// Decode and validate a 32-byte incarnation-bound container header.
///
/// Validation order is framing (length, magic, version, flags, size,
/// reserved), then CRC, then a nonzero incarnation. Any deviation fails closed.
pub(super) fn decode_bound_header(
    bytes: &[u8],
    magic: [u8; 4],
    path: &Path,
) -> Result<StoreIncarnation, StoreError> {
    let corruption = |reason| StoreError::Corruption {
        path: path.to_path_buf(),
        reason,
    };
    if bytes.len() != BOUND_HEADER_BYTES {
        return Err(corruption("container header must be exactly 32 bytes"));
    }
    if bytes[..4] != magic {
        return Err(corruption("invalid container header magic"));
    }
    if bytes[4] != BOUND_HEADER_VERSION {
        return Err(corruption("unsupported container header version"));
    }
    if bytes[5] != 0 {
        return Err(corruption("unsupported container header flags"));
    }
    if u16::from_le_bytes([bytes[6], bytes[7]]) != BOUND_HEADER_BYTES as u16 {
        return Err(corruption("invalid container header size field"));
    }
    if bytes[24..BOUND_HEADER_CHECKSUM_OFFSET]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(corruption("nonzero container header reserved field"));
    }
    let stored = u32::from_le_bytes(
        bytes[BOUND_HEADER_CHECKSUM_OFFSET..]
            .try_into()
            .map_err(|_| corruption("invalid container header checksum field"))?,
    );
    if crc32c::crc32c(&bytes[..BOUND_HEADER_CHECKSUM_OFFSET]) != stored {
        return Err(corruption("container header checksum mismatch"));
    }
    let raw: [u8; 16] = bytes[8..24]
        .try_into()
        .map_err(|_| corruption("invalid container header incarnation field"))?;
    StoreIncarnation::from_bytes(raw)
        .ok_or_else(|| corruption("container header incarnation is all zeroes"))
}

/// Encode the fixed 32-byte store identity.
fn encode_identity(incarnation: StoreIncarnation) -> [u8; BOUND_HEADER_BYTES] {
    encode_bound_header(STORE_IDENTITY_MAGIC, incarnation)
}

/// Decode and validate a store identity, failing closed on any deviation.
fn decode_identity(bytes: &[u8], path: &Path) -> Result<StoreIncarnation, StoreError> {
    decode_bound_header(bytes, STORE_IDENTITY_MAGIC, path)
}

fn require_supported_platform() -> Result<(), StoreError> {
    if cfg!(unix) {
        Ok(())
    } else {
        // Directory durability barriers are Unix-only in `durable-fs`, so
        // presenting a successfully locked Windows store as durable would be a
        // lie. This is a temporary scope limit, not a locking limitation.
        Err(StoreError::UnsupportedPlatform)
    }
}

fn canonical_directory(directory: &Path) -> Result<PathBuf, StoreError> {
    if !directory.is_dir() {
        return Err(StoreError::InvalidPath(
            "store directory does not exist or is not a directory",
        ));
    }
    std::fs::canonicalize(directory).map_err(|source| StoreError::Io {
        operation: "canonicalize store directory",
        path: directory.to_path_buf(),
        source,
    })
}

/// Reject an existing directory that holds anything but the stable lock file.
fn require_only_lock_file(directory: &Path) -> Result<(), StoreError> {
    let entries = std::fs::read_dir(directory).map_err(|source| StoreError::Io {
        operation: "read store directory",
        path: directory.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| StoreError::Io {
            operation: "read store directory entry",
            path: directory.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        if name == LOCK_FILE_NAME {
            let metadata = entry.metadata().map_err(|source| StoreError::Io {
                operation: "inspect store lock file",
                path: entry.path(),
                source,
            })?;
            if !metadata.is_file() {
                return Err(StoreError::InvalidPath(
                    "store lock path is not a regular file",
                ));
            }
            continue;
        }
        if name == IDENTITY_FILE_NAME {
            return Err(StoreError::AlreadyExists);
        }
        return Err(StoreError::NotEmpty);
    }
    Ok(())
}

fn open_lock_file(directory: &Path) -> Result<File, StoreError> {
    let path = directory.join(LOCK_FILE_NAME);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(StoreError::InvalidPath(
                    "store lock path is not a regular file",
                ));
            }
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(StoreError::Io {
                operation: "inspect store lock file",
                path: path.clone(),
                source,
            });
        }
    }
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| StoreError::Io {
            operation: "open store lock file",
            path,
            source,
        })
}

fn acquire_lock(file: &File, directory: &Path) -> Result<(), StoreError> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => Err(StoreError::Busy),
        Err(std::fs::TryLockError::Error(source)) => Err(StoreError::Io {
            operation: "lock store directory",
            path: directory.to_path_buf(),
            source,
        }),
    }
}

fn generate_incarnation() -> Result<StoreIncarnation, StoreError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| StoreError::Random(io::Error::other(error)))?;
    StoreIncarnation::from_bytes(bytes).ok_or(StoreError::InvalidRandomIncarnation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::ids::test_incarnation;
    use crate::vnext::{PersistentPageIo, SegmentedFileLogDevice, SegmentedLogConfig, UndoStore};
    use durable_fs::SyncClass;
    use std::io::{BufRead, BufReader, Write as _};
    use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

    const CHILD_READY: &str = "SEERDB-STORE-CHILD-READY";
    const CHILD_TEST: &str = "vnext::store::tests::store_two_process_child";
    const CHILD_MODE: &str = "SEERDB_STORE_TEST_MODE";
    const CHILD_DIR: &str = "SEERDB_STORE_TEST_DIR";

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn custom_log_config() -> SegmentedLogConfig {
        SegmentedLogConfig {
            segment_bytes: 4096,
            sync_class: SyncClass::KernelBarrier,
        }
    }

    #[test]
    fn identity_golden_layout_and_crc_coverage() {
        let incarnation = test_incarnation(5);
        let bytes = encode_identity(incarnation);
        assert_eq!(bytes.len(), BOUND_HEADER_BYTES);
        assert_eq!(&bytes[..4], b"OMSI");
        assert_eq!(bytes[4], BOUND_HEADER_VERSION);
        assert_eq!(bytes[5], 0);
        assert_eq!(u16::from_le_bytes([bytes[6], bytes[7]]), 32);
        assert_eq!(&bytes[8..24], incarnation.as_bytes());
        assert_eq!(&bytes[24..28], &[0, 0, 0, 0]);
        assert_eq!(
            u32::from_le_bytes(bytes[28..32].try_into().expect("checksum")),
            crc32c::crc32c(&bytes[..28])
        );
        assert_eq!(
            decode_identity(&bytes, Path::new("store.identity")).expect("decodes"),
            incarnation
        );
    }

    #[test]
    fn identity_field_mutations_truncations_and_extra_bytes_fail() {
        let incarnation = test_incarnation(5);
        let bytes = encode_identity(incarnation);
        let path = Path::new("store.identity");
        let mutate = |offset: usize, value: u8| {
            let mut copy = bytes;
            copy[offset] = value;
            copy
        };
        for bad in [
            mutate(0, b'X'),
            mutate(4, 2),
            mutate(5, 1),
            mutate(6, 33),
            mutate(24, 1),
            mutate(28, bytes[28] ^ 1),
        ] {
            assert!(decode_identity(&bad, path).is_err());
        }
        let mut zero = bytes;
        zero[8..24].fill(0);
        let crc = crc32c::crc32c(&zero[..28]);
        zero[28..32].copy_from_slice(&crc.to_le_bytes());
        assert!(decode_identity(&zero, path).is_err());
        for cut in 0..BOUND_HEADER_BYTES {
            assert!(decode_identity(&bytes[..cut], path).is_err());
        }
        let mut extended = bytes.to_vec();
        extended.push(0);
        assert!(decode_identity(&extended, path).is_err());
    }

    #[test]
    fn create_publishes_exactly_one_identity_and_open_requires_it() {
        let directory = tempdir();
        let store = StoreDirectory::create(directory.path()).expect("create");
        let incarnation = store.incarnation();
        let identity = std::fs::read(directory.path().join(IDENTITY_FILE_NAME)).expect("read");
        assert_eq!(
            decode_identity(&identity, &directory.path().join(IDENTITY_FILE_NAME)).expect("decode"),
            incarnation
        );
        drop(store);
        let reopened = StoreDirectory::open(directory.path(), Some(incarnation)).expect("reopen");
        assert_eq!(reopened.incarnation(), incarnation);
    }

    #[test]
    fn second_handle_on_same_directory_reports_busy() {
        let directory = tempdir();
        let store = StoreDirectory::create(directory.path()).expect("create");
        assert!(matches!(
            StoreDirectory::create(directory.path()),
            Err(StoreError::Busy)
        ));
        assert!(matches!(
            StoreDirectory::open(directory.path(), None),
            Err(StoreError::Busy)
        ));
        drop(store);
        assert!(StoreDirectory::open(directory.path(), None).is_ok());
    }

    #[test]
    fn open_requires_an_existing_lock_and_identity() {
        let missing_lock = tempdir();
        let store = StoreDirectory::create(missing_lock.path()).expect("create");
        drop(store);
        std::fs::remove_file(missing_lock.path().join(LOCK_FILE_NAME)).expect("remove lock");
        assert!(matches!(
            StoreDirectory::open(missing_lock.path(), None),
            Err(StoreError::MissingLock)
        ));

        let missing_identity = tempdir();
        let store = StoreDirectory::create(missing_identity.path()).expect("create");
        drop(store);
        std::fs::remove_file(missing_identity.path().join(IDENTITY_FILE_NAME)).expect("remove id");
        assert!(matches!(
            StoreDirectory::open(missing_identity.path(), None),
            Err(StoreError::MissingIdentity)
        ));
        assert!(!missing_identity.path().join(IDENTITY_FILE_NAME).exists());
    }

    #[test]
    fn create_never_replaces_an_existing_identity() {
        let directory = tempdir();
        let store = StoreDirectory::create(directory.path()).expect("create");
        drop(store);
        let identity_path = directory.path().join(IDENTITY_FILE_NAME);
        let corrupt = b"not-a-valid-identity";
        std::fs::write(&identity_path, corrupt).expect("corrupt identity");
        assert!(matches!(
            StoreDirectory::create(directory.path()),
            Err(StoreError::AlreadyExists)
        ));
        assert_eq!(std::fs::read(&identity_path).expect("read"), corrupt);
    }

    #[test]
    fn create_rejects_foreign_contents_without_publishing_identity() {
        let directory = tempdir();
        std::fs::write(directory.path().join("notes.txt"), b"foreign").expect("foreign file");
        assert!(matches!(
            StoreDirectory::create(directory.path()),
            Err(StoreError::NotEmpty)
        ));
        assert!(!directory.path().join(IDENTITY_FILE_NAME).exists());
    }

    #[test]
    fn component_claims_are_exclusive_and_types_coexist() {
        let directory = tempdir();
        let store = StoreDirectory::create(directory.path()).expect("create");
        let wal = store.claim(StoreComponent::Wal).expect("wal");
        let undo = store.claim(StoreComponent::Undo).expect("undo");
        let pages = store.claim(StoreComponent::Pages).expect("pages");
        assert!(matches!(
            store.claim(StoreComponent::Wal),
            Err(StoreError::ComponentBusy(StoreComponent::Wal))
        ));
        assert!(matches!(
            store.claim(StoreComponent::Undo),
            Err(StoreError::ComponentBusy(StoreComponent::Undo))
        ));
        assert!(matches!(
            store.claim(StoreComponent::Pages),
            Err(StoreError::ComponentBusy(StoreComponent::Pages))
        ));
        drop((wal, undo, pages));
        store.claim(StoreComponent::Wal).expect("wal reclaim");
    }

    #[test]
    fn dropping_directory_while_a_lease_lives_keeps_the_writer_lock() {
        let directory = tempdir();
        let store = StoreDirectory::create(directory.path()).expect("create");
        let lease = store.claim(StoreComponent::Wal).expect("claim");
        drop(store);
        assert!(matches!(
            StoreDirectory::open(directory.path(), None),
            Err(StoreError::Busy)
        ));
        drop(lease);
        assert!(StoreDirectory::open(directory.path(), None).is_ok());
    }

    #[test]
    fn sharing_a_component_through_arc_retains_ownership() {
        let directory = tempdir();
        let store = StoreDirectory::create(directory.path()).expect("create");
        let io = std::sync::Arc::new(
            PersistentPageIo::create(&store, 512, SyncClass::KernelBarrier).expect("page store"),
        );
        drop(store);
        assert!(matches!(
            StoreDirectory::open(directory.path(), None),
            Err(StoreError::Busy)
        ));
        let clone = std::sync::Arc::clone(&io);
        drop(io);
        assert!(matches!(
            StoreDirectory::open(directory.path(), None),
            Err(StoreError::Busy)
        ));
        drop(clone);
        assert!(StoreDirectory::open(directory.path(), None).is_ok());
    }

    #[test]
    fn failed_constructor_releases_its_claim() {
        let directory = tempdir();
        let store = StoreDirectory::create(directory.path()).expect("create");
        // No WAL directory exists yet, so opening the WAL fails after claiming.
        assert!(SegmentedFileLogDevice::open(&store, custom_log_config()).is_err());
        let lease = store.claim(StoreComponent::Wal).expect("claim released");
        drop(lease);
    }

    #[test]
    fn different_components_bind_to_the_same_incarnation() {
        let directory = tempdir();
        let store = StoreDirectory::create(directory.path()).expect("create");
        let wal = SegmentedFileLogDevice::create(&store, custom_log_config()).expect("wal");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let pages = PersistentPageIo::create(&store, 512, SyncClass::KernelBarrier).expect("pages");
        let incarnation = store.incarnation();
        assert_eq!(wal.store(), incarnation);
        assert_eq!(undo.store(), incarnation);
        assert_eq!(pages.store(), incarnation);
    }

    #[test]
    fn corrupt_identity_is_never_repaired() {
        let directory = tempdir();
        let store = StoreDirectory::create(directory.path()).expect("create");
        drop(store);
        let identity_path = directory.path().join(IDENTITY_FILE_NAME);
        let mut bytes = std::fs::read(&identity_path).expect("read");
        bytes[3] ^= 0x01;
        std::fs::write(&identity_path, &bytes).expect("corrupt");
        assert!(matches!(
            StoreDirectory::open(directory.path(), None),
            Err(StoreError::Corruption { .. })
        ));
        assert_eq!(std::fs::read(&identity_path).expect("read"), bytes);
    }

    // --- two-process ownership tests -------------------------------------

    struct ChildProc {
        child: Child,
        stdin: ChildStdin,
        stdout: BufReader<ChildStdout>,
    }

    fn spawn_child(directory: &Path, mode: &str) -> ChildProc {
        let exe = std::env::current_exe().expect("current exe");
        let mut child = Command::new(exe)
            .arg("--exact")
            .arg(CHILD_TEST)
            .arg("--nocapture")
            .env(CHILD_MODE, mode)
            .env(CHILD_DIR, directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn child");
        let stdin = child.stdin.take().expect("child stdin");
        let stdout = BufReader::new(child.stdout.take().expect("child stdout"));
        ChildProc {
            child,
            stdin,
            stdout,
        }
    }

    fn wait_ready(child: &mut ChildProc) {
        let mut line = String::new();
        loop {
            line.clear();
            let read = child
                .stdout
                .read_line(&mut line)
                .expect("read child stdout");
            assert!(read > 0, "child exited before signaling readiness");
            if line.trim() == CHILD_READY {
                return;
            }
        }
    }

    fn release(child: &mut ChildProc) {
        writeln!(child.stdin, "go").expect("release child");
        child.stdin.flush().ok();
    }

    /// Subprocess body. Re-executed with `--exact` by the parent tests.
    #[test]
    fn store_two_process_child() {
        let Ok(mode) = std::env::var(CHILD_MODE) else {
            return;
        };
        let directory = std::path::PathBuf::from(std::env::var(CHILD_DIR).expect("child dir"));
        match mode.as_str() {
            "own" => {
                let _store = StoreDirectory::create(&directory).expect("child create");
                println!("{CHILD_READY}");
                wait_for_stdin();
            }
            "lease" => {
                let store = StoreDirectory::create(&directory).expect("child create");
                let lease = store.claim(StoreComponent::Wal).expect("child claim");
                drop(store);
                println!("{CHILD_READY}");
                wait_for_stdin();
                drop(lease);
            }
            "creator" => {
                println!("{CHILD_READY}");
                wait_for_stdin();
                match StoreDirectory::create(&directory) {
                    Ok(_) => {}
                    Err(StoreError::Busy | StoreError::AlreadyExists) => std::process::exit(2),
                    Err(other) => {
                        eprintln!("unexpected creator error: {other}");
                        std::process::exit(3);
                    }
                }
            }
            other => panic!("unknown child mode {other}"),
        }
    }

    fn wait_for_stdin() {
        let mut line = String::new();
        std::io::stdin()
            .read_line(&mut line)
            .expect("child release read");
    }

    #[test]
    fn child_ownership_contends_and_releases_on_exit() {
        let directory = tempdir();
        let mut child = spawn_child(directory.path(), "own");
        wait_ready(&mut child);
        assert!(matches!(
            StoreDirectory::create(directory.path()),
            Err(StoreError::Busy)
        ));
        assert!(matches!(
            StoreDirectory::open(directory.path(), None),
            Err(StoreError::Busy)
        ));
        release(&mut child);
        assert_eq!(child.child.wait().expect("child exits").code(), Some(0));
        let store = StoreDirectory::open(directory.path(), None).expect("parent acquires");
        assert!(StoreDirectory::open(directory.path(), None).is_err());
        drop(store);
    }

    #[test]
    fn kill_without_destructors_still_binds_the_same_incarnation() {
        let directory = tempdir();
        let mut child = spawn_child(directory.path(), "own");
        wait_ready(&mut child);
        let identity_path = directory.path().join(IDENTITY_FILE_NAME);
        let identity = std::fs::read(&identity_path).expect("identity");
        let expected = decode_identity(&identity, &identity_path).expect("decode");
        child.child.kill().expect("kill child");
        child.child.wait().expect("reap child");
        let store = StoreDirectory::open(directory.path(), Some(expected)).expect("acquire");
        assert_eq!(store.incarnation(), expected);
    }

    #[test]
    fn child_holding_only_a_component_lease_blocks_the_parent() {
        let directory = tempdir();
        let mut child = spawn_child(directory.path(), "lease");
        wait_ready(&mut child);
        assert!(matches!(
            StoreDirectory::open(directory.path(), None),
            Err(StoreError::Busy)
        ));
        release(&mut child);
        assert_eq!(child.child.wait().expect("child exits").code(), Some(0));
        assert!(StoreDirectory::open(directory.path(), None).is_ok());
    }

    #[test]
    fn two_racing_creators_publish_exactly_one_identity() {
        let directory = tempdir();
        let mut first = spawn_child(directory.path(), "creator");
        let mut second = spawn_child(directory.path(), "creator");
        wait_ready(&mut first);
        wait_ready(&mut second);
        release(&mut first);
        release(&mut second);
        let first_code = first.child.wait().expect("first exits").code();
        let second_code = second.child.wait().expect("second exits").code();
        let codes = [first_code, second_code];
        assert_eq!(
            codes.iter().filter(|code| **code == Some(0)).count(),
            1,
            "exactly one creator must succeed: {codes:?}"
        );
        assert_eq!(
            codes.iter().filter(|code| **code == Some(2)).count(),
            1,
            "the losing creator must report contention: {codes:?}"
        );
        let store = StoreDirectory::open(directory.path(), None).expect("identity exists");
        let identity = std::fs::read(directory.path().join(IDENTITY_FILE_NAME)).expect("read");
        assert_eq!(
            decode_identity(&identity, &directory.path().join(IDENTITY_FILE_NAME)).expect("decode"),
            store.incarnation()
        );
    }
}
