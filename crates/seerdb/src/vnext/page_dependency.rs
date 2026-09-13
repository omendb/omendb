//! In-process page dependency tracking and write-ahead materialization gate.
//!
//! This is not the persistent checkpoint envelope. It records conservative
//! per-logical-page WAL/undo requirements across buffer eviction/reload within
//! one runtime and wraps `PageIo` so a dirty image cannot be written before both
//! durability frontiers cover those requirements. Checkpoint publication later
//! persists equivalent metadata together with recovery authority.

use super::{Lsn, PageIo, PageKey, VersionId};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

const DEPENDENCY_SHARDS: usize = 64;

/// Write-ahead dependencies carried by one logical page image.
///
/// `Lsn(0)` means no WAL dependency beyond the initial frontier. Undo identity
/// zero is reserved, so `None` cleanly represents no referenced undo record.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PageDependencies {
    required_wal: Lsn,
    required_undo: Option<VersionId>,
}

impl Default for PageDependencies {
    fn default() -> Self {
        Self::none()
    }
}

impl PageDependencies {
    #[must_use]
    pub const fn none() -> Self {
        Self {
            required_wal: Lsn::new(0),
            required_undo: None,
        }
    }

    #[must_use]
    pub const fn new(required_wal: Lsn, required_undo: Option<VersionId>) -> Self {
        Self {
            required_wal,
            required_undo,
        }
    }

    #[must_use]
    pub const fn required_wal(self) -> Lsn {
        self.required_wal
    }

    #[must_use]
    pub const fn required_undo(self) -> Option<VersionId> {
        self.required_undo
    }

    /// Conservatively combine two requirements for one logical page.
    #[must_use]
    pub fn merged(self, other: Self) -> Self {
        let required_wal = self.required_wal.max(other.required_wal);
        let required_undo = match (self.required_undo, other.required_undo) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        };
        Self {
            required_wal,
            required_undo,
        }
    }

    #[must_use]
    fn is_satisfied_by(self, durable_wal: Lsn, durable_undo: u64) -> bool {
        durable_wal >= self.required_wal
            && self
                .required_undo
                .is_none_or(|required| durable_undo >= required.get())
    }
}

/// Durability requirements attached to one prepared page mutation.
///
/// Grouping the exact dependency table with the decision LSN keeps install
/// entry points concrete and makes it impossible to materialize an image
/// against a frontier belonging to a different attempt.
#[derive(Clone, Copy)]
pub struct PageMaterialization<'a> {
    dependencies: &'a PageDependencyTable,
    required_wal: Lsn,
}

impl<'a> PageMaterialization<'a> {
    #[must_use]
    pub const fn new(dependencies: &'a PageDependencyTable, required_wal: Lsn) -> Self {
        Self {
            dependencies,
            required_wal,
        }
    }

    #[must_use]
    pub const fn dependencies(self) -> &'a PageDependencyTable {
        self.dependencies
    }

    #[must_use]
    pub const fn required_wal(self) -> Lsn {
        self.required_wal
    }
}

/// Sharded logical-page requirements plus monotonic runtime durability frontiers.
pub struct PageDependencyTable {
    shards: [RwLock<HashMap<PageKey, PageDependencies>>; DEPENDENCY_SHARDS],
    durable_wal: AtomicU64,
    durable_undo: AtomicU64,
}

impl Default for PageDependencyTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PageDependencyTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            durable_wal: AtomicU64::new(0),
            durable_undo: AtomicU64::new(0),
        }
    }

    /// Merge requirements into one page without allowing either dependency to
    /// move backward.
    pub fn merge(
        &self,
        key: PageKey,
        required: PageDependencies,
    ) -> Result<PageDependencies, PageDependencyError> {
        let shard = Self::shard(key);
        let mut entries = self.shards[shard]
            .write()
            .map_err(|_| PageDependencyError::Poisoned(shard))?;
        let merged = entries
            .get(&key)
            .copied()
            .unwrap_or_default()
            .merged(required);
        if merged == PageDependencies::none() {
            entries.remove(&key);
        } else {
            entries.insert(key, merged);
        }
        Ok(merged)
    }

    /// Current conservative requirements for one page.
    pub fn requirements(&self, key: PageKey) -> Result<PageDependencies, PageDependencyError> {
        let shard = Self::shard(key);
        let entries = self.shards[shard]
            .read()
            .map_err(|_| PageDependencyError::Poisoned(shard))?;
        Ok(entries.get(&key).copied().unwrap_or_default())
    }

    /// Whether the current durable frontiers cover these exact requirements.
    #[must_use]
    pub(crate) fn frontiers_cover(&self, required: PageDependencies) -> bool {
        required.is_satisfied_by(
            self.durable_wal(),
            self.durable_undo.load(Ordering::Acquire),
        )
    }

    /// Capture one page's conservative requirements, refusing the write when
    /// either durable frontier does not cover them.
    ///
    /// The returned value is the exact requirement that was checked, so a
    /// persistent backend can encode the captured requirement instead of
    /// repeating the lookup and risking a different answer.
    pub(crate) fn checked_requirements(&self, key: PageKey) -> io::Result<PageDependencies> {
        let required = self
            .requirements(key)
            .map_err(|error| io::Error::other(error.to_string()))?;
        let durable_wal = self.durable_wal();
        let durable_undo = self.durable_undo.load(Ordering::Acquire);
        if !required.is_satisfied_by(durable_wal, durable_undo) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "page {key:?} requires WAL {:?} and undo {:?}; durable frontiers are {:?} and {:?}",
                    required.required_wal(),
                    required.required_undo(),
                    durable_wal,
                    self.durable_undo(),
                ),
            ));
        }
        Ok(required)
    }

    /// Advance the durable WAL frontier after a successful barrier.
    pub fn advance_wal(&self, durable: Lsn) {
        self.durable_wal.fetch_max(durable.get(), Ordering::AcqRel);
    }

    /// Advance the durable undo frontier after a successful barrier.
    pub fn advance_undo(&self, durable: VersionId) {
        self.durable_undo.fetch_max(durable.get(), Ordering::AcqRel);
    }

    #[must_use]
    pub fn durable_wal(&self) -> Lsn {
        Lsn::new(self.durable_wal.load(Ordering::Acquire))
    }

    #[must_use]
    pub fn durable_undo(&self) -> Option<VersionId> {
        let value = self.durable_undo.load(Ordering::Acquire);
        (value != 0).then_some(VersionId::new(value))
    }

    /// Whether one page is currently eligible for physical writeback.
    pub fn is_eligible(&self, key: PageKey) -> Result<bool, PageDependencyError> {
        let required = self.requirements(key)?;
        Ok(required.is_satisfied_by(
            self.durable_wal(),
            self.durable_undo.load(Ordering::Acquire),
        ))
    }

    fn shard(key: PageKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & (DEPENDENCY_SHARDS - 1)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum PageDependencyError {
    #[error("page-dependency shard {0} is poisoned")]
    Poisoned(usize),
}

/// `PageIo` decorator enforcing the dependency table before physical writes.
pub struct DependencyCheckedPageIo {
    inner: Arc<dyn PageIo>,
    dependencies: Arc<PageDependencyTable>,
}

impl DependencyCheckedPageIo {
    #[must_use]
    pub fn new(inner: Arc<dyn PageIo>, dependencies: Arc<PageDependencyTable>) -> Self {
        Self {
            inner,
            dependencies,
        }
    }

    #[must_use]
    pub fn dependencies(&self) -> &Arc<PageDependencyTable> {
        &self.dependencies
    }
}

impl PageIo for DependencyCheckedPageIo {
    fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()> {
        self.inner.read_page(key, destination)
    }

    fn write_page(&self, key: PageKey, source: &[u8]) -> io::Result<()> {
        self.dependencies.checked_requirements(key)?;
        self.inner.write_page(key, source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{PageId, StorageObjectId};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct CountingPageIo {
        writes: AtomicU64,
    }

    impl PageIo for CountingPageIo {
        fn read_page(&self, _key: PageKey, destination: &mut [u8]) -> io::Result<()> {
            destination.fill(0);
            Ok(())
        }

        fn write_page(&self, _key: PageKey, _source: &[u8]) -> io::Result<()> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn key(page: u64) -> PageKey {
        PageKey::new(StorageObjectId::new(1), PageId::new(page))
    }

    #[test]
    fn page_requirements_merge_monotonically() {
        let dependencies = PageDependencyTable::new();
        dependencies
            .merge(
                key(1),
                PageDependencies::new(Lsn::new(20), Some(VersionId::new(3))),
            )
            .expect("first merge");
        let merged = dependencies
            .merge(
                key(1),
                PageDependencies::new(Lsn::new(10), Some(VersionId::new(5))),
            )
            .expect("second merge");
        assert_eq!(
            merged,
            PageDependencies::new(Lsn::new(20), Some(VersionId::new(5)))
        );
    }

    #[test]
    fn page_writeback_waits_for_both_durability_domains() {
        let inner = Arc::new(CountingPageIo::default());
        let dependencies = Arc::new(PageDependencyTable::new());
        let io = DependencyCheckedPageIo::new(inner.clone(), dependencies.clone());
        dependencies
            .merge(
                key(2),
                PageDependencies::new(Lsn::new(30), Some(VersionId::new(7))),
            )
            .expect("requirements merge");

        let blocked = io
            .write_page(key(2), &[1, 2, 3])
            .expect_err("frontiers are behind");
        assert_eq!(blocked.kind(), io::ErrorKind::WouldBlock);
        dependencies.advance_wal(Lsn::new(30));
        assert!(io.write_page(key(2), &[1, 2, 3]).is_err());
        dependencies.advance_undo(VersionId::new(7));
        io.write_page(key(2), &[1, 2, 3])
            .expect("both frontiers cover page");
        assert_eq!(inner.writes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn requirements_survive_reads_and_frontiers_never_move_backward() {
        let inner = Arc::new(CountingPageIo::default());
        let dependencies = Arc::new(PageDependencyTable::new());
        let io = DependencyCheckedPageIo::new(inner, dependencies.clone());
        let required = PageDependencies::new(Lsn::new(40), Some(VersionId::new(9)));
        dependencies.merge(key(3), required).expect("merge");
        dependencies.advance_wal(Lsn::new(50));
        dependencies.advance_wal(Lsn::new(20));
        dependencies.advance_undo(VersionId::new(12));
        dependencies.advance_undo(VersionId::new(4));
        let mut bytes = [0u8; 8];
        io.read_page(key(3), &mut bytes)
            .expect("read passes through");

        assert_eq!(
            dependencies.requirements(key(3)).expect("requirements"),
            required
        );
        assert_eq!(dependencies.durable_wal(), Lsn::new(50));
        assert_eq!(dependencies.durable_undo(), Some(VersionId::new(12)));
        assert!(dependencies.is_eligible(key(3)).expect("eligibility"));
    }
}
