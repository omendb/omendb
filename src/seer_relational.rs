//! Transitional typed OmenDB adoption slice backed by Rust SeerDB.
//!
//! This type deliberately keeps the logical model in OmenDB and routes every
//! durable row, secondary-index, and catalog change through one SeerDB commit
//! batch. Its generic kernel parameter exists for conformance tests while the
//! capability-rich SeerDB transaction API is built; it is not the target
//! production storage-plugin boundary.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::ffi::CString;
use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::attempt::{digest_kv_mutations, encode_record, seer_key};
use crate::kernel::StorageKernel;
use crate::relational::{
    Catalog, ColumnId, ForeignKeyDefinition, IndexDefinition, LogicalVerification,
    RelationalMutation, RelationalSchemaDefinition, RelationalSnapshot,
    RelationalSnapshotCaptureOptions, RelationalSnapshotTable, RelationalStore, Row, TableId,
    Value, build_snapshot_capture, decode_catalog, decode_row, encode_catalog, encode_row,
    foreign_key_values, index_values_key, row_from_storage_identity, row_identity_bytes,
    row_identity_bytes_for_lookup, row_index_key,
};
use crate::row_identity::encode_legacy_key;
use crate::seer_kernel::{SeerCheckpointReport, SeerCompactionReport};
use crate::{
    AttemptRecord, CommitId, DbError, DurabilityStatus, IndexId, Key, KvMutation, Result,
    SeerKernel, SeerKernelConfig, SnapshotIdentity, StorageIdentity, TransactionAttemptId,
};

const CATALOG_KEY: &[u8] = b"\x00omendb/catalog/v1";
const ROW_NAMESPACE: u8 = 0x10;
const INDEX_NAMESPACE: u8 = 0x20;

struct IndexScanBounds<'a> {
    start: Option<&'a [u8]>,
    end: Option<&'a [u8]>,
    limit: usize,
}

/// Result of importing the current logical state from the legacy
/// [`RelationalStore`] into a fresh SeerDB directory.
///
/// Migration deliberately copies only the source's current commit. Historical
/// commit IDs and retained snapshots are not portable between the two storage
/// formats; callers must use an archive or an application-level export when
/// history preservation is required.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LegacyMigrationReport {
    pub source_commit: CommitId,
    pub target_commit: CommitId,
    pub target_identity: StorageIdentity,
    pub table_count: usize,
    pub row_count: usize,
    pub index_entry_count: usize,
    pub mutation_count: usize,
    pub history_preserved: bool,
    pub retained_snapshot_count: usize,
    pub pre_cutover_snapshots_invalidated: bool,
}

/// Policy for migrating the current state of the temporary relational store.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacyMigrationOptions {
    /// Permit migration to discard retained source snapshots. Historical
    /// lineage is never copied by this migration path.
    pub allow_history_loss: bool,
}

/// A typed relational store using SeerDB as its sole durable publication
/// authority. This is the current adoption slice and transitional relational
/// implementation; it will move from the CAS conformance seam to SeerDB's
/// capability-rich transaction API.
/// Publications retained for stale-snapshot certification. Every successful
/// publication appends one entry; the oldest entries prune once the window
/// exceeds its budget, and members whose snapshot falls below the retained
/// window hard-conflict instead of certifying.
const COMMITTED_WINDOW_MAX_ENTRIES: usize = 4096;

struct CommittedWrites {
    entries: VecDeque<CommittedWriteEntry>,
    limit: usize,
}

impl Default for CommittedWrites {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            limit: COMMITTED_WINDOW_MAX_ENTRIES,
        }
    }
}

#[derive(Default, Clone)]
struct CommittedWriteEntry {
    commit: u64,
    identities: BTreeSet<(TableId, Vec<u8>)>,
    uniques: BTreeSet<(u64, Vec<u8>)>,
    tables: BTreeSet<TableId>,
    catalog_changed: bool,
}

pub struct SeerRelationalStore<K: StorageKernel = SeerKernel> {
    pub(crate) kernel: K,
    catalog: Catalog,
    committed_writes: Mutex<CommittedWrites>,
}

impl<K: StorageKernel> SeerRelationalStore<K> {
    /// Consume this store after flushing and closing its SeerDB handle.
    pub fn close(self) -> Result<()> {
        self.kernel.close()
    }

    /// Construct the transitional relational layer over a conformance
    /// [`StorageKernel`]. The kernel owns durability and recovery; OmenDB owns
    /// catalog, row, index, and constraint semantics. Production integration
    /// will call SeerDB directly once its transaction API replaces this seam.
    pub fn from_kernel(kernel: K) -> Result<Self> {
        let commit = kernel.commit_id();
        let catalog = match kernel.get(commit, CATALOG_KEY)? {
            Some(bytes) => decode_catalog(&bytes)?,
            None if commit == CommitId(0) => Catalog::default(),
            None => {
                return Err(DbError::Corruption {
                    artifact: "seerdb catalog",
                    reason: "catalog key is missing for a non-empty database".to_owned(),
                });
            }
        };
        Ok(Self {
            kernel,
            catalog,
            committed_writes: Mutex::new(CommittedWrites::default()),
        })
    }

    #[must_use]
    pub fn commit_id(&self) -> CommitId {
        self.kernel.commit_id()
    }

    /// Return every durable logical commit boundary exposed by SeerDB's
    /// manifest history. The list is observational; capture still acquires
    /// and releases a lease for every selected commit.
    pub(crate) fn published_commits(&self) -> Result<Vec<CommitId>> {
        self.kernel.published_commits()
    }

    /// Return physical publication state through the narrow OmenDB kernel
    /// projection rather than exposing SeerDB's status type.
    pub(crate) fn durability_status(&self) -> Result<DurabilityStatus> {
        self.kernel.durability_status()
    }

    #[must_use]
    pub(crate) fn retained_snapshot_count(&self) -> usize {
        self.kernel.retained_snapshot_count()
    }

    /// Return explicitly retained snapshot commits in ascending order.
    ///
    /// This is an observation of this store handle's in-process retention
    /// leases. It is not a commit-history catalog and does not acquire or
    /// extend a lease.
    #[must_use]
    pub(crate) fn retained_snapshot_commits(&self) -> Vec<CommitId> {
        self.kernel.retained_snapshot_commits()
    }

    pub(crate) fn capture_snapshot(
        &self,
        snapshot: CommitId,
        options: RelationalSnapshotCaptureOptions,
        rows_captured: &mut usize,
    ) -> Result<RelationalSnapshot> {
        let catalog = self.catalog_at(snapshot)?;
        let mut tables = Vec::with_capacity(catalog.tables().count());
        for table in catalog.tables() {
            let remaining = options.max_rows.saturating_sub(*rows_captured);
            let rows = self.scan(table.id, snapshot, remaining.saturating_add(1))?;
            if rows.len() > remaining {
                return Err(DbError::SnapshotCaptureLimit {
                    resource: "rows",
                    limit: options.max_rows,
                });
            }
            *rows_captured += rows.len();
            tables.push(RelationalSnapshotTable {
                table: table.id,
                rows,
            });
        }
        build_snapshot_capture(snapshot, catalog, tables)
    }

    /// Resolve a durable transaction attempt after reopening this history.
    pub fn resolve_attempt(&self, attempt: TransactionAttemptId) -> Result<Option<AttemptRecord>> {
        self.kernel.resolve_attempt(attempt)
    }

    pub(crate) fn attempt_records(&self, limit: usize) -> Result<Vec<AttemptRecord>> {
        self.kernel.attempt_records(limit)
    }

    pub(crate) fn import_attempt_records(
        &mut self,
        records: &[AttemptRecord],
    ) -> Result<Vec<AttemptRecord>> {
        self.kernel.import_attempt_records(records)
    }

    /// Forget durable attempt records after deciding that their identities
    /// will never be reused.
    pub fn forget_attempts(&mut self, attempts: &[TransactionAttemptId]) -> Result<usize> {
        self.kernel.forget_attempts(attempts)
    }

    /// Return the last catalog generation published by this store.
    ///
    /// Catalog changes must go through the store's schema methods so the
    /// durable catalog and the row/index state are published atomically.
    #[must_use]
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Return the stable database/history identity for this typed store.
    pub fn storage_identity(&self) -> Result<StorageIdentity> {
        self.kernel.storage_identity()
    }

    /// Qualify a typed snapshot commit with this store's database/history.
    pub fn snapshot_identity(&self, commit: CommitId) -> Result<SnapshotIdentity> {
        self.kernel.snapshot_identity(commit)
    }

    pub fn create_table(&mut self, table: crate::TableDefinition) -> Result<CommitId> {
        let mut candidate = self.catalog.clone();
        candidate.create_table(table)?;
        self.publish_catalog(candidate)
    }

    /// Publish a new table and its schema objects in one SeerDB commit.
    pub fn create_table_with_schema(
        &mut self,
        table: crate::TableDefinition,
        schema: RelationalSchemaDefinition,
    ) -> Result<CommitId> {
        self.create_table_with_schema_and_primary_key(table, None, schema)
    }

    pub fn create_table_with_schema_and_primary_key(
        &mut self,
        table: crate::TableDefinition,
        primary_key: Option<Vec<ColumnId>>,
        schema: RelationalSchemaDefinition,
    ) -> Result<CommitId> {
        let mut candidate = self.catalog.clone();
        candidate.create_table_with_primary_key(table, primary_key)?;
        for named in &schema.indexes {
            match &named.name {
                Some(name) => {
                    candidate.create_named_index(named.definition.clone(), name.clone())?
                }
                None => candidate.create_index(named.definition.clone())?,
            }
        }
        for named in &schema.foreign_keys {
            match &named.name {
                Some(name) => {
                    candidate.create_named_foreign_key(named.definition.clone(), name.clone())?
                }
                None => candidate.create_foreign_key(named.definition.clone())?,
            }
        }
        self.publish_catalog(candidate)
    }

    /// Atomically append a nullable column and publish the candidate catalog.
    /// Existing physical rows are logically backfilled with `NULL` at reads,
    /// avoiding a table-sized rewrite.
    pub fn add_nullable_column(
        &mut self,
        table: TableId,
        column: crate::ColumnDefinition,
    ) -> Result<CommitId> {
        let mut candidate = self.catalog.clone();
        candidate.add_nullable_column(table, column)?;
        self.publish_catalog(candidate)
    }

    /// Build all existing index entries and publish them with the catalog
    /// definition in one SeerDB batch. A failed publication cannot expose an
    /// index definition without its entries, or entries without its catalog.
    pub fn create_index(&mut self, index: IndexDefinition) -> Result<CommitId> {
        self.create_index_with_name(index, None)
    }

    /// Build and publish one index while retaining its SQL object name in the
    /// backend-neutral catalog.
    pub fn create_named_index(&mut self, index: IndexDefinition, name: String) -> Result<CommitId> {
        self.create_index_with_name(index, Some(name))
    }

    fn create_index_with_name(
        &mut self,
        index: IndexDefinition,
        name: Option<String>,
    ) -> Result<CommitId> {
        let mut candidate = self.catalog.clone();
        match name {
            Some(name) => candidate.create_named_index(index.clone(), name)?,
            None => candidate.create_index(index.clone())?,
        }
        let rows = self.scan(index.table, self.commit_id(), usize::MAX)?;
        let table = candidate.table(index.table)?;
        let mut mutations = Vec::new();
        let mut unique_values = BTreeMap::new();
        for row in rows {
            if let Some(values) = row_index_key(table, &index, &row)? {
                let identity = row_identity_bytes(&candidate, table, &row)?;
                if index.unique
                    && unique_values
                        .insert(values.clone(), identity.clone())
                        .is_some()
                {
                    return Err(DbError::UniqueViolation {
                        index: index.id.0,
                        key: values,
                    });
                }
                mutations.push(KvMutation::Put {
                    key: index_storage_key(index.table, index.id, &values, &identity),
                    value: identity,
                });
            }
        }
        mutations.push(KvMutation::Put {
            key: CATALOG_KEY.to_vec(),
            value: encode_catalog(&candidate)?,
        });
        let outcome = self.kernel.commit(self.commit_id(), &mutations)?;
        self.catalog = candidate;
        self.record_committed_writes(
            outcome.commit,
            CommittedWriteEntry {
                catalog_changed: true,
                ..CommittedWriteEntry::default()
            },
        );
        Ok(outcome.commit)
    }

    /// Validate and publish a foreign-key definition with the catalog bytes.
    pub fn create_foreign_key(&mut self, foreign_key: ForeignKeyDefinition) -> Result<CommitId> {
        self.create_foreign_key_with_name(foreign_key, None)
    }

    /// Validate and publish a named foreign-key definition with the catalog
    /// bytes so logical archive restore can preserve the object name.
    pub fn create_named_foreign_key(
        &mut self,
        foreign_key: ForeignKeyDefinition,
        name: String,
    ) -> Result<CommitId> {
        self.create_foreign_key_with_name(foreign_key, Some(name))
    }

    fn create_foreign_key_with_name(
        &mut self,
        foreign_key: ForeignKeyDefinition,
        name: Option<String>,
    ) -> Result<CommitId> {
        let mut candidate = self.catalog.clone();
        match name {
            Some(name) => candidate.create_named_foreign_key(foreign_key.clone(), name)?,
            None => candidate.create_foreign_key(foreign_key.clone())?,
        }
        self.validate_foreign_key_at(self.commit_id(), &candidate, &foreign_key, &BTreeMap::new())?;
        self.publish_catalog(candidate)
    }

    /// Begin a transaction from the current immutable generation.
    ///
    /// Beginning and reading do not require exclusive access to the logical
    /// store. The kernel owns the physical read-view lifetime; the relational
    /// store remains the authority for catalog and row/index semantics.
    pub fn begin(&self) -> Result<SeerRelationalTransaction<K>> {
        let read_view = self.kernel.begin_current_read_view()?;
        let snapshot = StorageKernel::view_commit_id(&self.kernel, &read_view);
        Ok(SeerRelationalTransaction {
            snapshot,
            read_view: Some(read_view),
            mutations: Vec::new(),
            point_reads: Mutex::new(BTreeSet::new()),
            table_reads: Mutex::new(BTreeSet::new()),
            attempt: None,
        })
    }

    /// Run one typed transaction and commit it when the closure staged writes.
    ///
    /// A closure error drops the active transaction without publication. A
    /// read-only closure returns its snapshot without creating a no-op commit.
    /// Commit errors retain SeerDB's normal serialization and recovery
    /// semantics.
    pub fn transaction<T, F>(&mut self, operation: F) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut SeerRelationalTransaction<K>) -> Result<T>,
    {
        let mut transaction = self.begin()?;
        let value = operation(self, &mut transaction)?;
        let commit = if transaction.is_read_only() {
            transaction.snapshot()
        } else {
            transaction.commit(self)?
        };
        Ok((value, commit))
    }

    /// Commit a typed batch through one SeerDB publication envelope.
    pub fn commit_batch(
        &mut self,
        mutations: impl IntoIterator<Item = RelationalMutation>,
    ) -> Result<CommitId> {
        let mut transaction = self.begin()?;
        for mutation in mutations {
            transaction.stage(self, mutation)?;
        }
        transaction.commit(self)
    }

    /// Delete a row by the catalog-owned identity encoded in its values.
    pub fn delete_row(&mut self, table: TableId, row: Row) -> Result<CommitId> {
        self.commit_batch([RelationalMutation::DeleteRow { table, row }])
    }

    /// Commit a typed batch with a durable idempotency record.
    pub fn commit_batch_with_attempt(
        &mut self,
        mutations: impl IntoIterator<Item = RelationalMutation>,
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        let mut transaction = self.begin()?;
        for mutation in mutations {
            transaction.stage(self, mutation)?;
        }
        transaction.commit_with_attempt(self, attempt)
    }

    /// Retain a published commit for explicit historical reads.
    pub fn retain(&mut self, snapshot: CommitId) -> Result<K::Lease> {
        StorageKernel::retain(&mut self.kernel, snapshot)
    }

    /// Release one caller-owned historical snapshot lease.
    pub fn release(&mut self, mut lease: K::Lease) -> Result<()> {
        StorageKernel::release_lease(&mut self.kernel, &mut lease)
    }

    /// Retain the current published root atomically with the kernel's
    /// current-frontier observation.
    pub fn retain_current(&mut self) -> Result<K::Lease> {
        StorageKernel::retain_current(&mut self.kernel)
    }

    pub fn get(&self, table: TableId, snapshot: CommitId, primary: Key) -> Result<Option<Row>> {
        let catalog = self.catalog_at(snapshot)?;
        let definition = catalog.table(table)?;
        validate_row_key(table, primary)?;
        let Some(bytes) = self
            .kernel
            .get(snapshot, &row_storage_key(table, primary))?
        else {
            return Ok(None);
        };
        let row = decode_row(primary, &bytes)?;
        Ok(Some(row.materialize_for(definition)?))
    }

    /// Look up a row through the catalog-owned composite primary-key identity.
    pub fn get_by_identity(
        &self,
        table: TableId,
        snapshot: CommitId,
        identity: &crate::RowIdentity,
    ) -> Result<Option<Row>> {
        let catalog = self.catalog_at(snapshot)?;
        let definition = catalog.table(table)?;
        let encoded = row_identity_bytes_for_lookup(&catalog, table, identity)?;
        let Some(bytes) = self
            .kernel
            .get(snapshot, &row_storage_key_identity(table, &encoded))?
        else {
            return Ok(None);
        };
        row_from_storage_identity(&catalog, definition, &encoded, &bytes).map(Some)
    }

    pub fn scan(&self, table: TableId, snapshot: CommitId, limit: usize) -> Result<Vec<Row>> {
        let catalog = self.catalog_at(snapshot)?;
        let definition = catalog.table(table)?;
        let (start, end) = row_range(table);
        self.kernel
            .scan(snapshot, &start, &end, limit)?
            .into_iter()
            .map(|(key, bytes)| {
                let identity = row_identity_from_storage_key(table, &key)?;
                row_from_storage_identity(&catalog, definition, identity, &bytes)
            })
            .collect()
    }

    fn get_by_identity_at(
        &self,
        catalog: &Catalog,
        table: TableId,
        snapshot: CommitId,
        identity: &[u8],
    ) -> Result<Option<Row>> {
        let definition = catalog.table(table)?;
        let Some(bytes) = self
            .kernel
            .get(snapshot, &row_storage_key_identity(table, identity))?
        else {
            return Ok(None);
        };
        row_from_storage_identity(catalog, definition, identity, &bytes).map(Some)
    }

    pub fn index_get(
        &self,
        table: TableId,
        snapshot: CommitId,
        index: IndexId,
        values: &[Value],
    ) -> Result<Vec<Row>> {
        let catalog = self.catalog_at(snapshot)?;
        let definition = Self::index_definition(&catalog, table, index)?;
        let encoded = index_values_key(catalog.table(table)?, definition, values)?;
        if snapshot == self.commit_id() {
            let view = self.kernel.begin_current_read_view()?;
            return self.index_rows_in_view(
                &catalog,
                &view,
                table,
                index,
                IndexScanBounds {
                    start: Some(&encoded),
                    end: Some(&encoded),
                    limit: usize::MAX,
                },
            );
        }
        let entries = self.index_entries(
            &catalog,
            snapshot,
            index,
            Some(&encoded),
            Some(&encoded),
            usize::MAX,
        )?;
        entries
            .into_iter()
            .map(|(_, primary)| {
                self.get_by_identity_at(&catalog, table, snapshot, &primary)?
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: format!("index {} references missing row {:?}", index.0, primary),
                    })
            })
            .collect()
    }

    pub fn index_scan(
        &self,
        table: TableId,
        snapshot: CommitId,
        index: IndexId,
        start: Option<&[Value]>,
        end: Option<&[Value]>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        let catalog = self.catalog_at(snapshot)?;
        let definition = Self::index_definition(&catalog, table, index)?;
        let table_definition = catalog.table(table)?;
        let start_key = start
            .map(|values| index_values_key(table_definition, definition, values))
            .transpose()?;
        let end_key = end
            .map(|values| index_values_key(table_definition, definition, values))
            .transpose()?;
        if snapshot == self.commit_id() {
            let view = self.kernel.begin_current_read_view()?;
            return self.index_rows_in_view(
                &catalog,
                &view,
                table,
                index,
                IndexScanBounds {
                    start: start_key.as_deref(),
                    end: end_key.as_deref(),
                    limit,
                },
            );
        }
        let entries = self.index_entries(
            &catalog,
            snapshot,
            index,
            start_key.as_deref(),
            end_key.as_deref(),
            limit,
        )?;
        entries
            .into_iter()
            .map(|(_, primary)| {
                self.get_by_identity_at(&catalog, table, snapshot, &primary)?
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: format!("index {} references missing row {:?}", index.0, primary),
                    })
            })
            .collect()
    }

    pub fn checkpoint(&mut self) -> Result<K::IntegrityReport> {
        StorageKernel::checkpoint(&mut self.kernel)
    }

    pub fn verify(&mut self) -> Result<K::IntegrityReport> {
        StorageKernel::verify(&mut self.kernel)
    }

    /// Verify the current typed relational view after the physical SeerDB
    /// check has passed. The result is derived from durable reads and does
    /// not publish, reclaim, or alter the active history.
    pub(crate) fn verify_logical(&self) -> Result<LogicalVerification> {
        let snapshot = self.commit_id();
        let catalog = self.catalog_at(snapshot)?;
        if catalog != self.catalog {
            return Err(DbError::Corruption {
                artifact: "seerdb catalog",
                reason: "durable catalog differs from the active catalog".to_owned(),
            });
        }
        let view = self.kernel.begin_current_read_view()?;

        let mut rows_by_table = BTreeMap::new();
        let mut row_count = 0;
        for table in catalog.tables() {
            let rows = self.rows_in_view(&catalog, &view, table.id)?;
            row_count += rows.len();
            rows_by_table.insert(table.id, rows);
        }
        for foreign_key in catalog.foreign_keys() {
            let child_table = catalog.table(foreign_key.table)?;
            let referenced_table = catalog.table(foreign_key.referenced_table)?;
            let referenced_values = rows_by_table
                .get(&foreign_key.referenced_table)
                .ok_or_else(|| DbError::Corruption {
                    artifact: "seerdb catalog",
                    reason: "foreign-key referenced table is missing".to_owned(),
                })?
                .iter()
                .map(|row| {
                    foreign_key_values(row, referenced_table, &foreign_key.referenced_columns)
                })
                .collect::<Result<Vec<_>>>()?;
            for row in rows_by_table
                .get(&foreign_key.table)
                .ok_or_else(|| DbError::Corruption {
                    artifact: "seerdb catalog",
                    reason: "foreign-key child table is missing".to_owned(),
                })?
            {
                let values = foreign_key_values(row, child_table, &foreign_key.columns)?;
                if values.iter().any(|value| matches!(value, Value::Null)) {
                    continue;
                }
                if !referenced_values
                    .iter()
                    .any(|candidate| candidate == &values)
                {
                    return Err(DbError::ForeignKeyViolation {
                        constraint: foreign_key.id.0,
                        table: foreign_key.table.0,
                        referenced_table: foreign_key.referenced_table.0,
                    });
                }
            }
        }

        let mut index_entry_count = 0;
        for index in catalog.indexes() {
            let table = catalog.table(index.table)?;
            let rows = rows_by_table
                .get(&index.table)
                .ok_or_else(|| DbError::Corruption {
                    artifact: "seerdb catalog",
                    reason: format!("index {} references a missing table", index.id.0),
                })?;
            let mut expected = BTreeMap::<Vec<u8>, BTreeSet<Vec<u8>>>::new();
            for row in rows {
                if let Some(values) = row_index_key(table, index, row)? {
                    expected
                        .entry(values)
                        .or_default()
                        .insert(row_identity_bytes(&catalog, table, row)?);
                }
            }
            if index.unique && expected.values().any(|primaries| primaries.len() > 1) {
                return Err(DbError::Corruption {
                    artifact: "seerdb secondary index",
                    reason: format!("unique index {} contains duplicate row values", index.id.0),
                });
            }
            let rows_by_identity = rows
                .iter()
                .map(|row| Ok((row_identity_bytes(&catalog, table, row)?, row.clone())))
                .collect::<Result<BTreeMap<_, _>>>()?;
            let actual = self
                .index_entries_in_view(
                    &catalog,
                    &view,
                    index.id,
                    &rows_by_identity,
                    IndexScanBounds {
                        start: None,
                        end: None,
                        limit: usize::MAX,
                    },
                )?
                .into_iter()
                .fold(
                    BTreeMap::<Vec<u8>, BTreeSet<Vec<u8>>>::new(),
                    |mut entries, (key, identity)| {
                        entries.entry(key).or_default().insert(identity);
                        entries
                    },
                );
            if actual != expected {
                return Err(DbError::Corruption {
                    artifact: "seerdb secondary index",
                    reason: format!("index {} membership differs from table rows", index.id.0),
                });
            }
            index_entry_count += expected.values().map(BTreeSet::len).sum::<usize>();
        }

        Ok(LogicalVerification {
            catalog_generation: catalog.generation(),
            table_count: catalog.tables().count(),
            index_count: catalog.indexes().count(),
            row_count,
            index_entry_count,
        })
    }

    fn rows_in_view(
        &self,
        catalog: &Catalog,
        view: &K::ReadView,
        table: TableId,
    ) -> Result<Vec<Row>> {
        let definition = catalog.table(table)?;
        let (start, end) = row_range(table);
        self.kernel
            .view_scan(view, &start, &end, usize::MAX)?
            .into_iter()
            .map(|(key, bytes)| {
                let identity = row_identity_from_storage_key(table, &key)?;
                row_from_storage_identity(catalog, definition, identity, &bytes)
            })
            .collect()
    }

    fn index_rows_in_view(
        &self,
        catalog: &Catalog,
        view: &K::ReadView,
        table: TableId,
        index: IndexId,
        bounds: IndexScanBounds<'_>,
    ) -> Result<Vec<Row>> {
        let definition = catalog.index(index).ok_or_else(|| {
            DbError::InvalidState(format!("secondary index {} does not exist", index.0))
        })?;
        let table_definition = catalog.table(table)?;
        self.index_entry_keys_in_view(catalog, view, index, bounds)?
            .into_iter()
            .map(|(values, primary)| {
                let row = self.row_in_view(catalog, view, table, primary)?;
                let actual =
                    row_index_key(table_definition, definition, &row)?.ok_or_else(|| {
                        DbError::Corruption {
                            artifact: "seerdb secondary index",
                            reason: format!(
                                "index {} references a row with a null indexed value",
                                index.0
                            ),
                        }
                    })?;
                if actual != values {
                    return Err(DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: "index entry key disagrees with its row".to_owned(),
                    });
                }
                Ok(row)
            })
            .collect()
    }

    fn row_in_view(
        &self,
        catalog: &Catalog,
        view: &K::ReadView,
        table: TableId,
        identity: Vec<u8>,
    ) -> Result<Row> {
        let bytes = self
            .kernel
            .view_get(view, &row_storage_key_identity(table, &identity))?
            .ok_or_else(|| DbError::Corruption {
                artifact: "seerdb secondary index",
                reason: "index references a missing row identity".to_owned(),
            })?;
        row_from_storage_identity(catalog, catalog.table(table)?, &identity, &bytes)
    }

    fn index_entry_keys_in_view(
        &self,
        catalog: &Catalog,
        view: &K::ReadView,
        index: IndexId,
        bounds: IndexScanBounds<'_>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let definition = catalog.index(index).ok_or_else(|| {
            DbError::InvalidState(format!("secondary index {} does not exist", index.0))
        })?;
        let prefix = index_prefix(definition.table, index);
        let physical_start = append(&prefix, bounds.start.unwrap_or_default());
        let physical_end = bounds
            .end
            .map(|values| prefix_end(&append(&prefix, values)))
            .unwrap_or_else(|| prefix_end(&prefix));

        self.kernel
            .view_scan(view, &physical_start, &physical_end, bounds.limit)?
            .into_iter()
            .map(|(key, value)| {
                if key.len() <= prefix.len() || key[..prefix.len()] != prefix {
                    return Err(DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: "index entry has an invalid namespace".to_owned(),
                    });
                }
                let identity = value;
                if !key.ends_with(&identity) || key.len() <= prefix.len() + identity.len() {
                    return Err(DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: "index entry value disagrees with its key".to_owned(),
                    });
                }
                let suffix_start = key.len() - identity.len();
                Ok((key[prefix.len()..suffix_start].to_vec(), identity))
            })
            .collect()
    }

    fn index_entries_in_view(
        &self,
        catalog: &Catalog,
        view: &K::ReadView,
        index: IndexId,
        rows_by_identity: &BTreeMap<Vec<u8>, Row>,
        bounds: IndexScanBounds<'_>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let definition = catalog.index(index).ok_or_else(|| {
            DbError::InvalidState(format!("secondary index {} does not exist", index.0))
        })?;
        let table_definition = catalog.table(definition.table)?;
        self.index_entry_keys_in_view(catalog, view, index, bounds)?
            .into_iter()
            .map(|(values, primary)| {
                let row = rows_by_identity
                    .get(&primary)
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: format!("index {} references missing row {:?}", index.0, primary),
                    })?;
                let expected =
                    row_index_key(table_definition, definition, row)?.ok_or_else(|| {
                        DbError::Corruption {
                            artifact: "seerdb secondary index",
                            reason: format!(
                                "index {} references a row with a null indexed value",
                                index.0
                            ),
                        }
                    })?;
                if expected != values {
                    return Err(DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: "index entry key disagrees with its row".to_owned(),
                    });
                }
                Ok((values, primary))
            })
            .collect()
    }

    pub fn compact(&mut self) -> Result<K::CompactionReport> {
        StorageKernel::compact(&mut self.kernel)
    }

    pub fn metrics(&self) -> Result<K::Metrics> {
        StorageKernel::metrics(&self.kernel)
    }

    fn publish_catalog(&mut self, candidate: Catalog) -> Result<CommitId> {
        let mutations = [KvMutation::Put {
            key: CATALOG_KEY.to_vec(),
            value: encode_catalog(&candidate)?,
        }];
        let outcome = self.kernel.commit(self.commit_id(), &mutations)?;
        self.catalog = candidate;
        self.record_committed_writes(
            outcome.commit,
            CommittedWriteEntry {
                catalog_changed: true,
                ..CommittedWriteEntry::default()
            },
        );
        Ok(outcome.commit)
    }

    /// Build the committed-write entry a publication contributes to the
    /// certification window.
    fn committed_entry_for(
        catalog: &Catalog,
        mutations: &[RelationalMutation],
    ) -> CommittedWriteEntry {
        let mut entry = CommittedWriteEntry::default();
        for identity in mutation_identities(catalog, mutations)
            .into_iter()
            .flatten()
        {
            entry.tables.insert(identity.0);
            entry.identities.insert(identity);
        }
        for mutation in mutations {
            let (table, row) = match mutation {
                RelationalMutation::Insert { table, row }
                | RelationalMutation::Update { table, row }
                | RelationalMutation::DeleteRow { table, row } => (*table, row),
                RelationalMutation::Delete { .. } => continue,
            };
            let Ok(definition) = catalog.table(table) else {
                continue;
            };
            for unique_index in catalog.indexes_for(table).filter(|index| index.unique) {
                if let Ok(Some(key)) = row_index_key(definition, unique_index, row) {
                    entry.uniques.insert((unique_index.id.0, key));
                }
            }
        }
        entry
    }

    #[cfg(test)]
    fn set_committed_window_limit(&self, limit: usize) {
        self.committed_writes
            .lock()
            .expect("committed writes poisoned")
            .limit = limit;
    }

    fn record_committed_writes(&self, commit: CommitId, mut entry: CommittedWriteEntry) {
        entry.commit = commit.0;
        let mut committed = self
            .committed_writes
            .lock()
            .expect("committed writes poisoned");
        committed.entries.push_back(entry);
        while committed.entries.len() > committed.limit {
            committed.entries.pop_front();
        }
    }

    /// Merge every committed publication in `(snapshot, current]`.
    ///
    /// Returns `None` when the retained window does not cover the full
    /// range (pruned history or no history yet), so callers must
    /// hard-conflict instead of certifying on incomplete evidence.
    /// The retained window covering `(snapshot, current]`, or `None` when
    /// the range is incomplete (pruned history or no history yet).
    fn committed_writes_since(
        &self,
        snapshot: CommitId,
    ) -> Option<std::sync::MutexGuard<'_, CommittedWrites>> {
        let committed = self
            .committed_writes
            .lock()
            .expect("committed writes poisoned");
        let first = committed.entries.front()?.commit;
        if first > snapshot.0 + 1 {
            return None;
        }
        Some(committed)
    }

    /// Decide whether a stale-snapshot member may join the current
    /// publication envelope. Certification fails closed: any overlap
    /// between the member's reads or writes and committed writes since its
    /// snapshot conflicts, as does any catalog change or FK-linked table
    /// activity in the window (row-level FK validation runs at publication
    /// state and cannot see window deletions through the member's old
    /// snapshot).
    fn certify_against_committed(
        &self,
        snapshot: CommitId,
        point_reads: &BTreeSet<(TableId, Vec<u8>)>,
        table_reads: &BTreeSet<TableId>,
        write_identities: &BTreeSet<(TableId, Vec<u8>)>,
        write_uniques: &BTreeSet<(u64, Vec<u8>)>,
        catalog: &Catalog,
    ) -> bool {
        let Some(window) = self.committed_writes_since(snapshot) else {
            return false;
        };
        // Per-entry intersection with early exit: certification runs on the
        // publication path, so the check allocates nothing and stops at the
        // first overlap instead of merging the whole window per member.
        let mut catalog_changed = false;
        for entry in window
            .entries
            .iter()
            .skip_while(|entry| entry.commit <= snapshot.0)
        {
            if write_identities
                .iter()
                .any(|id| entry.identities.contains(id))
            {
                return false;
            }
            if write_uniques
                .iter()
                .any(|value| entry.uniques.contains(value))
            {
                return false;
            }
            if point_reads
                .iter()
                .any(|read| entry.identities.contains(read))
            {
                return false;
            }
            if table_reads.iter().any(|table| entry.tables.contains(table)) {
                return false;
            }
            catalog_changed |= entry.catalog_changed;
        }
        if catalog_changed {
            return false;
        }
        for foreign_key in catalog.foreign_keys() {
            let linked = [foreign_key.table, foreign_key.referenced_table];
            let member_touched = linked.iter().any(|table| {
                table_reads.contains(table)
                    || write_identities.iter().any(|(written, _)| written == table)
            });
            if !member_touched {
                continue;
            }
            let window_touched = window
                .entries
                .iter()
                .skip_while(|entry| entry.commit <= snapshot.0)
                .any(|entry| linked.iter().any(|table| entry.tables.contains(table)));
            if window_touched {
                return false;
            }
        }
        true
    }

    fn commit_transaction(
        &self,
        snapshot: CommitId,
        mutations: &[RelationalMutation],
    ) -> Result<CommitId> {
        let batch = self.build_batch(snapshot, mutations)?;
        let outcome = self.kernel.commit(snapshot, &batch)?;
        // Identity encoding uses the current schema: this path only runs at
        // the publication point, where snapshot == current.
        self.record_committed_writes(
            outcome.commit,
            Self::committed_entry_for(&self.catalog, mutations),
        );
        Ok(outcome.commit)
    }

    /// Publish several prepared transactions as one durable SeerDB batch.
    ///
    /// Every transaction validates against its own snapshot exactly as the
    /// single-transaction path does; a stale snapshot is a serialization
    /// conflict and never commits. Same-snapshot transactions must have
    /// disjoint write identities and disjoint unique-index values — the
    /// first transaction to claim a row identity or unique value wins and
    /// later claimants conflict, because one coalesced publication would
    /// otherwise merge their writes or bypass the unique constraint.
    /// Coalesced survivors share one WAL append, one sync, and one commit
    /// envelope; each transaction's relational batch is fully validated
    /// before the combined kernel publication, so a combined failure is
    /// returned to every batch member and never retried individually (the
    /// kernel may hold a pending unfenced generation after a retryable
    /// failure such as capacity preflight).
    pub fn commit_transactions_coalesced(
        &self,
        prepared: Vec<PreparedSeerTransaction>,
    ) -> Vec<Result<CommitId>> {
        let current = self.commit_id();
        let mut results: Vec<Option<Result<CommitId>>> =
            (0..prepared.len()).map(|_| None).collect();
        let catalog = match self.catalog_at(current) {
            Ok(catalog) => catalog,
            Err(error) => {
                let reason = error.to_string();
                return (0..prepared.len())
                    .map(|_| {
                        Err(DbError::StorageSnapshotUnavailable {
                            snapshot: current.0,
                            reason: reason.clone(),
                        })
                    })
                    .collect();
            }
        };
        struct BatchMember {
            index: usize,
            snapshot: CommitId,
            mutations: Vec<RelationalMutation>,
            read_point: BTreeSet<(TableId, Vec<u8>)>,
            read_tables: BTreeSet<TableId>,
            write_identities: BTreeSet<(TableId, Vec<u8>)>,
            write_uniques: BTreeSet<(u64, Vec<u8>)>,
            attempt: Option<TransactionAttemptId>,
        }
        let mut batched: Vec<BatchMember> = Vec::new();
        let mut claimed_identities: std::collections::BTreeMap<(TableId, Vec<u8>), usize> =
            std::collections::BTreeMap::new();
        let mut claimed_unique: std::collections::BTreeMap<(u64, Vec<u8>), usize> =
            std::collections::BTreeMap::new();

        for (index, prepared) in prepared.into_iter().enumerate() {
            let (snapshot, mutations) = (prepared.snapshot, prepared.mutations);
            let read_point = prepared.point_reads;
            let read_tables = prepared.table_reads;
            if mutations.is_empty() {
                results[index] = Some(Ok(snapshot));
                continue;
            }
            // A caller-selected attempt that already published dedups to
            // its original commit without publishing again. Digest
            // verification for misuse happens at the session entry point,
            // which resolves before the closure ever stages mutations.
            if let Some(attempt) = prepared.attempt {
                match self.resolve_attempt(attempt) {
                    Ok(Some(record)) => {
                        results[index] = Some(Ok(record.commit));
                        continue;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        results[index] = Some(Err(error));
                        continue;
                    }
                }
            }
            let mut disjoint = Ok(());
            let mut own_identities = std::collections::BTreeSet::new();
            let mut own_unique = std::collections::BTreeSet::new();
            'claims: {
                // Repeated writes to one identity inside one transaction are
                // normal read-modify-write, not a conflict; claim each
                // transaction's deduplicated identities once.
                for identity in mutation_identities(&catalog, &mutations) {
                    match identity {
                        Ok(identity) => {
                            own_identities.insert(identity);
                        }
                        Err(error) => {
                            disjoint = Err(error);
                            break 'claims;
                        }
                    }
                }
                for identity in &own_identities {
                    let table = identity.0;
                    match claimed_identities.entry(identity.clone()) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(index);
                        }
                        std::collections::btree_map::Entry::Occupied(entry) => {
                            disjoint = Err(DbError::WriteWriteConflict {
                                table: table.0,
                                writer: *entry.get(),
                            });
                            break 'claims;
                        }
                    }
                }
                // Unique-index values must also be disjoint: two rows with
                // different identities can carry the same unique value, and
                // one envelope would publish both.
                for mutation in &mutations {
                    let (table, row) = match mutation {
                        RelationalMutation::Insert { table, row }
                        | RelationalMutation::Update { table, row }
                        | RelationalMutation::DeleteRow { table, row } => (*table, row),
                        RelationalMutation::Delete { .. } => continue,
                    };
                    let definition = match catalog.table(table) {
                        Ok(definition) => definition,
                        Err(error) => {
                            disjoint = Err(error);
                            break 'claims;
                        }
                    };
                    for unique_index in catalog.indexes_for(table).filter(|index| index.unique) {
                        let key = match row_index_key(definition, unique_index, row) {
                            Ok(key) => key,
                            Err(error) => {
                                disjoint = Err(error);
                                break 'claims;
                            }
                        };
                        if let Some(key) = key {
                            own_unique.insert((unique_index.id.0, key));
                        }
                    }
                }
                for claim in &own_unique {
                    match claimed_unique.entry(claim.clone()) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(index);
                        }
                        std::collections::btree_map::Entry::Occupied(entry) => {
                            disjoint = Err(DbError::UniqueViolation {
                                index: entry.key().0,
                                key: entry.key().1.clone(),
                            });
                            break 'claims;
                        }
                    }
                }
            }
            if let Err(error) = disjoint {
                results[index] = Some(Err(error));
                continue;
            }
            // A member whose snapshot is behind the publication point can
            // still join when its reads and writes are provably unaffected
            // by everything committed in between; otherwise it conflicts.
            if snapshot != current
                && !self.certify_against_committed(
                    snapshot,
                    &read_point,
                    &read_tables,
                    &own_identities,
                    &own_unique,
                    &catalog,
                )
            {
                claimed_identities.retain(|_, owner| *owner != index);
                claimed_unique.retain(|_, owner| *owner != index);
                results[index] = Some(Err(DbError::SerializationConflict {
                    snapshot: snapshot.0,
                    current: current.0,
                }));
                continue;
            }
            batched.push(BatchMember {
                index,
                snapshot,
                mutations,
                read_point,
                read_tables,
                write_identities: own_identities,
                write_uniques: own_unique,
                attempt: prepared.attempt,
            });
        }

        // Read-write cycle certification among batch members: all members
        // read the same snapshot, so a valid serial order exists unless two
        // members read what the other writes. Single-direction dependencies
        // serialize reader-first and commit; two-way cycles reject the later
        // submission. Table-level reads intersect any write to the table.
        let mut cycle_rejected: BTreeSet<usize> = BTreeSet::new();
        for i in 0..batched.len() {
            for j in i + 1..batched.len() {
                if cycle_rejected.contains(&i) || cycle_rejected.contains(&j) {
                    continue;
                }
                let a = &batched[i];
                let b = &batched[j];
                let writes_tables = |member: &BatchMember| {
                    member
                        .write_identities
                        .iter()
                        .map(|(table, _)| *table)
                        .collect::<BTreeSet<_>>()
                };
                let b_writes_tables = writes_tables(b);
                let a_writes_tables = writes_tables(a);
                let b_reads_a_writes = a
                    .read_point
                    .iter()
                    .any(|read| b.write_identities.contains(read))
                    || a.read_tables
                        .iter()
                        .any(|table| b_writes_tables.contains(table));
                let a_reads_b_writes = b
                    .read_point
                    .iter()
                    .any(|read| a.write_identities.contains(read))
                    || b.read_tables
                        .iter()
                        .any(|table| a_writes_tables.contains(table));
                if b_reads_a_writes && a_reads_b_writes {
                    cycle_rejected.insert(j);
                }
            }
        }
        if !cycle_rejected.is_empty() {
            for position in (0..batched.len()).rev() {
                let index = batched[position].index;
                if !cycle_rejected.contains(&index) {
                    continue;
                }
                claimed_identities.retain(|_, owner| *owner != index);
                claimed_unique.retain(|_, owner| *owner != index);
                let member = batched.remove(position);
                results[member.index] = Some(Err(DbError::SerializationConflict {
                    snapshot: member.snapshot.0,
                    current: current.0,
                }));
            }
        }

        if batched.len() == 1 && batched[0].snapshot == current {
            let member = batched.remove(0);
            results[member.index] = Some(match member.attempt {
                Some(attempt) => {
                    self.commit_transaction_with_attempt(current, &member.mutations, attempt)
                }
                None => self.commit_transaction(current, &member.mutations),
            });
        } else if !batched.is_empty() {
            // Validate every transaction's relational batch before the
            // combined publication so per-transaction errors stay
            // per-transaction and the kernel call is the only combined step.
            let mut window_entry = CommittedWriteEntry::default();
            for member in &batched {
                window_entry
                    .identities
                    .extend(member.write_identities.iter().cloned());
                window_entry
                    .uniques
                    .extend(member.write_uniques.iter().cloned());
                window_entry
                    .tables
                    .extend(member.write_identities.iter().map(|(table, _)| *table));
            }
            let mut validated: Vec<(usize, Vec<KvMutation>, Option<TransactionAttemptId>)> =
                Vec::with_capacity(batched.len());
            for member in batched {
                match self.build_batch(current, &member.mutations) {
                    Ok(batch) => validated.push((member.index, batch, member.attempt)),
                    Err(error) => results[member.index] = Some(Err(error)),
                }
            }
            if !validated.is_empty() {
                let combined = (|| -> Result<CommitId> {
                    let commit =
                        CommitId(current.0.checked_add(1).ok_or_else(|| {
                            DbError::InvalidState("commit ID exhausted".to_owned())
                        })?);
                    let mut mutations = Vec::new();
                    for (_, batch, _) in &validated {
                        mutations.extend(batch.iter().cloned());
                    }
                    // One durable idempotency record per attempted member,
                    // published inside the shared envelope.
                    for (_, batch, attempt) in &validated {
                        if let Some(attempt) = attempt {
                            mutations.push(KvMutation::Put {
                                key: seer_key(*attempt),
                                value: encode_record(crate::AttemptRecord {
                                    attempt: *attempt,
                                    commit,
                                    digest: digest_kv_mutations(batch),
                                })
                                .to_vec(),
                            });
                        }
                    }
                    let outcome = self.kernel.commit(current, &mutations)?;
                    Ok(outcome.commit)
                })();
                match combined {
                    Ok(commit) => {
                        self.record_committed_writes(commit, window_entry);
                        for (index, _, _) in validated {
                            results[index] = Some(Ok(commit));
                        }
                    }
                    // No per-transaction retry here: the failed combined
                    // attempt may have left an unfenced pending generation
                    // (capacity preflight), and a later flush would publish
                    // it. Every batch member observes the same error.
                    Err(error) => {
                        let reason = error.to_string();
                        for (index, _, _) in validated {
                            results[index] = Some(Err(DbError::Storage {
                                operation: "coalesced commit",
                                reason: reason.clone(),
                            }));
                        }
                    }
                }
            }
        }
        results
            .into_iter()
            .map(|result| result.expect("every result is assigned"))
            .collect()
    }

    fn commit_transaction_with_attempt(
        &self,
        snapshot: CommitId,
        mutations: &[RelationalMutation],
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        let batch = self.build_batch(snapshot, mutations)?;
        let outcome = self.kernel.commit_with_attempt(snapshot, attempt, &batch)?;
        Ok(outcome.commit)
    }

    fn commit_transaction_at_current(
        &self,
        current: CommitId,
        snapshot: CommitId,
        mutations: &[RelationalMutation],
    ) -> Result<CommitId> {
        let batch = self.build_batch(snapshot, mutations)?;
        let outcome = self.kernel.commit(current, &batch)?;
        self.record_committed_writes(
            outcome.commit,
            Self::committed_entry_for(&self.catalog, mutations),
        );
        Ok(outcome.commit)
    }

    fn commit_transaction_with_attempt_at_current(
        &self,
        current: CommitId,
        snapshot: CommitId,
        mutations: &[RelationalMutation],
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        let batch = self.build_batch(snapshot, mutations)?;
        let outcome = self.kernel.commit_with_attempt(current, attempt, &batch)?;
        self.record_committed_writes(
            outcome.commit,
            Self::committed_entry_for(&self.catalog, mutations),
        );
        Ok(outcome.commit)
    }

    fn build_batch(
        &self,
        snapshot: CommitId,
        mutations: &[RelationalMutation],
    ) -> Result<Vec<KvMutation>> {
        let catalog = self.catalog_at(snapshot)?;
        self.build_batch_with_catalog(snapshot, &catalog, mutations)
    }

    fn build_batch_with_catalog(
        &self,
        snapshot: CommitId,
        catalog: &Catalog,
        mutations: &[RelationalMutation],
    ) -> Result<Vec<KvMutation>> {
        if mutations.is_empty() {
            return Err(DbError::InvalidState("empty transaction".to_owned()));
        }
        let mut changed: BTreeMap<(TableId, Vec<u8>), Option<Row>> = BTreeMap::new();
        let mut before_rows: BTreeMap<(TableId, Vec<u8>), Option<Row>> = BTreeMap::new();
        for mutation in mutations {
            let (table, identity) = match mutation {
                RelationalMutation::Insert { table, row }
                | RelationalMutation::Update { table, row }
                | RelationalMutation::DeleteRow { table, row } => {
                    let definition = catalog.table(*table)?;
                    (*table, row_identity_bytes(catalog, definition, row)?)
                }
                RelationalMutation::Delete { table, primary } => {
                    (*table, legacy_identity_bytes(*table, *primary))
                }
            };
            let before = if let Some(before) = before_rows.get(&(table, identity.clone())) {
                before.clone()
            } else {
                let before = self.get_by_identity_at(catalog, table, snapshot, &identity)?;
                before_rows.insert((table, identity.clone()), before.clone());
                before
            };
            let visible = changed
                .get(&(table, identity.clone()))
                .cloned()
                .unwrap_or(before);
            match mutation {
                RelationalMutation::Insert { table, row } => {
                    let definition = catalog.table(*table)?;
                    row.validate(definition)?;
                    if visible.is_some() {
                        if let Some(primary_key) = catalog.primary_key(*table)
                            && let Some(index) = catalog
                                .indexes_for(*table)
                                .find(|index| index.unique && index.columns == primary_key)
                        {
                            return Err(DbError::UniqueViolation {
                                index: index.id.0,
                                key: row_index_key(definition, index, row)?.unwrap_or_default(),
                            });
                        }
                        return Err(DbError::InvalidState("row already exists".to_owned()));
                    }
                    changed.insert((*table, identity), Some(row.clone()));
                }
                RelationalMutation::Update { table, row } => {
                    let definition = catalog.table(*table)?;
                    row.validate(definition)?;
                    if visible.is_none() {
                        return Err(DbError::InvalidState("row does not exist".to_owned()));
                    }
                    changed.insert((*table, identity), Some(row.clone()));
                }
                RelationalMutation::Delete { table, primary } => {
                    catalog.table(*table)?;
                    validate_row_key(*table, *primary)?;
                    if visible.is_none() {
                        return Err(DbError::InvalidState("row does not exist".to_owned()));
                    }
                    changed.insert((*table, identity), None);
                }
                RelationalMutation::DeleteRow { table, row } => {
                    row.validate(catalog.table(*table)?)?;
                    if visible.is_none() {
                        return Err(DbError::InvalidState("row does not exist".to_owned()));
                    }
                    changed.insert((*table, identity), None);
                }
            }
        }

        self.validate_foreign_keys(snapshot, catalog, &changed)?;

        let mut index_owners: BTreeMap<(IndexId, Vec<u8>), Vec<u8>> = BTreeMap::new();
        let affected_indexes: Vec<IndexDefinition> = changed
            .keys()
            .flat_map(|(table, _)| catalog.indexes_for(*table).cloned())
            .map(|index| (index.id, index))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect();
        for index in &affected_indexes {
            if !index.unique {
                continue;
            }
            let table = catalog.table(index.table)?;
            let candidate_values = changed
                .iter()
                .filter(|((table_id, _), _)| *table_id == index.table)
                .filter_map(|(_, after)| after.as_ref())
                .map(|row| row_index_key(table, index, row))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>();
            for values in candidate_values {
                for (_, primary) in self.index_entries(
                    catalog,
                    snapshot,
                    index.id,
                    Some(&values),
                    Some(&values),
                    usize::MAX,
                )? {
                    let keep = match changed.get(&(index.table, primary.clone())) {
                        None => true,
                        Some(None) => false,
                        Some(Some(row)) => {
                            row_index_key(table, index, row)?.as_deref() == Some(values.as_slice())
                        }
                    };
                    if keep {
                        index_owners.insert((index.id, values.clone()), primary);
                    }
                }
            }
        }

        let mut row_mutations = Vec::new();
        let mut index_changes = Vec::new();
        for ((table, identity), after) in changed {
            let before = before_rows
                .get(&(table, identity.clone()))
                .cloned()
                .flatten();
            match &after {
                Some(row) => row_mutations.push(KvMutation::Put {
                    key: row_storage_key_identity(table, &identity),
                    value: encode_row(row)?,
                }),
                None => row_mutations.push(KvMutation::Delete {
                    key: row_storage_key_identity(table, &identity),
                }),
            }
            let definition = catalog.table(table)?;
            for index in catalog.indexes_for(table) {
                let old_values = before
                    .as_ref()
                    .map(|row| row_index_key(definition, index, row))
                    .transpose()?
                    .flatten();
                let new_values = after
                    .as_ref()
                    .map(|row| row_index_key(definition, index, row))
                    .transpose()?
                    .flatten();
                if old_values == new_values {
                    continue;
                }
                index_changes.push((
                    index.clone(),
                    table,
                    identity.clone(),
                    old_values,
                    new_values,
                ));
            }
        }
        for (index, _, _, old_values, _) in &index_changes {
            if let Some(values) = old_values {
                index_owners.remove(&(index.id, values.clone()));
            }
        }
        let mut index_mutations = Vec::new();
        for (index, table, identity, old_values, new_values) in index_changes {
            if let Some(values) = old_values {
                index_mutations.push(KvMutation::Delete {
                    key: index_storage_key(table, index.id, &values, &identity),
                });
            }
            if let Some(values) = new_values {
                if index.unique
                    && let Some(existing) = index_owners.get(&(index.id, values.clone()))
                    && *existing != identity
                {
                    return Err(DbError::UniqueViolation {
                        index: index.id.0,
                        key: values,
                    });
                }
                index_owners.insert((index.id, values.clone()), identity.clone());
                index_mutations.push(KvMutation::Put {
                    key: index_storage_key(table, index.id, &values, &identity),
                    value: identity,
                });
            }
        }
        row_mutations.extend(index_mutations);
        Ok(row_mutations)
    }

    fn validate_foreign_keys(
        &self,
        snapshot: CommitId,
        catalog: &Catalog,
        changed: &BTreeMap<(TableId, Vec<u8>), Option<Row>>,
    ) -> Result<()> {
        for foreign_key in catalog.foreign_keys() {
            self.validate_foreign_key_at(snapshot, catalog, foreign_key, changed)?;
        }
        Ok(())
    }

    fn validate_foreign_key_at(
        &self,
        snapshot: CommitId,
        catalog: &Catalog,
        foreign_key: &ForeignKeyDefinition,
        changed: &BTreeMap<(TableId, Vec<u8>), Option<Row>>,
    ) -> Result<()> {
        let child_table = catalog.table(foreign_key.table)?;
        let referenced_table = catalog.table(foreign_key.referenced_table)?;
        let referenced_index = catalog
            .indexes_for(foreign_key.referenced_table)
            .find(|index| index.unique && index.columns == foreign_key.referenced_columns)
            .ok_or_else(|| {
                DbError::InvalidState(format!(
                    "foreign key {} has no unique referenced index",
                    foreign_key.id.0
                ))
            })?;
        let mut referenced_values_removed = false;
        for ((table, identity), after) in changed {
            if *table != foreign_key.referenced_table {
                continue;
            }
            let before =
                self.get_by_identity_at(catalog, foreign_key.referenced_table, snapshot, identity)?;
            let old_values = before
                .as_ref()
                .map(|row| {
                    foreign_key_values(row, referenced_table, &foreign_key.referenced_columns)
                })
                .transpose()?
                .filter(|values| !values.iter().any(|value| matches!(value, Value::Null)))
                .map(|values| index_values_key(referenced_table, referenced_index, &values))
                .transpose()?;
            let new_values = after
                .as_ref()
                .map(|row| {
                    foreign_key_values(row, referenced_table, &foreign_key.referenced_columns)
                })
                .transpose()?
                .filter(|values| !values.iter().any(|value| matches!(value, Value::Null)))
                .map(|values| index_values_key(referenced_table, referenced_index, &values))
                .transpose()?;
            if old_values.is_some() && old_values != new_values {
                referenced_values_removed = true;
                break;
            }
        }
        let child_rows = if changed.is_empty() || referenced_values_removed {
            self.rows_with_changes(snapshot, foreign_key.table, changed)?
        } else {
            changed
                .iter()
                .filter(|((table, _), _)| *table == foreign_key.table)
                .filter_map(|(_, row)| row.as_ref().cloned())
                .collect()
        };
        for row in &child_rows {
            let values = foreign_key_values(row, child_table, &foreign_key.columns)?;
            if values.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            let encoded = index_values_key(referenced_table, referenced_index, &values)?;
            let mut referenced = false;
            for ((table, _), after) in changed {
                if *table != foreign_key.referenced_table {
                    continue;
                }
                if let Some(referenced_row) = after {
                    let values = foreign_key_values(
                        referenced_row,
                        referenced_table,
                        &foreign_key.referenced_columns,
                    )?;
                    if index_values_key(referenced_table, referenced_index, &values)? == encoded {
                        referenced = true;
                        break;
                    }
                }
            }
            if !referenced {
                for (_, primary) in self.index_entries(
                    catalog,
                    snapshot,
                    referenced_index.id,
                    Some(&encoded),
                    Some(&encoded),
                    usize::MAX,
                )? {
                    referenced = match changed.get(&(foreign_key.referenced_table, primary.clone()))
                    {
                        None => true,
                        Some(None) => false,
                        Some(Some(referenced_row)) => {
                            let values = foreign_key_values(
                                referenced_row,
                                referenced_table,
                                &foreign_key.referenced_columns,
                            )?;
                            index_values_key(referenced_table, referenced_index, &values)?
                                == encoded
                        }
                    };
                    if referenced {
                        break;
                    }
                }
            }
            if !referenced {
                return Err(DbError::ForeignKeyViolation {
                    constraint: foreign_key.id.0,
                    table: foreign_key.table.0,
                    referenced_table: foreign_key.referenced_table.0,
                });
            }
        }
        Ok(())
    }

    fn rows_with_changes(
        &self,
        snapshot: CommitId,
        table: TableId,
        changed: &BTreeMap<(TableId, Vec<u8>), Option<Row>>,
    ) -> Result<Vec<Row>> {
        let mut rows = self
            .scan(table, snapshot, usize::MAX)?
            .into_iter()
            .map(|row| {
                let identity = row_identity_bytes(&self.catalog, self.catalog.table(table)?, &row)?;
                Ok((identity, row))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        for ((changed_table, identity), row) in changed {
            if *changed_table != table {
                continue;
            }
            match row {
                Some(row) => {
                    rows.insert(identity.clone(), row.clone());
                }
                None => {
                    rows.remove(identity);
                }
            }
        }
        Ok(rows.into_values().collect())
    }

    pub(crate) fn catalog_at(&self, snapshot: CommitId) -> Result<Catalog> {
        if snapshot == self.commit_id() {
            return Ok(self.catalog.clone());
        }
        let Some(bytes) = self.kernel.get(snapshot, CATALOG_KEY)? else {
            if snapshot == CommitId(0) {
                return Ok(Catalog::default());
            }
            return Err(DbError::Corruption {
                artifact: "seerdb catalog",
                reason: format!("catalog key is missing at commit {}", snapshot.0),
            });
        };
        decode_catalog(&bytes)
    }

    fn index_definition(
        catalog: &Catalog,
        table: TableId,
        index: IndexId,
    ) -> Result<&IndexDefinition> {
        let definition = catalog.index(index).ok_or_else(|| {
            DbError::InvalidState(format!("secondary index {} does not exist", index.0))
        })?;
        if definition.table != table {
            return Err(DbError::InvalidState(format!(
                "secondary index {} belongs to table {}, not {}",
                index.0, definition.table.0, table.0
            )));
        }
        Ok(definition)
    }

    fn index_entries(
        &self,
        catalog: &Catalog,
        snapshot: CommitId,
        index: IndexId,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let definition = catalog.index(index).ok_or_else(|| {
            DbError::InvalidState(format!("secondary index {} does not exist", index.0))
        })?;
        let table_definition = catalog.table(definition.table)?;
        let prefix = index_prefix(definition.table, index);
        let start_key = append(&prefix, start.unwrap_or_default());
        let end_key = end
            .map(|values| prefix_end(&append(&prefix, values)))
            .unwrap_or_else(|| prefix_end(&prefix));
        self.kernel
            .scan(snapshot, &start_key, &end_key, limit)?
            .into_iter()
            .map(|(key, value)| {
                if key.len() <= prefix.len() || key[..prefix.len()] != prefix {
                    return Err(DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: "index entry has an invalid namespace".to_owned(),
                    });
                }
                let identity = value;
                if !key.ends_with(&identity) || key.len() <= prefix.len() + identity.len() {
                    return Err(DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: "index entry value disagrees with its key".to_owned(),
                    });
                }
                let suffix_start = key.len() - identity.len();
                let row = self
                    .get_by_identity_at(catalog, definition.table, snapshot, &identity)?
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: format!("index {} references a missing row identity", index.0),
                    })?;
                let values =
                    row_index_key(table_definition, definition, &row)?.ok_or_else(|| {
                        DbError::Corruption {
                            artifact: "seerdb secondary index",
                            reason: format!(
                                "index {} references a row with a null indexed value",
                                index.0
                            ),
                        }
                    })?;
                let encoded = &key[prefix.len()..suffix_start];
                if values != encoded {
                    return Err(DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: "index entry key disagrees with its row".to_owned(),
                    });
                }
                Ok((values, identity))
            })
            .collect()
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .map_err(|source| DbError::Io {
            operation: "open migration parent for sync",
            source,
        })?
        .sync_all()
        .map_err(|source| DbError::Io {
            operation: "sync migration parent directory",
            source,
        })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn path_cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "migration path contains an embedded NUL",
        )
    })
}

/// Publish a directory without replacing a concurrently-created destination.
///
/// The ordinary `rename` API is replace-on-Unix, which is unsafe for a
/// migration destination checked earlier in a long-running export. Use the
/// platform's exclusive rename primitive and fail closed where no such
/// primitive is available.
fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let from = path_cstring(from)?;
        let to = path_cstring(to)?;
        // SAFETY: both paths are NUL-terminated C strings whose storage lives
        // across the syscall, and AT_FDCWD refers to the current namespace.
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "macos")]
    {
        let from = path_cstring(from)?;
        let to = path_cstring(to)?;
        // SAFETY: both paths are NUL-terminated C strings whose storage lives
        // across the syscall; RENAME_EXCL makes the destination no-replace.
        let result = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (from, to);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "exclusive directory rename is unsupported on this platform",
        ))
    }
}

fn collect_legacy_migration(
    source: &RelationalStore,
) -> Result<(Catalog, Vec<KvMutation>, usize, usize)> {
    let catalog = source.catalog().clone();
    let snapshot = source.commit_id();
    let mut rows_by_table = BTreeMap::<TableId, Vec<Row>>::new();
    for table in catalog.tables() {
        let rows = source.scan(table.id, snapshot, usize::MAX)?;
        for row in &rows {
            row.validate(table)?;
            validate_row_key(table.id, row.primary)?;
        }
        rows_by_table.insert(table.id, rows);
    }
    validate_migrated_foreign_keys(&catalog, &rows_by_table)?;

    let mut mutations = Vec::new();
    let mut row_count = 0;
    for (table, rows) in &rows_by_table {
        let definition = catalog.table(*table)?;
        for row in rows {
            let identity = row_identity_bytes(&catalog, definition, row)?;
            mutations.push(KvMutation::Put {
                key: row_storage_key_identity(*table, &identity),
                value: encode_row(row)?,
            });
            row_count += 1;
        }
    }

    let mut owners = BTreeMap::<(IndexId, Vec<u8>), Vec<u8>>::new();
    let mut index_entry_count = 0;
    for index in catalog.indexes() {
        let table = catalog.table(index.table)?;
        let rows = rows_by_table.get(&index.table).ok_or_else(|| {
            DbError::InvalidState("index table is absent during migration".into())
        })?;
        for row in rows {
            let Some(values) = row_index_key(table, index, row)? else {
                continue;
            };
            let identity = row_identity_bytes(&catalog, table, row)?;
            if index.unique
                && let Some(existing) = owners.insert((index.id, values.clone()), identity.clone())
                && existing != identity
            {
                return Err(DbError::UniqueViolation {
                    index: index.id.0,
                    key: values,
                });
            }
            mutations.push(KvMutation::Put {
                key: index_storage_key(index.table, index.id, &values, &identity),
                value: identity,
            });
            index_entry_count += 1;
        }
    }

    if catalog.tables().next().is_some() || catalog.indexes().next().is_some() {
        mutations.push(KvMutation::Put {
            key: CATALOG_KEY.to_vec(),
            value: encode_catalog(&catalog)?,
        });
    }
    Ok((catalog, mutations, row_count, index_entry_count))
}

fn validate_migrated_foreign_keys(
    catalog: &Catalog,
    rows_by_table: &BTreeMap<TableId, Vec<Row>>,
) -> Result<()> {
    for foreign_key in catalog.foreign_keys() {
        let child_table = catalog.table(foreign_key.table)?;
        let referenced_table = catalog.table(foreign_key.referenced_table)?;
        let referenced_index = catalog
            .indexes()
            .find(|index| {
                index.table == foreign_key.referenced_table
                    && index.unique
                    && index.columns == foreign_key.referenced_columns
            })
            .ok_or_else(|| {
                DbError::InvalidState(format!(
                    "foreign key {} has no unique referenced index",
                    foreign_key.id.0
                ))
            })?;
        let referenced_rows = rows_by_table
            .get(&foreign_key.referenced_table)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let referenced_values = referenced_rows
            .iter()
            .map(|row| foreign_key_values(row, referenced_table, &foreign_key.referenced_columns))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|values| !values.iter().any(|value| matches!(value, Value::Null)))
            .map(|values| index_values_key(referenced_table, referenced_index, &values))
            .collect::<Result<Vec<_>>>()?;
        let child_rows = rows_by_table
            .get(&foreign_key.table)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for row in child_rows {
            let values = foreign_key_values(row, child_table, &foreign_key.columns)?;
            if values.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            let encoded = index_values_key(referenced_table, referenced_index, &values)?;
            if !referenced_values
                .iter()
                .any(|candidate| candidate == &encoded)
            {
                return Err(DbError::ForeignKeyViolation {
                    constraint: foreign_key.id.0,
                    table: foreign_key.table.0,
                    referenced_table: foreign_key.referenced_table.0,
                });
            }
        }
    }
    Ok(())
}

/// A typed transaction with a process-local generation-bound SeerDB read view.
///
/// The read view pins the immutable PMT/blob generation for the transaction's
/// lifetime. A durable snapshot lease is reserved for explicit historical
/// callers; ordinary in-process transactions do not create a retained blob
/// sidecar or add a durable retention record.
#[derive(Debug)]
/// A transaction prepared for coalesced publication: snapshot, staged
/// mutations, and the read sets certification consumes.
pub struct PreparedSeerTransaction {
    pub snapshot: CommitId,
    pub mutations: Vec<RelationalMutation>,
    pub point_reads: BTreeSet<(TableId, Vec<u8>)>,
    pub table_reads: BTreeSet<TableId>,
    pub attempt: Option<TransactionAttemptId>,
}

pub struct SeerRelationalTransaction<K: StorageKernel = SeerKernel> {
    snapshot: CommitId,
    read_view: Option<Arc<K::ReadView>>,
    mutations: Vec<RelationalMutation>,
    /// Row identities this transaction observed through point reads, in the
    /// same encoding the corresponding writes claim.
    point_reads: Mutex<BTreeSet<(TableId, Vec<u8>)>>,
    /// Tables observed through scans or index reads: any write to the table
    /// potentially conflicts, since the read span is not identity-bounded.
    table_reads: Mutex<BTreeSet<TableId>>,
    attempt: Option<TransactionAttemptId>,
}
impl<K: StorageKernel> SeerRelationalTransaction<K> {
    /// Attach a caller-selected idempotency identity published with this
    /// transaction's mutations.
    pub fn set_attempt(&mut self, attempt: TransactionAttemptId) {
        self.attempt = Some(attempt);
    }

    #[must_use]
    pub fn snapshot(&self) -> CommitId {
        self.snapshot
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.mutations.is_empty()
    }

    pub fn insert(
        &mut self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        row: Row,
    ) -> Result<()> {
        self.stage(store, RelationalMutation::Insert { table, row })
    }

    pub fn update(
        &mut self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        row: Row,
    ) -> Result<()> {
        self.stage(store, RelationalMutation::Update { table, row })
    }

    pub fn delete(
        &mut self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        primary: Key,
    ) -> Result<()> {
        self.stage(store, RelationalMutation::Delete { table, primary })
    }

    pub fn delete_row(
        &mut self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        row: Row,
    ) -> Result<()> {
        self.stage(store, RelationalMutation::DeleteRow { table, row })
    }

    pub fn get(
        &self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        primary: Key,
    ) -> Result<Option<Row>> {
        let view = self.read_view()?;
        let catalog = self.catalog_at_view(store, view)?;
        let definition = catalog.table(table)?;
        validate_row_key(table, primary)?;
        self.point_reads
            .lock()
            .expect("read set poisoned")
            .insert((table, legacy_identity_bytes(table, primary)));
        // Composite-primary-key tables store rows under identity encodings a
        // legacy key cannot name; a point read by legacy key on such a table
        // conservatively reads the whole table for conflict detection.
        if catalog.primary_key(table).is_some() {
            self.table_reads
                .lock()
                .expect("read set poisoned")
                .insert(table);
        }
        let mut row = store
            .kernel
            .view_get(view, &row_storage_key(table, primary))?
            .map(|bytes| decode_row(primary, &bytes))
            .transpose()?;
        row = row.map(|row| row.materialize_for(definition)).transpose()?;
        for mutation in &self.mutations {
            match mutation {
                RelationalMutation::Insert {
                    table: changed_table,
                    row: changed,
                }
                | RelationalMutation::Update {
                    table: changed_table,
                    row: changed,
                } if *changed_table == table && changed.primary == primary => {
                    row = Some(changed.clone());
                }
                RelationalMutation::Delete {
                    table: changed_table,
                    primary: changed,
                } if *changed_table == table && *changed == primary => {
                    row = None;
                }
                _ => {}
            }
        }
        Ok(row)
    }

    /// Look up a row through the catalog-owned composite primary-key identity,
    /// including staged inserts, updates, and identity-based deletes.
    pub fn get_by_identity(
        &self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        identity: &crate::RowIdentity,
    ) -> Result<Option<Row>> {
        let view = self.read_view()?;
        let catalog = self.catalog_at_view(store, view)?;
        let definition = catalog.table(table)?;
        let encoded = row_identity_bytes_for_lookup(&catalog, table, identity)?;
        self.point_reads
            .lock()
            .expect("read set poisoned")
            .insert((table, encoded.clone()));
        let mut row = store
            .kernel
            .view_get(view, &row_storage_key_identity(table, &encoded))?
            .map(|bytes| row_from_storage_identity(&catalog, definition, &encoded, &bytes))
            .transpose()?;

        for mutation in &self.mutations {
            match mutation {
                RelationalMutation::Insert {
                    table: changed_table,
                    row: changed,
                }
                | RelationalMutation::Update {
                    table: changed_table,
                    row: changed,
                } if *changed_table == table
                    && row_identity_bytes(&catalog, definition, changed)? == encoded =>
                {
                    row = Some(row_from_storage_identity(
                        &catalog,
                        definition,
                        &encoded,
                        &encode_row(changed)?,
                    )?);
                }
                RelationalMutation::DeleteRow {
                    table: changed_table,
                    row: changed,
                } if *changed_table == table
                    && row_identity_bytes(&catalog, definition, changed)? == encoded =>
                {
                    row = None;
                }
                _ => {}
            }
        }
        Ok(row)
    }

    /// Scan the transaction's fixed snapshot with staged row mutations
    /// overlaid in primary-key order.
    pub fn scan(
        &self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        limit: usize,
    ) -> Result<Vec<Row>> {
        let view = self.read_view()?;
        let catalog = self.catalog_at_view(store, view)?;
        let definition = catalog.table(table)?;
        self.table_reads
            .lock()
            .expect("read set poisoned")
            .insert(table);
        let (start, end) = row_range(table);
        let mut rows = BTreeMap::<Vec<u8>, Row>::new();
        for (key, bytes) in store.kernel.view_scan(view, &start, &end, usize::MAX)? {
            let identity = row_identity_from_storage_key(table, &key)?;
            let row = row_from_storage_identity(&catalog, definition, identity, &bytes)?;
            rows.insert(identity.to_vec(), row);
        }
        for mutation in &self.mutations {
            match mutation {
                RelationalMutation::Insert {
                    table: changed_table,
                    row,
                }
                | RelationalMutation::Update {
                    table: changed_table,
                    row,
                } if *changed_table == table => {
                    row.validate(definition)?;
                    rows.insert(row_identity_bytes(&catalog, definition, row)?, row.clone());
                }
                RelationalMutation::Delete {
                    table: changed_table,
                    primary,
                } if *changed_table == table => {
                    rows.remove(&legacy_identity_bytes(table, *primary));
                }
                RelationalMutation::DeleteRow {
                    table: changed_table,
                    row,
                } if *changed_table == table => {
                    rows.remove(&row_identity_bytes(&catalog, definition, row)?);
                }
                _ => {}
            }
        }
        Ok(rows.into_values().take(limit).collect())
    }

    /// Look up rows through a secondary index while preserving staged row and
    /// index changes in this transaction's snapshot.
    pub fn index_get(
        &self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        index: IndexId,
        values: &[Value],
    ) -> Result<Vec<Row>> {
        self.index_scan(store, table, index, Some(values), Some(values), usize::MAX)
    }

    /// Scan a secondary index in encoded-key order. The physical index is
    /// read through the transaction's immutable SeerDB view, then entries for
    /// staged rows are replaced from the transaction overlay.
    pub fn index_scan(
        &self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        index: IndexId,
        start: Option<&[Value]>,
        end: Option<&[Value]>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let view = self.read_view()?;
        let catalog = self.catalog_at_view(store, view)?;
        let definition = SeerRelationalStore::<K>::index_definition(&catalog, table, index)?;
        let table_definition = catalog.table(table)?;
        self.table_reads
            .lock()
            .expect("read set poisoned")
            .insert(table);
        let start_values = start
            .map(|values| index_values_key(table_definition, definition, values))
            .transpose()?;
        let end_values = end
            .map(|values| index_values_key(table_definition, definition, values))
            .transpose()?;
        if let (Some(start), Some(end)) = (&start_values, &end_values)
            && start > end
        {
            return Err(DbError::InvalidState(
                "secondary index scan start is after end".to_owned(),
            ));
        }

        let prefix = index_prefix(table, index);
        let physical_start = append(&prefix, start_values.as_deref().unwrap_or_default());
        let physical_end = end_values
            .as_deref()
            .map(|values| prefix_end(&append(&prefix, values)))
            .unwrap_or_else(|| prefix_end(&prefix));
        let mut rows = BTreeMap::<(Vec<u8>, Vec<u8>), Row>::new();
        let touched = self
            .mutations
            .iter()
            .filter_map(|mutation| match mutation {
                RelationalMutation::Insert {
                    table: changed,
                    row,
                }
                | RelationalMutation::Update {
                    table: changed,
                    row,
                } if *changed == table => Some(
                    row_identity_bytes(&catalog, table_definition, row)
                        .expect("validated row identity"),
                ),
                RelationalMutation::Delete {
                    table: changed,
                    primary,
                } if *changed == table => Some(legacy_identity_bytes(table, *primary)),
                RelationalMutation::DeleteRow {
                    table: changed,
                    row,
                } if *changed == table => Some(
                    row_identity_bytes(&catalog, table_definition, row)
                        .expect("validated row identity"),
                ),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        for (key, value) in
            store
                .kernel
                .view_scan(view, &physical_start, &physical_end, usize::MAX)?
        {
            if key.len() <= prefix.len() || key[..prefix.len()] != prefix {
                return Err(DbError::Corruption {
                    artifact: "seerdb secondary index",
                    reason: "index entry has an invalid namespace".to_owned(),
                });
            }
            let identity = value;
            if !key.ends_with(&identity) || key.len() <= prefix.len() + identity.len() {
                return Err(DbError::Corruption {
                    artifact: "seerdb secondary index",
                    reason: "index entry value disagrees with its key".to_owned(),
                });
            }
            let suffix_start = key.len() - identity.len();
            let encoded = key[prefix.len()..suffix_start].to_vec();
            if touched.contains(&identity) {
                let base = store
                    .kernel
                    .view_get(view, &row_storage_key_identity(table, &identity))?
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: "index references a missing row identity".to_owned(),
                    })?;
                let base_row =
                    row_from_storage_identity(&catalog, table_definition, &identity, &base)?;
                let base_values = row_index_key(table_definition, definition, &base_row)?
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: format!(
                            "index {} references a row with a null indexed value",
                            index.0
                        ),
                    })?;
                if base_values != encoded {
                    return Err(DbError::Corruption {
                        artifact: "seerdb secondary index",
                        reason: "index entry key disagrees with its row".to_owned(),
                    });
                }
                continue;
            }
            let row = store
                .get_by_identity_at(&catalog, table, self.snapshot, &identity)?
                .ok_or_else(|| DbError::Corruption {
                    artifact: "seerdb secondary index",
                    reason: "index references a missing row identity".to_owned(),
                })?;
            let values = row_index_key(table_definition, definition, &row)?.ok_or_else(|| {
                DbError::Corruption {
                    artifact: "seerdb secondary index",
                    reason: format!(
                        "index {} references a row with a null indexed value",
                        index.0
                    ),
                }
            })?;
            if values != encoded {
                return Err(DbError::Corruption {
                    artifact: "seerdb secondary index",
                    reason: "index entry key disagrees with its row".to_owned(),
                });
            }
            if index_values_in_bounds(&values, start_values.as_deref(), end_values.as_deref()) {
                rows.insert((values, identity), row);
            }
        }
        let visible_rows = self.scan(store, table, usize::MAX)?;
        for identity in touched {
            rows.retain(|(_, candidate), _| *candidate != identity);
            if let Some(row) = visible_rows.iter().find(|row| {
                row_identity_bytes(&catalog, table_definition, row)
                    .is_ok_and(|candidate| candidate == identity)
            }) && let Some(values) = row_index_key(table_definition, definition, row)?
                && index_values_in_bounds(&values, start_values.as_deref(), end_values.as_deref())
            {
                rows.insert((values, identity), row.clone());
            }
        }
        Ok(rows.into_values().take(limit).collect())
    }

    fn read_view(&self) -> Result<&Arc<K::ReadView>> {
        self.read_view
            .as_ref()
            .ok_or_else(|| DbError::InvalidState("transaction read view is released".into()))
    }

    fn catalog_at_view(
        &self,
        store: &SeerRelationalStore<K>,
        view: &K::ReadView,
    ) -> Result<Catalog> {
        match store.kernel.view_get(view, CATALOG_KEY)? {
            Some(bytes) => decode_catalog(&bytes),
            None if self.snapshot == CommitId(0) => Ok(Catalog::default()),
            None => Err(DbError::Corruption {
                artifact: "seerdb catalog",
                reason: format!("catalog key is missing at commit {}", self.snapshot.0),
            }),
        }
    }

    pub fn commit(mut self, store: &mut SeerRelationalStore<K>) -> Result<CommitId> {
        if self.is_read_only() {
            let snapshot = self.snapshot;
            self.read_view.take();
            return Ok(snapshot);
        }
        let current = store.commit_id();
        let commit = if current != self.snapshot {
            Err(DbError::SerializationConflict {
                snapshot: self.snapshot.0,
                current: current.0,
            })
        } else {
            store.commit_transaction(self.snapshot, &self.mutations)
        };
        self.read_view.take();
        commit
    }

    /// Consume the transaction into its snapshot, staged mutations, and
    /// read sets for coalesced publication.
    #[must_use]
    pub fn into_prepared(mut self) -> PreparedSeerTransaction {
        self.read_view.take();
        let point_reads = self
            .point_reads
            .get_mut()
            .expect("read set poisoned")
            .clone();
        let table_reads = self
            .table_reads
            .get_mut()
            .expect("read set poisoned")
            .clone();
        PreparedSeerTransaction {
            snapshot: self.snapshot,
            mutations: std::mem::take(&mut self.mutations),
            point_reads,
            table_reads,
            attempt: self.attempt,
        }
    }

    pub fn commit_with_attempt(
        mut self,
        store: &mut SeerRelationalStore<K>,
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        if self.is_read_only() {
            let snapshot = self.snapshot;
            self.read_view.take();
            return Ok(snapshot);
        }
        let current = store.commit_id();
        let commit = if current != self.snapshot {
            Err(DbError::SerializationConflict {
                snapshot: self.snapshot.0,
                current: current.0,
            })
        } else {
            store.commit_transaction_with_attempt(self.snapshot, &self.mutations, attempt)
        };
        self.read_view.take();
        commit
    }

    pub fn commit_validated(mut self, store: &mut SeerRelationalStore<K>) -> Result<CommitId> {
        if self.is_read_only() {
            let snapshot = self.snapshot;
            self.read_view.take();
            return Ok(snapshot);
        }
        let point_reads = self.point_reads.lock().expect("read set poisoned").clone();
        let table_reads = self.table_reads.lock().expect("read set poisoned").clone();
        let catalog = store.catalog_at(store.commit_id())?;
        let entry = SeerRelationalStore::<K>::committed_entry_for(&catalog, &self.mutations);
        let write_identities = entry.identities.clone();
        let write_uniques = entry.uniques.clone();
        let current = store.commit_id();
        let certified = current == self.snapshot
            || store.certify_against_committed(
                self.snapshot,
                &point_reads,
                &table_reads,
                &write_identities,
                &write_uniques,
                &catalog,
            );
        let commit = if !certified {
            Err(DbError::SerializationConflict {
                snapshot: self.snapshot.0,
                current: current.0,
            })
        } else {
            store.commit_transaction_at_current(current, self.snapshot, &self.mutations)
        };
        self.read_view.take();
        commit
    }

    pub fn commit_validated_with_attempt(
        mut self,
        store: &mut SeerRelationalStore<K>,
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        if self.is_read_only() {
            let snapshot = self.snapshot;
            self.read_view.take();
            return Ok(snapshot);
        }
        let point_reads = self.point_reads.lock().expect("read set poisoned").clone();
        let table_reads = self.table_reads.lock().expect("read set poisoned").clone();
        let catalog = store.catalog_at(store.commit_id())?;
        let entry = SeerRelationalStore::<K>::committed_entry_for(&catalog, &self.mutations);
        let current = store.commit_id();
        let certified = current == self.snapshot
            || store.certify_against_committed(
                self.snapshot,
                &point_reads,
                &table_reads,
                &entry.identities,
                &entry.uniques,
                &catalog,
            );
        let commit = if !certified {
            Err(DbError::SerializationConflict {
                snapshot: self.snapshot.0,
                current: current.0,
            })
        } else {
            store.commit_transaction_with_attempt_at_current(
                current,
                self.snapshot,
                &self.mutations,
                attempt,
            )
        };
        self.read_view.take();
        commit
    }

    pub fn abort(mut self, _store: &mut SeerRelationalStore<K>) -> Result<()> {
        self.read_view.take();
        Ok(())
    }

    fn stage(
        &mut self,
        store: &SeerRelationalStore<K>,
        mutation: RelationalMutation,
    ) -> Result<()> {
        let view = self.read_view()?;
        let catalog = self.catalog_at_view(store, view)?;
        let mutation = match mutation {
            RelationalMutation::Insert { table, row } => RelationalMutation::Insert {
                table,
                row: row.materialize_for(catalog.table(table)?)?,
            },
            RelationalMutation::Update { table, row } => RelationalMutation::Update {
                table,
                row: row.materialize_for(catalog.table(table)?)?,
            },
            mutation => mutation,
        };
        match &mutation {
            RelationalMutation::Insert { table, row } => {
                let definition = catalog.table(*table)?;
                row.validate(definition)?;
                let identity = row_identity_bytes(&catalog, definition, row)?;
                if self.row_by_identity(store, *table, &identity)?.is_some() {
                    if let Some(primary_key) = catalog.primary_key(*table)
                        && let Some(index) = catalog
                            .indexes_for(*table)
                            .find(|index| index.unique && index.columns == primary_key)
                    {
                        return Err(DbError::UniqueViolation {
                            index: index.id.0,
                            key: row_index_key(definition, index, row)?.unwrap_or_default(),
                        });
                    }
                    return Err(DbError::InvalidState("row already exists".to_owned()));
                }
            }
            RelationalMutation::Update { table, row } => {
                let definition = catalog.table(*table)?;
                row.validate(definition)?;
                let identity = row_identity_bytes(&catalog, definition, row)?;
                if self.row_by_identity(store, *table, &identity)?.is_none() {
                    return Err(DbError::InvalidState("row does not exist".to_owned()));
                }
            }
            RelationalMutation::Delete { table, primary } => {
                catalog.table(*table)?;
                validate_row_key(*table, *primary)?;
                let Some(previous) = self.get(store, *table, *primary)? else {
                    return Err(DbError::InvalidState("row does not exist".to_owned()));
                };
                self.expand_referential_actions(store, *table, &previous)?;
            }
            RelationalMutation::DeleteRow { table, row } => {
                let definition = catalog.table(*table)?;
                row.validate(definition)?;
                let identity = row_identity_bytes(&catalog, definition, row)?;
                if self.row_by_identity(store, *table, &identity)?.is_none() {
                    return Err(DbError::InvalidState("row does not exist".to_owned()));
                }
            }
        }
        self.mutations.push(mutation);
        Ok(())
    }

    /// Expand `ON DELETE` actions for one deleted parent row. Mirrors
    /// `RelationalStore::expand_referential_actions`: cascaded deletions are
    /// staged eagerly (visible to later staged reads via the mutation
    /// replay), cycles terminate because deleted rows vanish from the
    /// staged view, and `MAX_CASCADE_DEPTH` bounds deep chains. Constraints
    /// fire in catalog order, children in primary-key scan order.
    fn expand_referential_actions(
        &mut self,
        store: &SeerRelationalStore<K>,
        root_table: TableId,
        root_row: &Row,
    ) -> Result<()> {
        let view = self.read_view()?;
        let catalog = self.catalog_at_view(store, view)?;
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((root_table, root_row.clone(), 0usize));
        while let Some((parent_table, parent_row, depth)) = queue.pop_front() {
            for foreign_key in catalog.foreign_keys() {
                if foreign_key.referenced_table != parent_table {
                    continue;
                }
                if foreign_key.on_delete == crate::relational::ReferentialAction::Restrict {
                    // Enforced by the publication-time integrity pass.
                    continue;
                }
                if depth + 1 > crate::relational::MAX_CASCADE_DEPTH {
                    return Err(DbError::CascadeDepthExceeded {
                        constraint: foreign_key.id.0,
                        table: foreign_key.table.0,
                    });
                }
                let child_definition = catalog.table(foreign_key.table)?;
                let referenced_definition = catalog.table(parent_table)?;
                let required = crate::relational::foreign_key_values(
                    &parent_row,
                    referenced_definition,
                    &foreign_key.referenced_columns,
                )?;
                if required.iter().any(|value| matches!(value, Value::Null)) {
                    continue;
                }
                for child in self.scan(store, foreign_key.table, usize::MAX)? {
                    let values = crate::relational::foreign_key_values(
                        &child,
                        child_definition,
                        &foreign_key.columns,
                    )?;
                    if values.iter().any(|value| matches!(value, Value::Null)) {
                        continue;
                    }
                    if values != required {
                        continue;
                    }
                    match foreign_key.on_delete {
                        crate::relational::ReferentialAction::Restrict => {}
                        crate::relational::ReferentialAction::SetNull => {
                            let mut updated = child.clone();
                            for column in &foreign_key.columns {
                                updated.set_value(child_definition, *column, Value::Null)?;
                            }
                            updated.validate(child_definition)?;
                            self.mutations.push(RelationalMutation::Update {
                                table: foreign_key.table,
                                row: updated,
                            });
                        }
                        crate::relational::ReferentialAction::Cascade => {
                            self.mutations.push(RelationalMutation::Delete {
                                table: foreign_key.table,
                                primary: child.primary,
                            });
                            queue.push_back((foreign_key.table, child, depth + 1));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn row_by_identity(
        &self,
        store: &SeerRelationalStore<K>,
        table: TableId,
        identity: &[u8],
    ) -> Result<Option<Row>> {
        let view = self.read_view()?;
        let catalog = self.catalog_at_view(store, view)?;
        let definition = catalog.table(table)?;

        let mut row = store
            .kernel
            .view_get(view, &row_storage_key_identity(table, identity))?
            .map(|bytes| row_from_storage_identity(&catalog, definition, identity, &bytes))
            .transpose()?;

        for mutation in &self.mutations {
            match mutation {
                RelationalMutation::Insert {
                    table: changed_table,
                    row: changed,
                }
                | RelationalMutation::Update {
                    table: changed_table,
                    row: changed,
                } if *changed_table == table
                    && row_identity_bytes(&catalog, definition, changed)? == identity =>
                {
                    row = Some(changed.clone());
                }
                RelationalMutation::Delete {
                    table: changed_table,
                    primary,
                } if *changed_table == table
                    && legacy_identity_bytes(table, *primary) == identity =>
                {
                    row = None;
                }
                RelationalMutation::DeleteRow {
                    table: changed_table,
                    row: changed,
                } if *changed_table == table
                    && row_identity_bytes(&catalog, definition, changed)? == identity =>
                {
                    row = None;
                }
                _ => {}
            }
        }
        Ok(row)
    }
}
fn index_values_in_bounds(values: &[u8], start: Option<&[u8]>, end: Option<&[u8]>) -> bool {
    start.is_none_or(|start| values >= start) && end.is_none_or(|end| values <= end)
}

fn validate_row_key(table: TableId, key: Key) -> Result<()> {
    if key.0[..8] != table.0.to_be_bytes() {
        return Err(DbError::InvalidState(format!(
            "row key does not belong to table {}",
            table.0
        )));
    }
    Ok(())
}

fn row_prefix(table: TableId) -> Vec<u8> {
    let mut prefix = vec![ROW_NAMESPACE];
    prefix.extend_from_slice(&table.0.to_be_bytes());
    prefix
}

fn row_storage_key(table: TableId, primary: Key) -> Vec<u8> {
    row_storage_key_identity(table, &legacy_identity_bytes(table, primary))
}

fn row_storage_key_identity(table: TableId, identity: &[u8]) -> Vec<u8> {
    append(&row_prefix(table), identity)
}

fn row_range(table: TableId) -> (Vec<u8>, Vec<u8>) {
    let prefix = row_prefix(table);
    (prefix.clone(), prefix_end(&prefix))
}

fn index_prefix(table: TableId, index: IndexId) -> Vec<u8> {
    let mut prefix = vec![INDEX_NAMESPACE];
    prefix.extend_from_slice(&table.0.to_be_bytes());
    prefix.extend_from_slice(&index.0.to_be_bytes());
    prefix
}

fn index_storage_key(table: TableId, index: IndexId, values: &[u8], identity: &[u8]) -> Vec<u8> {
    let mut key = index_prefix(table, index);
    key.extend_from_slice(values);
    key.extend_from_slice(identity);
    key
}

fn row_identity_from_storage_key(table: TableId, key: &[u8]) -> Result<&[u8]> {
    let prefix = row_prefix(table);
    key.strip_prefix(prefix.as_slice())
        .ok_or_else(|| DbError::Corruption {
            artifact: "seerdb row",
            reason: "row key has an invalid namespace".to_owned(),
        })
}

fn legacy_identity_bytes(table: TableId, primary: Key) -> Vec<u8> {
    encode_legacy_key(table, primary).expect("legacy row identity has one non-null component")
}

fn mutation_identities(
    catalog: &Catalog,
    mutations: &[RelationalMutation],
) -> Vec<Result<(TableId, Vec<u8>)>> {
    mutations
        .iter()
        .map(|mutation| match mutation {
            RelationalMutation::Insert { table, row }
            | RelationalMutation::Update { table, row }
            | RelationalMutation::DeleteRow { table, row } => {
                let definition = catalog.table(*table)?;
                Ok((*table, row_identity_bytes(catalog, definition, row)?))
            }
            RelationalMutation::Delete { table, primary } => {
                Ok((*table, legacy_identity_bytes(*table, *primary)))
            }
        })
        .collect()
}

fn append(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(prefix.len() + suffix.len());
    value.extend_from_slice(prefix);
    value.extend_from_slice(suffix);
    value
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for position in (0..end.len()).rev() {
        if end[position] != u8::MAX {
            end[position] += 1;
            end.truncate(position + 1);
            return end;
        }
    }
    vec![u8::MAX]
}

/// Engine-specific surface available only on the SeerDB-backed store.
impl SeerRelationalStore<SeerKernel> {
    /// Create a typed store over a fresh, verified SeerDB directory.
    pub fn create(config: SeerKernelConfig) -> Result<Self> {
        Ok(Self {
            kernel: SeerKernel::create(&config)?,
            catalog: Catalog::default(),
            committed_writes: Mutex::new(CommittedWrites::default()),
        })
    }

    /// Open an existing typed store from its SeerDB directory.
    pub fn open(config: SeerKernelConfig) -> Result<Self> {
        Self::from_kernel(SeerKernel::open(&config)?)
    }

    /// Migrate the current logical state of the legacy relational store into
    /// a fresh SeerDB directory.
    ///
    /// The destination is built in a sibling staging directory, verified,
    /// closed, and published with an exclusive no-replace rename followed by
    /// parent-directory sync. The migration is a current-state handoff: it
    /// preserves catalog definitions, rows, secondary index entries, and
    /// foreign-key validity, but does not fabricate historical SeerDB
    /// commits for the legacy store's prior history.
    pub fn migrate_from_legacy(
        source: &RelationalStore,
        config: SeerKernelConfig,
    ) -> Result<(Self, LegacyMigrationReport)> {
        Self::migrate_from_legacy_with_options(source, config, LegacyMigrationOptions::default())
    }

    /// Migrate the source's current logical state with an explicit policy.
    pub fn migrate_from_legacy_with_options(
        source: &RelationalStore,
        config: SeerKernelConfig,
        options: LegacyMigrationOptions,
    ) -> Result<(Self, LegacyMigrationReport)> {
        if config.directory.exists() {
            return Err(DbError::InvalidState(
                "migration destination must not already exist".to_owned(),
            ));
        }
        let retained_snapshot_count = source.retained_snapshot_count();
        if retained_snapshot_count > 0 && !options.allow_history_loss {
            return Err(DbError::InvalidState(format!(
                "current-state migration would invalidate {retained_snapshot_count} retained source snapshot(s); set allow_history_loss explicitly"
            )));
        }
        let (catalog, mutations, row_count, index_entry_count) = collect_legacy_migration(source)?;
        let destination = config.directory.clone();
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|source| DbError::Io {
            operation: "create migration parent",
            source,
        })?;
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                DbError::InvalidState("migration destination has no valid name".into())
            })?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| DbError::InvalidState(format!("migration clock is invalid: {error}")))?
            .as_nanos();
        let staging = parent.join(format!(
            ".{name}.seerdb-migration-{}-{nonce}",
            std::process::id()
        ));
        if staging.exists() {
            return Err(DbError::InvalidState(
                "migration staging path already exists".to_owned(),
            ));
        }

        let staging_config = SeerKernelConfig {
            directory: staging.clone(),
            options: config.options.clone(),
        };
        let mut published = false;
        let result = (|| {
            let mut migrated = Self::create(staging_config)?;
            let target_commit = if mutations.is_empty() {
                CommitId(0)
            } else {
                migrated.kernel.commit(CommitId(0), &mutations)?.commit
            };
            migrated.catalog = catalog;
            migrated.checkpoint()?;
            migrated.verify()?;
            drop(migrated);
            rename_no_replace(&staging, &destination).map_err(|source| DbError::Io {
                operation: "publish migrated SeerDB directory",
                source,
            })?;
            published = true;
            sync_directory(parent).map_err(|error| DbError::MigrationPublished {
                destination: destination.display().to_string(),
                reason: error.to_string(),
            })?;
            let mut reopened = Self::open(config).map_err(|error| DbError::MigrationPublished {
                destination: destination.display().to_string(),
                reason: error.to_string(),
            })?;
            reopened
                .verify()
                .map_err(|error| DbError::MigrationPublished {
                    destination: destination.display().to_string(),
                    reason: error.to_string(),
                })?;
            let target_identity =
                reopened
                    .storage_identity()
                    .map_err(|error| DbError::MigrationPublished {
                        destination: destination.display().to_string(),
                        reason: error.to_string(),
                    })?;
            Ok((
                reopened,
                LegacyMigrationReport {
                    source_commit: source.commit_id(),
                    target_commit,
                    target_identity,
                    table_count: source.catalog().tables().count(),
                    row_count,
                    index_entry_count,
                    mutation_count: mutations.len(),
                    history_preserved: false,
                    retained_snapshot_count,
                    pre_cutover_snapshots_invalidated: retained_snapshot_count > 0,
                },
            ))
        })();
        if result.is_err() && !published {
            let _ = fs::remove_dir_all(&staging);
        }
        result
    }

    /// Create an independently verified immutable archive of the current
    /// typed database without changing this store's directory.
    pub fn snapshot<P: AsRef<Path>>(&mut self, destination: P) -> Result<seerdb::SnapshotReport> {
        self.kernel.snapshot(destination)
    }

    /// Restore an immutable archive into a new writable typed database.
    pub fn restore<P: AsRef<Path>>(
        config: SeerKernelConfig,
        archive: P,
    ) -> Result<(Self, seerdb::RestoreReport)> {
        let (kernel, report) = SeerKernel::restore(&config, archive)?;
        Ok((Self::from_kernel(kernel)?, report))
    }

    pub(crate) fn checkpoint_with_status(&mut self) -> Result<SeerCheckpointReport> {
        self.kernel.checkpoint_with_status()
    }

    pub(crate) fn compact_with_status(&mut self) -> Result<SeerCompactionReport> {
        self.kernel.compact_with_status()
    }

    pub fn compact_with_limit(
        &mut self,
        max_relocated_pages: usize,
    ) -> Result<seerdb::CompactionReport> {
        self.kernel.compact_with_limit(max_relocated_pages)
    }

    pub(crate) fn compact_with_limit_status(
        &mut self,
        max_relocated_pages: usize,
    ) -> Result<SeerCompactionReport> {
        self.kernel.compact_with_limit_status(max_relocated_pages)
    }

    #[cfg(feature = "seerdb-fault-injection")]
    /// Arm one SeerDB publication fault for the feature-gated R0 harness.
    pub fn inject_fault(&self, point: crate::FaultPoint) -> Result<()> {
        self.kernel.inject_fault(point)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "seerdb-fault-injection")]
    use crate::FaultPoint;
    use crate::row_identity::decode_legacy_key;
    use crate::{
        ColumnDefinition, ColumnId, ColumnType, ConstraintId, NamedIndexDefinition, RowIdentity,
        SeerKernelConfig, TableDefinition, TransactionAttemptId,
    };
    use std::sync::Arc;

    fn table() -> TableDefinition {
        TableDefinition {
            id: TableId(7),
            name: "users".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "email".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "age".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
            ],
        }
    }

    fn row(id: u64, email: &str, age: u64) -> Row {
        Row {
            primary: Key::new(7, id),
            values: vec![Value::Text(email.to_owned()), Value::U64(age)],
        }
    }

    fn index() -> IndexDefinition {
        IndexDefinition {
            id: IndexId(9),
            table: TableId(7),
            columns: vec![ColumnId(1)],
            unique: true,
        }
    }

    #[cfg(feature = "seerdb-fault-injection")]
    fn composite_table() -> TableDefinition {
        TableDefinition {
            id: TableId(70),
            name: "ledger".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "tenant_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "entry_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "state".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        }
    }

    #[cfg(feature = "seerdb-fault-injection")]
    fn composite_schema() -> RelationalSchemaDefinition {
        RelationalSchemaDefinition {
            indexes: vec![
                NamedIndexDefinition {
                    definition: IndexDefinition {
                        id: IndexId(80),
                        table: TableId(70),
                        columns: vec![ColumnId(1), ColumnId(2)],
                        unique: true,
                    },
                    name: Some("ledger_pk".to_owned()),
                },
                NamedIndexDefinition {
                    definition: IndexDefinition {
                        id: IndexId(81),
                        table: TableId(70),
                        columns: vec![ColumnId(3)],
                        unique: false,
                    },
                    name: Some("ledger_state".to_owned()),
                },
            ],
            foreign_keys: Vec::new(),
        }
    }

    #[cfg(feature = "seerdb-fault-injection")]
    fn composite_row(entry_id: u64, state: &str) -> Row {
        Row {
            primary: Key::new(70, entry_id),
            values: vec![
                Value::U64(7),
                Value::U64(entry_id),
                Value::Text(state.to_owned()),
            ],
        }
    }

    #[test]
    fn physical_row_and_index_keys_use_canonical_identity_bytes() {
        let primary = Key::new(7, 42);
        let row_key = row_storage_key(TableId(7), primary);
        let row_prefix = row_prefix(TableId(7));
        assert!(row_key.len() > row_prefix.len() + primary.0.len());
        let identity = RowIdentity::decode(&row_key[row_prefix.len()..]).expect("row identity");
        assert_eq!(identity.table(), TableId(7));
        assert_eq!(identity.columns(), &[ColumnId(0)]);
        assert_eq!(identity.values(), &[Value::Bytes(primary.0.to_vec())]);

        let identity = legacy_identity_bytes(TableId(7), primary);
        let index_key = index_storage_key(TableId(7), IndexId(9), b"encoded-values", &identity);
        assert!(index_key.ends_with(&row_key[row_prefix.len()..]));
        assert_eq!(
            decode_legacy_key(TableId(7), &row_key[row_prefix.len()..]).expect("decode key"),
            primary
        );
    }

    #[test]
    fn coalesced_publication_commits_same_snapshot_transactions_in_one_envelope() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        assert_eq!(store.create_table(table()).expect("table"), CommitId(1));

        let mut first = store.begin().expect("begin");
        first
            .insert(&store, TableId(7), row(1, "a@example.com", 30))
            .expect("insert");
        let mut second = store.begin().expect("begin");
        second
            .insert(&store, TableId(7), row(2, "b@example.com", 40))
            .expect("insert");
        let mut third = store.begin().expect("begin");
        third
            .insert(&store, TableId(7), row(3, "c@example.com", 50))
            .expect("insert");

        let prepared = vec![
            first.into_prepared(),
            second.into_prepared(),
            third.into_prepared(),
        ];
        let results = store.commit_transactions_coalesced(prepared);
        assert!(results.iter().all(Result::is_ok));
        // Three transactions, one durable publication: the commit id
        // advances by exactly one envelope.
        assert_eq!(store.commit_id(), CommitId(2));

        for id in 1..=3u64 {
            let found = store
                .get(TableId(7), store.commit_id(), Key::new(7, id))
                .expect("read");
            assert!(found.is_some(), "row {id} missing after coalesced commit");
        }
    }

    #[test]
    fn coalesced_publication_rejects_overlapping_writes_and_stale_snapshots() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        assert_eq!(store.create_table(table()).expect("table"), CommitId(1));

        // Two same-snapshot transactions claiming the same row identity.
        let mut winner = store.begin().expect("begin");
        winner
            .insert(&store, TableId(7), row(1, "winner@example.com", 30))
            .expect("insert");
        let mut loser = store.begin().expect("begin");
        loser
            .insert(&store, TableId(7), row(1, "loser@example.com", 30))
            .expect("insert");

        let prepared = vec![winner.into_prepared(), loser.into_prepared()];
        let results = store.commit_transactions_coalesced(prepared);
        assert!(results[0].is_ok());
        assert!(matches!(
            &results[1],
            Err(DbError::WriteWriteConflict { table: 7, .. })
        ));

        let stored = store
            .get(TableId(7), store.commit_id(), Key::new(7, 1))
            .expect("read")
            .expect("winner row");
        assert_eq!(
            stored.values[0],
            Value::Text("winner@example.com".to_owned())
        );

        // A transaction prepared before another publication is stale at
        // coalesced commit time.
        let mut stale = store.begin().expect("begin");
        stale
            .insert(&store, TableId(7), row(2, "stale@example.com", 40))
            .expect("insert");
        let stale_snapshot = stale.snapshot();
        let mut advancing = store.begin().expect("begin");
        advancing
            .insert(&store, TableId(7), row(9, "advance@example.com", 90))
            .expect("insert");
        let commit = advancing.commit(&mut store).expect("advance");
        assert!(commit.0 > stale_snapshot.0);

        // The stale member's insert touches nothing the window wrote, so
        // certification commits it instead of hard-conflicting; overlapping
        // stale members conflict (see
        // coalesced_publication_conflicts_stale_snapshots_overlapping_the_window).
        let results = store.commit_transactions_coalesced(vec![stale.into_prepared()]);
        assert!(
            results[0].is_ok(),
            "unaffected stale member commits: {results:?}"
        );
    }

    #[test]
    fn coalesced_publication_rejects_read_write_cycles_but_allows_one_direction() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        assert_eq!(store.create_table(table()).expect("table"), CommitId(1));
        for id in 1..=4u64 {
            let mut seed = store.begin().expect("begin");
            seed.insert(&store, TableId(7), row(id, &format!("seed{id}"), 100))
                .expect("insert");
            seed.commit(&mut store).expect("seed");
        }

        // Cycle: A writes row 1 and reads row 2; B writes row 2 and reads
        // row 1. No serial order exists, so the later submission conflicts.
        let mut cycle_a = store.begin().expect("begin");
        cycle_a
            .get(&store, TableId(7), Key::new(7, 2))
            .expect("read row 2");
        cycle_a
            .update(&store, TableId(7), row(1, "seed1", 150))
            .expect("update row 1");
        let mut cycle_b = store.begin().expect("begin");
        cycle_b
            .get(&store, TableId(7), Key::new(7, 1))
            .expect("read row 1");
        cycle_b
            .update(&store, TableId(7), row(2, "seed2", 250))
            .expect("update row 2");

        let results = store
            .commit_transactions_coalesced(vec![cycle_a.into_prepared(), cycle_b.into_prepared()]);
        assert!(results[0].is_ok(), "first member commits: {results:?}");
        assert!(
            matches!(&results[1], Err(DbError::SerializationConflict { .. })),
            "cycle member must conflict: {results:?}"
        );

        // One direction: C writes row 3; D reads row 3 and writes row 4.
        // Serializing C after D is valid, so both commit.
        let mut writer = store.begin().expect("begin");
        writer
            .update(&store, TableId(7), row(3, "seed3", 300))
            .expect("update row 3");
        let mut reader = store.begin().expect("begin");
        reader
            .get(&store, TableId(7), Key::new(7, 3))
            .expect("read row 3");
        reader
            .update(&store, TableId(7), row(4, "seed4", 400))
            .expect("update row 4");

        let results = store
            .commit_transactions_coalesced(vec![writer.into_prepared(), reader.into_prepared()]);
        assert!(
            results.iter().all(Result::is_ok),
            "single-direction dependency must commit: {results:?}"
        );
    }

    #[test]
    fn coalesced_publication_certifies_unaffected_stale_snapshots() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        assert_eq!(store.create_table(table()).expect("table"), CommitId(1));
        for id in 1..=4u64 {
            let mut seed = store.begin().expect("begin");
            seed.insert(&store, TableId(7), row(id, &format!("seed{id}"), 100))
                .expect("insert");
            seed.commit(&mut store).expect("seed");
        }

        // Stale member reads and writes rows the window never touches.
        let mut stale = store.begin().expect("begin");
        stale
            .get(&store, TableId(7), Key::new(7, 1))
            .expect("read row 1");
        stale
            .update(&store, TableId(7), row(1, "seed1", 150))
            .expect("update row 1");

        // Advance the publication point with unrelated work.
        let mut window = store.begin().expect("begin");
        window
            .update(&store, TableId(7), row(4, "seed4", 400))
            .expect("update row 4");
        window.commit(&mut store).expect("window commit");

        let mut fresh = store.begin().expect("begin");
        fresh
            .update(&store, TableId(7), row(3, "seed3", 300))
            .expect("update row 3");

        let results =
            store.commit_transactions_coalesced(vec![stale.into_prepared(), fresh.into_prepared()]);
        assert!(
            results.iter().all(Result::is_ok),
            "certified stale member must commit: {results:?}"
        );
        store.close().expect("close");
        let reopened = SeerRelationalStore::<SeerKernel>::open(config).expect("reopen");
        let committed = reopened
            .scan(TableId(7), reopened.commit_id(), 16)
            .expect("scan");
        let committed = committed
            .iter()
            .find(|row| row.primary == Key::new(7, 1))
            .expect("row exists");
        assert_eq!(committed.values[1], Value::U64(150));
    }

    #[test]
    fn coalesced_publication_conflicts_stale_snapshots_overlapping_the_window() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        assert_eq!(store.create_table(table()).expect("table"), CommitId(1));
        assert_eq!(store.create_index(index()).expect("index"), CommitId(2));
        for id in 1..=3u64 {
            let mut seed = store.begin().expect("begin");
            seed.insert(&store, TableId(7), row(id, &format!("seed{id}"), 100))
                .expect("insert");
            seed.commit(&mut store).expect("seed");
        }

        // Write-write overlap with the window: both rewrite row 2.
        let mut writer = store.begin().expect("begin");
        writer
            .update(&store, TableId(7), row(2, "seed2", 250))
            .expect("update row 2");
        // Read-write overlap: reads row 2, which the window rewrites.
        let mut reader = store.begin().expect("begin");
        reader
            .get(&store, TableId(7), Key::new(7, 2))
            .expect("read row 2");
        reader
            .update(&store, TableId(7), row(3, "seed3", 350))
            .expect("update row 3");
        // Unique-value overlap: inserts the email the window's update claims.
        let mut unique = store.begin().expect("begin");
        unique
            .insert(&store, TableId(7), row(9, "moved@example.test", 900))
            .expect("insert row 9");

        let mut window = store.begin().expect("begin");
        window
            .update(&store, TableId(7), row(2, "moved@example.test", 250))
            .expect("update row 2");
        window.commit(&mut store).expect("window commit");

        let results = store.commit_transactions_coalesced(vec![
            writer.into_prepared(),
            reader.into_prepared(),
            unique.into_prepared(),
        ]);
        for (name, result) in [
            ("write-write", &results[0]),
            ("read-write", &results[1]),
            ("unique", &results[2]),
        ] {
            assert!(
                matches!(result, Err(DbError::SerializationConflict { .. })),
                "{name} overlap must conflict: {results:?}"
            );
        }
    }

    #[test]
    fn coalesced_publication_hard_conflicts_when_window_history_is_pruned() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        assert_eq!(store.create_table(table()).expect("table"), CommitId(1));
        let mut seed = store.begin().expect("begin");
        seed.insert(&store, TableId(7), row(1, "seed1", 100))
            .expect("insert");
        seed.commit(&mut store).expect("seed");

        let mut stale = store.begin().expect("begin");
        stale
            .update(&store, TableId(7), row(1, "seed1", 150))
            .expect("update row 1");

        store.set_committed_window_limit(2);
        for generation in 0..4u64 {
            let mut window = store.begin().expect("begin");
            window
                .update(&store, TableId(7), row(1, &format!("gen{generation}"), 200))
                .expect("update row 1");
            window.commit(&mut store).expect("window commit");
        }

        let results = store.commit_transactions_coalesced(vec![stale.into_prepared()]);
        assert!(
            matches!(&results[0], Err(DbError::SerializationConflict { .. })),
            "pruned history must fail closed: {results:?}"
        );
    }

    #[test]
    fn coalesced_publication_rejects_shared_unique_values_across_transactions() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        store
            .create_table_with_schema(
                table(),
                RelationalSchemaDefinition {
                    indexes: vec![NamedIndexDefinition {
                        definition: index(),
                        name: Some("users_email_unique".to_owned()),
                    }],
                    foreign_keys: Vec::new(),
                },
            )
            .expect("table with unique index");

        // Disjoint row identities, same unique email: only the first may
        // publish; one envelope must never carry both.
        let mut first = store.begin().expect("begin");
        first
            .insert(&store, TableId(7), row(1, "same@example.com", 30))
            .expect("insert");
        let mut second = store.begin().expect("begin");
        second
            .insert(&store, TableId(7), row(2, "same@example.com", 40))
            .expect("insert");

        let prepared = vec![first.into_prepared(), second.into_prepared()];
        let results = store.commit_transactions_coalesced(prepared);
        assert!(results[0].is_ok());
        assert!(matches!(&results[1], Err(DbError::UniqueViolation { .. })));

        // A transaction may rewrite its own rows: repeated identities inside
        // one transaction are read-modify-write, not a conflict.
        let mut rewrite = store.begin().expect("begin");
        rewrite
            .insert(&store, TableId(7), row(5, "rmw@example.com", 50))
            .expect("insert");
        rewrite
            .update(&store, TableId(7), row(5, "rmw@example.com", 51))
            .expect("update");
        let results = store.commit_transactions_coalesced(vec![rewrite.into_prepared()]);
        assert!(results[0].is_ok());
    }

    #[test]
    fn typed_transaction_publishes_rows_and_index_atomically_and_reopens() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        assert_eq!(store.create_table(table()).expect("table"), CommitId(1));

        let first = row(1, "alice@example.com", 30);
        let mut transaction = store.begin().expect("begin");
        transaction
            .insert(&store, TableId(7), first.clone())
            .expect("insert");
        assert_eq!(
            transaction
                .get(&store, TableId(7), first.primary)
                .expect("transaction read"),
            Some(first.clone())
        );
        assert_eq!(transaction.commit(&mut store).expect("commit"), CommitId(2));

        assert_eq!(store.create_index(index()).expect("index"), CommitId(3));
        assert_eq!(
            store
                .index_get(
                    TableId(7),
                    CommitId(3),
                    IndexId(9),
                    &[Value::Text("alice@example.com".to_owned())],
                )
                .expect("index lookup"),
            vec![first.clone()]
        );
        assert_eq!(
            store
                .index_scan(TableId(7), CommitId(3), IndexId(9), None, None, 10)
                .expect("index scan"),
            vec![first.clone()]
        );

        let second = row(2, "bob@example.com", 31);
        let mut transaction = store.begin().expect("second begin");
        transaction
            .insert(&store, TableId(7), second.clone())
            .expect("second insert");
        assert_eq!(
            transaction.commit(&mut store).expect("second commit"),
            CommitId(4)
        );
        assert_eq!(
            store.scan(TableId(7), CommitId(4), 10).expect("scan").len(),
            2
        );

        let moved = row(1, "carol@example.com", 30);
        let mut transaction = store.begin().expect("update begin");
        transaction
            .update(&store, TableId(7), moved.clone())
            .expect("stage update");
        assert_eq!(
            transaction.commit(&mut store).expect("update commit"),
            CommitId(5)
        );
        assert!(
            store
                .index_get(
                    TableId(7),
                    CommitId(5),
                    IndexId(9),
                    &[Value::Text("alice@example.com".to_owned())],
                )
                .expect("old index lookup")
                .is_empty()
        );
        assert_eq!(
            store
                .index_get(
                    TableId(7),
                    CommitId(5),
                    IndexId(9),
                    &[Value::Text("carol@example.com".to_owned())],
                )
                .expect("new index lookup"),
            vec![moved]
        );

        let duplicate = row(3, "carol@example.com", 40);
        let mut transaction = store.begin().expect("duplicate begin");
        transaction
            .insert(&store, TableId(7), duplicate)
            .expect("stage duplicate row");
        assert!(matches!(
            transaction.commit(&mut store),
            Err(DbError::UniqueViolation { index: 9, .. })
        ));
        assert_eq!(store.commit_id(), CommitId(5));

        store.checkpoint().expect("checkpoint");
        store.verify().expect("verify");
        drop(store);

        let reopened = SeerRelationalStore::<SeerKernel>::open(config).expect("reopen");
        assert_eq!(reopened.commit_id(), CommitId(5));
        assert_eq!(
            reopened.catalog.table(TableId(7)).expect("table").name,
            "users"
        );
        assert_eq!(
            reopened
                .index_get(
                    TableId(7),
                    CommitId(5),
                    IndexId(9),
                    &[Value::Text("bob@example.com".to_owned())],
                )
                .expect("reopened index lookup"),
            vec![second]
        );
    }

    #[test]
    fn typed_transaction_overlays_row_scans_and_secondary_indexes() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");
        store.create_index(index()).expect("index");

        let first = row(1, "alice@example.com", 30);
        let second = row(2, "bob@example.com", 31);
        let mut transaction = store.begin().expect("begin");
        transaction
            .insert(&store, TableId(7), first.clone())
            .expect("stage first");
        transaction
            .insert(&store, TableId(7), second.clone())
            .expect("stage second");
        assert_eq!(
            transaction.scan(&store, TableId(7), 10).expect("scan"),
            vec![first.clone(), second.clone()]
        );
        assert_eq!(
            transaction
                .index_get(
                    &store,
                    TableId(7),
                    IndexId(9),
                    &[Value::Text("alice@example.com".to_owned())],
                )
                .expect("staged index lookup"),
            vec![first.clone()]
        );

        let moved = row(1, "carol@example.com", 32);
        transaction
            .update(&store, TableId(7), moved.clone())
            .expect("stage update");
        transaction
            .delete(&store, TableId(7), second.primary)
            .expect("stage delete");
        assert!(
            transaction
                .index_get(
                    &store,
                    TableId(7),
                    IndexId(9),
                    &[Value::Text("alice@example.com".to_owned())],
                )
                .expect("old staged index lookup")
                .is_empty()
        );
        assert_eq!(
            transaction
                .index_scan(
                    &store,
                    TableId(7),
                    IndexId(9),
                    Some(&[Value::Text("bob@example.com".to_owned())]),
                    Some(&[Value::Text("carol@example.com".to_owned())]),
                    10,
                )
                .expect("staged index range"),
            vec![moved.clone()]
        );
        assert_eq!(
            transaction
                .scan(&store, TableId(7), 10)
                .expect("final scan"),
            vec![moved.clone()]
        );
        assert_eq!(transaction.commit(&mut store).expect("commit"), CommitId(3));
        assert_eq!(
            store
                .index_get(
                    TableId(7),
                    CommitId(3),
                    IndexId(9),
                    &[Value::Text("carol@example.com".to_owned())],
                )
                .expect("committed index lookup"),
            vec![moved]
        );
        assert!(
            store
                .get(TableId(7), CommitId(3), second.primary)
                .expect("deleted row")
                .is_none()
        );
    }

    #[test]
    fn typed_snapshot_restore_preserves_catalog_indexes_and_forks_writable_history() {
        let source_parent = tempfile::tempdir().expect("source parent");
        let archive_parent = tempfile::tempdir().expect("archive parent");
        let restored_parent = tempfile::tempdir().expect("restore parent");
        let source_directory = source_parent.path().join("source");
        let archive = archive_parent.path().join("archive");
        let restored_directory = restored_parent.path().join("restored");
        let source_config = SeerKernelConfig::new(source_directory.clone());
        let restored_config = SeerKernelConfig::new(restored_directory.clone());
        let mut source =
            SeerRelationalStore::<SeerKernel>::create(source_config.clone()).expect("create");
        source.create_table(table()).expect("table");
        assert_eq!(source.create_index(index()).expect("index"), CommitId(2));
        let first = row(1, "alice@example.com", 30);
        let mut transaction = source.begin().expect("begin");
        transaction
            .insert(&source, TableId(7), first.clone())
            .expect("insert");
        assert_eq!(
            transaction.commit(&mut source).expect("commit"),
            CommitId(3)
        );
        source.checkpoint().expect("checkpoint");
        let snapshot = source.snapshot(&archive).expect("snapshot");

        let (mut restored, restore) =
            SeerRelationalStore::<SeerKernel>::restore(restored_config, &archive).expect("restore");
        assert_eq!(restore.source.commit_id, snapshot.source.commit_id);
        assert_eq!(restored.commit_id(), CommitId(3));
        assert_eq!(
            restored.catalog.table(TableId(7)).expect("catalog").name,
            "users"
        );
        assert_eq!(
            restored
                .get(TableId(7), CommitId(3), first.primary)
                .expect("restored row"),
            Some(first.clone())
        );
        assert_eq!(
            restored
                .index_get(
                    TableId(7),
                    CommitId(3),
                    IndexId(9),
                    &[Value::Text("alice@example.com".to_owned())],
                )
                .expect("restored index"),
            vec![first.clone()]
        );

        let second = row(2, "bob@example.com", 31);
        let mut transaction = restored.begin().expect("restored begin");
        transaction
            .insert(&restored, TableId(7), second.clone())
            .expect("restored insert");
        assert_eq!(
            transaction.commit(&mut restored).expect("restored commit"),
            CommitId(4)
        );
        restored.checkpoint().expect("restored checkpoint");
        assert_eq!(
            restored
                .index_get(
                    TableId(7),
                    CommitId(4),
                    IndexId(9),
                    &[Value::Text("bob@example.com".to_owned())],
                )
                .expect("restored new index"),
            vec![second.clone()]
        );
        drop(restored);
        drop(source);

        let mut source =
            SeerRelationalStore::<SeerKernel>::open(source_config).expect("reopen source");
        assert_eq!(source.commit_id(), CommitId(3));
        assert_eq!(
            source
                .get(TableId(7), CommitId(3), first.primary)
                .expect("source row"),
            Some(first)
        );
        assert_eq!(
            source
                .get(TableId(7), CommitId(3), second.primary)
                .expect("source second row"),
            None
        );
        assert!(
            source
                .index_get(
                    TableId(7),
                    CommitId(3),
                    IndexId(9),
                    &[Value::Text("bob@example.com".to_owned())],
                )
                .expect("source index")
                .is_empty()
        );
        source.verify().expect("source verify");
    }

    #[test]
    fn typed_workload_preserves_snapshot_index_and_restore_after_maintenance() {
        let source_parent = tempfile::tempdir().expect("source parent");
        let archive_parent = tempfile::tempdir().expect("archive parent");
        let restored_parent = tempfile::tempdir().expect("restored parent");
        let source_directory = source_parent.path().join("source");
        let archive = archive_parent.path().join("archive");
        let restored_directory = restored_parent.path().join("restored");
        let source_config = SeerKernelConfig::new(source_directory.clone());
        let restored_config = SeerKernelConfig::new(restored_directory);
        let mut store =
            SeerRelationalStore::<SeerKernel>::create(source_config.clone()).expect("create");
        store.create_table(table()).expect("table");
        store.create_index(index()).expect("index");

        let seeded = (1..=64)
            .map(|id| RelationalMutation::Insert {
                table: TableId(7),
                row: row(id, &format!("seed-{id}@example.com"), id),
            })
            .collect::<Vec<_>>();
        let seed_commit = store.commit_batch(seeded).expect("seed workload");
        let seed_rows = store
            .scan(TableId(7), seed_commit, usize::MAX)
            .expect("seed scan");
        let seed_index = store
            .index_scan(TableId(7), seed_commit, IndexId(9), None, None, usize::MAX)
            .expect("seed index scan");
        let mut model = seed_rows
            .iter()
            .cloned()
            .map(|row| (row.primary, row))
            .collect::<BTreeMap<_, _>>();
        let table_definition = table();
        let index_definition = index();
        let expected_index = |model: &BTreeMap<Key, Row>| {
            let mut rows = model.values().cloned().collect::<Vec<_>>();
            rows.sort_by_key(|row| {
                row_index_key(&table_definition, &index_definition, row)
                    .expect("workload index key")
            });
            rows
        };
        let retained = store.retain(seed_commit).expect("retain seed snapshot");

        let reader = store.begin().expect("reader begin");
        let mut loser = store.begin().expect("loser begin");
        let mut winner = store.begin().expect("winner begin");
        let old_first = model.get(&Key::new(7, 1)).expect("seed row").clone();
        let loser_row = row(1, "loser@example.com", 1_001);
        let winner_row = row(1, "winner@example.com", 1_002);
        loser
            .update(&store, TableId(7), loser_row.clone())
            .expect("stage loser");
        winner
            .update(&store, TableId(7), winner_row.clone())
            .expect("stage winner");
        assert_eq!(
            reader
                .get(&store, TableId(7), old_first.primary)
                .expect("reader snapshot"),
            Some(old_first)
        );
        let winner_commit = winner.commit(&mut store).expect("winner commit");
        assert_eq!(winner_commit, CommitId(seed_commit.0 + 1));
        assert!(matches!(
            loser.commit(&mut store),
            Err(DbError::SerializationConflict {
                snapshot,
                current
            }) if snapshot == seed_commit.0 && current == winner_commit.0
        ));
        reader.abort(&mut store).expect("reader abort");
        model.insert(winner_row.primary, winner_row);
        assert_eq!(
            store
                .scan(TableId(7), winner_commit, usize::MAX)
                .expect("winner scan"),
            model.values().cloned().collect::<Vec<_>>()
        );

        for round in 0..12_u64 {
            let update_id = 2 + round;
            let delete_id = 40 + round;
            let insert_id = 100 + round;
            let update_key = Key::new(7, update_id);
            let delete_key = Key::new(7, delete_id);
            let previous = model.get(&update_key).expect("update target").clone();
            let updated = row(
                update_id,
                &format!("updated-{round}@example.com"),
                2_000 + round,
            );
            let inserted = row(
                insert_id,
                &format!("inserted-{round}@example.com"),
                3_000 + round,
            );
            let mut transaction = store.begin().expect("round begin");
            transaction
                .update(&store, TableId(7), updated.clone())
                .expect("stage update");
            transaction
                .delete(&store, TableId(7), delete_key)
                .expect("stage delete");
            transaction
                .insert(&store, TableId(7), inserted.clone())
                .expect("stage insert");

            let mut expected_model = model.clone();
            expected_model.insert(update_key, updated.clone());
            expected_model.remove(&delete_key);
            expected_model.insert(inserted.primary, inserted.clone());
            assert_eq!(
                transaction
                    .scan(&store, TableId(7), usize::MAX)
                    .expect("round scan"),
                expected_model.values().cloned().collect::<Vec<_>>()
            );
            assert_eq!(
                transaction
                    .index_get(
                        &store,
                        TableId(7),
                        IndexId(9),
                        &[Value::Text(format!("updated-{round}@example.com"))],
                    )
                    .expect("round updated index"),
                vec![updated.clone()]
            );
            assert!(
                transaction
                    .index_get(
                        &store,
                        TableId(7),
                        IndexId(9),
                        &[previous.values[0].clone()],
                    )
                    .expect("round old index")
                    .is_empty()
            );
            assert_eq!(
                transaction
                    .index_scan(&store, TableId(7), IndexId(9), None, None, usize::MAX)
                    .expect("round index scan"),
                expected_index(&expected_model)
            );

            let commit = transaction.commit(&mut store).expect("round commit");
            assert_eq!(commit, CommitId(winner_commit.0 + round + 1));
            model = expected_model;
            assert_eq!(
                store
                    .scan(TableId(7), commit, usize::MAX)
                    .expect("current scan"),
                model.values().cloned().collect::<Vec<_>>()
            );
            assert_eq!(
                store
                    .index_scan(TableId(7), commit, IndexId(9), None, None, usize::MAX)
                    .expect("current index scan"),
                expected_index(&model)
            );

            if round % 3 == 2 {
                let report = store.compact_with_limit(1).expect("bounded compaction");
                assert!(report.relocated_pages <= 1);
                store.checkpoint().expect("maintenance checkpoint");
                store.verify().expect("maintenance verify");
                assert_eq!(
                    store
                        .scan(TableId(7), seed_commit, usize::MAX)
                        .expect("retained scan"),
                    seed_rows
                );
                assert_eq!(
                    store
                        .index_scan(TableId(7), seed_commit, IndexId(9), None, None, usize::MAX,)
                        .expect("retained index scan"),
                    seed_index
                );
            }
        }

        let current_commit = store.commit_id();
        let current_rows = model.values().cloned().collect::<Vec<_>>();
        let current_index = expected_index(&model);
        store.checkpoint().expect("pre-snapshot checkpoint");
        let snapshot = store.snapshot(&archive).expect("workload snapshot");
        assert_eq!(
            snapshot.source.commit_id,
            seerdb::CommitId::new(current_commit.0)
        );

        let (mut restored, restore) =
            SeerRelationalStore::<SeerKernel>::restore(restored_config, &archive)
                .expect("workload restore");
        assert_eq!(
            restore.source.commit_id,
            seerdb::CommitId::new(current_commit.0)
        );
        assert_eq!(restored.commit_id(), current_commit);
        assert_eq!(
            restored
                .scan(TableId(7), current_commit, usize::MAX)
                .expect("restored scan"),
            current_rows
        );
        assert_eq!(
            restored
                .index_scan(
                    TableId(7),
                    current_commit,
                    IndexId(9),
                    None,
                    None,
                    usize::MAX,
                )
                .expect("restored index scan"),
            current_index
        );
        restored.verify().expect("restored verify");

        let restored_row = row(2, "restored@example.com", 4_002);
        let mut restored_transaction = restored.begin().expect("restored update begin");
        restored_transaction
            .update(&restored, TableId(7), restored_row.clone())
            .expect("restored update stage");
        let restored_commit = restored_transaction
            .commit(&mut restored)
            .expect("restored update commit");
        assert_eq!(restored_commit, CommitId(current_commit.0 + 1));
        assert_eq!(
            store
                .get(TableId(7), current_commit, restored_row.primary)
                .expect("source remains independent"),
            model.get(&restored_row.primary).cloned()
        );
        assert_eq!(
            restored
                .index_get(
                    TableId(7),
                    restored_commit,
                    IndexId(9),
                    &[Value::Text("restored@example.com".to_owned())],
                )
                .expect("restored updated index"),
            vec![restored_row]
        );
        restored.compact_with_limit(1).expect("restored compaction");
        restored.checkpoint().expect("restored checkpoint");
        restored.verify().expect("restored maintenance verify");
        drop(restored);

        store.release(retained).expect("release seed snapshot");
        assert!(matches!(
            store.scan(TableId(7), seed_commit, usize::MAX),
            Err(DbError::StorageSnapshotUnavailable { snapshot, .. })
                if snapshot == seed_commit.0
        ));
        drop(store);

        let mut reopened =
            SeerRelationalStore::<SeerKernel>::open(source_config).expect("reopen workload");
        assert_eq!(reopened.commit_id(), current_commit);
        assert_eq!(
            reopened
                .scan(TableId(7), current_commit, usize::MAX)
                .expect("reopened scan"),
            current_rows
        );
        assert_eq!(
            reopened
                .index_scan(
                    TableId(7),
                    current_commit,
                    IndexId(9),
                    None,
                    None,
                    usize::MAX,
                )
                .expect("reopened index scan"),
            current_index
        );
        reopened.verify().expect("reopened verify");
    }

    #[test]
    fn legacy_current_state_migration_is_verified_and_reopenable() {
        let parent = tempfile::tempdir().expect("temporary directory");
        let source_directory = parent.path().join("legacy");
        let target_directory = parent.path().join("seerdb");
        let mut source = RelationalStore::create(crate::DatabaseConfig {
            directory: source_directory,
        })
        .expect("create legacy store");
        source.create_table(table()).expect("legacy table");
        source.create_index(index()).expect("legacy index");
        let first = row(1, "alice@example.com", 30);
        let second = row(2, "bob@example.com", 31);
        source
            .commit_batch([
                RelationalMutation::Insert {
                    table: TableId(7),
                    row: first.clone(),
                },
                RelationalMutation::Insert {
                    table: TableId(7),
                    row: second.clone(),
                },
            ])
            .expect("legacy rows");
        let source_commit = source.commit_id();
        let source_catalog = encode_catalog(source.catalog()).expect("source catalog");
        let source_rows = source
            .scan(TableId(7), source_commit, 10)
            .expect("source rows");

        let config = SeerKernelConfig::new(target_directory.clone());
        let (mut migrated, report) =
            SeerRelationalStore::migrate_from_legacy(&source, config.clone()).expect("migrate");
        assert_eq!(report.source_commit, source_commit);
        assert_eq!(report.target_commit, CommitId(1));
        assert_eq!(
            report.target_identity,
            migrated.storage_identity().expect("target identity")
        );
        assert_eq!(report.table_count, 1);
        assert_eq!(report.row_count, 2);
        assert_eq!(report.index_entry_count, 2);
        assert_eq!(report.mutation_count, 5);
        assert!(!report.history_preserved);
        assert_eq!(report.retained_snapshot_count, 0);
        assert!(!report.pre_cutover_snapshots_invalidated);
        assert_eq!(source.commit_id(), source_commit);
        assert_eq!(
            encode_catalog(source.catalog()).expect("source catalog after migration"),
            source_catalog
        );
        assert_eq!(
            source
                .scan(TableId(7), source_commit, 10)
                .expect("source rows after migration"),
            source_rows
        );
        assert_eq!(migrated.commit_id(), CommitId(1));
        assert_eq!(
            migrated
                .get(TableId(7), CommitId(1), first.primary)
                .expect("migrated first row"),
            Some(first.clone())
        );
        assert_eq!(
            migrated
                .index_get(
                    TableId(7),
                    CommitId(1),
                    IndexId(9),
                    &[Value::Text("bob@example.com".to_owned())],
                )
                .expect("migrated index"),
            vec![second.clone()]
        );
        migrated.checkpoint().expect("migration checkpoint");
        migrated.verify().expect("migration verify");
        drop(migrated);

        let reopened = SeerRelationalStore::<SeerKernel>::open(config).expect("reopen migrated");
        assert_eq!(reopened.commit_id(), CommitId(1));
        assert_eq!(
            reopened
                .scan(TableId(7), CommitId(1), 10)
                .expect("reopened scan"),
            vec![first, second]
        );
        assert!(
            fs::read_dir(target_directory)
                .expect("read migrated directory")
                .next()
                .is_some()
        );
    }

    #[test]
    fn legacy_migration_invalidates_retained_history_and_copies_current_state() {
        let parent = tempfile::tempdir().expect("temporary directory");
        let source_directory = parent.path().join("legacy");
        let target_directory = parent.path().join("seerdb");
        let mut source = RelationalStore::create(crate::DatabaseConfig {
            directory: source_directory,
        })
        .expect("create legacy store");
        source.create_table(table()).expect("legacy table");
        source.create_index(index()).expect("legacy index");

        let first = row(1, "alice@example.com", 30);
        source
            .commit_batch([RelationalMutation::Insert {
                table: TableId(7),
                row: first.clone(),
            }])
            .expect("legacy historical row");
        let historical_commit = source.commit_id();
        source
            .retain(historical_commit)
            .expect("retain legacy historical snapshot");

        let second = row(2, "bob@example.com", 31);
        source
            .commit_batch([RelationalMutation::Insert {
                table: TableId(7),
                row: second.clone(),
            }])
            .expect("legacy current row");
        let source_commit = source.commit_id();
        let historical_rows = source
            .scan(TableId(7), historical_commit, 10)
            .expect("retained legacy rows");
        let current_rows = source
            .scan(TableId(7), source_commit, 10)
            .expect("current legacy rows");
        assert_eq!(historical_rows, vec![first.clone()]);
        assert_eq!(current_rows, vec![first.clone(), second.clone()]);

        let config = SeerKernelConfig::new(target_directory);
        let (mut migrated, report) = SeerRelationalStore::migrate_from_legacy_with_options(
            &source,
            config.clone(),
            LegacyMigrationOptions {
                allow_history_loss: true,
            },
        )
        .expect("migrate");
        assert_eq!(report.source_commit, source_commit);
        assert_eq!(report.target_commit, CommitId(1));
        assert!(!report.history_preserved);
        assert_eq!(report.retained_snapshot_count, 1);
        assert!(report.pre_cutover_snapshots_invalidated);
        assert_ne!(report.source_commit, report.target_commit);
        assert_eq!(
            source
                .scan(TableId(7), historical_commit, 10)
                .expect("retained legacy rows after migration"),
            historical_rows
        );
        assert_eq!(
            source
                .scan(TableId(7), report.source_commit, 10)
                .expect("current legacy rows after migration"),
            current_rows
        );
        assert_eq!(migrated.commit_id(), report.target_commit);
        assert_eq!(
            migrated
                .scan(TableId(7), report.target_commit, 10)
                .expect("migrated current rows"),
            current_rows
        );
        migrated.checkpoint().expect("migration checkpoint");
        migrated.verify().expect("migration verify");
        drop(migrated);

        let reopened = SeerRelationalStore::<SeerKernel>::open(config).expect("reopen migrated");
        assert_eq!(
            reopened
                .scan(TableId(7), report.target_commit, 10)
                .expect("reopened current rows"),
            current_rows
        );
        source
            .release(historical_commit)
            .expect("release historical snapshot");
    }

    #[test]
    fn legacy_migration_refuses_retained_history_without_explicit_opt_in() {
        let parent = tempfile::tempdir().expect("temporary directory");
        let source_directory = parent.path().join("legacy");
        let target_directory = parent.path().join("seerdb");
        let mut source = RelationalStore::create(crate::DatabaseConfig {
            directory: source_directory,
        })
        .expect("create legacy store");
        source.create_table(table()).expect("legacy table");
        source
            .commit_batch([RelationalMutation::Insert {
                table: TableId(7),
                row: row(1, "alice@example.com", 30),
            }])
            .expect("legacy row");
        let historical_commit = source.commit_id();
        source
            .retain(historical_commit)
            .expect("retain legacy snapshot");

        let result = SeerRelationalStore::migrate_from_legacy(
            &source,
            SeerKernelConfig::new(target_directory.clone()),
        );
        assert!(matches!(
            result,
            Err(DbError::InvalidState(reason))
                if reason.contains("would invalidate 1 retained source snapshot")
        ));
        assert!(!target_directory.exists());
        assert_eq!(source.retained_snapshot_count(), 1);
        source
            .release(historical_commit)
            .expect("release historical snapshot");
    }

    #[test]
    fn typed_create_refuses_existing_store_without_resetting_catalog() {
        let parent = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(parent.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        store.create_table(table()).expect("table");
        drop(store);

        assert!(SeerRelationalStore::<SeerKernel>::create(config.clone()).is_err());
        let reopened =
            SeerRelationalStore::<SeerKernel>::open(config).expect("reopen existing store");
        assert_eq!(
            reopened
                .catalog
                .table(TableId(7))
                .expect("catalog table")
                .name,
            "users"
        );
    }

    #[test]
    fn migration_publication_never_replaces_existing_destination() {
        let parent = tempfile::tempdir().expect("temporary directory");
        let staging = parent.path().join(".staging");
        let destination = parent.path().join("seerdb");
        fs::create_dir(&staging).expect("create staging");
        fs::create_dir(&destination).expect("create destination");
        fs::write(destination.join("sentinel"), b"existing").expect("write sentinel");

        let result = rename_no_replace(&staging, &destination);
        assert!(result.is_err(), "exclusive rename replaced destination");
        assert!(staging.is_dir());
        assert_eq!(
            fs::read(destination.join("sentinel")).expect("read sentinel"),
            b"existing"
        );
    }

    #[test]
    fn legacy_migration_of_empty_source_is_reopenable() {
        let parent = tempfile::tempdir().expect("temporary directory");
        let source_directory = parent.path().join("legacy");
        let target_directory = parent.path().join("seerdb");
        let source = RelationalStore::create(crate::DatabaseConfig {
            directory: source_directory,
        })
        .expect("create empty legacy store");

        let config = SeerKernelConfig::new(target_directory.clone());
        let (mut migrated, report) =
            SeerRelationalStore::migrate_from_legacy(&source, config.clone()).expect("migrate");
        assert_eq!(report.source_commit, CommitId(0));
        assert_eq!(report.target_commit, CommitId(0));
        assert_eq!(report.table_count, 0);
        assert_eq!(report.row_count, 0);
        assert_eq!(report.index_entry_count, 0);
        assert_eq!(report.mutation_count, 0);
        assert!(!report.history_preserved);
        assert_eq!(migrated.commit_id(), CommitId(0));
        assert!(migrated.catalog.tables().next().is_none());
        migrated.checkpoint().expect("empty checkpoint");
        migrated.verify().expect("empty verify");
        drop(migrated);

        let reopened =
            SeerRelationalStore::<SeerKernel>::open(config).expect("reopen empty migration");
        assert_eq!(reopened.commit_id(), CommitId(0));
        assert!(reopened.catalog.tables().next().is_none());
    }

    #[test]
    fn failed_legacy_migration_removes_staging_and_destination() {
        let parent = tempfile::tempdir().expect("temporary directory");
        let source_directory = parent.path().join("legacy");
        let target_directory = parent.path().join("seerdb");
        let mut source = RelationalStore::create(crate::DatabaseConfig {
            directory: source_directory,
        })
        .expect("create legacy store");
        source.create_table(table()).expect("legacy table");
        source
            .commit_batch([RelationalMutation::Insert {
                table: TableId(7),
                row: row(1, "alice@example.com", 30),
            }])
            .expect("legacy row");

        let mut config = SeerKernelConfig::new(target_directory.clone());
        config.options.max_wal_bytes = 1;
        let result = SeerRelationalStore::migrate_from_legacy(&source, config);
        assert!(
            result.is_err(),
            "migration should fail its WAL admission gate"
        );
        assert!(!target_directory.exists());

        let target_name = target_directory
            .file_name()
            .and_then(|name| name.to_str())
            .expect("target name");
        let staging_prefix = format!(".{target_name}.seerdb-migration-");
        assert!(
            fs::read_dir(parent.path())
                .expect("read migration parent")
                .filter_map(std::result::Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&staging_prefix)),
            "failed migration left a staging directory"
        );
    }

    #[test]
    fn legacy_migration_refuses_existing_destination_before_source_read() {
        let parent = tempfile::tempdir().expect("temporary directory");
        let source_directory = parent.path().join("legacy");
        let target_directory = parent.path().join("seerdb");
        let mut source = RelationalStore::create(crate::DatabaseConfig {
            directory: source_directory,
        })
        .expect("create legacy store");
        source.create_table(table()).expect("legacy table");
        let source_commit = source.commit_id();
        let source_catalog = encode_catalog(source.catalog()).expect("source catalog");
        fs::create_dir(&target_directory).expect("create existing destination");

        let result = SeerRelationalStore::migrate_from_legacy(
            &source,
            SeerKernelConfig::new(target_directory.clone()),
        );
        assert!(result.is_err(), "existing destination must be refused");
        assert!(target_directory.is_dir());
        assert_eq!(source.commit_id(), source_commit);
        assert_eq!(
            encode_catalog(source.catalog()).expect("source catalog after refusal"),
            source_catalog
        );
    }

    #[test]
    fn legacy_migration_preserves_foreign_key_catalog_and_rows() {
        let parent_directory = tempfile::tempdir().expect("temporary directory");
        let source_directory = parent_directory.path().join("legacy");
        let target_directory = parent_directory.path().join("seerdb");
        let mut source = RelationalStore::create(crate::DatabaseConfig {
            directory: source_directory,
        })
        .expect("create legacy store");
        let parent = TableDefinition {
            id: TableId(80),
            name: "parents".to_owned(),
            columns: vec![ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            }],
        };
        let child = TableDefinition {
            id: TableId(81),
            name: "children".to_owned(),
            columns: vec![ColumnDefinition {
                id: ColumnId(1),
                name: "parent_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            }],
        };
        source.create_table(parent).expect("parent table");
        source.create_table(child).expect("child table");
        source
            .create_index(IndexDefinition {
                id: IndexId(80),
                table: TableId(80),
                columns: vec![ColumnId(1)],
                unique: true,
            })
            .expect("parent index");
        source
            .create_foreign_key(ForeignKeyDefinition {
                id: ConstraintId(80),
                table: TableId(81),
                columns: vec![ColumnId(1)],
                referenced_table: TableId(80),
                referenced_columns: vec![ColumnId(1)],
                on_delete: crate::relational::ReferentialAction::default(),
                timing: crate::relational::ConstraintTiming::default(),
            })
            .expect("foreign key");
        source
            .commit_batch([
                RelationalMutation::Insert {
                    table: TableId(80),
                    row: Row {
                        primary: Key::new(80, 1),
                        values: vec![Value::U64(1)],
                    },
                },
                RelationalMutation::Insert {
                    table: TableId(81),
                    row: Row {
                        primary: Key::new(81, 1),
                        values: vec![Value::U64(1)],
                    },
                },
            ])
            .expect("legacy rows");

        let config = SeerKernelConfig::new(target_directory);
        let (migrated, report) =
            SeerRelationalStore::migrate_from_legacy(&source, config).expect("migrate");
        assert_eq!(report.table_count, 2);
        assert_eq!(report.row_count, 2);
        assert_eq!(migrated.catalog.foreign_keys().count(), 1);
        assert_eq!(
            migrated
                .get(TableId(81), migrated.commit_id(), Key::new(81, 1))
                .expect("migrated child"),
            Some(Row {
                primary: Key::new(81, 1),
                values: vec![Value::U64(1)],
            })
        );
    }

    #[test]
    fn transaction_snapshot_is_owned_until_abort() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");
        let transaction = store.begin().expect("begin");
        assert_eq!(transaction.snapshot(), store.commit_id());
        transaction.abort(&mut store).expect("abort");
    }

    #[test]
    fn transaction_helper_commits_writes_and_skips_read_only_commit() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");
        let primary = Key::new(7, 91);
        let row = row(91, "transaction@example.com", 30);

        let (visible, commit) = store
            .transaction(|store, transaction| {
                transaction.insert(store, TableId(7), row.clone())?;
                transaction.get(store, TableId(7), primary)
            })
            .expect("transaction");
        assert_eq!(visible, Some(row));
        assert_eq!(commit, CommitId(2));

        let (count, read_only_commit) = store
            .transaction(|store, transaction| {
                transaction
                    .scan(store, TableId(7), 10)
                    .map(|rows| rows.len())
            })
            .expect("read-only transaction");
        assert_eq!(count, 1);
        assert_eq!(read_only_commit, commit);
        assert_eq!(store.commit_id(), commit);

        let failed = store.transaction(|store, transaction| -> crate::Result<()> {
            transaction.delete(store, TableId(7), primary)?;
            Err(DbError::InvalidState("abort from closure".to_owned()))
        });
        assert!(
            matches!(failed, Err(DbError::InvalidState(reason)) if reason == "abort from closure")
        );
        assert_eq!(store.commit_id(), commit);
        assert!(
            store
                .get(TableId(7), commit, primary)
                .expect("row")
                .is_some()
        );
    }

    #[test]
    fn direct_empty_commits_are_backend_neutral_read_only_boundaries() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");
        let current = store.commit_id();

        assert_eq!(
            store.commit_batch(std::iter::empty()).expect("empty batch"),
            current
        );
        let transaction = store.begin().expect("begin");
        assert_eq!(
            transaction.commit(&mut store).expect("empty transaction"),
            current
        );
        let attempt = TransactionAttemptId::new([7; 16]);
        let transaction = store.begin().expect("begin for attempt");
        assert_eq!(
            transaction
                .commit_with_attempt(&mut store, attempt)
                .expect("empty attempt transaction"),
            current
        );
        assert_eq!(store.resolve_attempt(attempt).expect("resolve"), None);
        assert_eq!(store.commit_id(), current);
    }

    #[test]
    fn typed_attempt_api_persists_and_forgets_cleanup_batch() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");
        let attempt = crate::TransactionAttemptId::new([12; 16]);
        let commit = store
            .commit_batch_with_attempt(
                [RelationalMutation::Insert {
                    table: TableId(7),
                    row: row(91, "attempt@example.com", 31),
                }],
                attempt,
            )
            .expect("attempt commit");
        assert_eq!(
            store
                .resolve_attempt(attempt)
                .expect("resolve")
                .expect("record")
                .commit,
            commit
        );
        assert_eq!(
            store.forget_attempts(&[attempt, attempt]).expect("forget"),
            1
        );
        assert!(store.resolve_attempt(attempt).expect("resolve").is_none());
    }

    #[test]
    fn current_transaction_read_view_does_not_rewalk_the_current_tree() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");
        store
            .commit_batch((1..=128).map(|id| RelationalMutation::Insert {
                table: TableId(7),
                row: row(id, &format!("user-{id}@example.com"), id),
            }))
            .expect("seed rows");

        let before = store.metrics().expect("metrics before begin");
        let transaction = store.begin().expect("begin");
        let after = store.metrics().expect("metrics after begin");
        assert_eq!(
            after.storage.logical_page_reads, before.storage.logical_page_reads,
            "current transaction pin must not traverse the entire B-tree"
        );
        transaction.abort(&mut store).expect("abort");
    }

    #[test]
    fn transaction_uses_process_local_view_without_durable_lease() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory.path().join("seerdb");
        let config = SeerKernelConfig::new(database_path.clone());
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");

        let retained_files = || {
            fs::read_dir(&database_path)
                .expect("read database directory")
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| entry.file_name().into_string().ok())
                .filter(|name| name.starts_with("seerdb.blob.retained."))
                .collect::<BTreeSet<_>>()
        };
        let before = retained_files();
        let transaction = store.begin().expect("begin");
        assert_eq!(store.kernel.active_lease_count(), 0);
        assert_eq!(retained_files(), before);
        drop(transaction);
        assert_eq!(store.kernel.active_lease_count(), 0);
        assert_eq!(retained_files(), before);
    }

    #[test]
    fn concurrent_read_transactions_begin_from_shared_store() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");
        store
            .commit_batch([RelationalMutation::Insert {
                table: TableId(7),
                row: row(1, "alice@example.com", 30),
            }])
            .expect("seed row");
        let store = Arc::new(store);

        std::thread::scope(|scope| {
            for _ in 0..2 {
                let store = Arc::clone(&store);
                scope.spawn(move || {
                    let transaction = store.begin().expect("begin shared transaction");
                    assert_eq!(
                        transaction
                            .scan(store.as_ref(), TableId(7), 10)
                            .expect("shared transaction scan"),
                        vec![row(1, "alice@example.com", 30)]
                    );
                });
            }
        });
    }

    #[test]
    fn foreign_keys_validate_final_batches_and_reject_orphans() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        let parent = TableDefinition {
            id: TableId(80),
            name: "parents".to_owned(),
            columns: vec![ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            }],
        };
        let child = TableDefinition {
            id: TableId(81),
            name: "children".to_owned(),
            columns: vec![ColumnDefinition {
                id: ColumnId(1),
                name: "parent_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            }],
        };
        store.create_table(parent).expect("parent table");
        store.create_table(child).expect("child table");
        store
            .create_index(IndexDefinition {
                id: IndexId(80),
                table: TableId(80),
                columns: vec![ColumnId(1)],
                unique: true,
            })
            .expect("parent unique index");
        store
            .create_foreign_key(ForeignKeyDefinition {
                id: ConstraintId(80),
                table: TableId(81),
                columns: vec![ColumnId(1)],
                referenced_table: TableId(80),
                referenced_columns: vec![ColumnId(1)],
                on_delete: crate::relational::ReferentialAction::default(),
                timing: crate::relational::ConstraintTiming::default(),
            })
            .expect("foreign key");

        let parent_key = Key::new(80, 1);
        let child_key = Key::new(81, 1);
        store
            .commit_batch([
                RelationalMutation::Insert {
                    table: TableId(80),
                    row: Row {
                        primary: parent_key,
                        values: vec![Value::U64(1)],
                    },
                },
                RelationalMutation::Insert {
                    table: TableId(81),
                    row: Row {
                        primary: child_key,
                        values: vec![Value::U64(1)],
                    },
                },
            ])
            .expect("parent and child atomic batch");
        let before = store.commit_id();
        assert!(matches!(
            store.commit_batch([RelationalMutation::Insert {
                table: TableId(81),
                row: Row {
                    primary: Key::new(81, 2),
                    values: vec![Value::U64(999)],
                },
            }]),
            Err(DbError::ForeignKeyViolation {
                constraint: 80,
                table: 81,
                referenced_table: 80,
            })
        ));
        assert_eq!(store.commit_id(), before);
        assert!(matches!(
            store.commit_batch([RelationalMutation::Delete {
                table: TableId(80),
                primary: parent_key,
            }]),
            Err(DbError::ForeignKeyViolation {
                constraint: 80,
                table: 81,
                referenced_table: 80,
            })
        ));
        assert!(
            store
                .get(TableId(80), before, parent_key)
                .expect("parent remains")
                .is_some()
        );
    }

    #[test]
    fn retained_transaction_reads_old_root_and_rejects_stale_commit() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");

        let mut old = store.begin().expect("old begin");
        let mut later = store.begin().expect("later begin");
        let later_row = row(2, "later@example.com", 20);
        later
            .insert(&store, TableId(7), later_row.clone())
            .expect("later insert");
        assert_eq!(later.commit(&mut store).expect("later commit"), CommitId(2));

        assert_eq!(
            old.get(&store, TableId(7), later_row.primary)
                .expect("historical read"),
            None
        );
        let old_row = row(1, "old@example.com", 10);
        old.insert(&store, TableId(7), old_row).expect("old insert");
        assert!(matches!(
            old.commit(&mut store),
            Err(DbError::SerializationConflict {
                snapshot: 1,
                current: 2
            })
        ));
        assert_eq!(store.commit_id(), CommitId(2));
    }

    #[test]
    fn historical_reads_use_the_catalog_at_the_retained_commit() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");
        store
            .commit_batch([RelationalMutation::Insert {
                table: TableId(7),
                row: row(1, "alice@example.com", 30),
            }])
            .expect("row");

        let historical = store.retain(CommitId(2)).expect("retain old schema");
        store.create_index(index()).expect("index");

        assert!(matches!(
            store.index_scan(TableId(7), CommitId(2), IndexId(9), None, None, 10),
            Err(DbError::InvalidState(message)) if message.contains("does not exist")
        ));
        store.release(historical).expect("release old schema");
    }

    #[test]
    fn typed_boundary_exposes_storage_metrics_and_bounded_reclaim() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config).expect("create");
        store.create_table(table()).expect("table");

        let mutations = (0..64)
            .map(|id| RelationalMutation::Insert {
                table: TableId(7),
                row: row(id, &format!("user-{id}@example.com"), id),
            })
            .collect::<Vec<_>>();
        store.commit_batch(mutations).expect("seed rows");

        let metrics = store.metrics().expect("storage metrics");
        assert!(metrics.data_bytes > 0);
        let report = store.compact_with_limit(1).expect("bounded compaction");
        assert!(report.relocated_pages <= 1);
    }

    #[cfg(feature = "seerdb-fault-injection")]
    #[test]
    fn native_publication_fault_matrix_reopens_old_or_complete_new_typed_state() {
        // WalTruncate is retired from the authoritative surface: under WAL
        // retention, log removal is threshold-gated cleanup after the
        // manifest has selected the generation, so its failure is benign by
        // design (recovery discards the stale log) and must not fail a
        // publication.
        const MATRIX: [FaultPoint; 11] = [
            FaultPoint::BeforeWalAppend,
            FaultPoint::AfterWalAppend,
            FaultPoint::WalSync,
            FaultPoint::AfterWalSync,
            FaultPoint::DataSync,
            FaultPoint::PackedPageSync,
            FaultPoint::ManifestMirrorSync,
            FaultPoint::ManifestSync,
            FaultPoint::AfterManifestPublish,
            FaultPoint::ShortWrite,
            FaultPoint::TornWrite,
        ];

        for point in MATRIX {
            let directory = tempfile::tempdir().expect("temporary directory");
            let config = SeerKernelConfig::new(directory.path().join("seerdb"));
            let mut store =
                SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
            store.create_table(table()).expect("table");
            store.create_index(index()).expect("index");
            let baseline = store.commit_id();

            store.inject_fault(point).expect("arm fault");
            let result = store.commit_batch([RelationalMutation::Insert {
                table: TableId(7),
                row: row(1, "fault@example.com", 42),
            }]);
            assert!(result.is_err(), "fault {point:?} did not fail the commit");
            drop(store);

            let reopened = SeerRelationalStore::<SeerKernel>::open(config)
                .unwrap_or_else(|error| panic!("reopen after {point:?}: {error}"));
            let recovered = reopened.commit_id();
            let old_generation = recovered == baseline;
            let new_generation = recovered == CommitId(baseline.0 + 1);
            assert!(
                old_generation || new_generation,
                "fault {point:?} recovered unexpected commit {} from baseline {}",
                recovered.0,
                baseline.0
            );

            let row_visible = reopened
                .get(TableId(7), recovered, Key::new(7, 1))
                .expect("typed row read")
                .is_some();
            let index_visible = !reopened
                .index_get(
                    TableId(7),
                    recovered,
                    IndexId(9),
                    &[Value::Text("fault@example.com".to_owned())],
                )
                .expect("typed index read")
                .is_empty();
            assert_eq!(row_visible, new_generation, "fault {point:?} partial row");
            assert_eq!(
                index_visible, new_generation,
                "fault {point:?} partial index"
            );
        }
    }

    #[cfg(feature = "seerdb-fault-injection")]
    #[test]
    fn composite_identity_catalog_and_rows_reopen_old_or_complete_new_after_faults() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut store = SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
        store
            .inject_fault(FaultPoint::AfterWalSync)
            .expect("arm schema fault");
        assert!(
            store
                .create_table_with_schema_and_primary_key(
                    composite_table(),
                    Some(vec![ColumnId(1), ColumnId(2)]),
                    composite_schema(),
                )
                .is_err()
        );
        drop(store);

        let mut reopened = SeerRelationalStore::<SeerKernel>::open(config).expect("reopen");
        let table_exists = reopened.catalog().table(TableId(70)).is_ok();
        let index_count = reopened.catalog().indexes_for(TableId(70)).count();
        assert_eq!(table_exists, index_count == 2);
        if table_exists {
            assert_eq!(
                reopened.catalog().primary_key(TableId(70)),
                Some([ColumnId(1), ColumnId(2)].as_slice())
            );
            assert_eq!(
                reopened.catalog().index_name(IndexId(80)),
                Some("ledger_pk")
            );
            assert_eq!(
                reopened.catalog().index_name(IndexId(81)),
                Some("ledger_state")
            );
            reopened.verify().expect("verify recovered catalog");
        }

        // WalTruncate is retired from the authoritative surface: under WAL
        // retention, log removal is threshold-gated cleanup after the
        // manifest has selected the generation, so its failure is benign by
        // design (recovery discards the stale log) and must not fail a
        // publication.
        const MATRIX: [FaultPoint; 11] = [
            FaultPoint::BeforeWalAppend,
            FaultPoint::AfterWalAppend,
            FaultPoint::WalSync,
            FaultPoint::AfterWalSync,
            FaultPoint::DataSync,
            FaultPoint::PackedPageSync,
            FaultPoint::ManifestMirrorSync,
            FaultPoint::ManifestSync,
            FaultPoint::AfterManifestPublish,
            FaultPoint::ShortWrite,
            FaultPoint::TornWrite,
        ];
        for point in MATRIX {
            let directory = tempfile::tempdir().expect("temporary directory");
            let config = SeerKernelConfig::new(directory.path().join("seerdb"));
            let mut store =
                SeerRelationalStore::<SeerKernel>::create(config.clone()).expect("create");
            store
                .create_table_with_schema_and_primary_key(
                    composite_table(),
                    Some(vec![ColumnId(1), ColumnId(2)]),
                    composite_schema(),
                )
                .expect("composite schema");
            let baseline = store
                .commit_batch([RelationalMutation::Insert {
                    table: TableId(70),
                    row: composite_row(1, "open"),
                }])
                .expect("old row");
            store.inject_fault(point).expect("arm row fault");
            assert!(
                store
                    .commit_batch([RelationalMutation::Insert {
                        table: TableId(70),
                        row: composite_row(2, "closed"),
                    }])
                    .is_err(),
                "fault {point:?} unexpectedly succeeded"
            );
            drop(store);

            let mut reopened = SeerRelationalStore::<SeerKernel>::open(config)
                .unwrap_or_else(|error| panic!("reopen after {point:?}: {error}"));
            reopened.verify().expect("verify recovered composite state");
            assert_eq!(
                reopened.catalog().primary_key(TableId(70)),
                Some([ColumnId(1), ColumnId(2)].as_slice())
            );
            let recovered = reopened.commit_id();
            let new_generation = recovered == CommitId(baseline.0 + 1);
            assert!(
                recovered == baseline || new_generation,
                "fault {point:?} recovered unexpected commit {} from baseline {}",
                recovered.0,
                baseline.0
            );
            assert_eq!(
                reopened
                    .scan(TableId(70), recovered, usize::MAX)
                    .expect("scan recovered rows")
                    .len(),
                if new_generation { 2 } else { 1 }
            );
            assert_eq!(
                reopened
                    .index_get(
                        TableId(70),
                        recovered,
                        IndexId(81),
                        &[Value::Text("closed".to_owned())],
                    )
                    .expect("recovered composite index")
                    .len(),
                usize::from(new_generation)
            );
        }
    }
}

#[cfg(test)]
mod kernel_swap_tests {
    use super::*;
    use crate::kernel::InMemoryKernel;

    fn users_table() -> crate::TableDefinition {
        crate::TableDefinition {
            id: TableId(7),
            name: "users".to_owned(),
            columns: vec![crate::ColumnDefinition {
                id: ColumnId(1),
                name: "email".to_owned(),
                data_type: crate::ColumnType::Text,
                nullable: false,
            }],
        }
    }

    #[test]
    fn relational_store_runs_over_the_in_memory_kernel() {
        let kernel = InMemoryKernel::new();
        let mut store = SeerRelationalStore::from_kernel(kernel).expect("store");
        store.create_table(users_table()).expect("create table");
        let row = Row {
            primary: Key::new(7, 1),
            values: vec![crate::Value::Text("a@b.c".to_owned())],
        };
        let commit = store
            .commit_batch([RelationalMutation::Insert {
                table: TableId(7),
                row,
            }])
            .expect("insert");
        assert!(commit.0 >= 2);

        // The catalog survives through the kernel: a fresh handle over the
        // same kernel sees the table without replaying mutations.
        assert!(
            store
                .catalog()
                .tables()
                .any(|table| table.name == "users" && table.id == TableId(7))
        );
    }
}
