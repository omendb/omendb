//! Direct qualification path from OmenDB's relational model to SeerDB.
//!
//! This module intentionally bypasses the transitional `StorageKernel` seam.
//! It is not wired into `RelationalDatabase::Backend::Seer` yet: the existing
//! facade still promises historical leases, archive/restore, and physical
//! status projections that the first SeerDB transaction API does not provide.
//! The direct path proves the planned ownership boundary with one catalog
//! tree, one tree per table, and one tree per index.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use seerdb::{CommitSeq, Error as SeerError, Options, Transaction, TransactionDatabase, TreeId};

use crate::relational::{
    Catalog, ColumnId, ForeignKeyDefinition, IndexDefinition, RelationalSchemaDefinition, Row,
    TableDefinition, TableId, Value, encode_catalog, encode_row, foreign_key_values,
    index_values_key, row_from_storage_identity, row_identity_bytes, row_index_key,
};
use crate::{DbError, IndexId, Result};

const DIRECT_CATALOG_MAGIC: &[u8; 4] = b"ODC1";
const DIRECT_CATALOG_MARKER: &[u8] = b"\x00omendb/direct/catalog/v1";
const MAX_CATALOG_BYTES: usize = 16 * 1024 * 1024;
const MAX_CATALOG_OBJECTS: usize = 1_000_000;

/// The direct SeerDB-backed relational qualification store.
///
/// It owns OmenDB's logical catalog and the mapping from relational objects
/// to SeerDB trees. SeerDB owns transaction identity, snapshot visibility,
/// write conflicts, and atomic publication.
pub(crate) struct DirectSeerStore {
    database: TransactionDatabase,
    catalog_tree: TreeId,
    catalog: Catalog,
    table_trees: BTreeMap<TableId, TreeId>,
    index_trees: BTreeMap<IndexId, TreeId>,
}

impl DirectSeerStore {
    /// Create an empty direct-format history and atomically publish its
    /// catalog tree marker.
    pub(crate) fn create<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let database = TransactionDatabase::create(path, options).map_err(map_seer_error)?;
        let mut transaction = database.begin().map_err(map_seer_error)?;
        let catalog_tree = transaction.create_tree().map_err(map_seer_error)?;
        let catalog = Catalog::default();
        let state = encode_catalog_state(&catalog, &BTreeMap::new(), &BTreeMap::new())?;
        transaction
            .put(catalog_tree, DIRECT_CATALOG_MARKER, &state)
            .map_err(map_seer_error)?;
        transaction.commit().map_err(map_seer_error)?;
        drop(transaction);
        Ok(Self {
            database,
            catalog_tree,
            catalog,
            table_trees: BTreeMap::new(),
            index_trees: BTreeMap::new(),
        })
    }

    /// Reopen and validate a direct-format history.
    pub(crate) fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let database = TransactionDatabase::open(path, options).map_err(map_seer_error)?;
        let mut transaction = database.begin().map_err(map_seer_error)?;
        let trees = transaction.list_trees().map_err(map_seer_error)?;
        let mut catalog_tree = None;
        let mut state = None;
        for tree in trees {
            if let Some(bytes) = transaction
                .get(tree, DIRECT_CATALOG_MARKER)
                .map_err(map_seer_error)?
            {
                if catalog_tree.replace(tree).is_some() {
                    return Err(DbError::Corruption {
                        artifact: "direct SeerDB catalog",
                        reason: "multiple catalog tree markers exist".to_owned(),
                    });
                }
                state = Some(decode_catalog_state(&bytes)?);
            }
        }
        let catalog_tree = catalog_tree.ok_or_else(|| DbError::Corruption {
            artifact: "direct SeerDB catalog",
            reason: "catalog tree marker is missing".to_owned(),
        })?;
        let (catalog, table_trees, index_trees) = state.ok_or_else(|| DbError::Corruption {
            artifact: "direct SeerDB catalog",
            reason: "catalog state is missing".to_owned(),
        })?;
        validate_mapping(
            &transaction,
            catalog_tree,
            &catalog,
            &table_trees,
            &index_trees,
        )?;
        transaction.abort().map_err(map_seer_error)?;
        drop(transaction);
        Ok(Self {
            database,
            catalog_tree,
            catalog,
            table_trees,
            index_trees,
        })
    }

    /// Flush and close the direct history.
    pub(crate) fn close(&self) -> Result<()> {
        self.database.close().map_err(map_seer_error)
    }

    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub(crate) fn table_tree(&self, table: TableId) -> Result<TreeId> {
        self.table_trees
            .get(&table)
            .copied()
            .ok_or_else(|| DbError::InvalidState(format!("table {} has no SeerDB tree", table.0)))
    }

    pub(crate) fn index_tree(&self, index: IndexId) -> Result<TreeId> {
        self.index_trees
            .get(&index)
            .copied()
            .ok_or_else(|| DbError::InvalidState(format!("index {} has no SeerDB tree", index.0)))
    }

    /// Create one table and its physical tree in one transaction.
    pub(crate) fn create_table(
        &mut self,
        table: TableDefinition,
        primary_key: Option<Vec<ColumnId>>,
    ) -> Result<CommitSeq> {
        let mut candidate = self.catalog.clone();
        candidate.create_table_with_primary_key(table.clone(), primary_key)?;
        let mut transaction = self.database.begin().map_err(map_seer_error)?;
        let tree = transaction.create_tree().map_err(map_seer_error)?;
        let mut table_trees = self.table_trees.clone();
        table_trees.insert(table.id, tree);
        let state = encode_catalog_state(&candidate, &table_trees, &self.index_trees)?;
        transaction
            .put(self.catalog_tree, DIRECT_CATALOG_MARKER, &state)
            .map_err(map_seer_error)?;
        let commit = transaction.commit().map_err(map_seer_error)?.csn;
        drop(transaction);
        self.catalog = candidate;
        self.table_trees = table_trees;
        Ok(commit)
    }

    /// Create one table with its secondary schema objects and physical
    /// trees in one transaction. Index trees stay empty because the new
    /// table has no rows yet.
    pub(crate) fn create_table_with_schema_and_primary_key(
        &mut self,
        table: TableDefinition,
        primary_key: Option<Vec<ColumnId>>,
        schema: RelationalSchemaDefinition,
    ) -> Result<CommitSeq> {
        let mut candidate = self.catalog.clone();
        candidate.create_table_with_primary_key(table.clone(), primary_key)?;
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
        let mut transaction = self.database.begin().map_err(map_seer_error)?;
        let tree = transaction.create_tree().map_err(map_seer_error)?;
        let mut table_trees = self.table_trees.clone();
        table_trees.insert(table.id, tree);
        let mut index_trees = self.index_trees.clone();
        for named in &schema.indexes {
            let index_tree = transaction.create_tree().map_err(map_seer_error)?;
            index_trees.insert(named.definition.id, index_tree);
        }
        let state = encode_catalog_state(&candidate, &table_trees, &index_trees)?;
        transaction
            .put(self.catalog_tree, DIRECT_CATALOG_MARKER, &state)
            .map_err(map_seer_error)?;
        let commit = transaction.commit().map_err(map_seer_error)?.csn;
        drop(transaction);
        self.catalog = candidate;
        self.table_trees = table_trees;
        self.index_trees = index_trees;
        Ok(commit)
    }

    /// Create one secondary index and build its entries atomically with the
    /// catalog mapping. The first direct path uses a deterministic encoded
    /// entry key and scans it for exact-value lookup; ordered index cursors
    /// are a later optimization once the physical key codec is benchmarked.
    pub(crate) fn create_index(&mut self, index: IndexDefinition) -> Result<CommitSeq> {
        self.create_index_with_name(index, None)
    }

    /// Create one secondary index while retaining its SQL object name in
    /// the catalog.
    pub(crate) fn create_named_index(
        &mut self,
        index: IndexDefinition,
        name: String,
    ) -> Result<CommitSeq> {
        self.create_index_with_name(index, Some(name))
    }

    fn create_index_with_name(
        &mut self,
        index: IndexDefinition,
        name: Option<String>,
    ) -> Result<CommitSeq> {
        if self.catalog.index(index.id) == Some(&index) {
            return self.database.commit_sequence().map_err(map_seer_error);
        }
        let mut candidate = self.catalog.clone();
        match &name {
            Some(name) => candidate.create_named_index(index.clone(), name.clone())?,
            None => candidate.create_index(index.clone())?,
        }
        let table_tree = self.table_tree(index.table)?;
        let table = candidate.table(index.table)?.clone();
        let mut transaction = self.database.begin().map_err(map_seer_error)?;
        let index_tree = transaction.create_tree().map_err(map_seer_error)?;
        let mut seen_unique = BTreeSet::new();
        for (identity, bytes) in transaction
            .scan(table_tree, &[], None, usize::MAX)
            .map_err(map_seer_error)?
        {
            let row = row_from_storage_identity(&candidate, &table, &identity, &bytes)?;
            if let Some(values) = row_index_key(&table, &index, &row)? {
                if index.unique && !seen_unique.insert(values.clone()) {
                    return Err(DbError::UniqueViolation {
                        index: index.id.0,
                        key: values,
                    });
                }
                let key = encode_index_entry(&values, &identity)?;
                transaction
                    .put(index_tree, &key, &identity)
                    .map_err(map_seer_error)?;
            }
        }
        let mut index_trees = self.index_trees.clone();
        index_trees.insert(index.id, index_tree);
        let state = encode_catalog_state(&candidate, &self.table_trees, &index_trees)?;
        transaction
            .put(self.catalog_tree, DIRECT_CATALOG_MARKER, &state)
            .map_err(map_seer_error)?;
        let commit = transaction.commit().map_err(map_seer_error)?.csn;
        drop(transaction);
        self.catalog = candidate;
        self.index_trees = index_trees;
        Ok(commit)
    }

    /// Append one nullable column atomically. Existing rows expose a
    /// logical `NULL` for the new field without a table-sized rewrite.
    pub(crate) fn add_nullable_column(
        &mut self,
        table: TableId,
        column: crate::ColumnDefinition,
    ) -> Result<CommitSeq> {
        let candidate = {
            let mut candidate = self.catalog.clone();
            candidate.add_nullable_column(table, column)?;
            candidate
        };
        self.publish_catalog(candidate)
    }

    /// Validate and publish one foreign-key definition.
    pub(crate) fn create_foreign_key(
        &mut self,
        foreign_key: ForeignKeyDefinition,
    ) -> Result<CommitSeq> {
        self.create_foreign_key_with_name(foreign_key, None)
    }

    /// Validate and publish one foreign-key definition with an object name.
    pub(crate) fn create_named_foreign_key(
        &mut self,
        foreign_key: ForeignKeyDefinition,
        name: String,
    ) -> Result<CommitSeq> {
        self.create_foreign_key_with_name(foreign_key, Some(name))
    }

    fn create_foreign_key_with_name(
        &mut self,
        foreign_key: ForeignKeyDefinition,
        name: Option<String>,
    ) -> Result<CommitSeq> {
        let candidate = {
            let mut candidate = self.catalog.clone();
            match &name {
                Some(name) => {
                    candidate.create_named_foreign_key(foreign_key.clone(), name.clone())?
                }
                None => candidate.create_foreign_key(foreign_key.clone())?,
            }
            candidate
        };
        self.begin_transaction()?
            .validate_one_foreign_key(&foreign_key)?;
        self.publish_catalog(candidate)
    }

    fn publish_catalog(&mut self, candidate: Catalog) -> Result<CommitSeq> {
        let mut transaction = self.database.begin().map_err(map_seer_error)?;
        let state = encode_catalog_state(&candidate, &self.table_trees, &self.index_trees)?;
        transaction
            .put(self.catalog_tree, DIRECT_CATALOG_MARKER, &state)
            .map_err(map_seer_error)?;
        let commit = transaction.commit().map_err(map_seer_error)?.csn;
        drop(transaction);
        self.catalog = candidate;
        Ok(commit)
    }

    /// Insert one row and all derived index entries atomically.
    pub(crate) fn insert(&self, table: TableId, row: Row) -> Result<CommitSeq> {
        let mut transaction = self.begin_transaction()?;
        transaction.insert(table, row)?;
        transaction.commit()
    }

    /// Replace one row by its identity and refresh derived index entries.
    pub(crate) fn update(&self, table: TableId, row: Row) -> Result<CommitSeq> {
        let mut transaction = self.begin_transaction()?;
        transaction.update(table, row)?;
        transaction.commit()
    }

    /// Delete one row and its derived index entries atomically.
    pub(crate) fn delete(&self, table: TableId, identity: &[u8]) -> Result<CommitSeq> {
        let mut transaction = self.begin_transaction()?;
        transaction.delete(table, identity)?;
        transaction.commit()
    }

    /// Read one row through a transaction-scoped fixed snapshot.
    pub(crate) fn get(&self, table: TableId, identity: &[u8]) -> Result<Option<Row>> {
        let definition = self.catalog.table(table)?.clone();
        let tree = self.table_tree(table)?;
        let mut transaction = self.database.begin().map_err(map_seer_error)?;
        let result = transaction
            .get(tree, identity)
            .map_err(map_seer_error)?
            .map(|bytes| row_from_storage_identity(&self.catalog, &definition, identity, &bytes))
            .transpose()?;
        transaction.abort().map_err(map_seer_error)?;
        Ok(result)
    }

    /// Scan all rows in one table in SeerDB key order.
    pub(crate) fn scan(&self, table: TableId) -> Result<Vec<Row>> {
        let definition = self.catalog.table(table)?.clone();
        let tree = self.table_tree(table)?;
        let mut transaction = self.database.begin().map_err(map_seer_error)?;
        let rows = transaction
            .scan(tree, &[], None, usize::MAX)
            .map_err(map_seer_error)?
            .into_iter()
            .map(|(identity, bytes)| {
                row_from_storage_identity(&self.catalog, &definition, &identity, &bytes)
            })
            .collect::<Result<Vec<_>>>()?;
        transaction.abort().map_err(map_seer_error)?;
        Ok(rows)
    }

    /// Return rows matching exact values in a secondary index.
    pub(crate) fn index_get(
        &self,
        table: TableId,
        index: IndexId,
        values: &[crate::Value],
    ) -> Result<Vec<Row>> {
        let definition = self.catalog.table(table)?.clone();
        let index_definition = self
            .catalog
            .index(index)
            .ok_or_else(|| DbError::InvalidState(format!("index {} does not exist", index.0)))?;
        if index_definition.table != table {
            return Err(DbError::InvalidState(format!(
                "index {} does not belong to table {}",
                index.0, table.0
            )));
        }
        let value_key = index_values_key(&definition, index_definition, values)?;
        let mut transaction = self.begin_transaction()?;
        let identities = transaction.index_identities_for(index, &value_key)?;
        let mut rows = Vec::with_capacity(identities.len());
        for row_identity in identities {
            rows.push(transaction.get(table, &row_identity)?.ok_or_else(|| {
                DbError::Corruption {
                    artifact: "direct SeerDB index",
                    reason: "index entry references a missing row".to_owned(),
                }
            })?);
        }
        Ok(rows)
    }

    /// Begin an explicit multi-statement transaction over the mapped trees.
    pub(crate) fn begin_transaction(&self) -> Result<DirectTransaction> {
        DirectTransaction::begin(self)
    }

    /// Return engine-level metrics including publication timing.
    pub(crate) fn metrics(&self) -> seerdb::DBMetrics {
        self.database
            .metrics()
            .expect("open database reports metrics")
    }

    /// Return the latest published commit sequence number.
    pub(crate) fn commit_seq(&self) -> CommitSeq {
        self.database
            .commit_sequence()
            .expect("open database reports its CSN")
    }

    /// Look up one row through its encoded composite identity.
    pub(crate) fn get_by_identity(&self, table: TableId, identity: &[u8]) -> Result<Option<Row>> {
        self.get(table, identity)
    }

    /// Read all rows reachable through one index in index-key order.
    pub(crate) fn index_scan(&self, table: TableId, index: IndexId) -> Result<Vec<Row>> {
        let mut transaction = self.begin_transaction()?;
        transaction.index_scan(table, index)
    }

    fn stage_indexes(
        &self,
        transaction: &mut Transaction,
        table: TableId,
        definition: &TableDefinition,
        row: &Row,
        identity: &[u8],
        insert: bool,
    ) -> Result<()> {
        let applicable: Vec<crate::relational::IndexDefinition> =
            self.catalog.indexes_for(table).cloned().collect();
        for index in applicable {
            let Some(values) = row_index_key(definition, &index, row)? else {
                continue;
            };
            let tree = self.index_tree(index.id)?;
            let key = encode_index_entry(&values, identity)?;
            if insert && index.unique {
                let mut existing = None;
                for (entry, existing_identity) in transaction
                    .scan(tree, &[], None, usize::MAX)
                    .map_err(map_seer_error)?
                {
                    let (existing_values, existing_identity) =
                        decode_index_entry(&entry, &existing_identity)?;
                    if existing_values == values {
                        existing = Some(existing_identity);
                        break;
                    }
                }
                if existing.is_some() {
                    return Err(DbError::UniqueViolation {
                        index: index.id.0,
                        key: values,
                    });
                }
            }
            if insert {
                transaction
                    .put(tree, &key, identity)
                    .map_err(map_seer_error)?;
            } else {
                transaction.delete(tree, &key).map_err(map_seer_error)?;
            }
        }
        Ok(())
    }
}

/// An explicit multi-operation transaction over the direct store's mapped
/// trees. Reads resolve at one fixed snapshot and observe the transaction's
/// own staged writes; `commit` publishes every staged row, index, and catalog
/// change as one atomic SeerDB commit.
pub(crate) struct DirectTransaction {
    catalog: Catalog,
    table_trees: BTreeMap<TableId, TreeId>,
    index_trees: BTreeMap<IndexId, TreeId>,
    transaction: Transaction,
}

impl DirectTransaction {
    pub(crate) fn begin(store: &DirectSeerStore) -> Result<Self> {
        let transaction = store.database.begin().map_err(map_seer_error)?;
        Ok(Self {
            catalog: store.catalog.clone(),
            table_trees: store.table_trees.clone(),
            index_trees: store.index_trees.clone(),
            transaction,
        })
    }

    /// Validate that current snapshot-plus-staged rows already satisfy one
    /// newly created foreign-key definition.
    pub(crate) fn validate_one_foreign_key(
        &self,
        foreign_key: &ForeignKeyDefinition,
    ) -> Result<()> {
        let child_definition = self.catalog.table(foreign_key.table)?;
        let referenced_definition = self.catalog.table(foreign_key.referenced_table)?;
        let referenced_index = self
            .catalog
            .indexes_for(foreign_key.referenced_table)
            .find(|index| index.unique && index.columns == foreign_key.referenced_columns)
            .ok_or_else(|| {
                DbError::InvalidState(format!(
                    "foreign key {} has no unique referenced index",
                    foreign_key.id.0
                ))
            })?;
        let referenced_values: std::collections::HashSet<Vec<u8>> = self
            .transaction
            .scan(self.index_tree(referenced_index.id)?, &[], None, usize::MAX)
            .map_err(map_seer_error)?
            .into_iter()
            .filter_map(|(entry, existing_identity)| {
                decode_index_entry(&entry, &existing_identity)
                    .ok()
                    .map(|(values, _)| values)
            })
            .collect();
        let child_rows = self
            .transaction
            .scan(self.table_tree(foreign_key.table)?, &[], None, usize::MAX)
            .map_err(map_seer_error)?;
        for (identity_bytes, bytes) in child_rows {
            let row = row_from_storage_identity(
                &self.catalog,
                child_definition,
                &identity_bytes,
                &bytes,
            )?;
            let values = foreign_key_values(&row, child_definition, &foreign_key.columns)?;
            if values.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            let encoded = index_values_key(referenced_definition, referenced_index, &values)?;
            if !referenced_values.contains(&encoded) {
                return Err(DbError::ForeignKeyViolation {
                    constraint: foreign_key.id.0,
                    table: foreign_key.table.0,
                    referenced_table: foreign_key.referenced_table.0,
                });
            }
        }
        Ok(())
    }

    fn table_tree(&self, table: TableId) -> Result<TreeId> {
        self.table_trees
            .get(&table)
            .copied()
            .ok_or_else(|| DbError::InvalidState(format!("table {} has no SeerDB tree", table.0)))
    }

    fn index_tree(&self, index: IndexId) -> Result<TreeId> {
        self.index_trees
            .get(&index)
            .copied()
            .ok_or_else(|| DbError::InvalidState(format!("index {} has no SeerDB tree", index.0)))
    }

    /// Read one row at this transaction's snapshot.
    pub(crate) fn get(&mut self, table: TableId, identity: &[u8]) -> Result<Option<Row>> {
        let definition = self.catalog.table(table)?.clone();
        let tree = self.table_tree(table)?;
        self.transaction
            .get(tree, identity)
            .map_err(map_seer_error)?
            .map(|bytes| row_from_storage_identity(&self.catalog, &definition, identity, &bytes))
            .transpose()
    }

    /// Scan all rows of one table at this transaction's snapshot.
    pub(crate) fn scan(&mut self, table: TableId) -> Result<Vec<Row>> {
        let definition = self.catalog.table(table)?.clone();
        let tree = self.table_tree(table)?;
        self.transaction
            .scan(tree, &[], None, usize::MAX)
            .map_err(map_seer_error)?
            .into_iter()
            .map(|(identity, bytes)| {
                row_from_storage_identity(&self.catalog, &definition, &identity, &bytes)
            })
            .collect()
    }

    /// Exact-value lookup through one secondary index.
    pub(crate) fn index_get(
        &mut self,
        table: TableId,
        index: IndexId,
        values: &[crate::Value],
    ) -> Result<Vec<Row>> {
        let definition = self.catalog.table(table)?.clone();
        let index_definition = self
            .catalog
            .index(index)
            .ok_or_else(|| DbError::InvalidState(format!("index {} does not exist", index.0)))?;
        if index_definition.table != table {
            return Err(DbError::InvalidState(format!(
                "index {} does not belong to table {}",
                index.0, table.0
            )));
        }
        let value_key = index_values_key(&definition, index_definition, values)?;
        let identities = self.index_identities_for(index, &value_key)?;
        let mut rows = Vec::with_capacity(identities.len());
        for row_identity in &identities {
            let bytes = self
                .transaction
                .get(self.table_tree(table)?, row_identity)
                .map_err(map_seer_error)?
                .ok_or_else(|| DbError::Corruption {
                    artifact: "direct SeerDB index",
                    reason: "index entry references a missing row".to_owned(),
                })?;
            rows.push(row_from_storage_identity(
                &self.catalog,
                &definition,
                row_identity,
                &bytes,
            )?);
        }
        Ok(rows)
    }

    /// Probe whether any staged or committed entry carries one value key
    /// under a unique index.
    ///
    /// The probe registers its key range as a transactional read dependency:
    /// if a concurrent transaction commits an entry inside the range after
    /// our snapshot, our commit fails with a serialization conflict. Without
    /// that registration two transactions could insert the same unique value
    /// concurrently from disjoint snapshots and both pass.
    fn index_has_conflict(&mut self, index: IndexId, value_key: &[u8]) -> Result<bool> {
        let prefix = index_entry_prefix(value_key);
        let end = prefix_successor(&prefix);
        let tree = self.index_tree(index)?;
        let mut cursor = self
            .transaction
            .cursor(tree, &prefix, end.as_deref())
            .map_err(map_seer_error)?;
        Ok(cursor.next().is_some_and(|result| match result {
            Ok((entry, _)) => entry.starts_with(&prefix),
            Err(_) => true,
        }))
    }

    /// Resolve one encoded value key to its row identities through a
    /// prefix-bounded index scan: same-value entries share an exact key
    /// prefix and therefore sort contiguously.
    pub(crate) fn index_identities_for(
        &mut self,
        index: IndexId,
        value_key: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let prefix = index_entry_prefix(value_key);
        let end = prefix_successor(&prefix);
        let tree = self.index_tree(index)?;
        let mut identities: Vec<Vec<u8>> = Vec::new();
        const PAGE: usize = 256;
        loop {
            let start: Vec<u8> = match identities.last() {
                Some(last) => encode_index_entry(value_key, last)?,
                None => prefix.clone(),
            };
            let batch = self
                .transaction
                .scan(tree, &start, end.as_deref(), PAGE)
                .map_err(map_seer_error)?;
            let mut advanced = false;
            let batch_count = batch.len();
            for (_entry, row_identity) in batch {
                identities.push(row_identity);
                advanced = true;
            }
            if !advanced || batch_count < PAGE {
                return Ok(identities);
            }
        }
    }

    /// Stage a row insert plus its derived index entries; uniqueness is
    /// checked against the snapshot and this transaction's staged state.
    pub(crate) fn insert(&mut self, table: TableId, row: Row) -> Result<()> {
        let definition = self.catalog.table(table)?.clone();
        row.validate(&definition)?;
        let identity = row_identity_bytes(&self.catalog, &definition, &row)?;
        let tree = self.table_tree(table)?;
        if self
            .transaction
            .get(tree, &identity)
            .map_err(map_seer_error)?
            .is_some()
        {
            return Err(DbError::InvalidState(format!(
                "row {:?} already exists in table {}",
                identity, table.0
            )));
        }
        self.transaction
            .put(tree, &identity, &encode_row(&row)?)
            .map_err(map_seer_error)?;
        self.stage_indexes_for(table, &definition, &row, &identity, true)
    }

    /// Replace one row identified by the incoming row's primary-key
    /// identity. The row must already exist at that identity; derived
    /// index entries are refreshed from the old and new contents.
    pub(crate) fn update(&mut self, table: TableId, row: Row) -> Result<()> {
        let definition = self.catalog.table(table)?.clone();
        row.validate(&definition)?;
        let identity = row_identity_bytes(&self.catalog, &definition, &row)?;
        let tree = self.table_tree(table)?;
        let bytes = self
            .transaction
            .get(tree, &identity)
            .map_err(map_seer_error)?
            .ok_or_else(|| DbError::InvalidState("row does not exist".to_owned()))?;
        let previous = row_from_storage_identity(&self.catalog, &definition, &identity, &bytes)?;
        self.stage_indexes_for(table, &definition, &previous, &identity, false)?;
        self.transaction
            .put(tree, &identity, &encode_row(&row)?)
            .map_err(map_seer_error)?;
        self.stage_indexes_for(table, &definition, &row, &identity, true)
    }

    /// Stage a row delete plus its derived index entries.
    pub(crate) fn delete(&mut self, table: TableId, identity: &[u8]) -> Result<()> {
        let definition = self.catalog.table(table)?.clone();
        let tree = self.table_tree(table)?;
        let bytes = self
            .transaction
            .get(tree, identity)
            .map_err(map_seer_error)?
            .ok_or_else(|| DbError::InvalidState("row does not exist".to_owned()))?;
        let row = row_from_storage_identity(&self.catalog, &definition, identity, &bytes)?;
        self.transaction
            .delete(tree, identity)
            .map_err(map_seer_error)?;
        self.stage_indexes_for(table, &definition, &row, identity, false)?;
        self.expand_referential_actions(table, &row, 0)
    }

    /// Expand `ON DELETE` actions for one deleted parent row. Cascaded
    /// deletions and NULL updates are staged eagerly so later staged reads
    /// observe them; cycles terminate because deleted rows vanish from the
    /// staged view. `Restrict` is enforced by commit-time referential
    /// validation instead.
    fn expand_referential_actions(
        &mut self,
        parent_table: TableId,
        parent_row: &Row,
        depth: usize,
    ) -> Result<()> {
        let applicable: Vec<ForeignKeyDefinition> = self
            .catalog
            .foreign_keys()
            .filter(|fk| fk.referenced_table == parent_table)
            .cloned()
            .collect();
        for foreign_key in applicable {
            if foreign_key.on_delete == crate::relational::ReferentialAction::Restrict {
                continue;
            }
            if depth + 1 > crate::relational::MAX_CASCADE_DEPTH {
                return Err(DbError::CascadeDepthExceeded {
                    constraint: foreign_key.id.0,
                    table: foreign_key.table.0,
                });
            }
            let child_definition = self.catalog.table(foreign_key.table)?.clone();
            let referenced_definition = self.catalog.table(parent_table)?.clone();
            let required = foreign_key_values(
                parent_row,
                &referenced_definition,
                &foreign_key.referenced_columns,
            )?;
            if required.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            for child in self.scan(foreign_key.table)? {
                let values = foreign_key_values(&child, &child_definition, &foreign_key.columns)?;
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
                            updated.set_value(&child_definition, *column, Value::Null)?;
                        }
                        let identity =
                            row_identity_bytes(&self.catalog, &child_definition, &updated)?;
                        self.update(foreign_key.table, updated)?;
                        drop(identity);
                    }
                    crate::relational::ReferentialAction::Cascade => {
                        let identity =
                            row_identity_bytes(&self.catalog, &child_definition, &child)?;
                        self.expand_referential_actions(foreign_key.table, &child, depth + 1)?;
                        self.delete(foreign_key.table, &identity)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Return the transaction's fixed read snapshot.
    pub(crate) fn snapshot_csn(&self) -> CommitSeq {
        self.transaction.snapshot()
    }

    /// Read all rows reachable through one index in index-key order,
    /// including this transaction's staged state.
    pub(crate) fn index_scan(&mut self, table: TableId, index: IndexId) -> Result<Vec<Row>> {
        let definition = self.catalog.table(table)?.clone();
        let tree = self.index_tree(index)?;
        let mut rows = Vec::new();
        for (entry, identity) in self
            .transaction
            .scan(tree, &[], None, usize::MAX)
            .map_err(map_seer_error)?
        {
            let (_, row_identity) = decode_index_entry(&entry, &identity)?;
            let bytes = self
                .transaction
                .get(self.table_tree(table)?, &row_identity)
                .map_err(map_seer_error)?
                .ok_or_else(|| DbError::Corruption {
                    artifact: "direct SeerDB index",
                    reason: "index entry references a missing row".to_owned(),
                })?;
            rows.push(row_from_storage_identity(
                &self.catalog,
                &definition,
                &row_identity,
                &bytes,
            )?);
        }
        Ok(rows)
    }

    fn stage_indexes_for(
        &mut self,
        table: TableId,
        definition: &TableDefinition,
        row: &Row,
        identity: &[u8],
        insert: bool,
    ) -> Result<()> {
        let applicable: Vec<crate::relational::IndexDefinition> =
            self.catalog.indexes_for(table).cloned().collect();
        for index in applicable {
            let Some(values) = row_index_key(definition, &index, row)? else {
                continue;
            };
            let tree = self.index_tree(index.id)?;
            let key = encode_index_entry(&values, identity)?;
            if insert && index.unique && self.index_has_conflict(index.id, &values)? {
                return Err(DbError::UniqueViolation {
                    index: index.id.0,
                    key: values,
                });
            }
            if insert {
                self.transaction
                    .put(tree, &key, identity)
                    .map_err(map_seer_error)?;
            } else {
                self.transaction
                    .delete(tree, &key)
                    .map_err(map_seer_error)?;
            }
        }
        Ok(())
    }

    /// Publish every staged change atomically after validating immediate
    /// foreign-key constraints against the transaction's own final state.
    pub(crate) fn commit(mut self) -> Result<CommitSeq> {
        self.validate_referential_integrity()?;
        self.transaction
            .commit()
            .map_err(map_seer_error)
            .map(|p| p.csn)
    }

    /// Validate every foreign key in the catalog against the transaction's
    /// snapshot-plus-staged state. Every non-null child reference must have
    /// a matching entry in the referenced table's unique covering index.
    fn validate_referential_integrity(&self) -> Result<()> {
        if self.catalog.foreign_keys().next().is_none() {
            return Ok(());
        }
        for foreign_key in self.catalog.foreign_keys() {
            let child_definition = self.catalog.table(foreign_key.table)?;
            let referenced_definition = self.catalog.table(foreign_key.referenced_table)?;
            let referenced_index = self
                .catalog
                .indexes_for(foreign_key.referenced_table)
                .find(|index| index.unique && index.columns == foreign_key.referenced_columns)
                .ok_or_else(|| {
                    DbError::InvalidState(format!(
                        "foreign key {} has no unique referenced index",
                        foreign_key.id.0
                    ))
                })?;
            let referenced_values: std::collections::HashSet<Vec<u8>> = self
                .transaction
                .scan(self.index_tree(referenced_index.id)?, &[], None, usize::MAX)
                .map_err(map_seer_error)?
                .into_iter()
                .filter_map(|(entry, existing_identity)| {
                    decode_index_entry(&entry, &existing_identity)
                        .ok()
                        .map(|(values, _)| values)
                })
                .collect();
            let child_rows = self
                .transaction
                .scan(self.table_tree(foreign_key.table)?, &[], None, usize::MAX)
                .map_err(map_seer_error)?;
            for (identity_bytes, bytes) in child_rows {
                let row = row_from_storage_identity(
                    &self.catalog,
                    child_definition,
                    &identity_bytes,
                    &bytes,
                )?;
                let values = foreign_key_values(&row, child_definition, &foreign_key.columns)?;
                if values.iter().any(|value| matches!(value, Value::Null)) {
                    continue;
                }
                let encoded = index_values_key(referenced_definition, referenced_index, &values)?;
                if !referenced_values.contains(&encoded) {
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
}
fn validate_mapping(
    transaction: &Transaction,
    catalog_tree: TreeId,
    catalog: &Catalog,
    table_trees: &BTreeMap<TableId, TreeId>,
    index_trees: &BTreeMap<IndexId, TreeId>,
) -> Result<()> {
    let trees = transaction.list_trees().map_err(map_seer_error)?;
    let trees = trees.into_iter().collect::<BTreeSet<_>>();
    if !trees.contains(&catalog_tree) {
        return Err(DbError::Corruption {
            artifact: "direct SeerDB catalog",
            reason: "catalog tree is not live".to_owned(),
        });
    }
    let mut mapped_trees = BTreeSet::from([catalog_tree]);
    for table in catalog.tables() {
        let tree = table_trees
            .get(&table.id)
            .ok_or_else(|| DbError::Corruption {
                artifact: "direct SeerDB catalog",
                reason: format!("table {} has no tree mapping", table.id.0),
            })?;
        if !trees.contains(tree) || !mapped_trees.insert(*tree) {
            return Err(DbError::Corruption {
                artifact: "direct SeerDB catalog",
                reason: format!("table {} has an invalid tree mapping", table.id.0),
            });
        }
    }
    for index in catalog.indexes() {
        let tree = index_trees
            .get(&index.id)
            .ok_or_else(|| DbError::Corruption {
                artifact: "direct SeerDB catalog",
                reason: format!("index {} has no tree mapping", index.id.0),
            })?;
        if !trees.contains(tree) || !mapped_trees.insert(*tree) {
            return Err(DbError::Corruption {
                artifact: "direct SeerDB catalog",
                reason: format!("index {} has an invalid tree mapping", index.id.0),
            });
        }
    }
    if mapped_trees != trees {
        return Err(DbError::Corruption {
            artifact: "direct SeerDB catalog",
            reason: "live tree set contains an orphan mapping".to_owned(),
        });
    }
    if table_trees
        .keys()
        .any(|table| catalog.table(*table).is_err())
        || index_trees
            .keys()
            .any(|index| catalog.index(*index).is_none())
    {
        return Err(DbError::Corruption {
            artifact: "direct SeerDB catalog",
            reason: "catalog contains an orphan tree mapping".to_owned(),
        });
    }
    Ok(())
}

fn encode_catalog_state(
    catalog: &Catalog,
    table_trees: &BTreeMap<TableId, TreeId>,
    index_trees: &BTreeMap<IndexId, TreeId>,
) -> Result<Vec<u8>> {
    let catalog_bytes = encode_catalog(catalog)?;
    if catalog_bytes.len() > MAX_CATALOG_BYTES
        || table_trees.len() > MAX_CATALOG_OBJECTS
        || index_trees.len() > MAX_CATALOG_OBJECTS
    {
        return Err(DbError::ResourceLimitExceeded(
            "direct SeerDB catalog exceeds its bound".to_owned(),
        ));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(DIRECT_CATALOG_MAGIC);
    bytes.extend_from_slice(&(catalog_bytes.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&catalog_bytes);
    put_count(&mut bytes, table_trees.len())?;
    for (table, tree) in table_trees {
        bytes.extend_from_slice(&table.0.to_be_bytes());
        bytes.extend_from_slice(&tree.get().to_be_bytes());
    }
    put_count(&mut bytes, index_trees.len())?;
    for (index, tree) in index_trees {
        bytes.extend_from_slice(&index.0.to_be_bytes());
        bytes.extend_from_slice(&tree.get().to_be_bytes());
    }
    if bytes.len() > MAX_CATALOG_BYTES {
        return Err(DbError::ResourceLimitExceeded(
            "encoded direct SeerDB catalog exceeds its bound".to_owned(),
        ));
    }
    Ok(bytes)
}

fn decode_catalog_state(
    bytes: &[u8],
) -> Result<(
    Catalog,
    BTreeMap<TableId, TreeId>,
    BTreeMap<IndexId, TreeId>,
)> {
    if bytes.len() > MAX_CATALOG_BYTES || bytes.get(..4) != Some(DIRECT_CATALOG_MAGIC) {
        return Err(DbError::Corruption {
            artifact: "direct SeerDB catalog",
            reason: "invalid catalog state header".to_owned(),
        });
    }
    let mut cursor = 4;
    let catalog_length = read_u32(bytes, &mut cursor)? as usize;
    let catalog_bytes = take(bytes, &mut cursor, catalog_length)?;
    let catalog = crate::relational::decode_catalog(catalog_bytes)?;
    let table_count = read_u32(bytes, &mut cursor)? as usize;
    if table_count > MAX_CATALOG_OBJECTS {
        return Err(DbError::ResourceLimitExceeded(
            "too many direct table mappings".to_owned(),
        ));
    }
    let mut table_trees = BTreeMap::new();
    for _ in 0..table_count {
        let table = TableId(read_u64(bytes, &mut cursor)?);
        let tree = TreeId::new(read_u64(bytes, &mut cursor)?);
        if table_trees.insert(table, tree).is_some() {
            return Err(DbError::Corruption {
                artifact: "direct SeerDB catalog",
                reason: "duplicate table mapping".to_owned(),
            });
        }
    }
    let index_count = read_u32(bytes, &mut cursor)? as usize;
    if index_count > MAX_CATALOG_OBJECTS {
        return Err(DbError::ResourceLimitExceeded(
            "too many direct index mappings".to_owned(),
        ));
    }
    let mut index_trees = BTreeMap::new();
    for _ in 0..index_count {
        let index = IndexId(read_u64(bytes, &mut cursor)?);
        let tree = TreeId::new(read_u64(bytes, &mut cursor)?);
        if index_trees.insert(index, tree).is_some() {
            return Err(DbError::Corruption {
                artifact: "direct SeerDB catalog",
                reason: "duplicate index mapping".to_owned(),
            });
        }
    }
    if cursor != bytes.len() {
        return Err(DbError::Corruption {
            artifact: "direct SeerDB catalog",
            reason: "catalog state has trailing bytes".to_owned(),
        });
    }
    Ok((catalog, table_trees, index_trees))
}

/// Byte prefix shared by every index entry carrying one encoded value key.
fn index_entry_prefix(values: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + values.len());
    bytes.extend_from_slice(
        &u32::try_from(values.len())
            .expect("index value length")
            .to_be_bytes(),
    );
    bytes.extend_from_slice(values);
    bytes
}

/// Exclusive upper bound for all keys beginning with one prefix: the
/// prefix with its last byte incremented. Overflow is impossible here
/// because the leading u32 length can never be all-zero bytes.
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for byte in end.iter_mut().rev() {
        match *byte {
            0xFF => *byte = 0x00,
            _ => {
                *byte += 1;
                return Some(end);
            }
        }
    }
    None
}

fn encode_index_entry(values: &[u8], identity: &[u8]) -> Result<Vec<u8>> {
    let values_len =
        u32::try_from(values.len()).map_err(|_| DbError::ValueTooLarge(values.len()))?;
    let identity_len =
        u32::try_from(identity.len()).map_err(|_| DbError::ValueTooLarge(identity.len()))?;
    let mut bytes = Vec::with_capacity(8 + values.len() + identity.len());
    bytes.extend_from_slice(&values_len.to_be_bytes());
    bytes.extend_from_slice(values);
    bytes.extend_from_slice(&identity_len.to_be_bytes());
    bytes.extend_from_slice(identity);
    Ok(bytes)
}

fn decode_index_entry(entry: &[u8], value: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut cursor = 0;
    let values_len = read_u32(entry, &mut cursor)? as usize;
    let values = take(entry, &mut cursor, values_len)?.to_vec();
    let identity_len = read_u32(entry, &mut cursor)? as usize;
    let identity = take(entry, &mut cursor, identity_len)?.to_vec();
    if cursor != entry.len() || identity != value {
        return Err(DbError::Corruption {
            artifact: "direct SeerDB index",
            reason: "index entry key and value disagree".to_owned(),
        });
    }
    Ok((values, identity))
}

fn put_count(bytes: &mut Vec<u8>, count: usize) -> Result<()> {
    bytes.extend_from_slice(
        &u32::try_from(count)
            .map_err(|_| DbError::ResourceLimitExceeded("catalog count overflows u32".into()))?
            .to_be_bytes(),
    );
    Ok(())
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| DbError::Corruption {
            artifact: "direct SeerDB catalog",
            reason: "length overflow".to_owned(),
        })?;
    let value = bytes.get(*cursor..end).ok_or_else(|| DbError::Corruption {
        artifact: "direct SeerDB catalog",
        reason: "truncated catalog state".to_owned(),
    })?;
    *cursor = end;
    Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_be_bytes(
        take(bytes, cursor, 4)?
            .try_into()
            .map_err(|_| DbError::Corruption {
                artifact: "direct SeerDB catalog",
                reason: "invalid u32".to_owned(),
            })?,
    ))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    Ok(u64::from_be_bytes(
        take(bytes, cursor, 8)?
            .try_into()
            .map_err(|_| DbError::Corruption {
                artifact: "direct SeerDB catalog",
                reason: "invalid u64".to_owned(),
            })?,
    ))
}

fn map_seer_error(error: SeerError) -> DbError {
    match error {
        SeerError::SerializationConflict { expected, current } => DbError::SerializationConflict {
            snapshot: expected.get(),
            current: current.get(),
        },
        SeerError::WriteConflict { tree, key } => DbError::SeerWriteConflict {
            tree: tree.get(),
            key,
        },
        SeerError::TreeNotFound(tree) => {
            DbError::InvalidState(format!("SeerDB tree {:?} is unavailable", tree))
        }
        SeerError::TreeConflict(tree) => DbError::SeerTreeConflict { tree: tree.get() },
        SeerError::Backpressure {
            required,
            available,
        } => DbError::StorageCapacity {
            requested: required,
            available,
        },
        SeerError::DatabaseBusy => DbError::StorageBusy {
            operation: "direct SeerDB",
            reason: "another writer owns the database".to_owned(),
        },
        SeerError::SnapshotUnavailable(reason) => DbError::StorageSnapshotUnavailable {
            snapshot: 0,
            reason,
        },
        SeerError::InvalidArgument(reason) => DbError::InvalidState(reason),
        SeerError::Corruption(reason)
        | SeerError::Check {
            message: reason, ..
        } => DbError::StorageCorruption { reason },
        SeerError::NeedsRecovery(reason) => DbError::StorageRecoveryRequired { reason },
        SeerError::DiskFull | SeerError::CapacityPreflight => DbError::StorageCapacity {
            requested: 1,
            available: 0,
        },
        SeerError::Io(source) => DbError::StorageIo {
            operation: "direct SeerDB",
            reason: source.to_string(),
        },
        other => DbError::Storage {
            operation: "direct SeerDB",
            reason: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDefinition, ColumnType, Key, Value};
    use tempfile::tempdir;

    fn table() -> TableDefinition {
        TableDefinition {
            id: TableId(1),
            name: "users".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "email".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        }
    }

    fn row(id: u64, email: &str) -> Row {
        Row {
            primary: Key::new(1, id),
            values: vec![Value::U64(id), Value::Text(email.to_owned())],
        }
    }

    fn store() -> (tempfile::TempDir, DirectSeerStore) {
        let directory = tempdir().expect("temporary directory");
        let store = DirectSeerStore::create(directory.path().join("db"), Options::for_test())
            .expect("create direct store");
        (directory, store)
    }

    #[test]
    fn explicit_transaction_batches_reads_and_writes_atomically() {
        let (_directory, mut store) = store();
        store
            .create_table(table(), Some(vec![ColumnId(1)]))
            .expect("create table");
        store
            .create_index(IndexDefinition {
                id: IndexId(1),
                table: TableId(1),
                columns: vec![ColumnId(2)],
                unique: false,
            })
            .expect("create index");
        store
            .insert(TableId(1), row(1, "a@example.com"))
            .expect("seed");

        // Reads inside an open transaction see staged writes; the store's
        // committed view does not.
        let mut transaction = store.begin_transaction().expect("begin");
        assert_eq!(
            transaction
                .get(
                    TableId(1),
                    &row_identity_bytes(
                        store.catalog(),
                        store.catalog().table(TableId(1)).expect("def"),
                        &row(2, "b@example.com")
                    )
                    .expect("identity")
                )
                .expect("staged get"),
            None
        );
        let staged_row = row(2, "b@example.com");
        transaction
            .insert(TableId(1), staged_row)
            .expect("stage insert");
        let scanned = transaction.scan(TableId(1)).expect("scan with staging");
        assert_eq!(scanned.len(), 2);

        // A duplicate primary key inside the same transaction must be
        // caught against staged state before any commit.
        let dup = row(2, "c@example.com");
        assert!(transaction.insert(TableId(1), dup).is_err());
        transaction.commit().expect("commit");

        assert_eq!(store.scan(TableId(1)).expect("committed scan").len(), 2);
        let matches = store
            .index_get(
                TableId(1),
                IndexId(1),
                &[Value::Text("b@example.com".into())],
            )
            .expect("index after commit");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn uncommitted_transaction_is_invisible_and_aborts_cleanly() {
        let (_directory, mut store) = store();
        store
            .create_table(table(), Some(vec![ColumnId(1)]))
            .expect("create table");
        store
            .insert(TableId(1), row(1, "a@example.com"))
            .expect("seed");

        {
            let mut transaction = store.begin_transaction().expect("begin");
            transaction
                .delete(
                    TableId(1),
                    &row_identity_bytes(
                        store.catalog(),
                        store.catalog().table(TableId(1)).expect("def"),
                        &row(1, "a@example.com"),
                    )
                    .expect("identity"),
                )
                .expect("stage delete");
            // Dropping without committing discards the staged delete.
        }
        assert_eq!(store.scan(TableId(1)).expect("scan survives drop").len(), 1);
    }

    #[test]
    fn overlapping_transactions_conflict_on_the_same_key() {
        let (_directory, mut store) = store();
        store
            .create_table(table(), Some(vec![ColumnId(1)]))
            .expect("create table");
        store
            .insert(TableId(1), row(1, "a@example.com"))
            .expect("seed");

        let identity_one = row_identity_bytes(
            store.catalog(),
            store.catalog().table(TableId(1)).expect("def"),
            &row(1, "a@example.com"),
        )
        .expect("identity one");
        let mut first = store.begin_transaction().expect("first begin");
        first
            .delete(TableId(1), &identity_one)
            .expect("first stage");

        let mut second = store.begin_transaction().expect("second begin");
        second
            .delete(TableId(1), &identity_one)
            .expect("second stage");
        second.commit().expect("second commits first");

        match first.commit() {
            Err(DbError::SerializationConflict { .. } | DbError::SeerWriteConflict { .. }) => {}
            other => panic!("overlapping delete must conflict, got {other:?}"),
        }
    }

    #[test]
    fn catalog_table_index_and_rows_reopen_through_direct_trees() {
        let (directory, mut store) = store();
        store
            .create_table(table(), Some(vec![ColumnId(1)]))
            .expect("create table");
        store
            .create_index(IndexDefinition {
                id: IndexId(1),
                table: TableId(1),
                columns: vec![ColumnId(2)],
                unique: true,
            })
            .expect("create index");
        store
            .insert(TableId(1), row(7, "alice@example.com"))
            .expect("insert");
        assert_eq!(store.scan(TableId(1)).expect("scan").len(), 1);
        let indexed = store
            .index_get(
                TableId(1),
                IndexId(1),
                &[Value::Text("alice@example.com".into())],
            )
            .expect("index get");
        assert_eq!(indexed.len(), 1);
        assert_eq!(indexed[0].values, row(7, "alice@example.com").values);
        store.close().expect("close");

        let reopened = DirectSeerStore::open(directory.path().join("db"), Options::for_test())
            .expect("reopen");
        assert_eq!(reopened.catalog().tables().count(), 1);
        assert_eq!(reopened.catalog().indexes().count(), 1);
        let reopened_rows = reopened.scan(TableId(1)).expect("reopened scan");
        assert_eq!(reopened_rows.len(), 1);
        assert_eq!(reopened_rows[0].values, row(7, "alice@example.com").values);
        let identity =
            row_identity_bytes(reopened.catalog(), &table(), &row(7, "alice@example.com"))
                .expect("identity");
        assert_eq!(
            reopened
                .get(TableId(1), &identity)
                .expect("reopened get")
                .expect("row")
                .values,
            row(7, "alice@example.com").values
        );
        reopened
            .delete(
                TableId(1),
                &row_identity_bytes(reopened.catalog(), &table(), &row(7, "alice@example.com"))
                    .expect("identity"),
            )
            .expect("delete");
        assert!(reopened.scan(TableId(1)).expect("empty scan").is_empty());
        reopened.close().expect("close reopened");
    }

    #[test]
    fn multi_table_multi_index_conformance_with_reopen() {
        let (directory, mut store) = store();
        store
            .create_table(table(), Some(vec![ColumnId(1)]))
            .expect("create users");
        let mut orders = table();
        orders.id = TableId(2);
        orders.name = "orders".to_owned();
        store
            .create_table(orders.clone(), Some(vec![ColumnId(1)]))
            .expect("create orders");
        store
            .create_index(IndexDefinition {
                id: IndexId(1),
                table: TableId(1),
                columns: vec![ColumnId(2)],
                unique: false,
            })
            .expect("create non-unique email index");
        store
            .create_index(IndexDefinition {
                id: IndexId(2),
                table: TableId(2),
                columns: vec![ColumnId(1)],
                unique: true,
            })
            .expect("create unique order index");

        // Two rows share an email in the non-unique index; each order is
        // unique by id.
        store
            .insert(TableId(1), row(1, "shared@example.com"))
            .expect("insert 1");
        store
            .insert(TableId(1), row(2, "shared@example.com"))
            .expect("insert 2");
        let mut order_row = row(10, "order@example.com");
        order_row.primary = Key::new(1, 10);
        store
            .insert(TableId(2), order_row.clone())
            .expect("insert order");

        // Non-unique index returns both matches; tables stay isolated.
        let matches = store
            .index_get(
                TableId(1),
                IndexId(1),
                &[Value::Text("shared@example.com".into())],
            )
            .expect("email lookup");
        assert_eq!(matches.len(), 2);
        assert!(store.scan(TableId(2)).expect("orders scan").len() == 1);

        store.close().expect("close");
        let reopened = DirectSeerStore::open(directory.path().join("db"), Options::for_test())
            .expect("reopen");
        assert_eq!(reopened.catalog().tables().count(), 2);
        assert_eq!(reopened.catalog().indexes().count(), 2);
        let matches = reopened
            .index_get(
                TableId(1),
                IndexId(1),
                &[Value::Text("shared@example.com".into())],
            )
            .expect("reopened email lookup");
        assert_eq!(matches.len(), 2);
        let orders_after = reopened.scan(TableId(2)).expect("reopened orders");
        assert_eq!(orders_after.len(), 1);
        assert_eq!(orders_after[0].values, order_row.values);
    }

    #[test]
    fn old_snapshot_reads_stay_stable_across_store_commits() {
        let (_directory, mut store) = store();
        store
            .create_table(table(), Some(vec![ColumnId(1)]))
            .expect("create table");
        store
            .insert(TableId(1), row(1, "a@example.com"))
            .expect("insert");

        // A raw fixed-snapshot transaction over the same mapped tree holds
        // its read even while the store publishes newer commits.
        let tree = store.table_tree(TableId(1)).expect("tree");
        let mut reader = store.database.begin().expect("reader begin");
        let before = reader
            .scan(tree, &[], None, usize::MAX)
            .expect("snapshot scan");
        assert_eq!(before.len(), 1);

        store
            .insert(TableId(1), row(2, "b@example.com"))
            .expect("second insert");

        let after = reader
            .scan(tree, &[], None, usize::MAX)
            .expect("stable scan");
        assert_eq!(after.len(), before.len());
        reader.abort().expect("abort reader");

        assert_eq!(store.scan(TableId(1)).expect("live scan").len(), 2);
    }

    #[test]
    fn controlled_concurrency_maps_write_conflicts() {
        let (_directory, store) = store();
        let database = std::sync::Arc::new(store.database);
        let tree = {
            let mut transaction = database.begin().expect("begin");
            let tree = transaction.create_tree().expect("tree");
            transaction.commit().expect("commit tree");
            tree
        };

        let first_db = std::sync::Arc::clone(&database);
        let second_db = std::sync::Arc::clone(&database);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let barrier_one = std::sync::Arc::clone(&barrier);
        let barrier_two = std::sync::Arc::clone(&barrier);
        let first = std::thread::spawn(move || {
            let mut transaction = first_db.begin().expect("first begin");
            transaction.put(tree, b"shared", b"one").expect("stage one");
            transaction
                .put(tree, b"extra-a", b"a")
                .expect("stage extra");
            barrier_one.wait();
            transaction.commit()
        });
        let second = std::thread::spawn(move || {
            let mut transaction = second_db.begin().expect("second begin");
            transaction.delete(tree, b"shared").expect("stage delete");
            transaction
                .put(tree, b"extra-b", b"b")
                .expect("stage other");
            barrier_two.wait();
            transaction.commit()
        });
        let outcomes = [first.join().expect("join"), second.join().expect("join")];
        let winners = outcomes.iter().filter(|r| r.is_ok()).count();
        assert_eq!(winners, 1, "exactly one overlapping writer commits");
        let loser = outcomes
            .into_iter()
            .find(|r| r.is_err())
            .expect("loser")
            .unwrap_err();
        match map_seer_error(loser) {
            DbError::SeerWriteConflict { .. } => {}
            other => panic!("expected a mapped write conflict, got {other:?}"),
        }
    }

    #[test]
    fn failed_operations_leave_no_partial_state() {
        let (_directory, mut store) = store();
        store
            .create_table(table(), Some(vec![ColumnId(1)]))
            .expect("create table");
        store
            .create_index(IndexDefinition {
                id: IndexId(1),
                table: TableId(1),
                columns: vec![ColumnId(2)],
                unique: true,
            })
            .expect("create unique index");
        store
            .insert(TableId(1), row(1, "dupe@example.com"))
            .expect("insert");

        // A duplicate insert must not leave the new row or either index
        // entry behind.
        assert!(
            store
                .insert(TableId(1), row(2, "dupe@example.com"))
                .is_err()
        );
        let rows = store.scan(TableId(1)).expect("scan after violation");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            store
                .index_get(
                    TableId(1),
                    IndexId(1),
                    &[Value::Text("dupe@example.com".into())]
                )
                .expect("index after violation")
                .len(),
            1
        );

        // An index build that hits duplicates mid-scan leaves the catalog,
        // mappings, and live-tree set untouched.
        let mut orders = table();
        orders.id = TableId(2);
        orders.name = "orders".to_owned();
        store
            .create_table(orders.clone(), Some(vec![ColumnId(1)]))
            .expect("create orders");
        let mut order_one = row(10, "same@example.com");
        order_one.primary = Key::new(1, 10);
        let mut order_two = row(11, "same@example.com");
        order_two.primary = Key::new(1, 11);
        store.insert(TableId(2), order_one).expect("order one");
        store.insert(TableId(2), order_two).expect("order two");

        let trees_before = {
            let transaction = store.database.begin().expect("begin");
            transaction.list_trees().expect("list trees").len()
        };
        let duplicate_index = IndexDefinition {
            id: IndexId(9),
            table: TableId(2),
            columns: vec![ColumnId(2)],
            unique: true,
        };
        assert!(
            store.create_index(duplicate_index).is_err(),
            "building a unique index over duplicate values must fail"
        );
        assert!(store.catalog().index(IndexId(9)).is_none());
        let mut transaction = store.database.begin().expect("begin after failure");
        assert_eq!(
            transaction.list_trees().expect("trees unchanged").len(),
            trees_before
        );
        transaction.abort().expect("abort probe");
    }

    #[test]
    fn malformed_catalog_states_are_rejected_on_open() {
        // Missing marker.
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("missing");
        TransactionDatabase::create(&path, Options::for_test()).expect("bare create");
        assert!(DirectSeerStore::open(&path, Options::for_test()).is_err());

        // Corrupt marker payload.
        let corrupt_path = directory.path().join("corrupt");
        let database = TransactionDatabase::create(&corrupt_path, Options::for_test()).expect("db");
        let mut transaction = database.begin().expect("begin");
        let tree = transaction.create_tree().expect("tree");
        transaction
            .put(tree, DIRECT_CATALOG_MARKER, b"not-a-catalog-state")
            .expect("put garbage marker");
        transaction.commit().expect("commit");
        drop(transaction);
        drop(database);
        match DirectSeerStore::open(&corrupt_path, Options::for_test()) {
            Err(
                err @ (DbError::Corruption {
                    artifact: "direct SeerDB catalog",
                    ..
                }
                | DbError::StorageCorruption { .. }),
            ) => {
                assert!(err.to_string().contains("header"));
            }
            other => panic!(
                "corrupt state must be rejected, got {:?}",
                other.map(|_| ())
            ),
        }

        // Duplicate markers across two trees.
        let duplicate_path = directory.path().join("duplicate");
        let database =
            TransactionDatabase::create(&duplicate_path, Options::for_test()).expect("db");
        let mut transaction = database.begin().expect("begin");
        let first_tree = transaction.create_tree().expect("first tree");
        let second_tree = transaction.create_tree().expect("second tree");
        let state = encode_catalog_state(&Catalog::default(), &BTreeMap::new(), &BTreeMap::new())
            .expect("encode empty state");
        transaction
            .put(first_tree, DIRECT_CATALOG_MARKER, &state)
            .expect("marker 1");
        transaction
            .put(second_tree, DIRECT_CATALOG_MARKER, &state)
            .expect("marker 2");
        transaction.commit().expect("commit");
        drop(transaction);
        drop(database);
        match DirectSeerStore::open(&duplicate_path, Options::for_test()) {
            Err(DbError::Corruption { reason, .. }) => {
                assert!(reason.contains("multiple catalog"), "{reason}");
            }
            other => panic!(
                "duplicate markers must be rejected, got {:?}",
                other.map(|_| ())
            ),
        }

        // Orphan live tree: two live trees but the mapping names only one.
        let orphan_path = directory.path().join("orphan");
        let database = TransactionDatabase::create(&orphan_path, Options::for_test()).expect("db");
        let mut transaction = database.begin().expect("begin");
        let catalog_tree = transaction.create_tree().expect("catalog tree");
        let _orphan_tree = transaction.create_tree().expect("orphan tree");
        let state = encode_catalog_state(&Catalog::default(), &BTreeMap::new(), &BTreeMap::new())
            .expect("encode");
        transaction
            .put(catalog_tree, DIRECT_CATALOG_MARKER, &state)
            .expect("marker");
        transaction.commit().expect("commit");
        drop(transaction);
        drop(database);
        assert!(DirectSeerStore::open(&orphan_path, Options::for_test()).is_err());

        // Catalog claims a table whose tree mapping is absent.
        let unmapped_path = directory.path().join("unmapped");
        let database =
            TransactionDatabase::create(&unmapped_path, Options::for_test()).expect("db");
        let mut candidate = Catalog::default();
        candidate
            .create_table_with_primary_key(table(), Some(vec![ColumnId(1)]))
            .expect("candidate table");
        let mut transaction = database.begin().expect("begin");
        let catalog_tree = transaction.create_tree().expect("catalog tree");
        let state = encode_catalog_state(&candidate, &BTreeMap::new(), &BTreeMap::new())
            .expect("encode with missing mapping");
        transaction
            .put(catalog_tree, DIRECT_CATALOG_MARKER, &state)
            .expect("marker");
        transaction.commit().expect("commit");
        drop(transaction);
        drop(database);
        match DirectSeerStore::open(&unmapped_path, Options::for_test()) {
            Err(DbError::Corruption { reason, .. }) => {
                assert!(reason.contains("no tree mapping"), "{reason}");
            }
            other => panic!(
                "unmapped table must be rejected, got {:?}",
                other.map(|_| ())
            ),
        }
    }

    #[test]
    fn row_and_unique_index_are_atomic_on_conflict() {
        let (_directory, mut store) = store();
        store
            .create_table(table(), Some(vec![ColumnId(1)]))
            .expect("create table");
        store
            .create_index(IndexDefinition {
                id: IndexId(1),
                table: TableId(1),
                columns: vec![ColumnId(2)],
                unique: true,
            })
            .expect("create index");
        store
            .insert(TableId(1), row(1, "same"))
            .expect("first insert");
        assert!(matches!(
            store.insert(TableId(1), row(2, "same")),
            Err(DbError::UniqueViolation { index: 1, .. })
        ));
        assert_eq!(store.scan(TableId(1)).expect("scan").len(), 1);
    }
}
