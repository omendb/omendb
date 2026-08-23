use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::fault::{FaultInjector, FaultPoint, NoFaults};
use crate::model::{CommitId, IndexId, Key, Mutation, StorageIdentity};
use crate::row_identity::{RowIdentity, decode_legacy_key, encode_legacy_key};
use crate::runtime::{Dispatch, Reactor, ReactorError, WorkClass, WorkId};
use crate::store::Transaction;
use crate::store::{CompactionBudget, CompactionReport, Database, DatabaseConfig, DatabaseMetrics};
use crate::{AttemptRecord, DbError, Result, TransactionAttemptId};

const ROW_MAGIC: [u8; 4] = *b"DBRW";
const ROW_VERSION: u8 = 1;
const CATALOG_MAGIC: [u8; 4] = *b"DBCT";
/// Maximum cascade generations per triggering delete statement.
pub(crate) const MAX_CASCADE_DEPTH: usize = 64;

const CATALOG_VERSION: u16 = 5;
const SCHEMA_MAGIC: [u8; 4] = *b"DBSJ";
const SCHEMA_VERSION: u16 = 2;
const SCHEMA_NAME: &str = "omendb.schema";
const SCHEMA_HEADER_BYTES: usize = 24;
const SCHEMA_MAX_PAYLOAD: usize = 64 * 1024 * 1024;
const MAX_SCHEMA_JOBS: usize = 64;
const ROW_NAMESPACE: u8 = 0x10;

// The temporary kernel is still a OmenDB-owned byte store. Keep the
// relational catalog in its reserved keyspace so schema bytes share the same
// WAL, checkpoint, snapshot, and recovery boundary as rows and indexes.
const CATALOG_KEY_TENANT: u64 = u64::MAX;

const SCHEMA_SUBMIT: u8 = 1;
const SCHEMA_RUNNING: u8 = 2;
const SCHEMA_COMPLETED: u8 = 3;
const SCHEMA_FAILED: u8 = 4;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TableId(pub u64);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ColumnId(pub u16);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConstraintId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnType {
    Bytes,
    Bool,
    I64,
    U64,
    Text,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnDefinition {
    pub id: ColumnId,
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableDefinition {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

impl TableDefinition {
    pub(crate) fn column(&self, id: ColumnId) -> Result<&ColumnDefinition> {
        self.columns
            .iter()
            .find(|column| column.id == id)
            .ok_or_else(|| DbError::InvalidState(format!("column {} does not exist", id.0)))
    }

    fn validate(&self) -> Result<()> {
        if self.id.0 == CATALOG_KEY_TENANT {
            return Err(DbError::InvalidState(
                "table ID is reserved for the durable catalog".to_owned(),
            ));
        }
        if self.columns.is_empty() {
            return Err(DbError::InvalidState(format!(
                "table {} must define at least one column",
                self.id.0
            )));
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut names = std::collections::BTreeSet::new();
        for column in &self.columns {
            if !ids.insert(column.id) {
                return Err(DbError::InvalidState(format!(
                    "table {} repeats column ID {}",
                    self.id.0, column.id.0
                )));
            }
            if !names.insert(column.name.clone()) {
                return Err(DbError::InvalidState(format!(
                    "table {} repeats column name {}",
                    self.id.0, column.name
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Catalog {
    generation: u64,
    tables: BTreeMap<TableId, TableDefinition>,
    primary_keys: BTreeMap<TableId, Vec<ColumnId>>,
    indexes: BTreeMap<IndexId, IndexDefinition>,
    index_names: BTreeMap<IndexId, String>,
    foreign_keys: BTreeMap<ConstraintId, ForeignKeyDefinition>,
    foreign_key_names: BTreeMap<ConstraintId, String>,
}

/// Bounds for one in-memory logical snapshot capture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalSnapshotCaptureOptions {
    /// Maximum number of rows across all snapshots in one capture.
    pub max_rows: usize,
    /// Maximum number of snapshots in one capture.
    pub max_snapshots: usize,
    /// Maximum number of durable transaction-attempt records observed in one
    /// capture. Archives currently refuse captures that contain any records,
    /// but the bound keeps the source observation finite for future transfer
    /// policies.
    pub max_attempts: usize,
}

impl RelationalSnapshotCaptureOptions {
    #[must_use]
    pub const fn new(max_rows: usize) -> Self {
        Self {
            max_rows,
            max_snapshots: 1_000_000,
            max_attempts: 1_024,
        }
    }

    /// Set the maximum number of logical snapshots observed by capture.
    #[must_use]
    pub const fn with_max_snapshots(mut self, max_snapshots: usize) -> Self {
        self.max_snapshots = max_snapshots;
        self
    }

    /// Set the maximum number of durable attempt records observed by capture.
    #[must_use]
    pub const fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

/// Rows belonging to one table in a captured logical snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalSnapshotTable {
    pub table: TableId,
    pub rows: Vec<Row>,
}

/// One backend-neutral logical snapshot suitable for later archive assembly.
///
/// Catalog definitions and rows are authoritative. Secondary indexes are
/// intentionally represented only by catalog definitions and their derived
/// membership is rebuilt and verified by a future target importer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalSnapshot {
    pub commit: CommitId,
    pub catalog: Catalog,
    pub tables: Vec<RelationalSnapshotTable>,
    pub catalog_digest: [u8; 32],
    pub logical_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogicalVerification {
    pub catalog_generation: u64,
    pub table_count: usize,
    pub index_count: usize,
    pub row_count: usize,
    pub index_entry_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDefinition {
    pub id: IndexId,
    pub table: TableId,
    pub columns: Vec<ColumnId>,
    pub unique: bool,
}

/// One index included in an atomic schema publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedIndexDefinition {
    pub definition: IndexDefinition,
    pub name: Option<String>,
}

/// Action taken on referencing child rows when a referenced key row is
/// deleted. `SetDefault` and update-referenced-key actions are refused by
/// the constraint-timing contract; they are not representable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReferentialAction {
    /// Reject the parent delete while referencing children remain (default).
    /// Enforced by the publication-time referential-integrity pass.
    #[default]
    Restrict,
    /// Stage deletion of referencing child rows when the parent is deleted.
    Cascade,
    /// Stage NULL into the referencing child columns when the parent is
    /// deleted. Requires every foreign-key column to be nullable.
    SetNull,
}

/// When a constraint's satisfaction is observable. In the serialized-writer
/// kernel both timings resolve at the publication-validation pass; the
/// attribute records declared intent for the SQL surface and future
/// concurrent profiles.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConstraintTiming {
    /// Every publication must satisfy the constraint (default).
    #[default]
    Immediate,
    /// Staged mutations may violate the constraint between publications;
    /// the publication-validation pass still resolves it before durability.
    DeferredToPublication,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForeignKeyDefinition {
    pub id: ConstraintId,
    pub table: TableId,
    pub columns: Vec<ColumnId>,
    pub referenced_table: TableId,
    pub referenced_columns: Vec<ColumnId>,
    pub on_delete: ReferentialAction,
    pub timing: ConstraintTiming,
}

/// One foreign key included in an atomic schema publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedForeignKeyDefinition {
    pub definition: ForeignKeyDefinition,
    pub name: Option<String>,
}

/// Additional schema objects to publish with one new table.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RelationalSchemaDefinition {
    pub indexes: Vec<NamedIndexDefinition>,
    pub foreign_keys: Vec<NamedForeignKeyDefinition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaChange {
    CreateTable(TableDefinition),
    CreateIndex(IndexDefinition),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchemaJobId(pub u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaJobState {
    Pending,
    Running,
    Completed,
    Failed(String),
}

#[derive(Clone, Debug)]
struct SchemaJob {
    change: SchemaChange,
    state: SchemaJobState,
    work_id: Option<WorkId>,
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaJobError {
    #[error("schema job {0:?} does not exist")]
    UnknownJob(SchemaJobId),
    #[error("schema job {0:?} is not pending")]
    InvalidState(SchemaJobId),
    #[error("schema job dispatch must be schema work")]
    WrongWorkClass,
    #[error("schema job {expected:?} does not match dispatched work {actual:?}")]
    WorkMismatch { expected: WorkId, actual: WorkId },
    #[error("reactor error: {0}")]
    Reactor(#[from] ReactorError),
    #[error("schema change error: {0}")]
    Database(#[from] DbError),
}

impl Catalog {
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn create_table(&mut self, table: TableDefinition) -> Result<()> {
        self.create_table_with_primary_key(table, None)
    }

    pub fn create_table_with_primary_key(
        &mut self,
        table: TableDefinition,
        primary_key: Option<Vec<ColumnId>>,
    ) -> Result<()> {
        table.validate()?;
        if self.tables.contains_key(&table.id)
            || self
                .tables
                .values()
                .any(|candidate| candidate.name == table.name)
        {
            return Err(DbError::InvalidState(format!(
                "table {} or name {} already exists",
                table.id.0, table.name
            )));
        }
        if let Some(columns) = &primary_key {
            self.validate_primary_key(&table, columns)?;
        }
        let table_id = table.id;
        self.tables.insert(table_id, table);
        if let Some(columns) = primary_key {
            self.primary_keys.insert(table_id, columns);
        }
        self.generation += 1;
        Ok(())
    }

    fn validate_primary_key(&self, table: &TableDefinition, columns: &[ColumnId]) -> Result<()> {
        if columns.is_empty() {
            return Err(DbError::InvalidState(format!(
                "table {} primary key must not be empty",
                table.id.0
            )));
        }
        let mut distinct = BTreeSet::new();
        for column_id in columns {
            let column = table.column(*column_id)?;
            if !distinct.insert(*column_id) {
                return Err(DbError::InvalidState(format!(
                    "table {} primary key repeats column {}",
                    table.id.0, column_id.0
                )));
            }
            if column.nullable {
                return Err(DbError::InvalidState(format!(
                    "table {} primary-key column {} must be NOT NULL",
                    table.id.0, column.name
                )));
            }
        }
        Ok(())
    }

    /// Append one nullable column without changing the meaning or position of
    /// existing columns. The owning store publishes the candidate catalog;
    /// older physical rows are logically extended with `NULL` at read time.
    pub fn add_nullable_column(&mut self, table: TableId, column: ColumnDefinition) -> Result<()> {
        if !column.nullable {
            return Err(DbError::InvalidState(
                "additive column changes must be nullable".to_owned(),
            ));
        }
        let table_definition = self.table(table)?;
        if table_definition
            .columns
            .iter()
            .any(|existing| existing.id == column.id || existing.name == column.name)
        {
            return Err(DbError::InvalidState(format!(
                "column {} or name {} already exists in table {}",
                column.id.0, column.name, table.0
            )));
        }
        self.tables
            .get_mut(&table)
            .expect("table was checked above")
            .columns
            .push(column);
        self.generation += 1;
        Ok(())
    }

    pub fn create_index(&mut self, index: IndexDefinition) -> Result<()> {
        self.validate_index_columns(&index)?;
        self.register_index(index)
    }

    /// Register one named index in the catalog. SQL object names are durable
    /// metadata owned by this catalog; callers cannot create a name that is
    /// silently discarded by the physical index definition.
    pub fn create_named_index(&mut self, index: IndexDefinition, name: String) -> Result<()> {
        self.validate_index_name(&name)?;
        self.validate_index_columns(&index)?;
        if self.indexes.contains_key(&index.id) {
            return Err(DbError::InvalidState(format!(
                "index {} already exists",
                index.id.0
            )));
        }
        self.indexes.insert(index.id, index.clone());
        self.index_names.insert(index.id, name);
        self.generation += 1;
        Ok(())
    }

    pub fn create_foreign_key(&mut self, foreign_key: ForeignKeyDefinition) -> Result<()> {
        self.validate_foreign_key(&foreign_key)?;
        if self.foreign_keys.contains_key(&foreign_key.id) {
            return Err(DbError::InvalidState(format!(
                "constraint {} already exists",
                foreign_key.id.0
            )));
        }
        self.foreign_keys.insert(foreign_key.id, foreign_key);
        self.generation += 1;
        Ok(())
    }

    /// Register one named foreign key in the durable catalog.
    pub fn create_named_foreign_key(
        &mut self,
        foreign_key: ForeignKeyDefinition,
        name: String,
    ) -> Result<()> {
        self.validate_constraint_name(&name)?;
        self.validate_foreign_key(&foreign_key)?;
        if self.foreign_keys.contains_key(&foreign_key.id) {
            return Err(DbError::InvalidState(format!(
                "constraint {} already exists",
                foreign_key.id.0
            )));
        }
        let id = foreign_key.id;
        self.foreign_keys.insert(foreign_key.id, foreign_key);
        self.foreign_key_names.insert(id, name);
        self.generation += 1;
        Ok(())
    }

    fn register_index(&mut self, index: IndexDefinition) -> Result<()> {
        self.validate_index_columns(&index)?;
        if self.indexes.contains_key(&index.id) {
            if self.indexes.get(&index.id) == Some(&index) {
                return Ok(());
            }
            return Err(DbError::InvalidState(format!(
                "index {} already exists",
                index.id.0
            )));
        }
        self.indexes.insert(index.id, index);
        self.generation += 1;
        Ok(())
    }

    fn validate_index_name(&self, name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(DbError::InvalidState(
                "index name must not be empty".to_owned(),
            ));
        }
        if self.index_names.values().any(|existing| existing == name)
            || self
                .foreign_key_names
                .values()
                .any(|existing| existing == name)
        {
            return Err(DbError::InvalidState(format!(
                "index name {name} already exists"
            )));
        }
        Ok(())
    }

    fn validate_constraint_name(&self, name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(DbError::InvalidState(
                "constraint name must not be empty".to_owned(),
            ));
        }
        if self.index_names.values().any(|existing| existing == name)
            || self
                .foreign_key_names
                .values()
                .any(|existing| existing == name)
        {
            return Err(DbError::InvalidState(format!(
                "constraint name {name} already exists"
            )));
        }
        Ok(())
    }

    fn validate_index_columns(&self, index: &IndexDefinition) -> Result<()> {
        if index.columns.is_empty() {
            return Err(DbError::InvalidState(format!(
                "index {} must define at least one column",
                index.id.0
            )));
        }
        let table = self.table(index.table)?;
        let mut columns = std::collections::BTreeSet::new();
        for column in &index.columns {
            table.column(*column)?;
            if !columns.insert(*column) {
                return Err(DbError::InvalidState(format!(
                    "index {} repeats column {}",
                    index.id.0, column.0
                )));
            }
        }
        Ok(())
    }

    fn validate_foreign_key(&self, foreign_key: &ForeignKeyDefinition) -> Result<()> {
        if foreign_key.columns.is_empty()
            || foreign_key.columns.len() != foreign_key.referenced_columns.len()
        {
            return Err(DbError::InvalidState(format!(
                "foreign key {} must have equally sized non-empty column lists",
                foreign_key.id.0
            )));
        }
        let table = self.table(foreign_key.table)?;
        let referenced_table = self.table(foreign_key.referenced_table)?;
        let mut columns = std::collections::BTreeSet::new();
        let mut referenced_columns = std::collections::BTreeSet::new();
        for (column, referenced_column) in foreign_key
            .columns
            .iter()
            .zip(&foreign_key.referenced_columns)
        {
            if !columns.insert(*column) || !referenced_columns.insert(*referenced_column) {
                return Err(DbError::InvalidState(format!(
                    "foreign key {} repeats a column",
                    foreign_key.id.0
                )));
            }
            let child = table.column(*column)?;
            let parent = referenced_table.column(*referenced_column)?;
            if child.data_type != parent.data_type {
                return Err(DbError::InvalidState(format!(
                    "foreign key {} maps incompatible column types",
                    foreign_key.id.0
                )));
            }
            if foreign_key.on_delete == ReferentialAction::SetNull && !child.nullable {
                return Err(DbError::InvalidState(format!(
                    "foreign key {} declares ON DELETE SET NULL on non-nullable column {}",
                    foreign_key.id.0, child.id.0
                )));
            }
        }
        if !self.indexes.values().any(|index| {
            index.table == foreign_key.referenced_table
                && index.unique
                && index.columns == foreign_key.referenced_columns
        }) {
            return Err(DbError::InvalidState(format!(
                "foreign key {} requires a unique index on referenced columns",
                foreign_key.id.0
            )));
        }
        Ok(())
    }

    pub fn table(&self, table: TableId) -> Result<&TableDefinition> {
        self.tables
            .get(&table)
            .ok_or_else(|| DbError::InvalidState(format!("table {} does not exist", table.0)))
    }

    /// Return the catalog-owned primary-key order for a table. `None` means
    /// the table still uses the legacy fixed-width typed [`Key`] contract.
    #[must_use]
    pub fn primary_key(&self, table: TableId) -> Option<&[ColumnId]> {
        self.primary_keys.get(&table).map(Vec::as_slice)
    }

    /// Iterate table definitions in stable ID order for storage migration and
    /// deterministic conformance tooling.
    pub fn tables(&self) -> impl Iterator<Item = &TableDefinition> {
        self.tables.values()
    }

    /// Iterate index definitions in stable ID order for storage migration and
    /// deterministic conformance tooling.
    pub fn indexes(&self) -> impl Iterator<Item = &IndexDefinition> {
        self.indexes.values()
    }

    pub(crate) fn indexes_for(&self, table: TableId) -> impl Iterator<Item = &IndexDefinition> {
        self.indexes
            .values()
            .filter(move |index| index.table == table)
    }

    pub(crate) fn index(&self, id: IndexId) -> Option<&IndexDefinition> {
        self.indexes.get(&id)
    }

    /// Return the durable SQL name for an index, if it was created through a
    /// named schema path.
    #[must_use]
    pub fn index_name(&self, id: IndexId) -> Option<&str> {
        self.index_names.get(&id).map(String::as_str)
    }

    /// Return the durable SQL name for a foreign key, if one was supplied.
    #[must_use]
    pub fn foreign_key_name(&self, id: ConstraintId) -> Option<&str> {
        self.foreign_key_names.get(&id).map(String::as_str)
    }

    /// Iterate foreign-key definitions in stable ID order for storage
    /// migration and deterministic conformance tooling.
    pub fn foreign_keys(&self) -> impl Iterator<Item = &ForeignKeyDefinition> {
        self.foreign_keys.values()
    }
}

fn catalog_key() -> Key {
    Key::new(CATALOG_KEY_TENANT, 0)
}

fn catalog_mutation(catalog: &Catalog) -> Result<Mutation> {
    Ok(Mutation::Put {
        key: catalog_key(),
        value: encode_catalog(catalog)?,
    })
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Bytes(Vec<u8>),
    Bool(bool),
    I64(i64),
    U64(u64),
    Text(String),
}

impl Value {
    pub(crate) fn matches(&self, data_type: ColumnType) -> bool {
        matches!(
            (self, data_type),
            (Self::Null, _)
                | (Self::Bytes(_), ColumnType::Bytes)
                | (Self::Bool(_), ColumnType::Bool)
                | (Self::I64(_), ColumnType::I64)
                | (Self::U64(_), ColumnType::U64)
                | (Self::Text(_), ColumnType::Text)
        )
    }

    pub(crate) fn encode(&self, bytes: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Null => bytes.push(0),
            Self::Bytes(value) => {
                bytes.push(1);
                put_bytes(bytes, value)?;
            }
            Self::Bool(value) => bytes.extend_from_slice(&[2, u8::from(*value)]),
            Self::I64(value) => {
                bytes.push(3);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Self::U64(value) => {
                bytes.push(4);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Self::Text(value) => {
                bytes.push(5);
                put_bytes(bytes, value.as_bytes())?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row {
    pub primary: Key,
    pub values: Vec<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelationalMutation {
    Insert {
        table: TableId,
        row: Row,
    },
    Update {
        table: TableId,
        row: Row,
    },
    Delete {
        table: TableId,
        primary: Key,
    },
    /// Delete by the catalog-owned row identity derived from the row values.
    /// This is used by SQL for composite primary keys; the legacy typed API
    /// continues to use [`Self::Delete`].
    #[doc(hidden)]
    DeleteRow {
        table: TableId,
        row: Row,
    },
}

#[derive(Debug)]
pub struct RelationalTransaction {
    transaction: Transaction,
    catalog: Catalog,
}

impl Row {
    pub fn validate(&self, table: &TableDefinition) -> Result<()> {
        if self.values.len() != table.columns.len() {
            return Err(DbError::InvalidState(format!(
                "row has {} values but table {} requires {}",
                self.values.len(),
                table.id.0,
                table.columns.len()
            )));
        }
        for (value, column) in self.values.iter().zip(&table.columns) {
            if matches!(value, Value::Null) && !column.nullable {
                return Err(DbError::InvalidState(format!(
                    "column {} is not nullable",
                    column.name
                )));
            }
            if !value.matches(column.data_type) {
                return Err(DbError::InvalidState(format!(
                    "column {} has the wrong type",
                    column.name
                )));
            }
        }
        Ok(())
    }

    /// Validate a stored row that may predate appended nullable columns.
    ///
    /// The physical row format is intentionally allowed to lag the catalog
    /// for this one metadata-only schema transition. Callers crossing back
    /// into the logical API should use [`Self::materialize_for`] so missing
    /// trailing values become explicit `NULL`s.
    pub(crate) fn validate_stored(&self, table: &TableDefinition) -> Result<()> {
        if self.values.len() > table.columns.len() {
            return Err(DbError::InvalidState(format!(
                "stored row has {} values but table {} requires at most {}",
                self.values.len(),
                table.id.0,
                table.columns.len()
            )));
        }
        for (value, column) in self.values.iter().zip(&table.columns) {
            if matches!(value, Value::Null) && !column.nullable {
                return Err(DbError::InvalidState(format!(
                    "column {} is not nullable",
                    column.name
                )));
            }
            if !value.matches(column.data_type) {
                return Err(DbError::InvalidState(format!(
                    "column {} has the wrong type",
                    column.name
                )));
            }
        }
        if table.columns[self.values.len()..]
            .iter()
            .any(|column| !column.nullable)
        {
            return Err(DbError::InvalidState(
                "stored row is missing a non-nullable column".to_owned(),
            ));
        }
        Ok(())
    }

    /// Return the logical row shape for a catalog that may have appended
    /// nullable columns since this row was written.
    pub(crate) fn materialize_for(&self, table: &TableDefinition) -> Result<Self> {
        self.validate_stored(table)?;
        if self.values.len() == table.columns.len() {
            return Ok(self.clone());
        }
        let mut row = self.clone();
        row.values.resize(table.columns.len(), Value::Null);
        Ok(row)
    }

    pub(crate) fn value(&self, table: &TableDefinition, column: ColumnId) -> Result<&Value> {
        let position = table
            .columns
            .iter()
            .position(|candidate| candidate.id == column)
            .ok_or_else(|| DbError::InvalidState(format!("column {} does not exist", column.0)))?;
        self.values
            .get(position)
            .ok_or_else(|| DbError::InvalidState(format!("row is missing column {}", column.0)))
    }

    pub(crate) fn set_value(
        &mut self,
        table: &TableDefinition,
        column: ColumnId,
        value: Value,
    ) -> Result<()> {
        let position = table
            .columns
            .iter()
            .position(|candidate| candidate.id == column)
            .ok_or_else(|| DbError::InvalidState(format!("column {} does not exist", column.0)))?;
        let slot = self
            .values
            .get_mut(position)
            .ok_or_else(|| DbError::InvalidState(format!("row is missing column {}", column.0)))?;
        *slot = value;
        Ok(())
    }
}

pub fn encode_row(row: &Row) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&ROW_MAGIC);
    bytes.push(ROW_VERSION);
    bytes.extend_from_slice(
        &u32::try_from(row.values.len())
            .map_err(|_| DbError::InvalidState("too many row values".to_owned()))?
            .to_le_bytes(),
    );
    for value in &row.values {
        value.encode(&mut bytes)?;
    }
    Ok(bytes)
}

pub fn decode_row(primary: Key, bytes: &[u8]) -> Result<Row> {
    if bytes.len() < 9 || bytes[..4] != ROW_MAGIC || bytes[4] != ROW_VERSION {
        return Err(DbError::Corruption {
            artifact: "row",
            reason: "invalid row header".to_owned(),
        });
    }
    let count = u32::from_le_bytes(bytes[5..9].try_into().expect("row count width"));
    if count as usize > bytes.len() {
        return Err(DbError::Corruption {
            artifact: "row",
            reason: "row value count exceeds payload".to_owned(),
        });
    }
    let mut cursor = 9;
    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let tag = *bytes.get(cursor).ok_or_else(|| DbError::Corruption {
            artifact: "row",
            reason: "missing value tag".to_owned(),
        })?;
        cursor += 1;
        let value =
            match tag {
                0 => Value::Null,
                1 => Value::Bytes(read_bytes(bytes, &mut cursor)?),
                2 => Value::Bool(match bytes.get(cursor) {
                    Some(0) => false,
                    Some(1) => true,
                    _ => {
                        return Err(DbError::Corruption {
                            artifact: "row",
                            reason: "invalid boolean".to_owned(),
                        });
                    }
                }),
                3 => Value::I64(read_i64(bytes, &mut cursor)?),
                4 => Value::U64(read_u64(bytes, &mut cursor)?),
                5 => Value::Text(String::from_utf8(read_bytes(bytes, &mut cursor)?).map_err(
                    |_| DbError::Corruption {
                        artifact: "row",
                        reason: "text is not UTF-8".to_owned(),
                    },
                )?),
                _ => {
                    return Err(DbError::Corruption {
                        artifact: "row",
                        reason: "unknown value tag".to_owned(),
                    });
                }
            };
        if tag == 2 {
            cursor += 1;
        }
        values.push(value);
    }
    if cursor != bytes.len() {
        return Err(DbError::Corruption {
            artifact: "row",
            reason: "trailing row bytes".to_owned(),
        });
    }
    Ok(Row { primary, values })
}

#[derive(Debug)]
pub struct RelationalStore {
    database: Database,
    catalog: Catalog,
    directory: PathBuf,
    schema_jobs: BTreeMap<SchemaJobId, SchemaJob>,
    next_schema_job: u64,
}

impl RelationalStore {
    pub fn create(config: DatabaseConfig) -> Result<Self> {
        let directory = config.directory.clone();
        Ok(Self {
            database: Database::create(config)?,
            catalog: Catalog::default(),
            directory,
            schema_jobs: BTreeMap::new(),
            next_schema_job: 0,
        })
    }

    /// Consume this store after closing its temporary storage handle.
    pub fn close(self) -> Result<()> {
        self.database.close()
    }

    pub fn open(config: DatabaseConfig, faults: &mut dyn FaultInjector) -> Result<Self> {
        let directory = config.directory.clone();
        let database = Database::open(config, faults)?;
        let catalog = if let Some(bytes) = database.get(database.commit_id(), catalog_key())? {
            decode_catalog(&bytes)?
        } else if database.commit_id().0 == 0 {
            Catalog::default()
        } else {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "durable catalog record is missing for non-empty database".to_owned(),
            });
        };
        let durable_jobs = read_schema_journal(&schema_journal_path(&directory))?;
        let next_schema_job = match durable_jobs.keys().next_back() {
            Some(id) => id.0.checked_add(1).ok_or_else(|| DbError::Corruption {
                artifact: "schema journal",
                reason: "schema job ID space exhausted".to_owned(),
            })?,
            None => 0,
        };
        let schema_jobs = durable_jobs
            .into_iter()
            .map(|(id, job)| {
                (
                    id,
                    SchemaJob {
                        change: job.change,
                        state: job.state,
                        work_id: None,
                    },
                )
            })
            .collect();
        let mut store = Self {
            database,
            catalog,
            directory,
            schema_jobs,
            next_schema_job,
        };
        store.recover_schema_jobs(faults)?;
        let mut catalog_indexes: Vec<IndexId> = store.catalog.indexes.keys().copied().collect();
        let mut database_indexes = store.database.secondary_index_ids();
        catalog_indexes.sort_unstable();
        database_indexes.sort_unstable();
        if catalog_indexes != database_indexes {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "catalog/index definitions differ from durable indexes".to_owned(),
            });
        }
        Ok(store)
    }

    pub fn checkpoint(&mut self, faults: &mut dyn FaultInjector) -> Result<()> {
        self.database.checkpoint(faults)
    }

    #[must_use]
    pub fn commit_id(&self) -> CommitId {
        self.database.commit_id()
    }

    /// Return every durable logical commit boundary. The temporary kernel
    /// refuses this observation for legacy checkpoint payloads that did not
    /// persist an authoritative commit catalog.
    pub(crate) fn published_commits(&self) -> Result<Vec<CommitId>> {
        self.database.published_commits()
    }

    /// Return the stable identity for this temporary database history.
    #[must_use]
    pub fn storage_identity(&self) -> StorageIdentity {
        self.database.storage_identity()
    }

    #[must_use]
    pub(crate) fn generation(&self) -> u64 {
        self.database.generation()
    }

    #[must_use]
    pub(crate) fn requires_recovery(&self) -> bool {
        self.database.requires_recovery()
    }

    /// Resolve a durable transaction attempt after reopening this history.
    pub fn resolve_attempt(&self, attempt: TransactionAttemptId) -> Result<Option<AttemptRecord>> {
        Ok(self.database.resolve_attempt(attempt))
    }

    pub(crate) fn attempt_records(&self, limit: usize) -> Result<Vec<AttemptRecord>> {
        self.database.attempt_records(limit)
    }

    pub(crate) fn import_attempt_records(
        &mut self,
        records: &[AttemptRecord],
    ) -> Result<Vec<AttemptRecord>> {
        self.database.import_attempt_records(records, &mut NoFaults)
    }

    /// Forget durable attempt records after deciding that their identities
    /// will never be reused.
    pub fn forget_attempts(&mut self, attempts: &[TransactionAttemptId]) -> Result<usize> {
        self.database.forget_attempts(attempts, &mut NoFaults)
    }

    /// Return the last catalog generation published by this store.
    ///
    /// Catalog changes must go through the store's schema methods so the
    /// durable catalog and the row/index state are published atomically.
    #[must_use]
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub(crate) fn catalog_at(&self, snapshot: CommitId) -> Result<Catalog> {
        match self.database.get(snapshot, catalog_key())? {
            Some(bytes) => decode_catalog(&bytes),
            None if snapshot == CommitId(0) => Ok(Catalog::default()),
            None => Err(DbError::Corruption {
                artifact: "catalog",
                reason: format!("durable catalog record is missing at commit {}", snapshot.0),
            }),
        }
    }

    pub fn retain(&mut self, snapshot: CommitId) -> Result<()> {
        self.database.retain(snapshot)
    }

    pub fn release(&mut self, snapshot: CommitId) {
        self.database.release(snapshot);
    }

    /// Return the number of historical snapshots that migration would need to
    /// invalidate if it copied only the current state.
    #[must_use]
    pub fn retained_snapshot_count(&self) -> usize {
        self.database.retained_snapshot_count()
    }

    /// Return explicitly retained snapshot commits in ascending order.
    ///
    /// This is an observation of this store handle's retention leases. It is
    /// not a commit-history catalog and does not acquire or extend a lease.
    #[must_use]
    pub fn retained_snapshot_commits(&self) -> Vec<CommitId> {
        self.database.retained_snapshot_commits()
    }

    pub(crate) fn capture_snapshot(
        &self,
        snapshot: CommitId,
        options: RelationalSnapshotCaptureOptions,
        rows_captured: &mut usize,
    ) -> Result<RelationalSnapshot> {
        let catalog = self.catalog_at(snapshot)?;
        let mut tables = Vec::with_capacity(catalog.tables.len());
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

    pub fn compact_with_budget(&mut self, budget: CompactionBudget) -> Result<CompactionReport> {
        self.database.compact_with_budget(budget)
    }

    pub fn compact_with_key_budget(&mut self, max_keys: usize) -> Result<CompactionReport> {
        self.database.compact_with_key_budget(max_keys)
    }

    pub fn compact_with_budget_and_faults(
        &mut self,
        budget: CompactionBudget,
        faults: &mut dyn FaultInjector,
    ) -> Result<CompactionReport> {
        self.database.compact_with_budget_and_faults(budget, faults)
    }

    #[must_use]
    pub fn metrics(&self) -> &DatabaseMetrics {
        self.database.metrics()
    }

    /// Verify the durable temporary history and its current relational view.
    ///
    /// The reopen is intentionally performed against a separate in-memory
    /// database. It replays the manifest, packed range, and WAL without
    /// publishing or reclaiming anything in this handle. The logical pass
    /// then checks the durable catalog, rows, foreign keys, and exact
    /// secondary-index membership.
    pub(crate) fn verify(&self) -> Result<LogicalVerification> {
        if self.database.requires_recovery() {
            return Err(DbError::RecoveryRequired);
        }
        let mut faults = NoFaults;
        let recovered = Database::open_for_verification(
            DatabaseConfig {
                directory: self.directory.clone(),
            },
            &mut faults,
        )?;
        if recovered.commit_id() != self.commit_id()
            || recovered.generation() != self.database.generation()
            || recovered.secondary_index_ids() != self.database.secondary_index_ids()
        {
            return Err(DbError::Corruption {
                artifact: "temporary database",
                reason: "reopened durable state differs from the active handle".to_owned(),
            });
        }

        let snapshot = self.commit_id();
        let catalog = self.catalog_at(snapshot)?;
        if catalog != self.catalog {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "durable catalog differs from the active catalog".to_owned(),
            });
        }

        let mut rows_by_table = BTreeMap::new();
        let mut row_count = 0;
        for table in catalog.tables() {
            let rows = self.scan(table.id, snapshot, usize::MAX)?;
            row_count += rows.len();
            rows_by_table.insert(table.id, rows);
        }
        for foreign_key in catalog.foreign_keys() {
            let child_table = catalog.table(foreign_key.table)?;
            let referenced_table = catalog.table(foreign_key.referenced_table)?;
            self.validate_foreign_key_rows(
                foreign_key,
                child_table,
                referenced_table,
                rows_by_table
                    .get(&foreign_key.table)
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "catalog",
                        reason: "foreign-key child table is missing".to_owned(),
                    })?,
                rows_by_table
                    .get(&foreign_key.referenced_table)
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "catalog",
                        reason: "foreign-key referenced table is missing".to_owned(),
                    })?,
            )?;
        }

        let mut index_entry_count = 0;
        for index in catalog.indexes() {
            let table = catalog.table(index.table)?;
            let rows = rows_by_table
                .get(&index.table)
                .ok_or_else(|| DbError::Corruption {
                    artifact: "catalog",
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
                    artifact: "secondary index",
                    reason: format!("unique index {} contains duplicate row values", index.id.0),
                });
            }
            let actual = self
                .database
                .index_scan_bytes(snapshot, index.id, &[], &[u8::MAX], usize::MAX)?
                .into_iter()
                .map(|(key, identity)| Ok((key, identity)))
                .collect::<Result<Vec<_>>>()?
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
                    artifact: "secondary index",
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

    pub fn create_table(&mut self, table: TableDefinition) -> Result<CommitId> {
        self.create_table_with_faults(table, &mut NoFaults)
    }

    /// Atomically append a nullable column and publish the candidate catalog.
    /// Existing physical rows are logically backfilled with `NULL` at reads,
    /// avoiding a table-sized rewrite.
    pub fn add_nullable_column(
        &mut self,
        table: TableId,
        column: ColumnDefinition,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        let mut candidate = self.catalog.clone();
        candidate.add_nullable_column(table, column)?;
        let mut transaction = self.database.begin();
        transaction.get(&self.database, catalog_key())?;
        transaction.put(catalog_key(), encode_catalog(&candidate)?);
        let commit = self.commit_transaction(transaction, &candidate, faults)?;
        self.catalog = candidate;
        Ok(commit)
    }

    fn create_table_with_faults(
        &mut self,
        table: TableDefinition,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        let mut candidate = self.catalog.clone();
        candidate.create_table(table)?;
        let commit = self
            .database
            .commit(vec![catalog_mutation(&candidate)?], faults)?;
        self.catalog = candidate;
        Ok(commit)
    }

    /// Publish a new table and its schema objects in one durable commit.
    pub fn create_table_with_schema(
        &mut self,
        table: TableDefinition,
        schema: RelationalSchemaDefinition,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.create_table_with_schema_and_primary_key(table, None, schema, faults)
    }

    pub fn create_table_with_schema_and_primary_key(
        &mut self,
        table: TableDefinition,
        primary_key: Option<Vec<ColumnId>>,
        schema: RelationalSchemaDefinition,
        faults: &mut dyn FaultInjector,
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
        let mut mutations = schema
            .indexes
            .into_iter()
            .map(|named| Mutation::CreateIndex {
                index: named.definition.id,
                unique: named.definition.unique,
            })
            .collect::<Vec<_>>();
        mutations.push(catalog_mutation(&candidate)?);
        let commit = self.database.commit(mutations, faults)?;
        self.catalog = candidate;
        Ok(commit)
    }

    pub fn submit_schema_job(
        &mut self,
        reactor: &mut Reactor,
        change: SchemaChange,
        deadline: Option<u64>,
        faults: &mut dyn FaultInjector,
    ) -> std::result::Result<SchemaJobId, SchemaJobError> {
        if self.schema_jobs.len() >= MAX_SCHEMA_JOBS {
            let terminal = self.schema_jobs.iter().find_map(|(id, job)| {
                matches!(
                    job.state,
                    SchemaJobState::Completed | SchemaJobState::Failed(_)
                )
                .then_some(*id)
            });
            let Some(terminal) = terminal else {
                return Err(SchemaJobError::Database(DbError::InvalidState(
                    "schema job queue is full".to_owned(),
                )));
            };
            let mut retained = self.schema_jobs.clone();
            retained.remove(&terminal);
            rewrite_schema_journal(&schema_journal_path(&self.directory), &retained, faults)
                .map_err(SchemaJobError::Database)?;
            self.schema_jobs = retained;
        }
        let id = SchemaJobId(self.next_schema_job);
        self.next_schema_job = self.next_schema_job.checked_add(1).ok_or_else(|| {
            SchemaJobError::Database(DbError::InvalidState(
                "schema job ID space exhausted".to_owned(),
            ))
        })?;
        let payload = encode_schema_change(&change)?;
        let work_id = reactor.submit(WorkClass::Schema, 1, deadline)?;
        if let Err(error) = append_schema_record(
            &schema_journal_path(&self.directory),
            id,
            SCHEMA_SUBMIT,
            &payload,
            faults,
        ) {
            reactor.cancel_queued(work_id)?;
            return Err(SchemaJobError::Database(error));
        }
        self.schema_jobs.insert(
            id,
            SchemaJob {
                change,
                state: SchemaJobState::Pending,
                work_id: Some(work_id),
            },
        );
        Ok(id)
    }

    pub fn resume_schema_job(
        &mut self,
        reactor: &mut Reactor,
        id: SchemaJobId,
        deadline: Option<u64>,
    ) -> std::result::Result<WorkId, SchemaJobError> {
        let job = self
            .schema_jobs
            .get_mut(&id)
            .ok_or(SchemaJobError::UnknownJob(id))?;
        if !matches!(job.state, SchemaJobState::Pending) || job.work_id.is_some() {
            return Err(SchemaJobError::InvalidState(id));
        }
        let work_id = reactor.submit(WorkClass::Schema, 1, deadline)?;
        job.work_id = Some(work_id);
        Ok(work_id)
    }

    pub fn run_schema_job(
        &mut self,
        reactor: &mut Reactor,
        dispatch: &Dispatch,
        id: SchemaJobId,
        faults: &mut dyn FaultInjector,
    ) -> std::result::Result<SchemaJobState, SchemaJobError> {
        let job = self
            .schema_jobs
            .get(&id)
            .ok_or(SchemaJobError::UnknownJob(id))?;
        if !matches!(job.state, SchemaJobState::Pending) {
            return Err(SchemaJobError::InvalidState(id));
        }
        if dispatch.work.class != WorkClass::Schema {
            return Err(SchemaJobError::WrongWorkClass);
        }
        let expected_work = job.work_id.ok_or(SchemaJobError::InvalidState(id))?;
        if dispatch.work.id != expected_work {
            return Err(SchemaJobError::WorkMismatch {
                expected: expected_work,
                actual: dispatch.work.id,
            });
        }
        let change = job.change.clone();
        if let Err(error) = append_schema_record(
            &schema_journal_path(&self.directory),
            id,
            SCHEMA_RUNNING,
            &[],
            faults,
        ) {
            reactor.complete(dispatch.worker)?;
            return Err(SchemaJobError::Database(error));
        }
        if let Some(job) = self.schema_jobs.get_mut(&id) {
            job.state = SchemaJobState::Running;
        }
        let result = self.ensure_schema_change(change, faults);
        let state = match result {
            Ok(()) => SchemaJobState::Completed,
            Err(error) => SchemaJobState::Failed(error.to_string()),
        };
        let payload = match &state {
            SchemaJobState::Completed => Vec::new(),
            SchemaJobState::Failed(message) => encode_string(message)?,
            _ => unreachable!("schema job result is terminal"),
        };
        let record = match &state {
            SchemaJobState::Completed => SCHEMA_COMPLETED,
            SchemaJobState::Failed(_) => SCHEMA_FAILED,
            _ => unreachable!("schema job result is terminal"),
        };
        if let Err(error) = append_schema_record(
            &schema_journal_path(&self.directory),
            id,
            record,
            &payload,
            faults,
        ) {
            reactor.complete(dispatch.worker)?;
            return Err(SchemaJobError::Database(error));
        }
        let completion = reactor.complete(dispatch.worker);
        if let Some(job) = self.schema_jobs.get_mut(&id) {
            job.state = state.clone();
            job.work_id = None;
        }
        completion?;
        if let SchemaJobState::Failed(message) = &state {
            return Err(SchemaJobError::Database(DbError::InvalidState(
                message.clone(),
            )));
        }
        Ok(state)
    }

    fn recover_schema_jobs(&mut self, faults: &mut dyn FaultInjector) -> Result<()> {
        let path = schema_journal_path(&self.directory);
        let recovery_ids: Vec<SchemaJobId> = self
            .schema_jobs
            .iter()
            .filter_map(|(id, job)| {
                matches!(
                    job.state,
                    SchemaJobState::Running | SchemaJobState::Completed
                )
                .then_some(*id)
            })
            .collect();
        for id in recovery_ids {
            let change = self
                .schema_jobs
                .get(&id)
                .ok_or_else(|| DbError::Corruption {
                    artifact: "schema journal",
                    reason: format!("schema job {} disappeared during recovery", id.0),
                })?
                .change
                .clone();
            self.ensure_schema_change(change, faults)?;
            let was_running = self
                .schema_jobs
                .get(&id)
                .is_some_and(|job| matches!(job.state, SchemaJobState::Running));
            if was_running {
                append_schema_record(&path, id, SCHEMA_COMPLETED, &[], faults)?;
                if let Some(job) = self.schema_jobs.get_mut(&id) {
                    job.state = SchemaJobState::Completed;
                }
            }
        }
        Ok(())
    }

    fn ensure_schema_change(
        &mut self,
        change: SchemaChange,
        faults: &mut dyn FaultInjector,
    ) -> Result<()> {
        match change {
            SchemaChange::CreateTable(table) => {
                if let Some(existing) = self.catalog.tables.get(&table.id) {
                    if existing == &table {
                        return Ok(());
                    }
                    return Err(DbError::InvalidState(format!(
                        "recovered table {} conflicts with catalog",
                        table.id.0
                    )));
                }
                self.create_table_with_faults(table, faults).map(|_| ())
            }
            SchemaChange::CreateIndex(index) => {
                if let Some(existing) = self.catalog.indexes.get(&index.id) {
                    if existing == &index {
                        return Ok(());
                    }
                    return Err(DbError::InvalidState(format!(
                        "recovered index {} conflicts with catalog",
                        index.id.0
                    )));
                }
                if let Ok(unique) = self.database.secondary_index_unique(index.id) {
                    if unique != index.unique {
                        return Err(DbError::InvalidState(format!(
                            "recovered index {} uniqueness differs from durable index",
                            index.id.0
                        )));
                    }
                    let mut candidate = self.catalog.clone();
                    candidate.register_index(index)?;
                    self.database
                        .commit(vec![catalog_mutation(&candidate)?], faults)?;
                    self.catalog = candidate;
                    Ok(())
                } else {
                    self.create_index(index, faults).map(|_| ())
                }
            }
        }
    }

    #[must_use]
    pub fn schema_job_status(&self, id: SchemaJobId) -> Option<SchemaJobState> {
        self.schema_jobs.get(&id).map(|job| job.state.clone())
    }

    pub fn create_index(
        &mut self,
        index: IndexDefinition,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.create_index_with_name(index, None, faults)
    }

    pub fn create_named_index(
        &mut self,
        index: IndexDefinition,
        name: String,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.create_index_with_name(index, Some(name), faults)
    }

    fn create_index_with_name(
        &mut self,
        index: IndexDefinition,
        name: Option<String>,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        if self.catalog.indexes.contains_key(&index.id) {
            return Err(DbError::InvalidState(format!(
                "index {} already exists",
                index.id.0
            )));
        }
        let mut candidate = self.catalog.clone();
        match name {
            Some(name) => candidate.create_named_index(index.clone(), name)?,
            None => candidate.create_index(index.clone())?,
        }
        let table = candidate.table(index.table)?;
        let (start, end) = table_range(index.table);
        let existing =
            self.database
                .scan_bytes(self.database.commit_id(), start, end, usize::MAX)?;
        let mut mutations = vec![Mutation::CreateIndex {
            index: index.id,
            unique: index.unique,
        }];
        for (physical_primary, bytes) in existing {
            let identity = row_identity_from_storage_key(index.table, &physical_primary)?;
            let row = row_from_storage_identity(&candidate, table, identity, &bytes)?;
            if let Some(index_key) = row_index_key(table, &index, &row)? {
                mutations.push(Mutation::ByteIndexPut {
                    index: index.id,
                    index_key,
                    primary: identity.to_vec(),
                });
            }
        }
        mutations.push(catalog_mutation(&candidate)?);
        let commit = self.database.commit(mutations, faults)?;
        self.catalog = candidate;
        Ok(commit)
    }

    pub fn create_foreign_key(&mut self, foreign_key: ForeignKeyDefinition) -> Result<CommitId> {
        self.create_foreign_key_with_name(foreign_key, None)
    }

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
        let child_table = candidate.table(foreign_key.table)?;
        let referenced_table = candidate.table(foreign_key.referenced_table)?;
        let snapshot = self.database.commit_id();
        let (child_start, child_end) = table_range(foreign_key.table);
        let child_rows = decode_rows(
            &candidate,
            child_table,
            self.database
                .scan_bytes(snapshot, child_start, child_end, usize::MAX)?,
        )?;
        let (referenced_start, referenced_end) = table_range(foreign_key.referenced_table);
        let referenced_rows = decode_rows(
            &candidate,
            referenced_table,
            self.database
                .scan_bytes(snapshot, referenced_start, referenced_end, usize::MAX)?,
        )?;
        self.validate_foreign_key_rows(
            &foreign_key,
            child_table,
            referenced_table,
            &child_rows,
            &referenced_rows,
        )?;
        let commit = self
            .database
            .commit(vec![catalog_mutation(&candidate)?], &mut NoFaults)?;
        self.catalog = candidate;
        Ok(commit)
    }

    /// Begin a typed transaction at the current commit.
    ///
    /// The transaction pins its process-local snapshot until it is committed
    /// or dropped. Call [`RelationalStore::retain`] separately when a
    /// snapshot must outlive the transaction for historical reads.
    pub fn begin(&self) -> Result<RelationalTransaction> {
        let mut transaction = self.database.begin();
        let catalog = match transaction.get(&self.database, catalog_key())? {
            Some(bytes) => decode_catalog(&bytes)?,
            None if transaction.snapshot() == CommitId(0) => Catalog::default(),
            None => {
                return Err(DbError::Corruption {
                    artifact: "catalog",
                    reason: format!(
                        "durable catalog record is missing at commit {}",
                        transaction.snapshot().0
                    ),
                });
            }
        };
        Ok(RelationalTransaction {
            transaction,
            catalog,
        })
    }

    /// Run one typed transaction and commit it when the closure staged writes.
    ///
    /// A closure error drops the active transaction without publication. A
    /// read-only closure returns its snapshot without creating a no-op commit.
    /// Commit errors retain the normal recovery and serialization semantics.
    pub fn transaction<T, F>(&mut self, operation: F) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut RelationalTransaction) -> Result<T>,
    {
        let mut transaction = self.begin()?;
        let value = operation(self, &mut transaction)?;
        let commit = if transaction.is_read_only() {
            transaction.snapshot()
        } else {
            transaction.commit(self, &mut NoFaults)?
        };
        Ok((value, commit))
    }

    pub fn insert(
        &mut self,
        table: TableId,
        row: Row,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.commit_batch([RelationalMutation::Insert { table, row }], faults)
    }

    pub fn get(&self, table: TableId, snapshot: CommitId, primary: Key) -> Result<Option<Row>> {
        let catalog = self.catalog_at(snapshot)?;
        let definition = catalog.table(table)?;
        ensure_table_key(table, primary)?;
        let Some(bytes) = self
            .database
            .get_bytes(snapshot, row_storage_key(table, primary))?
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
        identity: &RowIdentity,
    ) -> Result<Option<Row>> {
        let catalog = self.catalog_at(snapshot)?;
        let definition = catalog.table(table)?;
        let encoded = row_identity_bytes_for_lookup(&catalog, table, identity)?;
        let Some(bytes) = self
            .database
            .get_bytes(snapshot, row_storage_key_identity(table, &encoded))?
        else {
            return Ok(None);
        };
        row_from_storage_identity(&catalog, definition, &encoded, &bytes).map(Some)
    }

    pub fn index_get(
        &self,
        table: TableId,
        snapshot: CommitId,
        index: IndexId,
        values: &[Value],
    ) -> Result<Vec<Row>> {
        let catalog = self.catalog_at(snapshot)?;
        let index_key = index_key_for(&catalog, table, index, values)?;
        let identities = self.database.index_get_bytes(snapshot, index, &index_key)?;
        if catalog.primary_key(table).is_some() {
            self.index_rows_identity(&catalog, table, snapshot, identities)
        } else {
            self.index_rows(
                table,
                snapshot,
                identities
                    .into_iter()
                    .map(|primary| decode_legacy_key(table, &primary))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
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
        index_definition_in(&catalog, table, index)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let start_key = match start {
            Some(values) => index_key_for(&catalog, table, index, values)?,
            None => Vec::new(),
        };
        let end_key = match end {
            Some(values) => index_key_for(&catalog, table, index, values)?,
            None => vec![u8::MAX],
        };
        if start_key > end_key {
            return Err(DbError::InvalidState(
                "secondary index scan start is after end".to_owned(),
            ));
        }
        let entries = self
            .database
            .index_scan_bytes(snapshot, index, &start_key, &end_key, limit)?;
        if catalog.primary_key(table).is_some() {
            self.index_rows_identity(
                &catalog,
                table,
                snapshot,
                entries.into_iter().map(|(_, identity)| identity).collect(),
            )
        } else {
            self.index_rows(
                table,
                snapshot,
                entries
                    .into_iter()
                    .map(|(_, primary)| decode_legacy_key(table, &primary))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
    }

    pub fn update(
        &mut self,
        table: TableId,
        row: Row,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.commit_batch([RelationalMutation::Update { table, row }], faults)
    }

    pub fn delete(
        &mut self,
        table: TableId,
        primary: Key,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.commit_batch([RelationalMutation::Delete { table, primary }], faults)
    }

    /// Delete a row by the catalog-owned identity encoded in its values.
    pub fn delete_row(
        &mut self,
        table: TableId,
        row: Row,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.commit_batch([RelationalMutation::DeleteRow { table, row }], faults)
    }

    pub fn commit_batch(
        &mut self,
        mutations: impl IntoIterator<Item = RelationalMutation>,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        let mut transaction = self.database.begin();
        transaction.get(&self.database, catalog_key())?;
        for mutation in mutations {
            self.stage_mutation(&self.catalog, &mut transaction, mutation)?;
        }
        if transaction.is_read_only() {
            return Ok(transaction.snapshot());
        }
        let catalog = self.catalog.clone();
        self.commit_transaction(transaction, &catalog, faults)
    }

    /// Commit a typed batch with a durable idempotency record.
    pub fn commit_batch_with_attempt(
        &mut self,
        mutations: impl IntoIterator<Item = RelationalMutation>,
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        let mut transaction = self.database.begin();
        transaction.get(&self.database, catalog_key())?;
        for mutation in mutations {
            self.stage_mutation(&self.catalog, &mut transaction, mutation)?;
        }
        if transaction.is_read_only() {
            return Ok(transaction.snapshot());
        }
        let catalog = self.catalog.clone();
        self.commit_transaction_with_attempt(transaction, &catalog, attempt, &mut NoFaults)
    }

    fn stage_mutation(
        &self,
        catalog: &Catalog,
        transaction: &mut Transaction,
        mutation: RelationalMutation,
    ) -> Result<()> {
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
        match mutation {
            RelationalMutation::Insert { table, row } => {
                let definition = catalog.table(table)?;
                row.validate(definition)?;
                let identity = row_identity_bytes(catalog, definition, &row)?;
                if transaction
                    .get_bytes(&self.database, row_storage_key_identity(table, &identity))?
                    .is_some()
                {
                    if let Some(primary_key) = catalog.primary_key(table)
                        && let Some(index) = catalog
                            .indexes_for(table)
                            .find(|index| index.unique && index.columns == primary_key)
                    {
                        return Err(DbError::UniqueViolation {
                            index: index.id.0,
                            key: row_index_key(definition, index, &row)?.unwrap_or_default(),
                        });
                    }
                    return Err(DbError::InvalidState("row already exists".to_owned()));
                }
                transaction.put_bytes(
                    row_storage_key_identity(table, &identity),
                    encode_row(&row)?,
                );
                for index in catalog.indexes_for(table) {
                    if let Some(index_key) = row_index_key(definition, index, &row)? {
                        transaction.index_put_bytes(index.id, index_key, identity.clone());
                    }
                }
            }
            RelationalMutation::Update { table, row } => {
                let definition = catalog.table(table)?;
                row.validate(definition)?;
                let identity = row_identity_bytes(catalog, definition, &row)?;
                let Some(previous_bytes) = transaction
                    .get_bytes(&self.database, row_storage_key_identity(table, &identity))?
                else {
                    return Err(DbError::InvalidState("row does not exist".to_owned()));
                };
                let previous =
                    decode_row(row.primary, &previous_bytes)?.materialize_for(definition)?;
                transaction.put_bytes(
                    row_storage_key_identity(table, &identity),
                    encode_row(&row)?,
                );
                let physical_primary = identity;
                for index in catalog.indexes_for(table) {
                    let old_key = row_index_key(definition, index, &previous)?;
                    let new_key = row_index_key(definition, index, &row)?;
                    if old_key == new_key {
                        continue;
                    }
                    if let Some(old_key) = old_key {
                        transaction.index_delete_bytes(index.id, old_key, physical_primary.clone());
                    }
                    if let Some(new_key) = new_key {
                        transaction.index_put_bytes(index.id, new_key, physical_primary.clone());
                    }
                }
            }
            RelationalMutation::Delete { table, primary } => {
                let previous = self.stage_delete_raw(catalog, transaction, table, primary)?;
                self.expand_referential_actions(catalog, transaction, table, &previous)?;
            }
            RelationalMutation::DeleteRow { table, row } => {
                let definition = catalog.table(table)?;
                row.validate(definition)?;
                let identity = row_identity_bytes(catalog, definition, &row)?;
                let Some(previous_bytes) = transaction
                    .get_bytes(&self.database, row_storage_key_identity(table, &identity))?
                else {
                    return Err(DbError::InvalidState("row does not exist".to_owned()));
                };
                let previous =
                    decode_row(row.primary, &previous_bytes)?.materialize_for(definition)?;
                transaction.delete_bytes(row_storage_key_identity(table, &identity));
                for index in catalog.indexes_for(table) {
                    if let Some(index_key) = row_index_key(definition, index, &previous)? {
                        transaction.index_delete_bytes(index.id, index_key, identity.clone());
                    }
                }
            }
        }
        Ok(())
    }

    /// Stage one delete with no referential-action expansion. Cascaded
    /// deletes call this directly so grandchildren are expanded exactly once
    /// by the caller's queue, not re-entrantly per staged mutation.
    fn stage_delete_raw(
        &self,
        catalog: &Catalog,
        transaction: &mut Transaction,
        table: TableId,
        primary: Key,
    ) -> Result<Row> {
        let definition = catalog.table(table)?;
        ensure_table_key(table, primary)?;
        let Some(previous_bytes) =
            transaction.get_bytes(&self.database, row_storage_key(table, primary))?
        else {
            return Err(DbError::InvalidState("row does not exist".to_owned()));
        };
        let previous = decode_row(primary, &previous_bytes)?.materialize_for(definition)?;
        transaction.delete_bytes(row_storage_key(table, primary));
        for index in catalog.indexes_for(table) {
            if let Some(index_key) = row_index_key(definition, index, &previous)? {
                transaction.index_delete_bytes(
                    index.id,
                    index_key,
                    encode_legacy_key(table, primary)?,
                );
            }
        }
        Ok(previous)
    }

    /// Expand `ON DELETE` actions for one deleted parent row.
    ///
    /// Cascaded child deletions are staged eagerly and become ordinary
    /// staged mutations: visible to later reads in the same transaction and
    /// covered by the publication-time referential pass. Children already
    /// deleted by an earlier step vanish from the scanned view, which is
    /// what terminates reference cycles; `MAX_CASCADE_DEPTH` bounds
    /// legitimately deep chains. Determinism: constraints fire in catalog
    /// (constraint-id) order, children in primary-key scan order.
    fn expand_referential_actions(
        &self,
        catalog: &Catalog,
        transaction: &mut Transaction,
        root_table: TableId,
        root_row: &Row,
    ) -> Result<()> {
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((root_table, root_row.clone(), 0usize));
        while let Some((parent_table, parent_row, depth)) = queue.pop_front() {
            for foreign_key in catalog.foreign_keys.values() {
                if foreign_key.referenced_table != parent_table {
                    continue;
                }
                if foreign_key.on_delete == ReferentialAction::Restrict {
                    // Enforced by the publication-time integrity pass.
                    continue;
                }
                if depth + 1 > MAX_CASCADE_DEPTH {
                    return Err(DbError::CascadeDepthExceeded {
                        constraint: foreign_key.id.0,
                        table: foreign_key.table.0,
                    });
                }
                let child_definition = catalog.table(foreign_key.table)?;
                let referenced_definition = catalog.table(parent_table)?;
                let required = foreign_key_values(
                    &parent_row,
                    referenced_definition,
                    &foreign_key.referenced_columns,
                )?;
                if required.iter().any(|value| matches!(value, Value::Null)) {
                    continue;
                }
                let (start, end) = table_range(foreign_key.table);
                let child_rows = decode_rows(
                    catalog,
                    child_definition,
                    transaction.scan_bytes(&self.database, start, end, usize::MAX)?,
                )?;
                for child in child_rows {
                    let values =
                        foreign_key_values(&child, child_definition, &foreign_key.columns)?;
                    if values.iter().any(|value| matches!(value, Value::Null)) {
                        continue;
                    }
                    if values != required {
                        continue;
                    }
                    match foreign_key.on_delete {
                        ReferentialAction::Restrict => {}
                        ReferentialAction::SetNull => {
                            let mut updated = child.clone();
                            for column in &foreign_key.columns {
                                updated.set_value(child_definition, *column, Value::Null)?;
                            }
                            self.stage_mutation(
                                catalog,
                                transaction,
                                RelationalMutation::Update {
                                    table: foreign_key.table,
                                    row: updated,
                                },
                            )?;
                        }
                        ReferentialAction::Cascade => {
                            self.stage_delete_raw(
                                catalog,
                                transaction,
                                foreign_key.table,
                                child.primary,
                            )?;
                            queue.push_back((foreign_key.table, child, depth + 1));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn commit_transaction(
        &mut self,
        mut transaction: Transaction,
        catalog: &Catalog,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.validate_foreign_keys(catalog, &mut transaction)?;
        self.database.commit_transaction(transaction, faults)
    }

    fn commit_transaction_with_attempt(
        &mut self,
        mut transaction: Transaction,
        catalog: &Catalog,
        attempt: TransactionAttemptId,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.validate_foreign_keys(catalog, &mut transaction)?;
        self.database
            .commit_transaction_with_attempt(transaction, attempt, faults)
    }

    pub(crate) fn commit_transaction_validated(
        &mut self,
        mut transaction: Transaction,
        catalog: &Catalog,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.validate_foreign_keys(catalog, &mut transaction)?;
        self.database
            .commit_transaction_validated(transaction, faults)
    }

    pub(crate) fn commit_transaction_validated_with_attempt(
        &mut self,
        mut transaction: Transaction,
        catalog: &Catalog,
        attempt: TransactionAttemptId,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.validate_foreign_keys(catalog, &mut transaction)?;
        self.database
            .commit_transaction_validated_with_attempt(transaction, attempt, faults)
    }

    fn validate_foreign_keys(
        &self,
        catalog: &Catalog,
        transaction: &mut Transaction,
    ) -> Result<()> {
        for foreign_key in catalog.foreign_keys.values() {
            let child_table = catalog.table(foreign_key.table)?;
            let referenced_table = catalog.table(foreign_key.referenced_table)?;
            let (child_start, child_end) = table_range(foreign_key.table);
            let child_rows = decode_rows(
                catalog,
                child_table,
                transaction.scan_bytes(&self.database, child_start, child_end, usize::MAX)?,
            )?;
            let (referenced_start, referenced_end) = table_range(foreign_key.referenced_table);
            let referenced_rows = decode_rows(
                catalog,
                referenced_table,
                transaction.scan_bytes(
                    &self.database,
                    referenced_start,
                    referenced_end,
                    usize::MAX,
                )?,
            )?;
            self.validate_foreign_key_rows(
                foreign_key,
                child_table,
                referenced_table,
                &child_rows,
                &referenced_rows,
            )?;
        }
        Ok(())
    }

    fn validate_foreign_key_rows(
        &self,
        foreign_key: &ForeignKeyDefinition,
        child_table: &TableDefinition,
        referenced_table: &TableDefinition,
        child_rows: &[Row],
        referenced_rows: &[Row],
    ) -> Result<()> {
        let referenced_values = referenced_rows
            .iter()
            .map(|row| foreign_key_values(row, referenced_table, &foreign_key.referenced_columns))
            .collect::<Result<Vec<_>>>()?;
        for row in child_rows {
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
        Ok(())
    }

    pub fn scan(&self, table: TableId, snapshot: CommitId, limit: usize) -> Result<Vec<Row>> {
        let catalog = self.catalog_at(snapshot)?;
        let definition = catalog.table(table)?;
        let (start, end) = table_range(table);
        self.database
            .scan_bytes(snapshot, start, end, limit)?
            .into_iter()
            .map(|(physical_primary, bytes)| {
                let identity = row_identity_from_storage_key(table, &physical_primary)?;
                row_from_storage_identity(&catalog, definition, identity, &bytes)
            })
            .collect()
    }

    fn index_rows(
        &self,
        table: TableId,
        snapshot: CommitId,
        primaries: Vec<Key>,
    ) -> Result<Vec<Row>> {
        primaries
            .into_iter()
            .map(|primary| {
                self.get(table, snapshot, primary)?
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "secondary index",
                        reason: format!(
                            "index entry references missing row for table {} and key {:?}",
                            table.0, primary
                        ),
                    })
            })
            .collect()
    }

    fn index_rows_identity(
        &self,
        catalog: &Catalog,
        table: TableId,
        snapshot: CommitId,
        identities: Vec<Vec<u8>>,
    ) -> Result<Vec<Row>> {
        let definition = catalog.table(table)?;
        identities
            .into_iter()
            .map(|identity| {
                self.database
                    .get_bytes(snapshot, row_storage_key_identity(table, &identity))?
                    .ok_or_else(|| DbError::Corruption {
                        artifact: "secondary index",
                        reason: "index entry references missing row identity".to_owned(),
                    })
                    .and_then(|bytes| {
                        row_from_storage_identity(catalog, definition, &identity, &bytes)
                    })
            })
            .collect()
    }

    fn index_rows_transaction(
        &self,
        catalog: &Catalog,
        transaction: &mut Transaction,
        table: TableId,
        primaries: Vec<Key>,
    ) -> Result<Vec<Row>> {
        let definition = catalog.table(table)?;
        primaries
            .into_iter()
            .map(|primary| {
                ensure_table_key(table, primary)?;
                let Some(bytes) =
                    transaction.get_bytes(&self.database, row_storage_key(table, primary))?
                else {
                    return Err(DbError::Corruption {
                        artifact: "secondary index",
                        reason: format!(
                            "index entry references missing row for table {} and key {:?}",
                            table.0, primary
                        ),
                    });
                };
                let row = decode_row(primary, &bytes)?;
                row.materialize_for(definition)
            })
            .collect()
    }

    fn index_rows_transaction_identity(
        &self,
        catalog: &Catalog,
        transaction: &mut Transaction,
        table: TableId,
        identities: Vec<Vec<u8>>,
    ) -> Result<Vec<Row>> {
        let definition = catalog.table(table)?;
        identities
            .into_iter()
            .map(|identity| {
                let Some(bytes) = transaction
                    .get_bytes(&self.database, row_storage_key_identity(table, &identity))?
                else {
                    return Err(DbError::Corruption {
                        artifact: "secondary index",
                        reason: "index entry references missing row identity".to_owned(),
                    });
                };
                row_from_storage_identity(catalog, definition, &identity, &bytes)
            })
            .collect()
    }

    pub fn filter<'a, F>(rows: impl IntoIterator<Item = &'a Row>, predicate: F) -> Vec<Row>
    where
        F: Fn(&Row) -> bool,
    {
        rows.into_iter()
            .filter(|row| predicate(row))
            .cloned()
            .collect()
    }

    pub fn project(rows: &[Row], columns: &[usize]) -> Result<Vec<Vec<Value>>> {
        rows.iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|column| {
                        row.values.get(*column).cloned().ok_or_else(|| {
                            DbError::InvalidState(format!(
                                "projection column {} is out of bounds",
                                column
                            ))
                        })
                    })
                    .collect()
            })
            .collect()
    }

    pub fn nested_loop_join(
        left: &[Row],
        right: &[Row],
        left_column: usize,
        right_column: usize,
    ) -> Result<Vec<(Row, Row)>> {
        let mut joined = Vec::new();
        for left_row in left {
            let left_value = left_row.values.get(left_column).ok_or_else(|| {
                DbError::InvalidState(format!("join column {} is out of bounds", left_column))
            })?;
            for right_row in right {
                let right_value = right_row.values.get(right_column).ok_or_else(|| {
                    DbError::InvalidState(format!("join column {} is out of bounds", right_column))
                })?;
                if !matches!(left_value, Value::Null)
                    && !matches!(right_value, Value::Null)
                    && left_value == right_value
                {
                    joined.push((left_row.clone(), right_row.clone()));
                }
            }
        }
        Ok(joined)
    }

    pub fn hash_join(
        left: &[Row],
        right: &[Row],
        left_column: usize,
        right_column: usize,
    ) -> Result<Vec<(Row, Row)>> {
        let mut hash: HashMap<Value, Vec<Row>> = HashMap::new();
        for right_row in right {
            let value = right_row.values.get(right_column).ok_or_else(|| {
                DbError::InvalidState(format!("join column {} is out of bounds", right_column))
            })?;
            if !matches!(value, Value::Null) {
                hash.entry(value.clone())
                    .or_default()
                    .push(right_row.clone());
            }
        }
        let mut joined = Vec::new();
        for left_row in left {
            let value = left_row.values.get(left_column).ok_or_else(|| {
                DbError::InvalidState(format!("join column {} is out of bounds", left_column))
            })?;
            if let Some(matches) = hash.get(value) {
                joined.extend(
                    matches
                        .iter()
                        .cloned()
                        .map(|right_row| (left_row.clone(), right_row)),
                );
            }
        }
        Ok(joined)
    }
}

fn index_definition_in(
    catalog: &Catalog,
    table: TableId,
    index: IndexId,
) -> Result<&IndexDefinition> {
    let definition = catalog.indexes.get(&index).ok_or_else(|| {
        DbError::InvalidState(format!("secondary index {} does not exist", index.0))
    })?;
    if definition.table != table {
        return Err(DbError::InvalidState(format!(
            "secondary index {} belongs to table {}, not {}",
            index.0, definition.table.0, table.0
        )));
    }
    let table_definition = catalog.table(table)?;
    for column in &definition.columns {
        table_definition.column(*column)?;
    }
    Ok(definition)
}

fn index_key_for(
    catalog: &Catalog,
    table: TableId,
    index: IndexId,
    values: &[Value],
) -> Result<Vec<u8>> {
    let definition = index_definition_in(catalog, table, index)?;
    let table_definition = catalog.table(table)?;
    index_values_key(table_definition, definition, values)
}

impl RelationalTransaction {
    #[must_use]
    pub fn snapshot(&self) -> CommitId {
        self.transaction.snapshot()
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.transaction.is_read_only()
    }

    pub fn get(
        &mut self,
        store: &RelationalStore,
        table: TableId,
        primary: Key,
    ) -> Result<Option<Row>> {
        let definition = self.catalog.table(table)?;
        ensure_table_key(table, primary)?;
        let Some(bytes) = self
            .transaction
            .get_bytes(&store.database, row_storage_key(table, primary))?
        else {
            return Ok(None);
        };
        let row = decode_row(primary, &bytes)?;
        Ok(Some(row.materialize_for(definition)?))
    }

    /// Look up a row through the catalog-owned composite primary-key identity,
    /// including staged inserts, updates, and identity-based deletes.
    pub fn get_by_identity(
        &mut self,
        store: &RelationalStore,
        table: TableId,
        identity: &RowIdentity,
    ) -> Result<Option<Row>> {
        let definition = self.catalog.table(table)?;
        let encoded = row_identity_bytes_for_lookup(&self.catalog, table, identity)?;
        let row = self
            .transaction
            .get_bytes(&store.database, row_storage_key_identity(table, &encoded))?
            .map(|bytes| row_from_storage_identity(&self.catalog, definition, &encoded, &bytes))
            .transpose()?;
        Ok(row)
    }

    pub fn scan(
        &mut self,
        store: &RelationalStore,
        table: TableId,
        limit: usize,
    ) -> Result<Vec<Row>> {
        let definition = self.catalog.table(table)?;
        let (start, end) = table_range(table);
        self.transaction
            .scan_bytes(&store.database, start, end, limit)?
            .into_iter()
            .map(|(physical_primary, bytes)| {
                let identity = row_identity_from_storage_key(table, &physical_primary)?;
                row_from_storage_identity(&self.catalog, definition, identity, &bytes)
            })
            .collect()
    }

    pub fn index_get(
        &mut self,
        store: &RelationalStore,
        table: TableId,
        index: IndexId,
        values: &[Value],
    ) -> Result<Vec<Row>> {
        let index_key = index_key_for(&self.catalog, table, index, values)?;
        let identities = self
            .transaction
            .index_get_bytes(&store.database, index, index_key)?;
        if self.catalog.primary_key(table).is_some() {
            store.index_rows_transaction_identity(
                &self.catalog,
                &mut self.transaction,
                table,
                identities,
            )
        } else {
            store.index_rows_transaction(
                &self.catalog,
                &mut self.transaction,
                table,
                identities
                    .into_iter()
                    .map(|primary| decode_legacy_key(table, &primary))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
    }

    pub fn index_scan(
        &mut self,
        store: &RelationalStore,
        table: TableId,
        index: IndexId,
        start: Option<&[Value]>,
        end: Option<&[Value]>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        index_definition_in(&self.catalog, table, index)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let start_key = match start {
            Some(values) => index_key_for(&self.catalog, table, index, values)?,
            None => Vec::new(),
        };
        let end_key = match end {
            Some(values) => index_key_for(&self.catalog, table, index, values)?,
            None => vec![u8::MAX],
        };
        if start_key > end_key {
            return Err(DbError::InvalidState(
                "secondary index scan start is after end".to_owned(),
            ));
        }
        let entries =
            self.transaction
                .index_scan_bytes(&store.database, index, start_key, end_key, limit)?;
        if self.catalog.primary_key(table).is_some() {
            store.index_rows_transaction_identity(
                &self.catalog,
                &mut self.transaction,
                table,
                entries.into_iter().map(|(_, identity)| identity).collect(),
            )
        } else {
            store.index_rows_transaction(
                &self.catalog,
                &mut self.transaction,
                table,
                entries
                    .into_iter()
                    .map(|(_, primary)| decode_legacy_key(table, &primary))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
    }

    pub fn insert(&mut self, store: &RelationalStore, table: TableId, row: Row) -> Result<()> {
        store.stage_mutation(
            &self.catalog,
            &mut self.transaction,
            RelationalMutation::Insert { table, row },
        )
    }

    pub fn update(&mut self, store: &RelationalStore, table: TableId, row: Row) -> Result<()> {
        store.stage_mutation(
            &self.catalog,
            &mut self.transaction,
            RelationalMutation::Update { table, row },
        )
    }

    pub fn delete(&mut self, store: &RelationalStore, table: TableId, primary: Key) -> Result<()> {
        store.stage_mutation(
            &self.catalog,
            &mut self.transaction,
            RelationalMutation::Delete { table, primary },
        )
    }

    pub fn delete_row(&mut self, store: &RelationalStore, table: TableId, row: Row) -> Result<()> {
        store.stage_mutation(
            &self.catalog,
            &mut self.transaction,
            RelationalMutation::DeleteRow { table, row },
        )
    }

    pub fn commit(
        self,
        store: &mut RelationalStore,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        if self.is_read_only() {
            return Ok(self.snapshot());
        }
        let Self {
            transaction,
            catalog,
        } = self;
        store.commit_transaction(transaction, &catalog, faults)
    }

    pub fn commit_with_attempt(
        self,
        store: &mut RelationalStore,
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        if self.is_read_only() {
            return Ok(self.snapshot());
        }
        let Self {
            transaction,
            catalog,
        } = self;
        store.commit_transaction_with_attempt(transaction, &catalog, attempt, &mut NoFaults)
    }

    pub fn commit_validated(
        self,
        store: &mut RelationalStore,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        if self.is_read_only() {
            return Ok(self.snapshot());
        }
        let Self {
            transaction,
            catalog,
        } = self;
        store.commit_transaction_validated(transaction, &catalog, faults)
    }

    pub fn commit_validated_with_attempt(
        self,
        store: &mut RelationalStore,
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        if self.is_read_only() {
            return Ok(self.snapshot());
        }
        let Self {
            transaction,
            catalog,
        } = self;
        store.commit_transaction_validated_with_attempt(
            transaction,
            &catalog,
            attempt,
            &mut NoFaults,
        )
    }
}

fn decode_rows(
    catalog: &Catalog,
    table: &TableDefinition,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<Vec<Row>> {
    entries
        .into_iter()
        .map(|(storage_key, bytes)| {
            let identity = row_identity_from_storage_key(table.id, &storage_key)?;
            row_from_storage_identity(catalog, table, identity, &bytes)
        })
        .collect()
}

pub(crate) fn build_snapshot_capture(
    commit: CommitId,
    catalog: Catalog,
    tables: Vec<RelationalSnapshotTable>,
) -> Result<RelationalSnapshot> {
    let catalog_bytes = encode_catalog(&catalog)?;
    let catalog_digest = Sha256::digest(&catalog_bytes).into();
    let mut logical = Sha256::new();
    logical.update(b"OMENDB/relational-snapshot/v1");
    logical.update((catalog_bytes.len() as u64).to_le_bytes());
    logical.update(&catalog_bytes);
    for table in &tables {
        logical.update(table.table.0.to_le_bytes());
        logical.update((table.rows.len() as u64).to_le_bytes());
        for row in &table.rows {
            let encoded = encode_row(row)?;
            let identity = row_identity_bytes(&catalog, catalog.table(table.table)?, row)?;
            logical.update((identity.len() as u64).to_le_bytes());
            logical.update(identity);
            logical.update((encoded.len() as u64).to_le_bytes());
            logical.update(encoded);
        }
    }
    Ok(RelationalSnapshot {
        commit,
        catalog,
        tables,
        catalog_digest,
        logical_digest: logical.finalize().into(),
    })
}

fn ensure_table_key(table: TableId, key: Key) -> Result<()> {
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
    row_storage_key_identity(
        table,
        &encode_legacy_key(table, primary).expect("legacy primary identity is encodable"),
    )
}

fn row_storage_key_identity(table: TableId, identity: &[u8]) -> Vec<u8> {
    let mut key = row_prefix(table);
    key.extend_from_slice(identity);
    key
}

pub(crate) fn row_identity_bytes(
    catalog: &Catalog,
    table: &TableDefinition,
    row: &Row,
) -> Result<Vec<u8>> {
    let Some(columns) = catalog.primary_key(table.id) else {
        ensure_table_key(table.id, row.primary)?;
        return encode_legacy_key(table.id, row.primary);
    };
    let values = columns
        .iter()
        .map(|column| row.value(table, *column).cloned())
        .collect::<Result<Vec<_>>>()?;
    RowIdentity::new(table.id, columns.to_vec(), values)?.encode()
}

pub(crate) fn row_identity_bytes_for_lookup(
    catalog: &Catalog,
    table: TableId,
    identity: &RowIdentity,
) -> Result<Vec<u8>> {
    let definition = catalog.table(table)?;
    let columns = catalog.primary_key(table).ok_or_else(|| {
        DbError::InvalidState(format!(
            "row identity lookup requires a catalog-owned primary key for table {}",
            table.0
        ))
    })?;
    if identity.table() != table {
        return Err(DbError::InvalidState(format!(
            "row identity belongs to table {}, not {}",
            identity.table().0,
            table.0
        )));
    }
    if identity.columns() != columns {
        return Err(DbError::InvalidState(format!(
            "row identity columns do not match the primary key for table {}",
            table.0
        )));
    }
    for (column, value) in columns.iter().zip(identity.values()) {
        let definition = definition.column(*column)?;
        if !value.matches(definition.data_type) {
            return Err(DbError::InvalidState(format!(
                "row identity value has the wrong type for column {}",
                definition.name
            )));
        }
    }
    identity.encode()
}

pub(crate) fn row_from_storage_identity(
    catalog: &Catalog,
    table: &TableDefinition,
    identity: &[u8],
    bytes: &[u8],
) -> Result<Row> {
    let identity_value = RowIdentity::decode(identity)?;
    if identity_value.table() != table.id {
        return Err(DbError::Corruption {
            artifact: "row identity",
            reason: "row identity table does not match its namespace".to_owned(),
        });
    }
    let primary = match catalog.primary_key(table.id) {
        Some(columns) if identity_value.columns() == columns => {
            synthetic_primary(table.id, identity)
        }
        Some(_) => {
            return Err(DbError::Corruption {
                artifact: "row identity",
                reason: "row identity columns do not match the catalog primary key".to_owned(),
            });
        }
        None => decode_legacy_key(table.id, identity)?,
    };
    let row = decode_row(primary, bytes)?.materialize_for(table)?;
    if row_identity_bytes(catalog, table, &row)? != identity {
        return Err(DbError::Corruption {
            artifact: "row identity",
            reason: "row identity does not match row values".to_owned(),
        });
    }
    Ok(row)
}

fn synthetic_primary(table: TableId, identity: &[u8]) -> Key {
    let digest = Sha256::digest(identity);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&table.0.to_be_bytes());
    bytes[8..].copy_from_slice(&digest[..8]);
    Key(bytes)
}

fn row_identity_from_storage_key(table: TableId, storage_key: &[u8]) -> Result<&[u8]> {
    let prefix = row_prefix(table);
    storage_key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| DbError::Corruption {
            artifact: "row identity",
            reason: "row key is outside its table namespace".to_owned(),
        })
}

fn table_range(table: TableId) -> (Vec<u8>, Vec<u8>) {
    let prefix = row_prefix(table);
    (prefix.clone(), prefix_end(&prefix))
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

pub(crate) fn row_index_key(
    table: &TableDefinition,
    index: &IndexDefinition,
    row: &Row,
) -> Result<Option<Vec<u8>>> {
    let mut values = Vec::with_capacity(index.columns.len());
    for column in &index.columns {
        let value = row.value(table, *column)?;
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        values.push(value);
    }
    let mut bytes = Vec::new();
    for value in values {
        value.encode(&mut bytes)?;
    }
    Ok(Some(bytes))
}

pub(crate) fn foreign_key_values(
    row: &Row,
    table: &TableDefinition,
    columns: &[ColumnId],
) -> Result<Vec<Value>> {
    columns
        .iter()
        .map(|column| row.value(table, *column).cloned())
        .collect()
}

pub(crate) fn index_values_key(
    table: &TableDefinition,
    index: &IndexDefinition,
    values: &[Value],
) -> Result<Vec<u8>> {
    if values.len() != index.columns.len() {
        return Err(DbError::InvalidState(format!(
            "index {} requires {} values, got {}",
            index.id.0,
            index.columns.len(),
            values.len()
        )));
    }
    let mut bytes = Vec::new();
    for (value, column_id) in values.iter().zip(&index.columns) {
        let column = table.column(*column_id)?;
        if !value.matches(column.data_type) {
            return Err(DbError::InvalidState(format!(
                "index {} value has the wrong type for column {}",
                index.id.0, column.name
            )));
        }
        value.encode(&mut bytes)?;
    }
    Ok(bytes)
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| DbError::ValueTooLarge(value.len()))?;
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

fn read_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let end = cursor.checked_add(4).ok_or_else(|| DbError::Corruption {
        artifact: "row",
        reason: "value length overflow".to_owned(),
    })?;
    let length = u32::from_le_bytes(
        bytes
            .get(*cursor..end)
            .ok_or_else(|| DbError::Corruption {
                artifact: "row",
                reason: "missing value length".to_owned(),
            })?
            .try_into()
            .expect("row length width"),
    ) as usize;
    *cursor = end;
    let value_end = cursor
        .checked_add(length)
        .ok_or_else(|| DbError::Corruption {
            artifact: "row",
            reason: "value length overflow".to_owned(),
        })?;
    let value = bytes
        .get(*cursor..value_end)
        .ok_or_else(|| DbError::Corruption {
            artifact: "row",
            reason: "truncated value".to_owned(),
        })?
        .to_vec();
    *cursor = value_end;
    Ok(value)
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let end = cursor.checked_add(8).ok_or_else(|| DbError::Corruption {
        artifact: "row",
        reason: "integer length overflow".to_owned(),
    })?;
    let value = i64::from_le_bytes(
        bytes
            .get(*cursor..end)
            .ok_or_else(|| DbError::Corruption {
                artifact: "row",
                reason: "truncated integer".to_owned(),
            })?
            .try_into()
            .expect("i64 width"),
    );
    *cursor = end;
    Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let end = cursor.checked_add(8).ok_or_else(|| DbError::Corruption {
        artifact: "row",
        reason: "integer length overflow".to_owned(),
    })?;
    let value = u64::from_le_bytes(
        bytes
            .get(*cursor..end)
            .ok_or_else(|| DbError::Corruption {
                artifact: "row",
                reason: "truncated integer".to_owned(),
            })?
            .try_into()
            .expect("u64 width"),
    );
    *cursor = end;
    Ok(value)
}

#[derive(Clone, Debug)]
struct DurableSchemaJob {
    change: SchemaChange,
    state: SchemaJobState,
}

fn encode_schema_change(change: &SchemaChange) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    match change {
        SchemaChange::CreateTable(table) => {
            bytes.push(1);
            bytes.extend_from_slice(&table.id.0.to_le_bytes());
            put_string(&mut bytes, &table.name)?;
            put_u32(&mut bytes, table.columns.len())?;
            for column in &table.columns {
                bytes.extend_from_slice(&column.id.0.to_le_bytes());
                bytes.push(column_type_tag(column.data_type));
                bytes.push(u8::from(column.nullable));
                put_string(&mut bytes, &column.name)?;
            }
        }
        SchemaChange::CreateIndex(index) => {
            bytes.push(2);
            bytes.extend_from_slice(&index.id.0.to_le_bytes());
            bytes.extend_from_slice(&index.table.0.to_le_bytes());
            put_u32(&mut bytes, index.columns.len())?;
            for column in &index.columns {
                bytes.extend_from_slice(&column.0.to_le_bytes());
            }
            bytes.push(u8::from(index.unique));
        }
    }
    if bytes.len() > SCHEMA_MAX_PAYLOAD {
        return Err(DbError::InvalidState(
            "schema journal payload exceeds maximum".to_owned(),
        ));
    }
    Ok(bytes)
}

fn encode_string(value: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_string(&mut bytes, value)?;
    Ok(bytes)
}

fn decode_schema_change(bytes: &[u8]) -> Result<SchemaChange> {
    let mut cursor = SchemaCursor::new(bytes);
    let change = match cursor.byte()? {
        1 => {
            let id = TableId(cursor.u64()?);
            let name = cursor.string()?;
            let column_count = cursor.u32()? as usize;
            if column_count == 0 || column_count > 4096 {
                return Err(cursor.corrupt("invalid schema column count"));
            }
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                columns.push(ColumnDefinition {
                    id: ColumnId(cursor.u16()?),
                    data_type: column_type_from_tag(cursor.byte()?)
                        .map_err(|_| cursor.corrupt("unknown schema column type"))?,
                    nullable: cursor.byte()? != 0,
                    name: cursor.string()?,
                });
            }
            SchemaChange::CreateTable(TableDefinition { id, name, columns })
        }
        2 => {
            let id = IndexId(cursor.u64()?);
            let table = TableId(cursor.u64()?);
            let column_count = cursor.u32()? as usize;
            if column_count == 0 || column_count > 4096 {
                return Err(cursor.corrupt("invalid schema index column count"));
            }
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                columns.push(ColumnId(cursor.u16()?));
            }
            SchemaChange::CreateIndex(IndexDefinition {
                id,
                table,
                columns,
                unique: cursor.byte()? != 0,
            })
        }
        _ => return Err(cursor.corrupt("unknown schema change tag")),
    };
    cursor.finish()?;
    Ok(change)
}

fn append_schema_record(
    path: &Path,
    id: SchemaJobId,
    record_type: u8,
    payload: &[u8],
    faults: &mut dyn FaultInjector,
) -> Result<()> {
    if payload.len() > SCHEMA_MAX_PAYLOAD {
        return Err(DbError::InvalidState(
            "schema journal payload exceeds maximum".to_owned(),
        ));
    }
    fs::create_dir_all(path.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(|source| io_error("create schema journal directory", source))?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| io_error("open schema journal", source))?;
    write_schema_record(&mut file, id, record_type, payload, "append schema journal")?;
    file.sync_all()
        .map_err(|source| io_error("sync schema journal", source))?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .map_err(|source| io_error("open schema journal parent", source))?
            .sync_all()
            .map_err(|source| io_error("sync schema journal parent", source))?;
    }
    faults.check(FaultPoint::SchemaJournalSync)
}

fn write_schema_record(
    file: &mut File,
    id: SchemaJobId,
    record_type: u8,
    payload: &[u8],
    operation: &'static str,
) -> Result<()> {
    let frame = schema_frame(id, record_type, payload)?;
    file.write_all(&frame)
        .map_err(|source| io_error(operation, source))
}

fn schema_frame(id: SchemaJobId, record_type: u8, payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > SCHEMA_MAX_PAYLOAD {
        return Err(DbError::InvalidState(
            "schema journal payload exceeds maximum".to_owned(),
        ));
    }
    let mut frame = Vec::with_capacity(SCHEMA_HEADER_BYTES + payload.len());
    frame.extend_from_slice(&SCHEMA_MAGIC);
    frame.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
    frame.push(record_type);
    frame.push(0);
    frame.extend_from_slice(&id.0.to_le_bytes());
    frame.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| DbError::InvalidState("schema journal payload too large".to_owned()))?
            .to_le_bytes(),
    );
    frame.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn rewrite_schema_journal(
    path: &Path,
    jobs: &BTreeMap<SchemaJobId, SchemaJob>,
    faults: &mut dyn FaultInjector,
) -> Result<()> {
    fs::create_dir_all(path.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(|source| io_error("create schema journal directory", source))?;
    let temporary = path.with_extension("schema.tmp");
    let mut file = File::create(&temporary)
        .map_err(|source| io_error("create schema journal rewrite", source))?;
    for (id, job) in jobs {
        let payload = encode_schema_change(&job.change)?;
        write_schema_record(
            &mut file,
            *id,
            SCHEMA_SUBMIT,
            &payload,
            "write schema journal rewrite",
        )?;
        match &job.state {
            SchemaJobState::Pending => {}
            SchemaJobState::Running => write_schema_record(
                &mut file,
                *id,
                SCHEMA_RUNNING,
                &[],
                "write schema journal rewrite",
            )?,
            SchemaJobState::Completed => {
                write_schema_record(
                    &mut file,
                    *id,
                    SCHEMA_RUNNING,
                    &[],
                    "write schema journal rewrite",
                )?;
                write_schema_record(
                    &mut file,
                    *id,
                    SCHEMA_COMPLETED,
                    &[],
                    "write schema journal rewrite",
                )?;
            }
            SchemaJobState::Failed(message) => {
                write_schema_record(
                    &mut file,
                    *id,
                    SCHEMA_RUNNING,
                    &[],
                    "write schema journal rewrite",
                )?;
                let payload = encode_string(message)?;
                write_schema_record(
                    &mut file,
                    *id,
                    SCHEMA_FAILED,
                    &payload,
                    "write schema journal rewrite",
                )?;
            }
        }
    }
    file.sync_all()
        .map_err(|source| io_error("sync schema journal rewrite", source))?;
    fs::rename(&temporary, path).map_err(|source| io_error("publish schema journal", source))?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .map_err(|source| io_error("open schema journal parent", source))?
            .sync_all()
            .map_err(|source| io_error("sync schema journal parent", source))?;
    }
    faults.check(FaultPoint::SchemaJournalSync)
}

fn read_schema_journal(path: &Path) -> Result<BTreeMap<SchemaJobId, DurableSchemaJob>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(source) => return Err(io_error("open schema journal", source)),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| io_error("read schema journal", source))?;
    let mut jobs = BTreeMap::new();
    let mut offset = 0;
    while offset < bytes.len() {
        if bytes.len() - offset < SCHEMA_HEADER_BYTES {
            break;
        }
        let header = &bytes[offset..offset + SCHEMA_HEADER_BYTES];
        if header[..4] != SCHEMA_MAGIC
            || u16::from_le_bytes(header[4..6].try_into().expect("schema version width"))
                != SCHEMA_VERSION
            || header[7] != 0
        {
            return Err(schema_corruption("invalid schema journal header"));
        }
        let record_type = header[6];
        let id = SchemaJobId(u64::from_le_bytes(
            header[8..16].try_into().expect("schema job ID width"),
        ));
        let length = u32::from_le_bytes(
            header[16..20]
                .try_into()
                .expect("schema payload length width"),
        ) as usize;
        if length > SCHEMA_MAX_PAYLOAD {
            return Err(schema_corruption("schema journal payload exceeds maximum"));
        }
        let end = offset
            .checked_add(SCHEMA_HEADER_BYTES)
            .and_then(|value| value.checked_add(length))
            .ok_or_else(|| schema_corruption("schema journal frame length overflows"))?;
        if end > bytes.len() {
            break;
        }
        let payload = &bytes[offset + SCHEMA_HEADER_BYTES..end];
        let checksum =
            u32::from_le_bytes(header[20..24].try_into().expect("schema checksum width"));
        if crc32c::crc32c(payload) != checksum {
            return Err(schema_corruption("schema journal checksum mismatch"));
        }
        apply_schema_record(&mut jobs, record_type, id, payload)?;
        offset = end;
    }
    Ok(jobs)
}

fn apply_schema_record(
    jobs: &mut BTreeMap<SchemaJobId, DurableSchemaJob>,
    record_type: u8,
    id: SchemaJobId,
    payload: &[u8],
) -> Result<()> {
    match record_type {
        SCHEMA_SUBMIT => {
            if jobs.contains_key(&id) {
                return Err(schema_corruption("duplicate schema job submission"));
            }
            if jobs.len() >= MAX_SCHEMA_JOBS {
                return Err(schema_corruption("schema journal exceeds job bound"));
            }
            jobs.insert(
                id,
                DurableSchemaJob {
                    change: decode_schema_change(payload)?,
                    state: SchemaJobState::Pending,
                },
            );
        }
        SCHEMA_RUNNING => {
            let job = jobs
                .get_mut(&id)
                .ok_or_else(|| schema_corruption("schema job state has no submission"))?;
            if !matches!(job.state, SchemaJobState::Pending | SchemaJobState::Running) {
                return Err(schema_corruption(
                    "schema job entered running from terminal state",
                ));
            }
            if !payload.is_empty() {
                return Err(schema_corruption("running schema record has a payload"));
            }
            job.state = SchemaJobState::Running;
        }
        SCHEMA_COMPLETED => {
            let job = jobs
                .get_mut(&id)
                .ok_or_else(|| schema_corruption("schema job state has no submission"))?;
            if !matches!(
                job.state,
                SchemaJobState::Running | SchemaJobState::Completed
            ) {
                return Err(schema_corruption("schema job completed before running"));
            }
            if !payload.is_empty() {
                return Err(schema_corruption("completed schema record has a payload"));
            }
            job.state = SchemaJobState::Completed;
        }
        SCHEMA_FAILED => {
            let job = jobs
                .get_mut(&id)
                .ok_or_else(|| schema_corruption("schema job state has no submission"))?;
            if !matches!(
                job.state,
                SchemaJobState::Running | SchemaJobState::Failed(_)
            ) {
                return Err(schema_corruption("schema job failed before running"));
            }
            let mut cursor = SchemaCursor::new(payload);
            let message = cursor.string()?;
            cursor.finish()?;
            job.state = SchemaJobState::Failed(message);
        }
        _ => return Err(schema_corruption("unknown schema journal record type")),
    }
    Ok(())
}

fn schema_corruption(reason: &str) -> DbError {
    DbError::Corruption {
        artifact: "schema journal",
        reason: reason.to_owned(),
    }
}

fn schema_journal_path(directory: &Path) -> PathBuf {
    directory.join(SCHEMA_NAME)
}

struct SchemaCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SchemaCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn byte(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| self.corrupt("missing schema byte"))?;
        self.offset += 1;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16> {
        let end = self
            .offset
            .checked_add(2)
            .ok_or_else(|| self.corrupt("schema u16 overflow"))?;
        let value = u16::from_le_bytes(
            self.bytes
                .get(self.offset..end)
                .ok_or_else(|| self.corrupt("missing schema u16"))?
                .try_into()
                .expect("schema u16 width"),
        );
        self.offset = end;
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64> {
        let end = self
            .offset
            .checked_add(8)
            .ok_or_else(|| self.corrupt("schema u64 overflow"))?;
        let value = u64::from_le_bytes(
            self.bytes
                .get(self.offset..end)
                .ok_or_else(|| self.corrupt("missing schema u64"))?
                .try_into()
                .expect("schema u64 width"),
        );
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32> {
        let end = self
            .offset
            .checked_add(4)
            .ok_or_else(|| self.corrupt("schema u32 overflow"))?;
        let value = u32::from_le_bytes(
            self.bytes
                .get(self.offset..end)
                .ok_or_else(|| self.corrupt("missing schema u32"))?
                .try_into()
                .expect("schema u32 width"),
        );
        self.offset = end;
        Ok(value)
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes_value()?).map_err(|_| self.corrupt("invalid schema UTF-8"))
    }

    fn bytes_value(&mut self) -> Result<Vec<u8>> {
        let length_end = self
            .offset
            .checked_add(4)
            .ok_or_else(|| self.corrupt("schema length overflows"))?;
        let length = u32::from_le_bytes(
            self.bytes
                .get(self.offset..length_end)
                .ok_or_else(|| self.corrupt("missing schema value length"))?
                .try_into()
                .expect("schema length width"),
        ) as usize;
        self.offset = length_end;
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| self.corrupt("schema value length overflows"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| self.corrupt("truncated schema value"))?
            .to_vec();
        self.offset = end;
        Ok(value)
    }

    fn finish(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(self.corrupt("trailing schema bytes"))
        }
    }

    fn corrupt(&self, reason: &str) -> DbError {
        schema_corruption(reason)
    }
}

pub(crate) fn encode_catalog(catalog: &Catalog) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&CATALOG_MAGIC);
    bytes.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    bytes.extend_from_slice(&catalog.generation.to_le_bytes());
    put_u32(&mut bytes, catalog.tables.len())?;
    for table in catalog.tables.values() {
        bytes.extend_from_slice(&table.id.0.to_le_bytes());
        put_string(&mut bytes, &table.name)?;
        put_u32(&mut bytes, table.columns.len())?;
        for column in &table.columns {
            bytes.extend_from_slice(&column.id.0.to_le_bytes());
            bytes.push(column_type_tag(column.data_type));
            bytes.push(u8::from(column.nullable));
            put_string(&mut bytes, &column.name)?;
        }
        let primary_key = catalog.primary_keys.get(&table.id);
        put_u32(&mut bytes, primary_key.map_or(0, Vec::len))?;
        if let Some(primary_key) = primary_key {
            for column in primary_key {
                bytes.extend_from_slice(&column.0.to_le_bytes());
            }
        }
    }
    put_u32(&mut bytes, catalog.indexes.len())?;
    for index in catalog.indexes.values() {
        bytes.extend_from_slice(&index.id.0.to_le_bytes());
        bytes.extend_from_slice(&index.table.0.to_le_bytes());
        put_u32(&mut bytes, index.columns.len())?;
        for column in &index.columns {
            bytes.extend_from_slice(&column.0.to_le_bytes());
        }
        bytes.push(u8::from(index.unique));
        match catalog.index_names.get(&index.id) {
            Some(name) => {
                bytes.push(1);
                put_string(&mut bytes, name)?;
            }
            None => bytes.push(0),
        }
    }
    put_u32(&mut bytes, catalog.foreign_keys.len())?;
    for foreign_key in catalog.foreign_keys.values() {
        bytes.extend_from_slice(&foreign_key.id.0.to_le_bytes());
        bytes.extend_from_slice(&foreign_key.table.0.to_le_bytes());
        put_u32(&mut bytes, foreign_key.columns.len())?;
        for column in &foreign_key.columns {
            bytes.extend_from_slice(&column.0.to_le_bytes());
        }
        bytes.extend_from_slice(&foreign_key.referenced_table.0.to_le_bytes());
        put_u32(&mut bytes, foreign_key.referenced_columns.len())?;
        for column in &foreign_key.referenced_columns {
            bytes.extend_from_slice(&column.0.to_le_bytes());
        }
        bytes.push(foreign_key.on_delete as u8);
        bytes.push(foreign_key.timing as u8);
        match catalog.foreign_key_names.get(&foreign_key.id) {
            Some(name) => {
                bytes.push(1);
                put_string(&mut bytes, name)?;
            }
            None => bytes.push(0),
        }
    }
    Ok(bytes)
}

pub(crate) fn decode_catalog(bytes: &[u8]) -> Result<Catalog> {
    let mut cursor = CatalogCursor::new(bytes);
    let magic = [
        cursor.byte()?,
        cursor.byte()?,
        cursor.byte()?,
        cursor.byte()?,
    ];
    if magic != CATALOG_MAGIC {
        return Err(cursor.corrupt("unknown catalog format"));
    }
    let version = cursor.u16()?;
    if version != CATALOG_VERSION {
        return Err(cursor.corrupt("unsupported catalog version"));
    }
    let generation = cursor.u64()?;
    let mut catalog = Catalog {
        generation,
        tables: BTreeMap::new(),
        primary_keys: BTreeMap::new(),
        indexes: BTreeMap::new(),
        index_names: BTreeMap::new(),
        foreign_keys: BTreeMap::new(),
        foreign_key_names: BTreeMap::new(),
    };
    let table_count = cursor.u32()?;
    for _ in 0..table_count {
        let id = TableId(cursor.u64()?);
        let name = cursor.string()?;
        let mut columns = Vec::new();
        for _ in 0..cursor.u32()? {
            columns.push(ColumnDefinition {
                id: ColumnId(cursor.u16()?),
                data_type: column_type_from_tag(cursor.byte()?)?,
                nullable: cursor.byte()? != 0,
                name: cursor.string()?,
            });
        }
        let table = TableDefinition { id, name, columns };
        table.validate()?;
        let primary_key_count = cursor.u32()? as usize;
        if primary_key_count > 256 {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "primary key has too many columns".to_owned(),
            });
        }
        if primary_key_count != 0 {
            let mut primary_key = Vec::with_capacity(primary_key_count);
            for _ in 0..primary_key_count {
                primary_key.push(ColumnId(cursor.u16()?));
            }
            catalog
                .validate_primary_key(&table, &primary_key)
                .map_err(|error| DbError::Corruption {
                    artifact: "catalog",
                    reason: error.to_string(),
                })?;
            catalog.primary_keys.insert(id, primary_key);
        }
        if catalog.tables.insert(id, table).is_some() {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "duplicate table ID".to_owned(),
            });
        }
    }
    for _ in 0..cursor.u32()? {
        let id = IndexId(cursor.u64()?);
        let table = TableId(cursor.u64()?);
        let column_count = cursor.u32()? as usize;
        if column_count == 0 || column_count > 4096 {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "invalid index column count".to_owned(),
            });
        }
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(ColumnId(cursor.u16()?));
        }
        let index = IndexDefinition {
            id,
            table,
            columns,
            unique: cursor.byte()? != 0,
        };
        catalog.validate_index_columns(&index)?;
        if catalog.indexes.insert(index.id, index).is_some() {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "duplicate index ID".to_owned(),
            });
        }
        match cursor.byte()? {
            0 => {}
            1 => {
                let name = cursor.string()?;
                if name.is_empty() {
                    return Err(DbError::Corruption {
                        artifact: "catalog",
                        reason: "empty index name".to_owned(),
                    });
                }
                if catalog
                    .index_names
                    .values()
                    .any(|existing| existing == &name)
                {
                    return Err(DbError::Corruption {
                        artifact: "catalog",
                        reason: "duplicate index name".to_owned(),
                    });
                }
                catalog.index_names.insert(id, name);
            }
            _ => {
                return Err(DbError::Corruption {
                    artifact: "catalog",
                    reason: "invalid index name marker".to_owned(),
                });
            }
        }
    }
    for _ in 0..cursor.u32()? {
        let id = ConstraintId(cursor.u64()?);
        let table = TableId(cursor.u64()?);
        let column_count = cursor.u32()? as usize;
        if column_count == 0 || column_count > 4096 {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "invalid foreign-key column count".to_owned(),
            });
        }
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(ColumnId(cursor.u16()?));
        }
        let referenced_table = TableId(cursor.u64()?);
        let referenced_column_count = cursor.u32()? as usize;
        if referenced_column_count != column_count || referenced_column_count > 4096 {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "foreign-key column counts differ".to_owned(),
            });
        }
        let mut referenced_columns = Vec::with_capacity(referenced_column_count);
        for _ in 0..referenced_column_count {
            referenced_columns.push(ColumnId(cursor.u16()?));
        }
        let on_delete = match cursor.byte()? {
            0 => ReferentialAction::Restrict,
            1 => ReferentialAction::Cascade,
            2 => ReferentialAction::SetNull,
            other => {
                return Err(DbError::Corruption {
                    artifact: "catalog",
                    reason: format!("unknown referential action tag {other}"),
                });
            }
        };
        let timing = match cursor.byte()? {
            0 => ConstraintTiming::Immediate,
            1 => ConstraintTiming::DeferredToPublication,
            other => {
                return Err(DbError::Corruption {
                    artifact: "catalog",
                    reason: format!("unknown constraint-timing tag {other}"),
                });
            }
        };
        let foreign_key = ForeignKeyDefinition {
            id,
            table,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            timing,
        };
        catalog.validate_foreign_key(&foreign_key)?;
        if catalog
            .foreign_keys
            .insert(foreign_key.id, foreign_key)
            .is_some()
        {
            return Err(DbError::Corruption {
                artifact: "catalog",
                reason: "duplicate foreign-key ID".to_owned(),
            });
        }
        match cursor.byte()? {
            0 => {}
            1 => {
                let name = cursor.string()?;
                if name.is_empty() {
                    return Err(DbError::Corruption {
                        artifact: "catalog",
                        reason: "empty foreign-key name".to_owned(),
                    });
                }
                if catalog
                    .index_names
                    .values()
                    .any(|existing| existing == &name)
                    || catalog
                        .foreign_key_names
                        .values()
                        .any(|existing| existing == &name)
                {
                    return Err(DbError::Corruption {
                        artifact: "catalog",
                        reason: "duplicate schema object name".to_owned(),
                    });
                }
                catalog.foreign_key_names.insert(id, name);
            }
            _ => {
                return Err(DbError::Corruption {
                    artifact: "catalog",
                    reason: "invalid foreign-key name marker".to_owned(),
                });
            }
        }
    }
    cursor.finish()?;
    Ok(catalog)
}

fn put_u32(bytes: &mut Vec<u8>, value: usize) -> Result<()> {
    bytes.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| DbError::InvalidState("catalog contains too many entries".to_owned()))?
            .to_le_bytes(),
    );
    Ok(())
}

fn put_string(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    put_bytes(bytes, value.as_bytes())
}

fn column_type_tag(data_type: ColumnType) -> u8 {
    match data_type {
        ColumnType::Bytes => 1,
        ColumnType::Bool => 2,
        ColumnType::I64 => 3,
        ColumnType::U64 => 4,
        ColumnType::Text => 5,
    }
}

fn column_type_from_tag(tag: u8) -> Result<ColumnType> {
    match tag {
        1 => Ok(ColumnType::Bytes),
        2 => Ok(ColumnType::Bool),
        3 => Ok(ColumnType::I64),
        4 => Ok(ColumnType::U64),
        5 => Ok(ColumnType::Text),
        _ => Err(DbError::Corruption {
            artifact: "catalog",
            reason: "unknown column type".to_owned(),
        }),
    }
}

fn io_error(operation: &'static str, source: std::io::Error) -> DbError {
    DbError::Io { operation, source }
}

struct CatalogCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CatalogCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn byte(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| self.corrupt("missing byte"))?;
        self.offset += 1;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16> {
        let end = self
            .offset
            .checked_add(2)
            .ok_or_else(|| self.corrupt("u16 overflow"))?;
        let value = u16::from_le_bytes(
            self.bytes
                .get(self.offset..end)
                .ok_or_else(|| self.corrupt("missing u16"))?
                .try_into()
                .expect("u16 width"),
        );
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32> {
        let end = self
            .offset
            .checked_add(4)
            .ok_or_else(|| self.corrupt("u32 overflow"))?;
        let value = u32::from_le_bytes(
            self.bytes
                .get(self.offset..end)
                .ok_or_else(|| self.corrupt("missing u32"))?
                .try_into()
                .expect("u32 width"),
        );
        self.offset = end;
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64> {
        let end = self
            .offset
            .checked_add(8)
            .ok_or_else(|| self.corrupt("u64 overflow"))?;
        let value = u64::from_le_bytes(
            self.bytes
                .get(self.offset..end)
                .ok_or_else(|| self.corrupt("missing u64"))?
                .try_into()
                .expect("u64 width"),
        );
        self.offset = end;
        Ok(value)
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes_value()?).map_err(|_| self.corrupt("invalid UTF-8"))
    }

    fn bytes_value(&mut self) -> Result<Vec<u8>> {
        let length = self.u32()? as usize;
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| self.corrupt("value length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| self.corrupt("truncated value"))?
            .to_vec();
        self.offset = end;
        Ok(value)
    }

    fn finish(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(self.corrupt("trailing catalog bytes"))
        }
    }

    fn corrupt(&self, reason: &str) -> DbError {
        DbError::Corruption {
            artifact: "catalog",
            reason: reason.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Catalog, ColumnDefinition, ColumnId, ColumnType, ConstraintId, ConstraintTiming,
        ForeignKeyDefinition, IndexDefinition, NamedIndexDefinition, ReferentialAction,
        RelationalMutation, RelationalSchemaDefinition, RelationalStore, Row, SchemaChange,
        SchemaJobState, TableDefinition, TableId, Value, decode_catalog, encode_catalog,
        row_prefix, row_storage_key, table_range,
    };
    use crate::RowIdentity;
    use crate::fault::{FailOnce, FaultPoint, NoFaults};
    use crate::model::{CommitId, IndexId, Key, Mutation};
    use crate::row_identity::encode_legacy_key;
    use crate::runtime::{GovernorConfig, Reactor, ReactorConfig};
    use crate::store::{CompactionBudget, Database, DatabaseConfig};
    use crate::{DbError, TransactionAttemptId};

    fn table() -> TableDefinition {
        TableDefinition {
            id: TableId(11),
            name: "users".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "account".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "name".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        }
    }

    fn composite_table() -> TableDefinition {
        TableDefinition {
            id: TableId(12),
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

    fn composite_schema() -> RelationalSchemaDefinition {
        RelationalSchemaDefinition {
            indexes: vec![
                NamedIndexDefinition {
                    definition: IndexDefinition {
                        id: IndexId(20),
                        table: TableId(12),
                        columns: vec![ColumnId(1), ColumnId(2)],
                        unique: true,
                    },
                    name: Some("ledger_pk".to_owned()),
                },
                NamedIndexDefinition {
                    definition: IndexDefinition {
                        id: IndexId(21),
                        table: TableId(12),
                        columns: vec![ColumnId(3)],
                        unique: false,
                    },
                    name: Some("ledger_state".to_owned()),
                },
            ],
            foreign_keys: Vec::new(),
        }
    }

    fn composite_row(entry_id: u64, state: &str) -> Row {
        Row {
            primary: Key::new(12, entry_id),
            values: vec![
                Value::U64(7),
                Value::U64(entry_id),
                Value::Text(state.to_owned()),
            ],
        }
    }

    #[test]
    fn relational_store_derives_index_mutations_atomically() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(11),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");
        let primary = Key::new(11, 1);
        let commit = store
            .insert(
                TableId(11),
                Row {
                    primary,
                    values: vec![Value::U64(7), Value::Text("alice".to_owned())],
                },
                &mut NoFaults,
            )
            .expect("insert");
        store.database.retain(commit).expect("retain");
        assert_eq!(store.scan(TableId(11), commit, 10).expect("scan").len(), 1);
        assert_eq!(store.catalog.generation(), 2);
        let deleted = store
            .delete(TableId(11), primary, &mut NoFaults)
            .expect("delete");
        assert!(
            store
                .scan(TableId(11), deleted, 10)
                .expect("empty")
                .is_empty()
        );
        assert_eq!(
            store
                .database
                .index_scan_bytes(deleted, IndexId(11), &[0], &[u8::MAX], 10)
                .expect("index scan"),
            Vec::new()
        );
        assert_eq!(
            store.scan(TableId(11), commit, 10).expect("retained").len(),
            1
        );
    }

    #[test]
    fn temporary_row_keys_use_the_canonical_identity_boundary() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        let primary = Key::new(11, 7);
        let commit = store
            .insert(
                TableId(11),
                Row {
                    primary,
                    values: vec![Value::U64(9), Value::Text("identity".to_owned())],
                },
                &mut NoFaults,
            )
            .expect("insert");
        let (start, end) = table_range(TableId(11));
        let entries = store
            .database
            .scan_bytes(commit, start.clone(), end, 10)
            .expect("physical row scan");
        assert_eq!(entries.len(), 1);
        let (physical_key, _) = &entries[0];
        let prefix = row_prefix(TableId(11));
        let identity = RowIdentity::decode(
            physical_key
                .strip_prefix(prefix.as_slice())
                .expect("table namespace prefix"),
        )
        .expect("row identity");
        assert_eq!(identity.table(), TableId(11));
        assert_eq!(identity.columns(), &[ColumnId(0)]);
        assert_eq!(identity.values(), &[Value::Bytes(primary.0.to_vec())]);
    }

    #[test]
    fn reserved_catalog_table_id_is_rejected() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        let mut reserved = table();
        reserved.id = TableId(u64::MAX);
        assert!(matches!(
            store.create_table(reserved),
            Err(DbError::InvalidState(reason)) if reason.contains("reserved")
        ));
        assert_eq!(store.commit_id(), CommitId(0));
    }

    #[test]
    fn typed_reads_and_updates_preserve_index_and_snapshot_consistency() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(16),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");

        let alice = Key::new(11, 30);
        let alice_row = Row {
            primary: alice,
            values: vec![Value::U64(7), Value::Text("alice".to_owned())],
        };
        let alice_commit = store
            .insert(TableId(11), alice_row.clone(), &mut NoFaults)
            .expect("insert alice");
        store.database.retain(alice_commit).expect("retain alice");
        assert_eq!(
            store
                .get(TableId(11), alice_commit, alice)
                .expect("get alice"),
            Some(alice_row.clone())
        );
        assert_eq!(
            store
                .index_get(
                    TableId(11),
                    alice_commit,
                    IndexId(16),
                    &[Value::Text("alice".to_owned())],
                )
                .expect("index alice"),
            vec![alice_row.clone()]
        );

        let bob_row = Row {
            primary: alice,
            values: vec![Value::U64(7), Value::Text("bob".to_owned())],
        };
        let bob_commit = store
            .update(TableId(11), bob_row.clone(), &mut NoFaults)
            .expect("update bob");
        assert_eq!(
            store.get(TableId(11), bob_commit, alice).expect("get bob"),
            Some(bob_row.clone())
        );
        assert!(
            store
                .index_get(
                    TableId(11),
                    bob_commit,
                    IndexId(16),
                    &[Value::Text("alice".to_owned())],
                )
                .expect("old index")
                .is_empty()
        );
        assert_eq!(
            store
                .index_get(
                    TableId(11),
                    bob_commit,
                    IndexId(16),
                    &[Value::Text("bob".to_owned())],
                )
                .expect("new index"),
            vec![bob_row.clone()]
        );
        assert_eq!(
            store
                .get(TableId(11), alice_commit, alice)
                .expect("old row"),
            Some(alice_row)
        );

        let carol = Key::new(11, 31);
        let carol_row = Row {
            primary: carol,
            values: vec![Value::U64(8), Value::Text("carol".to_owned())],
        };
        let current = store
            .insert(TableId(11), carol_row.clone(), &mut NoFaults)
            .expect("insert carol");
        let indexed = store
            .index_scan(TableId(11), current, IndexId(16), None, None, 10)
            .expect("index scan");
        assert_eq!(
            indexed.iter().map(|row| row.primary).collect::<Vec<_>>(),
            vec![alice, carol]
        );

        let duplicate = Row {
            primary: alice,
            values: vec![Value::U64(7), Value::Text("carol".to_owned())],
        };
        assert!(matches!(
            store.update(TableId(11), duplicate, &mut NoFaults),
            Err(DbError::UniqueViolation { index: 16, .. })
        ));
        assert_eq!(
            store
                .get(TableId(11), current, alice)
                .expect("unchanged row"),
            Some(bob_row)
        );
        assert_eq!(
            store
                .index_get(
                    TableId(11),
                    current,
                    IndexId(16),
                    &[Value::Text("carol".to_owned())],
                )
                .expect("carol index"),
            vec![carol_row]
        );
    }

    #[test]
    fn relational_batches_commit_related_rows_once_and_roll_back_on_conflict() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(17),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");

        let alice = Row {
            primary: Key::new(11, 40),
            values: vec![Value::U64(7), Value::Text("alice".to_owned())],
        };
        let bob = Row {
            primary: Key::new(11, 41),
            values: vec![Value::U64(8), Value::Text("bob".to_owned())],
        };
        let before_batch = store.commit_id();
        let commit = store
            .commit_batch(
                [
                    RelationalMutation::Insert {
                        table: TableId(11),
                        row: alice.clone(),
                    },
                    RelationalMutation::Insert {
                        table: TableId(11),
                        row: bob.clone(),
                    },
                ],
                &mut NoFaults,
            )
            .expect("batch insert");
        assert_eq!(commit, CommitId(before_batch.0 + 1));
        assert_eq!(
            store.scan(TableId(11), commit, 10).expect("scan"),
            vec![alice.clone(), bob.clone()]
        );

        let carol = Row {
            primary: Key::new(11, 42),
            values: vec![Value::U64(9), Value::Text("carol".to_owned())],
        };
        let renamed_alice = Row {
            primary: alice.primary,
            values: vec![Value::U64(7), Value::Text("carol".to_owned())],
        };
        assert!(matches!(
            store.commit_batch(
                [
                    RelationalMutation::Insert {
                        table: TableId(11),
                        row: carol.clone(),
                    },
                    RelationalMutation::Update {
                        table: TableId(11),
                        row: renamed_alice,
                    },
                ],
                &mut NoFaults,
            ),
            Err(DbError::UniqueViolation { index: 17, .. })
        ));
        assert_eq!(store.database.commit_id(), commit);
        assert_eq!(
            store
                .get(TableId(11), commit, carol.primary)
                .expect("carol"),
            None
        );
        assert_eq!(
            store
                .get(TableId(11), commit, alice.primary)
                .expect("alice"),
            Some(alice)
        );
    }

    #[test]
    fn schema_and_rows_reopen_from_wal_without_checkpoint() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        store.create_table(table()).expect("table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(18),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");
        let row = Row {
            primary: Key::new(11, 43),
            values: vec![Value::U64(18), Value::Text("wal-only".to_owned())],
        };
        let commit = store
            .insert(TableId(11), row.clone(), &mut NoFaults)
            .expect("insert");
        drop(store);

        let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        assert!(reopened.catalog().table(TableId(11)).is_ok());
        assert!(
            reopened
                .catalog()
                .indexes()
                .any(|index| index.id == IndexId(18))
        );
        assert_eq!(
            reopened.get(TableId(11), commit, row.primary).expect("get"),
            Some(row)
        );
        assert!(!directory.path().join("omendb.catalog").exists());
    }

    #[test]
    fn schema_publication_reopens_old_or_complete_new_after_wal_fault() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        store.create_table(table()).expect("table");
        let row = Row {
            primary: Key::new(11, 44),
            values: vec![Value::U64(19), Value::Text("fault".to_owned())],
        };
        store
            .insert(TableId(11), row.clone(), &mut NoFaults)
            .expect("insert");
        assert!(
            store
                .create_index(
                    IndexDefinition {
                        id: IndexId(19),
                        table: TableId(11),
                        columns: vec![ColumnId(2)],
                        unique: true,
                    },
                    &mut FailOnce::at([FaultPoint::AfterWalSync]),
                )
                .is_err()
        );
        drop(store);

        let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        let catalog_has_index = reopened
            .catalog()
            .indexes()
            .any(|index| index.id == IndexId(19));
        let database_has_index = reopened
            .database
            .secondary_index_ids()
            .contains(&IndexId(19));
        assert_eq!(catalog_has_index, database_has_index);
        if catalog_has_index {
            assert_eq!(
                reopened
                    .index_get(
                        TableId(11),
                        reopened.commit_id(),
                        IndexId(19),
                        &[Value::Text("fault".to_owned())],
                    )
                    .expect("index"),
                vec![row]
            );
        }
    }

    #[test]
    fn table_schema_publication_reopens_old_or_complete_new_after_wal_fault() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        let table = TableDefinition {
            id: TableId(12),
            name: "events".to_owned(),
            columns: vec![ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            }],
        };
        let schema = RelationalSchemaDefinition {
            indexes: vec![NamedIndexDefinition {
                definition: IndexDefinition {
                    id: IndexId(20),
                    table: TableId(12),
                    columns: vec![ColumnId(1)],
                    unique: true,
                },
                name: Some("events_id_unique".to_owned()),
            }],
            foreign_keys: Vec::new(),
        };
        assert!(
            store
                .create_table_with_schema(
                    table,
                    schema,
                    &mut FailOnce::at([FaultPoint::AfterWalSync]),
                )
                .is_err()
        );
        drop(store);

        let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        let table_exists = reopened.catalog().table(TableId(12)).is_ok();
        let index_exists = reopened
            .catalog()
            .indexes()
            .any(|index| index.id == IndexId(20));
        assert_eq!(table_exists, index_exists);
        if table_exists {
            assert_eq!(
                reopened.catalog().index_name(IndexId(20)),
                Some("events_id_unique")
            );
        }
    }

    #[test]
    fn composite_identity_catalog_and_rows_reopen_old_or_complete_new_after_faults() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        assert!(
            store
                .create_table_with_schema_and_primary_key(
                    composite_table(),
                    Some(vec![ColumnId(1), ColumnId(2)]),
                    composite_schema(),
                    &mut FailOnce::at([FaultPoint::AfterWalSync]),
                )
                .is_err()
        );
        drop(store);

        let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        let table_exists = reopened.catalog().table(TableId(12)).is_ok();
        let index_count = reopened.catalog().indexes_for(TableId(12)).count();
        assert_eq!(table_exists, index_count == 2);
        if table_exists {
            assert_eq!(
                reopened.catalog().primary_key(TableId(12)),
                Some([ColumnId(1), ColumnId(2)].as_slice())
            );
            assert_eq!(
                reopened.catalog().index_name(IndexId(20)),
                Some("ledger_pk")
            );
            assert_eq!(
                reopened.catalog().index_name(IndexId(21)),
                Some("ledger_state")
            );
            reopened.verify().expect("verify recovered catalog");
        }

        for point in [
            FaultPoint::BeforeWalAppend,
            FaultPoint::ShortWrite,
            FaultPoint::TornWrite,
            FaultPoint::AfterWalAppend,
            FaultPoint::WalSync,
            FaultPoint::AfterWalSync,
        ] {
            let directory = tempfile::tempdir().expect("tempdir");
            let config = DatabaseConfig {
                directory: directory.path().to_path_buf(),
            };
            let mut store = RelationalStore::create(config.clone()).expect("create");
            store
                .create_table_with_schema_and_primary_key(
                    composite_table(),
                    Some(vec![ColumnId(1), ColumnId(2)]),
                    composite_schema(),
                    &mut NoFaults,
                )
                .expect("composite schema");
            let old_row = composite_row(1, "open");
            let baseline = store
                .insert(TableId(12), old_row, &mut NoFaults)
                .expect("old row");
            let new_row = composite_row(2, "closed");
            assert!(
                store
                    .insert(TableId(12), new_row, &mut FailOnce::at([point]),)
                    .is_err(),
                "fault {point:?} unexpectedly succeeded"
            );
            drop(store);

            let reopened = RelationalStore::open(config, &mut NoFaults)
                .unwrap_or_else(|error| panic!("reopen after {point:?}: {error}"));
            reopened.verify().expect("verify recovered composite state");
            assert_eq!(
                reopened.catalog().primary_key(TableId(12)),
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
            let rows = reopened
                .scan(TableId(12), recovered, usize::MAX)
                .expect("scan recovered rows");
            assert_eq!(rows.len(), if new_generation { 2 } else { 1 });
            assert_eq!(
                reopened
                    .index_get(
                        TableId(12),
                        recovered,
                        IndexId(21),
                        &[Value::Text("closed".to_owned())],
                    )
                    .expect("recovered composite index")
                    .len(),
                usize::from(new_generation)
            );
        }
    }

    #[test]
    fn nullable_column_publication_reopens_old_or_complete_new_after_wal_faults() {
        for (ordinal, point) in [
            FaultPoint::BeforeWalAppend,
            FaultPoint::ShortWrite,
            FaultPoint::TornWrite,
            FaultPoint::AfterWalAppend,
            FaultPoint::WalSync,
            FaultPoint::AfterWalSync,
        ]
        .into_iter()
        .enumerate()
        {
            let directory = tempfile::tempdir().expect("tempdir");
            let config = DatabaseConfig {
                directory: directory.path().to_path_buf(),
            };
            let mut store = RelationalStore::create(config.clone()).expect("create");
            store.create_table(table()).expect("table");
            let primary = Key::new(11, 44);
            let old_row = Row {
                primary,
                values: vec![Value::U64(19), Value::Text("fault".to_owned())],
            };
            store
                .insert(TableId(11), old_row.clone(), &mut NoFaults)
                .expect("insert");
            assert!(
                store
                    .add_nullable_column(
                        TableId(11),
                        ColumnDefinition {
                            id: ColumnId(3),
                            name: "note".to_owned(),
                            data_type: ColumnType::Text,
                            nullable: true,
                        },
                        &mut FailOnce::at([point]),
                    )
                    .is_err(),
                "fault point {point:?} unexpectedly succeeded in case {ordinal}"
            );
            drop(store);

            let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
            reopened.verify().expect("verify reopened schema state");
            let columns = reopened
                .catalog()
                .table(TableId(11))
                .expect("reopened table")
                .columns
                .len();
            let row = reopened
                .get(TableId(11), reopened.commit_id(), primary)
                .expect("reopened row")
                .expect("row exists");
            match columns {
                2 => assert_eq!(row, old_row),
                3 => assert_eq!(
                    row,
                    Row {
                        primary,
                        values: vec![Value::U64(19), Value::Text("fault".to_owned()), Value::Null,],
                    }
                ),
                actual => panic!("fault point {point:?} left {actual} columns"),
            }
            reopened.close().expect("close reopened");
        }
    }

    #[test]
    fn nullable_column_publication_does_not_rewrite_existing_rows() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config).expect("create");
        store.create_table(table()).expect("table");
        let row = Row {
            primary: Key::new(11, 45),
            values: vec![Value::U64(20), Value::Text("metadata-only".to_owned())],
        };
        store
            .insert(TableId(11), row.clone(), &mut NoFaults)
            .expect("insert");
        let before = store
            .database
            .get_bytes(store.commit_id(), row_storage_key(TableId(11), row.primary))
            .expect("read physical row before schema change")
            .expect("physical row before schema change");

        let commit = store
            .add_nullable_column(
                TableId(11),
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "note".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: true,
                },
                &mut NoFaults,
            )
            .expect("append nullable column");
        let after = store
            .database
            .get_bytes(commit, row_storage_key(TableId(11), row.primary))
            .expect("read physical row after schema change")
            .expect("physical row after schema change");
        assert_eq!(after, before);
        assert_eq!(
            store
                .get(TableId(11), commit, row.primary)
                .expect("read logical row"),
            Some(Row {
                primary: row.primary,
                values: vec![
                    Value::U64(20),
                    Value::Text("metadata-only".to_owned()),
                    Value::Null,
                ],
            })
        );
    }

    #[test]
    fn foreign_keys_validate_final_batch_state_and_parent_deletes() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        let parent_table = TableDefinition {
            id: TableId(30),
            name: "parents".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "tenant".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "parent_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
            ],
        };
        let child_table = TableDefinition {
            id: TableId(31),
            name: "children".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "tenant".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "parent_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
            ],
        };
        store.create_table(parent_table).expect("parent table");
        store.create_table(child_table).expect("child table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(30),
                    table: TableId(30),
                    columns: vec![ColumnId(1), ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("parent key index");
        store
            .create_foreign_key(ForeignKeyDefinition {
                id: ConstraintId(30),
                table: TableId(31),
                columns: vec![ColumnId(1), ColumnId(2)],
                referenced_table: TableId(30),
                referenced_columns: vec![ColumnId(1), ColumnId(2)],
                on_delete: ReferentialAction::default(),
                timing: ConstraintTiming::default(),
            })
            .expect("foreign key");

        let parent = Row {
            primary: Key::new(30, 1),
            values: vec![Value::U64(1), Value::U64(7)],
        };
        let child = Row {
            primary: Key::new(31, 1),
            values: vec![Value::U64(1), Value::U64(7)],
        };
        let before_invalid_insert = store.commit_id();
        assert!(matches!(
            store.insert(TableId(31), child.clone(), &mut NoFaults),
            Err(DbError::ForeignKeyViolation { constraint: 30, .. })
        ));
        assert_eq!(store.commit_id(), before_invalid_insert);

        let committed = store
            .commit_batch(
                [
                    RelationalMutation::Insert {
                        table: TableId(30),
                        row: parent.clone(),
                    },
                    RelationalMutation::Insert {
                        table: TableId(31),
                        row: child.clone(),
                    },
                ],
                &mut NoFaults,
            )
            .expect("parent and child batch");
        assert_eq!(
            store.scan(TableId(31), committed, 10).expect("child"),
            vec![child.clone()]
        );
        store.checkpoint(&mut NoFaults).expect("checkpoint");
        drop(store);

        let mut reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        assert!(matches!(
            reopened.insert(
                TableId(31),
                Row {
                    primary: Key::new(31, 2),
                    values: vec![Value::U64(1), Value::U64(99)],
                },
                &mut NoFaults,
            ),
            Err(DbError::ForeignKeyViolation { constraint: 30, .. })
        ));
        assert!(matches!(
            reopened.delete(TableId(30), parent.primary, &mut NoFaults),
            Err(DbError::ForeignKeyViolation { constraint: 30, .. })
        ));
        let deleted = reopened
            .commit_batch(
                [
                    RelationalMutation::Delete {
                        table: TableId(31),
                        primary: child.primary,
                    },
                    RelationalMutation::Delete {
                        table: TableId(30),
                        primary: parent.primary,
                    },
                ],
                &mut NoFaults,
            )
            .expect("delete child and parent");
        assert!(
            reopened
                .scan(TableId(30), deleted, 10)
                .expect("parents")
                .is_empty()
        );
        assert!(
            reopened
                .scan(TableId(31), deleted, 10)
                .expect("children")
                .is_empty()
        );
    }

    #[test]
    fn typed_transactions_expose_snapshot_reads_and_retryable_conflicts() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(50),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");
        let primary = Key::new(11, 50);
        let original = Row {
            primary,
            values: vec![Value::U64(7), Value::Text("original".to_owned())],
        };
        let seeded = store
            .insert(TableId(11), original.clone(), &mut NoFaults)
            .expect("seed");
        let mut reader = store.begin().expect("reader transaction");
        let mut writer = store.begin().expect("writer transaction");
        assert_eq!(reader.snapshot(), seeded);
        store.retain(seeded).expect("retain transaction snapshot");
        assert_eq!(
            reader
                .get(&store, TableId(11), primary)
                .expect("snapshot read"),
            Some(original)
        );
        let updated_row = Row {
            primary,
            values: vec![Value::U64(7), Value::Text("updated".to_owned())],
        };
        writer
            .update(&store, TableId(11), updated_row.clone())
            .expect("stage update");
        assert_eq!(
            writer
                .get(&store, TableId(11), primary)
                .expect("staged read"),
            Some(updated_row.clone())
        );
        assert_eq!(
            writer
                .index_get(
                    &store,
                    TableId(11),
                    IndexId(50),
                    &[Value::Text("updated".to_owned())],
                )
                .expect("staged index read"),
            vec![updated_row.clone()]
        );
        assert_eq!(
            writer
                .index_scan(&store, TableId(11), IndexId(50), None, None, 10)
                .expect("staged index scan"),
            vec![updated_row]
        );
        let updated_commit = writer
            .commit(&mut store, &mut NoFaults)
            .expect("writer commit");
        reader
            .insert(
                &store,
                TableId(11),
                Row {
                    primary: Key::new(11, 51),
                    values: vec![Value::U64(8), Value::Text("reader-write".to_owned())],
                },
            )
            .expect("stage reader write");
        assert!(matches!(
            reader.commit(&mut store, &mut NoFaults),
            Err(DbError::SerializationConflict {
                snapshot,
                current
            }) if snapshot == seeded.0 && current == updated_commit.0
        ));
        assert_eq!(store.commit_id(), updated_commit);
        assert_eq!(
            store
                .scan(TableId(11), updated_commit, 10)
                .expect("scan")
                .len(),
            1
        );
        store.release(seeded);
    }

    #[test]
    fn transaction_catalog_snapshot_is_historical_and_conflicts_with_schema_change() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        let row = Row {
            primary: Key::new(11, 52),
            values: vec![Value::U64(20), Value::Text("before".to_owned())],
        };
        let snapshot = store
            .insert(TableId(11), row.clone(), &mut NoFaults)
            .expect("insert");
        store.retain(snapshot).expect("retain snapshot");
        let mut transaction = store.begin().expect("begin");
        assert_eq!(transaction.snapshot(), snapshot);

        let index_commit = store
            .create_index(
                IndexDefinition {
                    id: IndexId(52),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");
        assert!(matches!(
            transaction.index_get(
                &store,
                TableId(11),
                IndexId(52),
                &[Value::Text("before".to_owned())],
            ),
            Err(DbError::InvalidState(reason)) if reason.contains("does not exist")
        ));
        assert!(matches!(
            store.index_get(
                TableId(11),
                snapshot,
                IndexId(52),
                &[Value::Text("before".to_owned())],
            ),
            Err(DbError::InvalidState(reason)) if reason.contains("does not exist")
        ));

        transaction
            .update(
                &store,
                TableId(11),
                Row {
                    primary: row.primary,
                    values: vec![Value::U64(20), Value::Text("stale".to_owned())],
                },
            )
            .expect("stage stale update");
        assert!(matches!(
            transaction.commit(&mut store, &mut NoFaults),
            Err(DbError::SerializationConflict { snapshot: old, current })
                if old == snapshot.0 && current == index_commit.0
        ));
        store.release(snapshot);
    }

    #[test]
    fn retained_historical_catalog_survives_checkpoint_compaction_and_reopen() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        store.create_table(table()).expect("table");
        let snapshot = store
            .insert(
                TableId(11),
                Row {
                    primary: Key::new(11, 53),
                    values: vec![Value::U64(21), Value::Text("retained".to_owned())],
                },
                &mut NoFaults,
            )
            .expect("insert");
        store.retain(snapshot).expect("retain");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(53),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");
        store.checkpoint(&mut NoFaults).expect("checkpoint");
        store
            .compact_with_budget(CompactionBudget {
                max_row_keys: 128,
                max_index_keys: 128,
            })
            .expect("compact");
        drop(store);

        let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        assert!(matches!(
            reopened.index_get(
                TableId(11),
                snapshot,
                IndexId(53),
                &[Value::Text("retained".to_owned())],
            ),
            Err(DbError::InvalidState(reason)) if reason.contains("does not exist")
        ));
    }

    #[test]
    fn transaction_helper_commits_writes_and_skips_read_only_commit() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        let primary = Key::new(11, 91);
        let row = Row {
            primary,
            values: vec![Value::U64(7), Value::Text("transaction".to_owned())],
        };

        let (visible, commit) = store
            .transaction(|store, transaction| {
                transaction.insert(store, TableId(11), row.clone())?;
                transaction.get(store, TableId(11), primary)
            })
            .expect("transaction");
        assert_eq!(visible, Some(row));
        assert_eq!(commit, store.commit_id());

        let (count, read_only_commit) = store
            .transaction(|store, transaction| {
                transaction
                    .scan(store, TableId(11), 10)
                    .map(|rows| rows.len())
            })
            .expect("read-only transaction");
        assert_eq!(count, 1);
        assert_eq!(read_only_commit, commit);
        assert_eq!(store.commit_id(), commit);

        let failed = store.transaction(|store, transaction| -> crate::Result<()> {
            transaction.delete(store, TableId(11), primary)?;
            Err(DbError::InvalidState("abort from closure".to_owned()))
        });
        assert!(
            matches!(failed, Err(DbError::InvalidState(reason)) if reason == "abort from closure")
        );
        assert_eq!(store.commit_id(), commit);
        assert!(
            store
                .get(TableId(11), commit, primary)
                .expect("row")
                .is_some()
        );
    }

    #[test]
    fn direct_empty_commits_are_backend_neutral_read_only_boundaries() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        let current = store.commit_id();

        assert_eq!(
            store
                .commit_batch(std::iter::empty(), &mut NoFaults)
                .expect("empty batch"),
            current
        );
        let transaction = store.begin().expect("begin");
        assert_eq!(
            transaction
                .commit(&mut store, &mut NoFaults)
                .expect("empty transaction"),
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
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        let attempt = crate::TransactionAttemptId::new([11; 16]);
        let commit = store
            .commit_batch_with_attempt(
                [RelationalMutation::Insert {
                    table: TableId(11),
                    row: Row {
                        primary: Key::new(11, 91),
                        values: vec![Value::U64(91), Value::Text("attempt".to_owned())],
                    },
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
    fn adding_a_foreign_key_rejects_existing_orphans_before_activation() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        let mut parent = table();
        parent.id = TableId(32);
        parent.name = "fk_parents".to_owned();
        let mut child = table();
        child.id = TableId(33);
        child.name = "fk_children".to_owned();
        store.create_table(parent).expect("parent table");
        store.create_table(child).expect("child table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(70),
                    table: TableId(32),
                    columns: vec![ColumnId(1)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("parent key index");
        let orphan = Row {
            primary: Key::new(33, 1),
            values: vec![Value::U64(99), Value::Text("orphan".to_owned())],
        };
        let orphan_commit = store
            .insert(TableId(33), orphan.clone(), &mut NoFaults)
            .expect("orphan");
        let foreign_key = ForeignKeyDefinition {
            id: ConstraintId(70),
            table: TableId(33),
            columns: vec![ColumnId(1)],
            referenced_table: TableId(32),
            referenced_columns: vec![ColumnId(1)],
            on_delete: ReferentialAction::default(),
            timing: ConstraintTiming::default(),
        };
        assert!(matches!(
            store.create_foreign_key(foreign_key.clone()),
            Err(DbError::ForeignKeyViolation { constraint: 70, .. })
        ));
        assert_eq!(store.commit_id(), orphan_commit);
        store
            .delete(TableId(33), orphan.primary, &mut NoFaults)
            .expect("remove orphan");
        store
            .create_foreign_key(foreign_key)
            .expect("activate foreign key");
    }

    #[test]
    fn compaction_fault_reopen_preserves_current_and_retained_typed_state() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        store.create_table(table()).expect("table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(60),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");
        let original = Row {
            primary: Key::new(11, 60),
            values: vec![Value::U64(7), Value::Text("before".to_owned())],
        };
        let seeded = store
            .insert(TableId(11), original.clone(), &mut NoFaults)
            .expect("seed");
        store.retain(seeded).expect("retain");
        store.checkpoint(&mut NoFaults).expect("checkpoint");
        let updated = Row {
            primary: original.primary,
            values: vec![Value::U64(7), Value::Text("after".to_owned())],
        };
        let current = store
            .update(TableId(11), updated.clone(), &mut NoFaults)
            .expect("update");
        assert!(matches!(
            store.compact_with_budget_and_faults(
                CompactionBudget {
                    max_row_keys: 1,
                    max_index_keys: 1,
                },
                &mut FailOnce::at([FaultPoint::DuringCompaction]),
            ),
            Err(DbError::InjectedFailure(FaultPoint::DuringCompaction))
        ));
        drop(store);

        let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        assert_eq!(
            reopened
                .scan(TableId(11), current, 10)
                .expect("current")
                .as_slice(),
            &[updated]
        );
        assert_eq!(
            reopened
                .scan(TableId(11), seeded, 10)
                .expect("retained")
                .as_slice(),
            &[original]
        );
    }

    #[test]
    fn relational_filter_project_and_join_are_deterministic() {
        let left = vec![Row {
            primary: Key::new(12, 1),
            values: vec![Value::U64(7), Value::Text("left".to_owned())],
        }];
        let right = vec![Row {
            primary: Key::new(13, 1),
            values: vec![Value::U64(7), Value::Bool(true)],
        }];
        let filtered = RelationalStore::filter(&left, |row| row.values[0] == Value::U64(7));
        assert_eq!(
            RelationalStore::project(&filtered, &[1]).expect("project"),
            vec![vec![Value::Text("left".to_owned())]]
        );
        assert_eq!(
            RelationalStore::nested_loop_join(&left, &right, 0, 0)
                .expect("join")
                .len(),
            1
        );
        assert_eq!(
            RelationalStore::hash_join(&left, &right, 0, 0)
                .expect("hash join")
                .len(),
            1
        );
        let null_left = vec![Row {
            primary: Key::new(12, 2),
            values: vec![Value::Null, Value::Text("null".to_owned())],
        }];
        assert!(
            RelationalStore::hash_join(&null_left, &right, 0, 0)
                .expect("null hash join")
                .is_empty()
        );
    }

    #[test]
    fn creating_an_index_builds_existing_rows_in_one_commit() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store.create_table(table()).expect("table");
        let primary = Key::new(11, 7);
        store
            .insert(
                TableId(11),
                Row {
                    primary,
                    values: vec![Value::U64(9), Value::Text("before".to_owned())],
                },
                &mut NoFaults,
            )
            .expect("insert");
        let commit = store
            .create_index(
                IndexDefinition {
                    id: IndexId(12),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index build");
        assert_eq!(
            store
                .database
                .index_scan_bytes(commit, IndexId(12), &[0], &[u8::MAX], 10)
                .expect("index scan")
                .len(),
            1
        );
    }

    #[test]
    fn composite_index_keys_preserve_tenant_scope_after_reopen() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        store
            .create_table(TableDefinition {
                id: TableId(18),
                name: "scoped_projects".to_owned(),
                columns: vec![
                    ColumnDefinition {
                        id: ColumnId(1),
                        name: "tenant".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(2),
                        name: "slug".to_owned(),
                        data_type: ColumnType::Text,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(3),
                        name: "active".to_owned(),
                        data_type: ColumnType::Bool,
                        nullable: false,
                    },
                ],
            })
            .expect("table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(18),
                    table: TableId(18),
                    columns: vec![ColumnId(1), ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");
        let first = Row {
            primary: Key::new(18, 1),
            values: vec![
                Value::U64(1),
                Value::Text("alpha".to_owned()),
                Value::Bool(true),
            ],
        };
        let second = Row {
            primary: Key::new(18, 2),
            values: vec![
                Value::U64(1),
                Value::Text("beta".to_owned()),
                Value::Bool(true),
            ],
        };
        let other_tenant = Row {
            primary: Key::new(18, 3),
            values: vec![
                Value::U64(2),
                Value::Text("alpha".to_owned()),
                Value::Bool(true),
            ],
        };
        store
            .commit_batch(
                [
                    RelationalMutation::Insert {
                        table: TableId(18),
                        row: first.clone(),
                    },
                    RelationalMutation::Insert {
                        table: TableId(18),
                        row: second,
                    },
                    RelationalMutation::Insert {
                        table: TableId(18),
                        row: other_tenant,
                    },
                ],
                &mut NoFaults,
            )
            .expect("rows");
        store.checkpoint(&mut NoFaults).expect("checkpoint");
        drop(store);

        let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        assert_eq!(
            reopened
                .index_get(
                    TableId(18),
                    reopened.database.commit_id(),
                    IndexId(18),
                    &[Value::U64(1), Value::Text("alpha".to_owned())],
                )
                .expect("scoped lookup"),
            vec![first]
        );
        assert_eq!(
            reopened
                .index_scan(
                    TableId(18),
                    reopened.database.commit_id(),
                    IndexId(18),
                    None,
                    None,
                    10,
                )
                .expect("index scan")
                .len(),
            3
        );
    }

    #[test]
    fn typed_batch_lifecycle_keeps_composite_indexes_and_retained_snapshot() {
        let directory = tempfile::tempdir().expect("tempdir");
        let table_id = TableId(19);
        let indexes = [IndexId(19), IndexId(20), IndexId(21)];
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        store
            .create_table(TableDefinition {
                id: table_id,
                name: "documents".to_owned(),
                columns: vec![
                    ColumnDefinition {
                        id: ColumnId(1),
                        name: "tenant".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(2),
                        name: "document".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(3),
                        name: "status".to_owned(),
                        data_type: ColumnType::Text,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(4),
                        name: "owner".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                    ColumnDefinition {
                        id: ColumnId(5),
                        name: "updated".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    },
                ],
            })
            .expect("table");
        for (id, columns) in [
            (indexes[0], vec![ColumnId(1), ColumnId(3)]),
            (indexes[1], vec![ColumnId(1), ColumnId(4)]),
            (indexes[2], vec![ColumnId(1), ColumnId(5)]),
        ] {
            store
                .create_index(
                    IndexDefinition {
                        id,
                        table: table_id,
                        columns,
                        unique: false,
                    },
                    &mut NoFaults,
                )
                .expect("index");
        }

        let first = Row {
            primary: Key::new(table_id.0, 1),
            values: vec![
                Value::U64(1),
                Value::U64(1),
                Value::Text("new".to_owned()),
                Value::U64(7),
                Value::U64(1),
            ],
        };
        let second = Row {
            primary: Key::new(table_id.0, 2),
            values: vec![
                Value::U64(1),
                Value::U64(2),
                Value::Text("new".to_owned()),
                Value::U64(8),
                Value::U64(1),
            ],
        };
        let seeded = store
            .commit_batch(
                [
                    RelationalMutation::Insert {
                        table: table_id,
                        row: first.clone(),
                    },
                    RelationalMutation::Insert {
                        table: table_id,
                        row: second.clone(),
                    },
                ],
                &mut NoFaults,
            )
            .expect("seed");
        store.retain(seeded).expect("retain");

        let updated = Row {
            primary: first.primary,
            values: vec![
                Value::U64(1),
                Value::U64(1),
                Value::Text("active".to_owned()),
                Value::U64(9),
                Value::U64(2),
            ],
        };
        let current = store
            .commit_batch(
                [
                    RelationalMutation::Update {
                        table: table_id,
                        row: updated.clone(),
                    },
                    RelationalMutation::Delete {
                        table: table_id,
                        primary: second.primary,
                    },
                ],
                &mut NoFaults,
            )
            .expect("update/delete");
        assert_eq!(
            store.scan(table_id, current, 10).expect("current"),
            vec![updated.clone()]
        );
        assert_eq!(
            store.scan(table_id, seeded, 10).expect("retained"),
            vec![first.clone(), second.clone()]
        );
        assert_eq!(
            store
                .index_get(
                    table_id,
                    current,
                    indexes[0],
                    &[Value::U64(1), Value::Text("active".to_owned())],
                )
                .expect("active index"),
            vec![updated.clone()]
        );
        assert!(
            store
                .index_get(
                    table_id,
                    current,
                    indexes[0],
                    &[Value::U64(1), Value::Text("new".to_owned())],
                )
                .expect("old status index")
                .is_empty()
        );
        assert_eq!(
            store
                .index_get(
                    table_id,
                    seeded,
                    indexes[0],
                    &[Value::U64(1), Value::Text("new".to_owned())],
                )
                .expect("retained status index"),
            vec![first.clone(), second.clone()]
        );
        for index in indexes {
            assert_eq!(
                store
                    .index_scan(table_id, current, index, None, None, 10)
                    .expect("current index scan")
                    .len(),
                1
            );
            assert_eq!(
                store
                    .index_scan(table_id, seeded, index, None, None, 10)
                    .expect("retained index scan")
                    .len(),
                2
            );
        }

        let report = store
            .compact_with_budget(CompactionBudget {
                max_row_keys: 2,
                max_index_keys: 4,
            })
            .expect("bounded compaction");
        assert!(report.row_keys_considered <= 2);
        assert!(report.index_keys_considered <= 4);
        assert_eq!(
            store
                .scan(table_id, seeded, 10)
                .expect("retained after compaction")
                .len(),
            2
        );
        assert!(matches!(
            store.delete(table_id, Key::new(table_id.0, 99), &mut NoFaults),
            Err(DbError::InvalidState(message)) if message == "row does not exist"
        ));
        assert_eq!(store.commit_id(), current);
        store.release(seeded);
        assert!(matches!(
            store.scan(table_id, seeded, 10),
            Err(DbError::SnapshotUnavailable(snapshot)) if snapshot == seeded.0
        ));
    }

    #[test]
    fn checkpoint_reopens_catalog_and_rows_together() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        store.create_table(table()).expect("table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(13),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");
        let key = Key::new(11, 8);
        store
            .insert(
                TableId(11),
                Row {
                    primary: key,
                    values: vec![Value::U64(10), Value::Text("persisted".to_owned())],
                },
                &mut NoFaults,
            )
            .expect("insert");
        store.checkpoint(&mut NoFaults).expect("checkpoint");
        store
            .insert(
                TableId(11),
                Row {
                    primary: Key::new(11, 9),
                    values: vec![Value::U64(11), Value::Text("after-checkpoint".to_owned())],
                },
                &mut NoFaults,
            )
            .expect("post-checkpoint insert");
        drop(store);
        let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        assert_eq!(reopened.catalog.generation(), 2);
        assert_eq!(
            reopened
                .scan(TableId(11), reopened.database.commit_id(), 10)
                .expect("scan")
                .len(),
            2
        );
        let entries = reopened
            .database
            .index_scan_bytes(
                reopened.database.commit_id(),
                IndexId(13),
                &[0],
                &[u8::MAX],
                10,
            )
            .expect("index");
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| {
            entry.1 == encode_legacy_key(TableId(11), key).expect("encode row identity")
        }));
    }

    #[test]
    fn catalog_publication_precedes_failed_data_checkpoint() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let mut store = RelationalStore::create(config.clone()).expect("create");
        store.create_table(table()).expect("table");
        store
            .create_index(
                IndexDefinition {
                    id: IndexId(14),
                    table: TableId(11),
                    columns: vec![ColumnId(2)],
                    unique: true,
                },
                &mut NoFaults,
            )
            .expect("index");
        store
            .insert(
                TableId(11),
                Row {
                    primary: Key::new(11, 10),
                    values: vec![Value::U64(12), Value::Text("recoverable".to_owned())],
                },
                &mut NoFaults,
            )
            .expect("insert");

        let checkpoint = store.checkpoint(&mut FailOnce::at([FaultPoint::AfterManifestPublish]));
        assert!(checkpoint.is_err());
        drop(store);

        let reopened = RelationalStore::open(config, &mut NoFaults).expect("reopen");
        assert_eq!(reopened.catalog.generation(), 2);
        assert_eq!(reopened.database.secondary_index_ids(), vec![IndexId(14)]);
        assert_eq!(
            reopened
                .scan(TableId(11), reopened.database.commit_id(), 10)
                .expect("scan")
                .len(),
            1
        );
    }

    #[test]
    fn catalog_data_checkpoint_fault_matrix_reopens_consistently() {
        let points = [
            FaultPoint::ShortWrite,
            FaultPoint::TornWrite,
            FaultPoint::PackedPageSync,
            FaultPoint::DataSync,
            FaultPoint::AfterWalSync,
            FaultPoint::ManifestSync,
            FaultPoint::AfterManifestPublish,
            FaultPoint::WalTruncate,
        ];
        for point in points {
            let directory = tempfile::tempdir().expect("tempdir");
            let config = crate::store::DatabaseConfig {
                directory: directory.path().to_path_buf(),
            };
            let mut store = RelationalStore::create(config.clone()).expect("create");
            store.create_table(table()).expect("table");
            store
                .create_index(
                    IndexDefinition {
                        id: IndexId(15),
                        table: TableId(11),
                        columns: vec![ColumnId(2)],
                        unique: true,
                    },
                    &mut NoFaults,
                )
                .expect("index");
            store
                .insert(
                    TableId(11),
                    Row {
                        primary: Key::new(11, point as u64 + 100),
                        values: vec![Value::U64(13), Value::Text("matrix".to_owned())],
                    },
                    &mut NoFaults,
                )
                .expect("insert");

            assert!(store.checkpoint(&mut FailOnce::at([point])).is_err());
            drop(store);

            let reopened = RelationalStore::open(config, &mut NoFaults)
                .unwrap_or_else(|error| panic!("reopen after {point:?}: {error}"));
            assert_eq!(reopened.catalog.generation(), 2);
            assert_eq!(reopened.database.secondary_index_ids(), vec![IndexId(15)]);
            assert_eq!(
                reopened
                    .scan(TableId(11), reopened.database.commit_id(), 10)
                    .expect("scan")
                    .len(),
                1
            );
        }
    }

    #[test]
    fn missing_or_malformed_durable_catalog_refuses_recovery() {
        for malformed in [false, true] {
            let directory = tempfile::tempdir().expect("tempdir");
            let config = DatabaseConfig {
                directory: directory.path().to_path_buf(),
            };
            let mut database = Database::create(config.clone()).expect("create database");
            database
                .commit(
                    vec![Mutation::Put {
                        key: if malformed {
                            Key::new(u64::MAX, 0)
                        } else {
                            Key::new(11, 1)
                        },
                        value: if malformed {
                            vec![1]
                        } else {
                            b"row without catalog".to_vec()
                        },
                    }],
                    &mut NoFaults,
                )
                .expect("write raw history");
            drop(database);

            assert!(matches!(
                RelationalStore::open(config, &mut NoFaults),
                Err(DbError::Corruption {
                    artifact: "catalog",
                    ..
                })
            ));
        }
    }

    #[test]
    fn catalog_payload_requires_a_supported_versioned_envelope() {
        let encoded = encode_catalog(&Catalog::default()).expect("encode empty catalog");
        assert_eq!(&encoded[..4], b"DBCT");
        assert_eq!(u16::from_le_bytes([encoded[4], encoded[5]]), 5);
        assert_eq!(
            decode_catalog(&encoded).expect("decode catalog"),
            Catalog::default()
        );

        let mut unsupported = encoded.clone();
        unsupported[4..6].copy_from_slice(&6_u16.to_le_bytes());
        assert!(matches!(
            decode_catalog(&unsupported),
            Err(DbError::Corruption { artifact: "catalog", reason })
                if reason == "unsupported catalog version"
        ));

        let legacy = encoded[6..].to_vec();
        assert!(matches!(
            decode_catalog(&legacy),
            Err(DbError::Corruption { artifact: "catalog", reason })
                if reason == "unknown catalog format"
        ));

        let mut empty_name = Catalog::default();
        empty_name.tables.insert(
            TableId(1),
            TableDefinition {
                id: TableId(1),
                name: "users".to_owned(),
                columns: vec![ColumnDefinition {
                    id: ColumnId(1),
                    data_type: ColumnType::I64,
                    nullable: false,
                    name: "id".to_owned(),
                }],
            },
        );
        empty_name.indexes.insert(
            IndexId(1),
            IndexDefinition {
                id: IndexId(1),
                table: TableId(1),
                columns: vec![ColumnId(1)],
                unique: false,
            },
        );
        empty_name.index_names.insert(IndexId(1), String::new());
        let encoded_empty_name = encode_catalog(&empty_name).expect("encode empty index name");
        assert!(matches!(
            decode_catalog(&encoded_empty_name),
            Err(DbError::Corruption { artifact: "catalog", reason })
                if reason == "empty index name"
        ));
    }

    #[test]
    fn schema_jobs_run_through_reactor_and_fail_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut store = RelationalStore::create(crate::store::DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create");
        let mut reactor = Reactor::new(ReactorConfig {
            workers: 1,
            governor: GovernorConfig {
                capacity: 4,
                protected_reserve: 1,
                max_queue_per_class: 4,
                max_in_flight: 1,
                overload_policy: crate::runtime::OverloadPolicy::default(),
            },
            demotion_after: None,
        })
        .expect("reactor");
        let create = store
            .submit_schema_job(
                &mut reactor,
                SchemaChange::CreateTable(table()),
                None,
                &mut NoFaults,
            )
            .expect("submit table job");
        let dispatch = reactor.dispatch(0).expect("dispatch table job");
        assert_eq!(
            store
                .run_schema_job(&mut reactor, &dispatch, create, &mut NoFaults)
                .expect("run table job"),
            SchemaJobState::Completed
        );
        assert_eq!(
            store.schema_job_status(create),
            Some(SchemaJobState::Completed)
        );
        assert!(store.catalog.table(TableId(11)).is_ok());

        let failed = store
            .submit_schema_job(
                &mut reactor,
                SchemaChange::CreateIndex(IndexDefinition {
                    id: IndexId(99),
                    table: TableId(404),
                    columns: vec![ColumnId(1)],
                    unique: false,
                }),
                None,
                &mut NoFaults,
            )
            .expect("submit failed job");
        let dispatch = reactor.dispatch(0).expect("dispatch failed job");
        assert!(
            store
                .run_schema_job(&mut reactor, &dispatch, failed, &mut NoFaults)
                .is_err()
        );
        assert!(matches!(
            store.schema_job_status(failed),
            Some(SchemaJobState::Failed(_))
        ));
        assert_eq!(reactor.busy_workers(), 0);
    }
}
