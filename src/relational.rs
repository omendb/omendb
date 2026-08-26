use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::model::{IndexId, Key};
use crate::row_identity::{RowIdentity, decode_legacy_key, encode_legacy_key};
use crate::{DbError, Result};

const ROW_MAGIC: [u8; 4] = *b"DBRW";
const ROW_VERSION: u8 = 1;
const CATALOG_MAGIC: [u8; 4] = *b"DBCT";
/// Maximum cascade generations per triggering delete statement.
pub(crate) const MAX_CASCADE_DEPTH: usize = 64;

const CATALOG_VERSION: u16 = 5;
const CATALOG_KEY_TENANT: u64 = u64::MAX;

// The temporary kernel is still a OmenDB-owned byte store. Keep the
// relational catalog in its reserved keyspace so schema bytes share the same
// WAL, checkpoint, snapshot, and recovery boundary as rows and indexes.

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

fn ensure_table_key(table: TableId, key: Key) -> Result<()> {
    if key.0[..8] != table.0.to_be_bytes() {
        return Err(DbError::InvalidState(format!(
            "row key does not belong to table {}",
            table.0
        )));
    }
    Ok(())
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
