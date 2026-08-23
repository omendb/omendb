use sqlparser::ast::{
    AlterTable, AlterTableOperation, ColumnOption, CreateIndex, CreateTable, DataType,
    NullsDistinctOption, TableConstraint,
};

use crate::{
    ColumnDefinition, ColumnId, ColumnType, CommitId, ConstraintId, DbError, IndexId,
    NamedForeignKeyDefinition, NamedIndexDefinition, RelationalDatabase,
    RelationalSchemaDefinition, Result, TableDefinition, TableId,
};

use super::{column_position, find_table, simple_object_name, unsupported};

pub(super) fn execute_create_index(
    database: &mut RelationalDatabase,
    create: &CreateIndex,
) -> Result<CommitId> {
    let name = create
        .name
        .as_ref()
        .ok_or_else(|| unsupported("CREATE INDEX", "an explicit index name is required"))?;
    if create.using.is_some()
        || create.concurrently
        || create.if_not_exists
        || !create.include.is_empty()
        || create.nulls_distinct.is_some()
        || !create.with.is_empty()
        || create.predicate.is_some()
        || !create.index_options.is_empty()
        || !create.alter_options.is_empty()
    {
        return Err(unsupported(
            "CREATE INDEX",
            "only a plain non-concurrent index over named columns is supported",
        ));
    }
    let index_name = simple_object_name(name, "index")?.to_owned();
    let table_name = simple_object_name(&create.table_name, "table")?;
    let (table_id, columns, index_id) = {
        let table = find_table(database.catalog(), table_name)?;
        let mut columns = Vec::with_capacity(create.columns.len());
        for index_column in &create.columns {
            if index_column.operator_class.is_some()
                || index_column.column.options.asc == Some(false)
                || index_column.column.options.nulls_first.is_some()
                || index_column.column.with_fill.is_some()
            {
                return Err(unsupported(
                    "CREATE INDEX",
                    "only ascending plain column keys are supported",
                ));
            }
            let position = column_position(table, &index_column.column.expr)?.ok_or_else(|| {
                DbError::InvalidState(format!(
                    "index column {} does not exist",
                    index_column.column.expr
                ))
            })?;
            let column = table.columns.get(position).ok_or_else(|| {
                DbError::InvalidState("index column position is invalid".to_owned())
            })?;
            columns.push(column.id);
        }
        let index_id = database
            .catalog()
            .indexes()
            .map(|index| index.id.0)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| DbError::InvalidState("no SQL index ID is available".to_owned()))?;
        (table.id, columns, IndexId(index_id))
    };
    database.create_named_index(
        crate::IndexDefinition {
            id: index_id,
            table: table_id,
            columns,
            unique: create.unique,
        },
        index_name,
    )
}

pub(super) fn execute_alter_table(
    database: &mut RelationalDatabase,
    alter: &AlterTable,
) -> Result<CommitId> {
    if alter.if_exists
        || alter.only
        || alter.location.is_some()
        || alter.on_cluster.is_some()
        || alter.table_type.is_some()
        || alter.operations.len() != 1
    {
        return Err(unsupported(
            "ALTER TABLE",
            "only one plain nullable ADD COLUMN operation is supported",
        ));
    }
    let table_name = simple_object_name(&alter.name, "table")?;
    let operation = &alter.operations[0];
    let AlterTableOperation::AddColumn {
        if_not_exists,
        column_def,
        column_position,
        ..
    } = operation
    else {
        return Err(unsupported(
            "ALTER TABLE",
            "only nullable ADD COLUMN is supported",
        ));
    };
    if *if_not_exists || column_position.is_some() {
        return Err(unsupported(
            "ALTER TABLE",
            "ADD COLUMN IF NOT EXISTS and column placement are not supported",
        ));
    }
    let mut nullable = true;
    for option in &column_def.options {
        if option.name.is_some() {
            return Err(unsupported(
                "ALTER TABLE",
                "named column constraints are not supported",
            ));
        }
        match &option.option {
            ColumnOption::Null => nullable = true,
            ColumnOption::NotNull => nullable = false,
            _ => {
                return Err(unsupported(
                    "ALTER TABLE",
                    "only a nullable column without a default or constraint is supported",
                ));
            }
        }
    }
    if !nullable {
        return Err(unsupported(
            "ALTER TABLE",
            "new columns must be nullable until backfill and validation exist",
        ));
    }

    let (table_id, column_id) = {
        let table = find_table(database.catalog(), table_name)?;
        let next_column = table
            .columns
            .iter()
            .map(|column| column.id.0)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| DbError::InvalidState("no SQL column ID is available".to_owned()))?;
        (table.id, ColumnId(next_column))
    };
    database.add_nullable_column(
        table_id,
        ColumnDefinition {
            id: column_id,
            name: column_def.name.value.clone(),
            data_type: sql_data_type(&column_def.data_type)?,
            nullable: true,
        },
    )
}

pub(super) fn execute_create_table(
    database: &mut RelationalDatabase,
    create: &CreateTable,
) -> Result<CommitId> {
    if create.or_replace
        || create.temporary
        || create.external
        || create.dynamic
        || create.global.is_some()
        || create.if_not_exists
        || create.transient
        || create.volatile
        || create.iceberg
        || create.snapshot
        || create.query.is_some()
        || create.like.is_some()
        || create.clone.is_some()
        || create.without_rowid
        || create.table_options.to_string() != ""
        || create.file_format.is_some()
        || create.location.is_some()
        || create.version.is_some()
        || create.comment.is_some()
        || create.on_commit.is_some()
        || create.on_cluster.is_some()
        || create.order_by.is_some()
        || create.partition_by.is_some()
        || create.cluster_by.is_some()
        || create.clustered_by.is_some()
        || create.inherits.is_some()
        || create.partition_of.is_some()
    {
        return Err(unsupported(
            "CREATE TABLE",
            "only a single plain table with scalar columns is supported",
        ));
    }
    let name = simple_object_name(&create.name, "table")?.to_owned();
    if create.columns.is_empty() {
        return Err(DbError::InvalidState(
            "SQL tables must define at least one column".to_owned(),
        ));
    }

    let mut columns = Vec::with_capacity(create.columns.len());
    let mut column_primary = None;
    for (position, column) in create.columns.iter().enumerate() {
        let mut nullable = true;
        let mut primary = false;
        for option in &column.options {
            match &option.option {
                ColumnOption::Null => nullable = true,
                ColumnOption::NotNull => nullable = false,
                ColumnOption::PrimaryKey(constraint) => {
                    if option.name.is_some()
                        || constraint.name.is_some()
                        || constraint.index_name.is_some()
                        || constraint.index_type.is_some()
                        || !constraint.index_options.is_empty()
                        || constraint.characteristics.is_some()
                    {
                        return Err(unsupported(
                            "CREATE TABLE",
                            "only an unnamed column-level PRIMARY KEY is supported",
                        ));
                    }
                    if primary || column_primary.is_some() {
                        return Err(DbError::InvalidState(
                            "CREATE TABLE defines more than one PRIMARY KEY".to_owned(),
                        ));
                    }
                    primary = true;
                    nullable = false;
                    column_primary =
                        Some(ColumnId(u16::try_from(position + 1).map_err(|_| {
                            DbError::InvalidState("too many SQL columns".to_owned())
                        })?));
                }
                ColumnOption::Unique(_) | ColumnOption::ForeignKey(_) => {
                    return Err(unsupported(
                        "CREATE TABLE",
                        "use table-level UNIQUE or FOREIGN KEY constraints",
                    ));
                }
                _ => {
                    return Err(unsupported(
                        "CREATE TABLE",
                        "only NULL, NOT NULL, and a single-column PRIMARY KEY are supported",
                    ));
                }
            }
        }
        columns.push(ColumnDefinition {
            id: ColumnId(
                u16::try_from(position + 1)
                    .map_err(|_| DbError::InvalidState("too many SQL columns".to_owned()))?,
            ),
            name: column.name.value.clone(),
            data_type: sql_data_type(&column.data_type)?,
            nullable,
        });
    }

    // System (catalog-owned wire/auth) tables occupy the very top of the
    // ID space; SQL-tier creation grows within the user band below them.
    const SYSTEM_TABLE_ID_FLOOR: u64 = u64::MAX - 15;
    let user_tables_max = database
        .catalog()
        .tables()
        .map(|table| table.id.0)
        .filter(|id| *id < SYSTEM_TABLE_ID_FLOOR)
        .max()
        .unwrap_or(0);
    let mut table_id = user_tables_max
        .checked_add(1)
        .ok_or_else(|| DbError::InvalidState("no SQL table ID is available".to_owned()))?;
    while database.catalog().tables().any(|table| table.id.0 == table_id) {
        table_id += 1;
        if table_id >= SYSTEM_TABLE_ID_FLOOR {
            return Err(DbError::InvalidState("no SQL table ID is available".to_owned()));
        }
    }
    let mut table = TableDefinition {
        id: TableId(table_id),
        name,
        columns,
    };
    table.columns.first().cloned().ok_or_else(|| {
        DbError::InvalidState("SQL tables must define at least one column".to_owned())
    })?;
    let mut named_indexes = Vec::new();
    let mut next_index_id = database
        .catalog()
        .indexes()
        .map(|index| index.id.0)
        .max()
        .unwrap_or(0);
    let mut next_constraint_id = database
        .catalog()
        .foreign_keys()
        .map(|foreign_key| foreign_key.id.0)
        .max()
        .unwrap_or(0);
    let mut primary_seen = false;
    let mut primary_columns = column_primary.map(|column| vec![column]);
    let mut primary_name = None;
    let mut foreign_key_specs = Vec::new();
    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey(primary) => {
                if primary_seen || column_primary.is_some() {
                    return Err(DbError::InvalidState(
                        "CREATE TABLE defines more than one PRIMARY KEY".to_owned(),
                    ));
                }
                if primary.index_type.is_some()
                    || primary.index_name.is_some()
                    || !primary.index_options.is_empty()
                    || primary.characteristics.is_some()
                {
                    return Err(unsupported(
                        "CREATE TABLE",
                        "only an immediate PRIMARY KEY over plain columns is supported",
                    ));
                }
                let columns = simple_index_columns(&table, &primary.columns, "PRIMARY KEY")?;
                for column_id in &columns {
                    let column = table
                        .columns
                        .iter_mut()
                        .find(|column| column.id == *column_id)
                        .ok_or_else(|| {
                            DbError::InvalidState("primary-key column is missing".to_owned())
                        })?;
                    column.nullable = false;
                }
                primary_columns = Some(columns);
                primary_name =
                    primary_object_name(primary.name.as_ref(), primary.index_name.as_ref())?;
                primary_seen = true;
            }
            TableConstraint::Unique(unique) => {
                if unique.index_type.is_some()
                    || !unique.index_options.is_empty()
                    || unique.characteristics.is_some()
                    || unique.nulls_distinct != NullsDistinctOption::None
                {
                    return Err(unsupported(
                        "CREATE TABLE",
                        "only plain immediate UNIQUE constraints are supported",
                    ));
                }
                next_index_id = next_index_id.checked_add(1).ok_or_else(|| {
                    DbError::InvalidState("no SQL index ID is available".to_owned())
                })?;
                named_indexes.push(NamedIndexDefinition {
                    definition: crate::IndexDefinition {
                        id: IndexId(next_index_id),
                        table: table.id,
                        columns: simple_index_columns(&table, &unique.columns, "UNIQUE")?,
                        unique: true,
                    },
                    name: primary_object_name(unique.name.as_ref(), unique.index_name.as_ref())?,
                });
            }
            TableConstraint::ForeignKey(foreign_key) => {
                if foreign_key.index_name.is_some()
                    || foreign_key.on_delete.is_some()
                    || foreign_key.on_update.is_some()
                    || foreign_key.match_kind.is_some()
                    || foreign_key.characteristics.is_some()
                    || foreign_key.columns.is_empty()
                    || foreign_key.columns.len() != foreign_key.referred_columns.len()
                {
                    return Err(unsupported(
                        "CREATE TABLE",
                        "only immediate foreign keys with explicit columns are supported",
                    ));
                }
                let foreign_table_name = simple_object_name(&foreign_key.foreign_table, "table")?;
                let referenced_table = if table.name == foreign_table_name {
                    &table
                } else {
                    find_table(database.catalog(), foreign_table_name)?
                };
                let columns = foreign_key
                    .columns
                    .iter()
                    .map(|column| column_id_by_name(&table, &column.value, "foreign-key"))
                    .collect::<Result<Vec<_>>>()?;
                let referenced_columns = foreign_key
                    .referred_columns
                    .iter()
                    .map(|column| column_id_by_name(referenced_table, &column.value, "referenced"))
                    .collect::<Result<Vec<_>>>()?;
                next_constraint_id = next_constraint_id.checked_add(1).ok_or_else(|| {
                    DbError::InvalidState("no SQL constraint ID is available".to_owned())
                })?;
                foreign_key_specs.push(NamedForeignKeyDefinition {
                    definition: crate::ForeignKeyDefinition {
                        id: ConstraintId(next_constraint_id),
                        table: table.id,
                        columns,
                        referenced_table: referenced_table.id,
                        referenced_columns,
                        on_delete: crate::ReferentialAction::default(),
                        timing: crate::ConstraintTiming::default(),
                    },
                    name: foreign_key.name.as_ref().map(|name| name.value.clone()),
                });
            }
            _ => {
                return Err(unsupported(
                    "CREATE TABLE",
                    "only PRIMARY KEY, UNIQUE, and immediate FOREIGN KEY constraints are supported",
                ));
            }
        }
    }

    if column_primary.is_none() && !primary_seen {
        return Err(DbError::InvalidState(
            "SQL tables must define a PRIMARY KEY".to_owned(),
        ));
    }

    let primary_columns = primary_columns
        .ok_or_else(|| DbError::InvalidState("SQL tables must define a PRIMARY KEY".to_owned()))?;
    next_index_id = next_index_id
        .checked_add(1)
        .ok_or_else(|| DbError::InvalidState("no SQL index ID is available".to_owned()))?;
    named_indexes.insert(
        0,
        NamedIndexDefinition {
            definition: crate::IndexDefinition {
                id: IndexId(next_index_id),
                table: table.id,
                columns: primary_columns.clone(),
                unique: true,
            },
            name: primary_name,
        },
    );

    database.create_table_with_schema_and_primary_key(
        table,
        Some(primary_columns),
        RelationalSchemaDefinition {
            indexes: named_indexes,
            foreign_keys: foreign_key_specs,
        },
    )
}

fn primary_object_name(
    constraint_name: Option<&sqlparser::ast::Ident>,
    index_name: Option<&sqlparser::ast::Ident>,
) -> Result<Option<String>> {
    match (constraint_name, index_name) {
        (Some(constraint), Some(index)) if constraint.value != index.value => Err(unsupported(
            "CREATE TABLE",
            "constraint and index names must match",
        )),
        (Some(constraint), _) => Ok(Some(constraint.value.clone())),
        (_, Some(index)) => Ok(Some(index.value.clone())),
        (None, None) => Ok(None),
    }
}

fn column_id_by_name(table: &TableDefinition, name: &str, role: &str) -> Result<ColumnId> {
    table
        .columns
        .iter()
        .find(|column| column.name == name)
        .map(|column| column.id)
        .ok_or_else(|| DbError::InvalidState(format!("{role} column {name} does not exist")))
}

fn simple_index_columns(
    table: &TableDefinition,
    columns: &[sqlparser::ast::IndexColumn],
    statement: &'static str,
) -> Result<Vec<ColumnId>> {
    if columns.is_empty() {
        return Err(DbError::InvalidState(format!(
            "{statement} must define at least one column"
        )));
    }
    columns
        .iter()
        .map(|column| {
            if column.operator_class.is_some()
                || column.column.options.asc == Some(false)
                || column.column.options.nulls_first.is_some()
                || column.column.with_fill.is_some()
            {
                return Err(unsupported(
                    "CREATE TABLE",
                    "only ascending plain column keys are supported",
                ));
            }
            let Some(position) = column_position(table, &column.column.expr)? else {
                return Err(DbError::InvalidState(format!(
                    "{statement} column {} does not exist",
                    column.column.expr
                )));
            };
            Ok(table.columns[position].id)
        })
        .collect()
}

fn sql_data_type(data_type: &DataType) -> Result<ColumnType> {
    let display = data_type.to_string().to_ascii_uppercase();
    if display == "BOOLEAN" || display == "BOOL" {
        return Ok(ColumnType::Bool);
    }
    if display == "TEXT"
        || display.starts_with("VARCHAR")
        || display.starts_with("CHARACTER VARYING")
        || display.starts_with("CHAR VARYING")
        || display.starts_with("CHARACTER")
        || display.starts_with("CHAR(")
        || display == "CHAR"
    {
        return Ok(ColumnType::Text);
    }
    if display == "BYTEA"
        || display == "BLOB"
        || display.starts_with("BYTES")
        || display.starts_with("VARBINARY")
    {
        return Ok(ColumnType::Bytes);
    }
    if matches!(
        display.as_str(),
        "BIGINT" | "INT" | "INTEGER" | "INT2" | "INT4" | "INT8" | "SMALLINT"
    ) {
        return Ok(ColumnType::I64);
    }
    Err(unsupported(
        "CREATE TABLE",
        "supported types are BIGINT, INTEGER, BOOLEAN, TEXT, and byte strings",
    ))
}
