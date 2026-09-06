//! Logical dump and restore.
//!
//! `dump_sql` renders one read-consistent snapshot of the catalog, data,
//! secondary indexes, and foreign keys as plain SQL that restores into
//! OmenDB (and, within the documented divergences, into PostgreSQL):
//! the data-in/data-out trust gate. `restore_sql` replays such a dump.
//!
//! Format contract (same text forms as the wire tier):
//! - tables in catalog ID order, `CREATE TABLE` with the primary key
//!   inline (the primary-key order is rebuilt by CREATE, never dumped
//!   as a separate index);
//! - data as multi-row `INSERT`s in scan (identity-key) order;
//! - named secondary indexes and `ADD CONSTRAINT UNIQUE` after data;
//! - foreign keys last as `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN
//!   KEY`, so any reference direction loads;
//! - literals: NULL/TRUE/FALSE/integers bare; text quoted with `''`
//!   escaping; floats/dates/timestamps/decimals/UUIDs quoted text
//!   (the shared input grammar parses all of these); bytes as
//!   PostgreSQL bytea hex `'\x...'`.
//!
//! Documented divergences from PostgreSQL: U64 columns dump as
//! `NUMERIC(20,0)` (PostgreSQL has no unsigned 64-bit integer), and
//! unnamed typed-API indexes gain a generated name on restore.

use std::fmt::Write as _;

use crate::relational::ColumnType;
use crate::{DbError, RelationalDatabase, Result, Value};

/// Maximum rows per emitted INSERT statement.
const ROWS_PER_INSERT: usize = 100;

/// Render the whole database (catalog, data, indexes, foreign keys) as
/// one SQL script from a single consistent read snapshot.
pub fn dump_sql(database: &mut RelationalDatabase) -> Result<String> {
    let (dump, _) = database.transaction(|store, transaction| {
        let mut out = String::new();
        let catalog = store.catalog();

        // Section 1: tables with inline primary keys.
        let tables: Vec<crate::TableDefinition> = catalog.tables().cloned().collect();
        for table in &tables {
            write_create_table(&mut out, catalog, table)?;
        }

        // Section 2: data in scan order.
        for table in &tables {
            let rows = transaction.scan(store, table.id, usize::MAX)?;
            write_inserts(&mut out, table, &rows)?;
        }

        // Section 3: named secondary indexes and UNIQUE constraints.
        // The primary-key order is created inline by CREATE TABLE and is
        // identified by matching the primary-key columns.
        for table in &tables {
            let primary_columns = catalog.primary_key(table.id);
            for index in catalog.indexes_for(table.id) {
                let index_is_plain_columns = primary_columns.is_some_and(|primary| {
                    index.parts.len() == primary.len()
                        && index.parts.iter().all(|part| {
                            part.as_column()
                                .is_some_and(|column| primary.contains(&column))
                        })
                });
                if index_is_plain_columns {
                    continue;
                }
                let name = catalog
                    .index_name(index.id)
                    .map(str::to_owned)
                    .unwrap_or_else(|| generated_index_name(table, index.id.0));
                let unique = if index.unique { "UNIQUE " } else { "" };
                let columns = index
                    .parts
                    .iter()
                    .map(|part| match part {
                        crate::IndexKeyPart::Column(column) => {
                            quote_identifier(&column_name(table, *column))
                        }
                        crate::IndexKeyPart::Expression(expression) => {
                            expression.to_sql_text(table).expect("valid table")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                // A partial index carries its WHERE clause so the
                // restore rebuilds the same filtered semantics.
                let predicate = index.predicate.as_ref().map(|predicate| {
                    let terms = predicate
                        .terms
                        .iter()
                        .map(|(column, value)| {
                            format!(
                                "{} = {}",
                                quote_identifier(&column_name(table, *column)),
                                literal_text(value)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    format!(" WHERE {terms}")
                });
                let _ = writeln!(
                    out,
                    "CREATE {unique}INDEX {} ON {} ({columns}){};",
                    quote_identifier(&name),
                    quote_identifier(&table.name),
                    predicate.unwrap_or_default(),
                );
            }
        }

        // Section 4: foreign keys last, after every referenced table has
        // its rows and covering indexes.
        for foreign_key in catalog.foreign_keys() {
            let child = catalog.table(foreign_key.table)?;
            let parent = catalog.table(foreign_key.referenced_table)?;
            let name = catalog
                .foreign_key_name(foreign_key.id)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("fk_{}", foreign_key.id.0));
            let child_columns = foreign_key
                .columns
                .iter()
                .map(|column| quote_identifier(&column_name(child, *column)))
                .collect::<Vec<_>>()
                .join(", ");
            let parent_columns = foreign_key
                .referenced_columns
                .iter()
                .map(|column| quote_identifier(&column_name(parent, *column)))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = writeln!(
                out,
                "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({child_columns}) REFERENCES {} ({parent_columns});",
                quote_identifier(&child.name),
                quote_identifier(&name),
                quote_identifier(&parent.name),
            );
        }
        Ok::<String, DbError>(out)
    })?;
    Ok(dump)
}

/// Replay a dump (or any supported SQL script) into the database.
/// Schema statements and constraints publish through their direct atomic
/// paths; consecutive INSERT statements are grouped into atomic batches
/// (up to [`crate::RELATIONAL_SQL_BATCH_LIMIT`]) so a failing chunk
/// rolls back without tearing the restored schema. Restore stops at the
/// first error; earlier committed chunks remain.
pub fn restore_sql(database: &mut RelationalDatabase, source: &str) -> Result<()> {
    let statements = split_statements(source)?;
    let mut pending: Vec<String> = Vec::new();
    for statement in statements {
        let is_insert = statement
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("INSERT");
        if is_insert {
            pending.push(statement);
            if pending.len() >= crate::RELATIONAL_SQL_BATCH_LIMIT {
                flush_inserts(database, &pending)?;
                pending.clear();
            }
        } else {
            flush_inserts(database, &pending)?;
            pending.clear();
            database.execute_sql(&statement)?;
        }
    }
    flush_inserts(database, &pending)?;
    Ok(())
}

fn flush_inserts(database: &mut RelationalDatabase, pending: &[String]) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let statements: Vec<&str> = pending.iter().map(String::as_str).collect();
    database.execute_sql_batch(&statements).map(|_| ())
}

// ---- dump rendering ---------------------------------------------------------

fn write_create_table(
    out: &mut String,
    catalog: &crate::relational::Catalog,
    table: &crate::TableDefinition,
) -> Result<()> {
    let mut columns = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        let null = if column.nullable { "" } else { " NOT NULL" };
        columns.push(format!(
            "{} {}{null}",
            quote_identifier(&column.name),
            sql_type_name(column.data_type),
        ));
    }
    if let Some(primary) = catalog.primary_key(table.id) {
        let names = primary
            .iter()
            .map(|column| quote_identifier(&column_name(table, *column)))
            .collect::<Vec<_>>()
            .join(", ");
        columns.push(format!("PRIMARY KEY ({names})"));
    }
    let _ = writeln!(
        out,
        "CREATE TABLE {} ({});",
        quote_identifier(&table.name),
        columns.join(", "),
    );
    Ok(())
}

fn write_inserts(
    out: &mut String,
    table: &crate::TableDefinition,
    rows: &[crate::Row],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let column_list = table
        .columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    for chunk in rows.chunks(ROWS_PER_INSERT) {
        let values = chunk
            .iter()
            .map(|row| render_row_values(table, row))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let _ = writeln!(
            out,
            "INSERT INTO {} ({column_list}) VALUES {values};",
            quote_identifier(&table.name),
        );
    }
    Ok(())
}

fn render_row_values(table: &crate::TableDefinition, row: &crate::Row) -> Result<String> {
    let mut rendered = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        let value = row
            .values
            .get(
                table
                    .columns
                    .iter()
                    .position(|candidate| candidate.id == column.id)
                    .expect("column position"),
            )
            .ok_or_else(|| {
                DbError::InvalidState(format!("row is missing column {}", column.name))
            })?;
        rendered.push(render_value(value));
    }
    Ok(format!("({})", rendered.join(", ")))
}

/// One value in INSERT-literal form. Quoted text drives every typed
/// conversion (the shared input grammar parses dates, timestamps,
/// decimals, floats, and UUIDs from text), so a dump restores through
/// the exact surface SQL clients use.
fn render_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(inner) => {
            if *inner {
                "TRUE".to_owned()
            } else {
                "FALSE".to_owned()
            }
        }
        Value::I64(inner) => inner.to_string(),
        Value::U64(inner) => inner.to_string(),
        Value::Text(inner) => quote_string(inner),
        Value::Bytes(inner) => format!("'\\x{}'", hex_encode(inner)),
        Value::Float64(inner) => quote_string(&crate::sql_types::float_text(inner.0)),
        Value::Date(inner) => quote_string(&crate::sql_types::date_text(inner.0)),
        Value::Timestamp(inner) => quote_string(&crate::sql_types::timestamp_text(inner.0)),
        Value::Decimal(inner) => quote_string(&crate::sql_types::decimal_text(inner)),
        Value::Uuid(inner) => quote_string(&inner.format()),
    }
}

fn sql_type_name(data_type: ColumnType) -> &'static str {
    match data_type {
        ColumnType::Bytes => "BYTEA",
        ColumnType::Bool => "BOOLEAN",
        ColumnType::I64 => "BIGINT",
        // PostgreSQL has no unsigned 64-bit integer; NUMERIC(20,0)
        // holds every u64 exactly. Documented divergence.
        ColumnType::U64 => "NUMERIC(20,0)",
        ColumnType::Text => "TEXT",
        ColumnType::Float64 => "DOUBLE PRECISION",
        ColumnType::Date => "DATE",
        ColumnType::Timestamp => "TIMESTAMP",
        ColumnType::Decimal => "NUMERIC",
        ColumnType::Uuid => "UUID",
    }
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_string(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Render one stored predicate value as a SQL literal.
fn literal_text(value: &crate::Value) -> String {
    match value {
        crate::Value::Null => "NULL".to_owned(),
        crate::Value::Bool(value) => {
            if *value {
                "TRUE".to_owned()
            } else {
                "FALSE".to_owned()
            }
        }
        crate::Value::I64(value) => value.to_string(),
        crate::Value::U64(value) => value.to_string(),
        crate::Value::Text(value) => quote_string(value),
        crate::Value::Float64(value) => value.0.to_string(),
        other => format!("{other:?}"),
    }
}

fn generated_index_name(table: &crate::TableDefinition, index_id: u64) -> String {
    format!("{}_ix_{}", table.name, index_id)
}

fn column_name(table: &crate::TableDefinition, column: crate::ColumnId) -> String {
    table
        .columns
        .iter()
        .find(|candidate| candidate.id == column)
        .map(|candidate| candidate.name.clone())
        .unwrap_or_else(|| format!("col_{}", column.0))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

// ---- restore: quote-aware statement splitting --------------------------------

/// Split a script into statements on `;` outside single-quoted strings
/// and double-quoted identifiers. Leading/trailing whitespace around
/// each statement is trimmed; empty statements are dropped.
fn split_statements(source: &str) -> Result<Vec<String>> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = source.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\'' => {
                current.push('\'');
                loop {
                    match chars.next() {
                        Some('\'') if chars.peek() == Some(&'\'') => {
                            current.push_str("''");
                            chars.next();
                        }
                        Some('\'') => {
                            current.push('\'');
                            break;
                        }
                        Some(other) => current.push(other),
                        None => {
                            return Err(DbError::InvalidState(
                                "unterminated string literal in dump".to_owned(),
                            ));
                        }
                    }
                }
            }
            '"' => {
                current.push('"');
                loop {
                    match chars.next() {
                        Some('"') if chars.peek() == Some(&'"') => {
                            current.push_str("\"\"");
                            chars.next();
                        }
                        Some('"') => {
                            current.push('"');
                            break;
                        }
                        Some(other) => current.push(other),
                        None => {
                            return Err(DbError::InvalidState(
                                "unterminated quoted identifier in dump".to_owned(),
                            ));
                        }
                    }
                }
            }
            ';' => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_owned());
                }
                current.clear();
            }
            other => current.push(other),
        }
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_owned());
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitting_respects_quotes_and_doubled_separators() {
        let script = "SELECT 'a;''b' AS \"x;y\"; INSERT; ;;;\nSELECT 2;";
        let statements = split_statements(script).expect("quote-aware split of mixed statements");
        assert_eq!(
            statements,
            vec!["SELECT 'a;''b' AS \"x;y\"", "INSERT", "SELECT 2",]
        );
    }

    #[test]
    fn unterminated_string_is_an_error() {
        assert!(split_statements("SELECT 'oops;").is_err());
    }
}
