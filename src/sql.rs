//! The first embedded SQL adoption tier.
//!
//! This module is an adapter, not a second relational engine. The parser owns
//! SQL syntax; the catalog, row validation, transaction lifecycle, and
//! backend publication remain owned by the typed facade. Keeping that split
//! explicit keeps SQL evaluation above the SeerDB-backed relational facade.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Statement, Value as AstValue, visit_expressions};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::{
    Catalog, ColumnType, DbError, RelationalDatabase, RelationalDatabaseTransaction, Result,
    TableDefinition, Value,
};

#[path = "sql_query.rs"]
mod query;
#[path = "sql_schema.rs"]
mod schema;
#[path = "sql_values.rs"]
mod values;

pub(crate) mod typed_input {
    //! Shared input grammars and coercion rules for SQL literals and wire
    //! parameters.

    pub(crate) use super::values::{coerce_for_comparison, coerce_value};
    #[cfg(feature = "pgwire")]
    pub(crate) use super::values::{
        parse_date_parameter as date, parse_decimal_parameter as decimal,
        parse_float_parameter as float, parse_timestamp_parameter as timestamp,
    };
}
#[path = "sql_write.rs"]
mod write;

mod arithmetic;
pub(crate) mod binding;
mod control;
#[path = "sql/explain.rs"]
mod explain;

#[cfg(feature = "pgwire")]
pub(crate) use control::{TransactionControl, statement_sources};

#[path = "sql/window.rs"]
pub(crate) mod window;

mod result {
    use crate::{CommitId, Value};

    /// One column in an embedded SQL result.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SqlColumn {
        pub name: String,
        /// The bound SQL result type. Public execution always supplies it;
        /// internal operators may leave it unset while constructing a result.
        pub column_type: Option<crate::ColumnType>,
    }

    impl SqlColumn {
        pub(crate) fn typed(name: impl Into<String>, column_type: crate::ColumnType) -> Self {
            Self {
                name: name.into(),
                column_type: Some(column_type),
            }
        }
    }

    /// A bounded embedded SQL result.
    ///
    /// `commit` is the commit observed or published by the direct database
    /// method. It is `None` when a statement is executed inside a caller-owned
    /// typed transaction and becomes populated by the outer transaction result.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SqlResult {
        pub columns: Vec<SqlColumn>,
        pub rows: Vec<Vec<Value>>,
        pub affected_rows: usize,
        pub commit: Option<CommitId>,
    }

    impl SqlResult {
        pub(super) fn rows(columns: Vec<SqlColumn>, rows: Vec<Vec<Value>>) -> Self {
            Self {
                columns,
                rows,
                affected_rows: 0,
                commit: None,
            }
        }

        pub(super) fn command(affected_rows: usize) -> Self {
            Self {
                columns: Vec::new(),
                rows: Vec::new(),
                affected_rows,
                commit: None,
            }
        }
    }
}

pub use result::{SqlColumn, SqlResult};

pub(crate) fn execute_with_params(
    database: &mut RelationalDatabase,
    source: &str,
    params: &[Value],
) -> Result<SqlResult> {
    let bound = binding::bind(database, source, &parameter_hints(params))?;
    execute_bound(database, &bound, params)
}

pub(crate) fn validate_bound(
    database: &RelationalDatabase,
    bound: &binding::BoundStatement,
) -> Result<()> {
    if !matches!(bound.effect, binding::StatementEffect::Control(_))
        && bound.catalog_stamp != Some(database.catalog_stamp())
    {
        return Err(DbError::SqlPreparedStatementInvalidated);
    }
    Ok(())
}

pub(crate) fn parameter_hints(params: &[Value]) -> Vec<Option<ColumnType>> {
    params.iter().map(query::value_static_type).collect()
}

pub(crate) fn execute_bound(
    database: &mut RelationalDatabase,
    bound: &binding::BoundStatement,
    params: &[Value],
) -> Result<SqlResult> {
    validate_bound(database, bound)?;
    let statement = &bound.statement;
    validate_parameters(statement, params)?;
    if let Statement::CreateTable(create) = statement {
        let commit = schema::execute_create_table(database, create)?;
        return Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            commit: Some(commit),
        });
    }
    if let Statement::AlterTable(alter) = statement {
        let commit = schema::execute_alter_table(database, alter)?;
        return Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            commit: Some(commit),
        });
    }
    if let Statement::CreateIndex(create) = statement {
        let commit = schema::execute_create_index(database, create)?;
        return Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            commit: Some(commit),
        });
    }
    if let Statement::Drop { .. } = statement {
        let commit = schema::execute_drop(database, statement)?;
        return Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            commit: Some(commit),
        });
    }

    let (mut result, commit) = database
        .transaction(|store, transaction| transaction.execute_bound_sql(store, bound, params))?;
    result.commit = Some(commit);
    Ok(result)
}

pub(crate) fn execute_in_transaction_with_params(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    source: &str,
    params: &[Value],
) -> Result<SqlResult> {
    transaction.ensure_sql_catalog(database)?;
    let bound = binding::bind(database, source, &parameter_hints(params))?;
    execute_bound_in_transaction(database, transaction, &bound, params)
}

pub(crate) fn execute_bound_in_transaction(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    bound: &binding::BoundStatement,
    params: &[Value],
) -> Result<SqlResult> {
    transaction.ensure_sql_catalog(database)?;
    validate_bound(database, bound)?;
    validate_parameters(&bound.statement, params)?;
    let mut result =
        execute_in_transaction_statement(database, transaction, &bound.statement, params)?;
    normalize_rows(&mut result.rows, &bound.columns)?;
    result.columns = bound.columns.clone();
    Ok(result)
}

fn coerce_result_value(value: Value, column_type: ColumnType, name: &str) -> Result<Value> {
    let actual = query::value_static_type(&value);
    if actual == Some(column_type)
        || matches!(
            (actual, column_type),
            (
                Some(ColumnType::I64 | ColumnType::U64),
                ColumnType::I64 | ColumnType::U64
            )
        )
    {
        return Ok(value);
    }
    values::coerce_value(
        value,
        &crate::ColumnDefinition {
            id: crate::ColumnId(0),
            name: name.to_owned(),
            data_type: column_type,
            nullable: true,
        },
    )
}

fn normalize_rows(rows: &mut [Vec<Value>], columns: &[SqlColumn]) -> Result<()> {
    for row in rows {
        if row.len() != columns.len() {
            return Err(DbError::InvalidState(
                "SQL executor violated the bound result width".to_owned(),
            ));
        }
        for (value, column) in row.iter_mut().zip(columns) {
            let column_type = column.column_type.ok_or_else(|| {
                DbError::InvalidState("SQL binding omitted a result type".to_owned())
            })?;
            *value = coerce_result_value(
                std::mem::replace(value, Value::Null),
                column_type,
                &column.name,
            )?;
        }
    }
    Ok(())
}

fn execute_in_transaction_statement(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    statement: &Statement,
    params: &[Value],
) -> Result<SqlResult> {
    if control::bind(statement)?.is_some() {
        return Err(unsupported(
            "transaction control",
            "use the typed transaction closure",
        ));
    }
    match statement {
        Statement::Explain {
            statement: explained,
            analyze,
            ..
        } => {
            if *analyze {
                return Err(unsupported(
                    "EXPLAIN",
                    "EXPLAIN ANALYZE is not supported; run the statement and time it",
                ));
            }
            let plan = explain::explain_statement(database, explained, params)?;
            Ok(SqlResult {
                columns: plan.columns,
                rows: plan.rows,
                affected_rows: 0,
                commit: None,
            })
        }
        Statement::ShowVariable { variable } => {
            let name = variable
                .iter()
                .map(|identifier| identifier.value.clone())
                .collect::<Vec<_>>()
                .join(" ")
                .to_lowercase();
            let value = match name.as_str() {
                "server_version" => env!("CARGO_PKG_VERSION").to_owned(),
                "server_encoding" | "client_encoding" => "UTF8".to_owned(),
                "transaction_isolation" => "serializable".to_owned(),
                "standard_conforming_strings" => "on".to_owned(),
                "max_connections" => "128".to_owned(),
                "search_path" => "public".to_owned(),
                other => {
                    let reason = format!(
                        "unsupported configuration parameter {other}; supported parameters are server_version, server_encoding, client_encoding, transaction_isolation, standard_conforming_strings, max_connections, search_path"
                    );
                    return Err(unsupported("SHOW", &reason));
                }
            };
            Ok(SqlResult::rows(
                vec![SqlColumn::typed(name, crate::ColumnType::Text)],
                vec![vec![Value::Text(value)]],
            ))
        }
        Statement::Query(query) => query::execute_query(database, transaction, query, params),
        Statement::Insert(insert) => write::execute_insert(database, transaction, insert, params),
        Statement::Update(update) => write::execute_update(database, transaction, update, params),
        Statement::Delete(delete) => write::execute_delete(database, transaction, delete, params),
        Statement::CreateTable(_) => Err(unsupported(
            "CREATE TABLE",
            "schema changes must use the direct database method",
        )),
        Statement::AlterTable(_) => Err(unsupported(
            "ALTER TABLE",
            "schema changes must use the direct database method",
        )),
        Statement::CreateIndex(_) => Err(unsupported(
            "CREATE INDEX",
            "schema changes must use the direct database method",
        )),
        Statement::Drop { .. } => Err(unsupported(
            "DROP",
            "schema changes must use the direct database method",
        )),
        other => Err(unsupported(
            statement_kind(other),
            "not in the embedded SQL tier",
        )),
    }
}

/// Infer parameter domains using the same binding used by execution.
pub(crate) fn describe_parameters(
    database: &RelationalDatabase,
    source: &str,
) -> Result<Vec<Option<ColumnType>>> {
    Ok(binding::bind(database, source, &[])?.parameter_types)
}

// Cache only syntax, by exact source. Every binding still resolves against the
// current catalog; typed/coerced ASTs are private to their retained binding.
const PARSE_CACHE_LIMIT: usize = 1024;

fn parse_cache() -> &'static std::sync::Mutex<HashMap<String, Statement>> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Statement>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub(super) fn parse_statement(sql: &str) -> Result<Statement> {
    parse_one(sql)
}

fn parse_one(source: &str) -> Result<Statement> {
    let parse = || {
        use sqlparser::keywords::Keyword;
        use sqlparser::tokenizer::{Token, Tokenizer};

        let dialect = GenericDialect {};
        let mut tokens = Tokenizer::new(&dialect, source)
            .tokenize()
            .map_err(|error| DbError::SqlParse(error.to_string()))?;
        // sqlparser has no PostgreSQL ABORT alias. Normalize only this token,
        // then parse the entire rollback grammar, including any suffix.
        if let Some(Token::Word(word)) = tokens
            .iter_mut()
            .find(|token| !matches!(token, Token::Whitespace(_) | Token::SemiColon))
            && word.keyword == Keyword::ABORT
        {
            word.keyword = Keyword::ROLLBACK;
            word.value = "ROLLBACK".to_owned();
        }
        let mut parser = Parser::new(&dialect).with_tokens(tokens);
        let statements = parser
            .parse_statements()
            .map_err(|error| DbError::SqlParse(error.to_string()))?;
        // parse_statements also serves procedural blocks and may stop at END.
        // Top-level SQL must consume every token before any boundary effect.
        if parser.peek_token().token != Token::EOF {
            return Err(DbError::SqlParse(
                "unexpected trailing SQL tokens".to_owned(),
            ));
        }
        match statements.as_slice() {
            [statement] => Ok(statement.clone()),
            [] => Err(unsupported("empty SQL", "one statement is required")),
            _ => Err(unsupported(
                "multiple statements",
                "execute one statement per call",
            )),
        }
    };
    let Ok(mut cache) = parse_cache().lock() else {
        return parse();
    };
    if let Some(cached) = cache.get(source) {
        return Ok(cached.clone());
    }
    let statement = parse()?;
    if cache.len() >= PARSE_CACHE_LIMIT {
        cache.clear();
    }
    cache.insert(source.to_owned(), statement.clone());
    Ok(statement)
}

fn parameter_indexes(statement: &Statement) -> Result<BTreeSet<usize>> {
    let mut indexes = BTreeSet::new();
    if let ControlFlow::Break(error) = visit_expressions(statement, |expression| {
        let Expr::Value(value) = expression else {
            return ControlFlow::Continue(());
        };
        let AstValue::Placeholder(name) = &value.value else {
            return ControlFlow::Continue(());
        };
        match values::parameter_index(name) {
            Ok(index) => {
                indexes.insert(index);
                ControlFlow::Continue(())
            }
            Err(error) => ControlFlow::Break(error),
        }
    }) {
        return Err(error);
    }
    Ok(indexes)
}

pub(crate) fn parameter_count(source: &str) -> Result<usize> {
    let indexes = parameter_indexes(&parse_one(source)?)?;
    Ok(indexes.last().map_or(0, |index| index + 1))
}

fn validate_parameters(statement: &Statement, params: &[Value]) -> Result<()> {
    let indexes = parameter_indexes(statement)?;
    if indexes.is_empty() {
        return if params.is_empty() {
            Ok(())
        } else {
            Err(DbError::SqlParameter(
                "statement does not reference supplied parameters".to_owned(),
            ))
        };
    }
    if params.is_empty() {
        return Err(DbError::SqlParameter(
            "statement references parameters but none were supplied".to_owned(),
        ));
    }
    if let Some(index) = indexes.iter().find(|index| **index >= params.len()) {
        return Err(DbError::SqlParameter(format!(
            "placeholder ${} requires parameter {}, but only {} were supplied",
            index + 1,
            index + 1,
            params.len()
        )));
    }
    for index in 0..params.len() {
        if !indexes.contains(&index) {
            return Err(DbError::SqlParameter(format!(
                "parameter ${} is not referenced",
                index + 1
            )));
        }
    }
    Ok(())
}

pub(super) fn column_position(
    table: &TableDefinition,
    expression: &sqlparser::ast::Expr,
) -> Result<Option<usize>> {
    let name = match expression {
        sqlparser::ast::Expr::Identifier(identifier) => identifier.value.as_str(),
        sqlparser::ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            parts[1].value.as_str()
        }
        sqlparser::ast::Expr::CompoundIdentifier(_) => {
            return Err(unsupported(
                "column",
                "qualified names are limited to table.column",
            ));
        }
        _ => return Ok(None),
    };
    Ok(table.columns.iter().position(|column| column.name == name))
}

pub(super) fn column_position_by_name(table: &TableDefinition, name: &str) -> Result<usize> {
    table
        .columns
        .iter()
        .position(|column| column.name == name)
        .ok_or_else(|| DbError::SqlUndefinedColumn {
            name: name.to_owned(),
        })
}

pub(super) fn table_from_join<'a>(
    catalog: &'a Catalog,
    table: &'a sqlparser::ast::TableWithJoins,
) -> Result<&'a TableDefinition> {
    if !table.joins.is_empty() {
        return Err(unsupported("FROM", "joins are not supported"));
    }
    let sqlparser::ast::TableFactor::Table { name, .. } = &table.relation else {
        return Err(unsupported("FROM", "only plain tables are supported"));
    };
    // Single-table statements accept a table alias: column resolution is
    // by column name, so the alias only qualifies references.
    let name = simple_object_name(name, "table")?;
    find_table(catalog, name)
}

pub(super) fn find_table<'a>(catalog: &'a Catalog, name: &str) -> Result<&'a TableDefinition> {
    catalog
        .tables()
        .find(|table| table.name == name)
        .ok_or_else(|| DbError::SqlUndefinedTable {
            name: name.to_owned(),
        })
}

pub(super) fn simple_object_name<'a>(
    name: &'a sqlparser::ast::ObjectName,
    kind: &'static str,
) -> Result<&'a str> {
    // A single `public` qualifier is accepted and validated: many tools
    // emit schema-qualified names unconditionally. Other schemas refuse.
    match name.0.len() {
        1 => {}
        2 => {
            let schema = name.0[0]
                .as_ident()
                .map(|identifier| identifier.value.as_str());
            if schema != Some("public") {
                return Err(unsupported(
                    kind,
                    "only the default \"public\" schema exists",
                ));
            }
        }
        _ => return Err(unsupported(kind, "name qualifiers nested too deep")),
    }
    name.0[name.0.len() - 1]
        .as_ident()
        .map(|identifier| identifier.value.as_str())
        .ok_or_else(|| unsupported(kind, "computed names are not supported"))
}

fn statement_kind(statement: &Statement) -> &'static str {
    match statement {
        Statement::Query(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::AlterTable(_) => "ALTER TABLE",
        Statement::CreateIndex(_) => "CREATE INDEX",
        Statement::Drop { .. } => "DROP",
        Statement::StartTransaction { .. } => "BEGIN",
        Statement::Commit { .. } => "COMMIT",
        Statement::Rollback { .. } => "ROLLBACK",
        _ => "statement",
    }
}

pub(super) fn unsupported(statement: &'static str, reason: &str) -> DbError {
    DbError::SqlUnsupported {
        statement,
        reason: reason.to_owned(),
    }
}
