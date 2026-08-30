//! The first embedded SQL adoption tier.
//!
//! This module is an adapter, not a second relational engine. The parser owns
//! SQL syntax; the catalog, row validation, transaction lifecycle, and
//! backend publication remain owned by the typed facade. Keeping that split
//! explicit lets the same statements qualify Temporary and Seer backends and
//! keeps a future replacement Rust storage engine behind the same boundary.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Query, Statement, Value as AstValue, visit_expressions,
};
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
#[path = "sql_write.rs"]
mod write;

mod result {
    use crate::{CommitId, Value};

    /// One column in an embedded SQL result.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SqlColumn {
        pub name: String,
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

#[allow(dead_code)]
pub(crate) fn execute(database: &mut RelationalDatabase, source: &str) -> Result<SqlResult> {
    execute_with_params(database, source, &[])
}

pub(crate) fn execute_with_params(
    database: &mut RelationalDatabase,
    source: &str,
    params: &[Value],
) -> Result<SqlResult> {
    let statement = parse_one(source)?;
    validate_parameters(&statement, params)?;
    if let Statement::CreateTable(create) = &statement {
        let commit = schema::execute_create_table(database, create)?;
        return Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            commit: Some(commit),
        });
    }
    if let Statement::AlterTable(alter) = &statement {
        let commit = schema::execute_alter_table(database, alter)?;
        return Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            commit: Some(commit),
        });
    }
    if let Statement::CreateIndex(create) = &statement {
        let commit = schema::execute_create_index(database, create)?;
        return Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            commit: Some(commit),
        });
    }

    let (mut result, commit) = database.transaction(|store, transaction| {
        execute_in_transaction_statement(store, transaction, &statement, params)
    })?;
    result.commit = Some(commit);
    Ok(result)
}

pub(crate) fn execute_in_transaction(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    source: &str,
) -> Result<SqlResult> {
    execute_in_transaction_with_params(database, transaction, source, &[])
}

pub(crate) fn execute_in_transaction_with_params(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    source: &str,
    params: &[Value],
) -> Result<SqlResult> {
    let statement = parse_one(source)?;
    validate_parameters(&statement, params)?;
    execute_in_transaction_statement(database, transaction, &statement, params)
}

fn execute_in_transaction_statement(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    statement: &Statement,
    params: &[Value],
) -> Result<SqlResult> {
    match statement {
        Statement::Query(query) => {
            query::execute_query(database, transaction, query, params).map_err(classify_query_error)
        }
        Statement::Insert(insert) => write::execute_insert(database, transaction, insert, params),
        Statement::Update(update) => write::execute_update(database, transaction, update, params),
        Statement::Delete(delete) => write::execute_delete(database, transaction, delete, params),
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. } => Err(unsupported(
            "transaction control",
            "use the typed transaction closure",
        )),
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
        other => Err(unsupported(
            statement_kind(other),
            "not in the embedded SQL tier",
        )),
    }
}

fn classify_query_error(error: DbError) -> DbError {
    const GROUPING_SUFFIX: &str =
        "' must appear in the GROUP BY clause or be used in an aggregate function";
    match error {
        DbError::InvalidState(reason) => {
            let column = reason
                .strip_prefix("column '")
                .and_then(|rest| rest.strip_suffix(GROUPING_SUFFIX))
                .map(str::to_owned);
            column.map_or(DbError::InvalidState(reason), |column| {
                DbError::SqlGroupingError { column }
            })
        }
        other => other,
    }
}

/// Infer the expected engine type of each positional parameter in a
/// statement so wire servers can report concrete parameter types to clients
/// that refuse unspecified types. Positions the statement context does not
/// determine map to `None`. Single-table scope only; joins and aliases are
/// outside the tier anyway.
pub(crate) fn describe_parameters(
    database: &RelationalDatabase,
    source: &str,
) -> Result<Vec<Option<ColumnType>>> {
    let statement = parse_one(source)?;
    let mut inferred = ParameterInference::default();
    match &statement {
        Statement::Insert(insert) => {
            let table_name = match &insert.table {
                sqlparser::ast::TableObject::TableName(name) => simple_object_name(name, "table")?,
                _ => return Ok(Vec::new()),
            };
            let table = find_table(database.catalog(), table_name)?;
            let positions = write::insert_positions(table, &insert.columns)
                .unwrap_or_else(|_| (0..table.columns.len()).collect());
            if let Some(source_query) = insert.source.as_deref()
                && let sqlparser::ast::SetExpr::Values(values) = source_query.body.as_ref()
            {
                for row in &values.rows {
                    for (expression, position) in row.iter().zip(&positions) {
                        inferred.observe_expression(expression, column_type_at(table, *position));
                    }
                }
            }
        }
        Statement::Update(update) => {
            let table = super_table_from_join(database.catalog(), &update.table);
            if let Some(table) = table {
                for assignment in &update.assignments {
                    if let Expr::Value(value) = &assignment.value
                        && let AstValue::Placeholder(_) = value.value
                    {
                        let name = match &assignment.target {
                            sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                                simple_object_name(name, "column").ok().map(str::to_owned)
                            }
                            sqlparser::ast::AssignmentTarget::Tuple(_) => None,
                        };
                        if let Some(column_type) = column_type_by_name(table, name.as_deref()) {
                            inferred.observe_placeholder(&assignment.value, column_type);
                        }
                    }
                }
                if let Some(selection) = &update.selection {
                    inferred.walk_predicate(selection, Some(table));
                }
            }
        }
        Statement::Delete(delete) => {
            let (sqlparser::ast::FromTable::WithFromKeyword(tables)
            | sqlparser::ast::FromTable::WithoutKeyword(tables)) = &delete.from;
            if tables.len() == 1 {
                let table = super_table_from_join(database.catalog(), &tables[0]);
                if let (Some(table), Some(selection)) = (table, &delete.selection) {
                    inferred.walk_predicate(selection, Some(table));
                }
            }
        }
        Statement::Query(query) => {
            inferred.walk_query(database, query);
        }
        _ => {}
    }
    Ok(inferred.finish())
}

fn super_table_from_join<'a>(
    catalog: &'a Catalog,
    table: &'a sqlparser::ast::TableWithJoins,
) -> Option<&'a TableDefinition> {
    crate::sql::table_from_join(catalog, table).ok()
}

fn column_type_at(table: &TableDefinition, position: usize) -> Option<ColumnType> {
    table.columns.get(position).map(|column| column.data_type)
}

fn column_type_by_name(table: &TableDefinition, name: Option<&str>) -> Option<ColumnType> {
    let name = name?;
    table
        .columns
        .iter()
        .find(|column| column.name == name)
        .map(|column| column.data_type)
}

#[derive(Default)]
struct ParameterInference {
    types: std::collections::BTreeMap<usize, ColumnType>,
}

impl ParameterInference {
    fn observe_placeholder(&mut self, expression: &Expr, column_type: ColumnType) {
        if let Expr::Value(value) = expression
            && let AstValue::Placeholder(name) = &value.value
            && let Ok(index) = values::parameter_index(name)
        {
            self.types.insert(index, column_type);
        }
    }

    fn observe_expression(&mut self, expression: &Expr, column_type: Option<ColumnType>) {
        if let Some(column_type) = column_type {
            self.observe_placeholder(expression, column_type);
        }
    }

    /// Infer comparison operands (`column OP $n`, `$n LIKE ...`) against a
    /// known single-table scope.
    fn walk_predicate(&mut self, expression: &Expr, table: Option<&TableDefinition>) {
        let _ = visit_expressions(expression, |visited| {
            match visited {
                Expr::BinaryOp { left, op, right } => {
                    if matches!(
                        op,
                        BinaryOperator::Eq
                            | BinaryOperator::NotEq
                            | BinaryOperator::Lt
                            | BinaryOperator::LtEq
                            | BinaryOperator::Gt
                            | BinaryOperator::GtEq
                            | BinaryOperator::And
                            | BinaryOperator::Or
                    ) {
                        if let Some(resolved) = identifier_type(left.as_ref(), table) {
                            self.observe_placeholder(right, resolved);
                        }
                        if let Some(resolved) = identifier_type(right.as_ref(), table) {
                            self.observe_placeholder(left, resolved);
                        }
                    }
                }
                Expr::Like { expr, pattern, .. } => {
                    if let Some(resolved) = identifier_type(expr, table) {
                        self.observe_placeholder(pattern, resolved);
                    }
                }
                _ => {}
            }
            ControlFlow::<DbError>::Continue(())
        });
    }

    fn walk_query(&mut self, database: &RelationalDatabase, query: &Query) {
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            return;
        };
        // Join queries infer over a synthetic scope whose columns are the
        // concatenation of both sides, so unqualified predicate columns
        // resolve to real types instead of falling back to TEXT.
        let joined_scope;
        let table = match select.from.first() {
            Some(from) if !from.joins.is_empty() && from.joins.len() == 1 => {
                let resolve = |factor: &sqlparser::ast::TableFactor| -> Option<&TableDefinition> {
                    match factor {
                        sqlparser::ast::TableFactor::Table { name, .. } => {
                            find_table(database.catalog(), simple_object_name(name, "table").ok()?)
                                .ok()
                        }
                        _ => None,
                    }
                };
                let (Some(left), Some(right)) =
                    (resolve(&from.relation), resolve(&from.joins[0].relation))
                else {
                    return;
                };
                joined_scope = TableDefinition {
                    id: left.id,
                    name: String::new(),
                    columns: left
                        .columns
                        .iter()
                        .chain(right.columns.iter())
                        .cloned()
                        .collect(),
                };
                Some(&joined_scope)
            }
            other => other.and_then(|from| super_table_from_join(database.catalog(), from)),
        };
        if let Some(selection) = &select.selection {
            self.walk_predicate(selection, table);
        }
        // LIMIT/OFFSET parameters are non-negative integers.
        if let Some(sqlparser::ast::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) = &query.limit_clause
            && limit_by.is_empty()
        {
            let offset_exprs = offset.iter().map(|offset| &offset.value);
            for window in limit.iter().chain(offset_exprs) {
                self.observe_expression(window, Some(ColumnType::U64));
            }
        }
    }

    fn finish(self) -> Vec<Option<ColumnType>> {
        let count = self.types.keys().next_back().map_or(0, |index| index + 1);
        let mut result = vec![None; count];
        for (index, column_type) in self.types {
            result[index] = Some(column_type);
        }
        result
    }
}

fn identifier_type(expression: &Expr, table: Option<&TableDefinition>) -> Option<ColumnType> {
    let name = match expression {
        Expr::Identifier(identifier) => identifier.value.as_str(),
        Expr::CompoundIdentifier(parts) => parts.last()?.value.as_str(),
        _ => return None,
    };
    column_type_by_name(table?, Some(name))
}

/// Successful parses are cached by exact source text so repeated
/// executions (wire prepared statements, describe probes) skip the
/// parser. Bounded: a full cache clears wholesale, which keeps hot
/// statement sets resident without LRU bookkeeping. Parse failures are
/// not cached - clients retry malformed SQL far less often than valid
/// hot statements.
const PARSE_CACHE_LIMIT: usize = 1024;

fn parse_cache() -> &'static std::sync::Mutex<HashMap<String, Statement>> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Statement>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Wire-tier helpers: only the feature-gated pgwire server consults these.
#[cfg(feature = "pgwire")]
pub(super) fn parse_statement(sql: &str) -> Result<Statement> {
    parse_one(sql)
}

#[cfg(feature = "pgwire")]
pub(super) fn table_from_join_name(table: &sqlparser::ast::TableWithJoins) -> Result<String> {
    let sqlparser::ast::TableFactor::Table { name, .. } = &table.relation else {
        return Err(unsupported("FROM", "only plain tables are supported"));
    };
    Ok(simple_object_name(name, "table")?.to_owned())
}

fn parse_one(source: &str) -> Result<Statement> {
    let parse = || {
        let statements = Parser::parse_sql(&GenericDialect {}, source)
            .map_err(|error| DbError::SqlParse(error.to_string()))?;
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

fn validate_parameters(statement: &Statement, params: &[Value]) -> Result<()> {
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

/// Tables a statement reads and writes, plus whether it is schema
/// administration. Used by the wire tier's grant enforcement; embedded
/// callers are trusted and never consult this.
#[cfg(feature = "pgwire")]
pub(super) fn statement_access(
    sql: &str,
) -> Result<(
    std::collections::BTreeSet<String>,
    std::collections::BTreeSet<String>,
    bool,
)> {
    use sqlparser::ast::{
        Expr as SqlExpr, SetExpr as SqlSetExpr, Statement as SqlStatement,
        TableObject as SqlTableObject,
    };

    fn collect_expr_tables(expr: &SqlExpr, read: &mut std::collections::BTreeSet<String>) {
        match expr {
            SqlExpr::Nested(inner) => collect_expr_tables(inner, read),
            SqlExpr::BinaryOp { left, right, .. } => {
                collect_expr_tables(left, read);
                collect_expr_tables(right, read);
            }
            SqlExpr::UnaryOp { expr: inner, .. } => collect_expr_tables(inner, read),
            SqlExpr::InList { expr, list, .. } => {
                collect_expr_tables(expr, read);
                for item in list {
                    collect_expr_tables(item, read);
                }
            }
            SqlExpr::InSubquery { subquery, .. } | SqlExpr::Subquery(subquery) => {
                collect_query_tables(subquery, read);
            }
            SqlExpr::Exists { subquery, .. } => collect_query_tables(subquery, read),
            SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => collect_expr_tables(inner, read),
            _ => {}
        }
    }

    fn collect_query_tables(
        query: &sqlparser::ast::Query,
        read: &mut std::collections::BTreeSet<String>,
    ) {
        collect_setexpr_tables(&query.body, read);
        if let Some(selection) = query_body_selection(&query.body)
            && let Some(where_clause) = &selection.selection
        {
            collect_expr_tables(where_clause, read);
        }
        // Table factors in the top-level FROM are handled above. Walk every
        // expression as well so scalar, EXISTS, and IN subqueries in
        // projections and join predicates cannot bypass table grants.
        let _ = visit_expressions(query, |expression| {
            match expression {
                SqlExpr::InSubquery { subquery, .. }
                | SqlExpr::Subquery(subquery)
                | SqlExpr::Exists { subquery, .. } => collect_query_tables(subquery, read),
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
    }

    fn query_body_selection(body: &SqlSetExpr) -> Option<&sqlparser::ast::Select> {
        match body {
            SqlSetExpr::Select(select) => Some(select.as_ref()),
            _ => None,
        }
    }

    fn collect_setexpr_tables(body: &SqlSetExpr, read: &mut std::collections::BTreeSet<String>) {
        match body {
            SqlSetExpr::Select(select) => {
                let select = select.as_ref();
                for from_item in &select.from {
                    collect_table_with_joins(from_item, read);
                }
                if let Some(selection_where) = &select.selection {
                    collect_expr_tables(selection_where, read);
                }
            }
            SqlSetExpr::SetOperation { left, right, .. } => {
                collect_setexpr_tables(left.as_ref(), read);
                collect_setexpr_tables(right.as_ref(), read);
            }
            _ => {}
        }
    }

    fn collect_factor_tables(
        factor: &sqlparser::ast::TableFactor,
        read: &mut std::collections::BTreeSet<String>,
    ) {
        match factor {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                if let Ok(table) = simple_object_name(name, "table") {
                    read.insert(table.to_owned());
                }
            }
            sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                collect_query_tables(subquery, read);
            }
            _ => {}
        }
    }

    fn collect_table_with_joins(
        table: &sqlparser::ast::TableWithJoins,
        read: &mut std::collections::BTreeSet<String>,
    ) {
        collect_factor_tables(&table.relation, read);
        for join in &table.joins {
            collect_factor_tables(&join.relation, read);
        }
    }

    fn object_to_write(target: &SqlTableObject, write: &mut std::collections::BTreeSet<String>) {
        if let SqlTableObject::TableName(name) = target
            && let Ok(table) = simple_object_name(name, "table")
        {
            write.insert(table.to_owned());
        }
    }

    let statement = parse_statement(sql)?;
    let mut read = std::collections::BTreeSet::new();
    let mut write = std::collections::BTreeSet::new();
    let admin = match &statement {
        SqlStatement::Query(query) => {
            collect_query_tables(query, &mut read);
            false
        }
        SqlStatement::Insert(insert) => {
            object_to_write(&insert.table, &mut write);
            if let Some(source) = &insert.source {
                collect_query_tables(source, &mut read);
            }
            false
        }
        SqlStatement::Update(update) => {
            if let Ok(table) = table_from_join_name(&update.table) {
                write.insert(table);
            }
            if let Some(kind) = &update.from {
                let tables = match kind {
                    sqlparser::ast::UpdateTableFromKind::BeforeSet(tables)
                    | sqlparser::ast::UpdateTableFromKind::AfterSet(tables) => tables,
                };
                for item in tables {
                    collect_table_with_joins(item, &mut read);
                }
            }
            if let Some(selection) = &update.selection {
                collect_expr_tables(selection, &mut read);
            }
            false
        }
        SqlStatement::Delete(delete) => {
            let tables = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            for item in tables {
                collect_factor_tables(&item.relation, &mut write);
            }
            if let Some(using) = &delete.using {
                for item in using {
                    collect_table_with_joins(item, &mut read);
                }
            }
            if let Some(selection) = &delete.selection {
                collect_expr_tables(selection, &mut read);
            }
            false
        }
        _ => true,
    };
    Ok((read, write, admin))
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
