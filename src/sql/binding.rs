//! Shared, non-executing SQL validation, signatures and resolved privileges.
//!
//! The retained AST is evaluated by the existing executor only after binding.
//! Nothing here evaluates an expression or opens a row/transaction interface.

use std::collections::{BTreeMap, BTreeSet};

use sqlparser::ast::Statement;

use crate::{ColumnType, DbError, RelationalDatabase, Result, TableId};

use super::{SqlColumn, control, unsupported};

mod ddl;
mod expressions;
mod queries;
mod rewrite;
mod scope;
mod writes;

pub(crate) use ddl::sql_type;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct BoundStatement {
    pub statement: Statement,
    pub catalog_stamp: Option<(u64, u64)>,
    pub parameter_types: Vec<Option<ColumnType>>,
    pub columns: Vec<SqlColumn>,
    #[cfg_attr(
        not(feature = "pgwire"),
        allow(
            dead_code,
            reason = "privileges are consumed by the optional wire boundary"
        )
    )]
    pub access: StatementAccess,
    pub effect: StatementEffect,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StatementAccess {
    pub read: BTreeSet<TableId>,
    pub write: BTreeSet<TableId>,
    pub requires_admin: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatementEffect {
    Read,
    Write,
    Schema,
    Control(control::TransactionControl),
}

/// Controls can be prepared and dispatched even when catalog/grant access is
/// unavailable. Complete parsing/control validation still precedes cleanup.
#[cfg(any(feature = "pgwire", test))]
pub(crate) fn bind_control(source: &str) -> Result<Option<BoundStatement>> {
    let statement = super::parse_statement(source)?;
    Ok(control::bind(&statement)?.map(|control| bound_control(statement, control)))
}

fn bound_control(statement: Statement, control: control::TransactionControl) -> BoundStatement {
    BoundStatement {
        statement,
        catalog_stamp: None,
        parameter_types: Vec::new(),
        columns: Vec::new(),
        access: StatementAccess::default(),
        effect: StatementEffect::Control(control),
    }
}

pub(crate) fn bind(
    database: &RelationalDatabase,
    source: &str,
    hints: &[Option<ColumnType>],
) -> Result<BoundStatement> {
    let mut statement = super::parse_statement(source)?;
    if let Some(control) = control::bind(&statement)? {
        if !hints.is_empty() {
            return Err(DbError::SqlParameter(
                "statement does not reference supplied parameters".into(),
            ));
        }
        return Ok(bound_control(statement, control));
    }
    let count = super::parameter_count(source)?;
    if hints.len() > count {
        return Err(DbError::SqlParameter(
            if count == 0 {
                "statement does not reference supplied parameters"
            } else {
                "more declared types than parameter positions"
            }
            .into(),
        ));
    }
    let mut parameters = hints.to_vec();
    parameters.resize(count, None);
    // Constraints are monotone: unknown -> concrete, then numeric promotion.
    // Revisit the AST so constraints discovered after a projection, or in the
    // other set arm/subquery, reach every occurrence before freezing. Output
    // contexts participate too, even when no parameters occur in the SQL.
    let mut prefer_outputs = false;
    let mut output_contexts = BTreeMap::new();
    loop {
        let previous = parameters.clone();
        let mut binder = Binder {
            database,
            parameters: &mut parameters,
            hints,
            access: StatementAccess::default(),
            prefer_outputs,
            literal_casts: BTreeMap::new(),
            output_contexts: &mut output_contexts,
            outputs_changed: false,
            #[cfg(test)]
            set_visits: 0,
        };
        let (columns, effect) = binder.statement(&statement)?;
        let outputs_changed = binder.outputs_changed;
        let access = binder.access;
        let literal_casts = binder.literal_casts;
        if parameters == previous && !outputs_changed && !prefer_outputs {
            // Only now may unresolved output expressions choose their preferred
            // domain. This includes outputs of EXISTS/scalar subqueries, but
            // never parameters used solely in ambiguous predicates or gaps.
            prefer_outputs = true;
            continue;
        }
        if parameters == previous && !outputs_changed {
            rewrite::coercions(&mut statement, &parameters, &literal_casts)?;
            return Ok(BoundStatement {
                statement,
                catalog_stamp: Some(database.catalog_stamp()),
                parameter_types: parameters,
                columns: columns
                    .into_iter()
                    .map(|column| {
                        SqlColumn::typed(column.name, column.ty.kind.unwrap_or(ColumnType::Text))
                    })
                    .collect(),
                access,
                effect,
            });
        }
    }
}

struct Binder<'a> {
    database: &'a RelationalDatabase,
    parameters: &'a mut Vec<Option<ColumnType>>,
    hints: &'a [Option<ColumnType>],
    access: StatementAccess,
    prefer_outputs: bool,
    // The cached parser currently discards source spans. The AST is immutable
    // during inference, so node addresses identify occurrences, not SQL text.
    // These keys are consumed before moving or mutating the tree, never kept
    // in BoundStatement and never dereferenced.
    literal_casts: BTreeMap<*const sqlparser::ast::Expr, ColumnType>,
    // Like parameter constraints, output domains survive root passes. This
    // propagates right-to-left set and subquery contexts without rebinding
    // descendants recursively within a pass. Keys have the same AST lifetime
    // restriction as literal_casts above.
    output_contexts: &'a mut BTreeMap<*const sqlparser::ast::SetExpr, Vec<Option<ColumnType>>>,
    outputs_changed: bool,
    #[cfg(test)]
    set_visits: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct Type {
    kind: Option<ColumnType>,
    // An SQL string literal is initially unknown, not a typed Text column.
    string: bool,
}

impl Type {
    fn known(kind: ColumnType) -> Self {
        Self {
            kind: Some(kind),
            string: false,
        }
    }

    fn common(self, other: Self) -> Result<Self> {
        Ok(Self {
            kind: common_type(self.kind, other.kind)?,
            string: self.string || other.string,
        })
    }
}

#[derive(Clone, Debug)]
struct Column {
    name: String,
    ty: Type,
}

fn numeric(kind: ColumnType) -> bool {
    matches!(
        kind,
        ColumnType::I64 | ColumnType::U64 | ColumnType::Float64 | ColumnType::Decimal
    )
}

/// Reconcile an entire expression domain. NULL/unknown contributes no preferred
/// type; callers default all-unknown outputs only at the output boundary.
/// I64/U64 share the integer comparison domain; other numeric domains promote
/// float > decimal > integer. This is also the evaluator's coercion authority.
pub(crate) fn common_type(
    left: Option<ColumnType>,
    right: Option<ColumnType>,
) -> Result<Option<ColumnType>> {
    use ColumnType::*;
    let (Some(left), Some(right)) = (left, right) else {
        return Ok(left.or(right));
    };
    if left == right {
        return Ok(Some(left));
    }
    if numeric(left) && numeric(right) {
        return Ok(Some(if left == Float64 || right == Float64 {
            Float64
        } else if left == Decimal || right == Decimal {
            Decimal
        } else {
            I64
        }));
    }
    Err(DbError::SqlDatatypeMismatch {
        column: format!("incompatible {left:?} and {right:?} contexts"),
    })
}

impl Binder<'_> {
    fn statement(&mut self, statement: &Statement) -> Result<(Vec<Column>, StatementEffect)> {
        let columns = match statement {
            Statement::Query(query) => self.query(query, &[])?,
            Statement::Insert(insert) => return Ok((self.insert(insert)?, StatementEffect::Write)),
            Statement::Update(update) => return Ok((self.update(update)?, StatementEffect::Write)),
            Statement::Delete(delete) => return Ok((self.delete(delete)?, StatementEffect::Write)),
            Statement::Explain {
                statement,
                analyze,
                query_plan,
                verbose,
                estimate,
                format,
                options,
                ..
            } => {
                if *analyze
                    || *query_plan
                    || *verbose
                    || *estimate
                    || format.is_some()
                    || options.is_some()
                {
                    return Err(unsupported(
                        "EXPLAIN",
                        "only plain non-executing EXPLAIN is supported",
                    ));
                }
                if !matches!(
                    statement.as_ref(),
                    Statement::Query(_)
                        | Statement::Insert(_)
                        | Statement::Update(_)
                        | Statement::Delete(_)
                ) {
                    return Err(unsupported(
                        "EXPLAIN",
                        "expected SELECT, INSERT, UPDATE or DELETE",
                    ));
                }
                self.statement(statement)?;
                vec![Column {
                    name: "Query Plan".into(),
                    ty: Type::known(ColumnType::Text),
                }]
            }
            Statement::ShowVariable { variable } => {
                let name = variable
                    .iter()
                    .map(|part| part.value.as_str())
                    .collect::<Vec<_>>()
                    .join(" ")
                    .to_ascii_lowercase();
                if !matches!(
                    name.as_str(),
                    "server_version"
                        | "server_encoding"
                        | "client_encoding"
                        | "transaction_isolation"
                        | "standard_conforming_strings"
                        | "max_connections"
                        | "search_path"
                ) {
                    return Err(unsupported("SHOW", "unsupported configuration parameter"));
                }
                vec![Column {
                    name,
                    ty: Type::known(ColumnType::Text),
                }]
            }
            Statement::CreateTable(_)
            | Statement::CreateIndex(_)
            | Statement::AlterTable(_)
            | Statement::Drop { .. } => {
                self.ddl(statement)?;
                return Ok((Vec::new(), StatementEffect::Schema));
            }
            _ => return Err(unsupported("statement", "not in the embedded SQL tier")),
        };
        Ok((columns, StatementEffect::Read))
    }
}
