use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, Insert, SelectItem, SetExpr,
    TableObject, TableWithJoins, Update, UpdateTableFromKind,
};

use crate::{ColumnType, DbError, Result};

use super::{Binder, Column, expressions::Mode, scope::Scope, unsupported};

fn target_error(error: DbError, column: &str) -> DbError {
    match error {
        DbError::SqlDatatypeMismatch { .. } => DbError::SqlDatatypeMismatch {
            column: column.to_owned(),
        },
        other => other,
    }
}

impl Binder<'_> {
    pub(super) fn insert(&mut self, insert: &Insert) -> Result<Vec<Column>> {
        if !insert.optimizer_hints.is_empty()
            || insert.or.is_some()
            || insert.ignore
            || insert.table_alias.is_some()
            || insert.overwrite
            || !insert.assignments.is_empty()
            || insert.partitioned.is_some()
            || !insert.after_columns.is_empty()
            || insert.has_table_keyword
            || insert.on.is_some()
            || insert.output.is_some()
            || insert.replace_into
            || insert.priority.is_some()
            || insert.insert_alias.is_some()
            || insert.settings.is_some()
            || insert.format_clause.is_some()
            || insert.multi_table_insert_type.is_some()
        {
            return Err(unsupported(
                "INSERT",
                "only plain INSERT VALUES or INSERT SELECT is supported",
            ));
        }
        let TableObject::TableName(name) = &insert.table else {
            return Err(unsupported("INSERT", "target must be a plain table"));
        };
        let table = crate::sql::find_table(
            self.database.catalog(),
            crate::sql::simple_object_name(name, "table")?,
        )?;
        self.access.write.insert(table.id);
        let positions = crate::sql::write::insert_positions(table, &insert.columns)?;
        let expected = positions
            .iter()
            .map(|position| Some(table.columns[*position].data_type))
            .collect::<Vec<_>>();
        let source = insert
            .source
            .as_deref()
            .ok_or_else(|| unsupported("INSERT", "a source is required"))?;
        match source.body.as_ref() {
            SetExpr::Values(values) => {
                if source.with.is_some()
                    || source.order_by.is_some()
                    || source.limit_clause.is_some()
                    || source.fetch.is_some()
                    || !source.locks.is_empty()
                    || source.for_clause.is_some()
                    || source.settings.is_some()
                    || source.format_clause.is_some()
                    || !source.pipe_operators.is_empty()
                {
                    return Err(unsupported("INSERT", "VALUES source must be plain"));
                }
                for row in &values.rows {
                    if row.len() != positions.len() {
                        return Err(DbError::InvalidState(
                            "INSERT row does not match target column count".into(),
                        ));
                    }
                    for (expr, position) in row.iter().zip(&positions) {
                        let column = &table.columns[*position];
                        self.expr(
                            expr,
                            &Scope::default(),
                            Mode::Literal,
                            Some(column.data_type),
                        )
                        .map_err(|error| target_error(error, &column.name))?;
                    }
                }
            }
            SetExpr::Select(_) => {
                let columns = self.query(source, &expected)?;
                if columns.len() != positions.len() {
                    return Err(DbError::InvalidState(
                        "INSERT SELECT does not match target column count".into(),
                    ));
                }
                for (column, expected) in columns.iter().zip(expected) {
                    if let (Some(actual), Some(expected)) = (column.ty.kind, expected) {
                        super::common_type(Some(actual), Some(expected))?;
                    }
                }
            }
            _ => return Err(unsupported("INSERT", "source must be VALUES or SELECT")),
        }
        self.returning(table, insert.returning.as_deref())
    }

    pub(super) fn update(&mut self, update: &Update) -> Result<Vec<Column>> {
        if !update.optimizer_hints.is_empty()
            || !update.table.joins.is_empty()
            || update.assignments.is_empty()
            || update.output.is_some()
            || update.or.is_some()
            || !update.order_by.is_empty()
            || update.limit.is_some()
        {
            return Err(unsupported("UPDATE", "unsupported UPDATE clauses"));
        }
        let (target, table) = self.factor(&update.table.relation, false)?;
        self.access.write.insert(table.id);
        let mut seen = std::collections::BTreeSet::new();
        for assignment in &update.assignments {
            let AssignmentTarget::ColumnName(name) = &assignment.target else {
                return Err(unsupported("UPDATE", "tuple assignments are not supported"));
            };
            let name = crate::sql::simple_object_name(name, "assignment column")?;
            let position = crate::sql::column_position_by_name(table, name)?;
            if !seen.insert(position) {
                return Err(DbError::InvalidState(
                    "column assigned more than once".into(),
                ));
            }
            self.assignment(
                &assignment.value,
                &target,
                table.columns[position].data_type,
            )
            .map_err(|error| target_error(error, name))?;
        }
        let scope = if let Some(from) = &update.from {
            let (UpdateTableFromKind::AfterSet(tables) | UpdateTableFromKind::BeforeSet(tables)) =
                from;
            if update.selection.is_none() {
                return Err(unsupported("UPDATE", "UPDATE FROM requires WHERE"));
            }
            self.cross_source(tables, target)?
        } else {
            target
        };
        if let Some(selection) = &update.selection {
            // The DML evaluator does not resolve subqueries today. Refuse them
            // rather than authorize an unexecuted/partially interpreted shape.
            self.predicate(selection, &scope, update.from.is_some(), false)?;
        }
        self.returning(table, update.returning.as_deref())
    }

    pub(super) fn delete(&mut self, delete: &Delete) -> Result<Vec<Column>> {
        let (FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables)) = &delete.from;
        if !delete.optimizer_hints.is_empty()
            || tables.len() != 1
            || !delete.tables.is_empty()
            || !tables[0].joins.is_empty()
            || delete.output.is_some()
            || !delete.order_by.is_empty()
            || delete.limit.is_some()
        {
            return Err(unsupported(
                "DELETE",
                "only one plain target table is supported",
            ));
        }
        let (target, table) = self.factor(&tables[0].relation, false)?;
        self.access.write.insert(table.id);
        let scope = if let Some(using) = &delete.using {
            if delete.selection.is_none() {
                return Err(unsupported("DELETE", "DELETE USING requires WHERE"));
            }
            self.cross_source(using, target)?
        } else {
            target
        };
        if let Some(selection) = &delete.selection {
            self.predicate(selection, &scope, delete.using.is_some(), false)?;
        }
        self.returning(table, delete.returning.as_deref())
    }

    fn cross_source(&mut self, tables: &[TableWithJoins], mut target: Scope) -> Result<Scope> {
        if tables.len() != 1 || !tables[0].joins.is_empty() {
            return Err(unsupported(
                "DML source",
                "only one plain source table is supported",
            ));
        }
        let (source, _) = self.factor(&tables[0].relation, true)?;
        target.append(source)?;
        Ok(target)
    }

    fn assignment(&mut self, expr: &Expr, scope: &Scope, expected: ColumnType) -> Result<()> {
        match expr {
            Expr::Nested(inner) => return self.assignment(inner, scope, expected),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Plus | BinaryOperator::Minus,
                right,
            } => {
                for operand in [left.as_ref(), right.as_ref()] {
                    self.assignment_operand(operand, scope, expected)?;
                }
                if !super::numeric(expected) {
                    return Err(DbError::SqlDatatypeMismatch {
                        column: "numeric assignment".into(),
                    });
                }
            }
            Expr::Identifier(_) => {
                self.expr(expr, scope, Mode::Operand, Some(expected))?;
            }
            _ => {
                self.expr(expr, scope, Mode::Literal, Some(expected))?;
            }
        }
        Ok(())
    }

    fn assignment_operand(
        &mut self,
        expr: &Expr,
        scope: &Scope,
        expected: ColumnType,
    ) -> Result<()> {
        if let Expr::Nested(inner) = expr {
            return self.assignment_operand(inner, scope, expected);
        }
        let mode = if matches!(expr, Expr::Identifier(_)) {
            Mode::Operand
        } else {
            Mode::Literal
        };
        let ty = self.expr(expr, scope, mode, Some(expected))?;
        self.require_numeric(ty)
    }

    fn returning(
        &self,
        table: &crate::TableDefinition,
        items: Option<&[SelectItem]>,
    ) -> Result<Vec<Column>> {
        let mut columns = Vec::new();
        for item in items.into_iter().flatten() {
            match item {
                SelectItem::Wildcard(options) if options.to_string().is_empty() => {
                    columns.extend(table.columns.iter().map(|column| Column {
                        name: column.name.clone(),
                        ty: super::Type::known(column.data_type),
                    }));
                }
                SelectItem::UnnamedExpr(Expr::Identifier(name)) => {
                    let position = crate::sql::column_position_by_name(table, &name.value)?;
                    columns.push(Column {
                        name: table.columns[position].name.clone(),
                        ty: super::Type::known(table.columns[position].data_type),
                    });
                }
                _ => {
                    return Err(unsupported(
                        "RETURNING",
                        "only target columns and * are supported",
                    ));
                }
            }
        }
        Ok(columns)
    }
}
