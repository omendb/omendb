//! Catalog-only DDL validation. Data-dependent constraint/backfill checks stay
//! in schema publication; binding never attempts those scans.
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, BinaryOperator, ColumnDef, ColumnOption, DataType,
    Expr, HiveDistributionStyle, NullsDistinctOption, ObjectType, RenameTableNameKind, Statement,
    TableConstraint, TimezoneInfo, Value,
};

use crate::{ColumnDefinition, ColumnId, ColumnType, DbError, Result, TableDefinition, TableId};

use super::{Binder, expressions::Mode, scope::Scope, unsupported};

impl Binder<'_> {
    pub(super) fn ddl(&mut self, statement: &Statement) -> Result<()> {
        self.access.requires_admin = true;
        match statement {
            Statement::CreateTable(create) => {
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
                    || !create.table_options.to_string().is_empty()
                    || create.file_format.is_some()
                    || create.location.is_some()
                    || create.version.is_some()
                    || create.comment.is_some()
                    || create.on_commit.is_some()
                    || create.on_cluster.is_some()
                    || create.primary_key.is_some()
                    || create.order_by.is_some()
                    || create.partition_by.is_some()
                    || create.cluster_by.is_some()
                    || create.clustered_by.is_some()
                    || create.inherits.is_some()
                    || create.partition_of.is_some()
                    || create.for_values.is_some()
                    || create.strict
                    || create.copy_grants
                    || create.enable_schema_evolution.is_some()
                    || create.change_tracking.is_some()
                    || create.data_retention_time_in_days.is_some()
                    || create.max_data_extension_time_in_days.is_some()
                    || create.default_ddl_collation.is_some()
                    || create.with_aggregation_policy.is_some()
                    || create.with_row_access_policy.is_some()
                    || create.with_storage_lifecycle_policy.is_some()
                    || create.with_tags.is_some()
                    || create.external_volume.is_some()
                    || create.base_location.is_some()
                    || create.catalog.is_some()
                    || create.catalog_sync.is_some()
                    || create.storage_serialization_policy.is_some()
                    || create.target_lag.is_some()
                    || create.warehouse.is_some()
                    || create.refresh_mode.is_some()
                    || create.initialize.is_some()
                    || create.require_user
                    || create.diststyle.is_some()
                    || create.distkey.is_some()
                    || create.sortkey.is_some()
                    || create.backup.is_some()
                    || !matches!(create.hive_distribution, HiveDistributionStyle::NONE)
                    || create.hive_formats.is_some()
                {
                    return Err(unsupported(
                        "CREATE TABLE",
                        "only plain scalar tables are supported",
                    ));
                }
                let name = crate::sql::simple_object_name(&create.name, "table")?;
                if create.columns.is_empty() {
                    return Err(unsupported(
                        "CREATE TABLE",
                        "at least one column is required",
                    ));
                }
                if self
                    .database
                    .catalog()
                    .tables()
                    .any(|table| table.name == name)
                {
                    return Err(DbError::InvalidState(format!(
                        "table {name} already exists"
                    )));
                }
                let mut seen = std::collections::BTreeSet::new();
                let mut primary = false;
                let mut columns = Vec::new();
                for (index, column) in create.columns.iter().enumerate() {
                    if !seen.insert(&column.name.value) {
                        return Err(DbError::InvalidState("duplicate column name".into()));
                    }
                    let (ty, nullable, column_primary) = column_type(column, false)?;
                    if primary && column_primary {
                        return Err(DbError::InvalidState("more than one PRIMARY KEY".into()));
                    }
                    primary |= column_primary;
                    columns.push(ColumnDefinition {
                        id: ColumnId(
                            u16::try_from(index + 1).map_err(|_| {
                                DbError::InvalidState("too many SQL columns".into())
                            })?,
                        ),
                        name: column.name.value.clone(),
                        data_type: ty,
                        nullable,
                    });
                }
                // This local definition exists only for resolving the new
                // columns/self-FK; it is never installed in the catalog.
                let table = TableDefinition {
                    id: TableId(0),
                    name: name.into(),
                    columns,
                };
                for constraint in &create.constraints {
                    if matches!(constraint, TableConstraint::PrimaryKey(_)) {
                        if primary {
                            return Err(DbError::InvalidState("more than one PRIMARY KEY".into()));
                        }
                        primary = true;
                    }
                    self.constraint(constraint, &table, true)?;
                }
            }
            Statement::CreateIndex(create) => {
                let name = create
                    .name
                    .as_ref()
                    .ok_or_else(|| unsupported("CREATE INDEX", "explicit name required"))?;
                crate::sql::simple_object_name(name, "index")?;
                if create.using.is_some()
                    || create.concurrently
                    || create.if_not_exists
                    || !create.include.is_empty()
                    || create.nulls_distinct.is_some()
                    || !create.with.is_empty()
                    || !create.index_options.is_empty()
                    || !create.alter_options.is_empty()
                    || create.columns.is_empty()
                {
                    return Err(unsupported(
                        "CREATE INDEX",
                        "only a plain index is supported",
                    ));
                }
                let table = crate::sql::find_table(
                    self.database.catalog(),
                    crate::sql::simple_object_name(&create.table_name, "table")?,
                )?;
                self.access.write.insert(table.id);
                let scope = table_scope(table);
                for key in &create.columns {
                    if key.operator_class.is_some()
                        || key.column.options.asc == Some(false)
                        || key.column.options.nulls_first.is_some()
                        || key.column.with_fill.is_some()
                    {
                        return Err(unsupported(
                            "CREATE INDEX",
                            "only ascending keys are supported",
                        ));
                    }
                    // The existing pure helper validates index-expression shape;
                    // our scope lookup additionally validates qualifiers.
                    crate::sql::schema::plan_index_expression(table, &key.column.expr)?;
                    let ty = self.expr(&key.column.expr, &scope, Mode::Computed, None)?;
                    if matches!(key.column.expr, Expr::BinaryOp { .. }) {
                        self.require_numeric(ty)?;
                    }
                }
                if let Some(predicate) = &create.predicate {
                    self.index_predicate(predicate, &scope)?;
                }
            }
            Statement::AlterTable(alter) => {
                if alter.if_exists
                    || alter.only
                    || alter.location.is_some()
                    || alter.on_cluster.is_some()
                    || alter.table_type.is_some()
                    || alter.operations.is_empty()
                {
                    return Err(unsupported("ALTER TABLE", "plain ALTER TABLE required"));
                }
                let table = crate::sql::find_table(
                    self.database.catalog(),
                    crate::sql::simple_object_name(&alter.name, "table")?,
                )?;
                self.access.write.insert(table.id);
                for operation in &alter.operations {
                    match operation {
                        AlterTableOperation::AddColumn {
                            if_not_exists: false,
                            column_def,
                            column_position: None,
                            ..
                        } => {
                            column_type(column_def, true)?;
                        }
                        AlterTableOperation::RenameColumn {
                            old_column_name, ..
                        } => {
                            crate::sql::column_position_by_name(table, &old_column_name.value)?;
                        }
                        AlterTableOperation::AlterColumn { column_name, op } => {
                            crate::sql::column_position_by_name(table, &column_name.value)?;
                            match op {
                                AlterColumnOperation::SetDataType {
                                    data_type,
                                    using: None,
                                    ..
                                } => {
                                    schema_type(data_type)?;
                                }
                                AlterColumnOperation::DropNotNull => {}
                                _ => {
                                    return Err(unsupported(
                                        "ALTER TABLE",
                                        "unsupported column operation",
                                    ));
                                }
                            }
                        }
                        AlterTableOperation::DropColumn {
                            column_names,
                            if_exists: false,
                            drop_behavior: None,
                            ..
                        } => {
                            for name in column_names {
                                crate::sql::column_position_by_name(table, &name.value)?;
                            }
                        }
                        AlterTableOperation::RenameTable {
                            table_name:
                                RenameTableNameKind::To(name) | RenameTableNameKind::As(name),
                        } => {
                            crate::sql::simple_object_name(name, "table")?;
                        }
                        AlterTableOperation::AddConstraint {
                            constraint,
                            not_valid: false,
                        } => {
                            self.constraint(constraint, table, false)?;
                        }
                        _ => return Err(unsupported("ALTER TABLE", "unsupported table operation")),
                    }
                }
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
            } => {
                if *cascade
                    || *restrict
                    || *purge
                    || *temporary
                    || table.is_some()
                    || names.is_empty()
                {
                    return Err(unsupported("DROP", "unsupported DROP options"));
                }
                for name in names {
                    let name = crate::sql::simple_object_name(name, "object")?;
                    let table_id = match object_type {
                        ObjectType::Table => self
                            .database
                            .catalog()
                            .tables()
                            .find(|table| table.name == name)
                            .map(|table| table.id),
                        ObjectType::Index => self
                            .database
                            .catalog()
                            .indexes()
                            .find(|index| {
                                self.database.catalog().index_name(index.id) == Some(name)
                            })
                            .map(|index| index.table),
                        _ => return Err(unsupported("DROP", "only TABLE and INDEX are supported")),
                    };
                    if let Some(id) = table_id {
                        self.access.write.insert(id);
                    } else if !if_exists {
                        return Err(DbError::InvalidState(format!(
                            "object {name} does not exist"
                        )));
                    }
                }
            }
            _ => return Err(unsupported("DDL", "unsupported schema statement")),
        }
        Ok(())
    }

    fn constraint(
        &mut self,
        constraint: &TableConstraint,
        table: &TableDefinition,
        creating: bool,
    ) -> Result<()> {
        match constraint {
            TableConstraint::PrimaryKey(primary) if creating => {
                if primary.index_type.is_some()
                    || primary.index_name.is_some()
                    || !primary.index_options.is_empty()
                    || primary.characteristics.is_some()
                {
                    return Err(unsupported(
                        "PRIMARY KEY",
                        "only immediate plain keys are supported",
                    ));
                }
                for key in &primary.columns {
                    self.constraint_key(key, table)?;
                }
            }
            TableConstraint::Unique(unique) => {
                if unique.index_type.is_some()
                    || !unique.index_options.is_empty()
                    || unique.characteristics.is_some()
                    || unique.nulls_distinct != NullsDistinctOption::None
                {
                    return Err(unsupported(
                        "UNIQUE",
                        "only plain immediate constraints are supported",
                    ));
                }
                for key in &unique.columns {
                    self.constraint_key(key, table)?;
                }
            }
            TableConstraint::ForeignKey(foreign) => {
                foreign_key_delete_action(foreign)?;
                let name = crate::sql::simple_object_name(&foreign.foreign_table, "table")?;
                let referenced = if name == table.name {
                    table
                } else {
                    crate::sql::find_table(self.database.catalog(), name)?
                };
                if name != table.name || !creating {
                    self.access.read.insert(referenced.id);
                }
                for (column, reference) in foreign.columns.iter().zip(&foreign.referred_columns) {
                    let column = crate::sql::column_position_by_name(table, &column.value)?;
                    let reference =
                        crate::sql::column_position_by_name(referenced, &reference.value)?;
                    super::common_type(
                        Some(table.columns[column].data_type),
                        Some(referenced.columns[reference].data_type),
                    )?;
                }
            }
            _ => return Err(unsupported("constraint", "unsupported table constraint")),
        }
        Ok(())
    }

    fn constraint_key(
        &self,
        key: &sqlparser::ast::IndexColumn,
        table: &TableDefinition,
    ) -> Result<()> {
        if key.operator_class.is_some()
            || key.column.options.asc == Some(false)
            || key.column.options.nulls_first.is_some()
            || key.column.with_fill.is_some()
        {
            return Err(unsupported(
                "constraint",
                "only plain ascending columns are supported",
            ));
        }
        table_scope(table).resolve(&key.column.expr)?;
        Ok(())
    }

    fn index_predicate(&mut self, expr: &Expr, scope: &Scope) -> Result<()> {
        match expr {
            Expr::Nested(inner) => self.index_predicate(inner, scope),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                self.index_predicate(left, scope)?;
                self.index_predicate(right, scope)
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let (column, literal) = if super::expressions::is_column(left) {
                    (left, right)
                } else {
                    (right, left)
                };
                let ty = scope.resolve(column)?.column.ty.kind;
                // Parameters cannot survive into a durable catalog predicate.
                if !static_literal(literal) {
                    return Err(unsupported(
                        "CREATE INDEX",
                        "partial predicates require literal equality",
                    ));
                }
                self.expr(literal, scope, Mode::Literal, ty)?;
                Ok(())
            }
            column if super::expressions::is_column(column) => {
                let ty = scope.ty(column)?;
                super::common_type(ty.kind, Some(ColumnType::Bool))?;
                Ok(())
            }
            _ => Err(unsupported(
                "CREATE INDEX",
                "partial predicates require boolean columns or equality conjunctions",
            )),
        }
    }
}

/// Shared by non-executing binding and schema publication, so a restored
/// constraint cannot be accepted with different options at either boundary.
pub(crate) fn foreign_key_delete_action(
    foreign: &sqlparser::ast::ForeignKeyConstraint,
) -> Result<crate::ReferentialAction> {
    use crate::ReferentialAction;
    use sqlparser::ast::ReferentialAction as SqlAction;

    if foreign.index_name.is_some()
        || foreign.on_update.is_some()
        || foreign.match_kind.is_some()
        || foreign.characteristics.is_some()
        || foreign.columns.is_empty()
        || foreign.columns.len() != foreign.referred_columns.len()
    {
        return Err(unsupported(
            "FOREIGN KEY",
            "only immediate explicit-column foreign keys without ON UPDATE or MATCH clauses are supported",
        ));
    }
    match foreign.on_delete {
        None | Some(SqlAction::Restrict) => Ok(ReferentialAction::Restrict),
        Some(SqlAction::Cascade) => Ok(ReferentialAction::Cascade),
        Some(SqlAction::SetNull) => Ok(ReferentialAction::SetNull),
        Some(SqlAction::NoAction | SqlAction::SetDefault) => Err(unsupported(
            "FOREIGN KEY",
            "ON DELETE supports only RESTRICT, CASCADE and SET NULL",
        )),
    }
}

fn static_literal(expr: &Expr) -> bool {
    // Literal-mode expression binding below checks every shape and function.
    // Here only exclude values that would leave an unbound catalog parameter.
    sqlparser::ast::visit_expressions(expr, |expr| {
        if matches!(expr, Expr::Value(value) if matches!(value.value, Value::Placeholder(_))) {
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    })
    .is_continue()
}

fn table_scope(table: &TableDefinition) -> Scope {
    Scope {
        relations: vec![table.name.clone()],
        columns: table
            .columns
            .iter()
            .map(|column| super::scope::ScopedColumn {
                qualifier: table.name.clone(),
                merged: false,
                coalesced: None,
                column: super::Column {
                    name: column.name.clone(),
                    ty: super::Type::known(column.data_type),
                },
            })
            .collect(),
    }
}

fn column_type(column: &ColumnDef, adding: bool) -> Result<(ColumnType, bool, bool)> {
    let mut nullable = true;
    let mut primary = false;
    for option in &column.options {
        if option.name.is_some() {
            return Err(unsupported(
                "column",
                "named column constraints are not supported",
            ));
        }
        match &option.option {
            ColumnOption::Null => nullable = true,
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Default(Expr::Value(value)) if !adding && value.value == Value::Null => {}
            ColumnOption::PrimaryKey(key) if !adding => {
                if primary
                    || key.name.is_some()
                    || key.index_name.is_some()
                    || key.index_type.is_some()
                    || !key.index_options.is_empty()
                    || key.characteristics.is_some()
                {
                    return Err(unsupported(
                        "column",
                        "only one unnamed PRIMARY KEY is supported",
                    ));
                }
                primary = true;
                nullable = false;
            }
            _ => {
                return Err(unsupported(
                    "column",
                    "unsupported column constraint or default",
                ));
            }
        }
    }
    if adding && !nullable {
        return Err(unsupported("ALTER TABLE", "added columns must be nullable"));
    }
    Ok((schema_type(&column.data_type)?, nullable, primary))
}

fn schema_type(ty: &DataType) -> Result<ColumnType> {
    let ty = sql_type(ty)?;
    // UInt64 is a native logical type and a binder-generated parameter cast,
    // not a supported SQL CREATE/ALTER column type in the current schema tier.
    if ty == ColumnType::U64 {
        return Err(unsupported(
            "data type",
            "UInt64 columns require the native schema API",
        ));
    }
    Ok(ty)
}

/// Pure SQL/cast type authority. Match AST variants, not display prefixes:
/// an unknown custom type named VARCHARHACK must not become Text.
pub(crate) fn sql_type(ty: &DataType) -> Result<ColumnType> {
    use sqlparser::ast::ExactNumberInfo;
    Ok(match ty {
        DataType::Bool | DataType::Boolean => ColumnType::Bool,
        DataType::Text
        | DataType::Char(_)
        | DataType::Character(_)
        | DataType::Varchar(_)
        | DataType::CharacterVarying(_)
        | DataType::CharVarying(_)
        | DataType::CharacterLargeObject(_) => ColumnType::Text,
        DataType::Bytea | DataType::Blob(None) | DataType::Bytes(_) | DataType::Varbinary(_) => {
            ColumnType::Bytes
        }
        DataType::BigInt(None)
        | DataType::Int(None)
        | DataType::Integer(None)
        | DataType::Int2(None)
        | DataType::Int4(None)
        | DataType::Int8(None)
        | DataType::SmallInt(None) => ColumnType::I64,
        DataType::DoublePrecision
        | DataType::Double(ExactNumberInfo::None)
        | DataType::Float8
        | DataType::Float64 => ColumnType::Float64,
        // REAL8 is the existing tier's one custom floating-point alias.
        DataType::Custom(name, parameters)
            if parameters.is_empty()
                && name.0.len() == 1
                && name.0[0]
                    .as_ident()
                    .is_some_and(|name| name.value.eq_ignore_ascii_case("real8")) =>
        {
            ColumnType::Float64
        }
        DataType::Date => ColumnType::Date,
        DataType::Uuid => ColumnType::Uuid,
        DataType::UInt64 => ColumnType::U64,
        DataType::Timestamp(_, TimezoneInfo::None) => ColumnType::Timestamp,
        DataType::Numeric(_) | DataType::Decimal(_) | DataType::Dec(_) => ColumnType::Decimal,
        _ => return Err(unsupported("data type", "unsupported SQL type")),
    })
}
