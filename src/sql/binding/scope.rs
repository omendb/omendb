use sqlparser::ast::{Expr, TableFactor};

use crate::{DbError, Result, TableDefinition};

use super::{Binder, Column, Type, unsupported};

#[derive(Clone, Default)]
pub(super) struct Scope {
    pub columns: Vec<ScopedColumn>,
    pub relations: Vec<String>,
}

#[derive(Clone)]
pub(super) struct ScopedColumn {
    pub qualifier: String,
    pub column: Column,
    // USING/NATURAL duplicates remain available qualified, but not in * or
    // unqualified lookup. This also keeps ON resolution separate from output.
    pub merged: bool,
    // The unqualified USING/NATURAL column has a common result domain;
    // qualified references still have the originating column's domain.
    pub coalesced: Option<Type>,
}

impl ScopedColumn {
    pub fn visible_type(&self) -> Type {
        self.coalesced.unwrap_or(self.column.ty)
    }
}

impl Scope {
    pub fn resolve(&self, expression: &Expr) -> Result<&ScopedColumn> {
        let (qualifier, name) = match expression {
            Expr::Identifier(name) => (None, name.value.as_str()),
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                (Some(parts[0].value.as_str()), parts[1].value.as_str())
            }
            _ => return Err(unsupported("column", "expected a column or table.column")),
        };
        let mut found = self.columns.iter().filter(|column| {
            column.column.name == name
                && match qualifier {
                    Some(qualifier) => qualifier == column.qualifier,
                    None => !column.merged,
                }
        });
        let first = found.next().ok_or_else(|| DbError::SqlUndefinedColumn {
            name: expression.to_string(),
        })?;
        if found.next().is_some() {
            return Err(unsupported(
                "column",
                "ambiguous unqualified column; qualify it",
            ));
        }
        Ok(first)
    }

    pub fn ty(&self, expression: &Expr) -> Result<Type> {
        let column = self.resolve(expression)?;
        Ok(if matches!(expression, Expr::Identifier(_)) {
            column.visible_type()
        } else {
            column.column.ty
        })
    }

    pub fn merge(&mut self, name: &str, incoming: Type) -> Result<()> {
        let reference = Expr::Identifier(sqlparser::ast::Ident::new(name));
        let common = self.ty(&reference)?.common(incoming)?;
        let column = self
            .columns
            .iter_mut()
            .find(|column| !column.merged && column.column.name == name)
            .expect("unique unqualified column was resolved above");
        column.coalesced = Some(common);
        Ok(())
    }

    pub fn same_column(&self, left: &Expr, right: &Expr) -> bool {
        let (Ok(left_column), Ok(right_column)) = (self.resolve(left), self.resolve(right)) else {
            return false;
        };
        std::ptr::eq(left_column, right_column)
            && (left_column.coalesced.is_none()
                || matches!(left, Expr::Identifier(_)) == matches!(right, Expr::Identifier(_)))
    }

    pub fn append(&mut self, other: Self) -> Result<()> {
        if other
            .relations
            .iter()
            .any(|name| self.relations.contains(name))
        {
            return Err(unsupported(
                "FROM",
                "duplicate relation name; use distinct aliases",
            ));
        }
        self.columns.extend(other.columns);
        self.relations.extend(other.relations);
        Ok(())
    }

    pub fn output(columns: &[Column]) -> Self {
        Self {
            columns: columns
                .iter()
                .cloned()
                .map(|column| ScopedColumn {
                    qualifier: String::new(),
                    column,
                    merged: false,
                    coalesced: None,
                })
                .collect(),
            relations: Vec::new(),
        }
    }
}

impl<'a> Binder<'a> {
    pub(super) fn factor(
        &mut self,
        factor: &TableFactor,
        read: bool,
    ) -> Result<(Scope, &'a TableDefinition)> {
        let TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } = factor
        else {
            return Err(unsupported(
                "FROM",
                "only plain catalog tables are supported",
            ));
        };
        if args.is_some()
            || !with_hints.is_empty()
            || version.is_some()
            || *with_ordinality
            || !partitions.is_empty()
            || json_path.is_some()
            || sample.is_some()
            || !index_hints.is_empty()
            || alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
        {
            return Err(unsupported(
                "FROM",
                "table modifiers and column aliases are not supported",
            ));
        }
        let name = crate::sql::simple_object_name(name, "table")?;
        let table = crate::sql::find_table(self.database.catalog(), name)?;
        if read {
            self.access.read.insert(table.id);
        }
        let qualifier = alias
            .as_ref()
            .map_or(name, |alias| alias.name.value.as_str())
            .to_owned();
        Ok((
            Scope {
                columns: table
                    .columns
                    .iter()
                    .map(|column| ScopedColumn {
                        qualifier: qualifier.clone(),
                        column: Column {
                            name: column.name.clone(),
                            ty: Type::known(column.data_type),
                        },
                        merged: false,
                        coalesced: None,
                    })
                    .collect(),
                relations: vec![qualifier],
            },
            table,
        ))
    }
}
