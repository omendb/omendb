use sqlparser::ast::{
    BinaryOperator as Op, Expr, Function, FunctionArg, FunctionArgExpr, GroupByExpr,
    JoinConstraint, JoinOperator, LimitClause, OrderBy, OrderByKind, Query, Select, SelectItem,
    SetExpr, SetOperator, SetQuantifier, UnaryOperator, WindowType,
};

use crate::{ColumnType, DbError, Result};

use super::{
    Binder, Column, Type,
    expressions::{Mode, arguments, comparison, expression_argument, is_column},
    scope::Scope,
    unsupported,
};

impl Binder<'_> {
    pub(super) fn query(
        &mut self,
        query: &Query,
        expected: &[Option<ColumnType>],
    ) -> Result<Vec<Column>> {
        if query.with.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return Err(unsupported("SELECT", "query modifiers are not supported"));
        }
        let columns = self.set_expr(&query.body, expected)?;
        if self.prefer_outputs && columns.iter().any(|column| column.ty.kind.is_none()) {
            let preferred = columns
                .iter()
                .map(|column| Some(column.ty.kind.unwrap_or(ColumnType::Text)))
                .collect::<Vec<_>>();
            self.constrain_outputs(&query.body, &preferred)?;
        }
        if let Some(order) = &query.order_by {
            let (scope, output_only) = match query.body.as_ref() {
                SetExpr::Select(select) if select.from.is_empty() => {
                    return Err(unsupported("SELECT", "literal SELECT has no ORDER BY"));
                }
                SetExpr::Select(select) if !aggregate_select(select) => (self.from(select)?, false),
                SetExpr::Select(select)
                    if select
                        .from
                        .first()
                        .is_some_and(|from| !from.joins.is_empty()) =>
                {
                    // Joined grouping can order by an unprojected group key;
                    // those keys must not leak into the statement's signature.
                    let input = self.from(select)?;
                    let mut order_columns = columns.clone();
                    if let GroupByExpr::Expressions(groups, _) = &select.group_by {
                        for group in groups {
                            let name = input.resolve(group)?.column.name.clone();
                            if !order_columns.iter().any(|column| column.name == name) {
                                order_columns.push(Column {
                                    name,
                                    ty: input.ty(group)?,
                                });
                            }
                        }
                    }
                    (Scope::output(&order_columns), true)
                }
                _ => (Scope::output(&columns), true),
            };
            self.order(order, &scope, output_only)?;
        }
        if let Some(limit) = &query.limit_clause {
            self.limit(limit)?;
        }
        Ok(columns)
    }

    pub(super) fn constrain_outputs(
        &mut self,
        body: &SetExpr,
        expected: &[Option<ColumnType>],
    ) -> Result<()> {
        let context = self
            .output_contexts
            .entry(body as *const SetExpr)
            .or_default();
        if context.len() < expected.len() {
            context.resize(expected.len(), None);
            self.outputs_changed = true;
        }
        for (current, expected) in context.iter_mut().zip(expected) {
            let common = super::common_type(*current, *expected)?;
            self.outputs_changed |= *current != common;
            *current = common;
        }
        Ok(())
    }

    fn set_expr(&mut self, body: &SetExpr, expected: &[Option<ColumnType>]) -> Result<Vec<Column>> {
        #[cfg(test)]
        {
            self.set_visits += 1;
        }
        self.constrain_outputs(body, expected)?;
        let expected = self.output_contexts[&(body as *const SetExpr)].clone();
        let columns = match body {
            SetExpr::Select(select) => self.select(select, &expected),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                if !matches!(
                    op,
                    SetOperator::Union | SetOperator::Intersect | SetOperator::Except
                ) || !matches!(
                    set_quantifier,
                    SetQuantifier::None | SetQuantifier::Distinct | SetQuantifier::All
                ) {
                    return Err(unsupported(
                        "set operation",
                        "unsupported operator or quantifier",
                    ));
                }
                // ALL for INTERSECT/EXCEPT is not implemented by the evaluator.
                if *set_quantifier == SetQuantifier::All && *op != SetOperator::Union {
                    return Err(unsupported(
                        "set operation",
                        "ALL is only supported for UNION",
                    ));
                }
                let mut lhs = self.set_expr(left, &expected)?;
                let rhs = self.set_expr(right, &expected)?;
                if lhs.len() != rhs.len() {
                    return Err(DbError::InvalidState(
                        "set operation sides have different column counts".into(),
                    ));
                }
                for (left, right) in lhs.iter_mut().zip(rhs) {
                    left.ty = left.ty.common(right.ty)?;
                }
                for (column, context) in lhs.iter_mut().zip(&expected) {
                    column.ty.kind = super::common_type(column.ty.kind, *context)?;
                }
                Ok(lhs)
            }
            // Parenthesized Query bodies are not accepted by execute_query.
            _ => Err(unsupported(
                "SELECT",
                "only SELECT and supported set operations are executable",
            )),
        }?;
        let inferred = columns
            .iter()
            .map(|column| column.ty.kind)
            .collect::<Vec<_>>();
        self.constrain_outputs(body, &inferred)?;
        Ok(columns)
    }

    fn select(&mut self, select: &Select, expected: &[Option<ColumnType>]) -> Result<Vec<Column>> {
        if !select.named_window.is_empty() {
            return Err(unsupported("OVER", "named windows are not supported"));
        }
        if !select.optimizer_hints.is_empty()
            || select.select_modifiers.is_some()
            || select.top.is_some()
            || select.exclude.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.qualify.is_some()
            || select.value_table_mode.is_some()
            || select
                .distinct
                .as_ref()
                .is_some_and(|distinct| !matches!(distinct, sqlparser::ast::Distinct::Distinct))
        {
            return Err(unsupported(
                "SELECT",
                "only plain SELECT clauses are supported",
            ));
        }
        let GroupByExpr::Expressions(groups, modifiers) = &select.group_by else {
            return Err(unsupported("GROUP BY", "GROUP BY ALL is not supported"));
        };
        if !modifiers.is_empty() {
            return Err(unsupported(
                "GROUP BY",
                "grouping modifiers are not supported",
            ));
        }
        let scope = self.from(select)?;
        let joined = select
            .from
            .first()
            .is_some_and(|from| !from.joins.is_empty());
        let aggregate = if select.from.is_empty() {
            !groups.is_empty()
        } else if joined {
            !groups.is_empty()
                || !select.projection.is_empty()
                    && select
                        .projection
                        .iter()
                        .all(|item| matches!(projection_expr(item), Some(Expr::Function(_))))
        } else {
            aggregate_select(select)
        };
        if select.from.is_empty()
            && (select.selection.is_some() || aggregate || select.having.is_some())
        {
            return Err(unsupported(
                "SELECT",
                "literal SELECT only supports literal projection",
            ));
        }
        if let Some(selection) = &select.selection {
            self.predicate(selection, &scope, joined, true)?;
        }
        for group in groups {
            scope.resolve(group)?;
        }
        if select.having.is_some() && (!aggregate || !joined) {
            return Err(unsupported(
                "HAVING",
                "HAVING is only implemented for grouped joins",
            ));
        }
        let mut columns = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(options)
                    if options.to_string().is_empty()
                        && !aggregate
                        && !scope.columns.is_empty() =>
                {
                    for column in scope.columns.iter().filter(|column| !column.merged) {
                        columns.push(Column {
                            name: if joined {
                                format!("{}.{}", column.qualifier, column.column.name)
                            } else {
                                column.column.name.clone()
                            },
                            ty: column.visible_type(),
                        });
                    }
                }
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    let hint = expected.get(columns.len()).copied().flatten();
                    let ty = if aggregate {
                        if is_column(expr) {
                            scope.resolve(expr)?;
                            if !groups.iter().any(|group| scope.same_column(expr, group)) {
                                return Err(DbError::InvalidState(
                                    "column must appear in GROUP BY or an aggregate".into(),
                                ));
                            }
                            scope.ty(expr)?
                        } else if let Expr::Function(function) = expr {
                            self.aggregate(function, &scope, joined, false)?
                        } else if !joined {
                            self.expr(expr, &scope, Mode::Literal, hint)?
                        } else {
                            return Err(unsupported(
                                "JOIN",
                                "grouped projections must be columns or aggregates",
                            ));
                        }
                    } else if let Expr::Function(function) = expr
                        && function.over.is_some()
                    {
                        if joined || select.from.is_empty() {
                            return Err(unsupported(
                                "OVER",
                                "windows require a single-table plain SELECT",
                            ));
                        }
                        self.window(function, &scope)?
                    } else if let Expr::Subquery(query) = expr {
                        if joined || select.from.is_empty() {
                            return Err(unsupported(
                                "subquery",
                                "scalar subqueries require a single-table projection",
                            ));
                        }
                        let columns = self.query(query, &[hint])?;
                        if columns.len() != 1 {
                            return Err(unsupported(
                                "subquery",
                                "scalar subquery must return one column",
                            ));
                        }
                        columns[0].ty
                    } else {
                        let mode = if select.from.is_empty() || joined && !is_column(expr) {
                            Mode::Literal
                        } else {
                            Mode::Computed
                        };
                        self.expr(expr, &scope, mode, hint)?
                    };
                    let name = match item {
                        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                        _ if is_column(expr) => scope.resolve(expr)?.column.name.clone(),
                        _ => match expr {
                            Expr::Function(function) if aggregate || function.over.is_some() => {
                                function.name.to_string().to_ascii_lowercase()
                            }
                            _ if joined => "literal".into(),
                            _ => expr.to_string(),
                        },
                    };
                    columns.push(Column { name, ty });
                }
                _ => return Err(unsupported("SELECT", "unsupported projection item")),
            }
        }
        if columns.is_empty() {
            return Err(unsupported("SELECT", "at least one column is required"));
        }
        if let Some(having) = &select.having {
            // HAVING sees group keys even when they are not selected. Column
            // projection aliases do not rename the grouping namespace.
            let mut having_columns = Vec::new();
            for group in groups {
                having_columns.push(Column {
                    name: scope.resolve(group)?.column.name.clone(),
                    ty: scope.ty(group)?,
                });
            }
            for (item, column) in select.projection.iter().zip(&columns) {
                if matches!(projection_expr(item), Some(Expr::Function(_))) {
                    having_columns.push(column.clone());
                }
            }
            self.having(having, select, &scope, &Scope::output(&having_columns))?;
        }
        Ok(columns)
    }

    fn from(&mut self, select: &Select) -> Result<Scope> {
        let Some(from) = select.from.first() else {
            return Ok(Scope::default());
        };
        if select.from.len() != 1 {
            return Err(unsupported(
                "FROM",
                "only one joined table tree is supported",
            ));
        }
        let (mut scope, _) = self.factor(&from.relation, true)?;
        for join in &from.joins {
            if join.global {
                return Err(unsupported("JOIN", "GLOBAL JOIN is not supported"));
            }
            let (mut incoming, _) = self.factor(&join.relation, true)?;
            let constraint = match &join.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c)
                | JoinOperator::CrossJoin(c) => c,
                _ => return Err(unsupported("JOIN", "unsupported join operator")),
            };
            if matches!(join.join_operator, JoinOperator::CrossJoin(_))
                && !matches!(constraint, JoinConstraint::None)
            {
                return Err(unsupported("JOIN", "CROSS JOIN cannot have a constraint"));
            }
            match constraint {
                JoinConstraint::On(expr) => {
                    let mut combined = scope.clone();
                    combined.append(incoming.clone())?;
                    self.on(expr, &scope, &incoming, &combined)?;
                }
                JoinConstraint::Using(names) => {
                    if names.is_empty() {
                        return Err(unsupported("JOIN", "USING requires columns"));
                    }
                    for name in names {
                        let name = crate::sql::simple_object_name(name, "USING column")?;
                        let reference = Expr::Identifier(sqlparser::ast::Ident::new(name));
                        let right = incoming.ty(&reference)?;
                        scope.merge(name, right)?;
                        for column in &mut incoming.columns {
                            if column.column.name == name {
                                column.merged = true;
                            }
                        }
                    }
                }
                JoinConstraint::Natural => {
                    let shared = incoming
                        .columns
                        .iter()
                        .filter(|right| {
                            scope
                                .columns
                                .iter()
                                .any(|left| !left.merged && left.column.name == right.column.name)
                        })
                        .map(|column| column.column.name.clone())
                        .collect::<Vec<_>>();
                    if shared.is_empty() {
                        return Err(unsupported("JOIN", "NATURAL JOIN found no shared columns"));
                    }
                    for name in shared {
                        let reference = Expr::Identifier(sqlparser::ast::Ident::new(&name));
                        scope.merge(&name, incoming.ty(&reference)?)?;
                        for column in &mut incoming.columns {
                            if column.column.name == name {
                                column.merged = true;
                            }
                        }
                    }
                }
                JoinConstraint::None => {}
            }
            scope.append(incoming)?;
        }
        Ok(scope)
    }

    fn on(
        &mut self,
        expr: &Expr,
        left_scope: &Scope,
        right_scope: &Scope,
        combined: &Scope,
    ) -> Result<()> {
        if let Expr::Nested(inner) = expr {
            return self.on(inner, left_scope, right_scope, combined);
        }
        if let Expr::BinaryOp { left, op, right } = expr {
            if *op == Op::And {
                self.on(left, left_scope, right_scope, combined)?;
                return self.on(right, left_scope, right_scope, combined);
            }
            if comparison(op) && is_column(left) && is_column(right) {
                // Resolve in the whole scope first to reject ambiguity. Each ON
                // comparison must span the accumulated and incoming relations.
                combined.resolve(left)?;
                combined.resolve(right)?;
                if !(left_scope.resolve(left).is_ok() && right_scope.resolve(right).is_ok()
                    || left_scope.resolve(right).is_ok() && right_scope.resolve(left).is_ok())
                {
                    return Err(unsupported(
                        "JOIN",
                        "ON must compare opposite sides of the join",
                    ));
                }
                return self.comparable(left, right, combined, Mode::Operand);
            }
        }
        Err(unsupported(
            "JOIN",
            "ON must be AND-connected column comparisons",
        ))
    }

    pub(super) fn predicate(
        &mut self,
        expr: &Expr,
        scope: &Scope,
        joined: bool,
        subqueries: bool,
    ) -> Result<()> {
        match expr {
            Expr::Nested(inner) if !joined => self.predicate(inner, scope, joined, subqueries),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } if !joined => self.predicate(expr, scope, joined, subqueries),
            Expr::BinaryOp {
                left,
                op: Op::And | Op::Or,
                right,
            } => {
                self.predicate(left, scope, joined, subqueries)?;
                self.predicate(right, scope, joined, subqueries)
            }
            Expr::BinaryOp { left, op, right } if comparison(op) => {
                if joined {
                    let left_mode = if is_column(left) {
                        Mode::Operand
                    } else {
                        Mode::Literal
                    };
                    let right_mode = if is_column(right) {
                        Mode::Operand
                    } else {
                        Mode::Literal
                    };
                    let ty = self
                        .expr(left, scope, left_mode, None)?
                        .common(self.expr(right, scope, right_mode, None)?)?;
                    self.expr(left, scope, left_mode, ty.kind)?;
                    self.expr(right, scope, right_mode, ty.kind)?;
                    Ok(())
                } else {
                    self.comparable(left, right, scope, Mode::Operand)
                }
            }
            Expr::InList { expr, list, .. } => {
                if joined {
                    scope.resolve(expr)?;
                }
                let mut ty = self.expr(expr, scope, Mode::Operand, None)?;
                for item in list {
                    ty = ty.common(self.expr(item, scope, Mode::Literal, None)?)?;
                }
                self.expr(expr, scope, Mode::Operand, ty.kind)?;
                for item in list {
                    self.expr(item, scope, Mode::Literal, ty.kind)?;
                }
                Ok(())
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                if joined {
                    scope.resolve(expr)?;
                    self.expr(low, scope, Mode::Literal, None)?;
                    self.expr(high, scope, Mode::Literal, None)?;
                }
                self.comparable(expr, low, scope, Mode::Operand)?;
                self.comparable(expr, high, scope, Mode::Operand)
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) if !joined => {
                self.expr(inner, scope, Mode::Operand, None)?;
                Ok(())
            }
            Expr::InSubquery { expr, subquery, .. } if !joined && subqueries => {
                let ty = self.expr(expr, scope, Mode::Operand, None)?;
                let columns = self.query(subquery, &[ty.kind])?;
                if columns.len() != 1 {
                    return Err(unsupported("IN", "subquery must return one column"));
                }
                let common = ty.common(columns[0].ty)?;
                self.expr(expr, scope, Mode::Operand, common.kind)?;
                self.constrain_outputs(&subquery.body, &[common.kind])?;
                Ok(())
            }
            Expr::Exists { subquery, .. } if !joined && subqueries => {
                self.query(subquery, &[])?;
                Ok(())
            }
            _ if !joined => {
                self.expr(expr, scope, Mode::Operand, Some(ColumnType::Bool))?;
                Ok(())
            }
            _ => Err(unsupported("WHERE", "unsupported joined predicate")),
        }
    }

    fn aggregate(
        &mut self,
        function: &Function,
        scope: &Scope,
        joined: bool,
        window: bool,
    ) -> Result<Type> {
        if function.over.is_some() && !window {
            return Err(unsupported(
                "aggregate",
                "windows cannot be combined with grouping",
            ));
        }
        let args = arguments(function)?;
        let name = function.name.to_string().to_ascii_lowercase();
        if !matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max")
            || joined && name == "avg"
        {
            return Err(unsupported(
                "aggregate",
                &format!("{name} is not a supported aggregate function"),
            ));
        }
        let ty = match args.as_slice() {
            [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] if name == "count" => Type::default(),
            [] if window && name == "count" => Type::default(),
            [arg] => scope.ty(expression_argument(arg)?)?,
            _ => {
                return Err(unsupported(
                    "aggregate",
                    "expected one column argument or COUNT(*)",
                ));
            }
        };
        if matches!(name.as_str(), "sum" | "avg") {
            self.require_numeric(ty)?;
        }
        Ok(match name.as_str() {
            "count" => Type::known(ColumnType::I64),
            "avg" => Type::known(ColumnType::Float64),
            _ => ty,
        })
    }

    fn window(&mut self, function: &Function, scope: &Scope) -> Result<Type> {
        let Some(WindowType::WindowSpec(spec)) = &function.over else {
            return Err(unsupported("OVER", "named windows are not supported"));
        };
        if spec.window_name.is_some() {
            return Err(unsupported("OVER", "named windows are not supported"));
        }
        if spec.window_frame.is_some() {
            return Err(unsupported("OVER", "window frames are not supported"));
        }
        for expr in &spec.partition_by {
            scope.resolve(expr)?;
        }
        for term in &spec.order_by {
            if term.with_fill.is_some() {
                return Err(unsupported("OVER", "WITH FILL is not supported"));
            }
            scope.resolve(&term.expr)?;
        }
        let args = arguments(function)?;
        match function.name.to_string().to_ascii_lowercase().as_str() {
            "row_number" | "rank" | "dense_rank" if args.is_empty() => {
                Ok(Type::known(ColumnType::I64))
            }
            "lag" | "lead" | "first_value" | "last_value" if args.len() == 1 => {
                scope.ty(expression_argument(args[0])?)
            }
            _ => self.aggregate(function, scope, false, true),
        }
    }

    fn having(
        &mut self,
        expr: &Expr,
        select: &Select,
        input: &Scope,
        output: &Scope,
    ) -> Result<()> {
        let Expr::BinaryOp { left, op, right } = expr else {
            return Err(unsupported("HAVING", "expected a comparison"));
        };
        if matches!(op, Op::And | Op::Or) {
            self.having(left, select, input, output)?;
            return self.having(right, select, input, output);
        }
        if !comparison(op) {
            return Err(unsupported("HAVING", "unsupported operator"));
        }
        let ty = if let Expr::Function(function) = left.as_ref() {
            if !select
                .projection
                .iter()
                .any(|item| projection_expr(item).is_some_and(|expr| expr == left.as_ref()))
            {
                return Err(unsupported(
                    "HAVING",
                    "aggregate must appear in SELECT projection",
                ));
            }
            self.aggregate(function, input, true, false)?
        } else {
            output.ty(left)?
        };
        let common = ty.common(self.expr(right, output, Mode::Literal, None)?)?;
        self.expr(right, output, Mode::Literal, common.kind)?;
        Ok(())
    }

    fn order(&mut self, order: &OrderBy, scope: &Scope, output_only: bool) -> Result<()> {
        let OrderByKind::Expressions(terms) = &order.kind else {
            return Err(unsupported("ORDER BY", "ORDER BY ALL is not supported"));
        };
        if order.interpolate.is_some() || terms.is_empty() {
            return Err(unsupported("ORDER BY", "unsupported ordering"));
        }
        for term in terms {
            if term.with_fill.is_some()
                || !is_column(&term.expr)
                || output_only && !matches!(term.expr, Expr::Identifier(_))
            {
                return Err(unsupported("ORDER BY", "ordering requires plain columns"));
            }
            scope.resolve(&term.expr)?;
        }
        Ok(())
    }

    fn limit(&mut self, limit: &LimitClause) -> Result<()> {
        let expressions = match limit {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } if limit_by.is_empty() => limit
                .iter()
                .chain(offset.iter().map(|offset| &offset.value))
                .collect::<Vec<_>>(),
            LimitClause::OffsetCommaLimit { offset, limit } => vec![offset, limit],
            _ => return Err(unsupported("LIMIT", "LIMIT BY is not supported")),
        };
        for expr in expressions {
            let ty = self.expr(
                expr,
                &Scope::default(),
                Mode::Literal,
                Some(ColumnType::I64),
            )?;
            if !ty
                .kind
                .is_some_and(|ty| matches!(ty, ColumnType::I64 | ColumnType::U64))
                || ty.string
                || negative_literal(expr)
                || null_literal(expr)
            {
                return Err(DbError::InvalidState(
                    "LIMIT/OFFSET requires a non-negative integer".into(),
                ));
            }
        }
        Ok(())
    }
}

fn null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::Cast { expr: inner, .. } => null_literal(inner),
        Expr::Value(value) => matches!(value.value, sqlparser::ast::Value::Null),
        _ => false,
    }
}

fn negative_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner)
        | Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr: inner,
        } => negative_literal(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(value) => {
                matches!(&value.value, sqlparser::ast::Value::Number(number, _) if number.parse::<i64>().is_ok_and(|number| number > 0))
            }
            _ => false,
        },
        _ => false,
    }
}

fn projection_expr(item: &SelectItem) -> Option<&Expr> {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => Some(expr),
        _ => None,
    }
}

fn aggregate_select(select: &Select) -> bool {
    !matches!(&select.group_by, GroupByExpr::Expressions(groups, modifiers) if groups.is_empty() && modifiers.is_empty())
        || select.projection.iter().filter_map(projection_expr).any(|expr| matches!(expr, Expr::Function(function)
            if function.over.is_none() && !function.name.to_string().eq_ignore_ascii_case("coalesce")
                && crate::sql::values::scalar_function_arity(&function.name.to_string()).is_none()))
}
