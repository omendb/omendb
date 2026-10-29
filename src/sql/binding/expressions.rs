use sqlparser::ast::{
    BinaryOperator as Op, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    UnaryOperator, Value as Literal,
};

use crate::{ColumnType, DbError, Result};

use super::{Binder, Type, common_type, numeric, scope::Scope, unsupported};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Mode {
    Literal,
    Computed,
    // eval_expression supports arithmetic and columns, but scalar functions
    // here still go through the literal evaluator, not plan_computed.
    Operand,
}

pub(super) fn is_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

pub(super) fn comparison(op: &Op) -> bool {
    matches!(
        op,
        Op::Eq | Op::NotEq | Op::Lt | Op::LtEq | Op::Gt | Op::GtEq
    )
}

pub(super) fn arguments(function: &Function) -> Result<Vec<&FunctionArg>> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
    {
        return Err(unsupported(
            "function",
            "function modifiers are not supported",
        ));
    }
    match &function.args {
        FunctionArguments::None => Ok(Vec::new()),
        FunctionArguments::List(list)
            if list.duplicate_treatment.is_none() && list.clauses.is_empty() =>
        {
            Ok(list.args.iter().collect())
        }
        _ => Err(unsupported(
            "function",
            "only plain arguments are supported",
        )),
    }
}

pub(super) fn expression_argument(arg: &FunctionArg) -> Result<&Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
        _ => Err(unsupported(
            "function",
            "expected an unnamed expression argument",
        )),
    }
}

impl Binder<'_> {
    fn parameter(&mut self, name: &str, expected: Option<ColumnType>) -> Result<Type> {
        let index = crate::sql::values::parameter_index(name)?;
        let current = self
            .parameters
            .get_mut(index)
            .ok_or_else(|| DbError::SqlParameter("parameter outside statement signature".into()))?;
        if let Some(expected) = expected {
            if let Some(hint) = self.hints.get(index).copied().flatten() {
                common_type(Some(hint), Some(expected)).map_err(|_| {
                    DbError::SqlDatatypeMismatch {
                        column: format!("{name} declared {hint:?}, required {expected:?}"),
                    }
                })?;
                return Ok(Type::known(hint));
            }
            *current = Some(match *current {
                Some(previous) => common_type(Some(previous), Some(expected))
                    .map_err(|_| DbError::SqlDatatypeMismatch {
                        column: format!(
                            "conflicting types for {name}: {previous:?} and {expected:?}"
                        ),
                    })?
                    .expect("two concrete types have a concrete common domain"),
                None => expected,
            });
        }
        Ok(Type {
            kind: *current,
            string: false,
        })
    }

    pub(super) fn expr(
        &mut self,
        expr: &Expr,
        scope: &Scope,
        mode: Mode,
        expected: Option<ColumnType>,
    ) -> Result<Type> {
        if mode == Mode::Computed && !computed_shape(expr) {
            // projection_plan falls back to literal_value only for an entire
            // expression, never for an arbitrary nested computed term.
            return self.expr(expr, scope, Mode::Literal, expected);
        }
        let ty = match expr {
            Expr::Value(value) => match &value.value {
                Literal::Placeholder(name) => return self.parameter(name, expected),
                Literal::Null => {
                    if let Some(expected) = expected {
                        self.literal_context(expr, expected)?;
                    }
                    Type {
                        kind: expected,
                        string: false,
                    }
                }
                Literal::Number(number, _) => {
                    if number.parse::<i64>().is_ok() {
                        Type::known(ColumnType::I64)
                    } else {
                        crate::sql::values::parse_decimal_text(number).map_err(|_| {
                            DbError::SqlNumericValueOutOfRange(format!(
                                "SQL numeric literal {number} is out of range"
                            ))
                        })?;
                        Type::known(ColumnType::Decimal)
                    }
                }
                Literal::Boolean(_) => Type::known(ColumnType::Bool),
                Literal::SingleQuotedByteStringLiteral(_)
                | Literal::DoubleQuotedByteStringLiteral(_) => Type::known(ColumnType::Bytes),
                Literal::HexStringLiteral(text) => {
                    if text.len() % 2 != 0 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        return Err(DbError::InvalidState("invalid hexadecimal literal".into()));
                    }
                    Type::known(ColumnType::Bytes)
                }
                value if value.clone().into_string().is_some() => {
                    if let Some(expected) = expected {
                        validate_string(
                            &value.clone().into_string().expect("guarded string literal"),
                            expected,
                        )?;
                        self.literal_context(expr, expected)?;
                    }
                    Type {
                        kind: None,
                        string: true,
                    }
                }
                _ => return Err(unsupported("literal", "this literal type is not supported")),
            },
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) if mode != Mode::Literal => {
                scope.ty(expr)?
            }
            Expr::Nested(inner) => return self.expr(inner, scope, mode, expected),
            Expr::UnaryOp {
                op: op @ (UnaryOperator::Plus | UnaryOperator::Minus),
                expr,
            } => {
                // Unary +/- is only implemented by literal_value, even in a
                // computed projection. Do not accept unary column expressions.
                let ty = self.expr(
                    expr,
                    scope,
                    Mode::Literal,
                    expected.filter(|kind| numeric(*kind)),
                )?;
                self.require_numeric(ty)?;
                let ty = self.expr(
                    expr,
                    scope,
                    Mode::Literal,
                    Some(ty.kind.unwrap_or(ColumnType::I64)),
                )?;
                if *op == UnaryOperator::Minus && ty.kind == Some(ColumnType::U64) {
                    return Err(unsupported(
                        "unary minus",
                        "unsigned operands are not supported",
                    ));
                }
                ty
            }
            Expr::BinaryOp { left, op, right }
                if mode != Mode::Literal
                    && matches!(
                        op,
                        Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo
                    ) =>
            {
                let left_type = self.expr(left, scope, mode, None)?;
                let right_type = self.expr(right, scope, mode, None)?;
                let mut result = left_type.common(right_type)?;
                if let Some(expected) = expected.filter(|kind| numeric(*kind)) {
                    result = result.common(Type::known(expected))?;
                }
                self.require_numeric(result)?;
                if *op == Op::Modulo
                    && result.kind.is_some_and(|kind| {
                        matches!(kind, ColumnType::Float64 | ColumnType::Decimal)
                    })
                {
                    return Err(unsupported(
                        "arithmetic",
                        "modulo requires integer operands",
                    ));
                }
                let context = result.kind.or(Some(ColumnType::I64));
                let left_type = self.expr(left, scope, mode, context)?;
                let right_type = self.expr(right, scope, mode, context)?;
                left_type.common(right_type)?
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } if mode == Mode::Computed => {
                if conditions.is_empty() {
                    return Err(unsupported("CASE", "at least one WHEN is required"));
                }
                for arm in conditions {
                    if let Some(operand) = operand {
                        self.comparable(operand, &arm.condition, scope, Mode::Computed)?;
                    } else {
                        match &arm.condition {
                            Expr::BinaryOp { left, op, right } if comparison(op) => {
                                self.comparable(left, right, scope, Mode::Computed)?;
                            }
                            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                                self.expr(inner, scope, Mode::Computed, None)?;
                            }
                            _ => {
                                return Err(unsupported(
                                    "CASE",
                                    "WHEN requires a comparison or NULL test",
                                ));
                            }
                        }
                    }
                }
                let mut expressions: Vec<&Expr> =
                    conditions.iter().map(|arm| &arm.result).collect();
                if let Some(otherwise) = else_result {
                    expressions.push(otherwise);
                }
                self.common_expressions(&expressions, scope, mode, expected)?
            }
            Expr::Function(function) if function.over.is_none() => {
                let args = arguments(function)?;
                let args = args
                    .iter()
                    .map(|arg| expression_argument(arg))
                    .collect::<Result<Vec<_>>>()?;
                let name = function.name.to_string().to_ascii_lowercase();
                let arg_mode = if mode == Mode::Computed {
                    mode
                } else {
                    Mode::Literal
                };
                if name == "coalesce" {
                    if args.is_empty() {
                        return Err(unsupported("COALESCE", "at least one argument is required"));
                    }
                    self.common_expressions(&args, scope, arg_mode, expected)?
                } else if let Some(arity) = crate::sql::values::scalar_function_arity(&name) {
                    if args.len() != arity {
                        return Err(unsupported("function", "incorrect argument count"));
                    }
                    match name.as_str() {
                        "upper" | "lower" | "btrim" | "ltrim" | "rtrim" | "length"
                        | "char_length" | "character_length" => {
                            self.expr(args[0], scope, arg_mode, Some(ColumnType::Text))?;
                            Type::known(
                                if matches!(
                                    name.as_str(),
                                    "length" | "char_length" | "character_length"
                                ) {
                                    ColumnType::I64
                                } else {
                                    ColumnType::Text
                                },
                            )
                        }
                        "date_part" | "date_trunc" => {
                            self.expr(args[0], scope, arg_mode, Some(ColumnType::Text))?;
                            self.datetime_field(args[0], name == "date_trunc")?;
                            let arg = self.expr(args[1], scope, arg_mode, None)?;
                            self.require_datetime(arg)?;
                            if name == "date_trunc"
                                && arg.kind == Some(ColumnType::Date)
                                && let Expr::Value(field) = args[0]
                                && field.value.clone().into_string().is_some_and(|field| {
                                    !matches!(
                                        field.to_ascii_lowercase().as_str(),
                                        "year" | "month" | "day"
                                    )
                                })
                            {
                                return Err(unsupported(
                                    "date_trunc",
                                    "DATE supports year, month and day truncation",
                                ));
                            }
                            self.expr(
                                args[1],
                                scope,
                                arg_mode,
                                Some(arg.kind.unwrap_or(ColumnType::Timestamp)),
                            )?;
                            Type::known(if name == "date_part" {
                                ColumnType::Float64
                            } else {
                                ColumnType::Timestamp
                            })
                        }
                        _ => {
                            let ty = self.expr(
                                args[0],
                                scope,
                                arg_mode,
                                expected.filter(|kind| numeric(*kind)),
                            )?;
                            self.require_numeric(ty)?;
                            // Existing round/floor/ceil only accept integer or float.
                            if ty.kind == Some(ColumnType::U64)
                                || name != "abs" && ty.kind == Some(ColumnType::Decimal)
                            {
                                return Err(unsupported(
                                    "function",
                                    "round/floor/ceil do not support decimal arguments",
                                ));
                            }
                            self.expr(
                                args[0],
                                scope,
                                arg_mode,
                                Some(ty.kind.unwrap_or(ColumnType::I64)),
                            )?
                        }
                    }
                } else if args.is_empty() {
                    Type::known(match name.as_str() {
                        "version" | "current_user" | "user" | "current_schema"
                        | "current_database" => ColumnType::Text,
                        "now" | "current_timestamp" => ColumnType::Timestamp,
                        "current_date" => ColumnType::Date,
                        _ => return Err(unsupported("function", "unknown function")),
                    })
                } else {
                    return Err(unsupported("function", "unknown function"));
                }
            }
            Expr::Cast {
                kind: sqlparser::ast::CastKind::Cast | sqlparser::ast::CastKind::DoubleColon,
                expr,
                data_type,
                array: false,
                format: None,
            } => {
                let target = super::sql_type(data_type)?;
                let arg_mode = if mode == Mode::Computed {
                    mode
                } else {
                    Mode::Literal
                };
                self.expr(expr, scope, arg_mode, Some(target))?;
                Type::known(target)
            }
            Expr::Extract { field, expr, .. } => {
                validate_datetime_field(&field.to_string().to_ascii_lowercase(), false)?;
                let arg_mode = if mode == Mode::Computed {
                    mode
                } else {
                    Mode::Literal
                };
                let ty = self.expr(expr, scope, arg_mode, None)?;
                self.require_datetime(ty)?;
                self.expr(
                    expr,
                    scope,
                    arg_mode,
                    Some(ty.kind.unwrap_or(ColumnType::Timestamp)),
                )?;
                Type::known(ColumnType::Decimal)
            }
            Expr::Floor { expr, field } | Expr::Ceil { expr, field } => {
                // Only ordinary numeric FLOOR/CEIL are implemented, not TO fields.
                if !matches!(
                    field,
                    sqlparser::ast::CeilFloorKind::DateTimeField(
                        sqlparser::ast::DateTimeField::NoDateTime
                    )
                ) {
                    return Err(unsupported(
                        "FLOOR/CEIL",
                        "field and scale modifiers are not supported",
                    ));
                }
                let arg_mode = if mode == Mode::Computed {
                    mode
                } else {
                    Mode::Literal
                };
                let ty = self.expr(
                    expr,
                    scope,
                    arg_mode,
                    expected.filter(|kind| numeric(*kind)),
                )?;
                self.require_numeric(ty)?;
                if ty
                    .kind
                    .is_some_and(|kind| matches!(kind, ColumnType::Decimal | ColumnType::U64))
                {
                    return Err(unsupported(
                        "FLOOR/CEIL",
                        "decimal arguments are not supported",
                    ));
                }
                self.expr(
                    expr,
                    scope,
                    arg_mode,
                    Some(ty.kind.unwrap_or(ColumnType::I64)),
                )?
            }
            _ => {
                return Err(unsupported(
                    "expression",
                    "this expression shape is not supported in this context",
                ));
            }
        };
        common_type(ty.kind, expected)?;
        Ok(if ty.kind.is_none() && ty.string {
            Type {
                kind: expected,
                ..ty
            }
        } else {
            ty
        })
    }

    fn literal_context(&mut self, expr: &Expr, expected: ColumnType) -> Result<()> {
        let node = expr as *const Expr;
        let domain = common_type(self.literal_casts.get(&node).copied(), Some(expected))?
            .expect("a concrete context has a concrete domain");
        self.literal_casts.insert(node, domain);
        Ok(())
    }

    pub(super) fn common_expressions(
        &mut self,
        expressions: &[&Expr],
        scope: &Scope,
        mode: Mode,
        expected: Option<ColumnType>,
    ) -> Result<Type> {
        let mut ty = Type {
            kind: expected,
            string: false,
        };
        for expr in expressions {
            ty = ty.common(self.expr(expr, scope, mode, None)?)?;
        }
        for expr in expressions {
            self.expr(expr, scope, mode, ty.kind)?;
        }
        Ok(ty)
    }

    pub(super) fn comparable(
        &mut self,
        left: &Expr,
        right: &Expr,
        scope: &Scope,
        mode: Mode,
    ) -> Result<()> {
        let left_type = self.expr(left, scope, mode, None)?;
        let right_type = self.expr(right, scope, mode, None)?;
        // Apply each concrete opposite-side context before reconciling. This
        // records conflicting parameter uses rather than losing the constraint
        // behind an incompatible transient expression type.
        self.expr(left, scope, mode, right_type.kind)?;
        self.expr(right, scope, mode, left_type.kind)?;
        left_type.common(right_type)?;
        Ok(())
    }

    pub(super) fn require_numeric(&self, ty: Type) -> Result<()> {
        if ty.kind.is_some_and(|kind| !numeric(kind)) {
            return Err(DbError::SqlDatatypeMismatch {
                column: "numeric expression".into(),
            });
        }
        Ok(())
    }

    fn require_datetime(&self, ty: Type) -> Result<()> {
        if ty
            .kind
            .is_some_and(|kind| !matches!(kind, ColumnType::Date | ColumnType::Timestamp))
        {
            return Err(DbError::SqlDatatypeMismatch {
                column: "datetime expression".into(),
            });
        }
        Ok(())
    }

    fn datetime_field(&self, expr: &Expr, trunc: bool) -> Result<()> {
        if let Expr::Value(value) = expr
            && let Some(text) = value.value.clone().into_string()
        {
            validate_datetime_field(&text.to_ascii_lowercase(), trunc)?;
        }
        Ok(())
    }
}

fn validate_string(text: &str, expected: ColumnType) -> Result<()> {
    use crate::sql::values;
    let valid = match expected {
        ColumnType::Text => true,
        ColumnType::I64 => text.trim().parse::<i64>().is_ok(),
        ColumnType::U64 => text.trim().parse::<u64>().is_ok(),
        ColumnType::Bool => matches!(
            text.trim().to_ascii_lowercase().as_str(),
            "true" | "false" | "t" | "f" | "1" | "0" | "yes" | "no" | "on" | "off"
        ),
        ColumnType::Float64 => values::parse_float_text(text).is_ok(),
        ColumnType::Decimal => values::parse_decimal_text(text).is_ok(),
        ColumnType::Date => values::parse_date_text(text).is_ok(),
        ColumnType::Timestamp => values::parse_timestamp_text(text).is_ok(),
        ColumnType::Uuid => crate::sql_types::UuidValue::parse(text).is_ok(),
        ColumnType::Bytes => text.strip_prefix("\\x").is_some_and(|text| {
            text.len() % 2 == 0 && text.bytes().all(|byte| byte.is_ascii_hexdigit())
        }),
    };
    if !valid {
        return Err(DbError::SqlDatatypeMismatch {
            column: format!("literal for {expected:?}"),
        });
    }
    Ok(())
}

fn computed_shape(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => true,
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            ..
        } => true,
        Expr::Nested(inner)
        | Expr::Extract { expr: inner, .. }
        | Expr::Floor { expr: inner, .. }
        | Expr::Ceil { expr: inner, .. }
        | Expr::Cast { expr: inner, .. } => computed_shape(inner),
        Expr::BinaryOp { left, op, right } => {
            matches!(
                op,
                Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo
            ) && computed_shape(left)
                && computed_shape(right)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_none_or(computed_shape)
                && conditions.iter().all(|arm| {
                    let condition = if operand.is_some() {
                        computed_shape(&arm.condition)
                    } else {
                        match &arm.condition {
                            Expr::BinaryOp { left, op, right } if comparison(op) => {
                                computed_shape(left) && computed_shape(right)
                            }
                            Expr::IsNull(inner) | Expr::IsNotNull(inner) => computed_shape(inner),
                            _ => false,
                        }
                    };
                    condition && computed_shape(&arm.result)
                })
                && else_result.as_deref().is_none_or(computed_shape)
        }
        Expr::Function(function) if function.over.is_none() => {
            let name = function.name.to_string().to_ascii_lowercase();
            (name == "coalesce" || crate::sql::values::scalar_function_arity(&name).is_some())
                && arguments(function).is_ok_and(|args| {
                    args.into_iter()
                        .all(|arg| expression_argument(arg).is_ok_and(computed_shape))
                })
        }
        _ => false,
    }
}

fn validate_datetime_field(field: &str, trunc: bool) -> Result<()> {
    let supported = if trunc {
        matches!(field, "year" | "month" | "day" | "hour" | "minute")
    } else {
        matches!(
            field,
            "year" | "month" | "day" | "hour" | "minute" | "second" | "epoch"
        )
    };
    if !supported {
        return Err(unsupported("datetime field", "unsupported datetime field"));
    }
    Ok(())
}
