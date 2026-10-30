//! Translate SQL operators at the syntax boundary, not inside numeric evaluation.
use sqlparser::ast::BinaryOperator;

use crate::arithmetic::{self, Operator};
use crate::{Result, Value};

pub(super) fn evaluate(op: &BinaryOperator, lhs: &Value, rhs: &Value) -> Result<Value> {
    let op = match op {
        BinaryOperator::Plus => Operator::Add,
        BinaryOperator::Minus => Operator::Subtract,
        BinaryOperator::Multiply => Operator::Multiply,
        BinaryOperator::Divide => Operator::Divide,
        BinaryOperator::Modulo => Operator::Modulo,
        _ => {
            return Err(super::unsupported(
                "arithmetic",
                "only arithmetic operators are supported here",
            ));
        }
    };
    arithmetic::evaluate(op, lhs, rhs)
}
