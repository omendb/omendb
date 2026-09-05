//! Wire-query shared shapes: parsed statements, transaction blocks, and
//! the per-connection registries every handler tier reads.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pgwire::api::ClientInfo;
use pgwire::api::Type;
use pgwire::api::results::FieldInfo;

use crate::RelationalDatabaseTransaction;
use pgwire::error::PgWireResult;

pub(super) const TX_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// A wire-parsed statement: raw SQL plus the parameter types the client
/// declared at Parse time. Placeholder count is derived by scanning the SQL
/// text so ParameterDescription reports the arity the engine will enforce.
#[derive(Debug, Clone)]
pub(super) struct ParsedStatement {
    pub(super) sql: String,
    pub(super) parameter_types: Vec<Option<Type>>,
}

impl ParsedStatement {
    pub(super) fn placeholder_count(sql: &str) -> usize {
        let mut max_placeholder = 0usize;
        let mut in_single_quote = false;
        let mut chars = sql.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '\'' => in_single_quote = !in_single_quote,
                '$' if !in_single_quote => {
                    let mut number = 0usize;
                    let mut saw_digit = false;
                    while let Some(d) = chars.peek() {
                        if let Some(digit) = d.to_digit(10) {
                            number = number * 10 + digit as usize;
                            saw_digit = true;
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if saw_digit {
                        max_placeholder = max_placeholder.max(number);
                    }
                }
                _ => {}
            }
        }
        max_placeholder
    }
}

/// Parser that keeps the client's declared parameter types and derives
/// placeholder arity from the SQL text. Result schemas are computed by
/// execution probes in the describe handlers, not here.
pub(super) struct PlaceholderParser;

#[async_trait]
impl pgwire::api::stmt::QueryParser for PlaceholderParser {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<ParsedStatement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(ParsedStatement {
            sql: sql.to_owned(),
            parameter_types: types.to_vec(),
        })
    }

    fn get_parameter_types(&self, stmt: &ParsedStatement) -> PgWireResult<Vec<Type>> {
        Ok((0..ParsedStatement::placeholder_count(&stmt.sql))
            .map(|index| {
                stmt.parameter_types
                    .get(index)
                    .cloned()
                    .flatten()
                    .unwrap_or(Type::UNKNOWN)
            })
            .collect())
    }

    fn get_result_schema(
        &self,
        _stmt: &ParsedStatement,
        _format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(Vec::new())
    }
}

/// One connection's open transaction block.
pub(super) struct TransactionBlock {
    pub(super) transaction: RelationalDatabaseTransaction,
    /// Set when a statement inside the block failed; the block then rejects
    /// everything except ROLLBACK (and COMMIT, which rolls back).
    pub(super) errored: bool,
    pub(super) last_used: Instant,
}

pub(super) type ProbeCache = HashMap<(String, String), Arc<Vec<(String, Type)>>>;

pub(super) type IdentityMap = Mutex<HashMap<std::net::SocketAddr, String>>;
pub(super) type FailureDelays = Mutex<std::collections::HashMap<std::net::IpAddr, u32>>;
