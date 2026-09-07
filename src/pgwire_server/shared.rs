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
/// declared at Parse time. The SQL parser owns placeholder discovery and bounds.
#[derive(Debug, Clone)]
pub(super) struct ParsedStatement {
    pub(super) sql: String,
    pub(super) parameter_types: Vec<Option<Type>>,
    pub(super) parameter_count: usize,
}

/// Preserve declared types and use the SQL tier's parsed parameter positions.
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
        let parameter_count = crate::sql::parameter_count(sql).map_err(super::map_db_error)?;
        Ok(ParsedStatement {
            parameter_count,
            sql: sql.to_owned(),
            parameter_types: types.to_vec(),
        })
    }

    fn get_parameter_types(&self, stmt: &ParsedStatement) -> PgWireResult<Vec<Type>> {
        Ok((0..stmt.parameter_count)
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
