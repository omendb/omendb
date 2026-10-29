//! Bound wire statements and per-connection registries.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pgwire::api::Type;

use crate::RelationalDatabaseTransaction;

pub(super) const TX_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Parse freezes both the SQL binding and the wire parameter interpretation.
/// Neither Describe nor Execute may infer a replacement signature from rows.
#[derive(Debug, Clone)]
pub(super) struct ParsedStatement {
    pub(super) sql: String,
    pub(super) bound: Arc<crate::sql::binding::BoundStatement>,
    pub(super) parameter_types: Vec<Type>,
}

/// One connection's open transaction block.
pub(super) struct TransactionBlock {
    /// None is a rollback-only block: its engine resources are released, but
    /// its boundary remains until explicit cleanup or connection teardown.
    pub(super) transaction: Option<RelationalDatabaseTransaction>,
    pub(super) last_used: Instant,
}

/// Mandatory, explicit authority installed only by successful startup.
#[derive(Clone, Debug)]
pub(super) enum SessionAuthority {
    Bootstrap,
    Principal {
        id: crate::PrincipalId,
        login: String,
    },
}

pub(super) type IdentityMap = Mutex<HashMap<std::net::SocketAddr, SessionAuthority>>;
pub(super) type FailureDelays = Mutex<std::collections::HashMap<std::net::IpAddr, u32>>;
