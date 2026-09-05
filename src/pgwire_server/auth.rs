//! Wire authentication: trust mode, catalog-backed SCRAM, grants, and
//! the startup-mode dispatch.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::sasl::scram::{ScramAuth, gen_salted_password, random_nonce};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::cancellation::{CancellationRegistry, WireCancelHandler};
use super::handler::OmenDbHandler;
use super::lifecycle::{QueryWorkers, read_lock, write_lock};
use super::shared::{FailureDelays, IdentityMap};
use super::{SharedDatabase, pg_error};
use crate::{
    ColumnDefinition, ColumnId, ColumnType, DbError, RelationalDatabase, TableDefinition, TableId,
    Value,
};

/// Reserved identities for the wire-auth catalog table, far above the
/// ranges user tables occupy.
const AUTH_TABLE_ID: u64 = u64::MAX - 1;
const AUTH_USERNAME_COLUMN_ID: u16 = u16::MAX - 1;
const AUTH_SECRET_COLUMN_ID: u16 = u16::MAX;
const GRANTS_TABLE_ID: u64 = u64::MAX - 2;

/// Bounded authentication-failure delay per source address: the first
/// failure waits 100ms, then the delay doubles per consecutive failure and
/// caps at 5 seconds. Keyed by source IP (pgwire exposes it via
/// ClientInfo::socket_addr); counters live for the server's lifetime.
const AUTH_FAILURE_DELAY_BASE_MS: u64 = 100;
const AUTH_FAILURE_DELAY_CAP_MS: u64 = 5_000;

/// Credential storage for wire authentication. The table is created by
/// `serve` and populated through `provision_wire_user`; an empty table
/// keeps trust mode so local development is unchanged.
const AUTH_TABLE: &str = "pgwire_auth";
const SCRAM_ITERATIONS: usize = 4096;
const SALT_LEN: usize = 32;

pub(super) fn auth_failure_delay_ms(failures: u32) -> u64 {
    if failures == 0 {
        return 0;
    }
    let exponent = failures.saturating_sub(1).min(6);
    AUTH_FAILURE_DELAY_BASE_MS
        .saturating_mul(1 << exponent)
        .min(AUTH_FAILURE_DELAY_CAP_MS)
}

pub(super) fn build_factory(
    database: &SharedDatabase,
    listener_addr: std::net::SocketAddr,
    query_workers: Arc<QueryWorkers>,
    statement_timeout: Option<Duration>,
    max_result_bytes: Option<usize>,
    slow_statement_threshold: Option<Duration>,
) -> std::io::Result<Arc<HandlerFactory>> {
    {
        let mut database =
            write_lock(database).map_err(|error| std::io::Error::other(error.to_string()))?;
        if !database
            .catalog()
            .tables()
            .any(|table| table.name == AUTH_TABLE)
        {
            database
                .create_table(auth_table_definition())
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
    }
    let has_users = {
        let database =
            read_lock(database).map_err(|error| std::io::Error::other(error.to_string()))?;
        let mut transaction = database
            .begin()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let result = transaction
            .execute_sql(&database, "SELECT username FROM pgwire_auth")
            .map_err(|error| std::io::Error::other(error.to_string()));
        drop(transaction);
        !result?.rows.is_empty()
    };

    // Trust mode authenticates implicitly, so it must never be reachable
    // from a non-loopback interface.
    if !has_users && !listener_addr.ip().is_loopback() {
        return Err(std::io::Error::other(
            "trust authentication requires a loopback listener; provision wire users before binding a public interface",
        ));
    }

    let auth = has_users.then(|| {
        Arc::new(WireAuthSource {
            database: Arc::clone(database),
        })
    });
    let identities = Arc::new(Mutex::new(HashMap::new()));
    let failure_delays = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let cancellations = Arc::new(CancellationRegistry::new());
    Ok(Arc::new(HandlerFactory {
        handler: Arc::new(OmenDbHandler {
            database: Arc::clone(database),
            transactions: Arc::new(Mutex::new(HashMap::new())),
            cancellations: Arc::clone(&cancellations),
            schema_probes: Arc::new(Mutex::new(HashMap::new())),
            identities: Arc::clone(&identities),
            query_workers,
            statement_timeout,
            max_result_bytes,
            slow_statement_threshold,
        }),
        auth: auth.map(|auth_source| {
            Arc::new(ScramComponents {
                auth_source,
                identities: Arc::clone(&identities),
                failure_delays: Arc::clone(&failure_delays),
                cancellations: Arc::clone(&cancellations),
            })
        }),
        cancel_handler: Arc::new(WireCancelHandler { cancellations }),
    }))
}

fn auth_table_definition() -> TableDefinition {
    TableDefinition {
        id: TableId(AUTH_TABLE_ID),
        name: AUTH_TABLE.to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(AUTH_USERNAME_COLUMN_ID),
                name: "username".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(AUTH_SECRET_COLUMN_ID),
                name: "secret".to_owned(),
                data_type: ColumnType::Bytes,
                nullable: false,
            },
        ],
    }
}

fn grants_table_definition() -> TableDefinition {
    TableDefinition {
        id: TableId(GRANTS_TABLE_ID),
        name: "pgwire_grants".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "role".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "table_name".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "can_read".to_owned(),
                data_type: ColumnType::Bool,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "can_write".to_owned(),
                data_type: ColumnType::Bool,
                nullable: false,
            },
        ],
    }
}

/// Grant one role read and/or write access to one table. The wildcard
/// table name "*" with can_write grants schema administration (DDL).
/// Enforcement activates as soon as any grant row exists; before that,
/// authenticated roles have full access so bootstrap provisioning works.
pub fn provision_wire_grant(
    database: &mut RelationalDatabase,
    role: &str,
    table_name: &str,
    can_read: bool,
    can_write: bool,
) -> Result<(), DbError> {
    if !database
        .catalog()
        .tables()
        .any(|table| table.name == "pgwire_grants")
    {
        database.create_table(grants_table_definition())?;
    }
    database.execute_sql_with_params(
        "DELETE FROM pgwire_grants WHERE role = $1 AND table_name = $2",
        &[
            Value::Text(role.to_owned()),
            Value::Text(table_name.to_owned()),
        ],
    )?;
    database.execute_sql_with_params(
        "INSERT INTO pgwire_grants (role, table_name, can_read, can_write) VALUES ($1, $2, $3, $4)",
        &[
            Value::Text(role.to_owned()),
            Value::Text(table_name.to_owned()),
            Value::Bool(can_read),
            Value::Bool(can_write),
        ],
    )?;
    Ok(())
}

/// Create or replace the credential table and store one user's SCRAM
/// secret (salt || salted password) through the typed facade.
pub fn provision_wire_user(
    database: &mut RelationalDatabase,
    username: &str,
    password: &str,
) -> Result<(), DbError> {
    if !database
        .catalog()
        .tables()
        .any(|table| table.name == AUTH_TABLE)
    {
        database.create_table(auth_table_definition())?;
    }
    // Fixed-width salt so the storage layout (salt || salted password)
    // splits deterministically on read.
    let mut salt = random_nonce().into_bytes();
    salt.resize(SALT_LEN, b'0');
    let mut secret = salt.clone();
    secret.extend_from_slice(&gen_salted_password(password, &salt, SCRAM_ITERATIONS));
    // Heap tables have no caller-visible key, so re-provisioning a user
    // must replace their row explicitly (delete-then-insert, like the
    // grants path) instead of relying on a key collision.
    database.execute_sql_with_params(
        "DELETE FROM pgwire_auth WHERE username = $1",
        &[Value::Text(username.to_owned())],
    )?;
    database.execute_sql_with_params(
        "INSERT INTO pgwire_auth (username, secret) VALUES ($1, $2)",
        &[Value::Text(username.to_owned()), Value::Bytes(secret)],
    )?;
    Ok(())
}

struct WireAuthSource {
    database: SharedDatabase,
}

impl std::fmt::Debug for WireAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireAuthSource").finish_non_exhaustive()
    }
}

fn invalid_login() -> PgWireError {
    pg_error("28P01", "password authentication failed".to_owned())
}

#[async_trait]
impl AuthSource for WireAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().ok_or_else(invalid_login)?;
        let database = read_lock(&self.database)?;
        let mut transaction = database.begin().map_err(|_| invalid_login())?;
        let result = transaction
            .execute_sql_with_params(
                &database,
                "SELECT secret FROM pgwire_auth WHERE username = $1",
                &[Value::Text(username.to_owned())],
            )
            .map_err(|_| invalid_login());
        drop(transaction);
        let result = result.map_err(|_| invalid_login())?;
        let secret = match result.rows.first().and_then(|row| row.first()) {
            Some(Value::Bytes(bytes)) if bytes.len() > SALT_LEN => bytes.clone(),
            _ => return Err(invalid_login()),
        };
        let (salt, salted) = secret.split_at(SALT_LEN);
        Ok(Password::new(Some(salt.to_vec()), salted.to_vec()))
    }
}

/// Trust mode: the credential table is absent or empty, so connections
/// authenticate implicitly (loopback, single-user development).
struct TrustStartup;

#[async_trait]
impl pgwire::api::auth::noop::NoopStartupHandler for TrustStartup {}

/// Dispatches between trust mode and SCRAM based on what `serve` found at
/// startup. Mode is fixed for the server's lifetime; provisioning users
/// after start takes effect on restart.
#[allow(clippy::large_enum_variant)]
enum StartupMode {
    Trust(TrustStartup, Arc<CancellationRegistry>),
    Scram(
        SASLAuthStartupHandler<DefaultServerParameterProvider>,
        Arc<IdentityMap>,
        Arc<FailureDelays>,
        Arc<CancellationRegistry>,
    ),
}

impl StartupMode {
    /// Bounded exponential delay for the source address of a failed
    /// authentication exchange: 100ms doubling per consecutive failure,
    /// capped at 5s. Counters are server-lifetime; the delay runs before
    /// the error is sent so attackers cannot skip it by dropping early.
    fn register_failure_delay(&self, addr: std::net::SocketAddr) -> u64 {
        let Ok(mut delays) = self.failure_delays().lock() else {
            return AUTH_FAILURE_DELAY_CAP_MS;
        };
        let failures = delays.entry(addr.ip()).and_modify(|c| *c += 1).or_insert(1);
        auth_failure_delay_ms(*failures)
    }

    fn failure_delays(&self) -> &Arc<FailureDelays> {
        match self {
            StartupMode::Trust(_, _) => unreachable!("trust mode never delays"),
            StartupMode::Scram(_, _, delays, _) => delays,
        }
    }

    fn identities(&self) -> Option<&Arc<IdentityMap>> {
        match self {
            StartupMode::Trust(_, _) => None,
            StartupMode::Scram(_, identities, _, _) => Some(identities),
        }
    }

    fn cancellations(&self) -> &Arc<CancellationRegistry> {
        match self {
            StartupMode::Trust(_, cancellations) => cancellations,
            StartupMode::Scram(_, _, _, cancellations) => cancellations,
        }
    }
}

#[async_trait]
impl StartupHandler for StartupMode {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: pgwire::api::ClientInfo + futures::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
        Self: Sync,
    {
        let result = match self {
            StartupMode::Trust(trust, _) => trust.on_startup(client, message).await,
            StartupMode::Scram(sasl, _, _, _) => sasl.on_startup(client, message).await,
        };
        match result {
            Ok(())
                if matches!(
                    client.state(),
                    pgwire::api::PgWireConnectionState::ReadyForQuery
                ) =>
            {
                // Register the protocol cancel identity only after startup
                // authentication succeeds. The registry owns the wire
                // identity and the database operation token together.
                self.cancellations().register(client);
                // Record the authenticated role for grant enforcement.
                // The startup packet's user parameter lands in client
                // metadata (that is where the SCRAM machinery reads it).
                if let Some(identities) = self.identities() {
                    let user = client
                        .metadata()
                        .get("user")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_owned());
                    if let Ok(mut identities) = identities.lock() {
                        let addr = client.socket_addr();
                        identities.insert(addr, user);
                    }
                }
                Ok(())
            }
            Ok(()) => Ok(()),
            Err(error) => {
                let delay = self.register_failure_delay(client.socket_addr());
                if delay > 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                Err(error)
            }
        }
    }
}

/// `None` selects trust mode; `Some` builds a fresh SCRAM handler per
/// connection because SASL exchange state must not be shared.
#[derive(Clone)]
struct ScramComponents {
    auth_source: Arc<WireAuthSource>,
    identities: Arc<IdentityMap>,
    failure_delays: Arc<FailureDelays>,
    cancellations: Arc<CancellationRegistry>,
}

pub(super) struct HandlerFactory {
    pub(super) handler: Arc<OmenDbHandler>,
    auth: Option<Arc<ScramComponents>>,
    cancel_handler: Arc<WireCancelHandler>,
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn cancel_handler(&self) -> Arc<impl pgwire::api::cancel::CancelHandler> {
        self.cancel_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(match self.auth.as_ref() {
            Some(components) => {
                let components = (**components).clone();
                StartupMode::Scram(
                    SASLAuthStartupHandler::new(
                        Arc::new(DefaultServerParameterProvider::default()),
                    )
                    .with_scram(ScramAuth::new(components.auth_source)),
                    components.identities,
                    components.failure_delays,
                    components.cancellations,
                )
            }
            None => StartupMode::Trust(TrustStartup, self.handler.cancellations.clone()),
        })
    }
}
