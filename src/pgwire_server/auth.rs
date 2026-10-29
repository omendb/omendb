//! Native principal authentication, provisioning, and startup dispatch.

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
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::cancellation::{CancellationRegistry, WireCancelHandler};
use super::handler::OmenDbHandler;
use super::lifecycle::{QueryWorkers, read_lock};
use super::shared::{FailureDelays, IdentityMap, SessionAuthority};
use super::{SharedDatabase, map_db_error, pg_error};
use crate::{DbError, Principal, RelationalDatabase};

const AUTH_FAILURE_DELAY_BASE_MS: u64 = 100;
const AUTH_FAILURE_DELAY_CAP_MS: u64 = 5_000;
const SCRAM_ITERATIONS: usize = 4096;
const SALT_LEN: usize = 32;
const SECRET_LEN: usize = 64;

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
    let bootstrap = read_lock(database)
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .authorization_bootstrap_enabled()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if bootstrap && !listener_addr.ip().is_loopback() {
        return Err(std::io::Error::other(
            "bootstrap authentication requires a loopback listener; provision native principals before binding a public interface",
        ));
    }

    let identities = Arc::new(Mutex::new(HashMap::new()));
    let cancellations = Arc::new(CancellationRegistry::new());
    Ok(Arc::new(HandlerFactory {
        handler: Arc::new(OmenDbHandler {
            database: Arc::clone(database),
            transactions: Arc::new(Mutex::new(HashMap::new())),
            cancellations: Arc::clone(&cancellations),
            identities,
            query_workers,
            statement_timeout,
            max_result_bytes,
            slow_statement_threshold,
        }),
        allow_bootstrap: listener_addr.ip().is_loopback(),
        failure_delays: Arc::new(Mutex::new(HashMap::new())),
        cancel_handler: Arc::new(WireCancelHandler { cancellations }),
    }))
}

/// Grant an existing native principal access to the current table identity.
/// Write access includes read access. There is no wildcard table authority.
pub fn provision_wire_grant(
    database: &mut RelationalDatabase,
    role: &str,
    table_name: &str,
    can_read: bool,
    can_write: bool,
) -> Result<(), DbError> {
    let table = database
        .catalog()
        .tables()
        .find(|table| table.name == table_name)
        .map(|table| table.id)
        .ok_or_else(|| DbError::InvalidState(format!("table {table_name} does not exist")))?;
    database.transaction(|database, transaction| {
        let principal = transaction
            .principal(database, role)?
            .ok_or_else(|| DbError::InvalidState(format!("principal {role} does not exist")))?;
        transaction.set_table_grant(database, principal.id, table, can_read, can_write)
    })?;
    Ok(())
}

/// Explicit database administration, including all current data and DDL.
pub fn provision_wire_admin(
    database: &mut RelationalDatabase,
    role: &str,
    enabled: bool,
) -> Result<(), DbError> {
    database.transaction(|database, transaction| {
        let principal = transaction
            .principal(database, role)?
            .ok_or_else(|| DbError::InvalidState(format!("principal {role} does not exist")))?;
        transaction.set_database_administrator(database, principal.id, enabled)
    })?;
    Ok(())
}

/// Atomically create a native principal or rotate its SCRAM credential.
/// Rotation preserves the principal identity and its grants. The native core
/// stores opaque bytes; this adapter owns salt32 || salted_password32.
pub fn provision_wire_user(
    database: &mut RelationalDatabase,
    username: &str,
    password: &str,
) -> Result<(), DbError> {
    let mut salt = random_nonce().into_bytes();
    salt.resize(SALT_LEN, b'0');
    let mut secret = salt.clone();
    secret.extend_from_slice(&gen_salted_password(password, &salt, SCRAM_ITERATIONS));
    database.transaction(|database, transaction| {
        let id = match transaction.principal(database, username)? {
            Some(principal) => principal.id,
            None => transaction.create_principal(database, username)?,
        };
        transaction.set_principal_credential(database, id, Some(secret))
    })?;
    Ok(())
}

/// One source per connection. The identity is captured in the same committed
/// read as the credential and never looked up by name again after SCRAM proof.
struct WireAuthSource {
    database: SharedDatabase,
    principal: Mutex<Option<Principal>>,
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
        let (principal, credential) = read_lock(&self.database)?
            .authentication_principal(username)
            .map_err(|_| invalid_login())?
            .ok_or_else(invalid_login)?;
        let secret = credential
            .filter(|secret| secret.len() == SECRET_LEN)
            .ok_or_else(invalid_login)?;
        let mut pin = self.principal.lock().map_err(|_| invalid_login())?;
        if pin.is_some() {
            return Err(invalid_login());
        }
        *pin = Some(principal);
        let (salt, salted) = secret.split_at(SALT_LEN);
        Ok(Password::new(Some(salt.to_vec()), salted.to_vec()))
    }
}

struct BootstrapStartup;

#[async_trait]
impl pgwire::api::auth::noop::NoopStartupHandler for BootstrapStartup {}

#[allow(clippy::large_enum_variant)]
enum StartupMode {
    Bootstrap(BootstrapStartup),
    Scram {
        handler: SASLAuthStartupHandler<DefaultServerParameterProvider>,
        source: Arc<WireAuthSource>,
    },
}

struct ConnectionStartup {
    database: SharedDatabase,
    identities: Arc<IdentityMap>,
    failure_delays: Arc<FailureDelays>,
    cancellations: Arc<CancellationRegistry>,
    allow_bootstrap: bool,
    mode: Mutex<Option<Arc<StartupMode>>>,
}

impl ConnectionStartup {
    /// Pick current native mode on the first Startup, not at listener creation.
    /// Only synchronous construction happens under this short-lived lock.
    fn mode(&self, message: &PgWireFrontendMessage) -> PgWireResult<Arc<StartupMode>> {
        let mut mode = self
            .mode
            .lock()
            .map_err(|_| pg_error("XX000", "startup state poisoned".to_owned()))?;
        if matches!(message, PgWireFrontendMessage::Startup(_)) {
            if mode.is_some() {
                return Err(pg_error("08P01", "duplicate Startup message".to_owned()));
            }
            let bootstrap = read_lock(&self.database)?
                .authorization_bootstrap_enabled()
                .map_err(map_db_error)?;
            let selected = if bootstrap {
                if !self.allow_bootstrap {
                    return Err(pg_error(
                        "28000",
                        "bootstrap requires a loopback listener".to_owned(),
                    ));
                }
                StartupMode::Bootstrap(BootstrapStartup)
            } else {
                let source = Arc::new(WireAuthSource {
                    database: Arc::clone(&self.database),
                    principal: Mutex::new(None),
                });
                StartupMode::Scram {
                    handler: SASLAuthStartupHandler::new(Arc::new(
                        DefaultServerParameterProvider::default(),
                    ))
                    .with_scram(ScramAuth::new(source.clone())),
                    source,
                }
            };
            *mode = Some(Arc::new(selected));
        }
        mode.clone()
            .ok_or_else(|| pg_error("08P01", "expected Startup message".to_owned()))
    }

    fn register_identity<C: pgwire::api::ClientInfo>(
        &self,
        client: &C,
        mode: &StartupMode,
    ) -> PgWireResult<()> {
        let authority = match mode {
            StartupMode::Bootstrap(_) => {
                // A principal may have been provisioned while startup yielded.
                read_lock(&self.database)?
                    .bootstrap_authorization()
                    .map_err(map_db_error)?;
                SessionAuthority::Bootstrap
            }
            StartupMode::Scram { source, .. } => {
                let principal = source
                    .principal
                    .lock()
                    .map_err(|_| invalid_login())?
                    .clone()
                    .ok_or_else(invalid_login)?;
                // Fail closed if this incarnation disappeared during the proof.
                read_lock(&self.database)?
                    .authorization(principal.id)
                    .map_err(map_db_error)?;
                SessionAuthority::Principal {
                    id: principal.id,
                    login: principal.name,
                }
            }
        };
        self.identities
            .lock()
            .map_err(|_| pg_error("XX000", "identity registry poisoned".to_owned()))?
            .insert(client.socket_addr(), authority);
        self.cancellations.register(client);
        Ok(())
    }

    fn register_failure_delay(&self, addr: std::net::SocketAddr) -> u64 {
        let Ok(mut delays) = self.failure_delays.lock() else {
            return AUTH_FAILURE_DELAY_CAP_MS;
        };
        let failures = delays
            .entry(addr.ip())
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
        auth_failure_delay_ms(*failures)
    }
}

#[async_trait]
impl StartupHandler for ConnectionStartup {
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
        let result: PgWireResult<()> = async {
            let mode = self.mode(&message)?;
            match mode.as_ref() {
                StartupMode::Bootstrap(handler) => handler.on_startup(client, message).await?,
                StartupMode::Scram { handler, .. } => handler.on_startup(client, message).await?,
            }
            if matches!(
                client.state(),
                pgwire::api::PgWireConnectionState::ReadyForQuery
            ) {
                self.register_identity(client, &mode)?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let delay = self.register_failure_delay(client.socket_addr());
            tokio::time::sleep(Duration::from_millis(delay)).await;
            // pgwire turns ordinary ERROR into query-ready state. Every startup
            // error, including registration after pgwire sent ReadyForQuery,
            // must instead close the connection. No sync guard spans an await.
            let mut error = ErrorInfo::from(error);
            error.severity = "FATAL".to_owned();
            error.severity_nonlocalized = Some("FATAL".to_owned());
            return Err(PgWireError::UserError(Box::new(error)));
        }
        Ok(())
    }
}

pub(super) struct HandlerFactory {
    pub(super) handler: Arc<OmenDbHandler>,
    allow_bootstrap: bool,
    failure_delays: Arc<FailureDelays>,
    cancel_handler: Arc<WireCancelHandler>,
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn error_handler(&self) -> Arc<impl pgwire::api::ErrorHandler> {
        self.handler.clone()
    }

    fn cancel_handler(&self) -> Arc<impl pgwire::api::cancel::CancelHandler> {
        self.cancel_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(ConnectionStartup {
            database: self.handler.database.clone(),
            identities: self.handler.identities.clone(),
            failure_delays: self.failure_delays.clone(),
            cancellations: self.handler.cancellations.clone(),
            allow_bootstrap: self.allow_bootstrap,
            mode: Mutex::new(None),
        })
    }
}
