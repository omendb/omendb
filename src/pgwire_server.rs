//! PostgreSQL wire-protocol serving for the bounded embedded SQL tier.
//!
//! V1 scope: loopback trust or catalog-backed SCRAM authentication, simple and
//! extended query protocols with parameterized execution, cooperative query
//! cancellation, and wire transaction blocks (`BEGIN`/`COMMIT`/`ROLLBACK`)
//! mapped to the typed transaction API with PostgreSQL aborted-state semantics.
//! Contract details and non-goals live in the repository's server documentation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::sasl::scram::{ScramAuth, gen_salted_password, random_nonce};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::cancel::CancelHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::startup::SecretKey;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinSet;

use crate::{
    CancellationToken, ColumnDefinition, ColumnId, ColumnType, DbError, OperationControl,
    RelationalDatabase, RelationalDatabaseTransaction, TableDefinition, TableId, Value,
};

/// Reserved identities for the wire-auth catalog table, far above the
/// ranges user tables occupy.
const AUTH_TABLE_ID: u64 = u64::MAX - 1;
const AUTH_USERNAME_COLUMN_ID: u16 = u16::MAX - 1;
const AUTH_SECRET_COLUMN_ID: u16 = u16::MAX;
const GRANTS_TABLE_ID: u64 = u64::MAX - 2;

/// Bounded authentication-failure delay per source address: the first
/// failure is immediate, then exponential backoff capped at 5 seconds.
/// Keyed by source IP (pgwire exposes it via ClientInfo::socket_addr);
/// counters live for the server's lifetime.
const AUTH_FAILURE_DELAY_BASE_MS: u64 = 100;
const AUTH_FAILURE_DELAY_CAP_MS: u64 = 5_000;

/// A shared database behind the wire server.
///
/// The kernel serves concurrent snapshot reads and buffers in-block writes
/// inside the transaction until publication, so statements hold the read
/// lock while executing; only publication paths (autocommit writes, DDL,
/// COMMIT) take the write lock. This is the serialized-writer +
/// concurrent-reader shape from `design/OLTP_COMPETITIVE_GAPS.md`.
pub type SharedDatabase = Arc<RwLock<RelationalDatabase>>;

const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(1);

fn read_lock(
    database: &SharedDatabase,
) -> PgWireResult<std::sync::RwLockReadGuard<'_, RelationalDatabase>> {
    database
        .read()
        .map_err(|_| pg_error("XX000", "database lock poisoned".to_owned()))
}

fn write_lock(
    database: &SharedDatabase,
) -> PgWireResult<std::sync::RwLockWriteGuard<'_, RelationalDatabase>> {
    database
        .write()
        .map_err(|_| pg_error("XX000", "database lock poisoned".to_owned()))
}

fn read_lock_with_cancellation<'a>(
    database: &'a SharedDatabase,
    cancellation: &CancellationToken,
) -> PgWireResult<std::sync::RwLockReadGuard<'a, RelationalDatabase>> {
    loop {
        if cancellation.is_cancelled() {
            return Err(map_db_error(DbError::Cancelled));
        }
        match database.try_read() {
            Ok(guard) => {
                if cancellation.is_cancelled() {
                    drop(guard);
                    return Err(map_db_error(DbError::Cancelled));
                }
                return Ok(guard);
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::sleep(LOCK_POLL_INTERVAL);
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(pg_error("XX000", "database lock poisoned".to_owned()));
            }
        }
    }
}

fn write_lock_with_cancellation<'a>(
    database: &'a SharedDatabase,
    cancellation: &CancellationToken,
) -> PgWireResult<std::sync::RwLockWriteGuard<'a, RelationalDatabase>> {
    loop {
        if cancellation.is_cancelled() {
            return Err(map_db_error(DbError::Cancelled));
        }
        match database.try_write() {
            Ok(guard) => {
                if cancellation.is_cancelled() {
                    drop(guard);
                    return Err(map_db_error(DbError::Cancelled));
                }
                return Ok(guard);
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::sleep(LOCK_POLL_INTERVAL);
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(pg_error("XX000", "database lock poisoned".to_owned()));
            }
        }
    }
}

/// A persistent single-node PostgreSQL-wire server configuration.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Directory containing the durable OmenDB database.
    pub database_path: PathBuf,
    /// Address on which the PostgreSQL wire listener is bound.
    pub bind_addr: std::net::SocketAddr,
    /// Create the database directory when it does not exist.
    pub create_if_missing: bool,
    /// Maximum number of connection tasks admitted at once.
    pub max_connections: usize,
}

impl ServerConfig {
    /// Build a server configuration with local development defaults.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>, bind_addr: std::net::SocketAddr) -> Self {
        Self {
            database_path: path.into(),
            bind_addr,
            create_if_missing: true,
            max_connections: 128,
        }
    }

    /// Set whether a missing database directory is created during startup.
    #[must_use]
    pub fn with_create_if_missing(mut self, create_if_missing: bool) -> Self {
        self.create_if_missing = create_if_missing;
        self
    }

    /// Set the connection admission bound.
    #[must_use]
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }
}

/// Errors from server startup, lifecycle, or durable shutdown.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("server configuration is invalid: {0}")]
    InvalidConfiguration(String),
    #[error("server database error: {0}")]
    Database(#[from] DbError),
    #[error("server I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("server task failed: {0}")]
    Task(String),
    #[error("server database handle still has live references during shutdown")]
    LiveDatabaseReferences,
    #[error("server database lock is poisoned during shutdown")]
    DatabaseLockPoisoned,
}

/// A bounded diagnostic projection of the server lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerStatus {
    pub active_connections: usize,
    pub accepted_connections: u64,
    pub rejected_connections: u64,
    pub max_connections: usize,
    /// Synchronous query and describe operations currently on tracked workers.
    pub active_operations: usize,
    /// Terminal operations whose worker returned a result or error.
    pub completed_operations: u64,
    /// Completed operations that returned a wire error.
    pub failed_operations: u64,
    /// Completed operations whose cancellation token was observed as cancelled.
    pub cancelled_operations: u64,
    pub shutting_down: bool,
}

struct ServerState {
    shutdown: AtomicBool,
    notify: Notify,
    active_connections: AtomicUsize,
    accepted_connections: AtomicU64,
    rejected_connections: AtomicU64,
    max_connections: usize,
    query_workers: Arc<QueryWorkers>,
}

/// Owns the lifetime accounting for synchronous database work moved off the
/// Tokio scheduler. Shutdown cancels operation tokens, joins connection tasks,
/// and waits for this counter to reach zero before closing the database.
struct QueryWorkers {
    active: AtomicUsize,
    completed: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    notify: Notify,
}

impl QueryWorkers {
    fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    fn spawn_operation<F, T>(
        self: &Arc<Self>,
        cancellation: CancellationToken,
        work: F,
    ) -> tokio::task::JoinHandle<PgWireResult<T>>
    where
        F: FnOnce() -> PgWireResult<T> + Send + 'static,
        T: Send + 'static,
    {
        self.active.fetch_add(1, Ordering::AcqRel);
        let tracker = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _guard = QueryWorkerGuard {
                tracker: Arc::clone(&tracker),
            };
            let result = work();
            tracker.completed.fetch_add(1, Ordering::Relaxed);
            if result.is_err() {
                tracker.failed.fetch_add(1, Ordering::Relaxed);
            }
            if cancellation.is_cancelled() {
                tracker.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            result
        })
    }

    fn status(&self) -> (usize, u64, u64, u64) {
        (
            self.active.load(Ordering::Acquire),
            self.completed.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.cancelled.load(Ordering::Relaxed),
        )
    }

    async fn wait_for_idle(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    fn complete(&self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }
}

struct QueryWorkerGuard {
    tracker: Arc<QueryWorkers>,
}

impl Drop for QueryWorkerGuard {
    fn drop(&mut self) {
        self.tracker.complete();
    }
}

impl ServerState {
    fn new(max_connections: usize) -> Self {
        Self {
            shutdown: AtomicBool::new(false),
            notify: Notify::new(),
            active_connections: AtomicUsize::new(0),
            accepted_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            max_connections,
            query_workers: Arc::new(QueryWorkers::new()),
        }
    }

    fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait_for_shutdown(&self) {
        let notified = self.notify.notified();
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }

    fn status(&self) -> ServerStatus {
        let (active_operations, completed_operations, failed_operations, cancelled_operations) =
            self.query_workers.status();
        ServerStatus {
            active_connections: self.active_connections.load(Ordering::Acquire),
            accepted_connections: self.accepted_connections.load(Ordering::Acquire),
            rejected_connections: self.rejected_connections.load(Ordering::Acquire),
            max_connections: self.max_connections,
            active_operations,
            completed_operations,
            failed_operations,
            cancelled_operations,
            shutting_down: self.shutdown.load(Ordering::Acquire),
        }
    }
}

/// A cloneable signal used to request server shutdown from another task.
#[derive(Clone)]
pub struct ServerShutdownHandle {
    state: Arc<ServerState>,
}

impl ServerShutdownHandle {
    /// Request shutdown. The call is idempotent and does not wait for cleanup.
    pub fn shutdown(&self) {
        self.state.request_shutdown();
    }

    /// Return whether shutdown has been requested.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.state.shutdown.load(Ordering::Acquire)
    }
}

/// A running persistent OmenDB server. Dropping it requests shutdown; call
/// [`Self::shutdown`] when the caller must observe the close result.
pub struct RunningServer {
    address: std::net::SocketAddr,
    state: Arc<ServerState>,
    task: Option<tokio::task::JoinHandle<Result<(), ServerError>>>,
}

impl RunningServer {
    /// Open or create the configured database, bind the wire listener, and
    /// start the bounded accept loop.
    pub async fn start(config: ServerConfig) -> Result<Self, ServerError> {
        if config.max_connections == 0 {
            return Err(ServerError::InvalidConfiguration(
                "max_connections must be positive".to_owned(),
            ));
        }
        if config.max_connections > Semaphore::MAX_PERMITS {
            return Err(ServerError::InvalidConfiguration(format!(
                "max_connections exceeds the runtime limit of {}",
                Semaphore::MAX_PERMITS
            )));
        }
        let database_path = config.database_path;
        let backend = crate::RelationalBackendConfig::new(database_path.clone());
        let database = if database_path_exists(&database_path) {
            RelationalDatabase::open(backend)?
        } else if config.create_if_missing {
            RelationalDatabase::create(backend)?
        } else {
            return Err(ServerError::InvalidConfiguration(format!(
                "database path does not exist: {}",
                database_path.display()
            )));
        };
        let listener = match TcpListener::bind(config.bind_addr).await {
            Ok(listener) => listener,
            Err(error) => {
                database.close()?;
                return Err(error.into());
            }
        };
        let address = match listener.local_addr() {
            Ok(address) => address,
            Err(error) => {
                database.close()?;
                return Err(error.into());
            }
        };
        let shared = Arc::new(RwLock::new(database));
        let state = Arc::new(ServerState::new(config.max_connections));
        let factory = match build_factory(&shared, address, Arc::clone(&state.query_workers)) {
            Ok(factory) => factory,
            Err(error) => {
                close_shared_database(shared)?;
                return Err(error.into());
            }
        };
        let task_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let serving = run_accept_loop(
                listener,
                factory,
                Arc::clone(&task_state),
                config.max_connections,
            )
            .await
            .map_err(ServerError::Io);
            let closed = close_shared_database(shared);
            match (serving, closed) {
                (Err(error), _) => Err(error),
                (Ok(()), Ok(())) => Ok(()),
                (Ok(()), Err(error)) => Err(error),
            }
        });
        Ok(Self {
            address,
            state,
            task: Some(task),
        })
    }

    /// Return the address selected by the listener, including an OS-assigned
    /// port when `bind_addr` used port zero.
    #[must_use]
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.address
    }

    /// Return a cloneable shutdown signal.
    #[must_use]
    pub fn shutdown_handle(&self) -> ServerShutdownHandle {
        ServerShutdownHandle {
            state: Arc::clone(&self.state),
        }
    }

    /// Return current connection and lifecycle counters.
    #[must_use]
    pub fn status(&self) -> ServerStatus {
        self.state.status()
    }

    /// Request shutdown, abort admitted connections, close the database, and
    /// report any listener or durable close error.
    pub async fn shutdown(mut self) -> Result<(), ServerError> {
        self.state.request_shutdown();
        let task = self
            .task
            .take()
            .ok_or_else(|| ServerError::Task("server shutdown was already awaited".to_owned()))?;
        task.await
            .map_err(|error| ServerError::Task(error.to_string()))?
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.state.request_shutdown();
    }
}

fn database_path_exists(path: &Path) -> bool {
    path.exists()
}

fn close_shared_database(shared: SharedDatabase) -> Result<(), ServerError> {
    let lock = Arc::try_unwrap(shared).map_err(|_| ServerError::LiveDatabaseReferences)?;
    let database = lock
        .into_inner()
        .map_err(|_| ServerError::DatabaseLockPoisoned)?;
    database.close()?;
    Ok(())
}

/// Serve PostgreSQL wire clients on an already-bound listener until the
/// listener errors. The listener's startup auth policy is derived from the
/// durable auth catalog; empty credentials are trust-only on loopback.
pub async fn serve(database: SharedDatabase, listener: TcpListener) -> std::io::Result<()> {
    let state = Arc::new(ServerState::new(Semaphore::MAX_PERMITS));
    let factory = build_factory(
        &database,
        listener.local_addr()?,
        Arc::clone(&state.query_workers),
    )?;
    run_accept_loop(listener, factory, state, Semaphore::MAX_PERMITS).await
}

async fn run_accept_loop(
    listener: TcpListener,
    factory: Arc<HandlerFactory>,
    state: Arc<ServerState>,
    max_connections: usize,
) -> std::io::Result<()> {
    let slots = Arc::new(Semaphore::new(max_connections.min(Semaphore::MAX_PERMITS)));
    let mut connections = JoinSet::new();
    let result = loop {
        tokio::select! {
            _ = state.wait_for_shutdown() => break Ok(()),
            accepted = listener.accept() => {
                let (socket, _peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                state.accepted_connections.fetch_add(1, Ordering::Relaxed);
                let permit = match Arc::clone(&slots).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        state.rejected_connections.fetch_add(1, Ordering::Relaxed);
                        drop(socket);
                        continue;
                    }
                };
                state.active_connections.fetch_add(1, Ordering::AcqRel);
                let peer = socket.peer_addr().ok();
                let handler = Arc::clone(&factory.handler);
                let connection_factory = Arc::clone(&factory);
                let task_state = Arc::clone(&state);
                connections.spawn(async move {
                    let _permit = permit;
                    let _ = process_socket(socket, None, connection_factory).await;
                    if let Some(peer) = peer {
                        handler.cleanup_connection(peer);
                    }
                    task_state.active_connections.fetch_sub(1, Ordering::AcqRel);
                });
            }
        }
    };
    // Cancel workers before aborting connection tasks. Blocking workers are
    // tracked separately because aborting their async joiners cannot stop a
    // running blocking task.
    factory.handler.cancellations.cancel_all();
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    factory.handler.query_workers.wait_for_idle().await;
    state.active_connections.store(0, Ordering::Release);
    result
}

/// Bind a listener and spawn the accept loop on the current tokio runtime,
/// returning the bound local address (useful for tests picking port 0).
pub async fn spawn(
    database: SharedDatabase,
    bind_addr: std::net::SocketAddr,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::task::JoinHandle<std::io::Result<()>>,
)> {
    let listener = TcpListener::bind(bind_addr).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(serve(database, listener));
    Ok((addr, handle))
}

/// Credential storage for wire authentication. The table is created by
/// `serve` and populated through `provision_wire_user`; an empty table
/// keeps trust mode so local development is unchanged.
const AUTH_TABLE: &str = "pgwire_auth";
const SCRAM_ITERATIONS: usize = 4096;
const SALT_LEN: usize = 32;

fn build_factory(
    database: &SharedDatabase,
    listener_addr: std::net::SocketAddr,
    query_workers: Arc<QueryWorkers>,
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
        let exponent = (*failures - 1).min(6);
        AUTH_FAILURE_DELAY_BASE_MS
            .saturating_mul(1 << exponent)
            .min(AUTH_FAILURE_DELAY_CAP_MS)
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

struct HandlerFactory {
    handler: Arc<OmenDbHandler>,
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

/// How long an untouched open transaction block survives before the idle
/// reaper aborts it. Stale blocks hold no kernel locks, so this bounds
/// orphaned-block memory only, not correctness.
const TX_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// A wire-parsed statement: raw SQL plus the parameter types the client
/// declared at Parse time. Placeholder count is derived by scanning the SQL
/// text so ParameterDescription reports the arity the engine will enforce.
#[derive(Debug, Clone)]
struct ParsedStatement {
    sql: String,
    parameter_types: Vec<Option<Type>>,
}

impl ParsedStatement {
    fn placeholder_count(sql: &str) -> usize {
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
struct PlaceholderParser;

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
struct TransactionBlock {
    transaction: RelationalDatabaseTransaction,
    /// Set when a statement inside the block failed; the block then rejects
    /// everything except ROLLBACK (and COMMIT, which rolls back).
    errored: bool,
    last_used: Instant,
}

type ProbeCache = HashMap<(String, String), Arc<Vec<(String, Type)>>>;

type IdentityMap = Mutex<HashMap<std::net::SocketAddr, String>>;
type FailureDelays = Mutex<std::collections::HashMap<std::net::IpAddr, u32>>;
type CancelKey = (i32, Vec<u8>);

/// Owns the bridge between PostgreSQL cancel requests and one cooperative
/// operation token per authenticated connection. The server's cancel handler
/// routes protocol identities here; this registry owns both the identity and
/// the database operation's cancellation state.
struct CancellationRegistry {
    entries: Mutex<HashMap<CancelKey, CancellationEntry>>,
}

struct CancellationEntry {
    address: std::net::SocketAddr,
    active: Option<Arc<WireQueryCancellation>>,
}

struct WireQueryCancellation {
    token: CancellationToken,
}

struct QueryCancellationLease {
    registry: Arc<CancellationRegistry>,
    key: CancelKey,
    operation: Arc<WireQueryCancellation>,
}

impl CancellationRegistry {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn register<C: ClientInfo>(&self, client: &C) {
        let (pid, secret_key) = client.pid_and_secret_key();
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(
                (pid, secret_key.to_bytes().to_vec()),
                CancellationEntry {
                    address: client.socket_addr(),
                    active: None,
                },
            );
        }
    }

    fn begin<C: ClientInfo>(
        self: &Arc<Self>,
        client: &C,
    ) -> Option<(CancellationToken, QueryCancellationLease)> {
        let (pid, secret_key) = client.pid_and_secret_key();
        let key = (pid, secret_key.to_bytes().to_vec());
        let operation = Arc::new(WireQueryCancellation {
            token: CancellationToken::new(),
        });
        let mut entries = self.entries.lock().ok()?;
        let entry = entries.get_mut(&key)?;
        entry.active = Some(Arc::clone(&operation));
        Some((
            operation.token.clone(),
            QueryCancellationLease {
                registry: Arc::clone(self),
                key,
                operation,
            },
        ))
    }

    fn cancel(&self, pid: i32, secret_key: &SecretKey) -> bool {
        let key = (pid, secret_key.to_bytes().to_vec());
        let Ok(entries) = self.entries.lock() else {
            return false;
        };
        let Some(entry) = entries.get(&key) else {
            return false;
        };
        if let Some(operation) = &entry.active {
            operation.token.cancel();
            true
        } else {
            false
        }
    }

    fn finish(&self, key: &CancelKey, operation: &Arc<WireQueryCancellation>) {
        if let Ok(mut entries) = self.entries.lock()
            && let Some(entry) = entries.get_mut(key)
            && entry
                .active
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, operation))
        {
            entry.active = None;
        }
    }

    fn cleanup_connection(&self, address: std::net::SocketAddr) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.retain(|_, entry| {
                if entry.address == address {
                    if let Some(operation) = &entry.active {
                        operation.token.cancel();
                    }
                    false
                } else {
                    true
                }
            });
        }
    }

    fn cancel_all(&self) {
        if let Ok(entries) = self.entries.lock() {
            for entry in entries.values() {
                if let Some(operation) = &entry.active {
                    operation.token.cancel();
                }
            }
        }
    }
}

impl Drop for QueryCancellationLease {
    fn drop(&mut self) {
        self.operation.token.cancel();
        self.registry.finish(&self.key, &self.operation);
    }
}

struct WireCancelHandler {
    cancellations: Arc<CancellationRegistry>,
}

#[async_trait]
impl CancelHandler for WireCancelHandler {
    async fn on_cancel_request(&self, request: CancelRequest) {
        self.cancellations.cancel(request.pid, &request.secret_key);
    }
}

#[derive(Clone)]
struct OmenDbHandler {
    database: SharedDatabase,
    transactions: Arc<Mutex<HashMap<std::net::SocketAddr, TransactionBlock>>>,
    cancellations: Arc<CancellationRegistry>,
    /// Authenticated role per connection pid, recorded by the startup
    /// handler after a successful exchange and read per statement for
    /// grant enforcement.
    identities: Arc<IdentityMap>,
    /// Describe-probe results keyed by (source text, declared parameter
    /// types). Stores the schema-independent column list so each prepare
    /// probes once instead of every describe; FieldInfo is rebuilt per
    /// call with the portal's negotiated format. Cleared wholesale on
    /// any DDL statement - DDL is rare and stale schemas are worse than
    /// re-probing.
    schema_probes: Arc<Mutex<ProbeCache>>,
    query_workers: Arc<QueryWorkers>,
}

fn pg_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

fn map_db_error(error: crate::DbError) -> PgWireError {
    let (code, message) = match &error {
        crate::DbError::Cancelled => (
            "57014",
            "canceling statement due to user request".to_owned(),
        ),
        crate::DbError::DeadlineExceeded => ("57014", error.to_string()),
        crate::DbError::UniqueViolation { .. } => ("23505", error.to_string()),
        crate::DbError::ForeignKeyViolation { .. }
        | crate::DbError::CascadeDepthExceeded { .. } => ("23503", error.to_string()),
        crate::DbError::SerializationConflict { .. }
        | crate::DbError::SeerWriteConflict { .. }
        | crate::DbError::SeerTreeConflict { .. }
        | crate::DbError::WriteWriteConflict { .. } => ("40001", error.to_string()),
        crate::DbError::SqlParse(_) => ("42601", error.to_string()),
        crate::DbError::SqlUnsupported { .. } => {
            ("0A000", format!("feature not supported: {error}"))
        }
        _ => ("XX000", error.to_string()),
    };
    pg_error(code, message)
}

/// Encode one logical value. Native types let pgwire's `ToSqlText` honor the
/// negotiated column format (text or binary) instead of forcing text.
fn encode_value(encoder: &mut DataRowEncoder, value: &Value) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field(&Option::<String>::None),
        Value::Bool(inner) => encoder.encode_field(inner),
        Value::U64(inner) => encoder.encode_field(&(*inner as i64)),
        Value::I64(inner) => encoder.encode_field(inner),
        Value::Text(inner) => encoder.encode_field(inner),
        Value::Bytes(inner) => encoder.encode_field(inner),
    }
}

fn value_type(value: Option<&Value>) -> Type {
    match value {
        Some(Value::Bool(_)) => Type::BOOL,
        Some(Value::U64(_) | Value::I64(_)) => Type::INT8,
        Some(Value::Text(_)) => Type::VARCHAR,
        Some(Value::Bytes(_)) => Type::BYTEA,
        _ => Type::TEXT,
    }
}

fn fields_from_result(
    result: &crate::SqlResult,
    format: &pgwire::api::portal::Format,
) -> Arc<Vec<FieldInfo>> {
    let sample = result.rows.first();
    Arc::new(
        result
            .columns
            .iter()
            .enumerate()
            .map(|(position, column)| {
                let ty = value_type(sample.and_then(|row| row.get(position)));
                let field_format = format.format_for(position);
                FieldInfo::new(column.name.clone(), None, None, ty, field_format)
            })
            .collect::<Vec<_>>(),
    )
}

fn encode_response_with_format(
    result: crate::SqlResult,
    format: &pgwire::api::portal::Format,
) -> Response {
    if result.rows.is_empty() && result.columns.is_empty() {
        return Response::Execution(Tag::new("OK").with_rows(result.affected_rows));
    }
    let schema = fields_from_result(&result, format);
    let rows = result.rows.clone();
    let data_row_stream = stream::iter(rows).then(move |row| {
        let schema = schema.clone();
        async move {
            let mut encoder = DataRowEncoder::new(schema);
            for value in &row {
                encode_value(&mut encoder, value)?;
            }
            Ok(encoder.take_row())
        }
    });
    Response::Query(QueryResponse::new(
        fields_from_result(&result, format),
        data_row_stream,
    ))
}

fn column_type_to_pg(column_type: crate::ColumnType) -> Type {
    match column_type {
        crate::ColumnType::Bool => Type::BOOL,
        crate::ColumnType::U64 | crate::ColumnType::I64 => Type::INT8,
        crate::ColumnType::Text => Type::VARCHAR,
        crate::ColumnType::Bytes => Type::BYTEA,
    }
}

impl OmenDbHandler {
    /// Resolve the wire type reported for each parameter: client-declared
    /// types win; otherwise statement-context inference from the SQL tier;
    /// otherwise text as the least-lossy fallback.
    fn resolved_parameter_types(
        &self,
        statement: &ParsedStatement,
        cancellation: &CancellationToken,
    ) -> PgWireResult<Vec<Type>> {
        let count = ParsedStatement::placeholder_count(&statement.sql);
        let database = read_lock_with_cancellation(&self.database, cancellation)?;
        let inferred = database
            .sql_parameter_types(&statement.sql)
            .map_err(map_db_error)?;
        Ok((0..count)
            .map(|index| {
                statement
                    .parameter_types
                    .get(index)
                    .cloned()
                    .flatten()
                    .or_else(|| {
                        inferred
                            .get(index)
                            .and_then(|column_type| column_type.map(column_type_to_pg))
                    })
                    .unwrap_or(Type::TEXT)
            })
            .collect())
    }
}

fn decode_parameter(raw: Option<&[u8]>, binary: bool, declared: &Type) -> PgWireResult<Value> {
    let declared = declared.clone();
    let Some(raw) = raw else {
        return Ok(Value::Null);
    };
    if binary {
        return Ok(match declared {
            Type::BOOL => Value::Bool(!raw.is_empty() && raw[0] != 0),
            Type::INT2 => match <[u8; 2]>::try_from(raw) {
                Ok(inner) => Value::I64(i64::from(i16::from_be_bytes(inner))),
                Err(_) => return Err(pg_error("22P02", "malformed int2 parameter".to_owned())),
            },
            Type::INT4 => match <[u8; 4]>::try_from(raw) {
                Ok(inner) => Value::I64(i64::from(i32::from_be_bytes(inner))),
                Err(_) => return Err(pg_error("22P02", "malformed int4 parameter".to_owned())),
            },
            Type::INT8 => match <[u8; 8]>::try_from(raw) {
                Ok(inner) => Value::I64(i64::from_be_bytes(inner)),
                Err(_) => return Err(pg_error("22P02", "malformed int8 parameter".to_owned())),
            },
            Type::BYTEA => Value::Bytes(raw.to_vec()),
            _ => Value::Text(String::from_utf8_lossy(raw).into_owned()),
        });
    }
    let text = String::from_utf8_lossy(raw).into_owned();
    Ok(match declared {
        Type::BOOL => Value::Bool(matches!(text.as_str(), "t" | "true" | "1")),
        Type::INT2 | Type::INT4 | Type::INT8 => match text.parse::<i64>() {
            Ok(inner) => Value::I64(inner),
            Err(_) => {
                return Err(pg_error(
                    "22P02",
                    format!("invalid integer parameter: {text}"),
                ));
            }
        },
        Type::BYTEA => Value::Bytes(text.into_bytes()),
        _ => Value::Text(text),
    })
}

fn decode_parameters(
    parameters: &[Option<Vec<u8>>],
    parameter_format: &pgwire::api::portal::Format,
    resolved: &[Type],
) -> PgWireResult<Vec<Value>> {
    (0..parameters.len())
        .map(|index| {
            decode_parameter(
                parameters.get(index).and_then(|raw| raw.as_deref()),
                parameter_format.is_binary(index),
                resolved.get(index).unwrap_or(&Type::TEXT),
            )
        })
        .collect()
}

/// Classify a wire statement as a transaction-control command.
enum TransactionCommand {
    Begin,
    Commit,
    Rollback,
}

fn transaction_command(statement: &str) -> Option<TransactionCommand> {
    let head = statement
        .split_whitespace()
        .next()?
        .trim_start_matches('(')
        .to_ascii_uppercase();
    match head.as_str() {
        "BEGIN" | "START" => Some(TransactionCommand::Begin),
        "COMMIT" | "END" => Some(TransactionCommand::Commit),
        "ROLLBACK" | "ABORT" => Some(TransactionCommand::Rollback),
        _ => None,
    }
}

impl OmenDbHandler {
    fn cleanup_connection(&self, client_addr: std::net::SocketAddr) {
        if let Ok(mut blocks) = self.transactions.lock() {
            blocks.remove(&client_addr);
        }
        if let Ok(mut identities) = self.identities.lock() {
            identities.remove(&client_addr);
        }
        self.cancellations.cleanup_connection(client_addr);
    }

    fn begin_query<C: ClientInfo>(
        &self,
        client: &C,
    ) -> (CancellationToken, Option<QueryCancellationLease>) {
        self.cancellations.begin(client).map_or_else(
            || (CancellationToken::new(), None),
            |(token, lease)| (token, Some(lease)),
        )
    }

    fn lock_transactions(
        &self,
    ) -> PgWireResult<std::sync::MutexGuard<'_, HashMap<std::net::SocketAddr, TransactionBlock>>>
    {
        self.transactions
            .lock()
            .map_err(|_| pg_error("XX000", "transaction table poisoned".to_owned()))
    }

    /// Abort transaction blocks orphaned by disconnected clients. Cheap
    /// scan amortized over handled statements; stale blocks hold no locks.
    fn reap_idle_blocks(&self, blocks: &mut HashMap<std::net::SocketAddr, TransactionBlock>) {
        blocks.retain(|_, block| block.last_used.elapsed() < TX_IDLE_TIMEOUT);
    }

    /// Route one wire statement: transaction control, in-block execution,
    /// or autocommit. `format` is the negotiated result-column format; the
    /// statement executes exactly once.
    fn run_statement(
        &self,
        client_addr: std::net::SocketAddr,
        sql: &str,
        params: &[Value],
        format: &pgwire::api::portal::Format,
        cancellation: &CancellationToken,
    ) -> PgWireResult<Response> {
        if cancellation.is_cancelled() {
            return Err(map_db_error(DbError::Cancelled));
        }
        if Self::is_schema_statement(sql)
            && let Ok(mut cache) = self.schema_probes.lock()
        {
            cache.clear();
        }
        // The transaction map lock guards map membership only. Statement
        // execution happens OUTSIDE the lock: a slow statement on one
        // connection must never block other connections from reaching
        // the database RwLock. In-block work takes its block out of the
        // map for the duration and reinserts it after.
        {
            let mut blocks = self.lock_transactions()?;
            self.reap_idle_blocks(&mut blocks);
        }
        self.enforce_grants(client_addr, sql, cancellation)?;
        if cancellation.is_cancelled() {
            return Err(map_db_error(DbError::Cancelled));
        }
        match transaction_command(sql) {
            Some(TransactionCommand::Begin) => {
                let mut blocks = self.lock_transactions()?;
                if let Some(block) = blocks.get_mut(&client_addr) {
                    block.last_used = Instant::now();
                    if block.errored {
                        return Err(pg_error(
                            "25P02",
                            "current transaction is aborted, commands ignored until end of transaction block".to_owned(),
                        ));
                    }
                    return Ok(Response::TransactionStart(Tag::new("BEGIN")));
                }
                drop(blocks);
                // begin() only needs &self: opening a block takes no write
                // access and publishes nothing.
                let control = OperationControl::with_cancellation(cancellation.clone());
                let transaction = read_lock_with_cancellation(&self.database, cancellation)?
                    .begin_with_control(&control)
                    .map_err(map_db_error)?;
                if cancellation.is_cancelled() {
                    return Err(map_db_error(DbError::Cancelled));
                }
                let mut blocks = self.lock_transactions()?;
                blocks.insert(
                    client_addr,
                    TransactionBlock {
                        transaction,
                        errored: false,
                        last_used: Instant::now(),
                    },
                );
                Ok(Response::TransactionStart(Tag::new("BEGIN")))
            }
            Some(TransactionCommand::Commit) => {
                let block = {
                    let mut blocks = self.lock_transactions()?;
                    blocks.remove(&client_addr)
                };
                let Some(mut block) = block else {
                    return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
                };
                if block.errored {
                    drop(block.transaction);
                    return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
                }
                // Publication is the serialized-writer boundary; the map
                // is free while this commit publishes.
                let control = OperationControl::with_cancellation(cancellation.clone());
                block.transaction.set_operation_control(&control);
                let _database = write_lock_with_cancellation(&self.database, cancellation)?;
                block.transaction.commit().map_err(map_db_error)?;
                Ok(Response::TransactionEnd(Tag::new("COMMIT")))
            }
            Some(TransactionCommand::Rollback) => {
                let block = {
                    let mut blocks = self.lock_transactions()?;
                    blocks.remove(&client_addr)
                };
                if let Some(block) = block {
                    drop(block.transaction);
                }
                Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
            }
            None => {
                // Membership check under the lock; execution outside it.
                let in_block = { self.lock_transactions()?.contains_key(&client_addr) };
                if !in_block {
                    return self.run_autocommit(sql, params, format, cancellation);
                }

                // Take this connection's block out so other connections
                // proceed while its statements execute.
                let mut block = {
                    self.lock_transactions()?
                        .remove(&client_addr)
                        .expect("membership checked above")
                };
                if block.errored {
                    self.lock_transactions()?.insert(client_addr, block);
                    return Err(pg_error(
                        "25P02",
                        "current transaction is aborted, commands ignored until end of transaction block".to_owned(),
                    ));
                }
                block.last_used = Instant::now();
                let control = OperationControl::with_cancellation(cancellation.clone());
                block.transaction.set_operation_control(&control);
                // Buffered into the transaction; publication happens at
                // COMMIT under the write lock, so execution only needs
                // shared access.
                let database = read_lock_with_cancellation(&self.database, cancellation)?;
                let outcome = block
                    .transaction
                    .execute_sql_with_params(&database, sql, params);
                drop(database);
                let response = match outcome {
                    Ok(result) => Ok(encode_response_with_format(result, format)),
                    Err(error) => {
                        block.errored = true;
                        Err(map_db_error(error))
                    }
                };
                self.lock_transactions()?.insert(client_addr, block);
                response
            }
        }
    }

    /// Grant enforcement for authenticated roles. Inactive until any
    /// row exists in pgwire_grants (bootstrap state grants full access
    /// so provisioning works); then a role gets exactly its granted
    /// access. The wildcard table "*" with can_write is schema
    /// administration (DDL). Trust-mode connections carry no identity
    /// and are loopback-only.
    fn enforce_grants(
        &self,
        client_addr: std::net::SocketAddr,
        sql: &str,
        cancellation: &CancellationToken,
    ) -> PgWireResult<()> {
        let Some(role) = self
            .identities
            .lock()
            .ok()
            .and_then(|identities| identities.get(&client_addr).cloned())
        else {
            return Ok(());
        };

        let database = read_lock_with_cancellation(&self.database, cancellation)?;
        if !database
            .catalog()
            .tables()
            .any(|table| table.name == "pgwire_grants")
        {
            return Ok(());
        }
        let mut transaction = database.begin().map_err(map_db_error)?;
        let result = transaction.execute_sql_with_params(
            &database,
            "SELECT table_name, can_read, can_write FROM pgwire_grants WHERE role = $1",
            &[Value::Text(role.clone())],
        );
        drop(transaction);
        let result = result.map_err(map_db_error)?;
        if cancellation.is_cancelled() {
            return Err(map_db_error(DbError::Cancelled));
        }

        // No rows at all means grants exist only for other roles; this
        // role still defaults to deny.
        let mut admin = false;
        let mut table_grants: HashMap<String, (bool, bool)> = HashMap::new();
        for row in &result.rows {
            if cancellation.is_cancelled() {
                return Err(map_db_error(DbError::Cancelled));
            }
            let Some(Value::Text(table_name)) = row.first() else {
                continue;
            };
            let can_read = matches!(row.get(1), Some(Value::Bool(true)));
            let can_write = matches!(row.get(2), Some(Value::Bool(true)));
            if table_name == "*" && can_write {
                admin = true;
            }
            table_grants.insert(table_name.clone(), (can_read, can_write));
        }

        let (read_tables, write_tables, requires_admin) =
            crate::sql::statement_access(sql).map_err(map_db_error)?;
        // Admin grant must be consulted BEFORE refusing DDL.
        if admin {
            return Ok(());
        }
        if requires_admin {
            return Err(pg_error(
                "42501",
                format!("permission denied: role {role} lacks schema administration grant"),
            ));
        }
        for table in &write_tables {
            match table_grants.get(table) {
                Some((_, true)) => {}
                _ => {
                    return Err(pg_error(
                        "42501",
                        format!(
                            "permission denied for table {table}: role {role} lacks write grant"
                        ),
                    ));
                }
            }
        }
        for table in &read_tables {
            if write_tables.contains(table) {
                continue;
            }
            match table_grants.get(table) {
                Some((true, _)) | Some((_, true)) => {}
                _ => {
                    return Err(pg_error(
                        "42501",
                        format!(
                            "permission denied for table {table}: role {role} lacks read grant"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Autocommit path: no transaction block exists for this connection.
    fn run_autocommit(
        &self,
        sql: &str,
        params: &[Value],
        format: &pgwire::api::portal::Format,
        cancellation: &CancellationToken,
    ) -> PgWireResult<Response> {
        if Self::is_schema_statement(sql) {
            // Schema changes are owned by the direct database method rather
            // than a relational transaction. Cancellation is therefore a
            // preflight check for this non-interruptible publication.
            if cancellation.is_cancelled() {
                return Err(map_db_error(DbError::Cancelled));
            }
            let mut database = write_lock_with_cancellation(&self.database, cancellation)?;
            if cancellation.is_cancelled() {
                return Err(map_db_error(DbError::Cancelled));
            }
            return Ok(encode_response_with_format(
                database
                    .execute_sql_with_params(sql, params)
                    .map_err(map_db_error)?,
                format,
            ));
        }
        let control = OperationControl::with_cancellation(cancellation.clone());
        if Self::is_row_returning(sql) && !Self::has_returning_clause(sql) {
            // Reads scale: snapshot query under shared access via an
            // autocommit transaction that aborts on completion.
            let database = read_lock_with_cancellation(&self.database, cancellation)?;
            let mut transaction = database
                .begin_with_control(&control)
                .map_err(map_db_error)?;
            let result = transaction.execute_sql_with_params(&database, sql, params);
            drop(transaction);
            return Ok(encode_response_with_format(
                result.map_err(map_db_error)?,
                format,
            ));
        }
        let mut database = write_lock_with_cancellation(&self.database, cancellation)?;
        let (result, _) = database
            .transaction_with_control(&control, |database, transaction| {
                transaction.execute_sql_with_params(database, sql, params)
            })
            .map_err(map_db_error)?;
        Ok(encode_response_with_format(result, format))
    }

    fn is_schema_statement(statement: &str) -> bool {
        matches!(
            statement
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .trim_start_matches('(')
                .to_ascii_uppercase()
                .as_str(),
            "CREATE" | "DROP" | "ALTER"
        )
    }

    /// True when a statement produces rows and is safe to probe for its
    /// result schema. DML and DDL describe as zero-column in PostgreSQL,
    /// so they skip execution entirely here.
    /// DML with a RETURNING clause produces rows but MUST take the
    /// write path - the read-lock probe would discard its writes.
    fn has_returning_clause(statement: &str) -> bool {
        statement
            .split_whitespace()
            .enumerate()
            .any(|(index, token)| index > 0 && token.eq_ignore_ascii_case("RETURNING"))
    }

    fn is_row_returning(statement: &str) -> bool {
        matches!(
            statement
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .trim_start_matches('(')
                .to_ascii_uppercase()
                .as_str(),
            "SELECT" | "WITH" | "TABLE" | "VALUES" | "SHOW"
        )
    }

    /// Compute a statement's result schema without durable effect: the typed
    /// transaction aborts on drop, so describe is side-effect free. Probes
    /// bind benign values for placeholders using the client's declared
    /// parameter types.
    fn describe_schema(
        &self,
        sql: &str,
        resolved: &[Type],
        format: &pgwire::api::portal::Format,
        cancellation: &CancellationToken,
    ) -> PgWireResult<Arc<Vec<FieldInfo>>> {
        if !Self::is_row_returning(sql) && !Self::has_returning_clause(sql) {
            return Ok(Arc::new(Vec::new()));
        }
        let probe_params: Vec<Value> = resolved
            .iter()
            .map(|declared| match declared {
                &Type::BOOL => Value::Bool(false),
                &Type::INT2 | &Type::INT4 | &Type::INT8 => Value::I64(0),
                &Type::BYTEA => Value::Bytes(Vec::new()),
                _ => Value::Text(String::new()),
            })
            .collect();
        let type_signature = resolved
            .iter()
            .map(|ty| ty.name().to_owned())
            .collect::<Vec<_>>()
            .join(",");
        if let Ok(cache) = self.schema_probes.lock()
            && let Some(columns) = cache.get(&(sql.to_owned(), type_signature.clone()))
        {
            return Ok(Arc::new(
                columns
                    .iter()
                    .enumerate()
                    .map(|(position, (name, ty))| {
                        FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            ty.clone(),
                            format.format_for(position),
                        )
                    })
                    .collect(),
            ));
        }
        let database = read_lock_with_cancellation(&self.database, cancellation)?;
        let control = OperationControl::with_cancellation(cancellation.clone());
        let mut transaction = database
            .begin_with_control(&control)
            .map_err(map_db_error)?;
        let result = transaction.execute_sql_with_params(&database, sql, &probe_params);
        drop(transaction);
        let result = result.map_err(map_db_error)?;
        if cancellation.is_cancelled() {
            return Err(map_db_error(DbError::Cancelled));
        }
        let sample = result.rows.first();
        let columns: Arc<Vec<(String, Type)>> = Arc::new(
            result
                .columns
                .iter()
                .enumerate()
                .map(|(position, column)| {
                    (
                        column.name.clone(),
                        value_type(sample.and_then(|row| row.get(position))),
                    )
                })
                .collect(),
        );
        if let Ok(mut cache) = self.schema_probes.lock()
            && cache.len() >= 1024
        {
            cache.clear();
        }
        if let Ok(mut cache) = self.schema_probes.lock() {
            cache.insert((sql.to_owned(), type_signature), columns.clone());
        }
        Ok(Arc::new(
            columns
                .iter()
                .enumerate()
                .map(|(position, (name, ty))| {
                    FieldInfo::new(
                        name.clone(),
                        None,
                        None,
                        ty.clone(),
                        format.format_for(position),
                    )
                })
                .collect(),
        ))
    }
}

#[async_trait]
impl SimpleQueryHandler for OmenDbHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let client_addr = _client.socket_addr();
        let (cancellation, _lease) = self.begin_query(_client);
        let handler = self.clone();
        let query = query.to_owned();
        self.query_workers
            .spawn_operation(cancellation.clone(), move || {
                let mut responses = Vec::new();
                // The embedded tier accepts one statement per call;
                // simple-protocol strings may carry several, so split
                // conservatively on semicolons.
                for statement in query.split(';') {
                    let statement = statement.trim();
                    if statement.is_empty() {
                        continue;
                    }
                    responses.push(handler.run_statement(
                        client_addr,
                        statement,
                        &[],
                        &pgwire::api::portal::Format::UnifiedText,
                        &cancellation,
                    )?);
                }
                if responses.is_empty() {
                    responses.push(Response::Execution(Tag::new("OK")));
                }
                Ok(responses)
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))?
    }
}

#[async_trait]
impl pgwire::api::query::ExtendedQueryHandler for OmenDbHandler {
    type Statement = ParsedStatement;
    type QueryParser = PlaceholderParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(PlaceholderParser)
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (cancellation, _lease) = self.begin_query(_client);
        let handler = self.clone();
        let statement = portal.statement.statement.clone();
        let parameters = portal
            .parameters
            .iter()
            .map(|raw| raw.as_ref().map(|raw| raw.to_vec()))
            .collect::<Vec<_>>();
        let parameter_format = portal.parameter_format.clone();
        let format = portal.result_column_format.clone();
        let client_addr = _client.socket_addr();
        self.query_workers
            .spawn_operation(cancellation.clone(), move || {
                let resolved = handler.resolved_parameter_types(&statement, &cancellation)?;
                let params = decode_parameters(&parameters, &parameter_format, &resolved)?;
                handler.run_statement(client_addr, &statement.sql, &params, &format, &cancellation)
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))?
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        statement: &pgwire::api::stmt::StoredStatement<Self::Statement>,
    ) -> PgWireResult<pgwire::api::results::DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (cancellation, _lease) = self.begin_query(_client);
        let handler = self.clone();
        let statement = statement.statement.clone();
        let (parameter_types, fields) = self
            .query_workers
            .spawn_operation(cancellation.clone(), move || {
                let parameter_types =
                    handler.resolved_parameter_types(&statement, &cancellation)?;
                let fields = handler.describe_schema(
                    &statement.sql,
                    &parameter_types,
                    &pgwire::api::portal::Format::UnifiedBinary,
                    &cancellation,
                )?;
                Ok::<_, PgWireError>((parameter_types, fields))
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))??;
        let fields = Arc::try_unwrap(fields).unwrap_or_default();
        Ok(pgwire::api::results::DescribeStatementResponse::new(
            parameter_types,
            fields,
        ))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
    ) -> PgWireResult<pgwire::api::results::DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (cancellation, _lease) = self.begin_query(_client);
        let handler = self.clone();
        let statement = portal.statement.statement.clone();
        let format = portal.result_column_format.clone();
        let fields = self
            .query_workers
            .spawn_operation(cancellation.clone(), move || {
                let resolved = handler.resolved_parameter_types(&statement, &cancellation)?;
                handler.describe_schema(&statement.sql, &resolved, &format, &cancellation)
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))??;
        let fields = Arc::try_unwrap(fields).unwrap_or_default();
        Ok(pgwire::api::results::DescribePortalResponse::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::api::DefaultClient;

    #[test]
    fn cancellation_registry_routes_and_releases_query_tokens() {
        let address = "127.0.0.1:6543".parse().expect("test address");
        let mut client = DefaultClient::<ParsedStatement>::new(address, false);
        client.set_pid_and_secret_key(17, SecretKey::I32(23));
        let registry = Arc::new(CancellationRegistry::new());
        registry.register(&client);

        let (token, lease) = registry.begin(&client).expect("registered client");
        assert!(!token.is_cancelled());
        assert!(registry.cancel(17, &SecretKey::I32(23)));
        assert!(token.is_cancelled());
        drop(lease);
        assert!(!registry.cancel(17, &SecretKey::I32(23)));

        registry.cleanup_connection(address);
        assert!(registry.begin(&client).is_none());
    }

    #[tokio::test]
    async fn query_worker_tracker_records_terminal_operation_outcomes() {
        let workers = Arc::new(QueryWorkers::new());
        let _ = workers
            .spawn_operation(CancellationToken::new(), || Ok::<_, PgWireError>(()))
            .await
            .expect("successful operation");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        workers
            .spawn_operation(cancellation, || {
                Err::<(), _>(pg_error("XX000", "synthetic failure".to_owned()))
            })
            .await
            .expect("worker completed")
            .expect_err("failed operation");
        assert_eq!(workers.status(), (0, 2, 1, 1));
    }

    #[tokio::test]
    async fn query_worker_tracker_drains_dropped_join_handles() {
        let workers = Arc::new(QueryWorkers::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        drop(workers.spawn_operation(CancellationToken::new(), move || {
            started_tx.send(()).expect("worker start receiver");
            release_rx.recv().expect("worker release");
            Ok::<_, PgWireError>(())
        }));
        started_rx.await.expect("worker started");

        let waiter = tokio::spawn({
            let workers = Arc::clone(&workers);
            async move { workers.wait_for_idle().await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());
        release_tx.send(()).expect("release worker");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("worker tracker drained")
            .expect("waiter task");
    }
}
