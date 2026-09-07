//! Server lifecycle: configuration, connection admission, blocking-query
//! worker accounting, shutdown, and the running-server handle.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinSet;

use super::auth::{HandlerFactory, build_factory};
use super::{OperationControl, SharedDatabase, map_db_error, pg_error};
use crate::{CancellationToken, DbError, RelationalDatabase};

const MAX_CAPACITY_CANCEL_CONNECTIONS: usize = 8;

const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(1);

pub(crate) fn read_lock(
    database: &SharedDatabase,
) -> PgWireResult<std::sync::RwLockReadGuard<'_, RelationalDatabase>> {
    database
        .read()
        .map_err(|_| pg_error("XX000", "database lock poisoned".to_owned()))
}

pub(crate) fn write_lock(
    database: &SharedDatabase,
) -> PgWireResult<std::sync::RwLockWriteGuard<'_, RelationalDatabase>> {
    database
        .write()
        .map_err(|_| pg_error("XX000", "database lock poisoned".to_owned()))
}

pub(crate) fn read_lock_with_control<'a>(
    database: &'a SharedDatabase,
    control: &OperationControl,
) -> PgWireResult<std::sync::RwLockReadGuard<'a, RelationalDatabase>> {
    loop {
        control.check().map_err(map_db_error)?;
        match database.try_read() {
            Ok(guard) => {
                if let Err(error) = control.check() {
                    drop(guard);
                    return Err(map_db_error(error));
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

pub(crate) fn write_lock_with_control<'a>(
    database: &'a SharedDatabase,
    control: &OperationControl,
) -> PgWireResult<std::sync::RwLockWriteGuard<'a, RelationalDatabase>> {
    loop {
        control.check().map_err(map_db_error)?;
        match database.try_write() {
            Ok(guard) => {
                if let Err(error) = control.check() {
                    drop(guard);
                    return Err(map_db_error(error));
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
    /// Maximum number of session connections admitted at once. At capacity,
    /// up to eight additional sockets may read a bounded cancel packet for
    /// at most one second; those sockets cannot start a session.
    pub max_connections: usize,
    /// Optional cooperative deadline for each wire statement and describe.
    /// `None` disables the deadline; zero is an immediate deadline.
    pub statement_timeout: Option<Duration>,
    /// Optional estimated result-payload byte bound per wire statement.
    pub max_result_bytes: Option<usize>,
    /// Ack commits after the group-synced WAL append instead of full
    /// publication; trades recovery replay for commit latency.
    pub wal_first_commits: bool,
    /// Publication-sync durability class. Defaults to the device barrier
    /// (power-loss safe; ~4.2 ms/sync on macOS F_FULLFSYNC); the kernel
    /// barrier class (~0.03 ms/sync) survives process and kernel crash
    /// only. See `crate::SyncClass`.
    pub sync_class: crate::SyncClass,
    /// Log statements that take longer than this to execute (including
    /// commit publication) to the server log. `None` disables slow
    /// statement logging.
    pub slow_statement_threshold: Option<Duration>,
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
            statement_timeout: None,
            max_result_bytes: None,
            wal_first_commits: false,
            sync_class: crate::SyncClass::default(),
            slow_statement_threshold: None,
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

    /// Set the cooperative statement and describe deadline.
    #[must_use]
    pub fn with_statement_timeout(mut self, statement_timeout: Option<Duration>) -> Self {
        self.statement_timeout = statement_timeout;
        self
    }

    /// Set the estimated result-payload byte bound per wire statement.
    #[must_use]
    pub fn with_max_result_bytes(mut self, max_result_bytes: Option<usize>) -> Self {
        self.max_result_bytes = max_result_bytes;
        self
    }

    /// Ack commits after the group-synced WAL append (WAL-first mode).
    #[must_use]
    pub fn with_wal_first_commits(mut self, wal_first_commits: bool) -> Self {
        self.wal_first_commits = wal_first_commits;
        self
    }

    /// Select the publication-sync durability class.
    #[must_use]
    pub fn with_sync_class(mut self, sync_class: crate::SyncClass) -> Self {
        self.sync_class = sync_class;
        self
    }

    /// Set the duration above which a statement (including its commit
    /// publication) is logged to the server log.
    #[must_use]
    pub fn with_slow_statement_threshold(mut self, threshold: Option<Duration>) -> Self {
        self.slow_statement_threshold = threshold;
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
    /// Admitted session connections, excluding bounded cancel-only readers.
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

pub(super) struct ServerState {
    shutdown: AtomicBool,
    notify: Notify,
    active_connections: AtomicUsize,
    accepted_connections: AtomicU64,
    rejected_connections: AtomicU64,
    max_connections: usize,
    #[cfg(test)]
    retained_connection_tasks: AtomicUsize,
    query_workers: Arc<QueryWorkers>,
}

/// Owns the lifetime accounting for synchronous database work moved off the
/// Tokio scheduler. Shutdown cancels operation tokens, joins connection tasks,
/// and waits for this counter to reach zero before closing the database.
pub(crate) struct QueryWorkers {
    active: AtomicUsize,
    completed: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    notify: Notify,
}

impl QueryWorkers {
    pub(super) fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    pub(crate) fn spawn_operation<F, T>(
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

    pub(crate) fn status(&self) -> (usize, u64, u64, u64) {
        (
            self.active.load(Ordering::Acquire),
            self.completed.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.cancelled.load(Ordering::Relaxed),
        )
    }

    pub(crate) async fn wait_for_idle(&self) {
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

pub(crate) struct QueryWorkerGuard {
    tracker: Arc<QueryWorkers>,
}

impl Drop for QueryWorkerGuard {
    fn drop(&mut self) {
        self.tracker.complete();
    }
}

impl ServerState {
    pub(super) fn new(max_connections: usize) -> Self {
        Self {
            shutdown: AtomicBool::new(false),
            notify: Notify::new(),
            active_connections: AtomicUsize::new(0),
            accepted_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            max_connections,
            #[cfg(test)]
            retained_connection_tasks: AtomicUsize::new(0),
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

    pub(crate) fn status(&self) -> ServerStatus {
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
        let mut backend = crate::RelationalBackendConfig::new(database_path.clone());
        if config.wal_first_commits {
            backend = backend.with_wal_first_commits();
        }
        backend = backend.with_sync_class(config.sync_class);
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
        let factory = match build_factory(
            &shared,
            address,
            Arc::clone(&state.query_workers),
            config.statement_timeout,
            config.max_result_bytes,
            config.slow_statement_threshold,
        ) {
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
        None,
        None,
        None,
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
    let mut control_connections = JoinSet::new();
    let control_slots = Arc::new(Semaphore::new(MAX_CAPACITY_CANCEL_CONNECTIONS));
    let result = loop {
        #[cfg(test)]
        state
            .retained_connection_tasks
            .store(connections.len(), Ordering::Release);
        tokio::select! {
            _ = state.wait_for_shutdown() => break Ok(()),
            _ = connections.join_next(), if !connections.is_empty() => {},
            _ = control_connections.join_next(), if !control_connections.is_empty() => {},
            accepted = listener.accept() => {
                let (socket, _peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                state.accepted_connections.fetch_add(1, Ordering::Relaxed);
                let permit = match Arc::clone(&slots).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        let Ok(control_permit) = Arc::clone(&control_slots).try_acquire_owned() else {
                            state.rejected_connections.fetch_add(1, Ordering::Relaxed);
                            drop(socket);
                            continue;
                        };
                        let cancellations = Arc::clone(&factory.handler.cancellations);
                        let task_state = Arc::clone(&state);
                        control_connections.spawn(async move {
                            let _permit = control_permit;
                            let mut socket = socket;
                            if !super::cancellation::receive_capacity_cancel(&mut socket, &cancellations).await {
                                task_state.rejected_connections.fetch_add(1, Ordering::Relaxed);
                            }
                        });
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
    control_connections.abort_all();
    while connections.join_next().await.is_some() {}
    while control_connections.join_next().await.is_some() {}
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_request_works_at_session_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let database = Arc::new(RwLock::new(
            RelationalDatabase::create(crate::RelationalBackendConfig::new(
                directory.path().join("db"),
            ))
            .unwrap(),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(ServerState::new(1));
        let factory = build_factory(
            &database,
            address,
            Arc::clone(&state.query_workers),
            None,
            None,
            None,
        )
        .unwrap();
        let server = tokio::spawn(run_accept_loop(listener, factory, Arc::clone(&state), 1));
        let dsn = format!("host=127.0.0.1 port={} user=omendb", address.port());
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        assert!(
            tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .is_err()
        );
        let cancel = client.cancel_token();
        let (release, released) = std::sync::mpsc::channel();
        let (locked, acquired) = std::sync::mpsc::channel();
        let locked_database = Arc::clone(&database);
        let lock_thread = std::thread::spawn(move || {
            let _lock = locked_database.write().unwrap();
            locked.send(()).unwrap();
            let _ = released.recv_timeout(Duration::from_secs(5));
        });
        acquired.recv_timeout(Duration::from_secs(2)).unwrap();
        let query = tokio::spawn(async move { client.batch_execute("SELECT 1").await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.status().active_operations == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let sent = cancel.cancel_query(tokio_postgres::NoTls).await;
        let outcome = tokio::time::timeout(Duration::from_secs(2), query).await;
        let _ = release.send(());
        lock_thread.join().unwrap();
        state.request_shutdown();
        server.await.unwrap().unwrap();
        let _ = connection.await;
        sent.expect("send cancellation at capacity");
        let error = outcome
            .expect("cancelled query must finish before lock release")
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::QUERY_CANCELED)
        );
    }

    #[tokio::test]
    async fn incomplete_capacity_control_connections_are_bounded_and_expire() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let directory = tempfile::tempdir().unwrap();
        let server = RunningServer::start(
            ServerConfig::new(directory.path().join("db"), "127.0.0.1:0".parse().unwrap())
                .with_max_connections(1),
        )
        .await
        .unwrap();
        let session = tokio::net::TcpStream::connect(server.local_addr())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.status().active_connections != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut controls = Vec::new();
        for _ in 0..MAX_CAPACITY_CANCEL_CONNECTIONS {
            let mut socket = tokio::net::TcpStream::connect(server.local_addr())
                .await
                .unwrap();
            socket.write_all(&[0]).await.unwrap();
            controls.push(socket);
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.status().accepted_connections != 1 + MAX_CAPACITY_CANCEL_CONNECTIONS as u64
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut overflow = tokio::net::TcpStream::connect(server.local_addr())
            .await
            .unwrap();
        let mut byte = [0];
        let closed = tokio::time::timeout(Duration::from_millis(500), overflow.read(&mut byte))
            .await
            .expect("excess control connection rejected immediately");
        assert!(matches!(closed, Ok(0) | Err(_)));
        tokio::time::timeout(Duration::from_secs(2), async {
            for mut socket in controls {
                assert!(matches!(socket.read(&mut byte).await, Ok(0) | Err(_)));
            }
        })
        .await
        .expect("partial startup controls expired");
        assert_eq!(server.status().active_connections, 1);
        assert_eq!(
            server.status().rejected_connections,
            MAX_CAPACITY_CANCEL_CONNECTIONS as u64 + 1
        );
        drop(session);
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn completed_connections_are_reaped_while_listener_is_running() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let server = RunningServer::start(
            ServerConfig::new(directory.path().join("db"), "127.0.0.1:0".parse().unwrap())
                .with_max_connections(1),
        )
        .await
        .expect("start server");
        for _ in 0..3 {
            let socket = tokio::net::TcpStream::connect(server.local_addr())
                .await
                .expect("connect");
            tokio::time::timeout(Duration::from_secs(2), async {
                while server
                    .state
                    .retained_connection_tasks
                    .load(Ordering::Acquire)
                    == 0
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("connection task registered");
            drop(socket);
            tokio::time::timeout(Duration::from_secs(2), async {
                while server
                    .state
                    .retained_connection_tasks
                    .load(Ordering::Acquire)
                    != 0
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("completed task reaped without shutdown or another connection");
            assert_eq!(server.status().active_connections, 0);
            assert!(!server.status().shutting_down);
        }
        server.shutdown().await.expect("shutdown server");
    }
}
