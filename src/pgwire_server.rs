//! PostgreSQL wire-protocol serving for the bounded embedded SQL tier.
//!
//! V1 scope per `design/PGWIRE_V1.md`: trust authentication (local-only
//! until real auth lands), simple and extended query protocols with
//! parameterized execution, and wire transaction blocks (`BEGIN`/`COMMIT`/
//! `ROLLBACK`) mapped to the typed transaction API with PostgreSQL
//! aborted-state semantics. Contract details and non-goals live in the
//! design note.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::{stream, StreamExt};use pgwire::api::auth::sasl::scram::{
    gen_salted_password, random_nonce, ScramAuth,
};
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use crate::{
    ColumnDefinition, ColumnId, ColumnType, DbError, RelationalDatabase,
    RelationalDatabaseTransaction, TableDefinition, TableId, Value,
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

fn read_lock(
    database: &SharedDatabase,
) -> PgWireResult<std::sync::RwLockReadGuard<'_, RelationalDatabase>> {
    database.read().map_err(|_| {
        pg_error("XX000", "database lock poisoned".to_owned())
    })
}

fn write_lock(
    database: &SharedDatabase,
) -> PgWireResult<std::sync::RwLockWriteGuard<'_, RelationalDatabase>> {
    database.write().map_err(|_| {
        pg_error("XX000", "database lock poisoned".to_owned())
    })
}

/// Serve PostgreSQL wire clients on an already-bound listener until the
/// listener errors. Each connection runs against `database` with trust auth.
pub async fn serve(database: SharedDatabase, listener: TcpListener) -> std::io::Result<()> {
    let auth_table_present = {
        let mut database = write_lock(&database).map_err(|err| {
            std::io::Error::other(err.to_string())
        })?;
        if !database
            .catalog()
            .tables()
            .any(|table| table.name == AUTH_TABLE)
        {
            database
                .create_table(auth_table_definition())
                .map_err(|err| std::io::Error::other(err.to_string()))?;
        }
        true
    };
    let _ = auth_table_present;
    let has_users = {
        let database = read_lock(&database).map_err(|err| {
            std::io::Error::other(err.to_string())
        })?;
        let mut transaction = database.begin().map_err(|err| std::io::Error::other(err.to_string()))?;
        let result = transaction
            .execute_sql(&database, "SELECT username FROM pgwire_auth")
            .map_err(|err| std::io::Error::other(err.to_string()));
        drop(transaction);
        !result?.rows.is_empty()
    };

    // Trust mode authenticates implicitly, so it must never be reachable
    // from a non-loopback interface.
    if !has_users && !listener.local_addr()?.ip().is_loopback() {
        return Err(std::io::Error::other(
            "trust authentication requires a loopback listener; provision wire users before binding a public interface",
        ));
    }

    let auth = has_users.then(|| {
        Arc::new(WireAuthSource {
            database: database.clone(),
        })
    });
    let identities = Arc::new(Mutex::new(HashMap::new()));
    let failure_delays = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let factory = Arc::new(HandlerFactory {
        handler: Arc::new(OmenDbHandler {
            database: database.clone(),
            transactions: Mutex::new(HashMap::new()),
            schema_probes: Mutex::new(HashMap::new()),
            identities: Arc::clone(&identities),
        }),
        auth: auth.map(|auth_source| {
            Arc::new(ScramComponents {
                auth_source,
                identities: Arc::clone(&identities),
                failure_delays: Arc::clone(&failure_delays),
            })
        }),
    });
    loop {
        let (socket, _peer) = listener.accept().await?;
        let factory = factory.clone();
        tokio::spawn(async move { process_socket(socket, None, factory).await });
    }
}

/// Bind a listener and spawn the accept loop on the current tokio runtime,
/// returning the bound local address (useful for tests picking port 0).
pub async fn spawn(
    database: SharedDatabase,
    bind_addr: std::net::SocketAddr,
) -> std::io::Result<(std::net::SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
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
        &[Value::Text(role.to_owned()), Value::Text(table_name.to_owned())],
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
    if !database.catalog().tables().any(|table| table.name == AUTH_TABLE) {
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
    pg_error(
        "28P01",
        "password authentication failed".to_owned(),
    )
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
    Trust(TrustStartup),
    Scram(
        SASLAuthStartupHandler<DefaultServerParameterProvider>,
        Arc<IdentityMap>,
        Arc<FailureDelays>,
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
            StartupMode::Trust(_) => unreachable!("trust mode never delays"),
            StartupMode::Scram(_, _, delays) => delays,
        }
    }

    fn identities(&self) -> Option<&Arc<IdentityMap>> {
        match self {
            StartupMode::Trust(_) => None,
            StartupMode::Scram(_, identities, _) => Some(identities),
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
        C: pgwire::api::ClientInfo
            + futures::Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
        Self: Sync,
    {
        let result = match self {
            StartupMode::Trust(trust) => trust.on_startup(client, message).await,
            StartupMode::Scram(sasl, _, _) => sasl.on_startup(client, message).await,
        };
        match result {
            Ok(()) => {
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
}

struct HandlerFactory {
    handler: Arc<OmenDbHandler>,
    auth: Option<Arc<ScramComponents>>,
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(
        &self,
    ) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(match self.auth.as_ref() {
            Some(components) => {
                let components = (**components).clone();
                StartupMode::Scram(
                    SASLAuthStartupHandler::new(Arc::new(
                        DefaultServerParameterProvider::default(),
                    ))
                    .with_scram(ScramAuth::new(components.auth_source)),
                    components.identities,
                    components.failure_delays,
                )
            }
            None => StartupMode::Trust(TrustStartup),
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

    fn get_parameter_types(
        &self,
        stmt: &ParsedStatement,
    ) -> PgWireResult<Vec<Type>> {
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

struct OmenDbHandler {
    database: SharedDatabase,
    transactions: Mutex<HashMap<std::net::SocketAddr, TransactionBlock>>,
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
    schema_probes: Mutex<ProbeCache>,
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
        crate::DbError::UniqueViolation { .. } => ("23505", error.to_string()),
        crate::DbError::ForeignKeyViolation { .. } | crate::DbError::CascadeDepthExceeded { .. } => {
            ("23503", error.to_string())
        }
        crate::DbError::SerializationConflict { .. } => ("40001", error.to_string()),
        crate::DbError::SqlUnsupported { .. } => ("0A000", format!("feature not supported: {error}")),
        _ => ("XX000", error.to_string()),
    };
    pg_error(code, message)
}

/// Encode one logical value. Native types let pgwire's `ToSqlText` honor the
/// negotiated column format (text or binary) instead of forcing text.
fn encode_value(
    encoder: &mut DataRowEncoder,
    value: &Value,
) -> PgWireResult<()> {
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
    fn resolved_parameter_types(&self, statement: &ParsedStatement) -> Vec<Type> {
        let count = ParsedStatement::placeholder_count(&statement.sql);
        let inferred = self
            .database
            .read()
            .ok()
            .and_then(|database| database.sql_parameter_types(&statement.sql).ok())
            .unwrap_or_default();
        (0..count)
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
            .collect()
    }
}

fn decode_parameter(
    portal: &pgwire::api::portal::Portal<ParsedStatement>,
    resolved: &[Type],
    index: usize,
) -> PgWireResult<Value> {
    let declared = resolved.get(index).cloned().unwrap_or(Type::TEXT);
    let Some(raw) = portal.parameters.get(index).and_then(|raw| raw.as_ref()) else {
        return Ok(Value::Null);
    };
    if portal.parameter_format.is_binary(index) {
        return Ok(match declared {
            Type::BOOL => Value::Bool(!raw.is_empty() && raw[0] != 0),
            Type::INT2 => match <[u8; 2]>::try_from(raw.as_ref()) {
                Ok(inner) => Value::I64(i64::from(i16::from_be_bytes(inner))),
                Err(_) => {
                    return Err(pg_error("22P02", "malformed int2 parameter".to_owned()))
                }
            },
            Type::INT4 => match <[u8; 4]>::try_from(raw.as_ref()) {
                Ok(inner) => Value::I64(i64::from(i32::from_be_bytes(inner))),
                Err(_) => {
                    return Err(pg_error("22P02", "malformed int4 parameter".to_owned()))
                }
            },
            Type::INT8 => match <[u8; 8]>::try_from(raw.as_ref()) {
                Ok(inner) => Value::I64(i64::from_be_bytes(inner)),
                Err(_) => {
                    return Err(pg_error("22P02", "malformed int8 parameter".to_owned()))
                }
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
                ))
            }
        },
        Type::BYTEA => Value::Bytes(text.into_bytes()),
        _ => Value::Text(text),
    })
}

fn decode_parameters(
    portal: &pgwire::api::portal::Portal<ParsedStatement>,
    resolved: &[Type],
) -> PgWireResult<Vec<Value>> {
    (0..portal.parameter_len())
        .map(|index| decode_parameter(portal, resolved, index))
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
    fn lock_transactions(
        &self,
    ) -> PgWireResult<
        std::sync::MutexGuard<'_, HashMap<std::net::SocketAddr, TransactionBlock>>,
    > {
        self.transactions.lock().map_err(|_| {
            pg_error("XX000", "transaction table poisoned".to_owned())
        })
    }

    /// Abort transaction blocks orphaned by disconnected clients. Cheap
    /// scan amortized over handled statements; stale blocks hold no locks.
    fn reap_idle_blocks(
        &self,
        blocks: &mut HashMap<std::net::SocketAddr, TransactionBlock>,
    ) {
        blocks.retain(|_, block| block.last_used.elapsed() < TX_IDLE_TIMEOUT);
    }

    fn execute_locked(
        database: &mut RelationalDatabase,
        sql: &str,
        params: &[Value],
    ) -> PgWireResult<crate::SqlResult> {
        database
            .execute_sql_with_params(sql, params)
            .map_err(map_db_error)
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
    ) -> PgWireResult<Response> {
        if matches!(
            sql.split_whitespace().next().unwrap_or_default(),
            "CREATE" | "DROP" | "ALTER"
        ) && let Ok(mut cache) = self.schema_probes.lock()
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
        self.enforce_grants(client_addr, sql)?;
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
                let transaction = read_lock(&self.database)?
                    .begin()
                    .map_err(map_db_error)?;
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
                let Some(block) = block else {
                    return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
                };
                if block.errored {
                    drop(block.transaction);
                    return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
                }
                // Publication is the serialized-writer boundary; the map
                // is free while this commit publishes.
                let mut database = write_lock(&self.database)?;
                block.transaction.commit(&mut database).map_err(map_db_error)?;
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
                let in_block = {
                    self.lock_transactions()?
                        .contains_key(&client_addr)
                };
                if !in_block {
                    return self.run_autocommit(sql, params, format);
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
                // Buffered into the transaction; publication happens at
                // COMMIT under the write lock, so execution only needs
                // shared access.
                let database = read_lock(&self.database)?;
                let outcome =
                    block.transaction.execute_sql_with_params(&database, sql, params);
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
    ) -> PgWireResult<()> {
        let Some(role) = self
            .identities
            .lock()
            .ok()
            .and_then(|identities| identities.get(&client_addr).cloned())
        else {
            return Ok(());
        };

        let database = read_lock(&self.database)?;
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

        // No rows at all means grants exist only for other roles; this
        // role still defaults to deny.
        let mut admin = false;
        let mut table_grants: HashMap<String, (bool, bool)> = HashMap::new();
        for row in &result.rows {
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
                format!(
                    "permission denied: role {role} lacks schema administration grant"
                ),
            ));
        }
        for table in &write_tables {
            match table_grants.get(table) {
                Some((_, true)) => {}
                _ => {
                    return Err(pg_error(
                        "42501",
                        format!("permission denied for table {table}: role {role} lacks write grant"),
                    ))
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
                        format!("permission denied for table {table}: role {role} lacks read grant"),
                    ))
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
    ) -> PgWireResult<Response> {
        if Self::is_row_returning(sql) && !Self::has_returning_clause(sql) {
            // Reads scale: snapshot query under shared access via an
            // autocommit transaction that aborts on completion.
            let database = read_lock(&self.database)?;
            let mut transaction = database.begin().map_err(map_db_error)?;
            let result = transaction.execute_sql_with_params(&database, sql, params);
            drop(transaction);
            return Ok(encode_response_with_format(result.map_err(map_db_error)?, format));
        }
        let mut database = write_lock(&self.database)?;
        Ok(encode_response_with_format(
            Self::execute_locked(&mut database, sql, params)?,
            format,
        ))
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
        let database = read_lock(&self.database)?;
        let mut transaction = database.begin().map_err(map_db_error)?;
        let result =
            transaction.execute_sql_with_params(&database, sql, &probe_params);
        drop(transaction);
        let result = result.map_err(map_db_error)?;
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
        let mut responses = Vec::new();
        // The embedded tier accepts one statement per call; simple-protocol
        // strings may carry several, so split conservatively on semicolons.
        for statement in query.split(';') {
            let statement = statement.trim();
            if statement.is_empty() {
                continue;
            }
            responses.push(self.run_statement(
                client_addr,
                statement,
                &[],
                &pgwire::api::portal::Format::UnifiedText,
            )?);
        }
        if responses.is_empty() {
            responses.push(Response::Execution(Tag::new("OK")));
        }
        Ok(responses)
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
        let resolved = self.resolved_parameter_types(&portal.statement.statement);
        let params = decode_parameters(portal, &resolved)?;
        self.run_statement(
            _client.socket_addr(),
            &portal.statement.statement.sql,
            &params,
            &portal.result_column_format,
        )
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        statement: &pgwire::api::stmt::StoredStatement<Self::Statement>,
    ) -> PgWireResult<pgwire::api::results::DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let parameter_types = self.resolved_parameter_types(&statement.statement);
        let resolved = self.resolved_parameter_types(&statement.statement);
        let fields = Arc::try_unwrap(
            self.describe_schema(
                &statement.statement.sql,
                &resolved,
                &pgwire::api::portal::Format::UnifiedBinary,
            )?,
        )
        .unwrap_or_default();
        Ok(pgwire::api::results::DescribeStatementResponse::new(
            parameter_types, fields,
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
        let resolved = self.resolved_parameter_types(&portal.statement.statement);
        let fields = Arc::try_unwrap(
            self.describe_schema(
                &portal.statement.statement.sql,
                &resolved,
                &portal.result_column_format,
            )?,
        )
        .unwrap_or_default();
        Ok(pgwire::api::results::DescribePortalResponse::new(fields))
    }
}
