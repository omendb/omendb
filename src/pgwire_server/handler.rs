//! The per-connection query handler: statement execution, transaction
//! blocks, grant enforcement, and simple/extended protocol dispatch.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{FieldInfo, Response, Tag};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};

use super::cancellation::{CancellationRegistry, QueryCancellationLease};
use super::encoding::{
    check_result_payload, column_type_to_pg, decode_parameters, encode_response_with_format,
    value_type,
};
use super::lifecycle::{QueryWorkers, read_lock_with_control, write_lock_with_control};
use super::shared::TX_IDLE_TIMEOUT;
use super::shared::{
    IdentityMap, ParsedStatement, PlaceholderParser, ProbeCache, TransactionBlock,
};
use super::{CancellationToken, OperationControl, SharedDatabase, Value, map_db_error, pg_error};

#[derive(Clone)]
pub(super) struct OmenDbHandler {
    pub(super) database: SharedDatabase,
    pub(super) transactions: Arc<Mutex<HashMap<std::net::SocketAddr, TransactionBlock>>>,
    pub(super) cancellations: Arc<CancellationRegistry>,
    /// Authenticated role per connection pid, recorded by the startup
    /// handler after a successful exchange and read per statement for
    /// grant enforcement.
    pub(super) identities: Arc<IdentityMap>,
    /// Describe-probe results keyed by (source text, declared parameter
    /// types). Stores the schema-independent column list so each prepare
    /// probes once instead of every describe; FieldInfo is rebuilt per
    /// call with the portal's negotiated format. Cleared wholesale on
    /// any DDL statement - DDL is rare and stale schemas are worse than
    /// re-probing.
    pub(super) schema_probes: Arc<Mutex<ProbeCache>>,
    pub(super) query_workers: Arc<QueryWorkers>,
    pub(super) statement_timeout: Option<Duration>,
    pub(super) max_result_bytes: Option<usize>,
    /// Statements whose execution (including commit publication) exceeds
    /// this duration are logged to stderr.
    pub(super) slow_statement_threshold: Option<Duration>,
}

impl OmenDbHandler {
    /// Resolve the wire type reported for each parameter: client-declared
    /// types win; otherwise statement-context inference from the SQL tier;
    /// otherwise text as the least-lossy fallback.
    fn resolved_parameter_types(
        &self,
        statement: &ParsedStatement,
        control: &OperationControl,
    ) -> PgWireResult<Vec<Type>> {
        let count = ParsedStatement::placeholder_count(&statement.sql);
        let database = read_lock_with_control(&self.database, control)?;
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
    pub(super) fn cleanup_connection(&self, client_addr: std::net::SocketAddr) {
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

    fn operation_control(&self, cancellation: &CancellationToken) -> OperationControl {
        let control = OperationControl::with_cancellation(cancellation.clone());
        match self
            .statement_timeout
            .and_then(|timeout| Instant::now().checked_add(timeout))
        {
            Some(deadline) => control.with_deadline(deadline),
            None => control,
        }
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
        control: &OperationControl,
    ) -> PgWireResult<Response> {
        let started = self.slow_statement_threshold.map(|_| Instant::now());
        let response = self.run_statement_inner(client_addr, sql, params, format, control);
        if let (Some(started), Some(threshold)) = (started, self.slow_statement_threshold) {
            let elapsed = started.elapsed();
            if elapsed >= threshold {
                let identity = self
                    .identities
                    .lock()
                    .ok()
                    .and_then(|identities| identities.get(&client_addr).cloned())
                    .unwrap_or_else(|| "trust".to_owned());
                eprintln!(
                    "omendbd: slow statement: {{\"duration_ms\":{:.3},\"user\":\"{}\",\"statement\":\"{}\"}}",
                    elapsed.as_secs_f64() * 1000.0,
                    identity,
                    Self::statement_log_line(sql),
                );
            }
        }
        response
    }

    /// One physical line per statement: collapse embedded newlines and
    /// bracket the text so multi-statement text stays machine-greppable.
    fn statement_log_line(sql: &str) -> String {
        sql.lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn run_statement_inner(
        &self,
        client_addr: std::net::SocketAddr,
        sql: &str,
        params: &[Value],
        format: &pgwire::api::portal::Format,
        control: &OperationControl,
    ) -> PgWireResult<Response> {
        control.check().map_err(map_db_error)?;
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
        self.enforce_grants(client_addr, sql, control)?;
        control.check().map_err(map_db_error)?;
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
                let transaction = read_lock_with_control(&self.database, control)?
                    .begin_with_control(control)
                    .map_err(map_db_error)?;
                control.check().map_err(map_db_error)?;
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
                block.transaction.set_operation_control(control);
                let _database = write_lock_with_control(&self.database, control)?;
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
                    return self.run_autocommit(sql, params, format, control);
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
                block.transaction.set_operation_control(control);
                // Buffered into the transaction; publication happens at
                // COMMIT under the write lock, so execution only needs
                // shared access.
                let database = read_lock_with_control(&self.database, control)?;
                let outcome = block
                    .transaction
                    .execute_sql_with_params(&database, sql, params);
                drop(database);
                let response = match outcome {
                    Ok(result) => {
                        encode_response_with_format(result, format, self.max_result_bytes)
                    }
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
        control: &OperationControl,
    ) -> PgWireResult<()> {
        let Some(role) = self
            .identities
            .lock()
            .ok()
            .and_then(|identities| identities.get(&client_addr).cloned())
        else {
            return Ok(());
        };

        let database = read_lock_with_control(&self.database, control)?;
        if !database
            .catalog()
            .tables()
            .any(|table| table.name == "pgwire_grants")
        {
            return Ok(());
        }
        let mut transaction = database.begin_with_control(control).map_err(map_db_error)?;
        let result = transaction.execute_sql_with_params(
            &database,
            "SELECT table_name, can_read, can_write FROM pgwire_grants WHERE role = $1",
            &[Value::Text(role.clone())],
        );
        drop(transaction);
        let result = result.map_err(map_db_error)?;
        control.check().map_err(map_db_error)?;

        // No rows at all means grants exist only for other roles; this
        // role still defaults to deny.
        let mut admin = false;
        let mut table_grants: HashMap<String, (bool, bool)> = HashMap::new();
        for row in &result.rows {
            control.check().map_err(map_db_error)?;
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
        control: &OperationControl,
    ) -> PgWireResult<Response> {
        if Self::is_schema_statement(sql) {
            // Schema changes are owned by the direct database method rather
            // than a relational transaction. Cancellation is therefore a
            // preflight check for this non-interruptible publication.
            control.check().map_err(map_db_error)?;
            let mut database = write_lock_with_control(&self.database, control)?;
            control.check().map_err(map_db_error)?;
            return encode_response_with_format(
                database
                    .execute_sql_with_params(sql, params)
                    .map_err(map_db_error)?,
                format,
                self.max_result_bytes,
            );
        }
        control.check().map_err(map_db_error)?;
        if Self::is_row_returning(sql) && !Self::has_returning_clause(sql) {
            // Reads scale: snapshot query under shared access via an
            // autocommit transaction that aborts on completion.
            let database = read_lock_with_control(&self.database, control)?;
            let mut transaction = database.begin_with_control(control).map_err(map_db_error)?;
            let result = transaction.execute_sql_with_params(&database, sql, params);
            drop(transaction);
            return encode_response_with_format(
                result.map_err(map_db_error)?,
                format,
                self.max_result_bytes,
            );
        }
        let mut database = write_lock_with_control(&self.database, control)?;
        let (result, _) = database
            .transaction_with_control(control, |database, transaction| {
                let result = transaction.execute_sql_with_params(database, sql, params)?;
                check_result_payload(&result, self.max_result_bytes)?;
                Ok(result)
            })
            .map_err(map_db_error)?;
        encode_response_with_format(result, format, self.max_result_bytes)
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
            "SELECT" | "WITH" | "TABLE" | "VALUES" | "SHOW" | "EXPLAIN"
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
        control: &OperationControl,
    ) -> PgWireResult<Arc<Vec<FieldInfo>>> {
        control.check().map_err(map_db_error)?;
        if !Self::is_row_returning(sql) && !Self::has_returning_clause(sql) {
            return Ok(Arc::new(Vec::new()));
        }
        let returning_columns = {
            let database = read_lock_with_control(&self.database, control)?;
            crate::sql::describe_returning_columns(&database, sql).map_err(map_db_error)?
        };
        if let Some(columns) = returning_columns {
            return Ok(Arc::new(
                columns
                    .into_iter()
                    .enumerate()
                    .map(|(position, (name, column_type))| {
                        FieldInfo::new(
                            name,
                            None,
                            None,
                            column_type_to_pg(column_type),
                            format.format_for(position),
                        )
                    })
                    .collect(),
            ));
        }
        let probe_params: Vec<Value> = resolved
            .iter()
            .map(|declared| match declared {
                &Type::BOOL => Value::Bool(false),
                &Type::INT2 | &Type::INT4 | &Type::INT8 => Value::I64(0),
                &Type::BYTEA => Value::Bytes(Vec::new()),
                &Type::FLOAT4 | &Type::FLOAT8 => Value::Float64(crate::sql_types::F64::new(0.0)),
                &Type::DATE => Value::Date(crate::sql_types::DateValue(0)),
                &Type::TIMESTAMP => Value::Timestamp(crate::sql_types::TimestampValue(0)),
                &Type::NUMERIC => {
                    Value::Decimal(crate::sql_types::DecimalValue::new(0, 0).expect("zero"))
                }
                &Type::UUID => Value::Uuid(crate::sql_types::UuidValue([0; 16])),
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
        let database = read_lock_with_control(&self.database, control)?;
        let mut transaction = database.begin_with_control(control).map_err(map_db_error)?;
        let result = transaction.execute_sql_with_params(&database, sql, &probe_params);
        drop(transaction);
        let result = result.map_err(map_db_error)?;
        control.check().map_err(map_db_error)?;
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
                    let control = handler.operation_control(&cancellation);
                    responses.push(handler.run_statement(
                        client_addr,
                        statement,
                        &[],
                        &pgwire::api::portal::Format::UnifiedText,
                        &control,
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
                let control = handler.operation_control(&cancellation);
                let resolved = handler.resolved_parameter_types(&statement, &control)?;
                let params = decode_parameters(&parameters, &parameter_format, &resolved)?;
                handler.run_statement(client_addr, &statement.sql, &params, &format, &control)
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
                let control = handler.operation_control(&cancellation);
                let parameter_types = handler.resolved_parameter_types(&statement, &control)?;
                let fields = handler.describe_schema(
                    &statement.sql,
                    &parameter_types,
                    &pgwire::api::portal::Format::UnifiedBinary,
                    &control,
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
                let control = handler.operation_control(&cancellation);
                let resolved = handler.resolved_parameter_types(&statement, &control)?;
                handler.describe_schema(&statement.sql, &resolved, &format, &control)
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))??;
        let fields = Arc::try_unwrap(fields).unwrap_or_default();
        Ok(pgwire::api::results::DescribePortalResponse::new(fields))
    }
}
