//! The per-connection query handler: statement execution, transaction
//! blocks, grant enforcement, and simple/extended protocol dispatch.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(test)]
#[path = "handler_tests.rs"]
mod tests;

use crate::sql::TransactionControl;
use crate::sql::binding::{StatementAccess, StatementEffect};

mod prepare;
use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgwire::api::ClientPortalStore;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{FieldInfo, Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::response::TransactionStatus;

use super::cancellation::{CancellationRegistry, QueryCancellationLease};
use super::encoding::{
    check_result_payload, column_type_to_pg, decode_parameters, encode_response_with_format,
    fields_from_columns,
};
use super::lifecycle::{QueryWorkers, read_lock_with_control, write_lock_with_control};
use super::shared::TX_IDLE_TIMEOUT;
use super::shared::{IdentityMap, ParsedStatement, SessionAuthority, TransactionBlock};
use super::{CancellationToken, OperationControl, SharedDatabase, Value, map_db_error, pg_error};

#[derive(Clone)]
pub(super) struct OmenDbHandler {
    pub(super) database: SharedDatabase,
    pub(super) transactions: Arc<Mutex<HashMap<std::net::SocketAddr, TransactionBlock>>>,
    pub(super) cancellations: Arc<CancellationRegistry>,
    /// Explicit authority per connection, installed only after successful
    /// startup. Absence or registry poisoning never confers bootstrap access.
    pub(super) identities: Arc<IdentityMap>,
    pub(super) query_workers: Arc<QueryWorkers>,
    pub(super) statement_timeout: Option<Duration>,
    pub(super) max_result_bytes: Option<usize>,
    /// Statements whose execution (including commit publication) exceeds
    /// this duration are logged to stderr.
    pub(super) slow_statement_threshold: Option<Duration>,
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

    fn fail_block(&self, client_addr: std::net::SocketAddr) {
        let transaction = {
            let Ok(mut blocks) = self.transactions.lock() else {
                return;
            };
            let Some(block) = blocks.get_mut(&client_addr) else {
                return;
            };
            block.last_used = Instant::now();
            block.transaction.take()
        };
        drop(transaction);
    }

    /// The engine map, not the previous protocol status, owns transaction state.
    /// In particular, commit consumes a block even when its outcome is an error.
    fn sync_transaction_status<C: ClientInfo>(&self, client: &mut C) -> PgWireResult<()> {
        let blocks = self.lock_transactions()?;
        let status = match blocks.get(&client.socket_addr()) {
            Some(block) if block.transaction.is_none() => TransactionStatus::Error,
            Some(_) => TransactionStatus::Transaction,
            None => TransactionStatus::Idle,
        };
        client.set_transaction_status(status);
        Ok(())
    }

    fn lock_transactions(
        &self,
    ) -> PgWireResult<std::sync::MutexGuard<'_, HashMap<std::net::SocketAddr, TransactionBlock>>>
    {
        self.transactions
            .lock()
            .map_err(|_| pg_error("XX000", "transaction table poisoned".to_owned()))
    }

    /// Release idle engine transactions, never the session's rollback boundary.
    /// Return owned resources so their destruction happens outside the map lock.
    fn reap_idle_blocks(
        &self,
        blocks: &mut HashMap<std::net::SocketAddr, TransactionBlock>,
    ) -> Vec<crate::RelationalDatabaseTransaction> {
        blocks
            .values_mut()
            .filter(|block| block.last_used.elapsed() >= TX_IDLE_TIMEOUT)
            .filter_map(|block| block.transaction.take())
            .collect()
    }

    /// Route one wire statement: transaction control, in-block execution,
    /// or autocommit. `format` is the negotiated result-column format; the
    /// statement executes exactly once.
    fn run_statement(
        &self,
        address: std::net::SocketAddr,
        sql: &str,
        params: &[Value],
        format: &pgwire::api::portal::Format,
        control: &OperationControl,
    ) -> PgWireResult<Response> {
        let hints = crate::sql::parameter_hints(params)
            .into_iter()
            .map(|ty| ty.map(column_type_to_pg))
            .collect::<Vec<_>>();
        let result = self
            .bind_statement(address, sql, &hints, control)
            .and_then(|statement| {
                self.run_bound_statement(address, &statement, params, format, control)
            });
        if result.is_err() {
            self.fail_block(address);
        }
        result
    }

    fn run_bound_statement(
        &self,
        client_addr: std::net::SocketAddr,
        statement: &ParsedStatement,
        params: &[Value],
        format: &pgwire::api::portal::Format,
        control: &OperationControl,
    ) -> PgWireResult<Response> {
        let started = self.slow_statement_threshold.map(|_| Instant::now());
        let response = self.run_statement_inner(client_addr, statement, params, format, control);
        if response.is_err() {
            self.fail_block(client_addr);
        }
        if let (Some(started), Some(threshold)) = (started, self.slow_statement_threshold) {
            let elapsed = started.elapsed();
            if elapsed >= threshold {
                let identity = self
                    .identities
                    .lock()
                    .ok()
                    .and_then(|identities| identities.get(&client_addr).cloned())
                    .map(|authority| match authority {
                        SessionAuthority::Bootstrap => "bootstrap".to_owned(),
                        SessionAuthority::Principal { login, .. } => login,
                    })
                    .unwrap_or_else(|| "unauthenticated".to_owned());
                eprintln!(
                    "omendbd: slow statement: {{\"duration_ms\":{:.3},\"user\":\"{}\",\"statement\":\"{}\"}}",
                    elapsed.as_secs_f64() * 1000.0,
                    identity,
                    Self::statement_log_line(&statement.sql),
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
        statement: &ParsedStatement,
        params: &[Value],
        format: &pgwire::api::portal::Format,
        control: &OperationControl,
    ) -> PgWireResult<Response> {
        control.check().map_err(map_db_error)?;
        let transaction_control = match statement.bound.effect {
            StatementEffect::Control(control) => Some(control),
            _ => None,
        };
        self.check_boundary(client_addr, transaction_control)?;
        match transaction_control {
            Some(TransactionControl::Begin) => {
                let mut blocks = self.lock_transactions()?;
                if let Some(block) = blocks.get_mut(&client_addr) {
                    block.last_used = Instant::now();
                    if block.transaction.is_none() {
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
                        transaction: Some(transaction),
                        last_used: Instant::now(),
                    },
                );
                Ok(Response::TransactionStart(Tag::new("BEGIN")))
            }
            Some(TransactionControl::Commit) => {
                let block = {
                    let mut blocks = self.lock_transactions()?;
                    blocks.remove(&client_addr)
                };
                let Some(block) = block else {
                    return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
                };
                let Some(mut transaction) = block.transaction else {
                    return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
                };
                // Publication is the serialized-writer boundary; the map
                // is free while this commit publishes.
                //
                // No outer write guard: the engine's group-commit lane
                // already serializes publication and stages concurrently
                // under its own prepare mutex, and schema
                // changes are fenced by the catalog-marker range every
                // transaction registers at begin. Holding the exclusive
                // guard across commit() serializes staging too, collapsing
                // every wave to a singleton (measured members=1.00 across
                // 3008 waves) and blocking all readers for the wave
                // duration; the lane is the only serialization needed.
                transaction.set_operation_control(control);
                transaction.commit().map_err(map_db_error)?;
                Ok(Response::TransactionEnd(Tag::new("COMMIT")))
            }
            Some(TransactionControl::Rollback) => {
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
                    return self.run_autocommit(client_addr, statement, params, format, control);
                }

                // Take this connection's block out so other connections
                // proceed while its statements execute.
                let mut block = {
                    self.lock_transactions()?
                        .remove(&client_addr)
                        .expect("membership checked above")
                };
                let Some(transaction) = block.transaction.as_mut() else {
                    self.lock_transactions()?.insert(client_addr, block);
                    return Err(pg_error(
                        "25P02",
                        "current transaction is aborted, commands ignored until end of transaction block".to_owned(),
                    ));
                };
                block.last_used = Instant::now();
                transaction.set_operation_control(control);
                // Buffered into the transaction; publication happens at
                // COMMIT under the write lock, so execution only needs
                // shared access.
                let response = (|| {
                    let database = read_lock_with_control(&self.database, control)?;
                    let _authorization =
                        self.validate_access(client_addr, &database, statement, control)?;
                    let result = transaction
                        .execute_bound_sql(&database, &statement.bound, params)
                        .map_err(map_db_error)?;
                    drop(database);
                    encode_response_with_format(result, format, self.max_result_bytes)
                })();
                if response.is_err() {
                    block.transaction = None;
                }
                self.lock_transactions()?.insert(client_addr, block);
                response
            }
        }
    }

    /// Read current committed native authorization independently of any open
    /// data transaction. Keep the owned view alive for the authorized operation;
    /// execution uses the binder's TableIds, never a second name lookup.
    fn authorize(
        &self,
        client_addr: std::net::SocketAddr,
        database: &crate::RelationalDatabase,
        access: &StatementAccess,
        control: &OperationControl,
    ) -> PgWireResult<crate::AuthorizationView> {
        let authority = self
            .identities
            .lock()
            .map_err(|_| pg_error("XX000", "identity registry poisoned".to_owned()))?
            .get(&client_addr)
            .cloned()
            .ok_or_else(|| pg_error("42501", "session has no authorization identity".to_owned()))?;
        let authorization = match authority {
            SessionAuthority::Bootstrap => database.bootstrap_authorization(),
            SessionAuthority::Principal { id, .. } => database.authorization(id),
        }
        .map_err(map_db_error)?;
        control.check().map_err(map_db_error)?;
        if access.requires_admin {
            authorization
                .require_administration(database)
                .map_err(map_db_error)?;
        }
        for table in &access.write {
            authorization
                .require_write(database, *table)
                .map_err(map_db_error)?;
        }
        for table in &access.read {
            authorization
                .require_read(database, *table)
                .map_err(map_db_error)?;
        }
        control.check().map_err(map_db_error)?;
        Ok(authorization)
    }

    /// Autocommit path: no transaction block exists for this connection.
    fn run_autocommit(
        &self,
        client_addr: std::net::SocketAddr,
        statement: &ParsedStatement,
        params: &[Value],
        format: &pgwire::api::portal::Format,
        control: &OperationControl,
    ) -> PgWireResult<Response> {
        control.check().map_err(map_db_error)?;
        match statement.bound.effect {
            StatementEffect::Schema => {
                let mut database = write_lock_with_control(&self.database, control)?;
                let _authorization =
                    self.validate_access(client_addr, &database, statement, control)?;
                encode_response_with_format(
                    crate::sql::execute_bound(&mut database, &statement.bound, params)
                        .map_err(map_db_error)?,
                    format,
                    self.max_result_bytes,
                )
            }
            StatementEffect::Read => {
                let database = read_lock_with_control(&self.database, control)?;
                let _authorization =
                    self.validate_access(client_addr, &database, statement, control)?;
                let mut transaction = database.begin_with_control(control).map_err(map_db_error)?;
                let result = transaction.execute_bound_sql(&database, &statement.bound, params);
                drop(transaction);
                encode_response_with_format(
                    result.map_err(map_db_error)?,
                    format,
                    self.max_result_bytes,
                )
            }
            StatementEffect::Write => {
                let mut database = write_lock_with_control(&self.database, control)?;
                let _authorization =
                    self.validate_access(client_addr, &database, statement, control)?;
                let (result, _) = database
                    .transaction_with_control(control, |database, transaction| {
                        let result =
                            transaction.execute_bound_sql(database, &statement.bound, params)?;
                        check_result_payload(&result, self.max_result_bytes)?;
                        Ok(result)
                    })
                    .map_err(map_db_error)?;
                encode_response_with_format(result, format, self.max_result_bytes)
            }
            StatementEffect::Control(_) => unreachable!("controls are routed by the session"),
        }
    }
}

impl pgwire::api::ErrorHandler for OmenDbHandler {
    fn on_error<C>(&self, client: &C, _error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        // Parse, Bind and Describe failures do not pass through execution.
        self.fail_block(client.socket_addr());
    }
}

#[async_trait]
impl SimpleQueryHandler for OmenDbHandler {
    async fn on_query<C>(
        &self,
        client: &mut C,
        query: pgwire::messages::simplequery::Query,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        use pgwire::api::PgWireConnectionState;
        use pgwire::api::query::{
            send_execution_response, send_query_response, send_ready_for_query,
        };
        use pgwire::messages::response::EmptyQueryResponse;

        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        client.set_state(PgWireConnectionState::QueryInProgress);
        let result = async {
            for response in self.do_query(client, &query.query).await? {
                match response {
                    Response::Query(rows) => send_query_response(client, rows, true).await?,
                    Response::Execution(tag)
                    | Response::TransactionStart(tag)
                    | Response::TransactionEnd(tag) => {
                        send_execution_response(client, tag).await?;
                    }
                    Response::EmptyQuery => {
                        client
                            .feed(PgWireBackendMessage::EmptyQueryResponse(
                                EmptyQueryResponse::new(),
                            ))
                            .await?
                    }
                    _ => return Err(pg_error("XX000", "unexpected SQL response kind".to_owned())),
                }
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            self.fail_block(client.socket_addr());
        }
        // Unlike pgwire's default responder, never infer final state from
        // command tags or the status captured before this batch executed.
        self.sync_transaction_status(client)?;
        result?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        send_ready_for_query(client, client.transaction_status()).await
    }

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
                for statement in crate::sql::statement_sources(&query).map_err(map_db_error)? {
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
                    responses.push(Response::EmptyQuery);
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
    type QueryParser = Self;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(self.clone())
    }

    async fn on_parse<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::extendedquery::Parse,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if message
            .type_oids
            .iter()
            .any(|oid| *oid != 0 && Type::from_oid(*oid).is_none())
        {
            return Err(pg_error(
                "0A000",
                "unsupported parameter type OID".to_owned(),
            ));
        }
        let statement =
            pgwire::api::stmt::StoredStatement::parse(client, &message, self.query_parser())
                .await?;
        client.portal_store().put_statement(Arc::new(statement));
        client
            .send(PgWireBackendMessage::ParseComplete(
                pgwire::messages::extendedquery::ParseComplete::new(),
            ))
            .await?;
        Ok(())
    }

    async fn on_bind<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::extendedquery::Bind,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message
            .statement_name
            .as_deref()
            .unwrap_or(pgwire::api::DEFAULT_NAME);
        let statement = client
            .portal_store()
            .get_statement(name)
            .ok_or_else(|| PgWireError::StatementNotFound(name.to_owned()))?;
        let parameters = statement.statement.parameter_types.len();
        let columns = statement.statement.bound.columns.len();
        let valid_codes = |codes: &[i16], count| {
            (codes.len() <= 1 || codes.len() == count)
                && codes.iter().all(|code| matches!(code, 0 | 1))
        };
        if message.parameters.len() != parameters
            || !valid_codes(&message.parameter_format_codes, parameters)
            || !valid_codes(&message.result_column_format_codes, columns)
        {
            return Err(pg_error(
                "08P01",
                "Bind parameter or format count does not match the prepared signature".to_owned(),
            ));
        }
        let portal = pgwire::api::portal::Portal::try_new(&message, statement)?;
        client.portal_store().put_portal(Arc::new(portal));
        client
            .send(PgWireBackendMessage::BindComplete(
                pgwire::messages::extendedquery::BindComplete::new(),
            ))
            .await?;
        Ok(())
    }

    async fn on_sync<C>(
        &self,
        client: &mut C,
        _message: pgwire::messages::extendedquery::Sync,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.portal_store().rm_portal(pgwire::api::DEFAULT_NAME);
        self.sync_transaction_status(client)?;
        pgwire::api::query::send_ready_for_query(client, client.transaction_status()).await
    }

    async fn on_execute<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::extendedquery::Execute,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let result = async {
            let name = message.name.as_deref().unwrap_or(pgwire::api::DEFAULT_NAME);
            let portal = client
                .portal_store()
                .get_portal(name)
                .ok_or_else(|| PgWireError::PortalNotFound(name.to_owned()))?;
            let transaction_control = match portal.statement.statement.bound.effect {
                StatementEffect::Control(control) => Some(control),
                _ => None,
            };
            self.check_boundary(client.socket_addr(), transaction_control)?;
            if message.max_rows < 0 {
                return Err(pg_error(
                    "08P01",
                    "Execute row count cannot be negative".to_owned(),
                ));
            }
            if message.max_rows > 0 {
                return Err(pg_error(
                    "0A000",
                    "incremental portal fetching is not supported".to_owned(),
                ));
            }
            // pgwire skips do_query for completed/suspended portals. The
            // retained binding must still be checked on every Execute.
            let initial = matches!(
                &*portal.state.lock().await,
                pgwire::api::portal::PortalExecutionState::Initial
            );
            let _authorization = if !initial {
                let (cancellation, _lease) = self.begin_query(client);
                let handler = self.clone();
                let address = client.socket_addr();
                self.query_workers
                    .spawn_operation(cancellation.clone(), move || {
                        let control = handler.operation_control(&cancellation);
                        handler.validate_statement(address, &portal.statement.statement, &control)
                    })
                    .await
                    .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))??
            } else {
                None
            };
            self._on_execute(client, message).await
        }
        .await;
        if result.is_err() {
            self.fail_block(client.socket_addr());
        }
        self.sync_transaction_status(client)?;
        result
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
                control.check().map_err(map_db_error)?;
                handler.check_boundary(
                    client_addr,
                    match statement.bound.effect {
                        StatementEffect::Control(control) => Some(control),
                        _ => None,
                    },
                )?;
                let params =
                    decode_parameters(&parameters, &parameter_format, &statement.parameter_types)?;
                handler.run_bound_statement(client_addr, &statement, &params, &format, &control)
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))?
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        statement: &pgwire::api::stmt::StoredStatement<Self::Statement>,
    ) -> PgWireResult<pgwire::api::results::DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (cancellation, _lease) = self.begin_query(client);
        let handler = self.clone();
        let statement = statement.statement.clone();
        let address = client.socket_addr();
        self.query_workers
            .spawn_operation(cancellation.clone(), move || {
                let control = handler.operation_control(&cancellation);
                let _authorization = handler.validate_statement(address, &statement, &control)?;
                let fields = fields_from_columns(
                    &statement.bound.columns,
                    &pgwire::api::portal::Format::UnifiedText,
                )?;
                Ok(pgwire::api::results::DescribeStatementResponse::new(
                    statement.parameter_types.clone(),
                    fields,
                ))
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))?
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
    ) -> PgWireResult<pgwire::api::results::DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (cancellation, _lease) = self.begin_query(client);
        let handler = self.clone();
        let statement = portal.statement.statement.clone();
        let format = portal.result_column_format.clone();
        let address = client.socket_addr();
        self.query_workers
            .spawn_operation(cancellation.clone(), move || {
                let control = handler.operation_control(&cancellation);
                let _authorization = handler.validate_statement(address, &statement, &control)?;
                Ok(pgwire::api::results::DescribePortalResponse::new(
                    fields_from_columns(&statement.bound.columns, &format)?,
                ))
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))?
    }
}
