//! Simple Query message boundaries and implicit transaction ownership.

use super::*;
use pgwire::api::query::SimpleQueryHandler;

struct SimpleQueryFailure {
    responses: Vec<Response>,
    error: PgWireError,
}

impl OmenDbHandler {
    fn begin_implicit(
        &self,
        address: std::net::SocketAddr,
        control: &OperationControl,
    ) -> PgWireResult<()> {
        if self.lock_transactions()?.contains_key(&address) {
            return Ok(());
        }
        let transaction = read_lock_with_control(&self.database, control)?
            .begin_with_control(control)
            .map_err(map_db_error)?;
        control.check().map_err(map_db_error)?;
        self.lock_transactions()?.insert(
            address,
            TransactionBlock {
                kind: TransactionKind::Implicit,
                transaction: Some(transaction),
                last_used: Instant::now(),
            },
        );
        Ok(())
    }

    /// Consume the implicit owner before publication, just as explicit COMMIT
    /// does. An error after a durable decision cannot recreate an aborted block.
    pub(super) fn commit_implicit(
        &self,
        address: std::net::SocketAddr,
        control: &OperationControl,
    ) -> PgWireResult<()> {
        let block = {
            let mut blocks = self.lock_transactions()?;
            if !blocks
                .get(&address)
                .is_some_and(|block| block.kind == TransactionKind::Implicit)
            {
                return Ok(());
            }
            blocks
                .remove(&address)
                .expect("implicit membership checked")
        };
        let mut transaction = block
            .transaction
            .ok_or_else(|| pg_error("25P02", "current transaction is aborted".to_owned()))?;
        transaction.set_operation_control(control);
        transaction.commit().map_err(map_db_error)?;
        Ok(())
    }

    /// A failed message removes its implicit boundary, but a regular block
    /// remains rollback-only until the client explicitly closes it.
    fn fail_simple_query(&self, address: std::net::SocketAddr) {
        let transaction = {
            let Ok(mut blocks) = self.transactions.lock() else {
                return;
            };
            if blocks
                .get(&address)
                .is_some_and(|block| block.kind == TransactionKind::Implicit)
            {
                blocks.remove(&address).and_then(|block| block.transaction)
            } else {
                blocks.get_mut(&address).and_then(|block| {
                    block.last_used = Instant::now();
                    block.transaction.take()
                })
            }
        };
        drop(transaction);
    }

    pub(super) fn run_simple_statement(
        &self,
        address: std::net::SocketAddr,
        source: &str,
        control: &OperationControl,
        end_implicit: bool,
    ) -> PgWireResult<Response> {
        let result = self
            .bind_statement(address, source, &[], control)
            .and_then(|statement| {
                self.run_bound_statement(
                    address,
                    &statement,
                    &[],
                    &pgwire::api::portal::Format::UnifiedText,
                    control,
                    end_implicit,
                )
            });
        if result.is_err() {
            self.fail_block(address);
        }
        result
    }

    fn run_simple_query(
        &self,
        address: std::net::SocketAddr,
        query: &str,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Response>, SimpleQueryFailure> {
        let mut responses = Vec::new();
        let result = (|| {
            let sources = crate::sql::statement_sources(query).map_err(map_db_error)?;
            // PostgreSQL parses the entire message before executing even an
            // earlier COMMIT. Semantic binding and authorization stay sequential.
            for source in &sources {
                crate::sql::parse_statement(source).map_err(map_db_error)?;
            }
            for (index, source) in sources.iter().enumerate() {
                let control = self.operation_control(cancellation);
                if sources.len() > 1 {
                    self.begin_implicit(address, &control)?;
                }
                responses.push(self.run_simple_statement(
                    address,
                    source,
                    &control,
                    index + 1 == sources.len(),
                )?);
            }
            Ok::<_, PgWireError>(())
        })();
        if let Err(error) = result {
            self.fail_simple_query(address);
            return Err(SimpleQueryFailure { responses, error });
        }
        if responses.is_empty() {
            responses.push(Response::EmptyQuery);
        }
        Ok(responses)
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
                            .await?;
                    }
                    Response::Error(error) => {
                        client
                            .feed(PgWireBackendMessage::ErrorResponse((*error).into()))
                            .await?;
                    }
                    _ => return Err(pg_error("XX000", "unexpected SQL response kind".to_owned())),
                }
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            // Worker/delivery failures must not leave a message-owned block
            // behind. Already-consumed commits have no boundary to abort.
            self.fail_simple_query(client.socket_addr());
        }
        self.sync_transaction_status(client)?;
        result?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        send_ready_for_query(client, client.transaction_status()).await
    }

    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let address = client.socket_addr();
        let (cancellation, _lease) = self.begin_query(client);
        let handler = self.clone();
        let query = query.to_owned();
        let result = self
            .query_workers
            .spawn_operation(cancellation.clone(), move || {
                handler.run_simple_query(address, &query, &cancellation)
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))?;
        Ok(result.unwrap_or_else(|failure| {
            // Keep worker failure accounting independent of wire framing.
            // Prefix completions survive even when their implicit work rolls back.
            let mut responses = failure.responses;
            responses.push(Response::Error(Box::new(failure.error.into())));
            responses
        }))
    }
}
