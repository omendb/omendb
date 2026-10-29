//! Non-executing preparation and validation of retained wire bindings.

use super::*;
use pgwire::api::stmt::QueryParser;

impl OmenDbHandler {
    pub(super) fn check_boundary(
        &self,
        address: std::net::SocketAddr,
        control: Option<TransactionControl>,
    ) -> PgWireResult<()> {
        let expired = {
            let mut blocks = self.lock_transactions()?;
            self.reap_idle_blocks(&mut blocks)
        };
        drop(expired);
        if self
            .lock_transactions()?
            .get(&address)
            .is_some_and(|block| block.transaction.is_none())
            && !matches!(
                control,
                Some(TransactionControl::Commit | TransactionControl::Rollback)
            )
        {
            return Err(pg_error(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn check_transaction_catalog(
        &self,
        address: std::net::SocketAddr,
        database: &crate::RelationalDatabase,
    ) -> PgWireResult<()> {
        if let Some(transaction) = self
            .lock_transactions()?
            .get(&address)
            .and_then(|block| block.transaction.as_ref())
        {
            transaction
                .ensure_sql_catalog(database)
                .map_err(map_db_error)?;
        }
        Ok(())
    }

    pub(super) fn bind_statement(
        &self,
        address: std::net::SocketAddr,
        source: &str,
        declared: &[Option<Type>],
        control: &OperationControl,
    ) -> PgWireResult<ParsedStatement> {
        control.check().map_err(map_db_error)?;
        if let Some(bound) = crate::sql::binding::bind_control(source).map_err(map_db_error)? {
            let StatementEffect::Control(transaction_control) = bound.effect else {
                unreachable!("control binder returned an executable statement");
            };
            self.check_boundary(address, Some(transaction_control))?;
            if !declared.is_empty() {
                return Err(pg_error(
                    "08P01",
                    "transaction controls have no parameters".to_owned(),
                ));
            }
            return Ok(ParsedStatement {
                sql: source.to_owned(),
                bound: Arc::new(bound),
                parameter_types: Vec::new(),
            });
        }
        self.check_boundary(address, None)?;
        let hints = declared
            .iter()
            .map(|ty| {
                ty.as_ref()
                    .filter(|ty| **ty != Type::UNKNOWN)
                    .map(parameter_type)
                    .transpose()
            })
            .collect::<PgWireResult<Vec<_>>>()?;
        let database = read_lock_with_control(&self.database, control)?;
        self.check_transaction_catalog(address, &database)?;
        if declared.len() > crate::sql::parameter_count(source).map_err(map_db_error)? {
            return Err(pg_error(
                "08P01",
                "too many declared parameter types".to_owned(),
            ));
        }
        let bound = crate::sql::binding::bind(&database, source, &hints).map_err(map_db_error)?;
        let parameter_types = bound
            .parameter_types
            .iter()
            .enumerate()
            .map(|(index, inferred)| {
                declared
                    .get(index)
                    .cloned()
                    .flatten()
                    .filter(|ty| *ty != Type::UNKNOWN)
                    .or_else(|| inferred.map(column_type_to_pg))
                    .ok_or_else(|| {
                        map_db_error(crate::DbError::SqlIndeterminateParameter { index: index + 1 })
                    })
            })
            .collect::<PgWireResult<Vec<_>>>()?;
        let _authorization = self.authorize(address, &database, &bound.access, control)?;
        control.check().map_err(map_db_error)?;
        Ok(ParsedStatement {
            sql: source.to_owned(),
            bound: Arc::new(bound),
            parameter_types,
        })
    }

    pub(super) fn validate_statement(
        &self,
        address: std::net::SocketAddr,
        statement: &ParsedStatement,
        control: &OperationControl,
    ) -> PgWireResult<Option<crate::AuthorizationView>> {
        control.check().map_err(map_db_error)?;
        let transaction_control = match statement.bound.effect {
            StatementEffect::Control(control) => Some(control),
            _ => None,
        };
        self.check_boundary(address, transaction_control)?;
        // Cleanup owns no catalog dependencies and must always remain usable.
        if transaction_control.is_some() {
            return Ok(None);
        }
        let database = read_lock_with_control(&self.database, control)?;
        let authorization = self.validate_access(address, &database, statement, control)?;
        self.check_transaction_catalog(address, &database)?;
        Ok(Some(authorization))
    }

    // Execution calls this while holding the same catalog guard it uses for
    // the statement. Describe uses it before exposing the retained signature.
    pub(super) fn validate_access(
        &self,
        address: std::net::SocketAddr,
        database: &crate::RelationalDatabase,
        statement: &ParsedStatement,
        control: &OperationControl,
    ) -> PgWireResult<crate::AuthorizationView> {
        crate::sql::validate_bound(database, &statement.bound).map_err(map_db_error)?;
        self.authorize(address, database, &statement.bound.access, control)
    }
}

fn parameter_type(ty: &Type) -> PgWireResult<crate::ColumnType> {
    use crate::ColumnType;
    Ok(match *ty {
        Type::BOOL => ColumnType::Bool,
        Type::INT2 | Type::INT4 | Type::INT8 => ColumnType::I64,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => ColumnType::Text,
        Type::BYTEA => ColumnType::Bytes,
        Type::FLOAT4 | Type::FLOAT8 => ColumnType::Float64,
        Type::DATE => ColumnType::Date,
        Type::TIMESTAMP => ColumnType::Timestamp,
        Type::NUMERIC => ColumnType::Decimal,
        Type::UUID => ColumnType::Uuid,
        _ => {
            return Err(pg_error(
                "0A000",
                format!("unsupported parameter type {ty}"),
            ));
        }
    })
}

#[async_trait]
impl QueryParser for OmenDbHandler {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<ParsedStatement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (cancellation, _lease) = self.begin_query(client);
        let handler = self.clone();
        let address = client.socket_addr();
        let sql = sql.to_owned();
        let types = types.to_vec();
        self.query_workers
            .spawn_operation(cancellation.clone(), move || {
                let control = handler.operation_control(&cancellation);
                handler.bind_statement(address, &sql, &types, &control)
            })
            .await
            .map_err(|error| pg_error("XX000", format!("query worker failed: {error}")))?
    }

    fn get_parameter_types(&self, statement: &ParsedStatement) -> PgWireResult<Vec<Type>> {
        Ok(statement.parameter_types.clone())
    }

    fn get_result_schema(
        &self,
        statement: &ParsedStatement,
        format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        fields_from_columns(
            &statement.bound.columns,
            format.unwrap_or(&pgwire::api::portal::Format::UnifiedText),
        )
    }
}
