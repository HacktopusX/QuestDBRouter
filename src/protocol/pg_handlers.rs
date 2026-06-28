//! PG handlers that intercept QuestDB dialect SQL before datafusion-postgres parses it.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_postgres::{DfSessionService, Parser};
use datafusion_postgres::hooks::QueryHook;
use log::error;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::Response;
use pgwire::api::stmt::QueryParser;
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ConnectionManager, ErrorHandler, PgWireServerHandlers, Type,
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser as SqlParser;

use crate::routing::has_questdb_extension;

use super::pg_hook::RouterQueryHook;

/// Raw dialect SQL stored during extended-query parse for execute-phase routing.
#[derive(Default, Clone)]
pub struct DialectSqlStore(pub Option<String>);

struct QuestDbStartupHandler {
    connection_manager: Arc<ConnectionManager>,
}

#[async_trait]
impl NoopStartupHandler for QuestDbStartupHandler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(self.connection_manager.clone())
    }
}

struct LoggingErrorHandler;

impl ErrorHandler for LoggingErrorHandler {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        error!("Sending error: {error}");
    }
}

pub struct QuestDbHandlerFactory {
    session: Arc<DfSessionService>,
    dialect_parser: Arc<DialectAwareParser>,
    router_hook: Arc<RouterQueryHook>,
    cancel_handler: Arc<DefaultCancelHandler>,
    startup_handler: Arc<QuestDbStartupHandler>,
}

impl QuestDbHandlerFactory {
    pub fn new(
        session_context: Arc<datafusion::prelude::SessionContext>,
        hooks: Vec<Arc<dyn QueryHook>>,
        router_hook: Arc<RouterQueryHook>,
    ) -> Self {
        let session = Arc::new(DfSessionService::new_with_hooks(session_context, hooks));
        let dialect_parser = Arc::new(DialectAwareParser {
            inner: ExtendedQueryHandler::query_parser(session.as_ref()),
        });
        let connection_manager = Arc::new(ConnectionManager::new());
        Self {
            session,
            dialect_parser,
            router_hook,
            cancel_handler: Arc::new(DefaultCancelHandler::new(connection_manager.clone())),
            startup_handler: Arc::new(QuestDbStartupHandler {
                connection_manager,
            }),
        }
    }
}

impl PgWireServerHandlers for QuestDbHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(DialectSimpleHandler {
            inner: self.session.clone(),
            router_hook: self.router_hook.clone(),
        })
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(DialectExtendedHandler {
            inner: self.session.clone(),
            dialect_parser: self.dialect_parser.clone(),
        })
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup_handler.clone()
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(LoggingErrorHandler)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.cancel_handler.clone()
    }
}

struct DialectSimpleHandler {
    inner: Arc<DfSessionService>,
    router_hook: Arc<RouterQueryHook>,
}

#[async_trait]
impl SimpleQueryHandler for DialectSimpleHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo
            + ClientPortalStore
            + futures::Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
    {
        if has_questdb_extension(query) {
            if let Some(resp) = self
                .router_hook
                .handle_raw_sql(query, None, client)
                .await
            {
                return Ok(vec![resp?]);
            }
        }
        SimpleQueryHandler::do_query(self.inner.as_ref(), client, query).await
    }
}

struct DialectExtendedHandler {
    inner: Arc<DfSessionService>,
    dialect_parser: Arc<DialectAwareParser>,
}

#[async_trait]
impl ExtendedQueryHandler for DialectExtendedHandler {
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;
    type QueryParser = DialectAwareParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.dialect_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo
            + ClientPortalStore
            + futures::Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<
            Statement = <DfSessionService as ExtendedQueryHandler>::Statement,
        >,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
    {
        ExtendedQueryHandler::do_query(self.inner.as_ref(), client, portal, max_rows).await
    }
}

pub struct DialectAwareParser {
    inner: Arc<Parser>,
}

#[async_trait]
impl QueryParser for DialectAwareParser {
    type Statement = <Parser as QueryParser>::Statement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if has_questdb_extension(sql) {
            client
                .session_extensions()
                .insert(DialectSqlStore(Some(sql.to_string())));

            let dummy_stmt = SqlParser::parse_sql(&GenericDialect {}, "SELECT 1")
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?
                .into_iter()
                .next()
                .ok_or_else(|| PgWireError::ApiError("empty dialect placeholder".into()))?;

            let logical_plan = RouterQueryHook::dummy_plan();

            return Ok((sql.to_string(), Some((dummy_stmt, logical_plan))));
        }

        self.inner.parse_sql(client, sql, types).await
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        self.inner.get_parameter_types(stmt)
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<pgwire::api::results::FieldInfo>> {
        self.inner.get_result_schema(stmt, column_format)
    }
}
