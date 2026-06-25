use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DFSchema, ParamValues, ScalarValue};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::hooks::{HookClient, QueryHook};
use pgwire::api::results::Response;
use pgwire::api::ClientInfo;
use pgwire::error::PgWireResult;

use crate::app::{AppState, ResolvedRoute};
use crate::federated::catalog::TableCatalog;
use crate::metrics;
use crate::routing::{classify_sql, RoutePlan, SqlClassify, SqlRouteError};

use super::pg_backend::{
    inline_sql_params, query_shard_pool, read_only_err, relay_passthrough, wire_err,
};

pub struct RouterQueryHook {
    state: AppState,
    catalog: Arc<TableCatalog>,
}

impl RouterQueryHook {
    pub fn new(state: AppState, catalog: Arc<TableCatalog>) -> Self {
        Self { state, catalog }
    }

    fn ensure_datafusion_tables(&self, sql: &str) -> Option<pgwire::error::PgWireError> {
        self.catalog
            .ensure_tables_in_sql(sql)
            .err()
            .map(|e| wire_err(e.to_string()))
    }

    /// Classify SQL for the PG hook path.
    ///
    /// Only single-shard keyed reads and session passthrough are intercepted.
    /// Everything else (federated scans, `information_schema`, `pg_catalog`,
    /// ORDER BY/LIMIT, CTEs, parse edge cases) is delegated to DataFusion.
    fn try_resolve_hook_route(
        &self,
        sql: &str,
        params: Option<&[String]>,
    ) -> Option<Result<ResolvedRoute, SqlRouteError>> {
        let routing = &self.state.config.routing;
        let classified = classify_sql(
            sql,
            &routing.shard_key,
            &self.state.table_registry,
            // DataFusion applies ORDER BY / LIMIT after federated merge.
            true,
        );

        match classified {
            Ok(SqlClassify::Passthrough) => Some(Ok(ResolvedRoute::Passthrough)),
            Ok(SqlClassify::Routable(plan)) => match plan {
                RoutePlan::SingleShard {
                    table,
                    shard_key,
                    shard_key_param,
                } => {
                    let key = if let Some(idx) = shard_key_param {
                        params
                            .and_then(|p| p.get(idx))
                            .cloned()
                            .unwrap_or_else(|| table.clone())
                    } else {
                        shard_key.unwrap_or_else(|| table.clone())
                    };
                    Some(Ok(ResolvedRoute::Single(self.state.route_key(&key))))
                }
                _ => None,
            },
            Err(SqlRouteError::Unsupported(msg)) if msg.contains("only read queries") => {
                Some(Err(SqlRouteError::Unsupported(msg)))
            }
            Err(_) => None,
        }
    }

    fn is_write_statement(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Insert(_)
                | Statement::Update { .. }
                | Statement::Delete(_)
                | Statement::CreateTable { .. }
                | Statement::CreateView { .. }
                | Statement::Drop { .. }
                | Statement::AlterTable { .. }
                | Statement::Truncate { .. }
                | Statement::Copy { .. }
        )
    }

    fn dummy_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })
    }

    fn params_to_strings(params: &ParamValues) -> Vec<String> {
        match params {
            ParamValues::List(list) => list
                .iter()
                .map(|v| scalar_to_string(&v.value))
                .collect(),
            ParamValues::Map(map) => {
                let mut pairs: Vec<_> = map.iter().collect();
                pairs.sort_by(|a, b| a.0.cmp(b.0));
                pairs
                    .into_iter()
                    .map(|(_, v)| scalar_to_string(&v.value))
                    .collect()
            }
        }
    }

    async fn handle_routed_sql(
        &self,
        sql: &str,
        params: Option<&[String]>,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let resolved = match self.try_resolve_hook_route(sql, params) {
            None => return None,
            Some(Err(SqlRouteError::Unsupported(msg))) if msg.contains("only read queries") => {
                return Some(Err(read_only_err()));
            }
            Some(Err(e)) => return Some(Err(wire_err(e.to_string()))),
            Some(Ok(route)) => route,
        };

        match resolved {
            ResolvedRoute::Passthrough => {
                Some(relay_passthrough(&self.state, client, sql).await)
            }
            ResolvedRoute::Single(shard) => {
                metrics::record_request("read", Some(shard.id));
                let backend_sql = match params {
                    Some(ps) if !ps.is_empty() => inline_sql_params(sql, ps),
                    _ => sql.to_string(),
                };
                let responses = query_shard_pool(&self.state.pg_pool, shard.id, &backend_sql).await;
                Some(responses.and_then(|r| crate::protocol::pg_backend::first_response(r)))
            }
            ResolvedRoute::Federated(_) => None,
        }
    }
}

fn scalar_to_string(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
        other => other.to_string(),
    }
}

#[async_trait]
impl QueryHook for RouterQueryHook {
    fn handle_simple_query<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        statement: &'life1 Statement,
        _session_context: &'life2 SessionContext,
        client: &'life3 mut dyn HookClient,
    ) -> Pin<Box<dyn std::future::Future<Output = Option<PgWireResult<Response>>> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
    {
        Box::pin(async move {
            if Self::is_write_statement(statement) {
                return Some(Err(read_only_err()));
            }

            let sql = statement.to_string();
            if let Some(err) = self.ensure_datafusion_tables(&sql) {
                return Some(Err(err));
            }
            self.handle_routed_sql(&sql, None, client).await
        })
    }

    fn handle_extended_parse_query<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        statement: &'life1 Statement,
        _session_context: &'life2 SessionContext,
        _client: &'life3 (dyn ClientInfo + Send + Sync),
    ) -> Pin<Box<dyn std::future::Future<Output = Option<PgWireResult<LogicalPlan>>> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
    {
        Box::pin(async move {
            if Self::is_write_statement(statement) {
                return None;
            }

            let sql = statement.to_string();
            if let Some(err) = self.ensure_datafusion_tables(&sql) {
                return Some(Err(err));
            }
            match self.try_resolve_hook_route(&sql, None) {
                None => None,
                Some(Ok(ResolvedRoute::Passthrough | ResolvedRoute::Single(_))) => {
                    Some(Ok(Self::dummy_plan()))
                }
                Some(Ok(ResolvedRoute::Federated(_))) => None,
                Some(Err(SqlRouteError::Unsupported(msg))) if msg.contains("only read queries") => {
                    Some(Err(read_only_err()))
                }
                Some(Err(e)) => Some(Err(wire_err(e.to_string()))),
            }
        })
    }

    fn handle_extended_query<'life0, 'life1, 'life2, 'life3, 'life4, 'life5, 'async_trait>(
        &'life0 self,
        statement: &'life1 Statement,
        _logical_plan: &'life2 LogicalPlan,
        params: &'life3 ParamValues,
        _session_context: &'life4 SessionContext,
        client: &'life5 mut dyn HookClient,
    ) -> Pin<Box<dyn std::future::Future<Output = Option<PgWireResult<Response>>> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        'life4: 'async_trait,
        'life5: 'async_trait,
    {
        Box::pin(async move {
            if Self::is_write_statement(statement) {
                return Some(Err(read_only_err()));
            }

            let sql = statement.to_string();
            let param_strings = Self::params_to_strings(params);
            if let Some(err) = self.ensure_datafusion_tables(&sql) {
                return Some(Err(err));
            }
            self.handle_routed_sql(&sql, Some(&param_strings), client)
                .await
        })
    }
}
