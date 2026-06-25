use datafusion::prelude::SessionContext;

use super::{fed_err, FederatedExecutor};
use crate::federated::catalog::{bare_table_name, extract_table_names};
use crate::federated::pg_types::{batches_to_pg_responses, schema_from_columns};
use crate::federated::provider::QuestDbShardTableProvider;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

pub async fn execute_join(executor: &FederatedExecutor, sql: &str) -> PgWireResult<Vec<Response>> {
    execute_sql_federated(executor, sql).await
}

pub async fn execute_sql_federated(
    executor: &FederatedExecutor,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let ctx = SessionContext::new();
    let tables = extract_table_names(sql);
    let registry = executor.state.table_registry.clone();
    let shard_key = executor.state.config.routing.shard_key.clone();

    for table in &tables {
        let bare = bare_table_name(table).to_string();
        let table_cfg = registry
            .get(&bare)
            .ok_or_else(|| fed_err(format!("table {bare} is not configured in routing.tables")))?;
        let schema = schema_from_columns(&table_cfg.columns)
            .map_err(|e| fed_err(e.to_string()))?;
        let provider = QuestDbShardTableProvider::new(
            bare.clone(),
            executor.clone(),
            registry.clone(),
            shard_key.clone(),
            schema,
        );
        ctx.register_table(&bare, std::sync::Arc::new(provider))
            .map_err(|e| fed_err(e.to_string()))?;
    }

    let df = ctx
        .sql(sql)
        .await
        .map_err(|e| fed_err(e.to_string()))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| fed_err(e.to_string()))?;

    batches_to_pg_responses(batches)
}
