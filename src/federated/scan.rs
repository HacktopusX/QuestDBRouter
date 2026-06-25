use super::{merge_query_responses, merge_scalar_aggregate, query_all_shards, FederatedExecutor};
use crate::routing::AggKind;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

pub async fn execute_full_scan(executor: &FederatedExecutor, sql: &str) -> PgWireResult<Vec<Response>> {
    let max_rows = executor.state.config.routing.max_federated_rows;
    let shard_results = query_all_shards(executor, sql).await?;
    let merged = merge_query_responses(shard_results, max_rows)?;
    Ok(merged)
}

pub async fn execute_aggregate_scan(
    executor: &FederatedExecutor,
    sql: &str,
    agg_kind: AggKind,
) -> PgWireResult<Vec<Response>> {
    let shard_results = query_all_shards(executor, sql).await?;
    merge_scalar_aggregate(shard_results, agg_kind)
}
