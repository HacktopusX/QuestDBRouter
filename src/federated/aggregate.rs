#[cfg(not(feature = "federated"))]
use super::{merge_query_responses, query_all_shards};
use super::FederatedExecutor;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

/// Execute GROUP BY by fanning out identical SQL and merging results.
pub async fn execute_group_by(
    executor: &FederatedExecutor,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    #[cfg(feature = "federated")]
    {
        return super::join::execute_sql_federated(executor, sql).await;
    }

    #[cfg(not(feature = "federated"))]
    {
        let max_rows = executor.state.config.routing.max_federated_rows;
        let shard_results = query_all_shards(executor, sql).await?;
        merge_query_responses(shard_results, max_rows)
    }
}
