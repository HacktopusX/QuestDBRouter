use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;

use crate::federated::arrow::{extract_query_rows, query_shard};
use crate::federated::pg_types::{decode_row_cells, rows_to_typed_batch_for_schema};
use crate::federated::FederatedExecutor;
use crate::routing::TableRegistry;

/// DataFusion table provider backed by QuestDB shards.
pub struct QuestDbShardTableProvider {
    table_name: String,
    executor: FederatedExecutor,
    schema: Arc<Schema>,
    _registry: TableRegistry,
    _default_shard_key: String,
}

impl QuestDbShardTableProvider {
    pub fn new(
        table_name: impl Into<String>,
        executor: FederatedExecutor,
        registry: TableRegistry,
        default_shard_key: impl Into<String>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            executor,
            schema,
            _registry: registry,
            _default_shard_key: default_shard_key.into(),
        }
    }

    fn scan_sql(&self, limit: Option<usize>) -> String {
        let cols = self
            .schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name().replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("SELECT {cols} FROM {}", self.table_name);
        // Per-shard LIMIT is an upper bound only; DataFusion still applies the
        // global limit above this scan. It just trims how much each shard returns.
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    }

    async fn fetch_all_batches(&self, sql: &str) -> DfResult<Vec<arrow::record_batch::RecordBatch>> {
        let shard_ids = self.executor.pool.shard_ids();
        let mut all_rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut pg_fields = None;

        for shard_id in shard_ids {
            let responses = query_shard(&self.executor.pool, shard_id, sql)
                .await
                .map_err(|e| DataFusionError::External(Box::new(std::io::Error::other(e.to_string()))))?;
            if let Some((fields, rows)) = extract_query_rows(&responses) {
                if pg_fields.is_none() {
                    pg_fields = Some(fields);
                }
                let col_count = pg_fields.as_ref().map(|f| f.len()).unwrap_or(0);
                for row in rows {
                    all_rows.push(decode_row_cells(&row, col_count));
                }
            }
        }

        let Some(fields) = pg_fields else {
            return Ok(vec![]);
        };

        Ok(vec![rows_to_typed_batch_for_schema(
            self.schema.as_ref(),
            &fields,
            &all_rows,
        )?])
    }
}

impl std::fmt::Debug for QuestDbShardTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuestDbShardTableProvider")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for QuestDbShardTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<arrow_schema::Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let sql = self.scan_sql(limit);
        let batches = self.fetch_all_batches(&sql).await?;
        Ok(MemorySourceConfig::try_new_exec(
            &[batches],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?)
    }
}
