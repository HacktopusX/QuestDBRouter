use crate::app::AppState;
use crate::federated::arrow::first_cell_f64;
use crate::metrics;
use crate::pool::ShardPgPool;
use crate::routing::{AggKind, RoutePlan};
use pgwire::api::client::query::{DefaultSimpleQueryHandler, Response as ClientResponse};
use pgwire::messages::data::DataRow;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use std::sync::Arc;
use std::time::Instant;

pub mod aggregate;
pub mod arrow;
#[cfg(feature = "federated")]
pub mod catalog;
#[cfg(feature = "federated")]
pub mod join;
#[cfg(feature = "federated")]
pub mod pg_types;
#[cfg(feature = "federated")]
pub mod provider;
pub mod scan;

/// Federated query executor for multi-shard reads.
#[derive(Clone)]
pub struct FederatedExecutor {
    pub state: AppState,
    pub pool: ShardPgPool,
}

impl FederatedExecutor {
    pub fn new(state: AppState) -> Self {
        let pool = state.pg_pool.clone();
        Self { state, pool }
    }

    pub async fn execute_simple(&self, sql: &str, plan: &RoutePlan) -> PgWireResult<Vec<Response>> {
        if !self.state.config.routing.federated_enabled {
            return Err(fed_err(
                "federated queries are disabled; enable routing.federated_enabled",
            ));
        }

        let start = Instant::now();
        let shard_ids = self.pool.shard_ids();
        metrics::record_federated_query(plan_label(plan));
        metrics::record_federated_shards(shard_ids.len());

        let result = match plan {
            RoutePlan::FullScan { .. } => scan::execute_full_scan(self, sql).await,
            RoutePlan::AggregateScan { agg_kind, .. } => {
                scan::execute_aggregate_scan(self, sql, *agg_kind).await
            }
            RoutePlan::GroupBy { .. } => aggregate::execute_group_by(self, sql).await,
            RoutePlan::Join { .. } => {
                #[cfg(feature = "federated")]
                {
                    join::execute_join(self, sql).await
                }
                #[cfg(not(feature = "federated"))]
                {
                    Err(fed_err(
                        "join queries require building with --features federated",
                    ))
                }
            }
            RoutePlan::SingleShard { .. } => {
                Err(fed_err("single-shard plan routed to federated executor"))
            }
        };

        metrics::record_merge_latency(start.elapsed().as_secs_f64());
        result
    }
}

pub(crate) fn fed_err(msg: impl Into<String>) -> pgwire::error::PgWireError {
    pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".into(),
        "XX000".into(),
        msg.into(),
    )))
}

fn plan_label(plan: &RoutePlan) -> &'static str {
    match plan {
        RoutePlan::FullScan { .. } => "full_scan",
        RoutePlan::AggregateScan { .. } => "aggregate_scan",
        RoutePlan::Join { .. } => "join",
        RoutePlan::GroupBy { .. } => "group_by",
        RoutePlan::SingleShard { .. } => "single_shard",
    }
}

pub(crate) async fn query_all_shards(
    executor: &FederatedExecutor,
    sql: &str,
) -> Result<Vec<(u32, Vec<ClientResponse>)>, pgwire::error::PgWireError> {
    let shard_ids = executor.pool.shard_ids();
    let mut handles = Vec::with_capacity(shard_ids.len());

    for shard_id in shard_ids {
        let pool = executor.pool.clone();
        let sql = sql.to_string();
        handles.push(tokio::spawn(async move {
            let mut client = pool
                .acquire(shard_id)
                .await
                .map_err(|e| fed_err(e.to_string()))?;
            let handler = DefaultSimpleQueryHandler::new();
            let responses = client
                .client_mut()
                .simple_query(handler, &sql)
                .await
                .map_err(|e| fed_err(e.to_string()))?;
            Ok::<_, pgwire::error::PgWireError>((shard_id, responses))
        }));
    }

    let mut out = Vec::with_capacity(handles.len());
    for handle in handles {
        out.push(handle.await.map_err(|e| fed_err(e.to_string()))??);
    }
    Ok(out)
}

pub(crate) fn merge_query_responses(
    shard_results: Vec<(u32, Vec<ClientResponse>)>,
    max_rows: u64,
) -> PgWireResult<Vec<Response>> {
    let mut fields: Option<Vec<FieldInfo>> = None;
    let mut all_rows: Vec<DataRow> = Vec::new();

    for (_shard_id, responses) in shard_results {
        for resp in responses {
            match resp {
                ClientResponse::Query((_tag, f, rows)) => {
                    if let Some(ref existing) = fields {
                        if existing.len() != f.len() {
                            return Err(fed_err("schema mismatch across shards"));
                        }
                    } else {
                        fields = Some(f);
                    }
                    for row in rows {
                        if all_rows.len() as u64 >= max_rows {
                            return Err(fed_err(format!(
                                "federated row limit exceeded ({max_rows})"
                            )));
                        }
                        all_rows.push(row);
                    }
                }
                ClientResponse::EmptyQuery => {}
                ClientResponse::Execution(_) => {}
            }
        }
    }

    let fields = fields.unwrap_or_default();
    let _row_count = all_rows.len();
    Ok(vec![Response::Query(QueryResponse::new(
        Arc::new(fields),
        futures::stream::iter(all_rows.into_iter().map(Ok)),
    ))])
}

pub(crate) fn merge_scalar_aggregate(
    shard_results: Vec<(u32, Vec<ClientResponse>)>,
    agg_kind: AggKind,
) -> PgWireResult<Vec<Response>> {
    let mut values: Vec<f64> = Vec::new();
    let mut fields: Option<Vec<FieldInfo>> = None;

    for (_shard_id, responses) in shard_results {
        for resp in responses {
            if let ClientResponse::Query((_tag, f, rows)) = resp {
                fields.get_or_insert(f);
                for row in rows {
                    if let Some(val) = first_cell_f64(&row) {
                        values.push(val);
                    }
                }
            }
        }
    }

    if values.is_empty() {
        return Ok(vec![Response::EmptyQuery]);
    }

    let merged = match agg_kind {
        AggKind::Count | AggKind::Sum => values.iter().sum::<f64>(),
        AggKind::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
        AggKind::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        AggKind::Avg => values.iter().sum::<f64>() / values.len() as f64,
    };

    let fields = fields.unwrap_or_else(|| {
        vec![FieldInfo::new(
            "count".into(),
            None,
            None,
            postgres_types::Type::INT8,
            pgwire::api::results::FieldFormat::Text,
        )]
    });

    let fields_arc = Arc::new(fields);
    let mut encoder = DataRowEncoder::new(fields_arc.clone());
    let text = if merged.fract() == 0.0 {
        format!("{}", merged as i64)
    } else {
        merged.to_string()
    };
    encoder
        .encode_field(&text)
        .map_err(|e| fed_err(e.to_string()))?;
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        fields_arc,
        futures::stream::iter(vec![Ok(row)]),
    ))])
}
