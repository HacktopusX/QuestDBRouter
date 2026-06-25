use crate::app::AppState;
use crate::metrics;
use crate::pool::ShardPgPool;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::{DefaultSimpleQueryHandler, Response as ClientResponse};
use pgwire::api::results::{QueryResponse, Response};
use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::client::PgWireClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) fn wire_err(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "XX000".into(),
        msg.into(),
    )))
}

pub(crate) fn read_only_err() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "42501".into(),
        "quest-router PG endpoint is read-only; use ILP for writes".into(),
    )))
}

struct ConnBackendState {
    backends: HashMap<u32, PgWireClient>,
    current_shard: Option<u32>,
}

impl ConnBackendState {
    fn new() -> Self {
        Self {
            backends: HashMap::new(),
            current_shard: None,
        }
    }
}

fn conn_state<C: ClientInfo + ?Sized>(client: &C) -> Arc<Mutex<ConnBackendState>> {
    client
        .session_extensions()
        .get_or_insert_with(|| Mutex::new(ConnBackendState::new()))
}

fn default_shard_id(state: &AppState) -> u32 {
    state.config.shards[0].id
}

fn resolve_shard_id(state: &AppState, current: Option<u32>) -> u32 {
    current.unwrap_or_else(|| default_shard_id(state))
}

async fn connect_backend<'a>(
    conn: &'a mut ConnBackendState,
    state: &AppState,
    shard_id: u32,
) -> PgWireResult<&'a mut PgWireClient> {
    use std::collections::hash_map::Entry;

    if let Entry::Vacant(entry) = conn.backends.entry(shard_id) {
        let shard = state
            .shard_ring
            .shard_by_id(shard_id)
            .ok_or_else(|| wire_err("shard not found"))?;
        let config = state
            .backend_pg_config(&shard)
            .map_err(|e| wire_err(e.to_string()))?;
        let startup = DefaultStartupHandler::new();
        let client = PgWireClient::connect(Arc::new(config), startup, None)
            .await
            .map_err(|e| wire_err(e.to_string()))?;
        entry.insert(client);
    }
    Ok(conn.backends.get_mut(&shard_id).expect("backend exists"))
}

fn client_to_server(responses: Vec<ClientResponse>) -> PgWireResult<Vec<Response>> {
    let mut out = Vec::with_capacity(responses.len());
    for resp in responses {
        out.push(match resp {
            ClientResponse::EmptyQuery => Response::EmptyQuery,
            ClientResponse::Query((_tag, fields, rows)) => {
                let schema = Arc::new(fields);
                let stream = futures::stream::iter(rows.into_iter().map(Ok));
                Response::Query(QueryResponse::new(schema, stream))
            }
            ClientResponse::Execution(tag) => Response::Execution(tag),
        });
    }
    Ok(out)
}

pub(crate) fn first_response(responses: Vec<Response>) -> PgWireResult<Response> {
    responses
        .into_iter()
        .next()
        .ok_or_else(|| wire_err("empty backend response"))
}

/// Replace `$1`, `$2`, … with SQL literals for backend `simple_query` forwarding.
pub(crate) fn inline_sql_params(sql: &str, params: &[String]) -> String {
    let mut out = sql.to_string();
    for (idx, value) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", idx + 1);
        let literal = if value.eq_ignore_ascii_case("null") {
            "NULL".to_string()
        } else {
            format!("'{}'", value.replace('\'', "''"))
        };
        out = out.replace(&placeholder, &literal);
    }
    out
}

/// Query a single shard via the shared pool (stateless reads).
pub(crate) async fn query_shard_pool(
    pool: &ShardPgPool,
    shard_id: u32,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let responses = crate::federated::arrow::query_shard(pool, shard_id, sql)
        .await
        .map_err(|e| wire_err(e.to_string()))?;
    client_to_server(responses)
}

#[cfg(test)]
mod tests {
    use super::inline_sql_params;

    #[test]
    fn inlines_positional_params_high_to_low() {
        let sql = "SELECT * FROM trades WHERE symbol = $1 AND id > $2";
        let out = inline_sql_params(sql, &["BTC-USD".into(), "42".into()]);
        assert_eq!(
            out,
            "SELECT * FROM trades WHERE symbol = 'BTC-USD' AND id > '42'"
        );
    }

    #[test]
    fn escapes_single_quotes() {
        let sql = "SELECT * FROM trades WHERE symbol = $1";
        let out = inline_sql_params(sql, &["A'B".into()]);
        assert_eq!(out, "SELECT * FROM trades WHERE symbol = 'A''B'");
    }
}

/// Relay session/passthrough SQL through a sticky per-connection backend client.
pub(crate) async fn relay_passthrough(
    state: &AppState,
    client: &mut dyn datafusion_postgres::hooks::HookClient,
    sql: &str,
) -> PgWireResult<Response> {
    let conn = conn_state(client);
    let guard = conn.lock().await;
    let shard_id = resolve_shard_id(state, guard.current_shard);
    drop(guard);

    metrics::record_request("read", Some(shard_id));

    let conn = conn_state(client);
    let mut guard = conn.lock().await;
    guard.current_shard = Some(shard_id);
    let backend = connect_backend(&mut guard, state, shard_id).await?;
    let handler = DefaultSimpleQueryHandler::new();
    let responses = backend
        .simple_query(handler, sql)
        .await
        .map_err(|e| wire_err(e.to_string()))?;
    let pg_responses = client_to_server(responses)?;
    drop(guard);
    first_response(pg_responses)
}
