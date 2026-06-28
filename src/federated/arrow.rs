use crate::federated::error::FederatedError;
use crate::pool::PooledClient;
use pgwire::api::client::query::{DefaultSimpleQueryHandler, Response as ClientResponse};
use pgwire::api::results::FieldInfo;
use pgwire::error::PgWireError;
use pgwire::messages::data::DataRow;

fn is_connection_error(err: &PgWireError) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("broken pipe")
        || msg.contains("connection reset")
        || msg.contains("connection refused")
        || msg.contains("connection closed")
        || msg.contains("unexpected eof")
        || msg.contains("unexpected remote message")
}

pub(crate) fn is_missing_table_error(err: &PgWireError) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("table does not exist")
}

/// Fetch query results from a single shard via the pool.
pub async fn query_shard(
    pool: &crate::pool::ShardPgPool,
    shard_id: u32,
    sql: &str,
) -> Result<Vec<ClientResponse>, PgWireError> {
    let mut client = pool
        .acquire(shard_id)
        .await
        .map_err(|e| {
            tracing::warn!(shard_id, err = %e, "shard query failed");
            FederatedError::ShardQuery { shard_id, message: e.to_string() }.to_pgwire()
        })?;
    match query_shard_client(&mut client, sql).await {
        Ok(responses) => Ok(responses),
        Err(e) if is_missing_table_error(&e) => Ok(vec![]),
        Err(e) if is_connection_error(&e) => {
            client.invalidate();
            drop(client);
            let mut client = pool
                .acquire(shard_id)
                .await
                .map_err(|e| {
                    tracing::warn!(shard_id, err = %e, "shard query failed on reconnect");
                    FederatedError::ShardQuery { shard_id, message: e.to_string() }.to_pgwire()
                })?;
            query_shard_client(&mut client, sql).await
        }
        Err(e) => Err(e),
    }
}

pub async fn query_shard_client(
    client: &mut PooledClient,
    sql: &str,
) -> Result<Vec<ClientResponse>, PgWireError> {
    let shard_id = client.shard_id();
    let handler = DefaultSimpleQueryHandler::new();
    client
        .client_mut()
        .map_err(|e| {
            tracing::warn!(shard_id, err = %e, "shard query failed");
            FederatedError::ShardQuery { shard_id, message: e.to_string() }.to_pgwire()
        })?
        .simple_query(handler, sql)
        .await
        .map_err(|e| {
            FederatedError::ShardQuery { shard_id, message: e.to_string() }.to_pgwire()
        })
}

pub fn extract_query_rows(
    responses: &[ClientResponse],
) -> Option<(Vec<FieldInfo>, Vec<DataRow>)> {
    for resp in responses {
        if let ClientResponse::Query((_tag, fields, rows)) = resp {
            return Some((fields.clone(), rows.clone()));
        }
    }
    None
}

pub fn first_cell_string(row: &DataRow) -> Option<String> {
    use bytes::Buf;
    let mut buf = row.data.as_ref();
    if buf.remaining() < 4 {
        return None;
    }
    let len = buf.get_i32();
    if len < 0 {
        return None;
    }
    if buf.remaining() < len as usize {
        return None;
    }
    let bytes = &buf[..len as usize];
    std::str::from_utf8(bytes).ok().map(|s| s.to_string())
}

pub fn first_cell_f64(row: &DataRow) -> Option<f64> {
    first_cell_string(row)?.parse().ok()
}
