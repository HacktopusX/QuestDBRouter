use std::collections::HashMap;

use log::debug;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::instrument;

use crate::app::AppState;
use crate::config::ShardConfig;
use crate::metadata::{MetadataProvider, Protocol};
use crate::metrics;
use crate::routing::{measurement_from_ilp, shard_key_from_ilp, RoutingError};

/// Per-connection or per-actor ILP upstream pool with lazy shard TCP connections.
pub struct IlpForwarder {
    upstreams: HashMap<u32, TcpStream>,
}

impl IlpForwarder {
    pub fn new() -> Self {
        Self {
            upstreams: HashMap::new(),
        }
    }

    /// Forward a single ILP line (must include trailing newline).
    pub async fn forward_line(
        &mut self,
        state: &AppState,
        line: &[u8],
        conn_id: &str,
    ) -> anyhow::Result<()> {
        if line.len() <= 1 {
            return Ok(());
        }

        let measurement = measurement_from_ilp(line).unwrap_or_default();
        let tag_name = state
            .metadata
            .snapshot()
            .table_registry()
            .shard_key_for(&measurement, state.metadata.default_shard_key());
        let key = shard_key_from_ilp(line, &tag_name);
        let shard = route_ilp_line(state, &key, conn_id)?;
        let shard_id = shard.id;

        let upstream = match self.upstreams.get_mut(&shard_id) {
            Some(s) => s,
            None => {
                let stream = TcpStream::connect(shard.ilp_address.as_str()).await?;
                self.upstreams.insert(shard_id, stream);
                self.upstreams.get_mut(&shard_id).expect("just inserted")
            }
        };

        upstream.write_all(line).await?;

        if let Some(hub) = &state.stream {
            hub.publish(line);
        }

        metrics::record_request("write", Some(shard_id));
        debug!("ilp line routed shard_id={shard_id} key={key} lines=1");
        Ok(())
    }

    /// Forward a line without newline; one is appended if missing.
    pub async fn forward_line_str(
        &mut self,
        state: &AppState,
        line: &str,
        conn_id: &str,
    ) -> anyhow::Result<()> {
        if line.ends_with('\n') {
            self.forward_line(state, line.as_bytes(), conn_id).await
        } else {
            let mut buf = line.as_bytes().to_vec();
            buf.push(b'\n');
            self.forward_line(state, &buf, conn_id).await
        }
    }

    /// Forward multiple ILP lines and flush all upstream connections.
    pub async fn forward_batch(
        &mut self,
        state: &AppState,
        lines: &[String],
        conn_id: &str,
    ) -> anyhow::Result<()> {
        for line in lines {
            self.forward_line_str(state, line, conn_id).await?;
        }
        self.flush().await
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        for upstream in self.upstreams.values_mut() {
            upstream.flush().await?;
        }
        Ok(())
    }
}

impl Default for IlpForwarder {
    fn default() -> Self {
        Self::new()
    }
}

#[instrument(name = "ilp.route", skip(state), fields(shard_id, key, conn_id))]
fn route_ilp_line(
    state: &AppState,
    key: &str,
    conn_id: &str,
) -> Result<ShardConfig, RoutingError> {
    let shard = state.route_key(key, Protocol::Ilp)?;
    tracing::Span::current().record("shard_id", shard.id);
    tracing::Span::current().record("key", key);
    tracing::Span::current().record("conn_id", conn_id);
    Ok(shard)
}
