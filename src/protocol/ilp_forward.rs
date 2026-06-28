use std::collections::HashMap;

use bytes::BytesMut;
use log::debug;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::app::AppState;
use crate::metadata::{MetadataProvider, Protocol};
use crate::metrics;
use crate::routing::{measurement_from_ilp_bytes, shard_key_from_ilp_cow};

/// Flush a shard's write buffer once it grows past this size, to bound memory
/// for large batches while still coalescing many small lines into few syscalls.
const SHARD_BUF_FLUSH_BYTES: usize = 256 * 1024;

struct ShardUpstream {
    stream: TcpStream,
    buf: BytesMut,
}

/// Per-connection or per-actor ILP upstream pool with lazy shard TCP connections.
///
/// Lines are routed to a shard and appended to that shard's write buffer; buffers
/// are written out on [`flush`](IlpForwarder::flush) (or when they exceed
/// [`SHARD_BUF_FLUSH_BYTES`]), coalescing contiguous traffic into few writes.
pub struct IlpForwarder {
    upstreams: HashMap<u32, ShardUpstream>,
}

impl IlpForwarder {
    pub fn new() -> Self {
        Self {
            upstreams: HashMap::new(),
        }
    }

    /// Route and buffer a single ILP line (trailing newline optional).
    pub async fn forward_line(
        &mut self,
        state: &AppState,
        line: &[u8],
        conn_id: &str,
    ) -> anyhow::Result<()> {
        self.route_and_buffer(state, line, conn_id).await
    }

    /// Route and buffer a line without a trailing newline; one is appended.
    pub async fn forward_line_str(
        &mut self,
        state: &AppState,
        line: &str,
        conn_id: &str,
    ) -> anyhow::Result<()> {
        self.route_and_buffer(state, line.as_bytes(), conn_id).await
    }

    async fn route_and_buffer(
        &mut self,
        state: &AppState,
        line: &[u8],
        _conn_id: &str,
    ) -> anyhow::Result<()> {
        // Skip empty / newline-only payloads.
        let content_len = line.iter().take_while(|b| **b != b'\n').count();
        if content_len == 0 {
            return Ok(());
        }

        let snapshot = state.metadata.snapshot();
        let measurement = measurement_from_ilp_bytes(line).unwrap_or("");
        let tag_name = snapshot
            .table_registry()
            .shard_key_for(measurement, state.metadata.default_shard_key());
        let key = shard_key_from_ilp_cow(line, &tag_name);
        let shard = state.route_key(key.as_ref(), Protocol::Ilp)?;
        let shard_id = shard.id;

        if !self.upstreams.contains_key(&shard_id) {
            let stream = TcpStream::connect(shard.ilp_address.as_str()).await?;
            self.upstreams.insert(
                shard_id,
                ShardUpstream {
                    stream,
                    buf: BytesMut::with_capacity(8 * 1024),
                },
            );
        }
        let upstream = self.upstreams.get_mut(&shard_id).expect("just inserted");
        upstream.buf.extend_from_slice(line);
        if line.last() != Some(&b'\n') {
            upstream.buf.extend_from_slice(b"\n");
        }

        if let Some(hub) = &state.stream {
            hub.publish(line);
        }
        metrics::record_request("write", Some(shard_id));

        if upstream.buf.len() >= SHARD_BUF_FLUSH_BYTES {
            let chunk = upstream.buf.split();
            upstream.stream.write_all(&chunk).await?;
        }
        debug!("ilp line buffered shard_id={shard_id} key={key}");
        Ok(())
    }

    /// Route and buffer multiple ILP lines, then flush all upstream connections.
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
            if !upstream.buf.is_empty() {
                let chunk = upstream.buf.split();
                upstream.stream.write_all(&chunk).await?;
            }
            upstream.stream.flush().await?;
        }
        Ok(())
    }
}

impl Default for IlpForwarder {
    fn default() -> Self {
        Self::new()
    }
}
