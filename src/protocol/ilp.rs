use crate::metrics;
use crate::metadata::{MetadataProvider, Protocol};
use crate::routing::{measurement_from_ilp, shard_key_from_ilp};
use crate::app::AppState;
use bytes::BytesMut;
use log::{debug, error, info};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::instrument;

const READ_BUF: usize = 64 * 1024;

pub async fn serve(state: AppState, listen: std::net::SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen).await?;
    info!("ilp listener ready on {listen}");

    loop {
        let (socket, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, state).await {
                error!("ilp connection error from {peer}: {e:#}");
            }
        });
    }
}

async fn handle_connection(mut client: TcpStream, state: AppState) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(READ_BUF);
    let mut upstreams: HashMap<u32, TcpStream> = HashMap::new();

    loop {
        let n = client.read_buf(&mut buf).await?;
        if n == 0 {
            break;
        }

        while let Some(newline) = buf.iter().position(|b| *b == b'\n') {
            let line = buf.split_to(newline + 1);
            if line.len() <= 1 {
                continue;
            }

            let measurement = measurement_from_ilp(&line).unwrap_or_default();
            let tag_name = state
                .metadata
                .snapshot()
                .table_registry()
                .shard_key_for(&measurement, state.metadata.default_shard_key());
            let key = shard_key_from_ilp(&line, &tag_name);
            let shard = route_ilp_line(&state, &key)?;
            let shard_id = shard.id;

            let upstream = match upstreams.get_mut(&shard_id) {
                Some(s) => s,
                None => {
                    let stream = TcpStream::connect(shard.ilp_address.as_str()).await?;
                    upstreams.insert(shard_id, stream);
                    upstreams.get_mut(&shard_id).expect("just inserted")
                }
            };

            upstream.write_all(&line).await?;

            if let Some(hub) = &state.stream {
                hub.publish(&line);
            }

            metrics::record_request("write", Some(shard_id));
            debug!("ilp line routed shard_id={shard_id} key={key} lines=1");
        }

        for upstream in upstreams.values_mut() {
            upstream.flush().await?;
        }
    }

    for upstream in upstreams.values_mut() {
        let _ = upstream.flush().await;
    }

    Ok(())
}

#[instrument(name = "ilp.route", skip(state), fields(shard_id, key))]
fn route_ilp_line(
    state: &AppState,
    key: &str,
) -> Result<crate::config::ShardConfig, crate::routing::RoutingError> {
    let shard = state.route_key(key, Protocol::Ilp)?;
    tracing::Span::current().record("shard_id", shard.id);
    tracing::Span::current().record("key", key);
    Ok(shard)
}
