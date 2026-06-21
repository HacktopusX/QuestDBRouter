use crate::metrics;
use crate::routing::shard_key_from_ilp;
use crate::app::AppState;
use bytes::BytesMut;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error};

const READ_BUF: usize = 64 * 1024;

pub async fn serve(state: AppState, listen: std::net::SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen).await?;
    tracing::info!(%listen, "ilp listener ready");

    loop {
        let (socket, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, state).await {
                error!(%peer, "ilp connection error: {e:#}");
            }
        });
    }
}

async fn handle_connection(mut client: TcpStream, state: AppState) -> anyhow::Result<()> {
    let tag_name = state.config.routing.shard_key.clone();
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

            let key = shard_key_from_ilp(&line, &tag_name);
            let shard = state.route_key(&key);
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
            upstream.flush().await?;

            metrics::record_request("write", Some(shard_id));
            debug!(shard_id, %key, lines = 1, "ilp line routed");
        }
    }

    Ok(())
}
