use crate::app::AppState;
use crate::metrics;
use crate::protocol::ilp_forward::IlpForwarder;
use bytes::BytesMut;
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, instrument};

const READ_BUF: usize = 64 * 1024;

pub async fn serve(state: AppState, listen: std::net::SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen).await?;
    info!("ilp listener ready on {listen}");

    loop {
        let (socket, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, peer, state).await {
                error!(conn_id = %peer, err = %e, "ilp connection error");
            }
        });
    }
}

#[instrument(name = "ilp.connection", skip(client, state), fields(conn_id = %peer))]
async fn handle_connection(
    mut client: TcpStream,
    peer: std::net::SocketAddr,
    state: AppState,
) -> anyhow::Result<()> {
    let conn_id = peer.to_string();
    let start = Instant::now();
    let mut buf = BytesMut::with_capacity(READ_BUF);
    let mut forwarder = IlpForwarder::new();

    loop {
        let n = client.read_buf(&mut buf).await?;
        if n == 0 {
            break;
        }

        while let Some(newline) = buf.iter().position(|b| *b == b'\n') {
            let line = buf.split_to(newline + 1);
            forwarder.forward_line(&state, &line, &conn_id).await?;
        }

        forwarder.flush().await?;
    }

    forwarder.flush().await?;
    metrics::record_duration("ilp", start.elapsed().as_secs_f64());
    Ok(())
}
