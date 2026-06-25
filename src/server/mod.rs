use crate::app::AppState;
use crate::config::Config;
use crate::metrics;
use crate::protocol::{ilp, pg};
use crate::stream;
use log::{info, warn};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

pub async fn run(config_path: &str) -> anyhow::Result<()> {
    let config = Config::from_file(config_path)?;
    info!(
        "starting quest-router ilp={} pg={} shards={} stream={}",
        config.listen.ilp,
        config.listen.pg,
        config.shards.len(),
        config.stream.enabled,
    );

    metrics::init(config.metrics.enabled, config.metrics.address)?;

    let state = AppState::new(config)?;
    let ilp_listen = state.config.listen.ilp;
    let pg_listen = state.config.listen.pg;
    let stream_enabled = state.config.stream.enabled;
    let stream_listen = state.config.listen.stream;
    let stream_hub = state.stream.clone();
    let health_state = state.clone();

    tokio::spawn(async move {
        health_check_loop(health_state).await;
    });

    let ilp_state = state.clone();
    let pg_state = state.clone();

    if stream_enabled {
        let hub = stream_hub
            .ok_or_else(|| anyhow::anyhow!("stream enabled but hub not initialized"))?;
        let listen = stream_listen
            .ok_or_else(|| anyhow::anyhow!("stream enabled but listen.stream not set"))?;
        tokio::try_join!(
            ilp::serve(ilp_state, ilp_listen),
            pg::serve(pg_state, pg_listen),
            stream::serve(hub, listen),
        )?;
    } else {
        tokio::try_join!(
            ilp::serve(ilp_state, ilp_listen),
            pg::serve(pg_state, pg_listen),
        )?;
    }

    Ok(())
}

async fn health_check_loop(state: AppState) {
    let interval = Duration::from_secs(state.config.health_check.interval_secs);
    let probe_timeout = Duration::from_secs(state.config.health_check.timeout_secs);

    loop {
        for shard in &state.config.shards {
            let ilp_ok = timeout(probe_timeout, TcpStream::connect(shard.ilp_address.as_str()))
                .await
                .map(|r| r.is_ok())
                .unwrap_or(false);
            let pg_ok = timeout(probe_timeout, TcpStream::connect(shard.pg_address.as_str()))
                .await
                .map(|r| r.is_ok())
                .unwrap_or(false);

            if !ilp_ok || !pg_ok {
                warn!(
                    "shard health check failed shard_id={} ilp_ok={ilp_ok} pg_ok={pg_ok}",
                    shard.id
                );
            }
        }
        tokio::time::sleep(interval).await;
    }
}
