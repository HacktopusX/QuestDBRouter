use crate::app::AppState;
use crate::config::Config;
use crate::metadata::{MetadataActor, MetadataHandle};
use crate::metrics;
use crate::protocol::ilp;
use crate::stream;
use log::{info, warn};
#[cfg(feature = "federated")]
use crate::protocol::{pg_gateway::DatafusionPgGateway, PgWireGateway};
#[cfg(not(feature = "federated"))]
use crate::protocol::pg;
use std::sync::Arc;
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

    let (metadata, _actor_task) = MetadataActor::spawn(config.clone());
    let state = AppState::new(config, metadata.clone())?;
    let ilp_listen = state.config.listen.ilp;
    let pg_listen = state.config.listen.pg;
    let stream_enabled = state.config.stream.enabled;
    let stream_listen = state.config.listen.stream;
    let stream_hub = state.stream.clone();

    let health_config = Arc::new(state.config.health_check.clone());
    let health_shards = state.config.shards.clone();
    tokio::spawn(async move {
        health_check_loop(metadata, health_config, health_shards).await;
    });

    if stream_enabled {
        let hub = stream_hub
            .ok_or_else(|| anyhow::anyhow!("stream enabled but hub not initialized"))?;
        let listen = stream_listen
            .ok_or_else(|| anyhow::anyhow!("stream enabled but listen.stream not set"))?;
        let ilp_state = state.clone();
        #[cfg(feature = "federated")]
        {
            let pg_gateway = DatafusionPgGateway::new(state.clone());
            tokio::try_join!(
                ilp::serve(ilp_state, ilp_listen),
                pg_gateway.serve(pg_listen),
                stream::serve(hub, listen),
            )?;
        }
        #[cfg(not(feature = "federated"))]
        {
            tokio::try_join!(
                ilp::serve(ilp_state, ilp_listen),
                pg::serve(state.clone(), pg_listen),
                stream::serve(hub, listen),
            )?;
        }
    } else {
        let ilp_state = state.clone();
        #[cfg(feature = "federated")]
        {
            let pg_gateway = DatafusionPgGateway::new(state.clone());
            tokio::try_join!(
                ilp::serve(ilp_state, ilp_listen),
                pg_gateway.serve(pg_listen),
            )?;
        }
        #[cfg(not(feature = "federated"))]
        {
            tokio::try_join!(
                ilp::serve(ilp_state, ilp_listen),
                pg::serve(state.clone(), pg_listen),
            )?;
        }
    }

    Ok(())
}

async fn health_check_loop(
    metadata: MetadataHandle,
    health: Arc<crate::config::HealthCheckConfig>,
    shards: Vec<crate::config::ShardConfig>,
) {
    let interval = Duration::from_secs(health.interval_secs);
    let probe_timeout = Duration::from_secs(health.timeout_secs);

    loop {
        for shard in &shards {
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

            metadata.report_health(shard.id, ilp_ok, pg_ok).await;
        }
        tokio::time::sleep(interval).await;
    }
}
