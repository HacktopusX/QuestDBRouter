use crate::app::AppState;
use crate::config::Config;
use crate::ingest::{self, IngestActor};
use crate::metadata::{MetadataActor, MetadataHandle};
use crate::metrics;
use crate::protocol::ilp;
use crate::stream;
use log::{info, warn};
#[cfg(feature = "federated")]
use crate::protocol::{pg_gateway::DatafusionPgGateway, PgWireGateway};
#[cfg(not(feature = "federated"))]
use crate::protocol::pg;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

pub async fn run(config_path: &str) -> anyhow::Result<()> {
    let config = Config::from_file(config_path)?;
    info!(
        "starting quest-router ilp={} pg={} shards={} stream={} ingest={}",
        config.listen.ilp,
        config.listen.pg,
        config.shards.len(),
        config.stream.enabled,
        config.ingest.enabled,
    );

    metrics::init(config.metrics.enabled, config.metrics.address)?;

    let (metadata, actor_task) = MetadataActor::spawn(config.clone());
    tokio::spawn(async move {
        if let Err(e) = actor_task.await {
            warn!("metadata actor task ended unexpectedly: {e}");
        }
    });
    let state = AppState::new(config.clone(), metadata.clone())?;
    let ilp_listen = state.config.listen.ilp;
    let pg_listen = state.config.listen.pg;
    let stream_enabled = state.config.stream.enabled;

    let health_config = Arc::new(state.config.health_check.clone());
    let health_shards = state.config.shards.clone();
    tokio::spawn(async move {
        health_check_loop(metadata, health_config, health_shards).await;
    });

    let ingest_enabled = config.ingest.enabled;
    let ingest_listen = config.ingest.listen;
    let ingest_http_state = if ingest_enabled {
        let (ingest_handle, ingest_task) = IngestActor::spawn(state.clone(), config.ingest.clone())?;
        let fetcher = Arc::new(ingest::fetcher::ObjectFetcher::from_config(&config.ingest.rustfs)?);

        if config.ingest.reconcile.enabled || config.ingest.reconcile.startup_scan {
            let reconcile_handle = ingest_handle.clone();
            let reconcile_fetcher = fetcher.clone();
            let reconcile_config = config.ingest.reconcile.clone();
            let checkpoint_path = config.ingest.checkpoint.path.clone();
            tokio::spawn(async move {
                ingest::reconcile::run_reconcile_loop(
                    reconcile_handle,
                    reconcile_fetcher,
                    checkpoint_path,
                    reconcile_config,
                )
                .await;
            });
        }

        tokio::spawn(async move {
            if let Err(e) = ingest_task.await {
                warn!("ingest actor task failed: {e:?}");
            }
        });

        Some((
            ingest::http::HttpState {
                handle: ingest_handle,
                default_bucket: config
                    .ingest
                    .rustfs
                    .bucket
                    .clone()
                    .unwrap_or_default(),
                prefix: config.ingest.rustfs.prefix.clone(),
            },
            ingest_listen.ok_or_else(|| {
                anyhow::anyhow!("ingest.listen required when ingest.enabled = true")
            })?,
        ))
    } else {
        None
    };

    // Collect every long-running listener into one homogeneous future set so the
    // wiring isn't duplicated across the stream/ingest/federated cfg combinations.
    type ServerTask = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
    let mut tasks: Vec<ServerTask> = Vec::new();

    tasks.push(Box::pin(ilp::serve(state.clone(), ilp_listen)));

    #[cfg(feature = "federated")]
    {
        let pg_gateway = DatafusionPgGateway::new(state.clone());
        tasks.push(Box::pin(async move { pg_gateway.serve(pg_listen).await }));
    }
    #[cfg(not(feature = "federated"))]
    {
        tasks.push(Box::pin(pg::serve(state.clone(), pg_listen)));
    }

    if stream_enabled {
        let hub = state
            .stream
            .clone()
            .ok_or_else(|| anyhow::anyhow!("stream enabled but hub not initialized"))?;
        let listen = state
            .config
            .listen
            .stream
            .ok_or_else(|| anyhow::anyhow!("stream enabled but listen.stream not set"))?;
        tasks.push(Box::pin(stream::serve(hub, listen)));
    }

    if let Some((http_state, ingest_addr)) = ingest_http_state {
        tasks.push(Box::pin(ingest::http::serve(http_state, ingest_addr)));
    }

    futures::future::try_join_all(tasks).await?;
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
