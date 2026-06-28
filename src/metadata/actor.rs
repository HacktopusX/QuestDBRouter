use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tracing::{debug, instrument, warn};

use crate::config::{Config, PgAuthConfig};
use crate::metrics;
use crate::routing::TableRegistry;

use super::provider::MetadataProvider;
use super::snapshot::{ClusterSnapshot, ShardHealth};

pub enum MetadataMessage {
    ReportHealth {
        shard_id: u32,
        ilp_ok: bool,
        pg_ok: bool,
    },
}

#[derive(Clone)]
pub struct MetadataHandle {
    snapshot_rx: watch::Receiver<Arc<ClusterSnapshot>>,
    tx: mpsc::Sender<MetadataMessage>,
    default_shard_key: Arc<str>,
    pg_auth: PgAuthConfig,
}

impl MetadataHandle {
    pub async fn report_health(&self, shard_id: u32, ilp_ok: bool, pg_ok: bool) {
        if self
            .tx
            .send(MetadataMessage::ReportHealth {
                shard_id,
                ilp_ok,
                pg_ok,
            })
            .await
            .is_err()
        {
            warn!("metadata mailbox full, health update dropped");
        }
    }
}

impl MetadataProvider for MetadataHandle {
    fn snapshot(&self) -> Arc<ClusterSnapshot> {
        self.snapshot_rx.borrow().clone()
    }

    fn default_shard_key(&self) -> &str {
        &self.default_shard_key
    }

    fn pg_auth(&self) -> &PgAuthConfig {
        &self.pg_auth
    }
}

pub struct MetadataActor;

impl MetadataActor {
    pub fn spawn(config: Config) -> (MetadataHandle, tokio::task::JoinHandle<()>) {
        let table_registry = TableRegistry::from_config(
            &config.routing.tables,
            &config.routing.shard_key,
        );
        let initial = Arc::new(ClusterSnapshot::new(
            config.shards.clone(),
            table_registry,
            config.health_check.exclude_unhealthy,
            config.health_check.min_healthy_shards,
        ));
        let (snapshot_tx, snapshot_rx) = watch::channel(initial);
        let (tx, rx) = mpsc::channel(256);

        let default_shard_key: Arc<str> = config.routing.shard_key.clone().into();
        let pg_auth = config.pg.clone();

        let handle = MetadataHandle {
            snapshot_rx,
            tx,
            default_shard_key: default_shard_key.clone(),
            pg_auth: pg_auth.clone(),
        };

        let join = tokio::spawn(async move {
            Self::run(rx, snapshot_tx).await;
        });

        (handle, join)
    }

    async fn run(
        mut rx: mpsc::Receiver<MetadataMessage>,
        snapshot_tx: watch::Sender<Arc<ClusterSnapshot>>,
    ) {
        while let Some(msg) = rx.recv().await {
            match msg {
                MetadataMessage::ReportHealth {
                    shard_id,
                    ilp_ok,
                    pg_ok,
                } => {
                    Self::apply_health(&snapshot_tx, shard_id, ilp_ok, pg_ok);
                }
            }
        }
    }

    #[instrument(name = "metadata.health_update", skip(snapshot_tx), fields(shard_id, ilp_ok, pg_ok))]
    fn apply_health(
        snapshot_tx: &watch::Sender<Arc<ClusterSnapshot>>,
        shard_id: u32,
        ilp_ok: bool,
        pg_ok: bool,
    ) {
        metrics::record_shard_health(shard_id, "ilp", ilp_ok);
        metrics::record_shard_health(shard_id, "pg", pg_ok);

        let current = snapshot_tx.borrow().clone();
        let prev = current
            .health
            .get(&shard_id)
            .copied()
            .unwrap_or(ShardHealth::healthy_all());
        if prev.ilp_ok == ilp_ok && prev.pg_ok == pg_ok {
            return;
        }

        let mut next = (*current).clone();
        next.health
            .insert(shard_id, ShardHealth { ilp_ok, pg_ok });
        debug!("metadata snapshot updated shard_id={shard_id}");
        let _ = snapshot_tx.send(Arc::new(next));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Endpoint;
    use crate::config::ShardConfig;
    use crate::metadata::snapshot::Protocol;

    fn test_config() -> Config {
        Config {
            listen: crate::config::ListenConfig {
                ilp: "127.0.0.1:9009".parse().unwrap(),
                pg: "127.0.0.1:8812".parse().unwrap(),
                stream: None,
            },
            shards: vec![
                ShardConfig {
                    id: 0,
                    ilp_address: Endpoint("127.0.0.1:9000".into()),
                    pg_address: Endpoint("127.0.0.1:8810".into()),
                    weight: 1,
                    virtual_nodes: 64,
                },
                ShardConfig {
                    id: 1,
                    ilp_address: Endpoint("127.0.0.1:9001".into()),
                    pg_address: Endpoint("127.0.0.1:8811".into()),
                    weight: 1,
                    virtual_nodes: 64,
                },
            ],
            routing: crate::config::RoutingConfig {
                shard_key: "symbol".into(),
                federated_enabled: true,
                max_federated_rows: 1000,
                scan_allow_order_by: false,
                pg_pool_size: 2,
                tables: vec![],
            },
            pg: PgAuthConfig::default(),
            health_check: crate::config::HealthCheckConfig {
                interval_secs: 5,
                timeout_secs: 2,
                exclude_unhealthy: true,
                min_healthy_shards: 1,
            },
            metrics: Default::default(),
            stream: Default::default(),
            ingest: Default::default(),
        }
    }

    #[tokio::test]
    async fn health_update_publishes_new_snapshot() {
        let (handle, join) = MetadataActor::spawn(test_config());
        handle.report_health(0, false, true).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let snap = handle.snapshot();
        assert!(!snap.health.get(&0).unwrap().ilp_ok);
        assert!(snap.health.get(&0).unwrap().pg_ok);

        let shard = snap.shard_for_key("btc", Protocol::Ilp).unwrap();
        assert_eq!(shard.id, 1);

        join.abort();
    }
}
