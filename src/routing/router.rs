use std::sync::Arc;

use crate::app::ResolvedRoute;
use crate::config::ShardConfig;
use crate::metadata::{MetadataHandle, MetadataProvider, Protocol};

use super::{classify_sql, RoutePlan, RoutingError, SqlClassify};

pub trait QueryRouter: Send + Sync {
    fn resolve_route(
        &self,
        sql: &str,
        params: Option<&[String]>,
    ) -> Result<ResolvedRoute, RoutingError>;

    fn route_key(&self, key: &str, protocol: Protocol) -> Result<ShardConfig, RoutingError>;

    fn plan_sql(&self, sql: &str) -> Result<RoutePlan, RoutingError>;
}

pub struct DefaultQueryRouter {
    metadata: MetadataHandle,
    scan_allow_order_by: bool,
}

impl DefaultQueryRouter {
    pub fn new(metadata: MetadataHandle, scan_allow_order_by: bool) -> Arc<Self> {
        Arc::new(Self {
            metadata,
            scan_allow_order_by,
        })
    }
}

impl QueryRouter for DefaultQueryRouter {
    fn resolve_route(
        &self,
        sql: &str,
        params: Option<&[String]>,
    ) -> Result<ResolvedRoute, RoutingError> {
        let snap = self.metadata.snapshot();
        match classify_sql(
            sql,
            self.metadata.default_shard_key(),
            snap.table_registry(),
            self.scan_allow_order_by,
        )? {
            SqlClassify::Passthrough => Ok(ResolvedRoute::Passthrough),
            SqlClassify::Routable(plan) => {
                if let RoutePlan::SingleShard {
                    table,
                    shard_key,
                    shard_key_param,
                } = &plan
                {
                    let key = if let Some(idx) = shard_key_param {
                        params
                            .and_then(|p| p.get(*idx))
                            .cloned()
                            .unwrap_or_else(|| table.clone())
                    } else {
                        shard_key.clone().unwrap_or_else(|| table.clone())
                    };
                    return Ok(ResolvedRoute::Single(
                        self.route_key(&key, Protocol::Pg)?,
                    ));
                }
                Ok(ResolvedRoute::Federated(plan))
            }
        }
    }

    fn route_key(&self, key: &str, protocol: Protocol) -> Result<ShardConfig, RoutingError> {
        self.metadata.snapshot().shard_for_key(key, protocol)
    }

    fn plan_sql(&self, sql: &str) -> Result<RoutePlan, RoutingError> {
        let snap = self.metadata.snapshot();
        match classify_sql(
            sql,
            self.metadata.default_shard_key(),
            snap.table_registry(),
            self.scan_allow_order_by,
        )? {
            SqlClassify::Routable(plan) => Ok(plan),
            SqlClassify::Passthrough => Err(RoutingError::Unsupported(
                "session/transaction SQL is not shard-routable".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, Endpoint, HealthCheckConfig, ListenConfig, RoutingConfig, ShardConfig};
    use crate::metadata::MetadataActor;

    fn test_config() -> Config {
        Config {
            listen: ListenConfig {
                ilp: "127.0.0.1:9009".parse().unwrap(),
                pg: "127.0.0.1:8812".parse().unwrap(),
                stream: None,
            },
            shards: vec![ShardConfig {
                id: 0,
                ilp_address: Endpoint("127.0.0.1:9000".into()),
                pg_address: Endpoint("127.0.0.1:8810".into()),
                weight: 1,
                virtual_nodes: 64,
            }],
            routing: RoutingConfig {
                shard_key: "symbol".into(),
                federated_enabled: true,
                max_federated_rows: 1000,
                scan_allow_order_by: false,
                pg_pool_size: 2,
                tables: vec![],
            },
            pg: Default::default(),
            health_check: HealthCheckConfig {
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
    async fn keyed_read_resolves_single_shard() {
        let (metadata, join) = MetadataActor::spawn(test_config());
        let router = DefaultQueryRouter::new(metadata, false);
        let route = router
            .resolve_route("SELECT * FROM trades WHERE symbol = 'BTC'", None)
            .unwrap();
        assert!(matches!(route, ResolvedRoute::Single(_)));
        join.abort();
    }

    #[tokio::test]
    async fn begin_is_passthrough() {
        let (metadata, join) = MetadataActor::spawn(test_config());
        let router = DefaultQueryRouter::new(metadata, false);
        let route = router.resolve_route("BEGIN", None).unwrap();
        assert!(matches!(route, ResolvedRoute::Passthrough));
        join.abort();
    }
}
