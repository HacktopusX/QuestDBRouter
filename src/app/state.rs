use std::sync::Arc;

use crate::config::{Config, PgAuthConfig, ShardConfig};
use crate::metadata::{MetadataHandle, MetadataProvider, Protocol};
use crate::pool::ShardPgPool;
use crate::routing::{DefaultQueryRouter, QueryRouter, RoutePlan, RoutingError};
use crate::stream::BroadcastHub;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub metadata: MetadataHandle,
    pub query_router: Arc<dyn QueryRouter>,
    pub pg_pool: ShardPgPool,
    pub stream: Option<Arc<BroadcastHub>>,
}

#[derive(Debug, Clone)]
pub enum ResolvedRoute {
    Single(ShardConfig),
    Federated(RoutePlan),
    Passthrough,
}

impl AppState {
    pub fn new(config: Config, metadata: MetadataHandle) -> anyhow::Result<Self> {
        if config.shards.is_empty() {
            anyhow::bail!("at least one shard is required");
        }
        let table_registry = metadata.snapshot().table_registry().clone();
        let pg_pool = ShardPgPool::new(
            config.shards.clone(),
            config.pg.clone(),
            config.routing.pg_pool_size,
        );
        let query_router: Arc<dyn QueryRouter> = DefaultQueryRouter::new(
            metadata.clone(),
            config.routing.scan_allow_order_by,
        );
        let stream = if config.stream.enabled {
            Some(Arc::new(BroadcastHub::new(
                config.stream.clone(),
                table_registry,
            )))
        } else {
            None
        };
        Ok(Self {
            config: Arc::new(config),
            metadata,
            query_router,
            pg_pool,
            stream,
        })
    }

    pub fn table_registry(&self) -> crate::routing::TableRegistry {
        self.metadata.snapshot().table_registry().clone()
    }

    pub fn route_key(&self, key: &str, protocol: Protocol) -> Result<ShardConfig, RoutingError> {
        self.query_router.route_key(key, protocol)
    }

    pub fn resolve_route(
        &self,
        sql: &str,
        params: Option<&[String]>,
    ) -> Result<ResolvedRoute, RoutingError> {
        self.query_router.resolve_route(sql, params)
    }

    pub fn route_sql_info(
        &self,
        info: &crate::routing::SqlRouteInfo,
        params: Option<&[String]>,
    ) -> Result<ShardConfig, RoutingError> {
        // Never silently fall back to the table name as the shard key — that would
        // mis-shard the query. Require an explicit literal or bound parameter.
        let key = if let Some(idx) = info.shard_key_param {
            params.and_then(|p| p.get(idx)).cloned().ok_or_else(|| {
                RoutingError::Unsupported(format!(
                    "missing bind parameter ${} for shard key",
                    idx + 1
                ))
            })?
        } else {
            info.shard_key.clone().ok_or_else(|| {
                RoutingError::Unsupported("query has no shard-key value to route on".into())
            })?
        };
        self.route_key(&key, Protocol::Pg)
    }

    pub fn route_sql(&self, sql: &str) -> Result<ShardConfig, RoutingError> {
        match self.resolve_route(sql, None)? {
            ResolvedRoute::Single(shard) => Ok(shard),
            ResolvedRoute::Passthrough => Err(RoutingError::Unsupported(
                "passthrough SQL cannot be shard-routed".into(),
            )),
            ResolvedRoute::Federated(_) => Err(RoutingError::Unsupported(
                "federated SQL requires federated executor".into(),
            )),
        }
    }

    pub fn plan_sql(&self, sql: &str) -> Result<RoutePlan, RoutingError> {
        self.query_router.plan_sql(sql)
    }

    pub fn backend_pg_config(
        &self,
        shard: &ShardConfig,
    ) -> anyhow::Result<pgwire::api::client::Config> {
        backend_pg_config(shard, self.metadata.pg_auth())
    }

    pub fn healthy_pg_shard_ids(&self) -> Result<Vec<u32>, RoutingError> {
        let snap = self.metadata.snapshot();
        snap.ensure_min_healthy(Protocol::Pg)?;
        Ok(snap
            .healthy_shards(Protocol::Pg)
            .into_iter()
            .map(|s| s.id)
            .collect())
    }
}

pub fn backend_pg_config(
    shard: &ShardConfig,
    auth: &PgAuthConfig,
) -> anyhow::Result<pgwire::api::client::Config> {
    let (host, port) = shard.pg_address.host_port()?;
    let conn = format!(
        "host={host} port={port} user={} password={} dbname={} sslmode=disable",
        auth.user,
        auth.password,
        auth.database,
    );
    conn.parse()
        .map_err(|e| anyhow::anyhow!("invalid pg config: {e}"))
}
