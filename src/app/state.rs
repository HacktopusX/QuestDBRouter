use crate::config::{Config, PgAuthConfig, ShardConfig};
use crate::pool::ShardPgPool;
use crate::routing::RoutePlan;
use crate::routing::{plan_sql, TableRegistry, ShardRing};
use crate::stream::BroadcastHub;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub shard_ring: ShardRing,
    pub table_registry: TableRegistry,
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
    pub fn new(config: Config) -> anyhow::Result<Self> {
        if config.shards.is_empty() {
            anyhow::bail!("at least one shard is required");
        }
        let shard_ring = ShardRing::from_shards(config.shards.clone());
        let table_registry = TableRegistry::from_config(
            &config.routing.tables,
            &config.routing.shard_key,
        );
        let pg_pool = ShardPgPool::new(
            config.shards.clone(),
            config.pg.clone(),
            config.routing.pg_pool_size,
        );
        let stream = if config.stream.enabled {
            Some(Arc::new(BroadcastHub::new(
                config.stream.clone(),
                table_registry.clone(),
            )))
        } else {
            None
        };
        Ok(Self {
            config: Arc::new(config),
            shard_ring,
            table_registry,
            pg_pool,
            stream,
        })
    }

    pub fn route_key(&self, key: &str) -> ShardConfig {
        self.shard_ring
            .shard_by_key(key)
            .unwrap_or_else(|| self.config.shards[0].clone())
    }

    pub fn resolve_route(
        &self,
        sql: &str,
        params: Option<&[String]>,
    ) -> Result<ResolvedRoute, crate::routing::SqlRouteError> {
        let routing = &self.config.routing;
        match crate::routing::classify_sql(
            sql,
            &routing.shard_key,
            &self.table_registry,
            routing.scan_allow_order_by,
        )? {
            crate::routing::SqlClassify::Passthrough => Ok(ResolvedRoute::Passthrough),
            crate::routing::SqlClassify::Routable(plan) => {
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
                    return Ok(ResolvedRoute::Single(self.route_key(&key)));
                }
                Ok(ResolvedRoute::Federated(plan))
            }
        }
    }

    /// Legacy helper for single-shard routing from parsed info.
    pub fn route_sql_info(
        &self,
        info: &crate::routing::SqlRouteInfo,
        params: Option<&[String]>,
    ) -> ShardConfig {
        let key = if let Some(idx) = info.shard_key_param {
            params
                .and_then(|p| p.get(idx))
                .cloned()
                .unwrap_or_else(|| info.table.clone())
        } else {
            info.shard_key
                .clone()
                .unwrap_or_else(|| info.table.clone())
        };
        self.route_key(&key)
    }

    pub fn route_sql(&self, sql: &str) -> Result<ShardConfig, crate::routing::SqlRouteError> {
        match self.resolve_route(sql, None)? {
            ResolvedRoute::Single(shard) => Ok(shard),
            ResolvedRoute::Passthrough => Err(crate::routing::SqlRouteError::Unsupported(
                "passthrough SQL cannot be shard-routed".into(),
            )),
            ResolvedRoute::Federated(_) => Err(crate::routing::SqlRouteError::Unsupported(
                "federated SQL requires federated executor".into(),
            )),
        }
    }

    pub fn plan_sql(&self, sql: &str) -> Result<RoutePlan, crate::routing::SqlRouteError> {
        plan_sql(
            sql,
            &self.config.routing.shard_key,
            &self.table_registry,
            self.config.routing.scan_allow_order_by,
        )
    }

    pub fn backend_pg_config(&self, shard: &ShardConfig) -> anyhow::Result<pgwire::api::client::Config> {
        backend_pg_config(shard, &self.config.pg)
    }
}

pub fn backend_pg_config(shard: &ShardConfig, auth: &PgAuthConfig) -> anyhow::Result<pgwire::api::client::Config> {
    let (host, port) = shard.pg_address.host_port()?;
    let conn = format!(
        "host={host} port={port} user={} password={} dbname={} sslmode=disable",
        auth.user,
        auth.password,
        auth.database,
    );
    conn.parse().map_err(|e| anyhow::anyhow!("invalid pg config: {e}"))
}
