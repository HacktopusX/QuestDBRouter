use crate::config::{Config, PgAuthConfig, ShardConfig};
use crate::routing::ShardRing;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub shard_ring: ShardRing,
}

impl AppState {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        if config.shards.is_empty() {
            anyhow::bail!("at least one shard is required");
        }
        let shard_ring = ShardRing::from_shards(config.shards.clone());
        Ok(Self {
            config: Arc::new(config),
            shard_ring,
        })
    }

    pub fn route_key(&self, key: &str) -> ShardConfig {
        self.shard_ring
            .shard_by_key(key)
            .unwrap_or_else(|| self.config.shards[0].clone())
    }

    pub fn route_sql(&self, sql: &str) -> ShardConfig {
        let key = crate::routing::shard_key_from_sql(sql, &self.config.routing.shard_key)
            .unwrap_or_else(|| self.config.routing.shard_key.clone());
        self.route_key(&key)
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
