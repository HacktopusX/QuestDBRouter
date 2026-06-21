use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub listen: ListenConfig,
    pub shards: Vec<ShardConfig>,
    pub routing: RoutingConfig,
    #[serde(default)]
    pub pg: PgAuthConfig,
    pub health_check: HealthCheckConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListenConfig {
    /// ILP line protocol (QuestDB default 9009)
    pub ilp: SocketAddr,
    /// PostgreSQL wire protocol (QuestDB default 8812)
    pub pg: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardConfig {
    pub id: u32,
    pub ilp_address: Endpoint,
    pub pg_address: Endpoint,
    #[serde(default = "default_weight")]
    pub weight: u32,
    #[serde(default = "default_vnodes")]
    pub virtual_nodes: u32,
}

/// Hostname or IP with port (e.g. `questdb-0:9009`, `127.0.0.1:8812`).
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct Endpoint(pub String);

impl Endpoint {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn host_port(&self) -> anyhow::Result<(String, u16)> {
        let (host, port_str) = self
            .0
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid endpoint address: {}", self.0))?;
        let port = port_str
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid endpoint port in: {}", self.0))?;
        Ok((host.to_string(), port))
    }
}

fn default_weight() -> u32 {
    1
}

fn default_vnodes() -> u32 {
    128
}

#[derive(Debug, Clone, Deserialize)]
pub struct RoutingConfig {
    /// Tag name (ILP) or column name (SQL) used to pick a shard.
    pub shard_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PgAuthConfig {
    #[serde(default = "default_pg_user")]
    pub user: String,
    #[serde(default = "default_pg_password")]
    pub password: String,
    #[serde(default = "default_pg_database")]
    pub database: String,
}

impl Default for PgAuthConfig {
    fn default() -> Self {
        Self {
            user: default_pg_user(),
            password: default_pg_password(),
            database: default_pg_database(),
        }
    }
}

fn default_pg_user() -> String {
    "admin".into()
}

fn default_pg_password() -> String {
    "quest".into()
}

fn default_pg_database() -> String {
    "qdb".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthCheckConfig {
    #[serde(default = "default_health_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_health_timeout")]
    pub timeout_secs: u64,
}

fn default_health_interval() -> u64 {
    5
}

fn default_health_timeout() -> u64 {
    2
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    pub address: Option<SocketAddr>,
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .add_source(config::Environment::with_prefix("QUEST_ROUTER").separator("__"))
            .build()?;
        settings.try_deserialize().map_err(anyhow::Error::from)
    }
}
