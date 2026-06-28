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
    #[serde(default)]
    pub stream: StreamConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListenConfig {
    /// ILP line protocol (QuestDB default 9009)
    pub ilp: SocketAddr,
    /// PostgreSQL wire protocol (QuestDB default 8812)
    pub pg: SocketAddr,
    /// WebSocket stream server (optional; required when stream.enabled)
    pub stream: Option<SocketAddr>,
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
    #[serde(default = "default_federated_enabled")]
    pub federated_enabled: bool,
    #[serde(default = "default_max_federated_rows")]
    pub max_federated_rows: u64,
    #[serde(default)]
    pub scan_allow_order_by: bool,
    #[serde(default = "default_pg_pool_size")]
    pub pg_pool_size: usize,
    #[serde(default)]
    pub tables: Vec<TableRoutingConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableRoutingConfig {
    pub name: String,
    #[serde(default = "default_table_sharded")]
    pub sharded: bool,
    pub shard_key: Option<String>,
    pub columns: Vec<ColumnConfig>,
}

fn default_table_sharded() -> bool {
    true
}

fn default_federated_enabled() -> bool {
    true
}

fn default_max_federated_rows() -> u64 {
    1_000_000
}

fn default_pg_pool_size() -> usize {
    4
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
    #[serde(default = "default_exclude_unhealthy")]
    pub exclude_unhealthy: bool,
    #[serde(default = "default_min_healthy_shards")]
    pub min_healthy_shards: usize,
}

fn default_exclude_unhealthy() -> bool {
    true
}

fn default_min_healthy_shards() -> usize {
    1
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

#[derive(Debug, Clone, Deserialize)]
pub struct StreamConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_broadcast_capacity")]
    pub broadcast_capacity: usize,
    #[serde(default = "default_replay_window")]
    pub replay_window: usize,
    #[serde(default = "default_topic_tags")]
    pub topic_tags: Vec<String>,
    #[serde(default = "default_topic_missing")]
    pub topic_missing: String,
    #[serde(default = "default_max_client_lag_drops")]
    pub max_client_lag_drops: u32,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            broadcast_capacity: default_broadcast_capacity(),
            replay_window: default_replay_window(),
            topic_tags: default_topic_tags(),
            topic_missing: default_topic_missing(),
            max_client_lag_drops: default_max_client_lag_drops(),
        }
    }
}

fn default_broadcast_capacity() -> usize {
    1024
}

fn default_replay_window() -> usize {
    1000
}

fn default_topic_tags() -> Vec<String> {
    vec!["symbol".into()]
}

fn default_topic_missing() -> String {
    "*".into()
}

fn default_max_client_lag_drops() -> u32 {
    50
}

#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    #[serde(default)]
    pub enabled: bool,
    pub listen: Option<SocketAddr>,
    #[serde(default = "default_ingest_mailbox_capacity")]
    pub mailbox_capacity: usize,
    #[serde(default)]
    pub rustfs: RustFsConfig,
    #[serde(default)]
    pub checkpoint: IngestCheckpointConfig,
    #[serde(default)]
    pub reconcile: IngestReconcileConfig,
    #[serde(default)]
    pub nautilus: NautilusIngestConfig,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: None,
            mailbox_capacity: default_ingest_mailbox_capacity(),
            rustfs: RustFsConfig::default(),
            checkpoint: IngestCheckpointConfig::default(),
            reconcile: IngestReconcileConfig::default(),
            nautilus: NautilusIngestConfig::default(),
        }
    }
}

fn default_ingest_mailbox_capacity() -> usize {
    256
}

#[derive(Debug, Clone, Deserialize)]
pub struct RustFsConfig {
    pub endpoint: Option<String>,
    pub bucket: Option<String>,
    #[serde(default)]
    pub prefix: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    #[serde(default = "default_rustfs_region")]
    pub region: String,
    #[serde(default = "default_true")]
    pub allow_http: bool,
}

impl Default for RustFsConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            bucket: None,
            prefix: String::new(),
            access_key: None,
            secret_key: None,
            region: default_rustfs_region(),
            allow_http: true,
        }
    }
}

fn default_rustfs_region() -> String {
    "us-east-1".into()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct IngestCheckpointConfig {
    #[serde(default = "default_checkpoint_path")]
    pub path: String,
}

impl Default for IngestCheckpointConfig {
    fn default() -> Self {
        Self {
            path: default_checkpoint_path(),
        }
    }
}

fn default_checkpoint_path() -> String {
    "./ingest.checkpoint.json".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct IngestReconcileConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_reconcile_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_true")]
    pub startup_scan: bool,
}

impl Default for IngestReconcileConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: default_reconcile_interval(),
            startup_scan: true,
        }
    }
}

fn default_reconcile_interval() -> u64 {
    300
}

#[derive(Debug, Clone, Deserialize)]
pub struct NautilusIngestConfig {
    #[serde(default = "default_trade_table")]
    pub trade_table: String,
    #[serde(default = "default_quote_table")]
    pub quote_table: String,
}

impl Default for NautilusIngestConfig {
    fn default() -> Self {
        Self {
            trade_table: default_trade_table(),
            quote_table: default_quote_table(),
        }
    }
}

fn default_trade_table() -> String {
    "trade_ticks".into()
}

fn default_quote_table() -> String {
    "quote_ticks".into()
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .add_source(config::Environment::with_prefix("QUEST_ROUTER").separator("__"))
            .build()?;
        let config: Self = settings.try_deserialize().map_err(anyhow::Error::from)?;
        if config.stream.enabled && config.listen.stream.is_none() {
            anyhow::bail!("stream.enabled requires listen.stream to be set");
        }
        if config.ingest.enabled && config.ingest.listen.is_none() {
            anyhow::bail!("ingest.enabled requires ingest.listen to be set");
        }
        Ok(config)
    }
}
