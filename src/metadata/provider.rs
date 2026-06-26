use std::sync::Arc;

use crate::config::PgAuthConfig;

use super::snapshot::ClusterSnapshot;

/// Read-only access to cluster topology and health snapshots.
pub trait MetadataProvider: Send + Sync {
    fn snapshot(&self) -> Arc<ClusterSnapshot>;
    fn default_shard_key(&self) -> &str;
    fn pg_auth(&self) -> &PgAuthConfig;
}
