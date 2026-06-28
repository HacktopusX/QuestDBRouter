use std::collections::HashMap;

use crate::config::ShardConfig;
use crate::routing::{RoutingError, ShardRing, TableRegistry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Ilp,
    Pg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShardHealth {
    pub ilp_ok: bool,
    pub pg_ok: bool,
}

impl ShardHealth {
    pub fn healthy_all() -> Self {
        Self {
            ilp_ok: true,
            pg_ok: true,
        }
    }

    pub fn is_ok_for(&self, protocol: Protocol) -> bool {
        match protocol {
            Protocol::Ilp => self.ilp_ok,
            Protocol::Pg => self.pg_ok,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub ring: ShardRing,
    pub table_registry: TableRegistry,
    pub health: HashMap<u32, ShardHealth>,
    pub exclude_unhealthy: bool,
    pub min_healthy_shards: usize,
}

impl ClusterSnapshot {
    pub fn new(
        shards: Vec<ShardConfig>,
        table_registry: TableRegistry,
        exclude_unhealthy: bool,
        min_healthy_shards: usize,
    ) -> Self {
        let health = shards
            .iter()
            .map(|s| (s.id, ShardHealth::healthy_all()))
            .collect();
        Self {
            ring: ShardRing::from_shards(shards),
            table_registry,
            health,
            exclude_unhealthy,
            min_healthy_shards,
        }
    }

    pub fn table_registry(&self) -> &TableRegistry {
        &self.table_registry
    }

    fn is_excluded(&self, shard_id: u32, protocol: Protocol) -> bool {
        self.exclude_unhealthy
            && self
                .health
                .get(&shard_id)
                .is_some_and(|h| !h.is_ok_for(protocol))
    }

    pub fn healthy_count(&self, protocol: Protocol) -> usize {
        if !self.exclude_unhealthy {
            return self.ring.shard_count();
        }
        self.ring
            .shards()
            .iter()
            .filter(|s| {
                self.health
                    .get(&s.id)
                    .is_some_and(|h| h.is_ok_for(protocol))
            })
            .count()
    }

    pub fn ensure_min_healthy(&self, protocol: Protocol) -> Result<(), RoutingError> {
        if !self.exclude_unhealthy {
            return Ok(());
        }
        let available = self.healthy_count(protocol);
        if available < self.min_healthy_shards {
            return Err(RoutingError::InsufficientHealthyShards {
                required: self.min_healthy_shards,
                available,
            });
        }
        Ok(())
    }

    pub fn shard_for_key(&self, key: &str, protocol: Protocol) -> Result<&ShardConfig, RoutingError> {
        self.ensure_min_healthy(protocol)?;
        self.ring
            .shard_by_key_filtered(key, |id| self.is_excluded(id, protocol))
            .ok_or_else(|| RoutingError::ShardNotFound {
                key: key.to_string(),
            })
    }

    pub fn shard_by_id(&self, id: u32, protocol: Protocol) -> Result<&ShardConfig, RoutingError> {
        self.ensure_min_healthy(protocol)?;
        let shard = self
            .ring
            .shard_by_id(id)
            .ok_or(RoutingError::ShardNotFound {
                key: id.to_string(),
            })?;
        if self.is_excluded(id, protocol) {
            return Err(RoutingError::ShardUnhealthy { shard_id: id });
        }
        Ok(shard)
    }

    /// Healthy shard ids for fan-out queries (no `ShardConfig` cloning).
    pub fn healthy_shard_ids(&self, protocol: Protocol) -> Vec<u32> {
        if !self.exclude_unhealthy {
            return self.ring.shards().iter().map(|s| s.id).collect();
        }
        self.ring
            .shards()
            .iter()
            .filter(|s| {
                self.health
                    .get(&s.id)
                    .is_some_and(|h| h.is_ok_for(protocol))
            })
            .map(|s| s.id)
            .collect()
    }

    pub fn default_healthy_shard(&self, protocol: Protocol) -> Result<&ShardConfig, RoutingError> {
        self.ensure_min_healthy(protocol)?;
        self.ring
            .shards()
            .iter()
            .find(|s| !self.is_excluded(s.id, protocol))
            .ok_or(RoutingError::InsufficientHealthyShards {
                required: self.min_healthy_shards.max(1),
                available: 0,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Endpoint;

    fn test_shard(id: u32) -> ShardConfig {
        ShardConfig {
            id,
            ilp_address: Endpoint(format!("127.0.0.1:900{id}")),
            pg_address: Endpoint(format!("127.0.0.1:881{id}")),
            weight: 1,
            virtual_nodes: 64,
        }
    }

    #[test]
    fn excludes_unhealthy_primary_shard() {
        let mut snap = ClusterSnapshot::new(
            vec![test_shard(0), test_shard(1)],
            TableRegistry::default(),
            true,
            1,
        );
        snap.health.insert(0, ShardHealth { ilp_ok: false, pg_ok: true });
        let shard = snap.shard_for_key("btc-usdt", Protocol::Ilp).unwrap();
        assert_eq!(shard.id, 1);
    }

    #[test]
    fn fails_when_insufficient_healthy_shards() {
        let mut snap = ClusterSnapshot::new(
            vec![test_shard(0), test_shard(1)],
            TableRegistry::default(),
            true,
            1,
        );
        snap.health.insert(0, ShardHealth { ilp_ok: false, pg_ok: false });
        snap.health.insert(1, ShardHealth { ilp_ok: false, pg_ok: false });
        let err = snap.shard_for_key("x", Protocol::Ilp).unwrap_err();
        assert!(matches!(
            err,
            RoutingError::InsufficientHealthyShards { .. }
        ));
    }

    #[test]
    fn healthy_shard_ids_excludes_bad_nodes() {
        let mut snap = ClusterSnapshot::new(
            vec![test_shard(0), test_shard(1)],
            TableRegistry::default(),
            true,
            1,
        );
        snap.health.insert(1, ShardHealth { ilp_ok: true, pg_ok: false });
        let ids = snap.healthy_shard_ids(Protocol::Pg);
        assert_eq!(ids, vec![0]);
    }
}
