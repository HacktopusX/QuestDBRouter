use crate::config::ShardConfig;
use ahash::AHasher;
use hashring::HashRing;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VNode {
    shard_id: u32,
    vnode_id: u32,
}

impl VNode {
    fn new(shard: &ShardConfig, vnode_id: u32) -> Self {
        Self {
            shard_id: shard.id,
            vnode_id,
        }
    }
}

/// Consistent-hash ring for shard selection. Thread-safe via interior Arc.
#[derive(Debug, Clone)]
pub struct ShardRing {
    ring: Arc<HashRing<VNode>>,
    shards: Arc<Vec<ShardConfig>>,
}

impl ShardRing {
    pub fn from_shards(shards: Vec<ShardConfig>) -> Self {
        let mut ring = HashRing::new();
        for shard in &shards {
            for vnode in 0..shard.virtual_nodes {
                ring.add(VNode::new(shard, vnode));
            }
        }
        Self {
            ring: Arc::new(ring),
            shards: Arc::new(shards),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shard_by_key(&self, key: &str) -> Option<ShardConfig> {
        #[derive(Hash)]
        struct Key<'a>(&'a str);
        let vnode = self.ring.get(&Key(key))?;
        self.shards
            .iter()
            .find(|s| s.id == vnode.shard_id)
            .cloned()
    }

    pub fn shard_by_id(&self, id: u32) -> Option<ShardConfig> {
        self.shards.iter().find(|s| s.id == id).cloned()
    }

    pub fn hash_key(key: &str) -> u64 {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Endpoint;

    fn test_shard(id: u32, port: u16) -> ShardConfig {
        let ilp = format!("127.0.0.1:{port}");
        let pg = format!("127.0.0.1:{}", port + 1000);
        ShardConfig {
            id,
            ilp_address: Endpoint(ilp),
            pg_address: Endpoint(pg),
            weight: 1,
            virtual_nodes: 64,
        }
    }

    #[test]
    fn same_key_maps_to_same_shard() {
        let ring = ShardRing::from_shards(vec![test_shard(0, 9000), test_shard(1, 9001)]);
        let a = ring.shard_by_key("sensor_a").unwrap().id;
        let b = ring.shard_by_key("sensor_a").unwrap().id;
        assert_eq!(a, b);
    }

    #[test]
    fn keys_distribute_across_shards() {
        let ring = ShardRing::from_shards(vec![
            test_shard(0, 9000),
            test_shard(1, 9001),
            test_shard(2, 9002),
        ]);
        let ids: Vec<u32> = (0..100)
            .map(|i| ring.shard_by_key(&format!("key_{i}")).unwrap().id)
            .collect();
        assert!(ids.contains(&0));
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }
}
