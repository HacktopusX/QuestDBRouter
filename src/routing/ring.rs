use crate::config::ShardConfig;
use ahash::AHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A point on the consistent-hash ring (one virtual node).
#[derive(Debug, Clone, Copy)]
struct RingPoint {
    hash: u64,
    shard_idx: usize,
}

/// Shard selector: consistent hashing over weighted virtual nodes.
#[derive(Debug, Clone)]
pub struct ShardRing {
    shards: Arc<Vec<ShardConfig>>,
    ring: Arc<Vec<RingPoint>>,
}

impl ShardRing {
    pub fn from_shards(shards: Vec<ShardConfig>) -> Self {
        let mut ring = Vec::new();
        for (shard_idx, shard) in shards.iter().enumerate() {
            let vnode_count = effective_vnode_count(shard);
            for v in 0..vnode_count {
                ring.push(RingPoint {
                    hash: Self::vnode_hash(shard.id, v),
                    shard_idx,
                });
            }
        }
        ring.sort_by_key(|p| p.hash);
        Self {
            shards: Arc::new(shards),
            ring: Arc::new(ring),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn vnode_count(&self) -> usize {
        self.ring.len()
    }

    pub fn shard_by_key(&self, key: &str) -> Option<ShardConfig> {
        if self.shards.is_empty() {
            return None;
        }
        let shard_idx = if self.ring.is_empty() {
            0
        } else {
            self.lookup_shard_idx(Self::hash_key(key))
        };
        Some(self.shards[shard_idx].clone())
    }

    pub fn shard_by_id(&self, id: u32) -> Option<ShardConfig> {
        self.shards.iter().find(|s| s.id == id).cloned()
    }

    pub fn hash_key(key: &str) -> u64 {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn vnode_hash(shard_id: u32, vnode: u32) -> u64 {
        let mut hasher = AHasher::default();
        shard_id.hash(&mut hasher);
        vnode.hash(&mut hasher);
        hasher.finish()
    }

    fn lookup_shard_idx(&self, hash: u64) -> usize {
        let ring = self.ring.as_ref();
        let idx = ring.partition_point(|p| p.hash < hash);
        let idx = if idx >= ring.len() { 0 } else { idx };
        ring[idx].shard_idx
    }
}

fn effective_vnode_count(shard: &ShardConfig) -> u32 {
    shard.weight.max(1).saturating_mul(shard.virtual_nodes.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Endpoint;

    fn test_shard(id: u32, port: u16) -> ShardConfig {
        test_shard_weighted(id, port, 1, 64)
    }

    fn test_shard_weighted(id: u32, port: u16, weight: u32, virtual_nodes: u32) -> ShardConfig {
        let ilp = format!("127.0.0.1:{port}");
        let pg = format!("127.0.0.1:{}", port + 1000);
        ShardConfig {
            id,
            ilp_address: Endpoint(ilp),
            pg_address: Endpoint(pg),
            weight,
            virtual_nodes,
        }
    }

    #[test]
    fn ring_builds_weighted_vnodes() {
        let ring = ShardRing::from_shards(vec![
            test_shard_weighted(0, 9000, 2, 10),
            test_shard_weighted(1, 9001, 1, 10),
        ]);
        assert_eq!(ring.vnode_count(), 30);
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

    #[test]
    fn ohlcv_symbols_use_stable_distinct_shards() {
        let ring = ShardRing::from_shards(vec![test_shard(0, 9000), test_shard(1, 9001)]);
        let btc = ring.shard_by_key("btc-usdt").unwrap().id;
        let eth = ring.shard_by_key("eth-usdt").unwrap().id;
        assert_eq!(ring.shard_by_key("btc-usdt").unwrap().id, btc);
        assert_eq!(ring.shard_by_key("eth-usdt").unwrap().id, eth);
    }

    #[test]
    fn load_test_symbols_split_across_two_shards() {
        let ring = ShardRing::from_shards(vec![test_shard(0, 9000), test_shard(1, 9001)]);
        let mut counts = [0u32; 2];
        for i in 0..32 {
            let id = ring.shard_by_key(&format!("SYM-{i:04}")).unwrap().id as usize;
            counts[id] += 1;
        }
        assert!(counts[0] > 0, "shard 0 empty");
        assert!(counts[1] > 0, "shard 1 empty");
    }

    #[test]
    fn weighted_shard_receives_more_keys() {
        let ring = ShardRing::from_shards(vec![
            test_shard_weighted(0, 9000, 3, 64),
            test_shard_weighted(1, 9001, 1, 64),
        ]);
        let mut counts = [0u32; 2];
        for i in 0..10_000 {
            let id = ring.shard_by_key(&format!("key-{i}")).unwrap().id as usize;
            counts[id] += 1;
        }
        // 3:1 weight ratio → expect ~75% / ~25% with tolerance.
        assert!(counts[0] > counts[1], "heavier shard should receive more keys");
        let ratio = counts[0] as f64 / counts[1] as f64;
        assert!(
            (2.0..=4.0).contains(&ratio),
            "expected ~3:1 ratio, got {}:{} ({ratio:.2})",
            counts[0],
            counts[1]
        );
    }

    #[test]
    fn ring_wraps_clockwise_past_last_vnode() {
        let ring = ShardRing::from_shards(vec![test_shard(0, 9000)]);
        assert_eq!(ring.shard_by_key("any-key").unwrap().id, 0);
    }
}
