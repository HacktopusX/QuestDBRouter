use crate::config::ShardConfig;
use hashring::HashRing;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VNode {
    shard_id: u32,
    vnode_id: u32,
}

/// Shard selector: consistent hashing over weighted virtual nodes.
#[derive(Debug, Clone)]
pub struct ShardRing {
    ring: Arc<HashRing<VNode>>,
    shards: Arc<Vec<ShardConfig>>,
}

impl ShardRing {
    pub fn from_shards(shards: Vec<ShardConfig>) -> Self {
        let mut ring = HashRing::new();
        for shard in &shards {
            let count = effective_vnode_count(shard);
            for v in 0..count {
                ring.add(VNode {
                    shard_id: shard.id,
                    vnode_id: v,
                });
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

    pub fn shards(&self) -> &[ShardConfig] {
        &self.shards
    }

    pub fn vnode_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| effective_vnode_count(s) as usize)
            .sum()
    }

    pub fn shard_by_key(&self, key: &str) -> Option<&ShardConfig> {
        if self.shards.is_empty() {
            return None;
        }
        #[derive(Hash)]
        struct Key<'a>(&'a str);
        let vnode = self.ring.get(&Key(key))?;
        self.shard_by_id(vnode.shard_id)
    }

    /// Consistent-hash lookup skipping shards for which `is_excluded(id)` is true.
    ///
    /// When the primary shard is excluded, this walks the ring deterministically
    /// using salted probes to land on the next healthy shard, then falls back to
    /// the lowest non-excluded shard id. The result is reproducible across process
    /// restarts (stable hashing) so a key's failover target is deterministic.
    ///
    /// NOTE: rows written to a failover shard while the primary is down are not
    /// migrated back when it recovers — that requires resharding (future phase).
    pub fn shard_by_key_filtered<F>(&self, key: &str, is_excluded: F) -> Option<&ShardConfig>
    where
        F: Fn(u32) -> bool,
    {
        if self.shards.is_empty() {
            return None;
        }
        if let Some(shard) = self.shard_by_key(key) {
            if !is_excluded(shard.id) {
                return Some(shard);
            }
        }
        let max_probes = self.vnode_count().max(1);
        for attempt in 1..=max_probes {
            if let Some(vnode) = self.ring.get(&(key, attempt)) {
                if !is_excluded(vnode.shard_id) {
                    if let Some(shard) = self.shard_by_id(vnode.shard_id) {
                        return Some(shard);
                    }
                }
            }
        }
        self.shards
            .iter()
            .filter(|s| !is_excluded(s.id))
            .min_by_key(|s| s.id)
    }

    pub fn shard_by_id(&self, id: u32) -> Option<&ShardConfig> {
        self.shards.iter().find(|s| s.id == id)
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

    #[test]
    fn filtered_lookup_skips_excluded_primary() {
        let ring = ShardRing::from_shards(vec![test_shard(0, 9000), test_shard(1, 9001)]);
        let primary = ring.shard_by_key("btc-usdt").unwrap().id;
        let fallback = ring
            .shard_by_key_filtered("btc-usdt", |id| id == primary)
            .unwrap();
        assert_ne!(fallback.id, primary);
    }

    #[test]
    fn filtered_lookup_is_deterministic_across_calls() {
        let ring = ShardRing::from_shards(vec![
            test_shard(0, 9000),
            test_shard(1, 9001),
            test_shard(2, 9002),
        ]);
        let primary = ring.shard_by_key("btc-usdt").unwrap().id;
        let first = ring
            .shard_by_key_filtered("btc-usdt", |id| id == primary)
            .unwrap()
            .id;
        for _ in 0..50 {
            let again = ring
                .shard_by_key_filtered("btc-usdt", |id| id == primary)
                .unwrap()
                .id;
            assert_eq!(again, first, "failover target must be deterministic");
            assert_ne!(again, primary);
        }
    }

    #[test]
    fn filtered_lookup_returns_none_when_all_excluded() {
        let ring = ShardRing::from_shards(vec![test_shard(0, 9000), test_shard(1, 9001)]);
        assert!(ring.shard_by_key_filtered("any", |_| true).is_none());
    }
}
