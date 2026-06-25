/// Aggregate function kinds recognized for federated merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// Execution plan produced from SQL analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutePlan {
    /// Point read: route to exactly one shard via shard-key hash.
    SingleShard {
        table: String,
        shard_key: Option<String>,
        shard_key_param: Option<usize>,
    },
    /// Full table scan across all shards (`SELECT *` / scan without shard key).
    FullScan {
        table: String,
    },
    /// Global aggregate without GROUP BY (`SELECT count(*) FROM t`).
    AggregateScan {
        table: String,
        agg_kind: AggKind,
    },
    /// Join across one or more tables (federated execution).
    Join {
        sql: String,
    },
    /// GROUP BY query requiring partial aggregate merge.
    GroupBy {
        table: String,
        group_cols: Vec<String>,
        agg_kinds: Vec<AggKind>,
    },
}

impl RoutePlan {
    pub fn is_federated(&self) -> bool {
        !matches!(self, RoutePlan::SingleShard { .. })
    }
}
