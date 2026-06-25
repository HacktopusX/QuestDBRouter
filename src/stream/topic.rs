use crate::config::StreamConfig;
use crate::routing::TableRegistry;
use crate::stream::row::IlpRow;

const TOPIC_SEPARATOR: char = '@';

/// Derive a topic key from an parsed ILP row using configured tag names.
pub fn derive_topic(row: &IlpRow, config: &StreamConfig) -> String {
    let parts: Vec<&str> = config
        .topic_tags
        .iter()
        .map(|tag| {
            row.tags
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(tag))
                .map(|(_, v)| v.as_str())
                .unwrap_or(config.topic_missing.as_str())
        })
        .collect();
    parts.join(&TOPIC_SEPARATOR.to_string())
}

/// Returns `true` when the measurement belongs to a sharded table in config.
pub fn is_broadcastable(measurement: &str, registry: &TableRegistry) -> bool {
    registry.get(measurement).is_some_and(|t| t.sharded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::row::parse_ilp_row;

    fn config_with_tags(tags: Vec<&str>) -> StreamConfig {
        StreamConfig {
            topic_tags: tags.into_iter().map(String::from).collect(),
            topic_missing: "*".into(),
            ..Default::default()
        }
    }

    fn registry_with(table: &str, sharded: bool) -> TableRegistry {
        TableRegistry::from_config(
            &[crate::config::TableRoutingConfig {
                name: table.into(),
                sharded,
                shard_key: None,
                columns: vec![],
            }],
            "symbol",
        )
    }

    #[test]
    fn single_tag_topic() {
        let row = parse_ilp_row(b"router_test_trades,symbol=BTC-OHLCV price=100 1\n").unwrap();
        let topic = derive_topic(&row, &config_with_tags(vec!["symbol"]));
        assert_eq!(topic, "BTC-OHLCV");
    }

    #[test]
    fn multi_tag_with_missing_fallback() {
        let row = parse_ilp_row(b"router_test_trades,symbol=BTC-OHLCV price=100 1\n").unwrap();
        let topic = derive_topic(
            &row,
            &config_with_tags(vec!["symbol", "exchange"]),
        );
        assert_eq!(topic, "BTC-OHLCV@*");
    }

    #[test]
    fn multi_tag_with_exchange() {
        let row =
            parse_ilp_row(b"router_test_trades,symbol=BTC-OHLCV,exchange=NASDAQ price=100 1\n")
                .unwrap();
        let topic = derive_topic(
            &row,
            &config_with_tags(vec!["symbol", "exchange"]),
        );
        assert_eq!(topic, "BTC-OHLCV@NASDAQ");
    }

    #[test]
    fn sharded_table_filter() {
        let reg = registry_with("router_test_trades", true);
        assert!(is_broadcastable("router_test_trades", &reg));
        assert!(!is_broadcastable("unknown_table", &reg));
    }

    #[test]
    fn non_sharded_table_skipped() {
        let reg = registry_with("local_metrics", false);
        assert!(!is_broadcastable("local_metrics", &reg));
    }

    #[test]
    fn tag_lookup_is_case_insensitive() {
        let row = parse_ilp_row(b"trades,Symbol=ETH price=1 1\n").unwrap();
        let topic = derive_topic(&row, &config_with_tags(vec!["symbol"]));
        assert_eq!(topic, "ETH");
    }
}
