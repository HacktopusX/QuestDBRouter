use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use log::info;

pub fn init(config_enabled: bool, listen: Option<SocketAddr>) -> anyhow::Result<()> {
    describe_counter!(
        "quest_router_requests_total",
        "Total HTTP requests routed by operation and shard"
    );
    describe_histogram!(
        "quest_router_request_duration_seconds",
        "End-to-end request duration in seconds"
    );

    describe_histogram!(
        "quest_router_merge_latency_seconds",
        "Federated query merge duration in seconds"
    );
    describe_counter!(
        "quest_router_federated_queries_total",
        "Total federated queries by plan type"
    );
    describe_histogram!(
        "quest_router_federated_shards_queried",
        "Number of shards queried per federated request"
    );

    describe_counter!(
        "quest_router_stream_ticks_published_total",
        "Total ILP ticks published to stream topics"
    );
    describe_gauge!(
        "quest_router_stream_subscribers",
        "Active stream subscribers per topic"
    );
    describe_counter!(
        "quest_router_stream_client_lagged_total",
        "Stream client lag events (slow consumer)"
    );
    describe_counter!(
        "quest_router_stream_client_dropped_total",
        "Stream clients disconnected due to excessive lag"
    );

    if config_enabled {
        let addr = listen.unwrap_or_else(|| "127.0.0.1:9090".parse().expect("valid addr"));
        PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()?;
        info!("prometheus metrics exporter listening on {addr}");
    } else {
        PrometheusBuilder::new().install()?;
    }

    Ok(())
}

pub fn record_request(operation: &str, shard_id: Option<u32>) {
    let shard = shard_id.map(|id| id.to_string()).unwrap_or_else(|| "unknown".into());
    counter!(
        "quest_router_requests_total",
        "operation" => operation.to_string(),
        "shard" => shard
    )
        .increment(1);
}

pub fn record_duration(operation: &str, duration_secs: f64) {
    histogram!(
        "quest_router_request_duration_seconds",
        "operation" => operation.to_string()
    )
        .record(duration_secs);
}

pub fn record_merge_latency(duration_secs: f64) {
    histogram!("quest_router_merge_latency_seconds").record(duration_secs);
}

pub fn record_federated_query(plan: &str) {
    counter!(
        "quest_router_federated_queries_total",
        "plan" => plan.to_string()
    )
    .increment(1);
}

pub fn record_federated_shards(count: usize) {
    histogram!("quest_router_federated_shards_queried").record(count as f64);
}

pub fn record_stream_tick_published() {
    counter!("quest_router_stream_ticks_published_total").increment(1);
}

pub fn record_stream_subscriber(topic: &str, count: usize) {
    gauge!(
        "quest_router_stream_subscribers",
        "topic" => topic.to_string()
    )
    .set(count as f64);
}

pub fn record_stream_client_lagged() {
    counter!("quest_router_stream_client_lagged_total").increment(1);
}

pub fn record_stream_client_dropped() {
    counter!("quest_router_stream_client_dropped_total").increment(1);
}
