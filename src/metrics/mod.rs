use metrics::{counter, describe_counter, describe_histogram, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tracing::info;

pub fn init(config_enabled: bool, listen: Option<SocketAddr>) -> anyhow::Result<()> {
    describe_counter!(
        "quest_router_requests_total",
        "Total HTTP requests routed by operation and shard"
    );
    describe_histogram!(
        "quest_router_request_duration_seconds",
        "End-to-end request duration in seconds"
    );

    if config_enabled {
        let addr = listen.unwrap_or_else(|| "127.0.0.1:9090".parse().expect("valid addr"));
        PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()?;
        info!(%addr, "prometheus metrics exporter listening");
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
