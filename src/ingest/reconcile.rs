use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};

use crate::config::IngestReconcileConfig;
use crate::metrics;

use super::actor::{IngestHandle, IngestSource, ProcessObject};
use super::checkpoint::CheckpointStore;
use super::fetcher::ObjectFetcher;

pub async fn run_reconcile_loop(
    handle: IngestHandle,
    fetcher: Arc<ObjectFetcher>,
    checkpoint_path: String,
    config: IngestReconcileConfig,
) {
    if config.startup_scan {
        reconcile_once(&handle, &fetcher, &checkpoint_path, IngestSource::Reconcile).await;
    }

    if !config.enabled {
        return;
    }

    let interval = Duration::from_secs(config.interval_secs.max(1));
    loop {
        tokio::time::sleep(interval).await;
        reconcile_once(&handle, &fetcher, &checkpoint_path, IngestSource::Reconcile).await;
    }
}

async fn reconcile_once(
    handle: &IngestHandle,
    fetcher: &ObjectFetcher,
    checkpoint_path: &str,
    source: IngestSource,
) {
    let checkpoint = match CheckpointStore::load(checkpoint_path) {
        Ok(c) => c,
        Err(e) => {
            warn!("reconcile checkpoint load failed: {e:#}");
            return;
        }
    };

    let objects = match fetcher.list_feather_objects().await {
        Ok(o) => o,
        Err(e) => {
            warn!("reconcile list failed: {e:#}");
            return;
        }
    };

    let mut lag = 0usize;
    for meta in objects {
        let key = meta.location.as_ref();
        if checkpoint.contains(fetcher.bucket(), key) {
            continue;
        }
        lag += 1;
        let msg = ProcessObject {
            bucket: fetcher.bucket().to_string(),
            key: key.to_string(),
            etag: meta.e_tag.clone(),
            source,
        };
        if handle.enqueue(msg).await.is_err() {
            warn!("reconcile mailbox full; will retry on next scan");
            break;
        }
    }

    metrics::record_ingest_reconcile_lag(lag);
    if lag > 0 {
        info!("reconcile enqueued {lag} object(s) for ingest");
    }
}
