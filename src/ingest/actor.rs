use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tracing::instrument;

use crate::app::AppState;
use crate::config::IngestConfig;
use crate::metrics;
use crate::protocol::ilp_forward::IlpForwarder;

use super::checkpoint::CheckpointStore;
use super::events::{matches_prefix, should_ingest_key};
use super::fetcher::ObjectFetcher;
use super::nautilus::decode_feather_to_ilp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestSource {
    Webhook,
    Reconcile,
}

#[derive(Debug, Clone)]
pub struct ProcessObject {
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub source: IngestSource,
}

pub enum IngestMessage {
    ProcessObject(ProcessObject),
}

#[derive(Clone)]
pub struct IngestHandle {
    tx: mpsc::Sender<IngestMessage>,
    capacity: usize,
}

impl IngestHandle {
    pub async fn enqueue(&self, msg: ProcessObject) -> Result<(), mpsc::error::TrySendError<IngestMessage>> {
        self.tx.try_send(IngestMessage::ProcessObject(msg))
    }

    pub fn mailbox_capacity(&self) -> usize {
        self.capacity
    }

    pub fn mailbox_depth(&self) -> usize {
        self.capacity.saturating_sub(self.tx.capacity())
    }
}

pub struct IngestActor;

impl IngestActor {
    pub fn spawn(
        state: AppState,
        config: IngestConfig,
    ) -> anyhow::Result<(IngestHandle, tokio::task::JoinHandle<()>)> {
        let fetcher = Arc::new(ObjectFetcher::from_config(&config.rustfs)?);
        let checkpoint = CheckpointStore::load(&config.checkpoint.path)
            .context("load ingest checkpoint")?;
        let (tx, rx) = mpsc::channel(config.mailbox_capacity);
        let capacity = config.mailbox_capacity;

        let handle = IngestHandle { tx, capacity };

        let join = tokio::spawn(async move {
            Self::run(state, config, fetcher, checkpoint, rx).await;
        });

        Ok((handle, join))
    }

    async fn run(
        state: AppState,
        config: IngestConfig,
        fetcher: Arc<ObjectFetcher>,
        mut checkpoint: CheckpointStore,
        mut rx: mpsc::Receiver<IngestMessage>,
    ) {
        let mut forwarder = IlpForwarder::new();

        while let Some(msg) = rx.recv().await {
            match msg {
                IngestMessage::ProcessObject(object) => {
                    metrics::record_ingest_mailbox_depth(fetcher.bucket(), rx.len());
                    let source = object.source;
                    let key = object.key.clone();
                    if let Err(e) = Self::process_object(
                        &state,
                        &config,
                        &fetcher,
                        &mut checkpoint,
                        &mut forwarder,
                        object,
                    )
                    .await
                    {
                        error!("ingest object failed key={key}: {e:#}");
                        metrics::record_ingest_object_processed(source, "error");
                    }
                }
            }
        }
    }

    #[instrument(name = "ingest.process", skip(state, config, fetcher, checkpoint, forwarder), fields(bucket, key))]
    async fn process_object(
        state: &AppState,
        config: &IngestConfig,
        fetcher: &ObjectFetcher,
        checkpoint: &mut CheckpointStore,
        forwarder: &mut IlpForwarder,
        object: ProcessObject,
    ) -> anyhow::Result<()> {
        tracing::Span::current().record("bucket", &object.bucket);
        tracing::Span::current().record("key", &object.key);

        if !should_ingest_key(&object.key) {
            return Ok(());
        }
        if !matches_prefix(&object.key, &config.rustfs.prefix) {
            return Ok(());
        }

        if !checkpoint.should_process(&object.bucket, &object.key, object.etag.as_deref()) {
            debug!("skipping already-processed object {}", object.key);
            metrics::record_ingest_object_processed(object.source, "skipped");
            return Ok(());
        }

        let mut last_err = None;
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
            }
            match Self::fetch_decode_forward(state, config, fetcher, forwarder, &object).await {
                Ok((table, rows)) => {
                    checkpoint.mark_processed(
                        &object.bucket,
                        &object.key,
                        object.etag.clone(),
                        rows,
                    )?;
                    metrics::record_ingest_object_processed(object.source, "ok");
                    metrics::record_ingest_rows_forwarded(&table, rows);
                    info!(
                        "ingested object key={} rows={} source={:?}",
                        object.key, rows, object.source
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "ingest attempt {} failed for {}: {e:#}",
                        attempt + 1,
                        object.key
                    );
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("ingest failed")))
    }

    async fn fetch_decode_forward(
        state: &AppState,
        config: &IngestConfig,
        fetcher: &ObjectFetcher,
        forwarder: &mut IlpForwarder,
        object: &ProcessObject,
    ) -> anyhow::Result<(String, u64)> {
        let (bytes, etag_from_store) = fetcher.get_object(&object.key).await?;
        let _etag = object.etag.clone().or(etag_from_store);

        let (table, lines) = decode_feather_to_ilp(&object.key, &bytes, &config.nautilus)?;
        let rows = lines.len() as u64;
        forwarder.forward_batch(state, &lines, "ingest").await?;
        Ok((table, rows))
    }
}
