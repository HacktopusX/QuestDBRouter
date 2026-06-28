use crate::app::backend_pg_config;
use crate::config::{PgAuthConfig, ShardConfig};
use crate::pool::PoolError;
use parking_lot::Mutex;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::tokio::client::PgWireClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Shared PG client pool keyed by shard id.
#[derive(Clone)]
pub struct ShardPgPool {
    inner: Arc<ShardPgPoolInner>,
}

struct ShardPgPoolInner {
    shards: HashMap<u32, ShardConfig>,
    auth: PgAuthConfig,
    max_per_shard: usize,
    clients: Mutex<HashMap<u32, Vec<PgWireClient>>>,
    semaphores: HashMap<u32, Arc<Semaphore>>,
}

impl ShardPgPool {
    pub fn new(shards: Vec<ShardConfig>, auth: PgAuthConfig, max_per_shard: usize) -> Self {
        let semaphores = shards
            .iter()
            .map(|s| (s.id, Arc::new(Semaphore::new(max_per_shard.max(1)))))
            .collect();
        let shard_map = shards.into_iter().map(|s| (s.id, s)).collect();
        Self {
            inner: Arc::new(ShardPgPoolInner {
                shards: shard_map,
                auth,
                max_per_shard: max_per_shard.max(1),
                clients: Mutex::new(HashMap::new()),
                semaphores,
            }),
        }
    }

    pub fn shard_ids(&self) -> Vec<u32> {
        let mut ids: Vec<_> = self.inner.shards.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub async fn acquire(&self, shard_id: u32) -> Result<PooledClient, PoolError> {
        let sem = self
            .inner
            .semaphores
            .get(&shard_id)
            .cloned()
            .ok_or(PoolError::UnknownShard { shard_id })?;
        let permit = sem.acquire_owned().await.map_err(|e| {
            tracing::warn!(shard_id, err = %e, "shard PG semaphore closed");
            PoolError::ConnectFailed {
                shard_id,
                source: Box::new(e),
            }
        })?;
        // Synchronous, await-free pop from the idle list; never held across `.await`.
        let pooled = {
            let mut guard = self.inner.clients.lock();
            guard.get_mut(&shard_id).and_then(|vec| vec.pop())
        };
        let client = match pooled {
            Some(client) => client,
            None => self.connect(shard_id).await?,
        };
        Ok(PooledClient {
            pool: self.clone(),
            shard_id,
            client: Some(client),
            return_to_pool: true,
            _permit: permit,
        })
    }

    async fn connect(&self, shard_id: u32) -> Result<PgWireClient, PoolError> {
        let shard = self
            .inner
            .shards
            .get(&shard_id)
            .ok_or(PoolError::UnknownShard { shard_id })?
            .clone();
        let config = backend_pg_config(&shard, &self.inner.auth).map_err(|e| {
            tracing::warn!(shard_id, err = %e, "shard PG config invalid");
            PoolError::ConnectFailed {
                shard_id,
                source: e.into(),
            }
        })?;
        let startup = DefaultStartupHandler::new();
        PgWireClient::connect(Arc::new(config), startup, None)
            .await
            .map_err(|e| {
                tracing::warn!(shard_id, err = %e, "shard PG connect failed");
                PoolError::ConnectFailed {
                    shard_id,
                    source: Box::new(e),
                }
            })
    }

    fn release(&self, shard_id: u32, client: PgWireClient) {
        let mut guard = self.inner.clients.lock();
        let vec = guard.entry(shard_id).or_default();
        if vec.len() < self.inner.max_per_shard {
            vec.push(client);
        }
    }
}

pub struct PooledClient {
    pool: ShardPgPool,
    shard_id: u32,
    client: Option<PgWireClient>,
    return_to_pool: bool,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PooledClient {
    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    pub fn client_mut(&mut self) -> Result<&mut PgWireClient, PoolError> {
        self.client
            .as_mut()
            .ok_or(PoolError::ClientTaken { shard_id: self.shard_id })
    }

    /// Drop a dead connection instead of returning it to the pool.
    pub fn invalidate(&mut self) {
        self.return_to_pool = false;
        self.client = None;
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if !self.return_to_pool {
            return;
        }
        // Release synchronously so the connection is back in the pool before the
        // capacity permit (`_permit`, dropped after this) is released — no spawn,
        // no window where a waiter sees a free permit but an empty pool.
        if let Some(client) = self.client.take() {
            self.pool.release(self.shard_id, client);
        }
    }
}
