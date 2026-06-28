use thiserror::Error;

#[derive(Debug, Error)]
pub enum PoolError {
    #[error("unknown shard {shard_id}")]
    UnknownShard { shard_id: u32 },
    #[error("shard {shard_id} connection failed: {source}")]
    ConnectFailed {
        shard_id: u32,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("client handle already taken for shard {shard_id}")]
    ClientTaken { shard_id: u32 },
}
