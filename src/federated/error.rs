use pgwire::error::{ErrorInfo, PgWireError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FederatedError {
    #[error("shard {shard_id} query failed: {message}")]
    ShardQuery { shard_id: u32, message: String },
    #[error("schema mismatch across shards: {0}")]
    SchemaMismatch(String),
    #[error("result exceeded max rows ({limit})")]
    RowLimitExceeded { limit: usize },
    #[error("federated execution disabled")]
    Disabled,
    #[error("insufficient healthy shards for federated query")]
    InsufficientHealthyShards,
}

impl FederatedError {
    pub fn to_pgwire(&self) -> PgWireError {
        let (code, msg) = match self {
            FederatedError::ShardQuery { .. } => ("XX000", self.to_string()),
            FederatedError::SchemaMismatch(_) => ("XX000", self.to_string()),
            FederatedError::RowLimitExceeded { .. } => ("53400", self.to_string()),
            FederatedError::Disabled => ("0A000", self.to_string()),
            FederatedError::InsufficientHealthyShards => ("08006", self.to_string()),
        };
        PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), code.into(), msg)))
    }
}
