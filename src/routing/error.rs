use pgwire::error::{ErrorInfo, PgWireError};
use thiserror::Error;

use super::SqlRouteError;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RoutingError {
    #[error("failed to parse SQL: {0}")]
    Parse(String),
    #[error("unsupported SQL: {0}")]
    Unsupported(String),
    #[error("no shard found for key: {key}")]
    ShardNotFound { key: String },
    #[error("shard {shard_id} is unhealthy")]
    ShardUnhealthy { shard_id: u32 },
    #[error("insufficient healthy shards: need {required}, have {available}")]
    InsufficientHealthyShards {
        required: usize,
        available: usize,
    },
}

impl RoutingError {
    pub fn pg_sqlstate(&self) -> (&'static str, String) {
        match self {
            RoutingError::Parse(msg) => ("42601", msg.clone()),
            RoutingError::Unsupported(msg) => ("42501", msg.clone()),
            RoutingError::ShardNotFound { key } => ("XX000", format!("no shard for key: {key}")),
            RoutingError::ShardUnhealthy { shard_id } => {
                ("08006", format!("shard {shard_id} is unhealthy"))
            }
            RoutingError::InsufficientHealthyShards {
                required,
                available,
            } => (
                "08006",
                format!("insufficient healthy shards: need {required}, have {available}"),
            ),
        }
    }

    pub fn to_pgwire(self) -> PgWireError {
        let (code, msg) = self.pg_sqlstate();
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".into(),
            code.into(),
            msg,
        )))
    }
}

impl From<SqlRouteError> for RoutingError {
    fn from(value: SqlRouteError) -> Self {
        match value {
            SqlRouteError::Parse(s) => RoutingError::Parse(s),
            SqlRouteError::Unsupported(s) => RoutingError::Unsupported(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_parse_to_syntax_sqlstate() {
        assert_eq!(
            RoutingError::Parse("bad".into()).pg_sqlstate().0,
            "42601"
        );
    }

    #[test]
    fn maps_unhealthy_to_connection_sqlstate() {
        assert_eq!(
            RoutingError::ShardUnhealthy { shard_id: 1 }.pg_sqlstate().0,
            "08006"
        );
    }
}
