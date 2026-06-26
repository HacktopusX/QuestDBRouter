use async_trait::async_trait;
use std::net::SocketAddr;

use crate::app::AppState;

#[async_trait]
pub trait PgWireGateway: Send + Sync {
    async fn serve(&self, listen: SocketAddr) -> anyhow::Result<()>;
}

pub struct DatafusionPgGateway {
    state: AppState,
}

impl DatafusionPgGateway {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl PgWireGateway for DatafusionPgGateway {
    async fn serve(&self, listen: SocketAddr) -> anyhow::Result<()> {
        super::pg::serve(self.state.clone(), listen).await
    }
}
