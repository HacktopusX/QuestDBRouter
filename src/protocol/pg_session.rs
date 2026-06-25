use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_postgres::datafusion_pg_catalog::pg_catalog::context::EmptyContextProvider;
use datafusion_postgres::datafusion_pg_catalog::setup_pg_catalog;

use crate::app::AppState;
use crate::federated::catalog::TableCatalog;

/// Build a DataFusion session and register shard tables from config.
pub async fn build_session_context(
    state: &AppState,
) -> anyhow::Result<(Arc<SessionContext>, Arc<TableCatalog>)> {
    if !state.config.routing.federated_enabled {
        anyhow::bail!("federated queries are disabled; enable routing.federated_enabled");
    }

    let session_config = SessionConfig::new().with_information_schema(true);
    let ctx = Arc::new(SessionContext::new_with_config(session_config));
    let catalog = Arc::new(TableCatalog::new(ctx.clone(), state.clone()));
    catalog.register_config_tables()?;

    let catalog_name = ctx
        .state()
        .config()
        .options()
        .catalog
        .default_catalog
        .clone();
    setup_pg_catalog(&ctx, &catalog_name, EmptyContextProvider)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok((ctx, catalog))
}
