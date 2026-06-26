use std::sync::Arc;

use datafusion::prelude::SessionContext;
use log::debug;
use sqlparser::ast::{ObjectNamePart, Query, Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::app::AppState;
use crate::federated::pg_types::schema_from_columns;
use crate::federated::provider::QuestDbShardTableProvider;
use crate::federated::FederatedExecutor;

/// Extract physical table references from a read query (best-effort).
pub fn extract_table_names(sql: &str) -> Vec<String> {
    let Ok(stmts) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return vec![];
    };
    let mut names = Vec::new();
    for stmt in stmts {
        if let Statement::Query(query) = stmt {
            collect_query_tables(&query, &mut names);
        }
    }
    names.sort_unstable();
    names.dedup();
    names
}

fn collect_query_tables(query: &Query, names: &mut Vec<String>) {
    if let SetExpr::Select(select) = query.body.as_ref() {
        collect_select_tables(select, names);
    }
}

fn collect_select_tables(select: &Select, names: &mut Vec<String>) {
    for twj in &select.from {
        if let TableFactor::Table { name, .. } = &twj.relation {
            names.push(object_name(name));
        }
        for j in &twj.joins {
            if let TableFactor::Table { name, .. } = &j.relation {
                names.push(object_name(name));
            }
        }
    }
}

fn object_name(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(id) => Some(id.value.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

pub fn bare_table_name(qualified: &str) -> &str {
    qualified.rsplit('.').next().unwrap_or(qualified)
}

fn is_system_relation(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.starts_with("information_schema")
        || lower.starts_with("pg_catalog")
        || lower.starts_with("pg_")
        || matches!(lower.as_str(), "information_schema" | "pg_catalog")
}

/// Registers configured shard tables into a DataFusion session at startup.
pub struct TableCatalog {
    ctx: Arc<SessionContext>,
    state: AppState,
    executor: FederatedExecutor,
}

impl TableCatalog {
    pub fn new(ctx: Arc<SessionContext>, state: AppState) -> Self {
        Self {
            ctx,
            executor: FederatedExecutor::new(state.clone()),
            state,
        }
    }

    pub fn register_config_tables(&self) -> anyhow::Result<()> {
        for table_cfg in &self.state.config.routing.tables {
            let name = &table_cfg.name;
            if is_system_relation(name) {
                continue;
            }
            let schema = schema_from_columns(&table_cfg.columns)
                .map_err(|e| anyhow::anyhow!("table {name}: {e}"))?;
            let provider = QuestDbShardTableProvider::new(
                name.clone(),
                self.executor.clone(),
                self.state.table_registry(),
                self.state.config.routing.shard_key.clone(),
                schema,
            );
            self.ctx
                .register_table(name, Arc::new(provider))
                .map_err(|e| anyhow::anyhow!("register_table {name}: {e}"))?;
            debug!("registered federated table {name}");
        }
        Ok(())
    }

    pub fn ensure_tables_in_sql(&self, sql: &str) -> anyhow::Result<()> {
        for qualified in extract_table_names(sql) {
            if is_system_relation(&qualified) {
                continue;
            }
            let table = bare_table_name(&qualified);
            if self.state.table_registry().get(table).is_none() {
                anyhow::bail!("table {table} is not configured in routing.tables");
            }
            if !self.ctx.table_exist(table)? {
                anyhow::bail!("table {table} is not registered");
            }
        }
        Ok(())
    }
}
