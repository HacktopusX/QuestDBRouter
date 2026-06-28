use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArguments, GroupByExpr, Ident, ObjectName,
    ObjectNamePart, Query, Select, SelectItem, SetExpr, Statement, TableFactor, Value,
    ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

use crate::routing::{classify_questdb_passthrough, has_questdb_extension, AggKind, RoutePlan};
use crate::routing::schema::TableRegistry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlRouteInfo {
    pub table: String,
    pub shard_key: Option<String>,
    /// 0-based bind parameter index when the shard key is a placeholder (`$1` → `0`).
    pub shard_key_param: Option<usize>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SqlRouteError {
    #[error("failed to parse SQL: {0}")]
    Parse(String),
    #[error("unsupported SQL: {0}")]
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlClassify {
    Routable(RoutePlan),
    /// Session/transaction control relayed to the active backend unchanged.
    Passthrough,
}

fn parse_one_statement(sql: &str) -> Result<Statement, SqlRouteError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| SqlRouteError::Parse(e.to_string()))?;

    if statements.len() != 1 {
        return Err(SqlRouteError::Unsupported(
            "only single-statement queries are supported".into(),
        ));
    }

    Ok(statements.into_iter().next().expect("len checked"))
}

fn is_passthrough_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Set { .. }
            | Statement::ShowVariable { .. }
            | Statement::ShowVariables { .. }
            | Statement::Discard { .. }
            | Statement::Deallocate { .. }
            | Statement::Close { .. }
    )
}

/// Classify SQL for PGWire handling.
#[tracing::instrument(name = "pg.classify", skip(registry), fields(table))]
pub fn classify_sql(
    sql: &str,
    shard_key_column: &str,
    registry: &TableRegistry,
    scan_allow_order_by: bool,
) -> Result<SqlClassify, SqlRouteError> {
    if has_questdb_extension(sql) {
        let plan = classify_questdb_passthrough(sql, shard_key_column)?;
        if let RoutePlan::SingleShard { ref table, .. } = plan {
            tracing::Span::current().record("table", table.as_str());
        }
        return Ok(SqlClassify::Routable(plan));
    }

    let statement = parse_one_statement(sql)?;
    if is_passthrough_statement(&statement) {
        return Ok(SqlClassify::Passthrough);
    }

    match statement {
        Statement::Query(query) => {
            let plan = plan_query(
                &query,
                sql,
                shard_key_column,
                registry,
                scan_allow_order_by,
            )?;
            if let RoutePlan::SingleShard { ref table, .. }
            | RoutePlan::FullScan { ref table }
            | RoutePlan::AggregateScan { ref table, .. }
            | RoutePlan::GroupBy { ref table, .. } = plan
            {
                tracing::Span::current().record("table", table.as_str());
            }
            Ok(SqlClassify::Routable(plan))
        }
        _ => Err(SqlRouteError::Unsupported(
            "only read queries (SELECT) are routed via SQL parser".into(),
        )),
    }
}

/// Analyze a read-only SQL statement for shard routing (legacy helper).
pub fn analyze_read_sql(sql: &str, shard_key_column: &str) -> Result<SqlRouteInfo, SqlRouteError> {
    let registry = TableRegistry::default();
    match classify_sql(sql, shard_key_column, &registry, false)? {
        SqlClassify::Routable(plan) => route_plan_to_info(plan),
        SqlClassify::Passthrough => Err(SqlRouteError::Unsupported(
            "session/transaction SQL is not shard-routable".into(),
        )),
    }
}

pub fn plan_sql(
    sql: &str,
    shard_key_column: &str,
    registry: &TableRegistry,
    scan_allow_order_by: bool,
) -> Result<RoutePlan, SqlRouteError> {
    match classify_sql(sql, shard_key_column, registry, scan_allow_order_by)? {
        SqlClassify::Routable(plan) => Ok(plan),
        SqlClassify::Passthrough => Err(SqlRouteError::Unsupported(
            "session/transaction SQL is not shard-routable".into(),
        )),
    }
}

fn route_plan_to_info(plan: RoutePlan) -> Result<SqlRouteInfo, SqlRouteError> {
    match plan {
        RoutePlan::SingleShard {
            table,
            shard_key,
            shard_key_param,
        } => Ok(SqlRouteInfo {
            table,
            shard_key,
            shard_key_param,
        }),
        _ => Err(SqlRouteError::Unsupported(
            "query is not a single-shard route".into(),
        )),
    }
}

fn plan_query(
    query: &Query,
    sql: &str,
    shard_key_column: &str,
    registry: &TableRegistry,
    scan_allow_order_by: bool,
) -> Result<RoutePlan, SqlRouteError> {
    if query.with.is_some() {
        return Err(SqlRouteError::Unsupported(
            "CTEs are not supported yet".into(),
        ));
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => {
            return Err(SqlRouteError::Unsupported(
                "only simple SELECT queries are supported".into(),
            ));
        }
    };

    if !select.from.is_empty() && select.from.iter().any(|f| !f.joins.is_empty()) {
        return Ok(RoutePlan::Join {
            sql: sql.to_string(),
        });
    }

    let table = primary_table(select)?;
    let shard_match = select
        .selection
        .as_ref()
        .map(|expr| shard_key_from_where(expr, shard_key_column))
        .unwrap_or_default();

    if shard_match.literal.is_some() || shard_match.param_index.is_some() {
        return Ok(RoutePlan::SingleShard {
            table,
            shard_key: shard_match.literal,
            shard_key_param: shard_match.param_index,
        });
    }

    let group_cols = extract_group_cols(&select.group_by);
    if !group_cols.is_empty() {
        let agg_kinds = extract_agg_kinds(select)?;
        return Ok(RoutePlan::GroupBy {
            table,
            group_cols,
            agg_kinds,
        });
    }

    if let Some(agg_kind) = detect_bare_aggregate(select)? {
        return Ok(RoutePlan::AggregateScan { table, agg_kind });
    }

    if !scan_allow_order_by {
        if query.order_by.is_some() {
            return Err(SqlRouteError::Unsupported(
                "ORDER BY on full scans is not supported".into(),
            ));
        }
        if query.limit_clause.is_some() || query.fetch.is_some() {
            return Err(SqlRouteError::Unsupported(
                "LIMIT/OFFSET on full scans is not supported".into(),
            ));
        }
    }

    let _ = registry;
    Ok(RoutePlan::FullScan { table })
}

fn primary_table(select: &Select) -> Result<String, SqlRouteError> {
    if select.from.len() != 1 {
        return Err(SqlRouteError::Unsupported(
            "exactly one table in FROM is required for non-join queries".into(),
        ));
    }

    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err(SqlRouteError::Unsupported(
            "joins must be handled via federated path".into(),
        ));
    }

    match &from.relation {
        TableFactor::Table { name, .. } => Ok(object_name(name)),
        _ => Err(SqlRouteError::Unsupported(
            "only plain table references are supported in FROM".into(),
        )),
    }
}

fn object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(Ident { value, .. }) => value.as_str(),
            ObjectNamePart::Function(_) => "?",
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[derive(Default)]
struct ShardKeyMatch {
    literal: Option<String>,
    param_index: Option<usize>,
}

fn shard_key_from_where(expr: &Expr, column: &str) -> ShardKeyMatch {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if column_matches(left, column) {
                shard_key_from_expr(right)
            } else if column_matches(right, column) {
                shard_key_from_expr(left)
            } else {
                ShardKeyMatch::default()
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let left_match = shard_key_from_where(left, column);
            if left_match.literal.is_some() || left_match.param_index.is_some() {
                left_match
            } else {
                shard_key_from_where(right, column)
            }
        }
        _ => ShardKeyMatch::default(),
    }
}

fn column_matches(expr: &Expr, column: &str) -> bool {
    let col = column.to_ascii_lowercase();
    match expr {
        Expr::Identifier(Ident { value, .. }) => value.eq_ignore_ascii_case(&col),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|Ident { value, .. }| value.eq_ignore_ascii_case(&col)),
        _ => false,
    }
}

fn shard_key_from_expr(expr: &Expr) -> ShardKeyMatch {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::Placeholder(p) => ShardKeyMatch {
                literal: None,
                param_index: placeholder_index(p),
            },
            _ => ShardKeyMatch {
                literal: value_to_string(value),
                param_index: None,
            },
        },
        Expr::Identifier(Ident { value, .. }) => ShardKeyMatch {
            literal: Some(value.clone()),
            param_index: None,
        },
        _ => ShardKeyMatch::default(),
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::SingleQuotedString(s)
        | Value::DoubleQuotedString(s)
        | Value::EscapedStringLiteral(s) => Some(s.clone()),
        Value::Number(n, _) => Some(n.clone()),
        Value::Boolean(b) => Some(b.to_string()),
        Value::Null => None,
        Value::Placeholder(_) => None,
        _ => None,
    }
}

fn placeholder_index(placeholder: &str) -> Option<usize> {
    let digits = placeholder.trim_start_matches(['$', ':']);
    let number: usize = digits.parse().ok()?;
    number.checked_sub(1)
}

fn extract_group_cols(group_by: &GroupByExpr) -> Vec<String> {
    match group_by {
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .filter_map(|e| match e {
                Expr::Identifier(Ident { value, .. }) => Some(value.clone()),
                Expr::CompoundIdentifier(parts) => parts
                    .last()
                    .map(|Ident { value, .. }| value.clone()),
                _ => None,
            })
            .collect(),
        GroupByExpr::All(_) => vec![],
    }
}

fn extract_agg_kinds(select: &Select) -> Result<Vec<AggKind>, SqlRouteError> {
    let mut kinds = Vec::new();
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(Expr::Function(func)) = item
            && let Some(kind) = agg_kind_from_function(func)
        {
            kinds.push(kind);
        }
    }
    if kinds.is_empty() {
        return Err(SqlRouteError::Unsupported(
            "GROUP BY requires aggregate functions in SELECT".into(),
        ));
    }
    Ok(kinds)
}

fn detect_bare_aggregate(select: &Select) -> Result<Option<AggKind>, SqlRouteError> {
    let has_group_by = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
        GroupByExpr::All(_) => true,
    };
    if has_group_by {
        return Ok(None);
    }
    let mut found = None;
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(func)) => {
                let kind = agg_kind_from_function(func).ok_or_else(|| {
                    SqlRouteError::Unsupported("unsupported aggregate function".into())
                })?;
                if found.is_some() {
                    return Err(SqlRouteError::Unsupported(
                        "multiple bare aggregates not supported in aggregate scan".into(),
                    ));
                }
                found = Some(kind);
            }
            SelectItem::Wildcard(_) => continue,
            _ => return Ok(None),
        }
    }
    Ok(found)
}

fn agg_kind_from_function(func: &Function) -> Option<AggKind> {
    let name = func.name.to_string().to_ascii_lowercase();
    match name.as_str() {
        "count" => Some(AggKind::Count),
        "sum" => Some(AggKind::Sum),
        "min" => Some(AggKind::Min),
        "max" => Some(AggKind::Max),
        "avg" => Some(AggKind::Avg),
        _ => None,
    }
    .filter(|_| {
        matches!(
            func.args,
            FunctionArguments::None | FunctionArguments::Subquery(_) | FunctionArguments::List(_)
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RoutePlan;

    fn registry() -> TableRegistry {
        TableRegistry::default()
    }

    #[test]
    fn select_with_shard_key() {
        let plan = plan_sql(
            "SELECT * FROM trades WHERE symbol = 'ETH-USD'",
            "symbol",
            &registry(),
            false,
        )
        .unwrap();
        assert!(matches!(
            plan,
            RoutePlan::SingleShard {
                shard_key: Some(ref k),
                ..
            } if k == "ETH-USD"
        ));
    }

    #[test]
    fn full_scan_classification() {
        let plan = plan_sql("SELECT * FROM trades", "symbol", &registry(), false).unwrap();
        assert!(matches!(plan, RoutePlan::FullScan { .. }));
    }

    #[test]
    fn aggregate_scan_classification() {
        let plan = plan_sql("SELECT count() FROM trades", "symbol", &registry(), false).unwrap();
        assert!(matches!(
            plan,
            RoutePlan::AggregateScan {
                agg_kind: AggKind::Count,
                ..
            }
        ));
    }

    #[test]
    fn join_classification() {
        let plan = plan_sql(
            "SELECT * FROM trades t JOIN orders o ON t.id = o.trade_id",
            "symbol",
            &registry(),
            false,
        )
        .unwrap();
        assert!(matches!(plan, RoutePlan::Join { .. }));
    }

    #[test]
    fn group_by_classification() {
        let plan = plan_sql(
            "SELECT symbol, count() FROM trades GROUP BY symbol",
            "symbol",
            &registry(),
            false,
        )
        .unwrap();
        assert!(matches!(plan, RoutePlan::GroupBy { .. }));
    }

    #[test]
    fn count_with_shard_key_is_single_shard() {
        let plan = plan_sql(
            "SELECT count() FROM router_test_trades WHERE symbol = 'HEALTH-TEST-SYM'",
            "symbol",
            &registry(),
            false,
        )
        .unwrap();
        assert!(matches!(
            plan,
            RoutePlan::SingleShard {
                shard_key: Some(ref k),
                ..
            } if k == "HEALTH-TEST-SYM"
        ));
    }

    #[test]
    fn rejects_insert() {
        let err = plan_sql("INSERT INTO trades VALUES ('BTC', 1.0)", "symbol", &registry(), false)
            .unwrap_err();
        assert!(matches!(err, SqlRouteError::Unsupported(_)));
    }

    #[test]
    fn begin_is_passthrough() {
        let kind = classify_sql("BEGIN", "symbol", &registry(), false).unwrap();
        assert_eq!(kind, SqlClassify::Passthrough);
    }

    #[test]
    fn questdb_sample_by_keyed_is_single_shard() {
        let plan = plan_sql(
            "SELECT ts, count() FROM trades WHERE symbol = 'BTC' SAMPLE BY 1h",
            "symbol",
            &registry(),
            false,
        )
        .unwrap();
        assert!(matches!(
            plan,
            RoutePlan::SingleShard {
                shard_key: Some(ref k),
                ..
            } if k == "BTC"
        ));
    }

    #[test]
    fn questdb_sample_by_unkeyed_is_unsupported() {
        let err = plan_sql(
            "SELECT ts FROM trades SAMPLE BY 1h",
            "symbol",
            &registry(),
            false,
        )
        .unwrap_err();
        assert!(matches!(err, SqlRouteError::Unsupported(_)));
        assert!(err.to_string().contains("questdb dialect:"));
    }

    #[test]
    fn questdb_latest_on_keyed_is_single_shard() {
        let plan = plan_sql(
            "SELECT symbol, price FROM trades WHERE symbol = 'ETH' LATEST ON ts PARTITION BY symbol",
            "symbol",
            &registry(),
            false,
        )
        .unwrap();
        assert!(matches!(
            plan,
            RoutePlan::SingleShard {
                shard_key: Some(ref k),
                ..
            } if k == "ETH"
        ));
    }
}
