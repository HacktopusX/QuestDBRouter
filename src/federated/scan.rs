use super::{
    merge_avg, merge_query_responses, merge_scalar_aggregate, query_all_shards, FederatedExecutor,
};
use crate::federated::error::FederatedError;
use crate::routing::AggKind;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, Statement,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub async fn execute_full_scan(executor: &FederatedExecutor, sql: &str) -> PgWireResult<Vec<Response>> {
    let max_rows = executor.state.config.routing.max_federated_rows;
    let shard_results = query_all_shards(executor, sql).await?;
    let merged = merge_query_responses(shard_results, max_rows)?;
    Ok(merged)
}

pub async fn execute_aggregate_scan(
    executor: &FederatedExecutor,
    sql: &str,
    agg_kind: AggKind,
) -> PgWireResult<Vec<Response>> {
    // AVG is not associative across shards; rewrite it into SUM/COUNT partials and
    // merge those for a correct global mean.
    if matches!(agg_kind, AggKind::Avg) {
        let rewritten = rewrite_avg_to_sum_count(sql).ok_or_else(|| {
            FederatedError::SchemaMismatch(
                "cannot decompose AVG into SUM/COUNT for federated merge".into(),
            )
            .to_pgwire()
        })?;
        let shard_results = query_all_shards(executor, &rewritten).await?;
        return merge_avg(shard_results);
    }

    let shard_results = query_all_shards(executor, sql).await?;
    merge_scalar_aggregate(shard_results, agg_kind)
}

/// Rewrite `SELECT avg(expr) FROM ...` into `SELECT sum(expr), count(expr) FROM ...`
/// while preserving the original FROM/WHERE so per-shard partials can be merged.
fn rewrite_avg_to_sum_count(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }

    let Statement::Query(query) = &mut stmts[0] else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };

    let mut avg_arg: Option<String> = None;
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(Expr::Function(func)) = item {
            if func.name.to_string().eq_ignore_ascii_case("avg") {
                avg_arg = avg_argument_sql(&func.args);
            }
        }
    }
    let arg = avg_arg?;

    // Build the replacement projection by parsing a throwaway query; this avoids
    // hand-constructing AST nodes and keeps the expression formatting consistent.
    let helper_sql = format!("SELECT sum({arg}) , count({arg})");
    let mut helper = Parser::parse_sql(&dialect, helper_sql.as_str()).ok()?;
    let Statement::Query(helper_query) = helper.drain(..).next()? else {
        return None;
    };
    let SetExpr::Select(helper_select) = *helper_query.body else {
        return None;
    };

    select.projection = helper_select.projection;
    Some(stmts[0].to_string())
}

fn avg_argument_sql(args: &FunctionArguments) -> Option<String> {
    if let FunctionArguments::List(list) = args {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = list.args.first() {
            return Some(expr.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::rewrite_avg_to_sum_count;

    #[test]
    fn rewrites_simple_avg() {
        let out = rewrite_avg_to_sum_count("SELECT avg(price) FROM trades").unwrap();
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("sum(price)"), "missing sum: {out}");
        assert!(lower.contains("count(price)"), "missing count: {out}");
        assert!(lower.contains("from trades"), "lost FROM: {out}");
    }

    #[test]
    fn preserves_where_clause() {
        let out =
            rewrite_avg_to_sum_count("SELECT avg(price) FROM trades WHERE side = 'buy'").unwrap();
        assert!(out.to_ascii_lowercase().contains("where side = 'buy'"), "lost WHERE: {out}");
    }

    #[test]
    fn ignores_non_avg() {
        assert!(rewrite_avg_to_sum_count("SELECT sum(price) FROM trades").is_none());
    }
}
