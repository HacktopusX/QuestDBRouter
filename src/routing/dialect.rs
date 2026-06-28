//! QuestDB dialect keyword fast-path for SQL that sqlparser cannot parse.
//!
//! Phase 1: detect `SAMPLE BY` / `LATEST ON`, extract routing metadata via
//! lightweight helpers, and route keyed queries as single-shard verbatim passthrough.

use crate::routing::{RoutePlan, SqlRouteError};

const QUESTDB_DIALECT_PREFIX: &str = "questdb dialect:";

/// Returns true when SQL contains QuestDB-specific extensions sqlparser cannot handle.
pub fn has_questdb_extension(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.contains("SAMPLE BY") || upper.contains("LATEST ON")
}

/// Classify QuestDB dialect SQL for shard routing (keyed → single shard).
pub fn classify_questdb_passthrough(
    sql: &str,
    shard_key_column: &str,
) -> Result<RoutePlan, SqlRouteError> {
    let table = extract_from_table(sql).ok_or_else(|| {
        SqlRouteError::Unsupported(format!(
            "{QUESTDB_DIALECT_PREFIX} could not extract table from FROM clause"
        ))
    })?;

    if let Some(literal) = extract_shard_key_literal(sql, shard_key_column) {
        return Ok(RoutePlan::SingleShard {
            table,
            shard_key: Some(literal),
            shard_key_param: None,
        });
    }

    if let Some(param_index) = extract_shard_key_param(sql, shard_key_column) {
        return Ok(RoutePlan::SingleShard {
            table,
            shard_key: None,
            shard_key_param: Some(param_index),
        });
    }

    Err(SqlRouteError::Unsupported(format!(
        "{QUESTDB_DIALECT_PREFIX} queries require a shard-key predicate (e.g. WHERE {shard_key_column} = 'X')"
    )))
}

/// Extract the primary table name from `FROM <name>`.
pub fn extract_from_table(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let from_pos = upper.find(" FROM ")?;
    let rest = &sql[from_pos + 6..];
    let trimmed = rest.trim_start();
    let end = trimmed
        .find(|c: char| c.is_whitespace() || c == ',' || c == ';')
        .unwrap_or(trimmed.len());
    let name = trimmed[..end].trim();
    if name.is_empty() || name == "(" {
        return None;
    }
    // Strip optional schema qualifier: public.trades → trades
    let table = name.rsplit('.').next().unwrap_or(name);
    Some(table.to_string())
}

/// Extract `WHERE <column> = 'literal'` (case-insensitive column match).
pub fn extract_shard_key_literal(sql: &str, column: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let col_upper = column.to_ascii_uppercase();
    let pattern = format!("WHERE {col_upper}");
    let where_pos = upper.find(&pattern)?;
    let rest = &sql[where_pos + pattern.len()..];
    let after_col = skip_column_and_eq(rest, column)?;
    parse_sql_string_literal(after_col)
}

/// Extract `WHERE <column> = $N` → 0-based parameter index.
pub fn extract_shard_key_param(sql: &str, column: &str) -> Option<usize> {
    let upper = sql.to_ascii_uppercase();
    let col_upper = column.to_ascii_uppercase();
    let pattern = format!("WHERE {col_upper}");
    let where_pos = upper.find(&pattern)?;
    let rest = &sql[where_pos + pattern.len()..];
    let after_eq = skip_column_and_eq(rest, column)?;
    let trimmed = after_eq.trim_start();
    if !trimmed.starts_with('$') {
        return None;
    }
    let digits: String = trimmed[1..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    let n: usize = digits.parse().ok()?;
    n.checked_sub(1)
}

fn skip_column_and_eq<'a>(rest: &'a str, _column: &str) -> Option<&'a str> {
    let trimmed = rest.trim_start();
    if !trimmed.starts_with('=') {
        return None;
    }
    Some(trimmed[1..].trim_start())
}

fn parse_sql_string_literal(s: &str) -> Option<String> {
    let trimmed = s.trim_start();
    if !trimmed.starts_with('\'') {
        return None;
    }
    let mut out = String::new();
    let mut chars = trimmed[1..].chars();
    while let Some(c) = chars.next() {
        if c == '\'' {
            match chars.next() {
                Some('\'') => out.push('\''),
                _ => return Some(out),
            }
        } else {
            out.push(c);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_sample_by() {
        assert!(has_questdb_extension("SELECT 1 SAMPLE BY 1h"));
        assert!(!has_questdb_extension("SELECT 1"));
    }

    #[test]
    fn detects_latest_on() {
        assert!(has_questdb_extension(
            "SELECT * FROM t LATEST ON ts PARTITION BY symbol"
        ));
    }

    #[test]
    fn keyed_sample_by_single_shard() {
        let sql = "SELECT ts, count() FROM trades WHERE symbol = 'BTC' SAMPLE BY 1h";
        let plan = classify_questdb_passthrough(sql, "symbol").unwrap();
        assert_eq!(
            plan,
            RoutePlan::SingleShard {
                table: "trades".into(),
                shard_key: Some("BTC".into()),
                shard_key_param: None,
            }
        );
    }

    #[test]
    fn keyed_sample_by_param() {
        let sql = "SELECT ts FROM trades WHERE symbol = $1 SAMPLE BY 15m";
        let plan = classify_questdb_passthrough(sql, "symbol").unwrap();
        assert_eq!(
            plan,
            RoutePlan::SingleShard {
                table: "trades".into(),
                shard_key: None,
                shard_key_param: Some(0),
            }
        );
    }

    #[test]
    fn unkeyed_sample_by_rejected() {
        let sql = "SELECT ts FROM trades SAMPLE BY 1h";
        let err = classify_questdb_passthrough(sql, "symbol").unwrap_err();
        assert!(matches!(err, SqlRouteError::Unsupported(_)));
        assert!(err.to_string().contains(QUESTDB_DIALECT_PREFIX));
    }

    #[test]
    fn keyed_latest_on_single_shard() {
        let sql = "SELECT symbol, price FROM trades WHERE symbol = 'ETH' LATEST ON ts PARTITION BY symbol";
        let plan = classify_questdb_passthrough(sql, "symbol").unwrap();
        assert_eq!(
            plan,
            RoutePlan::SingleShard {
                table: "trades".into(),
                shard_key: Some("ETH".into()),
                shard_key_param: None,
            }
        );
    }

    #[test]
    fn extract_literal_with_escaped_quote() {
        let sql = "SELECT 1 FROM t WHERE symbol = 'A''B' SAMPLE BY 1h";
        let lit = extract_shard_key_literal(sql, "symbol").unwrap();
        assert_eq!(lit, "A'B");
    }

    #[test]
    fn extract_from_table_strips_schema() {
        let sql = "SELECT * FROM public.trades WHERE symbol = 'X' SAMPLE BY 1h";
        assert_eq!(extract_from_table(sql).as_deref(), Some("trades"));
    }
}
