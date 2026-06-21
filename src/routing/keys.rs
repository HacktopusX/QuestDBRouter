use memchr::memchr;

/// Extract shard key from Influx line protocol (named tag on first line).
pub fn shard_key_from_ilp(body: &[u8], tag_name: &str) -> String {
    if body.is_empty() {
        return tag_name.to_string();
    }
    let end = memchr(b'\n', body).unwrap_or(body.len());
    let line = &body[..end];
    if let Some(tags_start) = memchr(b',', line) {
        let tags = &line[tags_start + 1..];
        let tag_section = match memchr(b' ', tags) {
            Some(space) => &tags[..space],
            None => tags,
        };
        for tag in tag_section.split(|b| *b == b',') {
            if let Some(eq) = memchr(b'=', tag) {
                let name = &tag[..eq];
                if name == tag_name.as_bytes() {
                    return String::from_utf8_lossy(&tag[eq + 1..]).into_owned();
                }
            }
        }
    }
    tag_name.to_string()
}

/// Extract shard key from a SQL statement (WHERE column = value).
pub fn shard_key_from_sql(query: &str, shard_key_column: &str) -> Option<String> {
    let needle = shard_key_column.to_ascii_lowercase();
    let lower = query.to_ascii_lowercase();
    if let Some(pos) = lower.find(&needle) {
        let after = &query[pos + needle.len()..];
        if let Some(eq_pos) = after.find('=') {
            let value_part = after[eq_pos + 1..].trim();
            return extract_quoted_or_token(value_part);
        }
    }
    None
}

fn extract_quoted_or_token(s: &str) -> Option<String> {
    let s = s.trim();
    if s.starts_with('\'') {
        let end = s[1..].find('\'')?;
        return Some(s[1..1 + end].to_string());
    }
    if s.starts_with('"') {
        let end = s[1..].find('"')?;
        return Some(s[1..1 + end].to_string());
    }
    let end = s
        .find(|c: char| c.is_whitespace() || c == ',' || c == ';')
        .unwrap_or(s.len());
    if end > 0 {
        Some(s[..end].to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ilp_tag_extraction() {
        let body = b"trades,symbol=BTC-USD,side=buy price=100 1234567890\n";
        assert_eq!(shard_key_from_ilp(body, "symbol"), "BTC-USD");
    }

    #[test]
    fn sql_key_extraction() {
        let q = "SELECT * FROM trades WHERE symbol = 'ETH-USD'";
        assert_eq!(shard_key_from_sql(q, "symbol"), Some("ETH-USD".into()));
    }
}
