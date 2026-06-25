use memchr::memchr;

/// Zero-copy measurement name from an ILP line (bytes before the first comma or space).
pub fn measurement_from_ilp_bytes(body: &[u8]) -> Option<&str> {
    if body.is_empty() {
        return None;
    }
    let end = memchr(b'\n', body).unwrap_or(body.len());
    let line = &body[..end];
    let name_end = memchr(b',', line)
        .unwrap_or(line.len())
        .min(memchr(b' ', line).unwrap_or(line.len()));
    if name_end == 0 {
        return None;
    }
    std::str::from_utf8(&line[..name_end]).ok()
}

/// Measurement name from the first ILP line (bytes before the first comma or space).
pub fn measurement_from_ilp(body: &[u8]) -> Option<String> {
    measurement_from_ilp_bytes(body).map(str::to_owned)
}

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
                if tag_name_eq(name, tag_name) {
                    return String::from_utf8_lossy(&tag[eq + 1..]).into_owned();
                }
            }
        }
    }
    tag_name.to_string()
}

fn tag_name_eq(name: &[u8], tag_name: &str) -> bool {
    std::str::from_utf8(name)
        .ok()
        .is_some_and(|n| n.eq_ignore_ascii_case(tag_name))
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
    fn ilp_tag_name_is_case_insensitive() {
        let body = b"trades,Symbol=ETH-USD price=100 1234567890\n";
        assert_eq!(shard_key_from_ilp(body, "symbol"), "ETH-USD");
    }

    #[test]
    fn ilp_measurement_extraction() {
        let body = b"router_test_ohlcv,symbol=btc-usdt,interval=5m open=1 1\n";
        assert_eq!(
            measurement_from_ilp(body).as_deref(),
            Some("router_test_ohlcv")
        );
    }
}
