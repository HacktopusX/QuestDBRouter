use memchr::memchr;
use std::borrow::Cow;

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
///
/// Borrows directly from `body` when the tag value is valid UTF-8 (the common
/// case), avoiding an allocation on the ILP write hot path.
pub fn shard_key_from_ilp_cow<'a>(body: &'a [u8], tag_name: &str) -> Cow<'a, str> {
    if body.is_empty() {
        return Cow::Owned(tag_name.to_string());
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
                    let value = &tag[eq + 1..];
                    return match std::str::from_utf8(value) {
                        Ok(s) => Cow::Borrowed(s),
                        Err(_) => Cow::Owned(String::from_utf8_lossy(value).into_owned()),
                    };
                }
            }
        }
    }
    Cow::Owned(tag_name.to_string())
}

/// Owned-`String` convenience wrapper around [`shard_key_from_ilp_cow`].
pub fn shard_key_from_ilp(body: &[u8], tag_name: &str) -> String {
    shard_key_from_ilp_cow(body, tag_name).into_owned()
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
