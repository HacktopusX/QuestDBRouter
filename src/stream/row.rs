use memchr::memchr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IlpRow {
    pub measurement: String,
    pub tags: Vec<(String, String)>,
    pub fields: Vec<(String, IlpField)>,
    pub timestamp_ns: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IlpField {
    Float(f64),
    Integer(i64),
    Bool(bool),
    String(String),
}

/// Parse a single ILP line (without trailing newline) into an `IlpRow`.
pub fn parse_ilp_row(line: &[u8]) -> Option<IlpRow> {
    if line.is_empty() {
        return None;
    }

    let line = line.strip_suffix(b"\n").unwrap_or(line);
    if line.is_empty() {
        return None;
    }

    let (measurement, rest) = split_at_first(line, b',')?;
    if measurement.is_empty() {
        return None;
    }

    let measurement = String::from_utf8_lossy(measurement).into_owned();

    let (tags_section, fields_section) = match memchr(b' ', rest) {
        Some(space) => (&rest[..space], &rest[space + 1..]),
        None => (rest, &[][..]),
    };

    let mut tags = Vec::new();
    if !tags_section.is_empty() {
        for tag in tags_section.split(|b| *b == b',') {
            if let Some(eq) = memchr(b'=', tag) {
                let name = String::from_utf8_lossy(&tag[..eq]).into_owned();
                let value = String::from_utf8_lossy(&tag[eq + 1..]).into_owned();
                tags.push((name, value));
            }
        }
    }

    let (fields_bytes, timestamp_ns) = if fields_section.is_empty() {
        (fields_section, None)
    } else {
        let ts_start = fields_section
            .iter()
            .rposition(|b| *b == b' ')
            .map(|i| i + 1)
            .unwrap_or(fields_section.len());
        let ts = if ts_start < fields_section.len() {
            std::str::from_utf8(&fields_section[ts_start..])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
        } else {
            None
        };
        let fields_end = fields_section
            .iter()
            .rposition(|b| *b == b' ')
            .unwrap_or(fields_section.len());
        (&fields_section[..fields_end], ts)
    };

    let mut fields = Vec::new();
    if !fields_bytes.is_empty() {
        for field in fields_bytes.split(|b| *b == b',') {
            if let Some(eq) = memchr(b'=', field) {
                let name = String::from_utf8_lossy(&field[..eq]).into_owned();
                let raw = &field[eq + 1..];
                if let Some(parsed) = parse_field_value(raw) {
                    fields.push((name, parsed));
                }
            }
        }
    }

    Some(IlpRow {
        measurement,
        tags,
        fields,
        timestamp_ns,
    })
}

fn split_at_first(haystack: &[u8], needle: u8) -> Option<(&[u8], &[u8])> {
    memchr(needle, haystack).map(|i| (&haystack[..i], &haystack[i + 1..]))
}

fn parse_field_value(raw: &[u8]) -> Option<IlpField> {
    if raw.is_empty() {
        return None;
    }
    if raw[0] == b'"' {
        let inner = raw.get(1..raw.len().saturating_sub(1))?;
        return Some(IlpField::String(String::from_utf8_lossy(inner).into_owned()));
    }
    if raw.ends_with(b"i") && raw.len() > 1 {
        let s = std::str::from_utf8(&raw[..raw.len() - 1]).ok()?;
        return s.parse::<i64>().ok().map(IlpField::Integer);
    }
    let s = std::str::from_utf8(raw).ok()?;
    match s {
        "t" | "T" | "true" | "TRUE" => Some(IlpField::Bool(true)),
        "f" | "F" | "false" | "FALSE" => Some(IlpField::Bool(false)),
        _ => s.parse::<f64>().ok().map(IlpField::Float),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_line() {
        let line = b"router_test_trades,symbol=BTC-OHLCV price=100.5 1234567890\n";
        let row = parse_ilp_row(line).unwrap();
        assert_eq!(row.measurement, "router_test_trades");
        assert_eq!(row.tags, vec![("symbol".into(), "BTC-OHLCV".into())]);
        assert_eq!(row.fields.len(), 1);
        assert!(matches!(row.fields[0].1, IlpField::Float(v) if (v - 100.5).abs() < f64::EPSILON));
        assert_eq!(row.timestamp_ns, Some(1234567890));
    }

    #[test]
    fn parse_multiple_tags_and_fields() {
        let line = b"trades,symbol=ETH,exchange=NASDAQ price=1.5,volume=100i,active=t 999\n";
        let row = parse_ilp_row(line).unwrap();
        assert_eq!(row.measurement, "trades");
        assert_eq!(
            row.tags,
            vec![
                ("symbol".into(), "ETH".into()),
                ("exchange".into(), "NASDAQ".into()),
            ]
        );
        assert_eq!(row.fields.len(), 3);
        assert!(matches!(row.fields[0].1, IlpField::Float(v) if (v - 1.5).abs() < f64::EPSILON));
        assert!(matches!(row.fields[1].1, IlpField::Integer(100)));
        assert!(matches!(row.fields[2].1, IlpField::Bool(true)));
        assert_eq!(row.timestamp_ns, Some(999));
    }

    #[test]
    fn parse_string_field() {
        let line = b"events,kind=alert msg=\"hello world\" 1\n";
        let row = parse_ilp_row(line).unwrap();
        assert!(matches!(
            row.fields[0].1,
            IlpField::String(ref s) if s == "hello world"
        ));
    }

    #[test]
    fn parse_no_timestamp() {
        let line = b"trades,symbol=X price=1\n";
        let row = parse_ilp_row(line).unwrap();
        assert_eq!(row.timestamp_ns, None);
    }

    #[test]
    fn parse_empty_returns_none() {
        assert!(parse_ilp_row(b"").is_none());
        assert!(parse_ilp_row(b"\n").is_none());
    }
}
