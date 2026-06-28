use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub event_name: String,
}

#[derive(Debug, Deserialize)]
struct S3NotificationEnvelope {
    #[serde(default, alias = "Records", alias = "records")]
    records: Vec<S3Record>,
    #[serde(default, alias = "EventName", alias = "event_name")]
    event_name: Option<String>,
    #[serde(default, alias = "Key", alias = "key")]
    key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct S3Record {
    #[serde(default, alias = "eventName", alias = "event_name")]
    event_name: Option<String>,
    #[serde(default)]
    s3: Option<S3Entity>,
}

#[derive(Debug, Deserialize)]
struct S3Entity {
    #[serde(default)]
    bucket: S3Bucket,
    #[serde(default)]
    object: S3Object,
}

#[derive(Debug, Default, Deserialize)]
struct S3Bucket {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Default, Deserialize)]
struct S3Object {
    #[serde(default)]
    key: String,
    #[serde(default, alias = "eTag", alias = "etag")]
    etag: Option<String>,
}

/// Parse RustFS / AWS S3 notification JSON into normalized object refs.
pub fn parse_notification(body: &str, default_bucket: &str) -> anyhow::Result<Vec<ObjectRef>> {
    let envelope: S3NotificationEnvelope = serde_json::from_str(body)?;

    let mut refs = Vec::new();

    if envelope.records.is_empty() {
        if let (Some(event_name), Some(key)) = (envelope.event_name, envelope.key) {
            if let Some(object) = normalize_key(&event_name, default_bucket, &key, None) {
                refs.push(object);
            }
        }
        return Ok(refs);
    }

    for record in envelope.records {
        let event_name = record.event_name.unwrap_or_default();
        if let Some(s3) = record.s3 {
            let bucket = if s3.bucket.name.is_empty() {
                default_bucket.to_string()
            } else {
                s3.bucket.name
            };
            let key = decode_object_key(&s3.object.key);
            if let Some(object) = normalize_key(&event_name, &bucket, &key, s3.object.etag) {
                refs.push(object);
            }
        }
    }

    Ok(refs)
}

fn decode_object_key(key: &str) -> String {
    key.replace("%20", " ").replace("+", " ")
}

fn normalize_key(
    event_name: &str,
    bucket: &str,
    key_or_full: &str,
    etag: Option<String>,
) -> Option<ObjectRef> {
    if !event_name.starts_with("s3:ObjectCreated:") {
        return None;
    }

    let (bucket, key) = if key_or_full.contains('/') && bucket.is_empty() {
        split_bucket_key(key_or_full)?
    } else if bucket.is_empty() {
        return None;
    } else {
        (bucket.to_string(), key_or_full.to_string())
    };

    if !should_ingest_key(&key) {
        return None;
    }

    Some(ObjectRef {
        bucket,
        key,
        etag,
        event_name: event_name.to_string(),
    })
}

fn split_bucket_key(full: &str) -> Option<(String, String)> {
    let (bucket, key) = full.split_once('/')?;
    Some((bucket.to_string(), key.to_string()))
}

pub fn should_ingest_key(key: &str) -> bool {
    if key.ends_with('/') {
        return false;
    }
    let lower = key.to_ascii_lowercase();
    if !(lower.ends_with(".feather") || lower.ends_with(".arrow")) {
        return false;
    }
    if lower.ends_with(".part") || lower.ends_with(".tmp") {
        return false;
    }
    true
}

pub fn matches_prefix(key: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    key.starts_with(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aws_records_shape() {
        let body = r#"{
            "Records": [{
                "eventName": "s3:ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "market-data"},
                    "object": {"key": "live/data/trades/BTCUSDT.BITUNIX/file.feather", "eTag": "abc"}
                }
            }]
        }"#;
        let refs = parse_notification(body, "market-data").unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].bucket, "market-data");
        assert!(refs[0].key.contains("file.feather"));
        assert_eq!(refs[0].etag.as_deref(), Some("abc"));
    }

    #[test]
    fn parses_rustfs_top_level_fallback() {
        let body = r#"{
            "EventName": "s3:ObjectCreated:Put",
            "Key": "market-data/live/data/trades/BTCUSDT.BITUNIX/x.feather"
        }"#;
        let refs = parse_notification(body, "market-data").unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].bucket, "market-data");
    }

    #[test]
    fn filters_non_feather_and_multipart() {
        assert!(!should_ingest_key("live/x.parquet"));
        assert!(!should_ingest_key("live/x.part"));
        assert!(should_ingest_key("live/x.feather"));
    }

    #[test]
    fn prefix_filter() {
        assert!(matches_prefix("live/a.feather", "live/"));
        assert!(!matches_prefix("archive/a.feather", "live/"));
    }
}
