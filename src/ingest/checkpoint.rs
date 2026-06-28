use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectKey {
    pub bucket: String,
    pub key: String,
}

impl ObjectKey {
    fn encode(bucket: &str, key: &str) -> String {
        format!("{bucket}\0{key}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedMeta {
    pub etag: Option<String>,
    pub processed_at: DateTime<Utc>,
    pub rows: u64,
}

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct CheckpointStore {
    path: PathBuf,
    entries: HashMap<String, ProcessedMeta>,
}

impl CheckpointStore {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, CheckpointError> {
        let path = path.as_ref().to_path_buf();
        let entries = if path.exists() {
            let data = std::fs::read_to_string(&path)?;
            if data.trim().is_empty() {
                HashMap::new()
            } else {
                serde_json::from_str(&data)?
            }
        } else {
            HashMap::new()
        };
        Ok(Self { path, entries })
    }

    pub fn should_process(&self, bucket: &str, key: &str, etag: Option<&str>) -> bool {
        let object = ObjectKey::encode(bucket, key);
        match self.entries.get(&object) {
            None => true,
            Some(meta) => match (meta.etag.as_deref(), etag) {
                (Some(stored), Some(current)) => stored != current,
                _ => true,
            },
        }
    }

    pub fn mark_processed(
        &mut self,
        bucket: &str,
        key: &str,
        etag: Option<String>,
        rows: u64,
    ) -> Result<(), CheckpointError> {
        self.entries.insert(
            ObjectKey::encode(bucket, key),
            ProcessedMeta {
                etag,
                processed_at: Utc::now(),
                rows,
            },
        );
        self.persist()
    }

    pub fn contains(&self, bucket: &str, key: &str) -> bool {
        self.entries
            .contains_key(&ObjectKey::encode(bucket, key))
    }

    fn persist(&self) -> Result<(), CheckpointError> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let tmp = self.path.with_extension("tmp");
        let data = serde_json::to_string_pretty(&self.entries)?;
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_checkpoint() -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("quest-router-checkpoint-{nanos}.json"))
    }

    #[test]
    fn skip_when_etag_unchanged() {
        let path = temp_checkpoint();
        let mut store = CheckpointStore::load(&path).unwrap();
        store
            .mark_processed("b", "k", Some("etag1".into()), 10)
            .unwrap();
        assert!(!store.should_process("b", "k", Some("etag1")));
        assert!(store.should_process("b", "k", Some("etag2")));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn atomic_persist_roundtrip() {
        let path = temp_checkpoint();
        {
            let mut store = CheckpointStore::load(&path).unwrap();
            store
                .mark_processed("bucket", "obj.feather", None, 5)
                .unwrap();
        }
        let store = CheckpointStore::load(&path).unwrap();
        assert!(store.contains("bucket", "obj.feather"));
        let _ = std::fs::remove_file(path);
    }
}
