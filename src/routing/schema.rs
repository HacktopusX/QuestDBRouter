use crate::config::TableRoutingConfig;
use std::borrow::Cow;
use std::collections::HashMap;

/// Registry of table routing metadata from config.
#[derive(Debug, Clone, Default)]
pub struct TableRegistry {
    tables: HashMap<String, TableRoutingConfig>,
}

impl TableRegistry {
    pub fn from_config(tables: &[TableRoutingConfig], default_shard_key: &str) -> Self {
        let mut map = HashMap::new();
        for t in tables {
            map.insert(t.name.to_ascii_lowercase(), t.clone());
        }
        let _ = default_shard_key;
        Self { tables: map }
    }

    pub fn get(&self, name: &str) -> Option<&TableRoutingConfig> {
        self.tables.get(&name.to_ascii_lowercase())
    }

    pub fn is_sharded(&self, name: &str) -> bool {
        self.get(name).map(|t| t.sharded).unwrap_or(true)
    }

    /// Shard-key column for `name`, borrowing from config when possible.
    pub fn shard_key_for<'a>(&'a self, name: &str, default: &'a str) -> Cow<'a, str> {
        self.get(name)
            .and_then(|t| t.shard_key.as_deref())
            .map(Cow::Borrowed)
            .unwrap_or(Cow::Borrowed(default))
    }
}
