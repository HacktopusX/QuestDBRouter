use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectMeta};

use crate::config::RustFsConfig;

pub struct ObjectFetcher {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    prefix: String,
}

impl ObjectFetcher {
    pub fn from_config(config: &RustFsConfig) -> anyhow::Result<Self> {
        let endpoint = config
            .endpoint
            .as_deref()
            .context("ingest.rustfs.endpoint is required when ingest is enabled")?;
        let bucket = config
            .bucket
            .as_deref()
            .context("ingest.rustfs.bucket is required when ingest is enabled")?
            .to_string();
        let access_key = config
            .access_key
            .as_deref()
            .context("ingest.rustfs.access_key is required when ingest is enabled")?;
        let secret_key = config
            .secret_key
            .as_deref()
            .context("ingest.rustfs.secret_key is required when ingest is enabled")?;

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&bucket)
            .with_region(&config.region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_endpoint(endpoint)
            .with_allow_http(config.allow_http);

        // RustFS expects path-style addressing.
        builder = builder.with_virtual_hosted_style_request(false);

        let store = builder.build().context("build S3 object store")?;

        Ok(Self {
            store: Arc::new(store),
            bucket,
            prefix: config.prefix.clone(),
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub async fn get_object(&self, key: &str) -> anyhow::Result<(Vec<u8>, Option<String>)> {
        let path = ObjectPath::from(key);
        let result = self.store.get(&path).await.context("get object")?;
        let meta = result.meta.clone();
        let bytes = result.bytes().await.context("read object bytes")?.to_vec();
        Ok((bytes, meta.e_tag))
    }

    pub async fn list_feather_objects(&self) -> anyhow::Result<Vec<ObjectMeta>> {
        let prefix = ObjectPath::from(self.prefix.trim_end_matches('/'));
        let mut stream = self.store.list_with_offset(Some(&prefix), &ObjectPath::from(""));
        let mut objects = Vec::new();

        while let Some(item) = stream.next().await {
            let meta = item.context("list object meta")?;
            let key = meta.location.as_ref();
            if super::events::should_ingest_key(key) && super::events::matches_prefix(key, &self.prefix) {
                objects.push(meta);
            }
        }

        Ok(objects)
    }
}
