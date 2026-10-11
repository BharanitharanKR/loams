//! Where a namespace's bytes come from: a store, the bucket name workers use for
//! it, and the cache in front of it.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use loams_cache::{CacheError, RangeCache, RangeCacheConfig};
use loams_store::{ObjectInfo, Store, StoreError};

/// The most object heads an [`Upstream`] remembers. Objects are immutable
/// (Iceberg files never change), so a remembered head never goes stale; the cap
/// only bounds memory, and the oldest half is dropped when it is reached.
pub const MAX_REMEMBERED_HEADS: usize = 100_000;

/// Store options: backend keys such as `aws_access_key_id`. They are
/// credentials, so they never print.
#[derive(Clone, Default)]
pub struct StoreOptions(pub Vec<(String, String)>);

impl fmt::Debug for StoreOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

/// Opening an upstream failed.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    /// The URL is not one this proxy can serve.
    #[error("the store URL {0:?} is not s3://, gs://, az://, file:// or memory://")]
    Url(String),
    /// The store could not be opened.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The cache could not be built.
    #[error(transparent)]
    Cache(#[from] CacheError),
}

/// One store as workers see it: `http://<proxy>/<bucket>/<prefix>/<key>`.
pub struct Upstream {
    bucket: String,
    prefix: String,
    store: Store,
    cache: RangeCache,
    heads: Mutex<HashMap<String, ObjectInfo>>,
}

impl fmt::Debug for Upstream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Upstream")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl Upstream {
    /// An upstream over `store` (already rooted at `prefix`) with `cache` in
    /// front of it. Workers name it as bucket `bucket`, and their keys start
    /// with `prefix/` (nothing when `prefix` is empty).
    pub fn new(bucket: &str, prefix: &str, store: Store, cache: RangeCache) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: prefix.trim_matches('/').to_string(),
            store,
            cache,
            heads: Mutex::new(HashMap::new()),
        }
    }

    /// Opens the store at `url` (`s3://bucket/prefix`, `gs://…`, `az://…`,
    /// `file:///dir`, `memory:///`) with `options`, credentials not given there
    /// coming from the front's environment, and a cache configured by `cache`.
    /// The bucket workers name is the URL's bucket (`local` for `file://` and
    /// `memory://`), and the prefix is the URL's path, so an Iceberg file's
    /// absolute `s3://bucket/prefix/…` location reads through the proxy as is.
    pub async fn open(
        url: &str,
        options: StoreOptions,
        cache: RangeCacheConfig,
    ) -> Result<Self, UpstreamError> {
        let (bucket, prefix) = bucket_and_prefix(url)?;
        let store = Store::from_url(url, options.0)?;
        let cache = RangeCache::new(store.clone(), cache).await?;
        Ok(Self::new(&bucket, &prefix, store, cache))
    }

    /// The bucket name workers use.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The key prefix every key workers send starts with (without its `/`).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The cache.
    pub fn cache(&self) -> &RangeCache {
        &self.cache
    }

    /// The store.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The store-relative path of a key workers sent, or `None` when the key is
    /// outside this upstream's prefix.
    pub fn relative<'k>(&self, key: &'k str) -> Option<&'k str> {
        if self.prefix.is_empty() {
            return Some(key);
        }
        key.strip_prefix(self.prefix.as_str())?.strip_prefix('/')
    }

    /// The object's head, remembered after the first lookup.
    pub async fn head(&self, path: &str) -> Result<ObjectInfo, StoreError> {
        if let Some(info) = self.locked().get(path) {
            return Ok(info.clone());
        }
        let info = self.store.head(path).await?;
        let mut heads = self.locked();
        if heads.len() >= MAX_REMEMBERED_HEADS {
            let drop: Vec<String> = heads
                .keys()
                .take(MAX_REMEMBERED_HEADS / 2)
                .cloned()
                .collect();
            for key in drop {
                heads.remove(&key);
            }
        }
        heads.insert(path.to_string(), info.clone());
        Ok(info)
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, HashMap<String, ObjectInfo>> {
        self.heads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The bucket and prefix of a store URL.
pub fn bucket_and_prefix(url: &str) -> Result<(String, String), UpstreamError> {
    let bad = || UpstreamError::Url(url.to_string());
    let (scheme, rest) = url.split_once("://").ok_or_else(bad)?;
    match scheme {
        "s3" | "s3a" | "gs" | "az" | "abfs" | "abfss" => {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(bad());
            }
            Ok((bucket.to_string(), prefix.trim_matches('/').to_string()))
        }
        "file" | "memory" => Ok(("local".to_string(), String::new())),
        _ => Err(bad()),
    }
}

/// Which upstream serves a namespace.
pub trait StoreResolver: Send + Sync + fmt::Debug {
    /// The namespace's upstream, or `None` when it has none.
    fn upstream(&self, namespace: &str) -> Option<Arc<Upstream>>;
}

/// One store for every namespace: `[house.store]` (each namespace confined to
/// its own `ns/<id>/` prefix by its [`crate::Grant`]).
#[derive(Clone, Debug)]
pub struct OneStore(pub Arc<Upstream>);

impl StoreResolver for OneStore {
    fn upstream(&self, _namespace: &str) -> Option<Arc<Upstream>> {
        Some(Arc::clone(&self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_and_prefixes_come_from_the_url() {
        let pair = |url| bucket_and_prefix(url).unwrap();
        assert_eq!(pair("s3://b"), ("b".into(), "".into()));
        assert_eq!(pair("s3://b/p/q/"), ("b".into(), "p/q".into()));
        assert_eq!(pair("file:///tmp/x"), ("local".into(), "".into()));
        assert!(bucket_and_prefix("http://x").is_err());
        assert!(bucket_and_prefix("s3:///p").is_err());
        assert_eq!(
            format!("{:?}", StoreOptions(vec![("k".into(), "secret".into())])),
            "[redacted]"
        );
    }
}
