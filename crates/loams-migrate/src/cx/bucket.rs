//! The bucket (S1, S2): immutable objects plus the metastore pointers that
//! name them. A migration writes new objects (never over an existing one)
//! and switches with one pointer CAS.

use bytes::Bytes;
use loams_common::NamespaceId;
use loams_common::meta::{Consistency, Fence, MetaError, MetaStore, Pointer};
use loams_store::{Store, StoreError};

use crate::migration::MigrationError;

/// See the module documentation.
#[derive(Clone, Copy, Debug)]
pub struct BucketCx<'a> {
    meta: &'a dyn MetaStore,
    store: &'a Store,
}

fn meta_error(e: MetaError) -> MigrationError {
    MigrationError::new("meta", e.to_string())
}

impl<'a> BucketCx<'a> {
    pub(crate) fn new(meta: &'a dyn MetaStore, store: &'a Store) -> Self {
        BucketCx { meta, store }
    }

    pub fn store(&self) -> &Store {
        self.store
    }

    /// The pointer `key` of `namespace`.
    pub async fn read_pointer(
        &self,
        namespace: NamespaceId,
        key: &str,
    ) -> Result<Option<Pointer>, MigrationError> {
        self.meta
            .pointer(Consistency::Linearizable, namespace, key)
            .await
            .map_err(meta_error)
    }

    pub async fn read_object(&self, path: &str) -> Result<Bytes, MigrationError> {
        self.store
            .get(path)
            .await
            .map(|(bytes, _)| bytes)
            .map_err(|e| MigrationError::new("bucket", format!("{path}: {e}")))
    }

    /// Writes a new object at `path`, never over another one. Writing the
    /// same bytes again (a resumed migration) succeeds; different bytes at
    /// an existing path are `object_exists`.
    pub async fn write_object_new(&self, path: &str, bytes: Bytes) -> Result<(), MigrationError> {
        match self.store.put_if_absent(path, bytes.clone()).await {
            Ok(_) => Ok(()),
            Err(StoreError::AlreadyExists { .. }) => {
                let existing = self.read_object(path).await?;
                if existing == bytes {
                    Ok(())
                } else {
                    Err(MigrationError::new(
                        "object_exists",
                        format!("{path} exists with other contents; objects are never rewritten"),
                    ))
                }
            }
            Err(e) => Err(MigrationError::new("bucket", format!("{path}: {e}"))),
        }
    }

    /// The switch: sets pointer `key` to `value` if it is still at
    /// `expected` (`None`: absent), as [`MetaCx::switch`](super::MetaCx::switch).
    pub async fn cas_pointer(
        &self,
        namespace: NamespaceId,
        key: &str,
        expected: Option<u64>,
        value: &str,
        fence: Option<Fence>,
    ) -> Result<u64, MigrationError> {
        super::MetaCx::new(self.meta)
            .switch(namespace, key, expected, value, fence)
            .await
    }
}
