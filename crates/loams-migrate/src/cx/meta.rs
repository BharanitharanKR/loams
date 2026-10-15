//! The metastore (S3): scan a namespace's pointers and switch with one
//! pointer CAS, its all-or-nothing unit (ruling T0-2).

use loams_common::NamespaceId;
use loams_common::meta::{
    ApplyError, Consistency, Fence, MetaError, MetaStore, Pointer, PointerCas,
};

use crate::migration::MigrationError;

/// See the module documentation.
#[derive(Clone, Copy, Debug)]
pub struct MetaCx<'a> {
    meta: &'a dyn MetaStore,
}

impl<'a> MetaCx<'a> {
    pub(crate) fn new(meta: &'a dyn MetaStore) -> Self {
        MetaCx { meta }
    }

    pub fn store(&self) -> &dyn MetaStore {
        self.meta
    }

    /// Every pointer of `namespace` under `prefix`, in key order.
    pub async fn scan_prefix(
        &self,
        namespace: NamespaceId,
        prefix: &str,
    ) -> Result<Vec<(String, Pointer)>, MigrationError> {
        self.meta
            .pointers_with_prefix(Consistency::Linearizable, namespace, prefix)
            .await
            .map_err(|e| MigrationError::new("meta", e.to_string()))
    }

    /// Writes pointer `key` (absent before) beside the old keys.
    pub async fn write_new(
        &self,
        namespace: NamespaceId,
        key: &str,
        value: &str,
        fence: Option<Fence>,
    ) -> Result<(), MigrationError> {
        self.switch(namespace, key, None, value, fence)
            .await
            .map(|_| ())
    }

    /// The switch: one pointer CAS. All or nothing: a mismatch writes
    /// nothing (`pointer_moved`); a retry that finds its own value succeeds.
    pub async fn switch(
        &self,
        namespace: NamespaceId,
        key: &str,
        expected: Option<u64>,
        value: &str,
        fence: Option<Fence>,
    ) -> Result<u64, MigrationError> {
        let cas = PointerCas {
            namespace,
            key: key.to_string(),
            expected,
            value: value.to_string(),
            fence,
            fresh: None,
        };
        match self.meta.cas_pointer(cas).await.into_result() {
            Ok(version) => Ok(version),
            Err(MetaError::Rejected(ApplyError::VersionMismatch {
                current: Some(current),
            })) if current.value == value => Ok(current.version),
            Err(MetaError::Rejected(ApplyError::VersionMismatch { current })) => {
                Err(MigrationError::new(
                    "pointer_moved",
                    format!(
                        "{key} is at {:?}, not {expected:?}",
                        current.map(|p| p.version)
                    ),
                ))
            }
            Err(e) => Err(MigrationError::new("meta", e.to_string())),
        }
    }
}
