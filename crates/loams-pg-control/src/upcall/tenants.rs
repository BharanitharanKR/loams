//! Which project a Neon tenant id is (PG2 Task 8), for `notify-attach`,
//! which names only the tenant.
//!
//! A tenant id is a one-way hash of the project id (Task 4), so it is
//! looked up, not inverted: a watch on the project records (`x/`) keeps a
//! map from each project key's tenant id to the key. It reads keys only
//! (`x/<ns>/<project_id>`; the name index `x/<ns>/n/` is skipped), and adds
//! no record (CP1: nothing new is keyed by a name).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures::StreamExt;

use crate::ids::{ProjectId, tenant_id};
use crate::model::ProjectKey;
use crate::store::{PgControlStore, StoreError, StoreEvent};

/// What a lookup found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Lookup {
    /// The watch has not sent its first full scan yet.
    NotSynced,
    Unknown,
    Found(ProjectKey),
}

#[derive(Debug, Default)]
struct Inner {
    synced: bool,
    by_tenant: HashMap<[u8; 16], ProjectKey>,
}

/// The tenant directory. [`follow`](Self::follow) feeds it.
#[derive(Debug, Default)]
pub(crate) struct Tenants {
    inner: RwLock<Inner>,
}

/// The project key `x/<ns>/<project_id>` names, if it is one.
fn project_key(key: &[u8]) -> Option<ProjectKey> {
    let rest = std::str::from_utf8(key).ok()?.strip_prefix("x/")?;
    let (namespace, id) = rest.split_once('/')?;
    if id.contains('/') {
        return None;
    }
    let project: ProjectId = id.parse().ok()?;
    Some(ProjectKey {
        namespace: namespace.to_owned(),
        id: project.to_string(),
    })
}

impl Tenants {
    /// Watches `store`'s projects into a new directory, until the returned
    /// task is aborted.
    ///
    /// # Errors
    ///
    /// The watch's (never for the fixed prefix `x/`).
    pub(crate) fn follow<S: PgControlStore>(
        store: &S,
    ) -> Result<(Arc<Self>, tokio::task::JoinHandle<()>), StoreError> {
        let tenants = Arc::new(Tenants::default());
        let mut events = store.watch(b"x/")?;
        let fed = Arc::downgrade(&tenants);
        let task = tokio::spawn(async move {
            while let Some(event) = events.next().await {
                let Some(tenants) = fed.upgrade() else {
                    return;
                };
                tenants.apply(&event);
            }
        });
        Ok((tenants, task))
    }

    fn apply(&self, event: &StoreEvent) {
        let mut inner = self.inner.write().unwrap_or_else(|p| p.into_inner());
        match event {
            StoreEvent::Synced => inner.synced = true,
            StoreEvent::Put { key, .. } => {
                if let Some(k) = project_key(key)
                    && let Ok(id) = k.id.parse::<ProjectId>()
                {
                    inner.by_tenant.insert(tenant_id(&id), k);
                }
            }
            StoreEvent::Delete { key } => {
                if let Some(k) = project_key(key)
                    && let Ok(id) = k.id.parse::<ProjectId>()
                {
                    inner.by_tenant.remove(&tenant_id(&id));
                }
            }
        }
    }

    pub(crate) fn find(&self, tenant: &[u8; 16]) -> Lookup {
        let inner = self.inner.read().unwrap_or_else(|p| p.into_inner());
        if !inner.synced {
            return Lookup::NotSynced;
        }
        inner
            .by_tenant
            .get(tenant)
            .cloned()
            .map_or(Lookup::Unknown, Lookup::Found)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_project_keys_count() {
        let id = ProjectId::new();
        let k = project_key(format!("x/acme/{id}").as_bytes()).expect("a project key");
        assert_eq!(k.namespace, "acme");
        assert_eq!(k.id, id.to_string());
        assert!(project_key(b"x/acme/n/shop").is_none());
        assert!(project_key(format!("X/{id}/br-1").as_bytes()).is_none());

        let t = Tenants::default();
        let put = StoreEvent::Put {
            key: format!("x/acme/{id}").into_bytes(),
            version: 1,
        };
        t.apply(&put);
        assert_eq!(t.find(&tenant_id(&id)), Lookup::NotSynced);
        t.apply(&StoreEvent::Synced);
        assert_eq!(t.find(&tenant_id(&id)), Lookup::Found(k));
        t.apply(&StoreEvent::Delete {
            key: format!("x/acme/{id}").into_bytes(),
        });
        assert_eq!(t.find(&tenant_id(&id)), Lookup::Unknown);
    }
}
