//! The server's [`WsLookup`] (ruling T18.1, plan GT1 Task 25): workspaces
//! as the bucket's workspace store holds them.

use async_trait::async_trait;
use loams_store::Store;

use super::{BucketWorkspaceStore, DELETED_LABEL, WorkspaceStore, WsError, fork_sources};
use crate::format::pb;
use crate::ids::{NamespaceId, Principal, RepoId, WsId};
use crate::objects::{ObjectsError, WsLookup, WsView};

/// [`WsLookup`] over [`BucketWorkspaceStore`]: a workspace is its latest
/// snapshot's owner and packs, and the workspaces it was forked from
/// (ruling T24.6). A deleted workspace (its latest snapshot labelled
/// [`DELETED_LABEL`]) is not found.
#[derive(Clone, Debug)]
pub struct StoreWsLookup {
    store: Store,
}

impl StoreWsLookup {
    /// The workspaces in `store`.
    pub fn new(store: Store) -> Self {
        Self { store }
    }
}

/// What a read knows of a workspace whose latest snapshot is `latest`:
/// its owner, packs and fork sources (none when `latest` names no fork
/// origin, since every snapshot carries its workspace's).
pub async fn view_of(
    store: &dyn WorkspaceStore,
    repo: &RepoId,
    latest: &pb::WorkspaceSnapshot,
) -> Result<WsView, WsError> {
    let owner = Principal::parse(&latest.owner).map_err(|err| WsError::Corrupt {
        path: format!("workspace {}", latest.ws_id),
        reason: err.to_string(),
    })?;
    let ws = WsId::parse(&latest.ws_id).map_err(|err| WsError::Corrupt {
        path: format!("workspace {}", latest.ws_id),
        reason: err.to_string(),
    })?;
    let sources = if latest.fork.is_some() {
        fork_sources(store, repo, &ws).await?
    } else {
        Vec::new()
    };
    Ok(WsView {
        owner,
        packs: latest.packs.clone(),
        sources,
    })
}

#[async_trait]
impl WsLookup for StoreWsLookup {
    async fn workspace(
        &self,
        ns: NamespaceId,
        repo: &RepoId,
        ws: &WsId,
    ) -> Result<Option<WsView>, ObjectsError> {
        let store = BucketWorkspaceStore::new(self.store.clone(), ns);
        let latest = match store.latest(repo, ws).await {
            Ok(latest) => latest,
            Err(WsError::NotFound) => return Ok(None),
            Err(err) => return Err(ObjectsError::Lookup(err.to_string())),
        };
        if latest.label == DELETED_LABEL {
            return Ok(None);
        }
        view_of(&store, repo, &latest)
            .await
            .map(Some)
            .map_err(|err| ObjectsError::Lookup(err.to_string()))
    }
}
