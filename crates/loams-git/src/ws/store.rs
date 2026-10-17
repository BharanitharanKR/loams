//! [`BucketWorkspaceStore`] and [`MemWorkspaceStore`] (§36 §19.4.1–§19.4.4;
//! plan GT1 Task 23).
//!
//! The bucket store follows `StoreWalStore`'s patterns:
//!
//! - **Snapshots** `ws/<ws_id>/snap/<n:020>.lws` are create-only. A refused
//!   PUT, or one whose outcome is unknown and whose resend is refused, reads
//!   the snapshot back: the same bytes are this writer's own (Committed),
//!   anything else fences it.
//! - **Names** `_ws_names/<owner>/<name>.json` (`{ws_id, created_unix_ms}`)
//!   are create-only and written before snapshot 0, so a lost name race
//!   writes nothing else. A refused name write reads the record back: the
//!   same `ws_id` is a replay.
//! - **The hint** `ws/<ws_id>/head` (`{n, written_unix_ms}`) only speeds
//!   readers up: [`WorkspaceStore::latest`] probes forward from it, and lists
//!   the snapshot names when it is absent, unreadable or names a collected
//!   snapshot.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use loams_store::{Store, StoreError};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{
    CreateWs, SnapAppended, WorkspaceStore, WsError, WsSummary, appended_ids, fork_zero, is_replay,
    snapshot_zero,
};
use crate::clock::{Clock, SystemClock};
use crate::format::pb;
use crate::format::snapshot::{decode_snapshot, encode_snapshot};
use crate::ids::{MountId, NamespaceId, Principal, RepoId, SnapN, WsId, WsName};
use crate::paths::RepoPaths;
use crate::retry::{self, is_transient, pause};

/// PUTs of one create-only object before an unknown outcome is returned as
/// an error.
const PUT_ATTEMPTS: usize = 5;
/// Object names per LIST page.
const LIST_PAGE: usize = 1000;
/// Name records read at once while listing an owner's workspaces.
const NAME_READS: usize = 16;
/// Listings [`WorkspaceStore::latest`] makes when the snapshot it found
/// disappears under it (retention).
const LIST_ROUNDS: usize = 3;
/// Sorts after every name inside a workspace's prefix (`head`, `packs/`,
/// `salvage/`, `snap/`), so listing after `ws/<ws_id>/~` skips to the next
/// workspace.
const PAST_WORKSPACE: &str = "~";

/// The JSON form of a name record (§19.4.1).
#[derive(Debug, Serialize, Deserialize)]
struct NameJson {
    ws_id: String,
    created_unix_ms: i64,
}

/// The JSON form of the `head` hint (§19.4.1).
#[derive(Debug, Serialize, Deserialize)]
struct HintJson {
    n: u64,
    written_unix_ms: i64,
}

/// How a create-only PUT ended.
enum Put {
    /// This call wrote the object.
    Written,
    /// The object exists: written by an earlier attempt of this call, or by
    /// another writer. The caller reads it back.
    Exists,
}

/// [`WorkspaceStore`] over a `loams_store::Store`, for the repositories of
/// one namespace (§36 §19.4.1's layout).
#[derive(Clone, Debug)]
pub struct BucketWorkspaceStore {
    store: Store,
    ns: NamespaceId,
    clock: Arc<dyn Clock>,
}

impl BucketWorkspaceStore {
    /// The workspaces of namespace `ns` in `store`, stamping new workspaces
    /// with the system clock.
    pub fn new(store: Store, ns: NamespaceId) -> Self {
        Self {
            store,
            ns,
            clock: Arc::new(SystemClock),
        }
    }

    /// Stamps new workspaces and hints with `clock` instead.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    fn paths(&self, repo: &RepoId) -> RepoPaths {
        RepoPaths::new(self.ns, repo)
    }

    /// A create-only PUT of `bytes`, resent after a transient error. A
    /// resend of a write that landed is refused as existing, which the
    /// caller resolves by reading back.
    async fn put_new(&self, path: &str, bytes: Bytes) -> Result<Put, WsError> {
        let mut attempt = 0;
        loop {
            match self.store.put_if_absent(path, bytes.clone()).await {
                Ok(_) => return Ok(Put::Written),
                Err(StoreError::AlreadyExists { .. }) => return Ok(Put::Exists),
                Err(err) if is_transient(&err) && attempt + 1 < PUT_ATTEMPTS => {
                    tracing::debug!(path, attempt, error = %err, "resending a create-only workspace object");
                    pause(attempt).await;
                    attempt += 1;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    /// The bytes at `path`, or `None` when absent.
    async fn read_raw(&self, path: &str) -> Result<Option<Bytes>, WsError> {
        match retry::retry(|| self.store.get(path)).await {
            Ok((bytes, _)) => Ok(Some(bytes)),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Snapshot `n` of `ws`, checked to be the snapshot its path names.
    async fn read_snapshot(
        &self,
        paths: &RepoPaths,
        ws: &WsId,
        n: SnapN,
    ) -> Result<Option<Arc<pb::WorkspaceSnapshot>>, WsError> {
        let path = paths.ws_snapshot(ws, n);
        let Some(bytes) = self.read_raw(&path).await? else {
            return Ok(None);
        };
        decode_named(&path, &bytes, ws, n).map(|snap| Some(Arc::new(snap)))
    }

    /// The `head` hint, or `None` when there is none or it is unreadable.
    async fn hint(&self, paths: &RepoPaths, ws: &WsId) -> Result<Option<SnapN>, WsError> {
        let path = paths.ws_head(ws);
        let Some(bytes) = self.read_raw(&path).await? else {
            return Ok(None);
        };
        match serde_json::from_slice::<HintJson>(&bytes) {
            Ok(json) => Ok(Some(SnapN(json.n))),
            Err(_) => {
                tracing::warn!(path, "ignoring an unreadable workspace hint");
                Ok(None)
            }
        }
    }

    /// The greatest snapshot number in a LIST of `snap/`.
    async fn list_last(&self, paths: &RepoPaths, ws: &WsId) -> Result<Option<SnapN>, WsError> {
        let prefix = paths.ws_snapshot_prefix(ws);
        let mut last = None;
        let mut after: Option<String> = None;
        loop {
            let page =
                retry::retry(|| self.store.list_page(&prefix, after.as_deref(), LIST_PAGE)).await?;
            for info in &page {
                if let Some((_, n)) = RepoPaths::parse_ws_snapshot(&info.path) {
                    last = Some(n);
                }
            }
            if page.len() < LIST_PAGE {
                return Ok(last);
            }
            after = page.last().map(|info| info.path.clone());
        }
    }

    /// Takes `name` for `ws`: create-only, and a replay when the record
    /// already names `ws`.
    async fn claim_name(
        &self,
        paths: &RepoPaths,
        owner: &Principal,
        name: &WsName,
        ws: &WsId,
    ) -> Result<(), WsError> {
        let path = paths.ws_name(owner, name);
        let doc = NameJson {
            ws_id: ws.to_string(),
            created_unix_ms: self.clock.now_unix_ms(),
        };
        let body = Bytes::from(serde_json::to_vec(&doc).expect("a name record serializes"));
        if let Put::Written = self.put_new(&path, body).await? {
            return Ok(());
        }
        let stored = self
            .read_name(&path)
            .await?
            .ok_or_else(|| WsError::corrupt(&path, "refused as existing but not returned"))?;
        if stored.ws_id == *ws {
            Ok(())
        } else {
            Err(WsError::WorkspaceNameTaken {
                name: name.clone(),
                holder: stored.ws_id,
            })
        }
    }

    /// The name record at `path`.
    async fn read_name(&self, path: &str) -> Result<Option<NameRecord>, WsError> {
        let Some(bytes) = self.read_raw(path).await? else {
            return Ok(None);
        };
        let json: NameJson =
            serde_json::from_slice(&bytes).map_err(|err| WsError::corrupt(path, err))?;
        let ws_id = WsId::parse(&json.ws_id).map_err(|err| WsError::corrupt(path, err))?;
        Ok(Some(NameRecord {
            ws_id,
            created_unix_ms: json.created_unix_ms,
        }))
    }

    /// Writes snapshot 0 (create and fork): a replay answers the stored one.
    async fn put_zero(
        &self,
        paths: &RepoPaths,
        spec: &CreateWs,
        snap: pb::WorkspaceSnapshot,
    ) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
        let bytes = encode_snapshot(&snap).map_err(WsError::from_format)?;
        // An id that already names a workspace is answered before any name
        // is claimed, so a conflicting spec never leaves a name record.
        if let Some(existing) = self.read_snapshot(paths, &spec.ws_id, SnapN::ZERO).await? {
            return replay_or_conflict(existing, &snap, spec);
        }
        if let Some(name) = &spec.name {
            self.claim_name(paths, &spec.owner, name, &spec.ws_id)
                .await?;
        }
        let path = paths.ws_snapshot(&spec.ws_id, SnapN::ZERO);
        if let Put::Written = self.put_new(&path, bytes).await? {
            return Ok(Arc::new(snap));
        }
        let existing = self
            .read_snapshot(paths, &spec.ws_id, SnapN::ZERO)
            .await?
            .ok_or_else(|| WsError::corrupt(&path, "refused as existing but not returned"))?;
        replay_or_conflict(existing, &snap, spec)
    }

    /// A page of `owner`'s named workspaces.
    async fn list_named(
        &self,
        paths: &RepoPaths,
        owner: &Principal,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WsSummary>, WsError> {
        let prefix = paths.ws_names_prefix(owner);
        let after = after.map(|name| format!("{prefix}{name}.json"));
        let page = retry::retry(|| self.store.list_page(&prefix, after.as_deref(), limit)).await?;
        let names: Vec<(WsName, String)> = page
            .into_iter()
            .filter_map(|info| Some((RepoPaths::parse_ws_name(&info.path)?, info.path)))
            .collect();
        stream::iter(names)
            .map(|(name, path)| async move {
                // A record purged since the LIST is left out.
                Ok::<_, WsError>(self.read_name(&path).await?.map(|record| WsSummary {
                    ws_id: record.ws_id,
                    name: Some(name),
                    created_unix_ms: Some(record.created_unix_ms),
                }))
            })
            .buffered(NAME_READS)
            .try_filter_map(|summary| async move { Ok(summary) })
            .try_collect()
            .await
    }

    /// A page of every workspace of the repository, from a LIST of `ws/`
    /// that skips past each workspace's objects once it has seen one.
    async fn list_all(
        &self,
        paths: &RepoPaths,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WsSummary>, WsError> {
        let prefix = paths.workspaces_prefix();
        let skip = |ws: &str| format!("{prefix}{ws}/{PAST_WORKSPACE}");
        let mut after = after.map(skip);
        let mut out = Vec::new();
        while out.len() < limit {
            let page =
                retry::retry(|| self.store.list_page(&prefix, after.as_deref(), LIST_PAGE)).await?;
            let mut last: Option<&str> = None;
            for info in &page {
                let Some(ws) = info
                    .path
                    .strip_prefix(&prefix)
                    .and_then(|rest| rest.split_once('/'))
                    .map(|(ws, _)| ws)
                else {
                    continue;
                };
                if last == Some(ws) {
                    continue;
                }
                last = Some(ws);
                if let Ok(ws_id) = WsId::parse(ws) {
                    out.push(WsSummary {
                        ws_id,
                        name: None,
                        created_unix_ms: None,
                    });
                    if out.len() == limit {
                        break;
                    }
                }
            }
            if page.len() < LIST_PAGE && out.len() < limit {
                break;
            }
            after = match last {
                Some(ws) => Some(skip(ws)),
                None => page.last().map(|info| info.path.clone()),
            };
        }
        Ok(out)
    }
}

/// The stored snapshot 0 when `offered` replays it; `IdConflict` otherwise.
fn replay_or_conflict(
    existing: Arc<pb::WorkspaceSnapshot>,
    offered: &pb::WorkspaceSnapshot,
    spec: &CreateWs,
) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
    if is_replay(&existing, offered) {
        Ok(existing)
    } else {
        Err(WsError::IdConflict {
            ws_id: spec.ws_id.clone(),
        })
    }
}

/// A name record, read.
struct NameRecord {
    ws_id: WsId,
    created_unix_ms: i64,
}

/// Decodes the snapshot at `path`, which names snapshot `n` of `ws`.
fn decode_named(
    path: &str,
    bytes: &[u8],
    ws: &WsId,
    n: SnapN,
) -> Result<pb::WorkspaceSnapshot, WsError> {
    let snap = decode_snapshot(bytes).map_err(|err| WsError::corrupt(path, err))?;
    if snap.n != n.0 || snap.ws_id != ws.as_str() {
        return Err(WsError::corrupt(
            path,
            format!("holds snapshot {}@{}", snap.ws_id, snap.n),
        ));
    }
    Ok(snap)
}

#[async_trait]
impl WorkspaceStore for BucketWorkspaceStore {
    async fn create(&self, spec: CreateWs) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
        let snap = snapshot_zero(&spec, self.clock.now_unix_ms())?;
        self.put_zero(&self.paths(&spec.repo), &spec, snap).await
    }

    async fn latest(
        &self,
        repo: &RepoId,
        ws: &WsId,
    ) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
        let paths = self.paths(repo);
        let mut current = match self.hint(&paths, ws).await? {
            Some(n) => self.read_snapshot(&paths, ws, n).await?,
            None => None,
        };
        let mut rounds = 0;
        while current.is_none() {
            // No hint, or the snapshot it names was collected: list.
            let Some(n) = self.list_last(&paths, ws).await? else {
                return Err(WsError::NotFound);
            };
            current = self.read_snapshot(&paths, ws, n).await?;
            rounds += 1;
            if current.is_none() && rounds >= LIST_ROUNDS {
                return Err(WsError::corrupt(
                    &paths.ws_snapshot(ws, n),
                    "listed but not returned",
                ));
            }
        }
        let mut current = current.expect("set by the loop");
        // Probe forward until a number is absent: the GET that misses is
        // issued after the call started, so the answer is linearizable.
        while let Some(next) = self
            .read_snapshot(&paths, ws, SnapN(current.n).next())
            .await?
        {
            current = next;
        }
        Ok(current)
    }

    async fn get(
        &self,
        repo: &RepoId,
        ws: &WsId,
        n: SnapN,
    ) -> Result<Option<Arc<pb::WorkspaceSnapshot>>, WsError> {
        self.read_snapshot(&self.paths(repo), ws, n).await
    }

    async fn append(&self, snap: &pb::WorkspaceSnapshot) -> Result<SnapAppended, WsError> {
        let (repo, ws, n) = appended_ids(snap)?;
        // Encoded once, so every resend writes the same bytes.
        let bytes = encode_snapshot(snap).map_err(WsError::from_format)?;
        let paths = self.paths(&repo);
        let path = paths.ws_snapshot(&ws, n);
        if let Put::Written = self.put_new(&path, bytes.clone()).await? {
            return Ok(SnapAppended::Committed { n });
        }
        // Refused: by this writer's earlier attempt (the same bytes), or by
        // another writer, which fences this one.
        let stored = self
            .read_raw(&path)
            .await?
            .ok_or_else(|| WsError::corrupt(&path, "refused as existing but not returned"))?;
        if stored == bytes {
            return Ok(SnapAppended::Committed { n });
        }
        let existing = decode_named(&path, &stored, &ws, n)?;
        Ok(SnapAppended::Fenced {
            existing: Arc::new(existing),
        })
    }

    async fn salvage(
        &self,
        snap: &pb::WorkspaceSnapshot,
        mount: &MountId,
    ) -> Result<Ulid, WsError> {
        let repo = RepoId::parse(&snap.repo_id).map_err(WsError::invalid)?;
        let ws = WsId::parse(&snap.ws_id).map_err(WsError::invalid)?;
        let bytes = encode_snapshot(snap).map_err(WsError::from_format)?;
        let id = Ulid::generate();
        let path = self.paths(&repo).ws_salvage(&ws, mount, id);
        if let Put::Exists = self.put_new(&path, bytes.clone()).await?
            && self.read_raw(&path).await?.as_ref() != Some(&bytes)
        {
            return Err(WsError::corrupt(&path, "another salvage holds this id"));
        }
        Ok(id)
    }

    async fn list(
        &self,
        repo: &RepoId,
        owner: Option<&Principal>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WsSummary>, WsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let paths = self.paths(repo);
        match owner {
            Some(owner) => self.list_named(&paths, owner, after, limit).await,
            None => self.list_all(&paths, after, limit).await,
        }
    }

    async fn fork(
        &self,
        from: (&RepoId, &WsId, SnapN),
        spec: CreateWs,
    ) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
        let paths = self.paths(&spec.repo);
        // A replay must not depend on the source still holding snapshot n.
        if let Some(existing) = self.read_snapshot(&paths, &spec.ws_id, SnapN::ZERO).await? {
            return replayed_fork(existing, from, &spec);
        }
        let source = self
            .read_snapshot(&self.paths(from.0), from.1, from.2)
            .await?
            .ok_or(WsError::NotFound)?;
        let snap = fork_zero(&source, from, &spec, self.clock.now_unix_ms())?;
        self.put_zero(&paths, &spec, snap).await
    }

    async fn publish_hint(&self, repo: &RepoId, ws: &WsId, n: SnapN) -> Result<(), WsError> {
        let json = HintJson {
            n: n.0,
            written_unix_ms: self.clock.now_unix_ms(),
        };
        let bytes = serde_json::to_vec(&json).expect("a hint serializes");
        self.store
            .put(&self.paths(repo).ws_head(ws), bytes.into())
            .await?;
        Ok(())
    }
}

/// Answers a fork whose snapshot 0 is already stored: the same fork (a
/// replay), or `IdConflict`.
fn replayed_fork(
    existing: Arc<pb::WorkspaceSnapshot>,
    from: (&RepoId, &WsId, SnapN),
    spec: &CreateWs,
) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
    let origin = pb::ForkOrigin {
        repo_id: from.0.to_string(),
        ws_id: from.1.to_string(),
        n: from.2.0,
    };
    let same = existing.repo_id == spec.repo.as_str()
        && existing.owner == spec.owner.as_str()
        && existing.name == spec.name.as_ref().map_or("", |name| name.as_str())
        && existing.fork.as_ref() == Some(&origin);
    if same {
        Ok(existing)
    } else {
        Err(WsError::IdConflict {
            ws_id: spec.ws_id.clone(),
        })
    }
}

/// The state of a [`MemWorkspaceStore`].
#[derive(Debug, Default)]
struct MemState {
    /// (repository, workspace) → n → snapshot.
    snaps: BTreeMap<(String, String), BTreeMap<u64, Arc<pb::WorkspaceSnapshot>>>,
    /// (repository, owner, `<name>.json`) → (workspace, created): keyed as
    /// the bucket names records, so listings come in the same order.
    names: BTreeMap<(String, String, String), (WsId, i64)>,
    /// (repository, workspace, mount, salvage id) → snapshot.
    salvage: HashMap<(String, String, String, Ulid), Arc<pb::WorkspaceSnapshot>>,
}

/// [`WorkspaceStore`] in memory, for tests and the in-process scale tests:
/// the bucket store's rules without a bucket. It keeps no hint (`latest` is
/// the greatest snapshot), and nothing fails.
#[derive(Debug)]
pub struct MemWorkspaceStore {
    state: Mutex<MemState>,
    clock: Arc<dyn Clock>,
}

impl Default for MemWorkspaceStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemWorkspaceStore {
    /// An empty store on the system clock.
    pub fn new() -> Self {
        Self {
            state: Mutex::default(),
            clock: Arc::new(SystemClock),
        }
    }

    /// Stamps new workspaces with `clock` instead.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    fn state(&self) -> std::sync::MutexGuard<'_, MemState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Writes snapshot 0 (create and fork) under one lock, as the bucket
    /// store does with two create-only PUTs.
    fn put_zero(
        &self,
        spec: &CreateWs,
        snap: pb::WorkspaceSnapshot,
    ) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
        encode_snapshot(&snap).map_err(WsError::from_format)?;
        let repo = spec.repo.to_string();
        let ws = spec.ws_id.to_string();
        let mut state = self.state();
        if let Some(existing) = state
            .snaps
            .get(&(repo.clone(), ws.clone()))
            .and_then(|chain| chain.get(&0))
        {
            return replay_or_conflict(existing.clone(), &snap, spec);
        }
        if let Some(name) = &spec.name {
            let key = (repo.clone(), spec.owner.to_string(), format!("{name}.json"));
            let (holder, _) = state
                .names
                .entry(key)
                .or_insert_with(|| (spec.ws_id.clone(), self.clock.now_unix_ms()));
            if *holder != spec.ws_id {
                return Err(WsError::WorkspaceNameTaken {
                    name: name.clone(),
                    holder: holder.clone(),
                });
            }
        }
        let snap = Arc::new(snap);
        state
            .snaps
            .entry((repo, ws))
            .or_default()
            .insert(0, snap.clone());
        Ok(snap)
    }
}

#[async_trait]
impl WorkspaceStore for MemWorkspaceStore {
    async fn create(&self, spec: CreateWs) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
        let snap = snapshot_zero(&spec, self.clock.now_unix_ms())?;
        self.put_zero(&spec, snap)
    }

    async fn latest(
        &self,
        repo: &RepoId,
        ws: &WsId,
    ) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
        self.state()
            .snaps
            .get(&(repo.to_string(), ws.to_string()))
            .and_then(|chain| chain.values().next_back().cloned())
            .ok_or(WsError::NotFound)
    }

    async fn get(
        &self,
        repo: &RepoId,
        ws: &WsId,
        n: SnapN,
    ) -> Result<Option<Arc<pb::WorkspaceSnapshot>>, WsError> {
        Ok(self
            .state()
            .snaps
            .get(&(repo.to_string(), ws.to_string()))
            .and_then(|chain| chain.get(&n.0).cloned()))
    }

    async fn append(&self, snap: &pb::WorkspaceSnapshot) -> Result<SnapAppended, WsError> {
        let (repo, ws, n) = appended_ids(snap)?;
        encode_snapshot(snap).map_err(WsError::from_format)?;
        let mut state = self.state();
        let chain = state
            .snaps
            .entry((repo.to_string(), ws.to_string()))
            .or_default();
        match chain.get(&n.0) {
            Some(existing) if **existing == *snap => Ok(SnapAppended::Committed { n }),
            Some(existing) => Ok(SnapAppended::Fenced {
                existing: existing.clone(),
            }),
            None => {
                chain.insert(n.0, Arc::new(snap.clone()));
                Ok(SnapAppended::Committed { n })
            }
        }
    }

    async fn salvage(
        &self,
        snap: &pb::WorkspaceSnapshot,
        mount: &MountId,
    ) -> Result<Ulid, WsError> {
        let repo = RepoId::parse(&snap.repo_id).map_err(WsError::invalid)?;
        let ws = WsId::parse(&snap.ws_id).map_err(WsError::invalid)?;
        encode_snapshot(snap).map_err(WsError::from_format)?;
        let id = Ulid::generate();
        self.state().salvage.insert(
            (repo.to_string(), ws.to_string(), mount.to_string(), id),
            Arc::new(snap.clone()),
        );
        Ok(id)
    }

    async fn list(
        &self,
        repo: &RepoId,
        owner: Option<&Principal>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WsSummary>, WsError> {
        let repo = repo.to_string();
        let state = self.state();
        Ok(match owner {
            Some(owner) => {
                let owner = owner.to_string();
                let lower = match after {
                    Some(name) => {
                        Bound::Excluded((repo.clone(), owner.clone(), format!("{name}.json")))
                    }
                    None => Bound::Included((repo.clone(), owner.clone(), String::new())),
                };
                state
                    .names
                    .range((lower, Bound::Unbounded))
                    .take_while(|((r, o, _), _)| *r == repo && *o == owner)
                    .take(limit)
                    .filter_map(|((_, _, file), (ws_id, created))| {
                        let name = WsName::parse(file.strip_suffix(".json")?).ok()?;
                        Some(WsSummary {
                            ws_id: ws_id.clone(),
                            name: Some(name),
                            created_unix_ms: Some(*created),
                        })
                    })
                    .collect()
            }
            None => {
                let lower = match after {
                    Some(ws) => Bound::Excluded((repo.clone(), ws.to_string())),
                    None => Bound::Included((repo.clone(), String::new())),
                };
                state
                    .snaps
                    .range((lower, Bound::Unbounded))
                    .take_while(|((r, _), _)| *r == repo)
                    .take(limit)
                    .filter_map(|((_, ws), _)| {
                        Some(WsSummary {
                            ws_id: WsId::parse(ws).ok()?,
                            name: None,
                            created_unix_ms: None,
                        })
                    })
                    .collect()
            }
        })
    }

    async fn fork(
        &self,
        from: (&RepoId, &WsId, SnapN),
        spec: CreateWs,
    ) -> Result<Arc<pb::WorkspaceSnapshot>, WsError> {
        if let Some(existing) = self.get(&spec.repo, &spec.ws_id, SnapN::ZERO).await? {
            return replayed_fork(existing, from, &spec);
        }
        let source = self
            .get(from.0, from.1, from.2)
            .await?
            .ok_or(WsError::NotFound)?;
        let snap = fork_zero(&source, from, &spec, self.clock.now_unix_ms())?;
        self.put_zero(&spec, snap)
    }

    async fn publish_hint(&self, _: &RepoId, _: &WsId, _: SnapN) -> Result<(), WsError> {
        Ok(())
    }
}
