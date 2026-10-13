//! The repository catalog (§36 §4.1, Ruling 21; plan GT1 Task 13, old GT1
//! Task 15).
//!
//! The catalog lives in the bucket, beside the repositories, and changes
//! only by create-only writes (ruling T0-6, R0.3: `file://` has no
//! conditional update, and local-first use has no metastore):
//!
//! ```text
//! ns/<ns>/repos/_names/<name>.json         the name record: {repo_id, format, parent, …}
//! ns/<ns>/repos/_deleting/<repo_id>.json   the deletion marker: {name, deleted_unix_ms}
//! ```
//!
//! - **Create** writes the new repository's checkpoint 0 under a fresh
//!   `repo_id`, then the name record with `put_if_absent`. The name is the
//!   commit point: a lost race leaves an orphan checkpoint under an id no
//!   record names (swept by GT1 Task 54), never a name without a
//!   repository. A record carries the digest of the idempotency key and of
//!   the request that wrote it, so a retry whose name write was undetermined
//!   reads the record back and answers it (`Replayed`); the same key with
//!   another request is `IdempotencyMismatch`, and another key is
//!   `AlreadyExists`.
//! - **Fork** is the same with [`fork_repo`]'s checkpoint: one checkpoint
//!   PUT plus the name. The record keeps the parent and the fork depth, so a
//!   fork past `MAX_FORK_DEPTH` is refused without reading the chain.
//! - **Delete** writes the create-only marker keyed by `repo_id`: the
//!   repository is hidden at once, its name stays taken (`Deleting`), and a
//!   marker can never name a later repository of the same name.
//! - **Purge** ([`BucketRepoCatalog::purge_deleted`]) runs after
//!   `GC_GRACE`: unless a record (active or deleting) names the repository
//!   as its fork parent, it deletes the repository's objects, then the name
//!   record (only while it still names this `repo_id`), then the marker. No
//!   record changes in place, so no step needs a compare-and-swap.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use loams_store::{Store, StoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::fork::fork_repo;
use crate::format::pb;
use crate::ids::{IdemKey, NamespaceId, RefName, RepoId, RepoName, Seq};
use crate::limits::MAX_FORK_DEPTH;
use crate::paths::RepoPaths;
use crate::reflog::{ReadAt, RefError, RefLog};
use crate::retry::retry;
use crate::state::{RepoState, StateError};
use crate::wal::{StoreWalStore, WalStore};

/// The version of the catalog's JSON documents.
const DOC_VERSION: u32 = 1;

/// Records read per listing page.
const LIST_PAGE: usize = 256;

/// Where a repository is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RepoLifecycle {
    /// Served.
    Active,
    /// Deleted and waiting to be purged.
    Deleting,
}

/// The repository a fork was made from.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ForkOrigin {
    /// The parent's name when the fork was made.
    pub name: RepoName,
    /// The parent.
    pub repo_id: RepoId,
    /// The parent's seq the fork reads.
    pub seq: Seq,
}

/// One repository as the catalog knows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoRecord {
    /// Its namespace.
    pub ns: NamespaceId,
    /// Its name.
    pub name: RepoName,
    /// Its immutable id; every object path uses it.
    pub repo_id: RepoId,
    /// SHA-1 only (Ruling 8).
    pub object_format: pb::ObjectFormat,
    /// When the name was taken.
    pub created_unix_ms: i64,
    /// Set on a fork.
    pub parent: Option<ForkOrigin>,
    /// Parents above it: 0 for a repository that is not a fork.
    pub fork_depth: u32,
    /// Active, or deleted and waiting to be purged.
    pub state: RepoLifecycle,
    /// When it was deleted.
    pub deleted_unix_ms: Option<i64>,
}

/// How a create or fork was answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Created {
    /// This call made the repository.
    New(RepoRecord),
    /// An earlier call with the same key and request made it.
    Replayed(RepoRecord),
}

impl Created {
    /// The repository, either way.
    pub fn record(&self) -> &RepoRecord {
        match self {
            Created::New(record) | Created::Replayed(record) => record,
        }
    }

    /// The repository, either way.
    pub fn into_record(self) -> RepoRecord {
        match self {
            Created::New(record) | Created::Replayed(record) => record,
        }
    }
}

/// The idempotency of a create or fork: the key and the digest of the
/// request it was sent with. `None` keys are never replayed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Idempotency {
    /// The caller's key.
    pub key: Option<IdemKey>,
    /// SHA-256 over the request's fields.
    pub request_digest: [u8; 32],
}

/// A repository to create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRepo {
    /// Its name.
    pub name: RepoName,
    /// SHA-1 only; anything else is `UnsupportedFormat`.
    pub object_format: pb::ObjectFormat,
    /// What `HEAD` points at.
    pub default_branch: RefName,
    /// The call's idempotency.
    pub idempotency: Idempotency,
}

/// A fork to make.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkRepo {
    /// The repository forked.
    pub source: RepoName,
    /// The fork's name.
    pub name: RepoName,
    /// The source's state to fork.
    pub at: ReadAt,
    /// The call's idempotency.
    pub idempotency: Idempotency,
}

/// What a purge sweep did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PurgeReport {
    /// Repositories whose objects, name and marker were deleted.
    pub purged: usize,
    /// Deleted repositories kept because a fork still reads them.
    pub waiting_for_forks: usize,
    /// Deleted repositories still inside their grace period.
    pub in_grace: usize,
}

/// Errors of the catalog.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// No repository has the name (or it was purged).
    #[error("repository not found")]
    NotFound,
    /// The name is taken by a repository another key created.
    #[error("a repository of that name exists")]
    AlreadyExists,
    /// The name's repository is deleted and waiting to be purged.
    #[error("the repository is being deleted")]
    Deleting,
    /// The key was used for another request.
    #[error("the idempotency key was used for another request")]
    IdempotencyMismatch,
    /// A fork of a repository already `MAX_FORK_DEPTH` forks deep.
    #[error("a fork may be at most {max} forks deep")]
    ForkTooDeep {
        /// `MAX_FORK_DEPTH`.
        max: usize,
    },
    /// The repository's state could not be read or written.
    #[error(transparent)]
    Ref(#[from] RefError),
    /// A catalog document that cannot be read.
    #[error("corrupt catalog document {path}: {reason}")]
    Corrupt {
        /// Its path.
        path: String,
        /// What is wrong.
        reason: String,
    },
    /// The object store failed (after retries, when transient).
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl From<StateError> for CatalogError {
    fn from(err: StateError) -> Self {
        CatalogError::Ref(RefError::State(err))
    }
}

/// The repository catalog of a bucket (Ruling 21).
#[async_trait]
pub trait RepoCatalog: Send + Sync + fmt::Debug {
    /// Creates an empty repository: checkpoint 0 (with `HEAD`), then the name.
    async fn create(&self, ns: NamespaceId, spec: CreateRepo) -> Result<Created, CatalogError>;
    /// Forks `spec.source`, whose ref log is `source`: one checkpoint PUT,
    /// then the name.
    async fn fork(
        &self,
        ns: NamespaceId,
        spec: ForkRepo,
        source: &dyn RefLog,
    ) -> Result<Created, CatalogError>;
    /// The repository named `name`, deleted or not; `None` when no record
    /// names it.
    async fn get(
        &self,
        ns: NamespaceId,
        name: &RepoName,
    ) -> Result<Option<RepoRecord>, CatalogError>;
    /// Up to `limit` active repositories whose names sort after `after`, by
    /// name, and whether more may follow.
    async fn list(
        &self,
        ns: NamespaceId,
        after: Option<&RepoName>,
        limit: usize,
    ) -> Result<(Vec<RepoRecord>, bool), CatalogError>;
    /// Marks `name` deleted at `now_unix_ms` (a create-only marker). A
    /// repeat answers the first marker's record.
    async fn mark_deleting(
        &self,
        ns: NamespaceId,
        name: &RepoName,
        now_unix_ms: i64,
    ) -> Result<RepoRecord, CatalogError>;
}

/// The name record as stored.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NameDoc {
    v: u32,
    repo_id: String,
    object_format: String,
    created_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<ParentDoc>,
    #[serde(default)]
    fork_depth: u32,
    /// Hex SHA-256 of the idempotency key; empty for an unkeyed call.
    #[serde(default)]
    idem_key: String,
    /// Hex SHA-256 of the request.
    #[serde(default)]
    request: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ParentDoc {
    name: String,
    repo_id: String,
    seq: u64,
}

/// The deletion marker as stored.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DeletingDoc {
    v: u32,
    name: String,
    deleted_unix_ms: i64,
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

fn key_digest(key: &Option<IdemKey>) -> String {
    key.as_ref().map_or_else(String::new, |key| {
        hex(&Sha256::digest(key.as_str().as_bytes()))
    })
}

fn format_name(format: pb::ObjectFormat) -> &'static str {
    match format {
        pb::ObjectFormat::Sha1 => "sha1",
        pb::ObjectFormat::Sha256 => "sha256",
        pb::ObjectFormat::Unspecified => "unspecified",
    }
}

/// Refuses every format but SHA-1 (Ruling 8).
fn require_sha1(format: pb::ObjectFormat) -> Result<(), CatalogError> {
    if format == pb::ObjectFormat::Sha1 {
        Ok(())
    } else {
        Err(StateError::UnsupportedFormat {
            format: format_name(format),
        }
        .into())
    }
}

/// The catalog over a bucket.
#[derive(Clone)]
pub struct BucketRepoCatalog {
    store: Store,
}

impl fmt::Debug for BucketRepoCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BucketRepoCatalog").finish_non_exhaustive()
    }
}

impl BucketRepoCatalog {
    /// The catalog of every namespace in `store`.
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// The store.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The WAL of repository `repo` in `ns`.
    pub fn wal(&self, ns: NamespaceId, repo: &RepoId) -> StoreWalStore {
        StoreWalStore::new(self.store.clone(), RepoPaths::new(ns, repo))
    }

    async fn read_json<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
    ) -> Result<Option<T>, CatalogError> {
        match retry(|| self.store.get(path)).await {
            Ok((bytes, _)) => {
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|err| CatalogError::Corrupt {
                        path: path.to_string(),
                        reason: err.to_string(),
                    })
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Writes `doc` at `path` unless an object is there: `Ok(true)` when
    /// this call wrote it.
    async fn create_json<T: Serialize>(&self, path: &str, doc: &T) -> Result<bool, CatalogError> {
        let bytes = Bytes::from(
            serde_json::to_vec(doc).map_err(|err| CatalogError::Corrupt {
                path: path.to_string(),
                reason: err.to_string(),
            })?,
        );
        match retry(|| self.store.put_if_absent(path, bytes.clone())).await {
            Ok(_) => Ok(true),
            Err(StoreError::AlreadyExists { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    async fn name_doc(
        &self,
        ns: NamespaceId,
        name: &RepoName,
    ) -> Result<Option<NameDoc>, CatalogError> {
        self.read_json(&RepoPaths::names_record(ns, name)).await
    }

    async fn deleting_doc(
        &self,
        ns: NamespaceId,
        repo: &RepoId,
    ) -> Result<Option<DeletingDoc>, CatalogError> {
        self.read_json(&RepoPaths::deleting_marker(ns, repo)).await
    }

    fn record_of(
        ns: NamespaceId,
        name: &RepoName,
        doc: &NameDoc,
        deleting: Option<&DeletingDoc>,
    ) -> Result<RepoRecord, CatalogError> {
        let path = RepoPaths::names_record(ns, name);
        let corrupt = |reason: String| CatalogError::Corrupt {
            path: path.clone(),
            reason,
        };
        if doc.v != DOC_VERSION {
            return Err(corrupt(format!("version {}", doc.v)));
        }
        let repo_id = RepoId::parse(&doc.repo_id).map_err(|err| corrupt(err.to_string()))?;
        let object_format = match doc.object_format.as_str() {
            "sha1" => pb::ObjectFormat::Sha1,
            other => return Err(corrupt(format!("object format {other:?}"))),
        };
        let parent = match &doc.parent {
            None => None,
            Some(parent) => Some(ForkOrigin {
                name: RepoName::parse(&parent.name).map_err(|err| corrupt(err.to_string()))?,
                repo_id: RepoId::parse(&parent.repo_id).map_err(|err| corrupt(err.to_string()))?,
                seq: Seq(parent.seq),
            }),
        };
        Ok(RepoRecord {
            ns,
            name: name.clone(),
            repo_id,
            object_format,
            created_unix_ms: doc.created_unix_ms,
            parent,
            fork_depth: doc.fork_depth,
            state: if deleting.is_some() {
                RepoLifecycle::Deleting
            } else {
                RepoLifecycle::Active
            },
            deleted_unix_ms: deleting.map(|d| d.deleted_unix_ms),
        })
    }

    /// The answer to a create or fork whose name is taken by `doc`.
    async fn taken(
        &self,
        ns: NamespaceId,
        name: &RepoName,
        doc: &NameDoc,
        idempotency: &Idempotency,
    ) -> Result<Created, CatalogError> {
        let record = self.record(ns, name, doc).await?;
        if record.state == RepoLifecycle::Deleting {
            return Err(CatalogError::Deleting);
        }
        let key = key_digest(&idempotency.key);
        if key.is_empty() || doc.idem_key != key {
            return Err(CatalogError::AlreadyExists);
        }
        if doc.request != hex(&idempotency.request_digest) {
            return Err(CatalogError::IdempotencyMismatch);
        }
        Ok(Created::Replayed(record))
    }

    async fn record(
        &self,
        ns: NamespaceId,
        name: &RepoName,
        doc: &NameDoc,
    ) -> Result<RepoRecord, CatalogError> {
        let repo_id = RepoId::parse(&doc.repo_id).map_err(|err| CatalogError::Corrupt {
            path: RepoPaths::names_record(ns, name),
            reason: err.to_string(),
        })?;
        let deleting = self.deleting_doc(ns, &repo_id).await?;
        Self::record_of(ns, name, doc, deleting.as_ref())
    }

    /// Takes `name` for `doc` (the commit point of a create or fork).
    async fn take_name(
        &self,
        ns: NamespaceId,
        name: &RepoName,
        doc: NameDoc,
        idempotency: &Idempotency,
    ) -> Result<Created, CatalogError> {
        let path = RepoPaths::names_record(ns, name);
        if self.create_json(&path, &doc).await? {
            return Ok(Created::New(Self::record_of(ns, name, &doc, None)?));
        }
        let existing = self.name_doc(ns, name).await?.ok_or_else(|| {
            // Created and purged between the PUT and the GET: the caller
            // retries.
            CatalogError::Store(StoreError::NotFound { path: path.clone() })
        })?;
        self.taken(ns, name, &existing, idempotency).await
    }

    fn new_doc(
        repo: &RepoId,
        now_unix_ms: i64,
        parent: Option<ParentDoc>,
        fork_depth: u32,
        idempotency: &Idempotency,
    ) -> NameDoc {
        NameDoc {
            v: DOC_VERSION,
            repo_id: repo.as_str().to_string(),
            object_format: "sha1".to_string(),
            created_unix_ms: now_unix_ms,
            parent,
            fork_depth,
            idem_key: key_digest(&idempotency.key),
            request: hex(&idempotency.request_digest),
        }
    }

    /// Every record of `ns` (active and deleting), keyed by name. A listing
    /// for purges, which must see every fork.
    async fn all_records(&self, ns: NamespaceId) -> Result<Vec<RepoRecord>, CatalogError> {
        let deleting = self.deleting_ids(ns).await?;
        let mut out = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let page = self
                .store
                .list_page(&RepoPaths::names_prefix(ns), after.as_deref(), LIST_PAGE)
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(last.path.clone());
            for info in &page {
                if let Some(record) = self.listed(ns, &info.path, &deleting).await? {
                    out.push(record);
                }
            }
        }
        Ok(out)
    }

    /// The ids and markers of every repository of `ns` marked deleted.
    async fn deleting_ids(
        &self,
        ns: NamespaceId,
    ) -> Result<BTreeMap<RepoId, DeletingDoc>, CatalogError> {
        let mut out = BTreeMap::new();
        for info in self.store.list(&RepoPaths::deleting_prefix(ns)).await? {
            let Some(repo) = RepoPaths::parse_deleting_marker(&info.path) else {
                continue;
            };
            if let Some(doc) = self.read_json::<DeletingDoc>(&info.path).await? {
                out.insert(repo, doc);
            }
        }
        Ok(out)
    }

    /// The record a listed name path holds; `None` for a path that is not a
    /// name record, or one deleted since it was listed.
    async fn listed(
        &self,
        ns: NamespaceId,
        path: &str,
        deleting: &BTreeMap<RepoId, DeletingDoc>,
    ) -> Result<Option<RepoRecord>, CatalogError> {
        let Some(name) = RepoPaths::parse_names_record(path) else {
            return Ok(None);
        };
        let Some(doc) = self.read_json::<NameDoc>(path).await? else {
            return Ok(None);
        };
        let marker = RepoId::parse(&doc.repo_id)
            .ok()
            .and_then(|id| deleting.get(&id));
        Self::record_of(ns, &name, &doc, marker).map(Some)
    }

    /// Purges every repository of `ns` deleted at least `grace` before
    /// `now_unix_ms` that no fork reads: its objects, then its name record
    /// (while it still names it), then its marker.
    pub async fn purge_deleted(
        &self,
        ns: NamespaceId,
        now_unix_ms: i64,
        grace: Duration,
    ) -> Result<PurgeReport, CatalogError> {
        let mut report = PurgeReport::default();
        let deleting = self.deleting_ids(ns).await?;
        if deleting.is_empty() {
            return Ok(report);
        }
        let records = self.all_records(ns).await?;
        let parents: HashSet<RepoId> = records
            .iter()
            .filter_map(|record| record.parent.as_ref().map(|p| p.repo_id.clone()))
            .collect();
        let grace_ms = i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
        for (repo, marker) in &deleting {
            if marker.deleted_unix_ms.saturating_add(grace_ms) > now_unix_ms {
                report.in_grace += 1;
                continue;
            }
            if parents.contains(repo) {
                report.waiting_for_forks += 1;
                continue;
            }
            self.purge_one(ns, repo, marker).await?;
            report.purged += 1;
        }
        Ok(report)
    }

    async fn purge_one(
        &self,
        ns: NamespaceId,
        repo: &RepoId,
        marker: &DeletingDoc,
    ) -> Result<(), CatalogError> {
        let prefix = format!("{}/", RepoPaths::new(ns, repo).root());
        loop {
            let page = self.store.list_page(&prefix, None, LIST_PAGE).await?;
            if page.is_empty() {
                break;
            }
            for info in page {
                match retry(|| self.store.delete(&info.path)).await {
                    Ok(()) | Err(StoreError::NotFound { .. }) => {}
                    Err(err) => return Err(err.into()),
                }
            }
        }
        if let Ok(name) = RepoName::parse(&marker.name)
            && let Some(doc) = self.name_doc(ns, &name).await?
            && doc.repo_id == repo.as_str()
        {
            let path = RepoPaths::names_record(ns, &name);
            match retry(|| self.store.delete(&path)).await {
                Ok(()) | Err(StoreError::NotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
        }
        let path = RepoPaths::deleting_marker(ns, repo);
        match retry(|| self.store.delete(&path)).await {
            Ok(()) | Err(StoreError::NotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }
        tracing::info!(%ns, %repo, "purged a deleted repository");
        Ok(())
    }
}

/// Wall-clock milliseconds, for the catalog's timestamps.
fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[async_trait]
impl RepoCatalog for BucketRepoCatalog {
    async fn create(&self, ns: NamespaceId, spec: CreateRepo) -> Result<Created, CatalogError> {
        require_sha1(spec.object_format)?;
        // A taken name is answered without writing an orphan checkpoint.
        if let Some(doc) = self.name_doc(ns, &spec.name).await? {
            return self.taken(ns, &spec.name, &doc, &spec.idempotency).await;
        }
        let repo = RepoId::generate();
        let mut state = RepoState::empty(spec.object_format);
        state
            .symrefs
            .insert("HEAD".to_string(), spec.default_branch.clone());
        self.wal(ns, &repo)
            .put_checkpoint(&state.to_checkpoint(0))
            .await
            .map_err(RefError::from)?;
        let doc = Self::new_doc(&repo, now_unix_ms(), None, 0, &spec.idempotency);
        self.take_name(ns, &spec.name, doc, &spec.idempotency).await
    }

    async fn fork(
        &self,
        ns: NamespaceId,
        spec: ForkRepo,
        source: &dyn RefLog,
    ) -> Result<Created, CatalogError> {
        if let Some(doc) = self.name_doc(ns, &spec.name).await? {
            return self.taken(ns, &spec.name, &doc, &spec.idempotency).await;
        }
        let parent = self
            .get(ns, &spec.source)
            .await?
            .ok_or(CatalogError::NotFound)?;
        if parent.state == RepoLifecycle::Deleting {
            return Err(CatalogError::Deleting);
        }
        let depth = parent.fork_depth + 1;
        if depth as usize > MAX_FORK_DEPTH {
            return Err(CatalogError::ForkTooDeep {
                max: MAX_FORK_DEPTH,
            });
        }
        let repo = RepoId::generate();
        let seq = fork_repo(source, &self.wal(ns, &repo), &parent.repo_id, spec.at).await?;
        let origin = ParentDoc {
            name: parent.name.as_str().to_string(),
            repo_id: parent.repo_id.as_str().to_string(),
            seq: seq.0,
        };
        let doc = Self::new_doc(&repo, now_unix_ms(), Some(origin), depth, &spec.idempotency);
        self.take_name(ns, &spec.name, doc, &spec.idempotency).await
    }

    async fn get(
        &self,
        ns: NamespaceId,
        name: &RepoName,
    ) -> Result<Option<RepoRecord>, CatalogError> {
        match self.name_doc(ns, name).await? {
            Some(doc) => self.record(ns, name, &doc).await.map(Some),
            None => Ok(None),
        }
    }

    async fn list(
        &self,
        ns: NamespaceId,
        after: Option<&RepoName>,
        limit: usize,
    ) -> Result<(Vec<RepoRecord>, bool), CatalogError> {
        let deleting = self.deleting_ids(ns).await?;
        let mut out = Vec::new();
        let mut cursor = after.map(|name| RepoPaths::names_record(ns, name));
        loop {
            let want = LIST_PAGE.max(limit + 1);
            let page = self
                .store
                .list_page(&RepoPaths::names_prefix(ns), cursor.as_deref(), want)
                .await?;
            let exhausted = page.len() < want;
            for info in &page {
                cursor = Some(info.path.clone());
                let Some(record) = self.listed(ns, &info.path, &deleting).await? else {
                    continue;
                };
                if record.state == RepoLifecycle::Deleting {
                    continue;
                }
                if out.len() == limit {
                    return Ok((out, true));
                }
                out.push(record);
            }
            if exhausted {
                return Ok((out, false));
            }
        }
    }

    async fn mark_deleting(
        &self,
        ns: NamespaceId,
        name: &RepoName,
        now_unix_ms: i64,
    ) -> Result<RepoRecord, CatalogError> {
        let doc = self
            .name_doc(ns, name)
            .await?
            .ok_or(CatalogError::NotFound)?;
        let record = self.record(ns, name, &doc).await?;
        if record.state == RepoLifecycle::Deleting {
            return Ok(record);
        }
        let marker = DeletingDoc {
            v: DOC_VERSION,
            name: name.as_str().to_string(),
            deleted_unix_ms: now_unix_ms,
        };
        let path = RepoPaths::deleting_marker(ns, &record.repo_id);
        self.create_json(&path, &marker).await?;
        // Ours or a racing delete's: the stored marker is the answer.
        let stored = self.deleting_doc(ns, &record.repo_id).await?;
        Self::record_of(ns, name, &doc, Some(stored.as_ref().unwrap_or(&marker)))
    }
}
