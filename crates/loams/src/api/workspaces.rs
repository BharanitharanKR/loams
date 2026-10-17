// The generated service traits return `impl Encodable<_>`; these impls name the concrete message
// type, which is the intended refinement (as `connect.rs` does).
#![allow(refining_impl_trait)]

//! `loams.repos.v1.WorkspaceService` on the main port (Loams Git, GT1 Task 25; design §36
//! §19.4, §19.10, D826, D827, D836).
//!
//! With the `git` feature workspaces are served over the bucket's workspace store
//! (`loams_git::ws::BucketWorkspaceStore`): create, fork, get, list, delete, the snapshots,
//! `SaveSnapshot` (fenced on `n`, after checking the objects it names), `WatchWorkspace`, and
//! the salvage of fenced writers. Access follows GT-Q13's default (Q750): whoever may read the
//! repository (the org role `member`) reads its workspaces, unless the server limits workspace
//! reads to owners (`--git-workspace-reads-owner`); writing one needs its owner, an org admin or
//! the operator. Without the feature every RPC answers `feature_not_in_variant`.

use std::sync::Arc;

use connectrpc::{
    ConnectError, RequestContext, Router, ServiceRequest, ServiceResult, ServiceStream,
};
use loams_proto::loams::repos::v1 as pb;
use loams_proto::loams::repos::v1::{WorkspaceService, WorkspaceServiceExt};

use super::AppState;
use super::repos::{PACKAGE, SERVED};

/// Registers `WorkspaceService`: served with the `git` feature and a git runtime, else the stub.
pub(super) fn register(router: Router, state: &AppState) -> Router {
    #[cfg(feature = "git")]
    if let Some(git) = &state.git {
        return Arc::new(served::WorkspacesServed {
            repos: super::repos::served::ReposServed {
                meta: Arc::clone(&state.meta),
                git: Arc::clone(git),
                principals: state.principals.clone(),
            },
        })
        .register(router);
    }
    let _ = state;
    Arc::new(WorkspacesAbsent).register(router)
}

#[cfg(feature = "git")]
pub(crate) use served::{may_read_workspace, may_write_workspace};

/// `WorkspaceService` when this binary does not serve it.
#[derive(Debug)]
struct WorkspacesAbsent;

fn absent(method: &str) -> ConnectError {
    let rpc = format!("{PACKAGE}.WorkspaceService/{method}");
    if SERVED {
        super::connect::refuse(
            connectrpc::ErrorCode::Unavailable,
            "unavailable",
            format!("{rpc} is not served by this node"),
            &[],
        )
    } else {
        super::connect::not_in_variant(&rpc)
    }
}

impl WorkspaceService for WorkspacesAbsent {
    async fn create_workspace(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::CreateWorkspaceRequest>,
    ) -> ServiceResult<pb::CreateWorkspaceResponse> {
        Err(absent("CreateWorkspace"))
    }

    async fn get_workspace(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::GetWorkspaceRequest>,
    ) -> ServiceResult<pb::GetWorkspaceResponse> {
        Err(absent("GetWorkspace"))
    }

    async fn list_workspaces(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ListWorkspacesRequest>,
    ) -> ServiceResult<pb::ListWorkspacesResponse> {
        Err(absent("ListWorkspaces"))
    }

    async fn fork_workspace(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ForkWorkspaceRequest>,
    ) -> ServiceResult<pb::ForkWorkspaceResponse> {
        Err(absent("ForkWorkspace"))
    }

    async fn delete_workspace(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::DeleteWorkspaceRequest>,
    ) -> ServiceResult<pb::DeleteWorkspaceResponse> {
        Err(absent("DeleteWorkspace"))
    }

    async fn list_snapshots(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ListSnapshotsRequest>,
    ) -> ServiceResult<pb::ListSnapshotsResponse> {
        Err(absent("ListSnapshots"))
    }

    async fn get_snapshot(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::GetSnapshotRequest>,
    ) -> ServiceResult<pb::GetSnapshotResponse> {
        Err(absent("GetSnapshot"))
    }

    async fn save_snapshot(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::SaveSnapshotRequest>,
    ) -> ServiceResult<pb::SaveSnapshotResponse> {
        Err(absent("SaveSnapshot"))
    }

    async fn watch_workspace(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::WatchWorkspaceRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchWorkspaceResponse>> {
        Err(absent("WatchWorkspace"))
    }

    async fn list_salvage(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ListSalvageRequest>,
    ) -> ServiceResult<pb::ListSalvageResponse> {
        Err(absent("ListSalvage"))
    }

    async fn restore_salvage(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::RestoreSalvageRequest>,
    ) -> ServiceResult<pb::RestoreSalvageResponse> {
        Err(absent("RestoreSalvage"))
    }

    async fn read_files(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ReadFilesRequest>,
    ) -> ServiceResult<ServiceStream<pb::ReadFilesResponse>> {
        Err(absent("ReadFiles"))
    }

    async fn apply_edits(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ApplyEditsRequest>,
    ) -> ServiceResult<pb::ApplyEditsResponse> {
        Err(absent("ApplyEdits"))
    }

    async fn diff(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::DiffRequest>,
    ) -> ServiceResult<ServiceStream<pb::DiffResponse>> {
        Err(absent("Diff"))
    }

    async fn rebase(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::RebaseRequest>,
    ) -> ServiceResult<pb::RebaseResponse> {
        Err(absent("Rebase"))
    }
}

#[cfg(feature = "git")]
mod served {
    use std::sync::Arc;
    use std::time::Duration;

    use buffa::{EnumValue, MessageField};
    use connectrpc::{
        ConnectError, ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult,
        ServiceStream,
    };
    use futures::{StreamExt, TryStreamExt};
    use loams_git::catalog::RepoRecord;
    use loams_git::format::pb as gitpb;
    use loams_git::format::snapshot::snapshot_commit;
    use loams_git::history::{Pathspec, TreeReader};
    use loams_git::limits::{
        HINT_EVERY, MAX_EDITS, MAX_FENCE_RETRIES, MAX_FORK_DEPTH, MAX_READ_PATHS,
    };
    use loams_git::objects::{Kind, WsView};
    use loams_git::odb::RangeOdb;
    use loams_git::ws::{
        BucketWorkspaceStore, DELETED_LABEL, EditError, FileEdit, FileMode, MergeLabels, NoEnv,
        NoScan, ReplayError, SalvageRef, SaveCheckError, SnapAppended, WorkspaceStore, WsBase,
        WsError, WsPackBlobs, WsPackError, apply_edits, changed_paths, check_save_objects,
        commit_tree, empty_tree, file_patch, fork_sources, put_workspace_pack, read_file,
        rebase_trees, replay_salvage, seal_snapshot, view_of, write_ws_pack,
    };
    use loams_git::{
        BlobId, CreateWs, GitAccess, MountId, Principal, RefName, RefSnapshot, RepoPath, RepoPaths,
        SnapN, StoreBlobStore, WsId, WsName,
    };
    use loams_proto::loams::repos::v1 as pb;
    use loams_proto::loams::repos::v1::__buffa::oneof::watch_workspace_response;
    use loams_proto::loams::repos::v1::WorkspaceService;
    use sha2::{Digest, Sha256};
    use tokio::sync::{broadcast, mpsc};
    use ulid::Ulid;

    use crate::api::connect::refuse;
    use crate::api::objects::{
        internal, odb_error, open_odb, pack_pb, parse_oid, workspace_not_found, ws_pack_error,
    };
    use crate::api::repos::served::{
        ReposServed, idem_key, invalid, key_reused, now_ms, page_size, repo_name, timestamp,
        unavailable,
    };

    /// `ListWorkspaces`', `ListSnapshots`' and `ListSalvage`'s page size when the request names
    /// none, and the largest they accept.
    const PAGE: (usize, usize) = (100, 1000);
    /// Snapshot and salvage records a listing reads at once.
    const LIST_READS: usize = 16;
    /// Messages a `WatchWorkspace` stream buffers before its producer waits.
    const WATCH_BUFFER: usize = 16;
    /// How often a `WatchWorkspace` stream looks for saves made through other nodes.
    const WATCH_POLL: Duration = HINT_EVERY;
    /// Messages a `ReadFiles` or `Diff` stream buffers before its producer waits.
    const STREAM_BUFFER: usize = 16;
    /// The most file or patch bytes in one `ReadFiles` or `Diff` message (ruling T26.4).
    const CHUNK_BYTES: usize = 1024 * 1024;

    /// `WorkspaceService`, served.
    #[derive(Debug)]
    pub(super) struct WorkspacesServed {
        pub(super) repos: ReposServed,
    }

    /// The caller of an RPC; `unauthenticated` without one.
    fn caller(ctx: &RequestContext) -> Result<&crate::auth::Principal, ConnectError> {
        ctx.extensions()
            .get::<crate::auth::Principal>()
            .ok_or_else(|| {
                refuse(
                    ErrorCode::Unauthenticated,
                    crate::auth::reason::TOKEN_MISSING,
                    "this RPC needs a credential",
                    &[],
                )
            })
    }

    /// The caller as a workspace owner and snapshot principal.
    fn caller_principal(ctx: &RequestContext) -> Result<Principal, ConnectError> {
        let subject = &caller(ctx)?.subject;
        Principal::parse(subject).map_err(|err| {
            refuse(
                ErrorCode::PermissionDenied,
                "permission_denied",
                format!("this credential cannot own a workspace: {err}"),
                &[],
            )
        })
    }

    /// Whether the caller is an org admin or the operator.
    async fn is_admin(repos: &ReposServed, ctx: &RequestContext) -> Result<bool, ConnectError> {
        let principal = caller(ctx)?;
        Ok(
            crate::api::git_auth::authorize_caller(&repos.principals, principal, GitAccess::Admin)
                .await
                .is_ok(),
        )
    }

    fn denied(what: &str) -> ConnectError {
        refuse(
            ErrorCode::PermissionDenied,
            "permission_denied",
            format!("only the workspace's owner or an org admin may {what} it"),
            &[],
        )
    }

    /// Refuses a caller who may not write a workspace owned by `owner` (Q750's default): its
    /// owner, an org admin and the operator may (ruling T24.5).
    pub(crate) async fn may_write_workspace(
        repos: &ReposServed,
        ctx: &RequestContext,
        owner: &str,
    ) -> Result<(), ConnectError> {
        if caller(ctx)?.subject == owner || is_admin(repos, ctx).await? {
            return Ok(());
        }
        Err(denied("write"))
    }

    /// Refuses a caller who may not read a workspace owned by `owner`: anyone who reads the
    /// repository (GT-Q13's default), or, on a server that limits workspace reads to owners,
    /// as for writing.
    pub(crate) async fn may_read_workspace(
        repos: &ReposServed,
        ctx: &RequestContext,
        owner: &str,
    ) -> Result<(), ConnectError> {
        if !repos.git.ws_reads_owner_only
            || caller(ctx)?.subject == owner
            || is_admin(repos, ctx).await?
        {
            return Ok(());
        }
        Err(denied("read"))
    }

    fn ws_error(err: WsError) -> ConnectError {
        match err {
            WsError::NotFound => refuse(
                ErrorCode::NotFound,
                "workspace_not_found",
                "no such workspace or snapshot",
                &[],
            ),
            WsError::WorkspaceNameTaken { name, holder } => refuse(
                ErrorCode::AlreadyExists,
                "workspace_name_taken",
                format!("you have a workspace named {name:?}"),
                &[
                    ("workspace_name", name.to_string()),
                    ("workspace", holder.to_string()),
                ],
            ),
            WsError::IdConflict { .. } => key_reused(),
            WsError::Invalid { reason } => invalid("snapshot", reason),
            WsError::TooLarge => invalid("snapshot", "the snapshot is over 1 MiB"),
            err @ WsError::Corrupt { .. } => internal("a corrupt workspace object", &err),
            WsError::Store(err) => unavailable("the workspace store", &err),
        }
    }

    fn fork_too_deep() -> ConnectError {
        refuse(
            ErrorCode::FailedPrecondition,
            "fork_too_deep",
            format!("a workspace may be forked at most {MAX_FORK_DEPTH} deep"),
            &[("max", MAX_FORK_DEPTH.to_string())],
        )
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// An object id field: 40 lowercase hex digits, or empty when `optional`.
    fn oid_field(field: &str, value: &str, optional: bool) -> Result<Vec<u8>, ConnectError> {
        if value.is_empty() && optional {
            return Ok(Vec::new());
        }
        Ok(parse_oid(field, value)?.as_bytes().to_vec())
    }

    fn ws_id(value: &str) -> Result<WsId, ConnectError> {
        WsId::parse(value).map_err(|err| invalid("workspace", err.to_string()))
    }

    fn writer(value: &str) -> Result<Option<MountId>, ConnectError> {
        if value.is_empty() {
            return Ok(None);
        }
        MountId::parse(value)
            .map(Some)
            .map_err(|err| invalid("writer", err.to_string()))
    }

    fn ws_name(value: &str) -> Result<Option<WsName>, ConnectError> {
        if value.is_empty() {
            return Ok(None);
        }
        WsName::parse(value)
            .map(Some)
            .map_err(|err| invalid("workspace_name", err.to_string()))
    }

    /// A workspace id for a create or fork: derived from the caller, the repository and the
    /// idempotency key, so a retry names the same workspace (the store is idempotent on it);
    /// generated without a key.
    fn new_ws_id(rpc: &str, record: &RepoRecord, owner: &Principal, key: Option<&str>) -> WsId {
        let Some(key) = key else {
            return WsId::generate();
        };
        let digest = Sha256::digest(
            format!("{rpc}\0{}\0{owner}\0{key}", record.repo_id.as_str()).as_bytes(),
        );
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        WsId::from_ulid(Ulid::from_bytes(bytes))
    }

    fn change_kind(kind: i32) -> pb::WorkspaceChangeKind {
        use gitpb::changed_path::Kind as K;
        match K::try_from(kind) {
            Ok(K::Modified) => pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_MODIFIED,
            Ok(K::Added) => pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_ADDED,
            Ok(K::Deleted) => pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_DELETED,
            Ok(K::Untracked) => pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_UNTRACKED,
            Ok(K::TypeChanged) => pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_TYPE_CHANGED,
            Err(_) => pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_UNSPECIFIED,
        }
    }

    fn change_kind_from(kind: &EnumValue<pb::WorkspaceChangeKind>) -> Result<i32, ConnectError> {
        use gitpb::changed_path::Kind as K;
        let kind = match kind {
            EnumValue::Known(pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_MODIFIED) => {
                K::Modified
            }
            EnumValue::Known(pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_ADDED) => K::Added,
            EnumValue::Known(pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_DELETED) => K::Deleted,
            EnumValue::Known(pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_UNTRACKED) => {
                K::Untracked
            }
            EnumValue::Known(pb::WorkspaceChangeKind::WORKSPACE_CHANGE_KIND_TYPE_CHANGED) => {
                K::TypeChanged
            }
            _ => return Err(invalid("snapshot.changed", "a change needs a known kind")),
        };
        Ok(kind as i32)
    }

    /// A stored snapshot as the API answers it.
    fn snapshot_pb(snap: &gitpb::WorkspaceSnapshot) -> pb::Snapshot {
        pb::Snapshot {
            workspace: snap.ws_id.clone(),
            n: snap.n,
            owner: snap.owner.clone(),
            workspace_name: snap.name.clone(),
            track: snap.track.clone(),
            base: hex(&snap.base),
            base_seq: snap.base_seq,
            head: hex(&snap.head),
            head_ref: snap.head_ref.clone(),
            refs: snap
                .refs
                .iter()
                .map(|r| pb::LocalRef {
                    name: r.name.clone(),
                    oid: hex(&r.oid),
                    upstream: r.upstream.clone(),
                    ..Default::default()
                })
                .collect(),
            index_tree: hex(&snap.index_tree),
            worktree_tree: hex(&snap.worktree_tree),
            snapshot_commit: hex(&snap.snapshot_commit),
            changed: snap
                .changed
                .iter()
                .map(|c| pb::WorkspaceChange {
                    path: c.path.clone(),
                    kind: EnumValue::Known(change_kind(c.kind)),
                    staged: c.staged,
                    ..Default::default()
                })
                .collect(),
            changed_overflow: snap.changed_overflow,
            packs: snap.packs.iter().map(pack_pb).collect(),
            writer: snap.writer.clone(),
            principal: snap.principal.clone(),
            created_at: MessageField::some(timestamp(snap.created_unix_ms)),
            label: snap.label.clone(),
            takeover: match &snap.takeover {
                Some(t) => MessageField::some(pb::Takeover {
                    from_mount: t.from_mount.clone(),
                    from_n: t.from_n,
                    reason: t.reason.clone(),
                    ..Default::default()
                }),
                None => MessageField::none(),
            },
            fork: match &snap.fork {
                Some(f) => MessageField::some(pb::WorkspaceFork {
                    workspace: f.ws_id.clone(),
                    n: f.n,
                    ..Default::default()
                }),
                None => MessageField::none(),
            },
            conflicts: snap
                .conflicts
                .iter()
                .map(|c| pb::WorkspaceConflict {
                    path: c.path.clone(),
                    base: hex(&c.base),
                    ours: hex(&c.ours),
                    theirs: hex(&c.theirs),
                    mode: c.mode,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The client's part of an offered snapshot, on top of `latest`'s identity (the
    /// workspace, repository, owner, name and fork origin are the server's).
    fn offered_snapshot(
        latest: &gitpb::WorkspaceSnapshot,
        offered: &pb::Snapshot,
        principal: &Principal,
    ) -> Result<gitpb::WorkspaceSnapshot, ConnectError> {
        let field = |name: &str| format!("snapshot.{name}");
        if offered.n == 0 {
            return Err(invalid(
                "snapshot.n",
                "snapshot 0 is written by CreateWorkspace or ForkWorkspace",
            ));
        }
        if offered.label == DELETED_LABEL {
            return Err(invalid(
                "snapshot.label",
                "the label `deleted` is DeleteWorkspace's",
            ));
        }
        writer(&offered.writer)?;
        if !offered.track.is_empty() {
            RefName::parse(&offered.track)
                .map_err(|err| invalid("snapshot.track", err.to_string()))?;
        }
        let created = offered
            .created_at
            .as_option()
            .ok_or_else(|| invalid("snapshot.created_at", "a save carries its time"))?;
        let created_unix_ms = created
            .seconds
            .saturating_mul(1000)
            .saturating_add(i64::from(created.nanos) / 1_000_000);
        let refs = offered
            .refs
            .iter()
            .map(|r| {
                Ok(gitpb::LocalRef {
                    name: r.name.clone(),
                    oid: oid_field(&field("refs"), &r.oid, false)?,
                    upstream: r.upstream.clone(),
                })
            })
            .collect::<Result<_, ConnectError>>()?;
        let changed = offered
            .changed
            .iter()
            .map(|c| {
                Ok(gitpb::ChangedPath {
                    path: c.path.clone(),
                    kind: change_kind_from(&c.kind)?,
                    staged: c.staged,
                })
            })
            .collect::<Result<_, ConnectError>>()?;
        let packs = offered
            .packs
            .iter()
            .map(|p| {
                parse_oid(&field("packs"), &p.checksum)?;
                Ok(gitpb::PackRef {
                    checksum: p.checksum.clone(),
                    object_count: p.object_count,
                    pack_len: p.pack_len,
                    idx_offset: p.idx_offset,
                    idx_len: p.idx_len,
                    has_bitmap: false,
                })
            })
            .collect::<Result<_, ConnectError>>()?;
        let conflicts = offered
            .conflicts
            .iter()
            .map(|c| {
                Ok(gitpb::Conflict {
                    path: c.path.clone(),
                    base: oid_field(&field("conflicts"), &c.base, true)?,
                    ours: oid_field(&field("conflicts"), &c.ours, true)?,
                    theirs: oid_field(&field("conflicts"), &c.theirs, true)?,
                    mode: c.mode,
                })
            })
            .collect::<Result<_, ConnectError>>()?;
        let takeover = offered.takeover.as_option().map(|t| gitpb::Takeover {
            from_mount: t.from_mount.clone(),
            from_n: t.from_n,
            reason: t.reason.clone(),
        });
        Ok(gitpb::WorkspaceSnapshot {
            n: offered.n,
            track: offered.track.clone(),
            base: oid_field(&field("base"), &offered.base, true)?,
            base_seq: offered.base_seq,
            head: oid_field(&field("head"), &offered.head, true)?,
            head_ref: offered.head_ref.clone(),
            refs,
            index_tree: oid_field(&field("index_tree"), &offered.index_tree, false)?,
            worktree_tree: oid_field(&field("worktree_tree"), &offered.worktree_tree, false)?,
            snapshot_commit: oid_field(&field("snapshot_commit"), &offered.snapshot_commit, false)?,
            changed,
            changed_overflow: offered.changed_overflow,
            packs,
            writer: offered.writer.clone(),
            principal: principal.to_string(),
            created_unix_ms,
            label: offered.label.clone(),
            takeover,
            conflicts,
            // The workspace's identity, as its latest snapshot holds it.
            ..gitpb::WorkspaceSnapshot {
                ws_id: latest.ws_id.clone(),
                object_format: latest.object_format,
                repo_id: latest.repo_id.clone(),
                owner: latest.owner.clone(),
                name: latest.name.clone(),
                fork: latest.fork.clone(),
                ..Default::default()
            }
        })
    }

    fn save_check_error(err: SaveCheckError) -> ConnectError {
        match err {
            SaveCheckError::Missing { oid, what } => refuse(
                ErrorCode::FailedPrecondition,
                "object_missing",
                format!("{what} {oid} is not in the repository or the workspace's packs"),
                &[("oid", oid.to_string())],
            ),
            err @ (SaveCheckError::Invalid { .. } | SaveCheckError::WrongKind { .. }) => {
                invalid("snapshot", err.to_string())
            }
            SaveCheckError::Odb(err) => odb_error(err),
        }
    }

    fn replay_error(err: ReplayError) -> ConnectError {
        match err {
            ReplayError::Invalid(reason) => invalid("salvage", reason),
            ReplayError::Odb(err) => odb_error(err),
            ReplayError::Build(loams_git::ws::TreeBuildError::Odb(err)) => odb_error(err),
            ReplayError::Build(err) => invalid("salvage", err.to_string()),
        }
    }

    fn salvage_ref(mount: &str, salvage: &str) -> Result<SalvageRef, ConnectError> {
        let mount = MountId::parse(mount).map_err(|err| invalid("mount", err.to_string()))?;
        if salvage.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(invalid("salvage", "26 lowercase ULID characters"));
        }
        let id = Ulid::from_string(salvage)
            .map_err(|_| invalid("salvage", "26 lowercase ULID characters"))?;
        Ok(SalvageRef { mount, id })
    }

    /// The packs of `first`, then those of `more` it does not name, each once.
    fn union_packs<'a>(
        first: &[gitpb::PackRef],
        more: impl IntoIterator<Item = &'a [gitpb::PackRef]>,
    ) -> Vec<gitpb::PackRef> {
        let mut out = first.to_vec();
        for packs in more {
            for pack in packs {
                if !out.iter().any(|p| p.checksum == pack.checksum) {
                    out.push(pack.clone());
                }
            }
        }
        out
    }

    fn edit_error(err: EditError) -> ConnectError {
        match err {
            EditError::Invalid { index, reason } => invalid(&format!("edits[{index}]"), reason),
            err @ EditError::TooMany { .. } => invalid("edits", err.to_string()),
            EditError::Env(refusal) => refuse(
                ErrorCode::InvalidArgument,
                "env_unsealed",
                refusal.to_string(),
                &[("path", refusal.path.clone())],
            ),
            err @ EditError::Snapshot(_) => internal("a corrupt snapshot", &err),
            EditError::Odb(err) => odb_error(err),
            EditError::Build(loams_git::ws::TreeBuildError::Odb(err)) => odb_error(err),
            EditError::Build(err) => invalid("edits", err.to_string()),
        }
    }

    /// A snapshot's tree field.
    fn tree_of(bytes: &[u8], field: &str) -> Result<loams_git::ObjectId, ConnectError> {
        loams_git::ObjectId::try_from(bytes).map_err(|_| {
            internal(
                "a corrupt snapshot",
                &format!("{field} is not an object id"),
            )
        })
    }

    fn repo_path(field: &str, value: &str) -> Result<RepoPath, ConnectError> {
        RepoPath::parse(value).map_err(|err| invalid(field, err.to_string()))
    }

    /// Edit `index` of an `ApplyEdits` request.
    fn file_edit(index: usize, edit: &pb::WorkspaceEdit) -> Result<FileEdit, ConnectError> {
        use loams_proto::loams::repos::v1::__buffa::oneof::workspace_edit::Edit;
        let field = |name: &str| format!("edits[{index}].{name}");
        match &edit.edit {
            Some(Edit::Write(write)) => Ok(FileEdit::Write {
                path: repo_path(&field("write.path"), &write.path)?,
                mode: match &write.mode {
                    EnumValue::Known(
                        pb::WorkspaceFileMode::WORKSPACE_FILE_MODE_UNSPECIFIED
                        | pb::WorkspaceFileMode::WORKSPACE_FILE_MODE_REGULAR,
                    ) => FileMode::Regular,
                    EnumValue::Known(pb::WorkspaceFileMode::WORKSPACE_FILE_MODE_EXECUTABLE) => {
                        FileMode::Executable
                    }
                    EnumValue::Known(pb::WorkspaceFileMode::WORKSPACE_FILE_MODE_SYMLINK) => {
                        FileMode::Symlink
                    }
                    EnumValue::Unknown(_) => {
                        return Err(invalid(&field("write.mode"), "an unknown mode"));
                    }
                },
                data: write.content.clone().into(),
            }),
            Some(Edit::Delete(delete)) => Ok(FileEdit::Delete {
                path: repo_path(&field("delete.path"), &delete.path)?,
            }),
            Some(Edit::Rename(rename)) => Ok(FileEdit::Rename {
                from: repo_path(&field("rename.old_path"), &rename.old_path)?,
                to: repo_path(&field("rename.new_path"), &rename.new_path)?,
            }),
            None => Err(invalid(
                &field("edit"),
                "an edit writes, deletes or renames",
            )),
        }
    }

    /// The snapshot after `on` with these trees and conflicts, written by `principal` now
    /// (the status hint dropped: readers diff the trees), sealed.
    fn successor(
        on: &gitpb::WorkspaceSnapshot,
        worktree: loams_git::ObjectId,
        index: loams_git::ObjectId,
        conflicts: Vec<gitpb::Conflict>,
        principal: &Principal,
        writer: Option<&MountId>,
    ) -> Result<gitpb::WorkspaceSnapshot, ConnectError> {
        seal_snapshot(gitpb::WorkspaceSnapshot {
            n: on.n + 1,
            worktree_tree: worktree.as_bytes().to_vec(),
            index_tree: index.as_bytes().to_vec(),
            changed: Vec::new(),
            changed_overflow: true,
            conflicts,
            writer: writer.map(ToString::to_string).unwrap_or_default(),
            principal: principal.to_string(),
            created_unix_ms: now_ms(),
            label: String::new(),
            takeover: None,
            ..on.clone()
        })
        .map_err(ws_error)
    }

    /// Whether a stored snapshot made the change `next` makes (a retried call's).
    fn same_change(stored: &gitpb::WorkspaceSnapshot, next: &gitpb::WorkspaceSnapshot) -> bool {
        stored.n == next.n
            && stored.worktree_tree == next.worktree_tree
            && stored.index_tree == next.index_tree
            && stored.conflicts == next.conflicts
            && stored.head == next.head
            && stored.principal == next.principal
    }

    impl WorkspacesServed {
        fn store(&self, record: &RepoRecord) -> BucketWorkspaceStore {
            BucketWorkspaceStore::new(self.repos.git.store.clone(), record.ns)
        }

        /// The repository a request names, after checking the caller holds `need`.
        async fn repo(
            &self,
            ctx: &RequestContext,
            need: GitAccess,
            ns_name: &str,
            name: &str,
        ) -> Result<RepoRecord, ConnectError> {
            self.repos.require(ctx, need).await?;
            let name = repo_name("name", name)?;
            let ns = self.repos.namespace(ns_name, false).await?;
            self.repos.active(ns, &name).await
        }

        /// Workspace `value`'s latest snapshot; `workspace_not_found` when it does not exist
        /// or was deleted.
        async fn latest(
            &self,
            record: &RepoRecord,
            value: &str,
        ) -> Result<(WsId, Arc<gitpb::WorkspaceSnapshot>), ConnectError> {
            let ws = ws_id(value)?;
            match self.store(record).latest(&record.repo_id, &ws).await {
                Ok(latest) if latest.label != DELETED_LABEL => Ok((ws, latest)),
                Ok(_) | Err(WsError::NotFound) => Err(workspace_not_found(value)),
                Err(err) => Err(ws_error(err)),
            }
        }

        /// The repository's latest refs and packs.
        async fn repo_snapshot(
            &self,
            ns_name: &str,
            record: &RepoRecord,
        ) -> Result<Arc<RefSnapshot>, ConnectError> {
            Ok(self.repos.read(ns_name, record, "").await?.1)
        }

        /// The repository's objects with workspace `ws`'s packs `view` searched last.
        async fn odb(
            &self,
            record: &RepoRecord,
            snapshot: &RefSnapshot,
            ws: &WsId,
            view: &WsView,
        ) -> Result<RangeOdb, ConnectError> {
            open_odb(&self.repos.git, record, snapshot, Some((ws, view)))
                .await
                .map_err(|reason| unavailable("opening the workspace's objects", &reason))
        }

        /// Snapshot `n` (the latest when `None`) of workspace `ws`.
        async fn snapshot_at(
            &self,
            record: &RepoRecord,
            ws: &WsId,
            latest: &Arc<gitpb::WorkspaceSnapshot>,
            n: Option<u64>,
        ) -> Result<Arc<gitpb::WorkspaceSnapshot>, ConnectError> {
            match n {
                None => Ok(Arc::clone(latest)),
                Some(n) if n == latest.n => Ok(Arc::clone(latest)),
                Some(n) => self
                    .store(record)
                    .get(&record.repo_id, ws, SnapN(n))
                    .await
                    .map_err(ws_error)?
                    .ok_or_else(|| {
                        refuse(
                            ErrorCode::NotFound,
                            "workspace_not_found",
                            format!("workspace {ws} has no snapshot {n}"),
                            &[("workspace", ws.to_string()), ("n", n.to_string())],
                        )
                    }),
            }
        }

        /// The repository's objects with the packs of `snap` and of `latest` searched last.
        async fn snapshot_odb(
            &self,
            ns_name: &str,
            record: &RepoRecord,
            ws: &WsId,
            latest: &gitpb::WorkspaceSnapshot,
            snap: &gitpb::WorkspaceSnapshot,
        ) -> Result<RangeOdb, ConnectError> {
            let store = self.store(record);
            let view = WsView {
                packs: union_packs(&latest.packs, [snap.packs.as_slice()]),
                ..view_of(&store, &record.repo_id, latest)
                    .await
                    .map_err(ws_error)?
            };
            let repo = self.repo_snapshot(ns_name, record).await?;
            self.odb(record, &repo, ws, &view).await
        }

        /// Stores `objects` and `next`'s snapshot commit as one workspace pack (scanned as an
        /// upload is); `packs` with it added.
        async fn store_objects(
            &self,
            record: &RepoRecord,
            ws: &WsId,
            next: &gitpb::WorkspaceSnapshot,
            mut objects: Vec<(Kind, bytes::Bytes)>,
            packs: &[gitpb::PackRef],
        ) -> Result<Vec<gitpb::PackRef>, ConnectError> {
            let (_, commit_body) =
                snapshot_commit(next).map_err(|err| internal("the snapshot commit", &err))?;
            objects.push((Kind::Commit, commit_body));
            let (_, _, lpk) =
                write_ws_pack(objects).map_err(|err| internal("writing the objects", &err))?;
            let paths = RepoPaths::new(record.ns, &record.repo_id);
            let blobs = StoreBlobStore::workspace(self.repos.git.store.clone(), &paths, ws);
            let (verified, _) = put_workspace_pack(&record.repo_id, &blobs, lpk, &NoScan)
                .await
                .map_err(|err| match err {
                    err @ WsPackError::Refused(_) => ws_pack_error(err),
                    err => internal("storing the objects", &err),
                })?;
            Ok(union_packs(packs, [std::slice::from_ref(&verified.pack)]))
        }

        /// Appends `snap`, then publishes the hint and tells this node's watchers.
        async fn append(
            &self,
            store: &BucketWorkspaceStore,
            record: &RepoRecord,
            ws: &WsId,
            snap: &gitpb::WorkspaceSnapshot,
        ) -> Result<SnapAppended, ConnectError> {
            let appended = store.append(snap).await.map_err(ws_error)?;
            if let SnapAppended::Committed { n } = appended {
                // The hint only speeds readers up; a failed write is not the save's.
                if let Err(err) = store.publish_hint(&record.repo_id, ws, n).await {
                    tracing::debug!(error = %err, "publishing a workspace hint failed");
                }
                let _ = self
                    .repos
                    .git
                    .ws_saved
                    .send((record.repo_id.clone(), ws.clone()));
            }
            Ok(appended)
        }
    }

    fn fenced(existing: &gitpb::WorkspaceSnapshot, salvage: Option<&SalvageRef>) -> ConnectError {
        let mut meta = vec![
            ("n", existing.n.to_string()),
            ("writer", existing.writer.clone()),
        ];
        let message = match salvage {
            Some(salvage) => {
                meta.push(("mount", salvage.mount.to_string()));
                meta.push(("salvage", loams_git::ids::lowercase_ulid(salvage.id)));
                format!(
                    "snapshot {} was saved by another writer; your state is kept as salvage {}",
                    existing.n,
                    loams_git::ids::lowercase_ulid(salvage.id)
                )
            }
            None => format!("snapshot {} was saved by another writer", existing.n),
        };
        refuse(ErrorCode::Aborted, "workspace_fenced", message, &meta)
    }

    impl WorkspaceService for WorkspacesServed {
        async fn create_workspace(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::CreateWorkspaceRequest>,
        ) -> ServiceResult<pb::CreateWorkspaceResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Write, &req.namespace, &req.name)
                .await?;
            let owner = caller_principal(&ctx)?;
            let key = idem_key(&req.idempotency_key)?;
            let name = ws_name(&req.workspace_name)?;
            let writer = writer(&req.writer)?;
            let snapshot = self.repo_snapshot(&req.namespace, &record).await?;
            let track = if req.track.is_empty() {
                snapshot.symrefs.get("HEAD").cloned().ok_or_else(|| {
                    invalid("track", "the repository has no default branch; name one")
                })?
            } else {
                RefName::parse(&req.track).map_err(|err| invalid("track", err.to_string()))?
            };
            let commit = if req.at.is_empty() {
                snapshot.refs.get(&track).copied()
            } else {
                Some(parse_oid("at", &req.at)?)
            };
            let tree = match commit {
                Some(commit) => {
                    let odb = open_odb(&self.repos.git, &record, &snapshot, None)
                        .await
                        .map_err(|reason| unavailable("opening the repository", &reason))?;
                    commit_tree(&odb, "at", &commit)
                        .await
                        .map_err(save_check_error)?
                }
                None => empty_tree(),
            };
            let ws = new_ws_id(
                "CreateWorkspace",
                &record,
                &owner,
                key.as_ref().map(|k| k.as_str()),
            );
            let created = self
                .store(&record)
                .create(CreateWs {
                    repo: record.repo_id.clone(),
                    ws_id: ws,
                    owner: owner.clone(),
                    name,
                    principal: owner,
                    writer,
                    base: Some(WsBase {
                        track: track.clone(),
                        commit,
                        tree,
                        seq: snapshot.seq,
                        head_ref: Some(track),
                    }),
                })
                .await
                .map_err(ws_error)?;
            Response::ok(pb::CreateWorkspaceResponse {
                snapshot: MessageField::some(snapshot_pb(&created)),
                ..Default::default()
            })
        }

        async fn get_workspace(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::GetWorkspaceRequest>,
        ) -> ServiceResult<pb::GetWorkspaceResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Read, &req.namespace, &req.name)
                .await?;
            let (_, latest) = self.latest(&record, &req.workspace).await?;
            may_read_workspace(&self.repos, &ctx, &latest.owner).await?;
            Response::ok(pb::GetWorkspaceResponse {
                snapshot: MessageField::some(snapshot_pb(&latest)),
                ..Default::default()
            })
        }

        async fn list_workspaces(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::ListWorkspacesRequest>,
        ) -> ServiceResult<pb::ListWorkspacesResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Read, &req.namespace, &req.name)
                .await?;
            let limit = page_size(req.page_size, PAGE)?;
            let owner = if req.owner.is_empty() {
                None
            } else {
                Some(
                    Principal::parse(&req.owner)
                        .map_err(|err| invalid("owner", err.to_string()))?,
                )
            };
            // Every workspace's ids are an administrator's listing when reads are owners'.
            match &owner {
                Some(owner) => may_read_workspace(&self.repos, &ctx, owner.as_str()).await?,
                None if self.repos.git.ws_reads_owner_only
                    && !is_admin(&self.repos, &ctx).await? =>
                {
                    return Err(denied("list every workspace; name yourself as `owner` to"));
                }
                None => {}
            }
            let after = (!req.page_token.is_empty()).then_some(req.page_token.as_str());
            let page = self
                .store(&record)
                .list(&record.repo_id, owner.as_ref(), after, limit)
                .await
                .map_err(ws_error)?;
            let next_page_token = if page.len() == limit {
                page.last()
                    .map(|s| s.cursor().to_owned())
                    .unwrap_or_default()
            } else {
                String::new()
            };
            Response::ok(pb::ListWorkspacesResponse {
                workspaces: page
                    .iter()
                    .map(|s| pb::WorkspaceSummary {
                        workspace: s.ws_id.to_string(),
                        workspace_name: s
                            .name
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                        owner: owner.as_ref().map(ToString::to_string).unwrap_or_default(),
                        created_at: match s.created_unix_ms {
                            Some(ms) => MessageField::some(timestamp(ms)),
                            None => MessageField::none(),
                        },
                        ..Default::default()
                    })
                    .collect(),
                next_page_token,
                ..Default::default()
            })
        }

        async fn fork_workspace(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::ForkWorkspaceRequest>,
        ) -> ServiceResult<pb::ForkWorkspaceResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Write, &req.namespace, &req.name)
                .await?;
            let owner = caller_principal(&ctx)?;
            let key = idem_key(&req.idempotency_key)?;
            let name = ws_name(&req.workspace_name)?;
            let writer = writer(&req.writer)?;
            let (source, latest) = self.latest(&record, &req.workspace).await?;
            may_read_workspace(&self.repos, &ctx, &latest.owner).await?;
            let store = self.store(&record);
            // The fork is one level below its source's chain (ruling T24.6).
            let depth = match latest.fork {
                Some(_) => match fork_sources(&store, &record.repo_id, &source).await {
                    Ok(sources) => sources.len(),
                    Err(WsError::Invalid { .. }) => return Err(fork_too_deep()),
                    Err(err) => return Err(ws_error(err)),
                },
                None => 0,
            };
            if depth + 1 > MAX_FORK_DEPTH {
                return Err(fork_too_deep());
            }
            let n = SnapN(req.n.unwrap_or(latest.n));
            let ws = new_ws_id(
                "ForkWorkspace",
                &record,
                &owner,
                key.as_ref().map(|k| k.as_str()),
            );
            let forked = store
                .fork(
                    (&record.repo_id, &source, n),
                    CreateWs {
                        repo: record.repo_id.clone(),
                        ws_id: ws,
                        owner: owner.clone(),
                        name,
                        principal: owner,
                        writer,
                        base: None,
                    },
                )
                .await
                .map_err(|err| match err {
                    WsError::NotFound => refuse(
                        ErrorCode::NotFound,
                        "workspace_not_found",
                        format!("workspace {source} has no snapshot {}", n.0),
                        &[("workspace", source.to_string())],
                    ),
                    other => ws_error(other),
                })?;
            Response::ok(pb::ForkWorkspaceResponse {
                snapshot: MessageField::some(snapshot_pb(&forked)),
                ..Default::default()
            })
        }

        async fn delete_workspace(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::DeleteWorkspaceRequest>,
        ) -> ServiceResult<pb::DeleteWorkspaceResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Write, &req.namespace, &req.name)
                .await?;
            idem_key(&req.idempotency_key)?;
            let principal = caller_principal(&ctx)?;
            let ws = ws_id(&req.workspace)?;
            let store = self.store(&record);
            for _ in 0..MAX_FENCE_RETRIES {
                let latest = match store.latest(&record.repo_id, &ws).await {
                    Ok(latest) => latest,
                    Err(WsError::NotFound) => return Err(workspace_not_found(&req.workspace)),
                    Err(err) => return Err(ws_error(err)),
                };
                may_write_workspace(&self.repos, &ctx, &latest.owner).await?;
                if latest.label == DELETED_LABEL {
                    // Deleted already: the same answer.
                    return Response::ok(pb::DeleteWorkspaceResponse {
                        snapshot: MessageField::some(snapshot_pb(&latest)),
                        ..Default::default()
                    });
                }
                let last = seal_snapshot(gitpb::WorkspaceSnapshot {
                    n: latest.n + 1,
                    writer: String::new(),
                    principal: principal.to_string(),
                    created_unix_ms: now_ms(),
                    label: DELETED_LABEL.to_owned(),
                    takeover: None,
                    ..(*latest).clone()
                })
                .map_err(ws_error)?;
                match self.append(&store, &record, &ws, &last).await? {
                    SnapAppended::Committed { .. } => {
                        return Response::ok(pb::DeleteWorkspaceResponse {
                            snapshot: MessageField::some(snapshot_pb(&last)),
                            ..Default::default()
                        });
                    }
                    // A save won the number: read the new latest and try again.
                    SnapAppended::Fenced { .. } => {}
                }
            }
            Err(refuse(
                ErrorCode::Aborted,
                "workspace_fenced",
                "the workspace kept changing while it was being deleted; retry",
                &[],
            ))
        }

        async fn list_snapshots(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::ListSnapshotsRequest>,
        ) -> ServiceResult<pb::ListSnapshotsResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Read, &req.namespace, &req.name)
                .await?;
            let limit = page_size(req.page_size, PAGE)?;
            let (ws, latest) = self.latest(&record, &req.workspace).await?;
            may_read_workspace(&self.repos, &ctx, &latest.owner).await?;
            let after = if req.page_token.is_empty() {
                None
            } else {
                Some(SnapN(req.page_token.parse::<u64>().map_err(|_| {
                    invalid("page_token", "not a token this listing answered")
                })?))
            };
            let store = self.store(&record);
            let numbers = store
                .list_snapshots(&record.repo_id, &ws, after, limit)
                .await
                .map_err(ws_error)?;
            let next_page_token = match numbers.last() {
                Some(n) if numbers.len() == limit => n.0.to_string(),
                _ => String::new(),
            };
            let (store, repo, ws) = (&store, &record.repo_id, &ws);
            let snapshots: Vec<pb::SnapshotSummary> = futures::stream::iter(numbers)
                .map(|n| async move { store.get(repo, ws, n).await })
                .buffered(LIST_READS)
                // A snapshot collected since the listing is left out.
                .try_filter_map(|snap| async move {
                    Ok(snap.map(|snap| pb::SnapshotSummary {
                        n: snap.n,
                        created_at: MessageField::some(timestamp(snap.created_unix_ms)),
                        label: snap.label.clone(),
                        writer: snap.writer.clone(),
                        principal: snap.principal.clone(),
                        snapshot_commit: hex(&snap.snapshot_commit),
                        ..Default::default()
                    }))
                })
                .try_collect()
                .await
                .map_err(ws_error)?;
            Response::ok(pb::ListSnapshotsResponse {
                snapshots,
                next_page_token,
                ..Default::default()
            })
        }

        async fn get_snapshot(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::GetSnapshotRequest>,
        ) -> ServiceResult<pb::GetSnapshotResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Read, &req.namespace, &req.name)
                .await?;
            let (ws, latest) = self.latest(&record, &req.workspace).await?;
            may_read_workspace(&self.repos, &ctx, &latest.owner).await?;
            let snap = self
                .store(&record)
                .get(&record.repo_id, &ws, SnapN(req.n))
                .await
                .map_err(ws_error)?
                .ok_or_else(|| {
                    refuse(
                        ErrorCode::NotFound,
                        "workspace_not_found",
                        format!("workspace {ws} has no snapshot {}", req.n),
                        &[("workspace", ws.to_string()), ("n", req.n.to_string())],
                    )
                })?;
            Response::ok(pb::GetSnapshotResponse {
                snapshot: MessageField::some(snapshot_pb(&snap)),
                ..Default::default()
            })
        }

        async fn save_snapshot(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::SaveSnapshotRequest>,
        ) -> ServiceResult<pb::SaveSnapshotResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Write, &req.namespace, &req.name)
                .await?;
            idem_key(&req.idempotency_key)?;
            let principal = caller_principal(&ctx)?;
            let (ws, latest) = self.latest(&record, &req.workspace).await?;
            may_write_workspace(&self.repos, &ctx, &latest.owner).await?;
            let offered = req
                .snapshot
                .as_option()
                .ok_or_else(|| invalid("snapshot", "a save carries its snapshot"))?;
            let snap = offered_snapshot(&latest, offered, &principal)?;
            if snap.n > latest.n + 1 {
                return Err(invalid(
                    "snapshot.n",
                    format!(
                        "the latest snapshot is {}; a save appends {}",
                        latest.n,
                        latest.n + 1
                    ),
                ));
            }
            // The snapshot commit is this snapshot's (Ruling 38), so a client cannot name
            // another commit as the worktree's.
            let (want, _) =
                snapshot_commit(&snap).map_err(|err| invalid("snapshot", err.to_string()))?;
            if want.as_bytes() != snap.snapshot_commit.as_slice() {
                return Err(invalid(
                    "snapshot.snapshot_commit",
                    format!("this snapshot's commit is {want}"),
                ));
            }
            let store = self.store(&record);
            let view = WsView {
                packs: snap.packs.clone(),
                ..view_of(&store, &record.repo_id, &latest)
                    .await
                    .map_err(ws_error)?
            };
            // Every pack it names is stored (under the workspace, or a fork source's).
            let paths = RepoPaths::new(record.ns, &record.repo_id);
            let blobs = WsPackBlobs::new(
                &self.repos.git.store,
                &paths,
                &ws,
                &view.sources,
                Some(&self.repos.git.h1),
            );
            for pack in &snap.packs {
                // The latest snapshot's packs were checked when it was saved.
                if latest
                    .packs
                    .iter()
                    .any(|held| held.checksum == pack.checksum)
                {
                    continue;
                }
                let checksum = parse_oid("snapshot.packs", &pack.checksum)?;
                let held = loams_git::BlobStore::head(&blobs, &BlobId::from_checksum(&checksum))
                    .await
                    .map_err(|err| unavailable("reading the workspace's packs", &err))?;
                if held.is_none() {
                    return Err(refuse(
                        ErrorCode::FailedPrecondition,
                        "object_missing",
                        format!("pack {checksum} is not stored for this workspace"),
                        &[("pack", checksum.to_string())],
                    ));
                }
            }
            let repo = self.repo_snapshot(&req.namespace, &record).await?;
            let odb = self.odb(&record, &repo, &ws, &view).await?;
            check_save_objects(&odb, &snap)
                .await
                .map_err(save_check_error)?;
            match self.append(&store, &record, &ws, &snap).await? {
                SnapAppended::Committed { .. } => Response::ok(pb::SaveSnapshotResponse {
                    snapshot: MessageField::some(snapshot_pb(&snap)),
                    ..Default::default()
                }),
                SnapAppended::Fenced { existing } => {
                    // The fenced writer's state is kept for it (§19.4.3).
                    let salvage = match writer(&snap.writer)? {
                        Some(mount) => {
                            let id = store.salvage(&snap, &mount).await.map_err(ws_error)?;
                            Some(SalvageRef { mount, id })
                        }
                        None => None,
                    };
                    Err(fenced(&existing, salvage.as_ref()))
                }
            }
        }

        async fn watch_workspace(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::WatchWorkspaceRequest>,
        ) -> ServiceResult<ServiceStream<pb::WatchWorkspaceResponse>> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Read, &req.namespace, &req.name)
                .await?;
            let (ws, latest) = self.latest(&record, &req.workspace).await?;
            may_read_workspace(&self.repos, &ctx, &latest.owner).await?;
            let (tx, mut rx) = mpsc::channel(WATCH_BUFFER);
            let watch = Watch {
                store: self.store(&record),
                saved: self.repos.git.ws_saved.subscribe(),
                repo: record.repo_id.clone(),
                ws,
                heartbeat: self.repos.git.watch_heartbeat,
                tx,
            };
            tokio::spawn(watch.run(latest, req.after_n));
            Response::ok(
                Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
                    as ServiceStream<pb::WatchWorkspaceResponse>,
            )
        }

        async fn list_salvage(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::ListSalvageRequest>,
        ) -> ServiceResult<pb::ListSalvageResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Read, &req.namespace, &req.name)
                .await?;
            let limit = page_size(req.page_size, PAGE)?;
            let (ws, latest) = self.latest(&record, &req.workspace).await?;
            may_read_workspace(&self.repos, &ctx, &latest.owner).await?;
            let after = match req.page_token.split_once('/') {
                Some((mount, id)) => Some(
                    salvage_ref(mount, id)
                        .map_err(|_| invalid("page_token", "not a token this listing answered"))?,
                ),
                None if req.page_token.is_empty() => None,
                None => return Err(invalid("page_token", "not a token this listing answered")),
            };
            let store = self.store(&record);
            let refs = store
                .list_salvage(&record.repo_id, &ws, after.as_ref(), limit)
                .await
                .map_err(ws_error)?;
            let next_page_token = match refs.last() {
                Some(last) if refs.len() == limit => {
                    format!("{}/{}", last.mount, loams_git::ids::lowercase_ulid(last.id))
                }
                _ => String::new(),
            };
            let (store, repo, ws) = (&store, &record.repo_id, &ws);
            let salvage: Vec<pb::Salvage> = futures::stream::iter(refs)
                .map(|salvage| async move {
                    let snap = store.get_salvage(repo, ws, &salvage).await?;
                    Ok::<_, WsError>(snap.map(|snap| pb::Salvage {
                        mount: salvage.mount.to_string(),
                        salvage: loams_git::ids::lowercase_ulid(salvage.id),
                        n: snap.n,
                        created_at: MessageField::some(timestamp(snap.created_unix_ms)),
                        ..Default::default()
                    }))
                })
                .buffered(LIST_READS)
                .try_filter_map(|salvage| async move { Ok(salvage) })
                .try_collect()
                .await
                .map_err(ws_error)?;
            Response::ok(pb::ListSalvageResponse {
                salvage,
                next_page_token,
                ..Default::default()
            })
        }

        async fn restore_salvage(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::RestoreSalvageRequest>,
        ) -> ServiceResult<pb::RestoreSalvageResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Write, &req.namespace, &req.name)
                .await?;
            idem_key(&req.idempotency_key)?;
            let principal = caller_principal(&ctx)?;
            let salvage = salvage_ref(&req.mount, &req.salvage)?;
            let writer_id = writer(&req.writer)?;
            let ws = ws_id(&req.workspace)?;
            let store = self.store(&record);
            let not_held = || {
                refuse(
                    ErrorCode::NotFound,
                    "workspace_not_found",
                    format!(
                        "workspace {} holds no salvage {}/{}",
                        req.workspace, req.mount, req.salvage
                    ),
                    &[("workspace", req.workspace.clone())],
                )
            };
            for _ in 0..MAX_FENCE_RETRIES {
                let (_, latest) = self.latest(&record, &req.workspace).await?;
                may_write_workspace(&self.repos, &ctx, &latest.owner).await?;
                let salvaged = store
                    .get_salvage(&record.repo_id, &ws, &salvage)
                    .await
                    .map_err(ws_error)?
                    .ok_or_else(not_held)?;
                let base_n = salvaged.n.checked_sub(1).ok_or_else(|| {
                    invalid("salvage", "a salvaged snapshot numbers past snapshot 0")
                })?;
                let base = store
                    .get(&record.repo_id, &ws, SnapN(base_n))
                    .await
                    .map_err(ws_error)?
                    .ok_or_else(|| {
                        refuse(
                            ErrorCode::FailedPrecondition,
                            "failed_precondition",
                            format!(
                                "snapshot {base_n}, which the salvage was built on, was collected"
                            ),
                            &[],
                        )
                    })?;
                let packs = union_packs(
                    &latest.packs,
                    [salvaged.packs.as_slice(), base.packs.as_slice()],
                );
                let view = WsView {
                    packs: packs.clone(),
                    ..view_of(&store, &record.repo_id, &latest)
                        .await
                        .map_err(ws_error)?
                };
                let repo = self.repo_snapshot(&req.namespace, &record).await?;
                let odb = self.odb(&record, &repo, &ws, &view).await?;
                let replayed = replay_salvage(&odb, &base, &salvaged, &latest)
                    .await
                    .map_err(replay_error)?;
                let conflict_paths: Vec<String> =
                    replayed.conflicts.iter().map(|c| c.path.clone()).collect();
                let mut conflicts = latest.conflicts.clone();
                let before = conflicts.len();
                for conflict in replayed.conflicts {
                    if !conflicts.iter().any(|c| c.path == conflict.path) {
                        conflicts.push(conflict);
                    }
                }
                if replayed.applied == 0 && conflicts.len() == before {
                    // Restored already (or nothing to restore): the latest snapshot holds it.
                    return Response::ok(pb::RestoreSalvageResponse {
                        snapshot: MessageField::some(snapshot_pb(&latest)),
                        conflicts: conflict_paths,
                        ..Default::default()
                    });
                }
                let mut next = seal_snapshot(gitpb::WorkspaceSnapshot {
                    n: latest.n + 1,
                    worktree_tree: replayed.worktree_tree.as_bytes().to_vec(),
                    index_tree: replayed.index_tree.as_bytes().to_vec(),
                    // The status hint would need the trees diffed: readers do that.
                    changed: Vec::new(),
                    changed_overflow: true,
                    conflicts,
                    writer: writer_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    principal: principal.to_string(),
                    created_unix_ms: now_ms(),
                    label: String::new(),
                    takeover: None,
                    ..(*latest).clone()
                })
                .map_err(ws_error)?;
                // The new trees and the snapshot commit, as one workspace pack.
                let (_, commit_body) =
                    snapshot_commit(&next).map_err(|err| invalid("salvage", err.to_string()))?;
                let mut objects = replayed.objects;
                objects.push((Kind::Commit, commit_body));
                let (_, _, lpk) = write_ws_pack(objects)
                    .map_err(|err| internal("writing the restored objects", &err))?;
                let paths = RepoPaths::new(record.ns, &record.repo_id);
                let blobs = StoreBlobStore::workspace(self.repos.git.store.clone(), &paths, &ws);
                let (verified, _) = put_workspace_pack(&record.repo_id, &blobs, lpk, &NoScan)
                    .await
                    .map_err(|err| internal("storing the restored objects", &err))?;
                next.packs = union_packs(&packs, [std::slice::from_ref(&verified.pack)]);
                match self.append(&store, &record, &ws, &next).await? {
                    SnapAppended::Committed { .. } => {
                        return Response::ok(pb::RestoreSalvageResponse {
                            snapshot: MessageField::some(snapshot_pb(&next)),
                            conflicts: conflict_paths,
                            ..Default::default()
                        });
                    }
                    // Another save took the number: replay on the new latest.
                    SnapAppended::Fenced { .. } => {}
                }
            }
            Err(refuse(
                ErrorCode::Aborted,
                "workspace_fenced",
                "the workspace kept changing while the salvage was restored; retry",
                &[],
            ))
        }

        async fn read_files(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::ReadFilesRequest>,
        ) -> ServiceResult<ServiceStream<pb::ReadFilesResponse>> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Read, &req.namespace, &req.name)
                .await?;
            let (ws, latest) = self.latest(&record, &req.workspace).await?;
            may_read_workspace(&self.repos, &ctx, &latest.owner).await?;
            if req.paths.len() > MAX_READ_PATHS {
                return Err(invalid(
                    "paths",
                    format!("at most {MAX_READ_PATHS} paths in one call"),
                ));
            }
            let paths = req
                .paths
                .iter()
                .map(|p| RepoPath::parse(p).map_err(|err| invalid("paths", err.to_string())))
                .collect::<Result<Vec<_>, _>>()?;
            let snap = self.snapshot_at(&record, &ws, &latest, req.n).await?;
            let tree = tree_of(&snap.worktree_tree, "worktree_tree")?;
            let odb = self
                .snapshot_odb(&req.namespace, &record, &ws, &latest, &snap)
                .await?;
            let (tx, mut rx) = mpsc::channel(STREAM_BUFFER);
            tokio::spawn(async move {
                let reader = TreeReader::new(&odb);
                for (path, name) in paths.iter().zip(&req.paths) {
                    let read = match read_file(&reader, tree, path, &NoEnv).await {
                        Ok(read) => read,
                        Err(err) => {
                            let _ = tx.send(Err(edit_error(err))).await;
                            return;
                        }
                    };
                    let head = pb::ReadFilesResponse {
                        path: name.clone(),
                        found: read.entry.is_some(),
                        mode: read.entry.map_or(0, |(mode, _)| mode),
                        oid: read
                            .entry
                            .map(|(_, oid)| oid.to_string())
                            .unwrap_or_default(),
                        size: read.data.as_ref().map_or(0, |d| d.len() as u64),
                        redacted: read.redacted,
                        ..Default::default()
                    };
                    let data = read.data.unwrap_or_default();
                    let mut offset = 0;
                    loop {
                        let end = (offset + CHUNK_BYTES).min(data.len());
                        let message = pb::ReadFilesResponse {
                            offset: offset as u64,
                            data: data.slice(offset..end).to_vec(),
                            ..head.clone()
                        };
                        if tx.send(Ok(message)).await.is_err() {
                            return;
                        }
                        offset = end;
                        if offset >= data.len() {
                            break;
                        }
                    }
                }
            });
            Response::ok(
                Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
                    as ServiceStream<pb::ReadFilesResponse>,
            )
        }

        async fn apply_edits(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::ApplyEditsRequest>,
        ) -> ServiceResult<pb::ApplyEditsResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Write, &req.namespace, &req.name)
                .await?;
            idem_key(&req.idempotency_key)?;
            let principal = caller_principal(&ctx)?;
            let writer_id = writer(&req.writer)?;
            let (ws, latest) = self.latest(&record, &req.workspace).await?;
            may_write_workspace(&self.repos, &ctx, &latest.owner).await?;
            if req.edits.len() > MAX_EDITS {
                return Err(invalid(
                    "edits",
                    format!("at most {MAX_EDITS} edits in one call"),
                ));
            }
            let edits = req
                .edits
                .iter()
                .enumerate()
                .map(|(i, edit)| file_edit(i, edit))
                .collect::<Result<Vec<_>, _>>()?;
            let n = req.expect_n;
            if latest.n < n {
                return Err(invalid(
                    "expect_n",
                    format!("the latest snapshot is {}", latest.n),
                ));
            }
            if latest.n > n + 1 {
                return Err(fenced(&latest, None));
            }
            let store = self.store(&record);
            // The snapshot the edits apply to: the latest, or the one before it when this
            // may be a retry of a call that saved.
            let on = if latest.n == n {
                Arc::clone(&latest)
            } else {
                store
                    .get(&record.repo_id, &ws, SnapN(n))
                    .await
                    .map_err(ws_error)?
                    .ok_or_else(|| fenced(&latest, None))?
            };
            let odb = self
                .snapshot_odb(&req.namespace, &record, &ws, &latest, &on)
                .await?;
            let edited = apply_edits(&odb, &on, edits, req.stage, &NoEnv)
                .await
                .map_err(edit_error)?;
            let mut conflicts = on.conflicts.clone();
            if req.stage {
                conflicts.retain(|c| !edited.touched.iter().any(|p| p.as_str() == c.path));
            }
            let mut next = successor(
                &on,
                edited.worktree_tree,
                edited.index_tree,
                conflicts,
                &principal,
                writer_id.as_ref(),
            )?;
            if latest.n == n + 1 {
                // A retry answers the snapshot its first attempt saved; anything else lost
                // the race.
                return if same_change(&latest, &next) {
                    Response::ok(pb::ApplyEditsResponse {
                        snapshot: MessageField::some(snapshot_pb(&latest)),
                        ..Default::default()
                    })
                } else {
                    Err(fenced(&latest, None))
                };
            }
            next.packs = self
                .store_objects(&record, &ws, &next, edited.objects, &on.packs)
                .await?;
            match self.append(&store, &record, &ws, &next).await? {
                SnapAppended::Committed { .. } => Response::ok(pb::ApplyEditsResponse {
                    snapshot: MessageField::some(snapshot_pb(&next)),
                    ..Default::default()
                }),
                SnapAppended::Fenced { existing } if same_change(&existing, &next) => {
                    Response::ok(pb::ApplyEditsResponse {
                        snapshot: MessageField::some(snapshot_pb(&existing)),
                        ..Default::default()
                    })
                }
                SnapAppended::Fenced { existing } => Err(fenced(&existing, None)),
            }
        }

        async fn diff(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::DiffRequest>,
        ) -> ServiceResult<ServiceStream<pb::DiffResponse>> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Read, &req.namespace, &req.name)
                .await?;
            let (ws, latest) = self.latest(&record, &req.workspace).await?;
            may_read_workspace(&self.repos, &ctx, &latest.owner).await?;
            let spec =
                Pathspec::parse(&req.paths).map_err(|err| invalid("paths", err.to_string()))?;
            let snap = self.snapshot_at(&record, &ws, &latest, req.n).await?;
            let new = tree_of(&snap.worktree_tree, "worktree_tree")?;
            let odb = self
                .snapshot_odb(&req.namespace, &record, &ws, &latest, &snap)
                .await?;
            let tree_of_commit = |bytes: Vec<u8>, field: &'static str| {
                let odb = &odb;
                async move {
                    if bytes.is_empty() {
                        return Ok(empty_tree());
                    }
                    let oid = loams_git::ObjectId::try_from(bytes.as_slice())
                        .map_err(|_| internal("a snapshot commit id", &field))?;
                    commit_tree(odb, field, &oid)
                        .await
                        .map_err(save_check_error)
                }
            };
            let against = match &req.against {
                EnumValue::Known(pb::DiffAgainst::DIFF_AGAINST_BASE) => {
                    tree_of_commit(snap.base.clone(), "base").await?
                }
                EnumValue::Known(pb::DiffAgainst::DIFF_AGAINST_INDEX) => {
                    tree_of(&snap.index_tree, "index_tree")?
                }
                EnumValue::Known(pb::DiffAgainst::DIFF_AGAINST_COMMIT) => {
                    let commit = parse_oid("commit", &req.commit)?;
                    tree_of_commit(commit.as_bytes().to_vec(), "commit").await?
                }
                EnumValue::Known(
                    pb::DiffAgainst::DIFF_AGAINST_HEAD | pb::DiffAgainst::DIFF_AGAINST_UNSPECIFIED,
                ) => tree_of_commit(snap.head.clone(), "head").await?,
                EnumValue::Unknown(_) => return Err(invalid("against", "an unknown value")),
            };
            let (tx, mut rx) = mpsc::channel(STREAM_BUFFER);
            tokio::spawn(async move {
                let reader = TreeReader::new(&odb);
                let changes = match changed_paths(&reader, against, new, &spec).await {
                    Ok(changes) => changes,
                    Err(err) => {
                        let _ = tx.send(Err(edit_error(err))).await;
                        return;
                    }
                };
                for change in changes {
                    let patch = match file_patch(&odb, &change).await {
                        Ok(patch) => patch,
                        Err(err) => {
                            let _ = tx.send(Err(edit_error(err))).await;
                            return;
                        }
                    };
                    let path = String::from_utf8_lossy(&change.path).into_owned();
                    for part in patch.chunks(CHUNK_BYTES) {
                        let message = pb::DiffResponse {
                            path: path.clone(),
                            patch: part.to_vec(),
                            ..Default::default()
                        };
                        if tx.send(Ok(message)).await.is_err() {
                            return;
                        }
                    }
                }
            });
            Response::ok(
                Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
                    as ServiceStream<pb::DiffResponse>,
            )
        }

        async fn rebase(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::RebaseRequest>,
        ) -> ServiceResult<pb::RebaseResponse> {
            let req = request.to_owned_message();
            let record = self
                .repo(&ctx, GitAccess::Write, &req.namespace, &req.name)
                .await?;
            idem_key(&req.idempotency_key)?;
            let principal = caller_principal(&ctx)?;
            let writer_id = writer(&req.writer)?;
            let onto_asked = if req.onto.is_empty() {
                None
            } else {
                Some(parse_oid("onto", &req.onto)?)
            };
            let store = self.store(&record);
            for _ in 0..MAX_FENCE_RETRIES {
                let (ws, latest) = self.latest(&record, &req.workspace).await?;
                may_write_workspace(&self.repos, &ctx, &latest.owner).await?;
                if !latest.conflicts.is_empty() {
                    return Err(refuse(
                        ErrorCode::FailedPrecondition,
                        "failed_precondition",
                        "resolve the workspace's conflicts before rebasing it",
                        &[
                            ("condition", "conflicts".to_owned()),
                            ("workspace", ws.to_string()),
                            ("conflicts", latest.conflicts.len().to_string()),
                        ],
                    ));
                }
                if latest.head != latest.base {
                    return Err(refuse(
                        ErrorCode::FailedPrecondition,
                        "failed_precondition",
                        "the workspace has local commits; rebase them with git, or land them",
                        &[
                            ("condition", "local_commits".to_owned()),
                            ("workspace", ws.to_string()),
                        ],
                    ));
                }
                let repo = self.repo_snapshot(&req.namespace, &record).await?;
                let (onto, theirs) = match onto_asked {
                    Some(onto) => (onto, onto.to_hex_with_len(7).to_string()),
                    None => {
                        let track = RefName::parse(&latest.track).map_err(|_| {
                            invalid("onto", "the workspace tracks no branch; name a commit")
                        })?;
                        let tip = repo.refs.get(&track).copied().ok_or_else(|| {
                            invalid("onto", format!("{track} has no commit; name one"))
                        })?;
                        (tip, track.to_string())
                    }
                };
                if onto.as_bytes() == latest.base.as_slice() {
                    return Response::ok(pb::RebaseResponse {
                        snapshot: MessageField::some(snapshot_pb(&latest)),
                        ..Default::default()
                    });
                }
                let view = view_of(&store, &record.repo_id, &latest)
                    .await
                    .map_err(ws_error)?;
                let odb = self.odb(&record, &repo, &ws, &view).await?;
                let onto_tree = commit_tree(&odb, "onto", &onto)
                    .await
                    .map_err(save_check_error)?;
                let base_tree = if latest.base.is_empty() {
                    empty_tree()
                } else {
                    let base = loams_git::ObjectId::try_from(latest.base.as_slice())
                        .map_err(|_| internal("the workspace's base", &"not an object id"))?;
                    commit_tree(&odb, "base", &base)
                        .await
                        .map_err(save_check_error)?
                };
                let labels = MergeLabels {
                    ours: "workspace",
                    base: "base",
                    theirs: &theirs,
                };
                let rebased = rebase_trees(
                    &odb,
                    base_tree,
                    onto_tree,
                    tree_of(&latest.worktree_tree, "worktree_tree")?,
                    tree_of(&latest.index_tree, "index_tree")?,
                    labels,
                )
                .await
                .map_err(edit_error)?;
                let conflict_paths: Vec<String> =
                    rebased.conflicts.iter().map(|c| c.path.clone()).collect();
                let moved = onto.as_bytes().to_vec();
                let mut refs = latest.refs.clone();
                for local in &mut refs {
                    if local.name == latest.head_ref {
                        local.oid = moved.clone();
                    }
                }
                let mut next = successor(
                    &gitpb::WorkspaceSnapshot {
                        base: moved.clone(),
                        base_seq: repo.seq.0,
                        head: moved,
                        refs,
                        ..(*latest).clone()
                    },
                    rebased.worktree_tree,
                    rebased.index_tree,
                    rebased.conflicts,
                    &principal,
                    writer_id.as_ref(),
                )?;
                next.packs = self
                    .store_objects(&record, &ws, &next, rebased.objects, &latest.packs)
                    .await?;
                match self.append(&store, &record, &ws, &next).await? {
                    SnapAppended::Committed { .. } => {
                        return Response::ok(pb::RebaseResponse {
                            snapshot: MessageField::some(snapshot_pb(&next)),
                            conflicts: conflict_paths,
                            ..Default::default()
                        });
                    }
                    // Another save took the number: rebase the new latest.
                    SnapAppended::Fenced { .. } => {}
                }
            }
            Err(refuse(
                ErrorCode::Aborted,
                "workspace_fenced",
                "the workspace kept changing while it was rebased; retry",
                &[],
            ))
        }
    }

    /// One `WatchWorkspace` stream's producer.
    struct Watch {
        store: BucketWorkspaceStore,
        saved: broadcast::Receiver<(loams_git::RepoId, WsId)>,
        repo: loams_git::RepoId,
        ws: WsId,
        heartbeat: Duration,
        tx: mpsc::Sender<Result<pb::WatchWorkspaceResponse, ConnectError>>,
    }

    impl Watch {
        /// Sends one message; `false` once the client has gone.
        async fn send(&self, message: Result<pb::WatchWorkspaceResponse, ConnectError>) -> bool {
            self.tx.send(message).await.is_ok()
        }

        async fn send_snapshot(&self, snap: &gitpb::WorkspaceSnapshot) -> bool {
            self.send(Ok(pb::WatchWorkspaceResponse {
                event: Some(watch_workspace_response::Event::from(snapshot_pb(snap))),
                ..Default::default()
            }))
            .await
        }

        /// Sends every snapshot after `at`; the last one sent (or `at`), and whether the
        /// stream goes on (the client is there, and the workspace was not deleted).
        async fn catch_up(&self, mut at: u64) -> (u64, bool) {
            loop {
                match self.store.get(&self.repo, &self.ws, SnapN(at + 1)).await {
                    Ok(Some(snap)) => {
                        if !self.send_snapshot(&snap).await {
                            return (at, false);
                        }
                        at = snap.n;
                        if snap.label == DELETED_LABEL {
                            return (at, false);
                        }
                    }
                    Ok(None) => return (at, true),
                    Err(err) => {
                        self.send(Err(ws_error(err))).await;
                        return (at, false);
                    }
                }
            }
        }

        async fn run(mut self, latest: Arc<gitpb::WorkspaceSnapshot>, after_n: Option<u64>) {
            let mut at = match after_n {
                Some(n) => n,
                None => {
                    if !self.send_snapshot(&latest).await {
                        return;
                    }
                    latest.n
                }
            };
            let mut quiet = tokio::time::Instant::now();
            let mut poll = tokio::time::interval(WATCH_POLL);
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let (next, going) = self.catch_up(at).await;
                if next != at {
                    quiet = tokio::time::Instant::now();
                }
                at = next;
                if !going {
                    return;
                }
                // Wait for a reason to look again: a save of this workspace through this node,
                // or the poll for saves through others; heartbeats meanwhile.
                loop {
                    tokio::select! {
                        saved = self.saved.recv() => match saved {
                            Ok((repo, ws)) if repo != self.repo || ws != self.ws => {}
                            Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => break,
                            Err(broadcast::error::RecvError::Closed) => return,
                        },
                        _ = poll.tick() => break,
                        () = tokio::time::sleep_until(quiet + self.heartbeat) => {
                            let beat = pb::WatchWorkspaceResponse {
                                event: Some(watch_workspace_response::Event::from(pb::Heartbeat {
                                    time: MessageField::some(timestamp(now_ms())),
                                    ..Default::default()
                                })),
                                ..Default::default()
                            };
                            if !self.send(Ok(beat)).await {
                                return;
                            }
                            quiet = tokio::time::Instant::now();
                        }
                        () = self.tx.closed() => return,
                    }
                }
            }
        }
    }
}
