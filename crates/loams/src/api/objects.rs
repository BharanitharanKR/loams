// The generated service traits return `impl Encodable<_>`; these impls name the concrete message
// type, which is the intended refinement (as `connect.rs` does).
#![allow(refining_impl_trait)]

//! `loams.repos.v1.ObjectService` on the main port and the immutable object route on the git
//! listener (Loams Git, GT1 Tasks 18 and 20; design §36 §19.5.1, §19.8, D829, D830, D833).
//!
//! With the `git` feature `GetObjects`, `GetTrees`, `HasObjects`, `Log` and `DiffStat` are
//! served over the repository's packs (`loams_git::odb::RangeOdb`, through the server's H1 cache) and, when a
//! request names a workspace, the workspace's packs after them. Callers need the org role
//! `member`, as for every `RepoService` read; whoever may read a repository may read its
//! workspaces (GT-Q13's default, ruling T18.2). Without the feature every RPC answers
//! `feature_not_in_variant`.

use std::sync::Arc;

use connectrpc::{
    ConnectError, RequestContext, Router, ServiceRequest, ServiceResult, ServiceStream,
};
use loams_proto::loams::repos::v1 as pb;
use loams_proto::loams::repos::v1::{ObjectService, ObjectServiceExt};

use super::AppState;
use super::repos::{PACKAGE, SERVED};

/// Registers `ObjectService`: served with the `git` feature and a git runtime, else the stub.
pub(super) fn register(router: Router, state: &AppState) -> Router {
    #[cfg(feature = "git")]
    if let Some(git) = &state.git {
        return Arc::new(served::ObjectsServed {
            repos: super::repos::served::ReposServed {
                meta: Arc::clone(&state.meta),
                git: Arc::clone(git),
                principals: state.principals.clone(),
            },
        })
        .register(router);
    }
    let _ = state;
    Arc::new(ObjectsAbsent).register(router)
}

/// `ObjectService` when this binary does not serve it.
#[derive(Debug)]
struct ObjectsAbsent;

fn absent(method: &str) -> ConnectError {
    let rpc = format!("{PACKAGE}.ObjectService/{method}");
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

impl ObjectService for ObjectsAbsent {
    async fn get_objects(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::GetObjectsRequest>,
    ) -> ServiceResult<ServiceStream<pb::GetObjectsResponse>> {
        Err(absent("GetObjects"))
    }

    async fn get_trees(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::GetTreesRequest>,
    ) -> ServiceResult<pb::GetTreesResponse> {
        Err(absent("GetTrees"))
    }

    async fn has_objects(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::HasObjectsRequest>,
    ) -> ServiceResult<pb::HasObjectsResponse> {
        Err(absent("HasObjects"))
    }

    async fn log(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::LogRequest>,
    ) -> ServiceResult<ServiceStream<pb::LogResponse>> {
        Err(absent("Log"))
    }

    async fn diff_stat(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::DiffStatRequest>,
    ) -> ServiceResult<pb::DiffStatResponse> {
        Err(absent("DiffStat"))
    }
}

#[cfg(feature = "git")]
pub(crate) use served::{ObjectRoute, serve_object};

#[cfg(feature = "git")]
mod served {
    use std::sync::Arc;

    use axum::http::{HeaderValue, StatusCode, header};
    use axum::response::{IntoResponse, Response as HttpResponse};
    use buffa::{EnumValue, MessageField};
    use connectrpc::{
        ConnectError, ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult,
        ServiceStream,
    };
    use futures::StreamExt;
    use loams_git::catalog::RepoRecord;
    use loams_git::derived::DerivedError;
    use loams_git::history::{
        ChangeKind, ChangedPath, HistoryError, LogEntry, LogFields, LogQuery, Pathspec, Signature,
        diff_stat, log, resolve_tree,
    };
    use loams_git::objects::{
        Kind, ObjectRead, ObjectsError, WsView, bitmap, get_objects, get_trees,
        grant_reads_workspace, has_objects, loose_object, workspace_odb,
    };
    use loams_git::odb::{OdbError, RangeOdb};
    use loams_git::{
        DerivedStore, ForkError, GitAccess, GitGrant, NamespaceId, ObjectId, ReadAt, RefSnapshot,
        RepoPaths, StoreBlobStore, WsId, open_fork_range_odb,
    };
    use loams_proto::loams::repos::v1 as pb;
    use loams_proto::loams::repos::v1::ObjectService;
    use tokio::sync::mpsc;

    use crate::api::connect::refuse;
    use crate::api::repos::GitRuntime;
    use crate::api::repos::served::{ReposServed, invalid, repo_name, token, unavailable};

    /// Commits a `Log` stream buffers before its walk waits.
    const LOG_BUFFER: usize = 64;
    /// The largest `data` of one `GetObjectsResponse`; larger objects come in chunks.
    const CHUNK: usize = 1024 * 1024;
    /// The `Cache-Control` of the object route: objects are immutable (§19.5.1).
    const IMMUTABLE: &str = "private, max-age=31536000, immutable";

    /// `ObjectService`, served.
    #[derive(Debug)]
    pub(super) struct ObjectsServed {
        pub(super) repos: ReposServed,
    }

    /// The objects `snapshot` of `record` reads, with workspace `ws` searched last.
    pub(crate) async fn open_odb(
        git: &GitRuntime,
        record: &RepoRecord,
        snapshot: &RefSnapshot,
        ws: Option<(&WsId, &WsView)>,
    ) -> Result<RangeOdb, String> {
        let failed = |err: &dyn std::fmt::Display| err.to_string();
        let odb = open_fork_range_odb(
            &git.store,
            record.ns,
            &record.repo_id,
            snapshot,
            Some(git.h1.clone()),
        )
        .await
        .map_err(|err: ForkError| failed(&err))?;
        let Some((ws, view)) = ws else {
            return Ok(odb);
        };
        let paths = RepoPaths::new(record.ns, &record.repo_id);
        let blobs = StoreBlobStore::workspace(git.store.clone(), &paths, ws)
            .with_range_cache(git.h1.clone());
        workspace_odb(&odb, Arc::new(blobs), view).map_err(|err| failed(&err))
    }

    /// The workspace a request names (`None` for an empty field).
    async fn workspace(
        git: &GitRuntime,
        record: &RepoRecord,
        value: &str,
    ) -> Result<Option<(WsId, WsView)>, ConnectError> {
        if value.is_empty() {
            return Ok(None);
        }
        let ws = WsId::parse(value).map_err(|err| invalid("workspace", err.to_string()))?;
        match git.workspaces.workspace(&record.repo_id, &ws).await {
            Ok(Some(view)) => Ok(Some((ws, view))),
            Ok(None) => Err(workspace_not_found(value)),
            Err(err) => Err(unavailable("the workspace lookup", &err)),
        }
    }

    fn workspace_not_found(ws: &str) -> ConnectError {
        refuse(
            ErrorCode::NotFound,
            "workspace_not_found",
            format!("no workspace {ws:?}"),
            &[("workspace", ws.to_owned())],
        )
    }

    fn object_missing(oid: &ObjectId) -> ConnectError {
        refuse(
            ErrorCode::FailedPrecondition,
            "object_missing",
            format!("object {oid} is not in the repository"),
            &[("oid", oid.to_string())],
        )
    }

    fn internal(what: &str, err: &dyn std::fmt::Display) -> ConnectError {
        tracing::error!(error = %err, "{what}");
        refuse(ErrorCode::Internal, "internal", what.to_owned(), &[])
    }

    fn odb_error(err: OdbError) -> ConnectError {
        match err {
            OdbError::NotFound(oid) => object_missing(&oid),
            OdbError::Blob(err) => unavailable("reading packs", &err),
            other => internal("a corrupt pack or object", &other),
        }
    }

    fn objects_error(err: ObjectsError) -> ConnectError {
        match err {
            ObjectsError::TooMany { max, got } => {
                invalid("oids", format!("{got} object ids; at most {max} per call"))
            }
            ObjectsError::TooDeep { max, got } => {
                invalid("depth", format!("depth {got}; at most {max}"))
            }
            ObjectsError::NotFound(oid) => object_missing(&oid),
            ObjectsError::Lookup(reason) => unavailable("the workspace lookup", &reason),
            ObjectsError::Odb(err) => odb_error(err),
            ObjectsError::Derived(DerivedError::WrongKind { oid, found, .. }) => invalid(
                "root",
                format!("{oid} is a {found}, not a tree, commit or tag"),
            ),
            ObjectsError::Derived(DerivedError::Odb(err)) => odb_error(err),
            ObjectsError::Derived(DerivedError::Store(err)) => unavailable("derived data", &err),
            ObjectsError::Derived(err) => internal("corrupt derived data", &err),
        }
    }

    fn history_error(err: HistoryError) -> ConnectError {
        match err {
            HistoryError::Pathspec { spec, reason } => {
                invalid("paths", format!("pathspec {spec:?}: {reason}"))
            }
            HistoryError::Rev { rev, reason } => {
                invalid("revs", format!("revision {rev:?}: {reason}"))
            }
            HistoryError::TooMany { what, max, got } => {
                invalid(what, format!("{got} {what}; at most {max} per call"))
            }
            HistoryError::Odb(err) => odb_error(err),
            HistoryError::Derived(DerivedError::Odb(err)) => odb_error(err),
            HistoryError::Derived(DerivedError::Store(err)) => {
                unavailable("the commit graph", &err)
            }
            HistoryError::Derived(err) => internal("a corrupt commit graph", &err),
        }
    }

    fn signature_pb(sig: Signature) -> pb::Signature {
        pb::Signature {
            name: sig.name,
            email: sig.email,
            time: sig.time,
            tz: sig.tz,
            ..Default::default()
        }
    }

    fn changed_pb(change: ChangedPath) -> pb::ChangedPath {
        let kind = match change.kind {
            ChangeKind::Added => pb::ChangeKind::CHANGE_KIND_ADDED,
            ChangeKind::Deleted => pb::ChangeKind::CHANGE_KIND_DELETED,
            ChangeKind::Modified => pb::ChangeKind::CHANGE_KIND_MODIFIED,
            ChangeKind::TypeChanged => pb::ChangeKind::CHANGE_KIND_TYPE_CHANGED,
        };
        pb::ChangedPath {
            path: change.path,
            kind: EnumValue::Known(kind),
            old_mode: change.old.map(|(mode, _)| mode),
            old_oid: change
                .old
                .map(|(_, oid)| oid.to_string())
                .unwrap_or_default(),
            new_mode: change.new.map(|(mode, _)| mode),
            new_oid: change
                .new
                .map(|(_, oid)| oid.to_string())
                .unwrap_or_default(),
            ..Default::default()
        }
    }

    fn log_commit(entry: LogEntry) -> pb::LogCommit {
        pb::LogCommit {
            oid: entry.oid.to_string(),
            tree: entry.tree.to_string(),
            parents: entry.parents.iter().map(ToString::to_string).collect(),
            author: match entry.author {
                Some(sig) => MessageField::some(signature_pb(sig)),
                None => MessageField::none(),
            },
            committer: match entry.committer {
                Some(sig) => MessageField::some(signature_pb(sig)),
                None => MessageField::none(),
            },
            message: entry.message.map(|m| m.to_vec()).unwrap_or_default(),
            changed: entry
                .changed
                .unwrap_or_default()
                .into_iter()
                .map(changed_pb)
                .collect(),
            ..Default::default()
        }
    }

    fn log_fields(values: &[EnumValue<pb::LogField>]) -> Result<LogFields, ConnectError> {
        if values.is_empty() {
            return Ok(LogFields::default());
        }
        let mut fields = LogFields {
            signatures: false,
            message: false,
            changed: false,
        };
        for value in values {
            match value {
                EnumValue::Known(pb::LogField::LOG_FIELD_SIGNATURES) => fields.signatures = true,
                EnumValue::Known(pb::LogField::LOG_FIELD_MESSAGE) => fields.message = true,
                EnumValue::Known(pb::LogField::LOG_FIELD_CHANGED) => fields.changed = true,
                _ => return Err(invalid("fields", "an unknown log field")),
            }
        }
        Ok(fields)
    }

    fn oids(field: &str, values: &[String]) -> Result<Vec<ObjectId>, ConnectError> {
        values.iter().map(|value| parse_oid(field, value)).collect()
    }

    fn parse_oid(field: &str, value: &str) -> Result<ObjectId, ConnectError> {
        let hex = value.len() == 40
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !hex {
            return Err(invalid(
                field,
                format!("{value:?} is not 40 lowercase hex digits"),
            ));
        }
        ObjectId::from_hex(value.as_bytes()).map_err(|err| invalid(field, err.to_string()))
    }

    fn kind_pb(kind: Kind) -> pb::ObjectKind {
        match kind {
            Kind::Commit => pb::ObjectKind::OBJECT_KIND_COMMIT,
            Kind::Tree => pb::ObjectKind::OBJECT_KIND_TREE,
            Kind::Blob => pb::ObjectKind::OBJECT_KIND_BLOB,
            Kind::Tag => pb::ObjectKind::OBJECT_KIND_TAG,
        }
    }

    /// One read's chunks.
    fn chunks(read: ObjectRead, token: &str) -> Vec<pb::GetObjectsResponse> {
        match read {
            ObjectRead::Missing(oid) => vec![pb::GetObjectsResponse {
                oid: oid.to_string(),
                missing: true,
                consistency_token: token.to_owned(),
                ..Default::default()
            }],
            ObjectRead::Found { oid, kind, data } => {
                let size = data.len() as u64;
                let mut out = Vec::new();
                let mut offset = 0usize;
                loop {
                    let end = data.len().min(offset + CHUNK);
                    out.push(pb::GetObjectsResponse {
                        oid: oid.to_string(),
                        kind: EnumValue::Known(kind_pb(kind)),
                        size,
                        offset: offset as u64,
                        data: data.slice(offset..end).to_vec(),
                        consistency_token: token.to_owned(),
                        ..Default::default()
                    });
                    offset = end;
                    if offset >= data.len() {
                        return out;
                    }
                }
            }
        }
    }

    impl ObjectsServed {
        /// The repository a request names, its snapshot and the object database to read.
        async fn open(
            &self,
            ctx: &RequestContext,
            ns_name: &str,
            name: &str,
            ws: &str,
            consistency: &str,
        ) -> Result<(RepoRecord, Arc<RefSnapshot>, Arc<RangeOdb>), ConnectError> {
            self.repos.require(ctx, GitAccess::Read).await?;
            let name = repo_name("name", name)?;
            let ns = self.repos.namespace(ns_name, false).await?;
            let record = self.repos.active(ns, &name).await?;
            let (_, snapshot) = self.repos.read(ns_name, &record, consistency).await?;
            let ws_view = workspace(&self.repos.git, &record, ws).await?;
            let odb = open_odb(
                &self.repos.git,
                &record,
                &snapshot,
                ws_view.as_ref().map(|(id, view)| (id, view)),
            )
            .await
            .map_err(|reason| unavailable("opening the repository's objects", &reason))?;
            Ok((record, snapshot, Arc::new(odb)))
        }
    }

    impl ObjectsServed {
        fn derived(&self, record: &RepoRecord) -> DerivedStore {
            DerivedStore::new(
                self.repos.git.store.clone(),
                RepoPaths::new(record.ns, &record.repo_id),
            )
            .with_range_cache(self.repos.git.h1.clone())
        }
    }

    impl ObjectService for ObjectsServed {
        async fn get_objects(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::GetObjectsRequest>,
        ) -> ServiceResult<ServiceStream<pb::GetObjectsResponse>> {
            let req = request.to_owned_message();
            let wanted = oids("oids", &req.oids)?;
            let (record, snapshot, odb) = self
                .open(
                    &ctx,
                    &req.namespace,
                    &req.name,
                    &req.workspace,
                    &req.consistency_token,
                )
                .await?;
            let at = token(&record.repo_id, snapshot.seq);
            let reads = get_objects(odb, wanted).await.map_err(objects_error)?;
            let stream = reads.flat_map(move |read| {
                let messages: Vec<Result<pb::GetObjectsResponse, ConnectError>> = match read {
                    Ok(read) => chunks(read, &at).into_iter().map(Ok).collect(),
                    Err(err) => vec![Err(objects_error(err))],
                };
                futures::stream::iter(messages)
            });
            Response::ok(Box::pin(stream) as ServiceStream<pb::GetObjectsResponse>)
        }

        async fn get_trees(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::GetTreesRequest>,
        ) -> ServiceResult<pb::GetTreesResponse> {
            let req = request.to_owned_message();
            let root = parse_oid("root", &req.root)?;
            let depth = match req.depth {
                None => loams_git::limits::TREE_PREFETCH_DEPTH,
                Some(depth) => u8::try_from(depth)
                    .ok()
                    .filter(|depth| *depth <= loams_git::objects::MAX_TREE_DEPTH)
                    .ok_or_else(|| invalid("depth", "0 to 8"))?,
            };
            let (record, snapshot, odb) = self
                .open(
                    &ctx,
                    &req.namespace,
                    &req.name,
                    &req.workspace,
                    &req.consistency_token,
                )
                .await?;
            let derived = self.derived(&record);
            let got = get_trees(&odb, &derived, &root, depth)
                .await
                .map_err(objects_error)?;
            Response::ok(pb::GetTreesResponse {
                trees: got
                    .trees
                    .iter()
                    .map(|tree| pb::Tree {
                        oid: tree.tree.to_string(),
                        entries: tree
                            .entries
                            .iter()
                            .map(|entry| pb::TreeEntry {
                                name: entry.name.clone(),
                                mode: entry.mode,
                                oid: entry.oid.to_string(),
                                size: entry.size,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                truncated: got.truncated,
                consistency_token: token(&record.repo_id, snapshot.seq),
                ..Default::default()
            })
        }

        async fn has_objects(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::HasObjectsRequest>,
        ) -> ServiceResult<pb::HasObjectsResponse> {
            let req = request.to_owned_message();
            let wanted = oids("oids", &req.oids)?;
            let (record, snapshot, odb) = self
                .open(
                    &ctx,
                    &req.namespace,
                    &req.name,
                    &req.workspace,
                    &req.consistency_token,
                )
                .await?;
            let held = has_objects(&odb, &wanted).await.map_err(objects_error)?;
            Response::ok(pb::HasObjectsResponse {
                present: bitmap(&held),
                consistency_token: token(&record.repo_id, snapshot.seq),
                ..Default::default()
            })
        }

        async fn log(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::LogRequest>,
        ) -> ServiceResult<ServiceStream<pb::LogResponse>> {
            let req = request.to_owned_message();
            let query = LogQuery {
                revs: req.revs.clone(),
                pathspec: Pathspec::parse(&req.paths).map_err(history_error)?,
                limit: req.limit.map(|limit| limit as usize),
                fields: log_fields(&req.fields)?,
            };
            // The commit graph holds repository commits only, so `Log` takes no workspace.
            let (record, snapshot, odb) = self
                .open(&ctx, &req.namespace, &req.name, "", &req.consistency_token)
                .await?;
            let derived = self.derived(&record);
            let at = token(&record.repo_id, snapshot.seq);
            let (tx, mut rx) = mpsc::channel::<Result<pb::LogResponse, ConnectError>>(LOG_BUFFER);
            tokio::spawn(async move {
                let (entries_tx, mut entries_rx) = mpsc::channel::<LogEntry>(LOG_BUFFER);
                let forward = {
                    let tx = tx.clone();
                    async move {
                        while let Some(entry) = entries_rx.recv().await {
                            let message = pb::LogResponse {
                                commit: MessageField::some(log_commit(entry)),
                                consistency_token: at.clone(),
                                ..Default::default()
                            };
                            if tx.send(Ok(message)).await.is_err() {
                                return;
                            }
                        }
                    }
                };
                let walk = async move {
                    let result = log(&odb, &derived, &snapshot, &query, &entries_tx).await;
                    drop(entries_tx);
                    result
                };
                let (result, ()) = tokio::join!(walk, forward);
                if let Err(err) = result {
                    let _ = tx.send(Err(history_error(err))).await;
                }
            });
            Response::ok(
                Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
                    as ServiceStream<pb::LogResponse>,
            )
        }

        async fn diff_stat(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, pb::DiffStatRequest>,
        ) -> ServiceResult<pb::DiffStatResponse> {
            let req = request.to_owned_message();
            let spec = Pathspec::parse(&req.paths).map_err(history_error)?;
            let (record, snapshot, odb) = self
                .open(
                    &ctx,
                    &req.namespace,
                    &req.name,
                    &req.workspace,
                    &req.consistency_token,
                )
                .await?;
            let from = resolve_tree(&odb, &snapshot, &req.from_rev)
                .await
                .map_err(history_error)?;
            let to = resolve_tree(&odb, &snapshot, &req.to_rev)
                .await
                .map_err(history_error)?;
            let files = diff_stat(&odb, Some(from), Some(to), &spec)
                .await
                .map_err(history_error)?;
            Response::ok(pb::DiffStatResponse {
                files: files
                    .into_iter()
                    .map(|file| pb::FileStat {
                        change: MessageField::some(changed_pb(file.change)),
                        added: file.added,
                        removed: file.removed,
                        ..Default::default()
                    })
                    .collect(),
                consistency_token: token(&record.repo_id, snapshot.seq),
                ..Default::default()
            })
        }
    }

    /// What the git listener asks of the object route.
    #[derive(Debug)]
    pub(crate) struct ObjectRoute<'a> {
        pub(crate) grant: &'a GitGrant,
        pub(crate) ns: NamespaceId,
        pub(crate) ns_name: &'a str,
        pub(crate) record: &'a RepoRecord,
        pub(crate) oid: &'a str,
        pub(crate) workspace: Option<&'a str>,
    }

    fn text(status: StatusCode, body: &'static str) -> HttpResponse {
        (status, body).into_response()
    }

    /// `GET /git/<ns>/<repo>.git/loams/objects/<oid>[?ws=<ws_id>]` (§19.5.1): the object in
    /// git's loose format, cacheable for a year. Objects only in a workspace's packs need the
    /// workspace, and a token whose `WsMatch` reaches it (ruling T18.2); otherwise they do not
    /// exist (404).
    pub(crate) async fn serve_object(git: &GitRuntime, route: ObjectRoute<'_>) -> HttpResponse {
        let unavailable = || text(StatusCode::SERVICE_UNAVAILABLE, "loams: try again later\n");
        // The listener's one 404, so a hidden object looks like any other absent one.
        let not_found = crate::api::git_auth::not_found;
        if route
            .grant
            .require(route.ns, &route.record.name, GitAccess::Read)
            .is_err()
        {
            return not_found();
        }
        let Ok(oid) = parse_oid("oid", route.oid) else {
            return not_found();
        };
        let ws = match route.workspace {
            None | Some("") => None,
            Some(value) => {
                let Ok(ws) = WsId::parse(value) else {
                    return not_found();
                };
                match git.workspaces.workspace(&route.record.repo_id, &ws).await {
                    Ok(Some(view)) if grant_reads_workspace(route.grant, &ws, &view) => {
                        Some((ws, view))
                    }
                    Ok(_) => return not_found(),
                    Err(err) => {
                        tracing::warn!(error = %err, "the workspace lookup failed");
                        return unavailable();
                    }
                }
            }
        };
        let log = match git
            .sequencers
            .get(
                route.ns,
                &format!("local/{}", route.ns_name),
                &route.record.repo_id,
            )
            .await
        {
            Ok(log) => log,
            Err(err) => {
                tracing::warn!(error = %err, "opening a repository for the object route failed");
                return unavailable();
            }
        };
        let snapshot = match loams_git::RefLog::snapshot(&log, ReadAt::Latest).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                tracing::warn!(error = %err, "reading a repository for the object route failed");
                return unavailable();
            }
        };
        let odb = match open_odb(
            git,
            route.record,
            &snapshot,
            ws.as_ref().map(|(id, v)| (id, v)),
        )
        .await
        {
            Ok(odb) => odb,
            Err(err) => {
                tracing::warn!(error = %err, "opening objects for the object route failed");
                return unavailable();
            }
        };
        match loose_object(&odb, &oid).await {
            Ok(Some(body)) => {
                let mut response = (StatusCode::OK, body).into_response();
                let headers = response.headers_mut();
                headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-git-loose-object"),
                );
                headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(IMMUTABLE));
                response
            }
            Ok(None) => not_found(),
            Err(err) => {
                tracing::warn!(error = %err, "reading an object for the object route failed");
                unavailable()
            }
        }
    }
}
