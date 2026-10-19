// The generated service traits return `impl Encodable<_>`; these impls name the concrete message
// type, which is the intended refinement (as `connect.rs` does).
#![allow(refining_impl_trait)]

//! `loams.graph.v1` on the main port (GR1 Task 5, design §48 §8, D741).
//!
//! With the `graph` feature and a graph runtime (dev and standalone), both services are served
//! over [`GraphAdmin`](loams_graph::service::admin::GraphAdmin): the catalog, the lazily opened
//! engine, and every engine call on the blocking pool. Otherwise every RPC answers
//! `unimplemented` with the reason `feature_not_in_variant` ([`GraphAbsent`]), so a caller can
//! tell "not in this build" from a missing route, and reflection never lists a service no route
//! answers.
//!
//! Every RPC is authorized per graph (GR1 Task 24, §48 §11, D750): the principal the port's
//! authentication layer admitted becomes a [`Caller`](loams_graph::authz::Caller), and
//! [`graph_auth`](super::graph_auth)'s built-in RBAC answers its relation on the graph from the
//! principal store's org and project roles. A search's expansion and the SQL graph and algorithm
//! table functions are not RPCs but are a principal's reads, checked the same way for `reader`
//! through [`graph_caller`](super::graph_caller) (GR1 R24.12); Loams' own calls into
//! the runtime on no principal's behalf (links, write-backs, forwarded calls) are not checked.
//!
//! A statement's deadline is the earlier of its own limit (`timeout_ms`, or the graph's) and the
//! client's Connect timeout ([`GraphAdmin::for_call`](loams_graph::service::admin::GraphAdmin::for_call),
//! GR1 Task 6).
//!
//! Consistency (GR1 Task 15, §48 §7.2): a `loams-consistency-token` request header is applied to
//! the request's own `consistency` the way `loams.collection.v1` applies it (it turns `strong`,
//! `eventual` or none into `at_least`, and merges into an `at_least`), and `Execute` and
//! `ExecuteBatch` answer their token in the same header as well as in the message.
//! `ExecuteStream`'s token is in its last chunk only: its headers go out before the statement
//! has run.
//!
//! Not served yet, answered `not_implemented`: `RestoreGraph`, `ExportGraph`, `ImportGraph`
//! (Task 29).

use std::sync::Arc;

#[cfg(feature = "graph")]
use connectrpc::Response;
use connectrpc::{RequestContext, Router, ServiceRequest, ServiceResult, ServiceStream};
use loams_proto::loams::graph::v1 as pb;
use loams_proto::loams::graph::v1::{
    GraphAdminService, GraphAdminServiceExt, GraphService, GraphServiceExt,
};
use loams_proto::loams::operations::v1::Operation;

use super::AppState;
#[cfg(feature = "graph")]
use super::connect::not_implemented;
use super::connect::not_in_variant;

/// Whether this server serves `loams.graph.v1` (for `GetInstance.services[]`).
pub(crate) fn served(state: &AppState) -> bool {
    #[cfg(feature = "graph")]
    {
        state.graph.is_some()
    }
    #[cfg(not(feature = "graph"))]
    {
        let _ = state;
        false
    }
}

/// Registers both graph services: served, or the `feature_not_in_variant` stub.
pub(super) fn register(router: Router, state: &AppState) -> Router {
    #[cfg(feature = "graph")]
    if let Some(admin) = &state.graph {
        // Every RPC is checked against the principal store's roles (GR1 Task 24, D750); the
        // runtime's own admin, which Loams' internal callers use, stays unchecked.
        let admin = admin
            .as_ref()
            .clone()
            .with_authorizer(super::graph_auth::authorizer(&state.principals));
        let served = Arc::new(Served {
            admin: Arc::new(admin),
        });
        let router = GraphAdminServiceExt::register(Arc::clone(&served), router);
        return GraphServiceExt::register(served, router);
    }
    let _ = state;
    let absent = Arc::new(GraphAbsent);
    let router = GraphAdminServiceExt::register(Arc::clone(&absent), router);
    GraphServiceExt::register(absent, router)
}

/// Both graph services when this server does not serve them.
#[derive(Debug)]
struct GraphAbsent;

impl GraphAdminService for GraphAbsent {
    async fn get_engine_info(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::GetEngineInfoRequest>,
    ) -> ServiceResult<pb::EngineInfo> {
        Err(not_in_variant(
            "loams.graph.v1.GraphAdminService/GetEngineInfo",
        ))
    }
    async fn create_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::CreateGraphRequest>,
    ) -> ServiceResult<pb::Graph> {
        Err(not_in_variant(
            "loams.graph.v1.GraphAdminService/CreateGraph",
        ))
    }
    async fn get_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::GetGraphRequest>,
    ) -> ServiceResult<pb::Graph> {
        Err(not_in_variant("loams.graph.v1.GraphAdminService/GetGraph"))
    }
    async fn list_graphs(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ListGraphsRequest>,
    ) -> ServiceResult<pb::ListGraphsResponse> {
        Err(not_in_variant(
            "loams.graph.v1.GraphAdminService/ListGraphs",
        ))
    }
    async fn update_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::UpdateGraphRequest>,
    ) -> ServiceResult<pb::Graph> {
        Err(not_in_variant(
            "loams.graph.v1.GraphAdminService/UpdateGraph",
        ))
    }
    async fn delete_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::DeleteGraphRequest>,
    ) -> ServiceResult<Operation> {
        Err(not_in_variant(
            "loams.graph.v1.GraphAdminService/DeleteGraph",
        ))
    }
    async fn get_schema(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::GetSchemaRequest>,
    ) -> ServiceResult<pb::GraphSchema> {
        Err(not_in_variant("loams.graph.v1.GraphAdminService/GetSchema"))
    }
    async fn restore_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::RestoreGraphRequest>,
    ) -> ServiceResult<Operation> {
        Err(not_in_variant(
            "loams.graph.v1.GraphAdminService/RestoreGraph",
        ))
    }
    async fn export_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ExportGraphRequest>,
    ) -> ServiceResult<Operation> {
        Err(not_in_variant(
            "loams.graph.v1.GraphAdminService/ExportGraph",
        ))
    }
    async fn import_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ImportGraphRequest>,
    ) -> ServiceResult<Operation> {
        Err(not_in_variant(
            "loams.graph.v1.GraphAdminService/ImportGraph",
        ))
    }
}

impl GraphService for GraphAbsent {
    async fn execute(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ExecuteRequest>,
    ) -> ServiceResult<pb::ExecuteResponse> {
        Err(not_in_variant("loams.graph.v1.GraphService/Execute"))
    }
    async fn execute_batch(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ExecuteBatchRequest>,
    ) -> ServiceResult<pb::ExecuteBatchResponse> {
        Err(not_in_variant("loams.graph.v1.GraphService/ExecuteBatch"))
    }
    async fn explain(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ExplainRequest>,
    ) -> ServiceResult<pb::Plan> {
        Err(not_in_variant("loams.graph.v1.GraphService/Explain"))
    }

    async fn execute_stream(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ExecuteStreamRequest>,
    ) -> ServiceResult<ServiceStream<pb::ResultChunk>> {
        Err(not_in_variant("loams.graph.v1.GraphService/ExecuteStream"))
    }
}

/// Both graph services over the server's graph runtime.
#[cfg(feature = "graph")]
#[derive(Debug)]
struct Served {
    admin: Arc<loams_graph::service::admin::GraphAdmin>,
}

#[cfg(feature = "graph")]
impl Served {
    /// The admin for one call: the caller the authentication layer admitted (anonymous without
    /// one, which reaches nothing; GR1 Task 24) and the client's deadline.
    fn admin_for(&self, ctx: &RequestContext) -> loams_graph::service::admin::GraphAdmin {
        let caller = super::graph_auth::caller_of(ctx.extensions().get::<crate::auth::Principal>());
        self.admin.for_call(ctx.deadline()).for_caller(caller)
    }
}

#[cfg(feature = "graph")]
impl GraphAdminService for Served {
    async fn get_engine_info(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetEngineInfoRequest>,
    ) -> ServiceResult<pb::EngineInfo> {
        Response::ok(
            self.admin_for(&ctx)
                .get_engine_info(request.to_owned_message())
                .await,
        )
    }
    async fn create_graph(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::CreateGraphRequest>,
    ) -> ServiceResult<pb::Graph> {
        Response::ok(
            self.admin_for(&ctx)
                .create_graph(request.to_owned_message())
                .await?,
        )
    }
    async fn get_graph(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetGraphRequest>,
    ) -> ServiceResult<pb::Graph> {
        Response::ok(
            self.admin_for(&ctx)
                .get_graph(request.to_owned_message())
                .await?,
        )
    }
    async fn list_graphs(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListGraphsRequest>,
    ) -> ServiceResult<pb::ListGraphsResponse> {
        Response::ok(
            self.admin_for(&ctx)
                .list_graphs(request.to_owned_message())
                .await?,
        )
    }
    async fn update_graph(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::UpdateGraphRequest>,
    ) -> ServiceResult<pb::Graph> {
        Response::ok(
            self.admin_for(&ctx)
                .update_graph(request.to_owned_message())
                .await?,
        )
    }
    async fn delete_graph(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::DeleteGraphRequest>,
    ) -> ServiceResult<Operation> {
        Response::ok(
            self.admin_for(&ctx)
                .delete_graph(request.to_owned_message())
                .await?,
        )
    }
    async fn get_schema(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetSchemaRequest>,
    ) -> ServiceResult<pb::GraphSchema> {
        Response::ok(
            self.admin_for(&ctx)
                .get_schema(request.to_owned_message())
                .await?,
        )
    }
    async fn restore_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::RestoreGraphRequest>,
    ) -> ServiceResult<Operation> {
        Err(not_implemented(
            "loams.graph.v1.GraphAdminService/RestoreGraph",
        ))
    }
    async fn export_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ExportGraphRequest>,
    ) -> ServiceResult<Operation> {
        Err(not_implemented(
            "loams.graph.v1.GraphAdminService/ExportGraph",
        ))
    }
    async fn import_graph(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ImportGraphRequest>,
    ) -> ServiceResult<Operation> {
        Err(not_implemented(
            "loams.graph.v1.GraphAdminService/ImportGraph",
        ))
    }
}

/// Applies the request's `loams-consistency-token` header, if any, to its `consistency` (GR1
/// Task 15): the rule [`read_consistency`](super::read_consistency) applies to collections.
#[cfg(feature = "graph")]
fn apply_token_header(
    ctx: &RequestContext,
    consistency: &mut buffa::MessageField<pb::Consistency, buffa::Inline<pb::Consistency>>,
) -> Result<(), connectrpc::ConnectError> {
    match ctx.headers().get(&super::CONSISTENCY_TOKEN) {
        Some(header) => loams_graph::consistency::apply_header(consistency, header.as_bytes()),
        None => Ok(()),
    }
}

/// `response` with its token in the `loams-consistency-token` header too, when it has one.
#[cfg(feature = "graph")]
fn with_token<M>(response: M, token: &str) -> Response<M> {
    let response = Response::new(response);
    if token.is_empty() {
        response
    } else {
        response.with_header(super::CONSISTENCY_TOKEN.as_str(), token.to_string())
    }
}

#[cfg(feature = "graph")]
impl GraphService for Served {
    async fn execute(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::ExecuteRequest>,
    ) -> ServiceResult<pb::ExecuteResponse> {
        let mut request = request.to_owned_message();
        apply_token_header(&ctx, &mut request.consistency)?;
        let answer = self.admin_for(&ctx).execute(request).await?;
        let token = answer.consistency_token.clone();
        Ok(with_token(answer, &token))
    }
    async fn execute_batch(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::ExecuteBatchRequest>,
    ) -> ServiceResult<pb::ExecuteBatchResponse> {
        // A batch is the owner's (it has no consistency of its own), so a request header has
        // nothing to change; the answer's token is in the header too.
        let answer = self
            .admin_for(&ctx)
            .execute_batch(request.to_owned_message())
            .await?;
        let token = answer.consistency_token.clone();
        Ok(with_token(answer, &token))
    }
    async fn explain(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::ExplainRequest>,
    ) -> ServiceResult<pb::Plan> {
        let mut request = request.to_owned_message();
        apply_token_header(&ctx, &mut request.consistency)?;
        Response::ok(self.admin_for(&ctx).explain(request).await?)
    }

    async fn execute_stream(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::ExecuteStreamRequest>,
    ) -> ServiceResult<ServiceStream<pb::ResultChunk>> {
        let mut request = request.to_owned_message();
        if let Some(inner) = request.request.as_option_mut() {
            apply_token_header(&ctx, &mut inner.consistency)?;
        } else if ctx.headers().contains_key(&super::CONSISTENCY_TOKEN) {
            let mut inner = pb::ExecuteRequest::default();
            apply_token_header(&ctx, &mut inner.consistency)?;
            request.request = inner.into();
        }
        Response::ok(self.admin_for(&ctx).execute_stream(request).await?)
    }
}

/// Graph forwarding between a cluster's graph nodes (GR1 Task 12, `GraphForward`): API1 Task 8
/// (`loams.internal.v1`) has not landed, so it is an internal REST route beside the hot and read
/// routes, `POST /internal/v1/graph/<method>` with the request's protobuf bytes, answered with
/// the response's protobuf bytes (`200`) or the Connect error as JSON (`422`). It moves to
/// `loams.internal.v1` with API1 Task 8 (R0.16). Unauthenticated, like every internal route
/// before MT1.
#[cfg(feature = "graph")]
pub(crate) const FORWARD_PATH: &str = "/internal/v1/graph/";

/// The node registry as Loams Graph's ownership sees it: the live nodes with the `graph` role.
#[cfg(feature = "graph")]
#[derive(Debug)]
pub(crate) struct RegistryMembership {
    registry: Arc<loams_hot::NodeRegistry>,
}

#[cfg(feature = "graph")]
impl RegistryMembership {
    pub(crate) fn new(registry: Arc<loams_hot::NodeRegistry>) -> Self {
        Self { registry }
    }
}

#[cfg(feature = "graph")]
fn graph_node(node: &loams_hot::NodeDescriptor) -> loams_graph::owner::GraphNode {
    loams_graph::owner::GraphNode {
        node_id: node.node_id,
        incarnation: node.incarnation,
        addr: node.addr.to_string(),
    }
}

#[cfg(feature = "graph")]
impl loams_graph::owner::Membership for RegistryMembership {
    fn me(&self) -> loams_graph::owner::GraphNode {
        graph_node(self.registry.me())
    }

    fn graph_nodes(&self) -> Vec<loams_graph::owner::GraphNode> {
        self.registry
            .live()
            .iter()
            .filter(|node| node.roles.graph)
            .map(graph_node)
            .collect()
    }
}

/// Forwards a graph call to its owner over [`FORWARD_PATH`].
#[cfg(feature = "graph")]
#[derive(Debug)]
pub(crate) struct HttpForwarder {
    client: reqwest::Client,
}

#[cfg(feature = "graph")]
impl HttpForwarder {
    pub(crate) fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(super::hot::OWNER_CONNECT_TIMEOUT)
                .build()?,
        })
    }
}

#[cfg(feature = "graph")]
impl loams_graph::owner::Forwarder for HttpForwarder {
    fn forward(
        &self,
        to: &loams_graph::owner::GraphNode,
        method: loams_graph::owner::ForwardMethod,
        request: bytes::Bytes,
        timeout: std::time::Duration,
    ) -> loams_graph::owner::ForwardFuture {
        use connectrpc::{ConnectError, ErrorCode};
        let url = format!("http://{}{FORWARD_PATH}{}", to.addr, method.as_str());
        let node = to.node_id;
        let call = self
            .client
            .post(url)
            .timeout(timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/proto")
            .body(request);
        Box::pin(async move {
            let unreachable = |err: &dyn std::fmt::Display| {
                tracing::warn!(node, error = %err, "forwarding a graph call failed");
                ConnectError::new(
                    ErrorCode::Unavailable,
                    format!("the graph's owner (node {node}) did not answer; retry"),
                )
            };
            let response = call.send().await.map_err(|err| unreachable(&err))?;
            let status = response.status();
            let body = response.bytes().await.map_err(|err| unreachable(&err))?;
            match status {
                reqwest::StatusCode::OK => Ok(body),
                // The owner's own refusal, as the Connect error it would answer.
                reqwest::StatusCode::UNPROCESSABLE_ENTITY => {
                    serde_json::from_slice::<ConnectError>(&body)
                        .map_err(|err| unreachable(&err))
                        .and_then(Err)
                }
                // A node still starting (or stopping): a retry can reach it.
                reqwest::StatusCode::SERVICE_UNAVAILABLE => Err(unreachable(&status)),
                // No such route (a node of another release), a body over its limit: a retry
                // cannot help (PR #443 review).
                other => {
                    tracing::warn!(node, status = %other, "a graph owner answered an unexpected status");
                    Err(ConnectError::new(
                        ErrorCode::Internal,
                        format!("the graph's owner (node {node}) answered {other}"),
                    ))
                }
            }
        })
    }
}

/// `POST /internal/v1/graph/{method}`: serves a call another graph node forwarded here.
#[cfg(feature = "graph")]
pub(crate) fn internal_routes() -> axum::Router<AppState> {
    use axum::extract::{DefaultBodyLimit, Path, State};
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    async fn serve(
        State(state): State<AppState>,
        Path(method): Path<String>,
        body: bytes::Bytes,
    ) -> axum::response::Response {
        let refused = |err: connectrpc::ConnectError| {
            let json = serde_json::to_vec(&err).unwrap_or_default();
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_TYPE, "application/json")],
                json,
            )
                .into_response()
        };
        let Some(admin) = &state.graph else {
            return refused(not_in_variant("loams.graph.v1 (forwarded)"));
        };
        let Some(method) = loams_graph::owner::ForwardMethod::parse(&method) else {
            return (StatusCode::NOT_FOUND, "no such graph method").into_response();
        };
        match admin.serve_forwarded(method, body).await {
            Ok(answer) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/proto")],
                answer,
            )
                .into_response(),
            Err(err) => refused(err),
        }
    }
    axum::Router::new()
        .route(
            &format!("{FORWARD_PATH}{{method}}"),
            axum::routing::post(serve),
        )
        .layer(DefaultBodyLimit::max(super::MAX_BODY_BYTES))
}
