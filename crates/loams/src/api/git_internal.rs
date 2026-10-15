//! Loams Git across a cluster (GT1 Task 14, design §36 §4.4, Rulings 26–27): the internal route
//! a repository's sequencer owner serves forwarded commits on, and the three seams
//! `loams_git::RepoSequencers` needs on a cluster node: placement over the node registry, the
//! owner lease over the metastore, and the HTTP forwarder.
//!
//! The route answers from this node whatever the placement says, so a commit is never forwarded
//! twice. Like every internal route it is unauthenticated and served only to the cluster's own
//! nodes (overview §6.9); networked serving waits for MT1 (D111, plan GT1 Ruling 30).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use loams_common::NamespaceId;
use loams_common::meta::{ApplyError, MetaError, MetaStore};
use loams_git::{
    FORWARD_PATH, LeaseError, RepoId, SeqForwarder, SeqLeases, SeqOwner, SeqPlacement,
    repo_placement_hash,
};
use loams_hot::{PlacementImpl, PlacementKey};

use super::AppState;

/// The largest forwarded commit: a transaction is at most one segment (1 MiB) of events; the
/// wire form adds a little.
const MAX_FORWARD_BYTES: usize = 2 * 1024 * 1024;

/// POST /internal/v1/git/commit: a postcard `ForwardedTxn` in, its answer out.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(FORWARD_PATH, post(commit))
        .layer(DefaultBodyLimit::max(MAX_FORWARD_BYTES))
}

async fn commit(State(state): State<AppState>, body: Bytes) -> Response {
    let Some(git) = &state.git else {
        return super::errors::unavailable(format!(
            "node {} does not serve Loams Git (no gateway role, or [git] disabled)",
            state.node_id
        ))
        .into_response();
    };
    let answer = git.sequencers.serve_forwarded(&body).await;
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        answer,
    )
        .into_response()
}

/// A repository's sequencer owner: the first `gateway` node of its rendezvous ranking (T0-13,
/// R0.6: `owners_of(key).first()`, so one node runs it even with replication above one).
#[derive(Debug)]
pub(crate) struct RegistryPlacement {
    placement: Arc<PlacementImpl>,
}

impl RegistryPlacement {
    pub(crate) fn new(placement: Arc<PlacementImpl>) -> Self {
        Self { placement }
    }
}

impl SeqPlacement for RegistryPlacement {
    fn owner(&self, ns: NamespaceId, repo: &RepoId) -> SeqOwner {
        let key = PlacementKey::repo(ns, repo_placement_hash(repo));
        let me = self.placement.registry().me().node_id;
        match self.placement.owners_of(&key).first() {
            None => SeqOwner::Here,
            Some(node) if node.node_id == me => SeqOwner::Here,
            Some(node) => SeqOwner::There {
                node_id: node.node_id,
                addr: node.addr.to_string(),
            },
        }
    }

    fn mark_unreachable(&self, node_id: u64) {
        self.placement.mark_unreachable(node_id);
    }
}

/// The owner lease through the metastore, as this process (`v1;<node>;<incarnation>`).
#[derive(Debug)]
pub(crate) struct MetaLeases {
    meta: Arc<dyn MetaStore>,
    owner: String,
}

impl MetaLeases {
    pub(crate) fn new(meta: Arc<dyn MetaStore>, owner: String) -> Self {
        Self { meta, owner }
    }
}

fn lease_error(err: MetaError) -> LeaseError {
    match err {
        MetaError::Rejected(ApplyError::LeaseHeld { owner, .. }) => {
            LeaseError::Held { holder: owner }
        }
        MetaError::Rejected(ApplyError::LeaseLost { .. }) => LeaseError::Lost,
        other => LeaseError::Unavailable(other.to_string()),
    }
}

#[async_trait]
impl SeqLeases for MetaLeases {
    async fn acquire(&self, key: &str, ttl: Duration) -> Result<u64, LeaseError> {
        self.meta
            .acquire_lease(key, &self.owner, ttl)
            .await
            .map(|grant| grant.epoch)
            .map_err(lease_error)
    }

    async fn renew(&self, key: &str, epoch: u64, ttl: Duration) -> Result<(), LeaseError> {
        self.meta
            .renew_lease(key, &self.owner, epoch, ttl)
            .await
            .map(|_| ())
            .map_err(lease_error)
    }

    async fn release(&self, key: &str, epoch: u64) {
        if let Err(err) = self.meta.release_lease(key, &self.owner, epoch).await {
            tracing::debug!(key, epoch, %err, "releasing a sequencer lease failed; it will expire");
        }
    }
}

/// Posts a forwarded commit to `http://<addr>/internal/v1/git/commit`.
#[derive(Debug)]
pub(crate) struct HttpSeqForwarder {
    client: reqwest::Client,
}

impl HttpSeqForwarder {
    pub(crate) fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SeqForwarder for HttpSeqForwarder {
    async fn forward(&self, addr: &str, body: Bytes) -> Result<Bytes, String> {
        let response = self
            .client
            .post(format!("http://{addr}{FORWARD_PATH}"))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("{addr} answered {status}"));
        }
        response.bytes().await.map_err(|err| err.to_string())
    }
}
