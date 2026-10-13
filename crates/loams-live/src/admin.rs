//! `loams.live.v1.LiveAdminService` (design §45 §6.3, D689; LV1 plan Task
//! 10): deploys, the deployment list, rollback by activation, index status
//! and function logs, over the app's [`AppEngine`].
//!
//! The server serves it beside `LiveService` on the Live listener, behind
//! the guard the engine gives [`LiveServer::start_guarded`] (its
//! authentication layer, `LOAMS_AUTH_MODE`). That layer names the caller
//! with a [`Caller`] in the request's extensions; a deployment records its
//! id as `created_by`. Without one (a server started without a guard) the
//! caller is [`deploy::UNAUTHENTICATED`]. Per-app authorization of these
//! calls is LV1 Task 15's.
//!
//! [`LiveServer::start_guarded`]: crate::LiveServer::start_guarded

use std::str::FromStr;

use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::catalog::IndexState;
use crate::deploy::{self, AppEngine, DeployOutcome, DeploymentRecord, PageRequest};
use crate::logs::{Read, TailFilter};
use crate::pb::{self, LiveAdminService};
use crate::schema::{IndexChange, Schema, SchemaChanges};
use crate::service::connect_error;
use crate::{LiveError, backfill};

/// Who is calling: the principal the server's authentication layer
/// admitted, put in the request's extensions for the admin service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// The principal's id (an OIDC subject, an API key's id, or
    /// `dev-loopback`); never a credential.
    pub id: String,
}

/// The `LiveAdminService` implementation of one app.
pub(crate) struct Admin {
    pub(crate) engine: AppEngine,
    /// Ends the log tails when the server stops.
    pub(crate) stop: CancellationToken,
}

fn caller(ctx: &RequestContext) -> String {
    ctx.extensions()
        .get::<Caller>()
        .map_or_else(|| deploy::UNAUTHENTICATED.to_string(), |c| c.id.clone())
}

/// A deployment id of a request: a ULID.
fn deployment_id(field: &str, text: &str) -> Result<Ulid, LiveError> {
    Ulid::from_str(text)
        .map_err(|_| LiveError::invalid(format!("{field}: {text:?} is not a deployment id")))
}

fn expected(text: Option<&str>) -> Result<Option<Ulid>, LiveError> {
    text.map(|t| deployment_id("expected_active", t))
        .transpose()
}

/// A deployment as the admin API shows it.
pub fn deployment(r: &DeploymentRecord) -> pb::Deployment {
    pb::Deployment {
        id: r.id.to_string(),
        bundle_sha256: data_encoding::HEXLOWER.encode(&r.bundle_sha256),
        created_at_ms: r.created_at_ms,
        created_by: r.created_by.clone(),
        message: r.message.clone(),
        parent: r.parent.map(|p| p.to_string()).unwrap_or_default(),
        state: r.state.to_proto().into(),
        failure_reason: match &r.state {
            deploy::DeploymentState::Failed { reason } => reason.clone(),
            _ => String::new(),
        },
        ..Default::default()
    }
}

/// Schema changes as the admin API shows them.
pub fn changes(c: &SchemaChanges) -> pb::SchemaChanges {
    let refs = |list: &[IndexChange]| {
        list.iter()
            .map(|i| pb::IndexRef {
                table: i.table.clone(),
                index: i.index.clone(),
                ..Default::default()
            })
            .collect()
    };
    pb::SchemaChanges {
        tables_created: c.create.clone(),
        validators_changed: c.validators.clone(),
        tables_validated: c.validate.clone(),
        indexes_added: refs(&c.indexes_added),
        indexes_dropped: refs(&c.indexes_dropped),
        ..Default::default()
    }
}

fn outcome(o: &DeployOutcome) -> (pb::Deployment, pb::SchemaChanges) {
    (deployment(&o.record), changes(&o.changes))
}

fn index_state(state: IndexState) -> pb::IndexState {
    match state {
        IndexState::Backfilling { .. } => pb::IndexState::INDEX_STATE_BACKFILLING,
        IndexState::Ready => pb::IndexState::INDEX_STATE_READY,
        IndexState::Dropping => pb::IndexState::INDEX_STATE_DROPPING,
    }
}

/// An AIP-158 page of the deployment list.
fn page_request(size: i32, token: &str) -> Result<PageRequest, LiveError> {
    let limit = match usize::try_from(size) {
        Err(_) => return Err(LiveError::invalid("page_size must not be negative")),
        Ok(0) => deploy::DEFAULT_PAGE,
        Ok(n) => n.min(deploy::MAX_PAGE),
    };
    let after = match token {
        "" => None,
        t => Some(deployment_id("page_token", t)?),
    };
    Ok(PageRequest { after, limit })
}

fn tail_response(read: Read) -> pb::TailLogsResponse {
    pb::TailLogsResponse {
        entries: read.entries.iter().map(|e| e.to_proto()).collect(),
        skipped: read.skipped,
        ..Default::default()
    }
}

// The generated trait returns `impl Encodable`; plain `async fn`s with the
// owned messages are how connect-rust documents implementing it, and
// `Admin` is private, so the refinement is not API.
#[allow(refining_impl_trait)]
impl LiveAdminService for Admin {
    async fn deploy(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::DeployRequest>,
    ) -> ServiceResult<pb::DeployResponse> {
        let req = request.to_owned_message();
        let by = caller(&ctx);
        let run = async {
            let schema = req
                .schema
                .as_option()
                .map(Schema::from_proto)
                .transpose()?
                .unwrap_or_default();
            let deploy_request = deploy::DeployRequest {
                bundle: req.bundle,
                schema,
                expected_active: expected(req.expected_active.as_deref())?,
                message: req.message,
                dry_run: req.dry_run,
            };
            deploy::deploy(&self.engine, deploy_request, &by).await
        };
        let done = run.await.map_err(|e| connect_error(&e))?;
        let (deployment, changes) = outcome(&done);
        Ok(Response::new(pb::DeployResponse {
            deployment: buffa::MessageField::some(deployment),
            changes: buffa::MessageField::some(changes),
            ..Default::default()
        }))
    }

    async fn list_deployments(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListDeploymentsRequest>,
    ) -> ServiceResult<pb::ListDeploymentsResponse> {
        let req = request.to_owned_message();
        let run = async {
            let page = page_request(req.page_size, &req.page_token)?;
            deploy::list_deployments(&self.engine, page).await
        };
        let page = run.await.map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::ListDeploymentsResponse {
            deployments: page.items.iter().map(deployment).collect(),
            next_page_token: page.next.map(|n| n.to_string()).unwrap_or_default(),
            ..Default::default()
        }))
    }

    async fn get_deployment(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetDeploymentRequest>,
    ) -> ServiceResult<pb::GetDeploymentResponse> {
        let req = request.to_owned_message();
        let run = async {
            let id = deployment_id("id", &req.id)?;
            deploy::get_deployment(&self.engine, id).await
        };
        let record = run.await.map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::GetDeploymentResponse {
            deployment: buffa::MessageField::some(deployment(&record)),
            schema: buffa::MessageField::some(record.schema.to_proto()),
            functions: record.functions.iter().map(|f| f.to_proto()).collect(),
            ..Default::default()
        }))
    }

    async fn activate_deployment(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::ActivateDeploymentRequest>,
    ) -> ServiceResult<pb::ActivateDeploymentResponse> {
        let req = request.to_owned_message();
        let by = caller(&ctx);
        let run = async {
            let id = deployment_id("id", &req.id)?;
            let expected_active = expected(req.expected_active.as_deref())?;
            deploy::activate(&self.engine, id, expected_active, &by).await
        };
        let done = run.await.map_err(|e| connect_error(&e))?;
        let (deployment, changes) = outcome(&done);
        Ok(Response::new(pb::ActivateDeploymentResponse {
            deployment: buffa::MessageField::some(deployment),
            changes: buffa::MessageField::some(changes),
            ..Default::default()
        }))
    }

    async fn get_index_status(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetIndexStatusRequest>,
    ) -> ServiceResult<pb::GetIndexStatusResponse> {
        let req = request.to_owned_message();
        let status = backfill::index_status(self.engine.runner(), &req.table, &req.index)
            .await
            .map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::GetIndexStatusResponse {
            state: index_state(status.state).into(),
            done: status.done,
            total_estimate: status.total_estimate,
            ..Default::default()
        }))
    }

    async fn tail_logs(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::TailLogsRequest>,
    ) -> ServiceResult<ServiceStream<pb::TailLogsResponse>> {
        let req = request.to_owned_message();
        let filter = TailFilter {
            since_ms: req.since_ms,
            function: req.function,
        };
        let tail = self
            .engine
            .runner()
            .logs()
            .tail(filter, req.follow, self.stop.clone())
            .map(|read| Ok(tail_response(read)));
        Ok(Response::new(tail.boxed()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pages_follow_aip_158() {
        assert_eq!(
            page_request(0, "").expect("a page").limit,
            deploy::DEFAULT_PAGE
        );
        assert_eq!(
            page_request(5_000, "").expect("a page").limit,
            deploy::MAX_PAGE
        );
        assert!(page_request(-1, "").is_err());
        assert!(page_request(10, "not a ulid").is_err());
        let id = Ulid::from_parts(1, 2);
        assert_eq!(
            page_request(10, &id.to_string()).expect("a page").after,
            Some(id)
        );
    }
}
