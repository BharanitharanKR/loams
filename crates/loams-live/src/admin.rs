//! `loams.live.v1.LiveAdminService` (design §45 §6.3, D689; LV1 plan Task
//! 10): deploys, the deployment list, rollback by activation, index status
//! and function logs, over the [`AppEngine`](crate::AppEngine) of the app
//! the request addresses (its `loams-live-app` header); and the app
//! directory (LV1 plan Task 13): creating, listing, reading, updating and
//! deleting apps.
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
use ulid::Ulid;

use crate::apps::{AppLimits, AppRecord, AppState, AppUpdate, NewApp, TrustedIssuer};
use crate::authz::{Action, Authorizer, Resource, ScopeAuthorizer, audit};
use crate::catalog::IndexState;
use crate::deploy::{self, DeployOutcome, DeploymentRecord, PageRequest};
use crate::engines::Engines;
use crate::logs::{Read, TailFilter};
use crate::pb::{self, LiveAdminService};
use crate::schema::{IndexChange, Schema, SchemaChanges};
use crate::service::{app_of, connect_error};
use crate::{LiveError, backfill};

/// Who is calling (moved to [`crate::auth`], LV1 plan Task 14).
pub use crate::auth::{Caller, CallerKind};

/// The `LiveAdminService` implementation of a server's apps.
pub(crate) struct Admin {
    pub(crate) engines: Engines,
}

fn caller(ctx: &RequestContext) -> String {
    Caller::of(ctx).id
}

/// The app a deployment call addresses, once the caller may deploy to it
/// (LV1 plan Task 15: the app fence, then `live:deploy`).
async fn deployer(
    engines: &Engines,
    ctx: &RequestContext,
) -> Result<std::sync::Arc<crate::engines::AppRuntime>, LiveError> {
    let (app, caller) = app_of(engines, ctx).await?;
    ScopeAuthorizer.check(
        &caller,
        Action::Deploy,
        &Resource::App(app.name().to_string()),
    )?;
    Ok(app)
}

/// Checks a directory call on the app `name` (or on the directory when
/// `None`): `live:admin`, the app fence for an app, and for the directory
/// a credential bound to no environment (LV1 plan Task 15).
async fn directory_admin(
    engines: &Engines,
    caller: &Caller,
    name: Option<&str>,
) -> Result<Resource, LiveError> {
    let resource = match name {
        None => {
            if let Some(env) = &caller.environment {
                return Err(LiveError::PermissionDenied {
                    reason: crate::authz::APP_MISMATCH,
                    message: format!(
                        "this credential is bound to the environment {env}; creating and \
                         listing apps needs one bound to none"
                    ),
                });
            }
            Resource::Directory
        }
        Some(name) => {
            if let Some(record) = engines.apps().get_by_name(name).await? {
                crate::authz::fence(caller, &record)?;
            }
            Resource::App(name.to_string())
        }
    };
    ScopeAuthorizer.check(caller, Action::Admin, &resource)?;
    Ok(resource)
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

/// An AIP-158 page of the app list: the page size and the id to start
/// after.
fn app_page(size: i32, token: &str) -> Result<(Option<u32>, usize), LiveError> {
    let limit =
        usize::try_from(size).map_err(|_| LiveError::invalid("page_size must not be negative"))?;
    let after = match token {
        "" => None,
        t => Some(
            t.parse::<u32>()
                .map_err(|_| LiveError::invalid(format!("page_token: {t:?} is not a token")))?,
        ),
    };
    Ok((after, limit))
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
            let app = deployer(&self.engines, &ctx).await?;
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
            if !deploy_request.dry_run {
                audit(
                    &Caller::of(&ctx),
                    Action::Deploy,
                    &Resource::App(app.name().to_string()),
                    "Deploy",
                );
            }
            deploy::deploy(app.engine(), deploy_request, &by).await
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
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListDeploymentsRequest>,
    ) -> ServiceResult<pb::ListDeploymentsResponse> {
        let req = request.to_owned_message();
        let run = async {
            let app = deployer(&self.engines, &ctx).await?;
            let page = page_request(req.page_size, &req.page_token)?;
            deploy::list_deployments(app.engine(), page).await
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
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetDeploymentRequest>,
    ) -> ServiceResult<pb::GetDeploymentResponse> {
        let req = request.to_owned_message();
        let run = async {
            let app = deployer(&self.engines, &ctx).await?;
            let id = deployment_id("id", &req.id)?;
            deploy::get_deployment(app.engine(), id).await
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
            let app = deployer(&self.engines, &ctx).await?;
            let id = deployment_id("id", &req.id)?;
            let expected_active = expected(req.expected_active.as_deref())?;
            audit(
                &Caller::of(&ctx),
                Action::Deploy,
                &Resource::App(app.name().to_string()),
                "ActivateDeployment",
            );
            deploy::activate(app.engine(), id, expected_active, &by).await
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
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetIndexStatusRequest>,
    ) -> ServiceResult<pb::GetIndexStatusResponse> {
        let req = request.to_owned_message();
        let run = async {
            let app = deployer(&self.engines, &ctx).await?;
            backfill::index_status(app.runner(), &req.table, &req.index).await
        };
        let status = run.await.map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::GetIndexStatusResponse {
            state: index_state(status.state).into(),
            done: status.done,
            total_estimate: status.total_estimate,
            ..Default::default()
        }))
    }

    async fn tail_logs(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::TailLogsRequest>,
    ) -> ServiceResult<ServiceStream<pb::TailLogsResponse>> {
        let req = request.to_owned_message();
        let app = deployer(&self.engines, &ctx)
            .await
            .map_err(|e| connect_error(&e))?;
        let filter = TailFilter {
            since_ms: req.since_ms,
            function: req.function,
        };
        // The tail ends when the server, or the app's engine, stops.
        let tail = app
            .runner()
            .logs()
            .tail(filter, req.follow, app.stopping().clone())
            .map(|read| Ok(tail_response(read)));
        Ok(Response::new(tail.boxed()))
    }

    async fn create_app(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::CreateAppRequest>,
    ) -> ServiceResult<pb::CreateAppResponse> {
        let req = request.to_owned_message();
        let new = NewApp {
            name: req.name,
            namespace: req.namespace,
            settings: crate::apps::AppSettings {
                limits: req
                    .limits
                    .as_option()
                    .map(AppLimits::from_proto)
                    .unwrap_or_default(),
                issuers: req.issuers.iter().map(TrustedIssuer::from_proto).collect(),
                allowed_origins: req.allowed_origins,
                allow_anonymous: req.allow_anonymous,
            },
        };
        let caller = Caller::of(&ctx);
        let run = async {
            let resource = directory_admin(&self.engines, &caller, None).await?;
            audit(&caller, Action::Admin, &resource, "CreateApp");
            self.engines.apps().create(new).await
        };
        let record = run.await.map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::CreateAppResponse {
            app: buffa::MessageField::some(record.to_proto()),
            ..Default::default()
        }))
    }

    async fn list_apps(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListAppsRequest>,
    ) -> ServiceResult<pb::ListAppsResponse> {
        let req = request.to_owned_message();
        let run = async {
            directory_admin(&self.engines, &Caller::of(&ctx), None).await?;
            let (after, limit) = app_page(req.page_size, &req.page_token)?;
            self.engines.apps().list(after, limit).await
        };
        let page = run.await.map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::ListAppsResponse {
            apps: page.apps.iter().map(AppRecord::to_proto).collect(),
            next_page_token: page.next.map(|n| n.to_string()).unwrap_or_default(),
            ..Default::default()
        }))
    }

    async fn get_app(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetAppRequest>,
    ) -> ServiceResult<pb::GetAppResponse> {
        let req = request.to_owned_message();
        let run = async {
            directory_admin(&self.engines, &Caller::of(&ctx), Some(&req.name)).await?;
            self.engines
                .apps()
                .get_by_name(&req.name)
                .await?
                .ok_or_else(|| LiveError::NotFound(format!("no Live app is named {}", req.name)))
        };
        let record = run.await.map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::GetAppResponse {
            app: buffa::MessageField::some(record.to_proto()),
            ..Default::default()
        }))
    }

    async fn update_app(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::UpdateAppRequest>,
    ) -> ServiceResult<pb::UpdateAppResponse> {
        let req = request.to_owned_message();
        let run = async {
            let caller = Caller::of(&ctx);
            let resource = directory_admin(&self.engines, &caller, Some(&req.name)).await?;
            audit(&caller, Action::Admin, &resource, "UpdateApp");
            let state = match req.state.map(|s| s.as_known()) {
                None => None,
                Some(Some(pb::AppState::APP_STATE_ACTIVE)) => Some(AppState::Active),
                Some(Some(pb::AppState::APP_STATE_DISABLED)) => Some(AppState::Disabled),
                Some(_) => {
                    return Err(LiveError::invalid(
                        "state: UpdateApp sets APP_STATE_ACTIVE or APP_STATE_DISABLED",
                    ));
                }
            };
            let update = AppUpdate {
                state,
                limits: req.limits.as_option().map(AppLimits::from_proto),
                issuers: req
                    .issuers
                    .as_option()
                    .map(|l| l.issuers.iter().map(TrustedIssuer::from_proto).collect()),
                allowed_origins: req.allowed_origins.as_option().map(|l| l.origins.clone()),
                allow_anonymous: req.allow_anonymous,
            };
            let record = self.engines.apps().update(&req.name, update).await?;
            // This node stops the app's engine now when it is disabled or
            // its limits changed; the others within the directory poll.
            self.engines.reconcile().await?;
            Ok(record)
        };
        let record = run.await.map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::UpdateAppResponse {
            app: buffa::MessageField::some(record.to_proto()),
            ..Default::default()
        }))
    }

    async fn delete_app(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::DeleteAppRequest>,
    ) -> ServiceResult<pb::DeleteAppResponse> {
        let req = request.to_owned_message();
        let run = async {
            let caller = Caller::of(&ctx);
            let resource = directory_admin(&self.engines, &caller, Some(&req.name)).await?;
            if req.name == self.engines.default_app() {
                return Err(LiveError::FailedPrecondition(format!(
                    "{} is this server's own app (--live-app); it cannot be deleted",
                    req.name
                )));
            }
            audit(&caller, Action::Admin, &resource, "DeleteApp");
            let record = self.engines.apps().delete(&req.name).await?;
            self.engines.stop_app(&record.name).await;
            Ok(record)
        };
        let record = run.await.map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::DeleteAppResponse {
            app: buffa::MessageField::some(record.to_proto()),
            ..Default::default()
        }))
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
