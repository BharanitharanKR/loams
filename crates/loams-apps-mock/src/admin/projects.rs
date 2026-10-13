//! `loams.admin.v1.ProjectService` (API2 Task 7): projects, access and
//! environments, from the console seed.

use std::sync::Arc;

use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::Value;

use super::journal::event;
use super::{ConsoleApi, fix, from_json, invalid, not_found, page};
use crate::proto::loams::admin::v1::{
    CreateEnvironmentRequest, CreateEnvironmentResponse, CreateProjectRequest,
    CreateProjectResponse, Environment, GetEnvironmentActivityRequest,
    GetEnvironmentActivityResponse, GetEnvironmentRequest, GetEnvironmentResponse,
    GetProjectRequest, GetProjectResponse, Grant, GrantAccessRequest, GrantAccessResponse,
    ListAccessRequest, ListAccessResponse, ListEnvironmentsRequest, ListEnvironmentsResponse,
    ListProjectsRequest, ListProjectsResponse, Project, ProjectRole, ProjectService, SubjectKind,
};

/// `loams.admin.v1.ProjectService`.
pub(crate) struct Projects(pub(crate) Arc<ConsoleApi>);

impl ConsoleApi {
    /// The seed project with this slug, or `not_found`.
    pub(crate) fn project(&self, slug: &str) -> Result<Value, ConnectError> {
        self.items("projects")?
            .into_iter()
            .find(|p| p["slug"] == slug)
            .ok_or_else(|| not_found("project", slug))
    }

    /// The seed environment `project`/`environment`, or `not_found`.
    pub(crate) fn environment(
        &self,
        project: &str,
        environment: &str,
    ) -> Result<Value, ConnectError> {
        self.project(project)?;
        self.items("environments")?
            .into_iter()
            .find(|e| e["project"] == project && e["slug"] == environment)
            .ok_or_else(|| not_found("environment", &format!("{project}/{environment}")))
    }
}

impl ProjectService for Projects {
    async fn list_projects(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListProjectsRequest>,
    ) -> ServiceResult<ListProjectsResponse> {
        let projects = self
            .0
            .items("projects")?
            .into_iter()
            .map(from_json::<Project>)
            .collect::<Result<Vec<_>, _>>()?;
        let (projects, next_page_token) = page(projects, request.page_size, request.page_token)?;
        Response::ok(ListProjectsResponse {
            projects,
            next_page_token,
            ..Default::default()
        })
    }

    async fn create_project(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateProjectRequest>,
    ) -> ServiceResult<CreateProjectResponse> {
        if request.name.is_empty() {
            return Err(invalid("name", "required"));
        }
        let project: Project = from_json(self.0.example("project")?)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "project.create",
            ("project", &project.id, request.name),
            &project.slug,
            "",
        ));
        Response::ok(CreateProjectResponse {
            project: project.into(),
            ..Default::default()
        })
    }

    async fn get_project(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GetProjectRequest>,
    ) -> ServiceResult<GetProjectResponse> {
        let project = self.0.project(request.project)?;
        Response::ok(GetProjectResponse {
            project: from_json::<Project>(project)?.into(),
            ..Default::default()
        })
    }

    async fn list_access(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListAccessRequest>,
    ) -> ServiceResult<ListAccessResponse> {
        self.0.project(request.project)?;
        let grants = self
            .0
            .entry_items("grants", request.project)?
            .into_iter()
            .map(|mut g| {
                fix::grant(&mut g);
                from_json::<Grant>(g)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (grants, next_page_token) = page(grants, request.page_size, request.page_token)?;
        Response::ok(ListAccessResponse {
            grants,
            next_page_token,
            ..Default::default()
        })
    }

    async fn grant_access(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GrantAccessRequest>,
    ) -> ServiceResult<GrantAccessResponse> {
        let request = request.to_owned_message();
        self.0.project(&request.project)?;
        let subject = request.subject.as_option().cloned().unwrap_or_default();
        if !matches!(
            subject.kind.as_known(),
            Some(SubjectKind::SUBJECT_KIND_USER | SubjectKind::SUBJECT_KIND_TEAM)
        ) || subject.id.is_empty()
        {
            return Err(invalid("subject", "a user or a team, by id"));
        }
        if request
            .role
            .as_known()
            .is_none_or(|r| r == ProjectRole::PROJECT_ROLE_UNSPECIFIED)
        {
            return Err(invalid("role", "required"));
        }
        let mut grant = self.0.example("grant")?;
        fix::grant(&mut grant);
        let grant: Grant = from_json(grant)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "project.access.grant",
            ("project", &request.project, &request.project),
            &request.project,
            "",
        ));
        Response::ok(GrantAccessResponse {
            grant: grant.into(),
            ..Default::default()
        })
    }

    async fn list_environments(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListEnvironmentsRequest>,
    ) -> ServiceResult<ListEnvironmentsResponse> {
        self.0.project(request.project)?;
        let environments = self
            .0
            .items("environments")?
            .into_iter()
            .filter(|e| e["project"] == request.project)
            .map(from_json::<Environment>)
            .collect::<Result<Vec<_>, _>>()?;
        let (environments, next_page_token) =
            page(environments, request.page_size, request.page_token)?;
        Response::ok(ListEnvironmentsResponse {
            environments,
            next_page_token,
            ..Default::default()
        })
    }

    async fn create_environment(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateEnvironmentRequest>,
    ) -> ServiceResult<CreateEnvironmentResponse> {
        self.0.project(request.project)?;
        if request.name.is_empty() {
            return Err(invalid("name", "required"));
        }
        let environment: Environment = from_json(self.0.example("environment")?)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "environment.create",
            ("environment", &environment.id, request.name),
            request.project,
            &environment.slug,
        ));
        Response::ok(CreateEnvironmentResponse {
            environment: environment.into(),
            ..Default::default()
        })
    }

    async fn get_environment(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GetEnvironmentRequest>,
    ) -> ServiceResult<GetEnvironmentResponse> {
        let environment = self.0.environment(request.project, request.environment)?;
        Response::ok(GetEnvironmentResponse {
            environment: from_json::<Environment>(environment)?.into(),
            ..Default::default()
        })
    }

    async fn get_environment_activity(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GetEnvironmentActivityRequest>,
    ) -> ServiceResult<GetEnvironmentActivityResponse> {
        let (project, environment) = (request.project, request.environment);
        self.0.environment(project, environment)?;
        // The seed keeps an environment's daily series under the REST route's
        // name for it.
        let mut activity = self
            .0
            .get("usage")?
            .get(format!("{project}/{environment}"))
            .cloned()
            .ok_or_else(|| {
                not_found("environment activity", &format!("{project}/{environment}"))
            })?;
        fix::activity(&mut activity);
        Response::ok(from_json::<GetEnvironmentActivityResponse>(activity)?)
    }
}
