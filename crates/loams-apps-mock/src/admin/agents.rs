//! `loams.admin.v1.AgentService` (API2 Task 7): agents, their tokens and
//! trust policies, and service accounts, from the console seed.

use std::sync::Arc;

use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{Value, json};

use super::journal::event;
use super::{ConsoleApi, fix, from_json, invalid, not_found, page};
use crate::proto::loams::admin::v1::{
    Agent, AgentService, AgentToken, CreateAgentRequest, CreateAgentResponse,
    CreateServiceAccountRequest, CreateServiceAccountResponse, CreateTrustPolicyRequest,
    CreateTrustPolicyResponse, GetAgentRequest, GetAgentResponse, ListAgentTokensRequest,
    ListAgentTokensResponse, ListAgentsRequest, ListAgentsResponse, ListServiceAccountsRequest,
    ListServiceAccountsResponse, ListTrustPoliciesRequest, ListTrustPoliciesResponse,
    ResumeAgentRequest, ResumeAgentResponse, ServiceAccount, SuspendAgentRequest,
    SuspendAgentResponse, TrustPolicy, UpdateAgentRequest, UpdateAgentResponse,
};

/// `loams.admin.v1.AgentService`.
pub(crate) struct Agents(pub(crate) Arc<ConsoleApi>);

impl Agents {
    /// The seed agent with this id, as JSON, or `not_found`.
    fn agent_json(&self, id: &str) -> Result<Value, ConnectError> {
        self.0
            .items("agents")?
            .into_iter()
            .find(|a| a["id"] == id)
            .ok_or_else(|| not_found("agent", id))
    }

    fn agent(&self, mut agent: Value) -> Result<Agent, ConnectError> {
        fix::agent(&mut agent);
        from_json(agent)
    }
}

impl AgentService for Agents {
    async fn list_agents(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListAgentsRequest>,
    ) -> ServiceResult<ListAgentsResponse> {
        self.0.project(request.project)?;
        let agents = self
            .0
            .items("agents")?
            .into_iter()
            .filter(|a| a["project"] == request.project)
            .map(|a| self.agent(a))
            .collect::<Result<Vec<_>, _>>()?;
        let (agents, next_page_token) = page(agents, request.page_size, request.page_token)?;
        Response::ok(ListAgentsResponse {
            agents,
            next_page_token,
            ..Default::default()
        })
    }

    async fn create_agent(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateAgentRequest>,
    ) -> ServiceResult<CreateAgentResponse> {
        self.0.project(request.project)?;
        if request.name.is_empty() {
            return Err(invalid("name", "required"));
        }
        let agent = self.agent(self.0.example("agent")?)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "agent.create",
            ("agent", &agent.id, request.name),
            request.project,
            "",
        ));
        Response::ok(CreateAgentResponse {
            agent: agent.into(),
            ..Default::default()
        })
    }

    async fn get_agent(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GetAgentRequest>,
    ) -> ServiceResult<GetAgentResponse> {
        let agent = self.agent(self.agent_json(request.agent)?)?;
        Response::ok(GetAgentResponse {
            agent: agent.into(),
            ..Default::default()
        })
    }

    async fn update_agent(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateAgentRequest>,
    ) -> ServiceResult<UpdateAgentResponse> {
        let request = request.to_owned_message();
        let changes = request.agent.as_option().cloned().unwrap_or_default();
        let mut agent = self.agent(self.agent_json(&changes.id)?)?;
        let paths = request
            .update_mask
            .as_option()
            .map(|mask| mask.paths.clone())
            .unwrap_or_default();
        for path in &paths {
            match path.as_str() {
                "description" => agent.description.clone_from(&changes.description),
                "policy" => agent.policy.clone_from(&changes.policy),
                "max_token_ttl_s" | "maxTokenTtlS" => {
                    agent.max_token_ttl_s = changes.max_token_ttl_s;
                }
                "limits" => agent.limits = changes.limits.clone(),
                other => {
                    return Err(invalid(
                        "update_mask",
                        &format!("`{other}` cannot be changed"),
                    ));
                }
            }
        }
        if !paths.is_empty() {
            self.0.journal.record(event(
                &self.0,
                &ctx,
                "agent.update",
                ("agent", &agent.id, &agent.name),
                &agent.project,
                "",
            ));
        }
        // Like `PATCH /api/v1/agents/{agent}`, answered but not kept.
        Response::ok(UpdateAgentResponse {
            agent: agent.into(),
            ..Default::default()
        })
    }

    async fn suspend_agent(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, SuspendAgentRequest>,
    ) -> ServiceResult<SuspendAgentResponse> {
        // What `POST /api/v1/agents/{agent}/suspend` answers: the agent,
        // suspended, with no live tokens.
        let mut agent = self.agent_json(request.agent)?;
        agent["status"] = json!("suspended");
        agent["active_tokens"] = json!(0);
        let agent = self.agent(agent)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "agent.suspend",
            ("agent", &agent.id, &agent.name),
            &agent.project,
            "",
        ));
        Response::ok(SuspendAgentResponse {
            agent: agent.into(),
            ..Default::default()
        })
    }

    async fn resume_agent(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ResumeAgentRequest>,
    ) -> ServiceResult<ResumeAgentResponse> {
        let mut agent = self.agent_json(request.agent)?;
        agent["status"] = json!("active");
        let agent = self.agent(agent)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "agent.resume",
            ("agent", &agent.id, &agent.name),
            &agent.project,
            "",
        ));
        Response::ok(ResumeAgentResponse {
            agent: agent.into(),
            ..Default::default()
        })
    }

    async fn list_agent_tokens(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListAgentTokensRequest>,
    ) -> ServiceResult<ListAgentTokensResponse> {
        self.agent_json(request.agent)?;
        let tokens = self
            .0
            .entry_items("tokens", request.agent)?
            .into_iter()
            .map(|mut t| {
                fix::token(&mut t);
                from_json::<AgentToken>(t)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (tokens, next_page_token) = page(tokens, request.page_size, request.page_token)?;
        Response::ok(ListAgentTokensResponse {
            tokens,
            next_page_token,
            ..Default::default()
        })
    }

    async fn list_trust_policies(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListTrustPoliciesRequest>,
    ) -> ServiceResult<ListTrustPoliciesResponse> {
        self.agent_json(request.agent)?;
        let policies = self
            .0
            .entry_items("trust_policies", request.agent)?
            .into_iter()
            .map(from_json::<TrustPolicy>)
            .collect::<Result<Vec<_>, _>>()?;
        let (trust_policies, next_page_token) =
            page(policies, request.page_size, request.page_token)?;
        Response::ok(ListTrustPoliciesResponse {
            trust_policies,
            next_page_token,
            ..Default::default()
        })
    }

    async fn create_trust_policy(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateTrustPolicyRequest>,
    ) -> ServiceResult<CreateTrustPolicyResponse> {
        let agent = self.agent(self.agent_json(request.agent)?)?;
        for (field, value) in [
            ("name", request.name),
            ("issuer", request.issuer),
            ("subject_pattern", request.subject_pattern),
        ] {
            if value.is_empty() {
                return Err(invalid(field, "required"));
            }
        }
        let policy: TrustPolicy = from_json(self.0.example("trust_policy")?)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "trust_policy.create",
            ("trust_policy", &policy.id, request.name),
            &agent.project,
            "",
        ));
        Response::ok(CreateTrustPolicyResponse {
            trust_policy: policy.into(),
            ..Default::default()
        })
    }

    async fn list_service_accounts(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListServiceAccountsRequest>,
    ) -> ServiceResult<ListServiceAccountsResponse> {
        self.0.project(request.project)?;
        let accounts = self
            .0
            .entry_items("service_accounts", request.project)?
            .into_iter()
            .map(from_json::<ServiceAccount>)
            .collect::<Result<Vec<_>, _>>()?;
        let (service_accounts, next_page_token) =
            page(accounts, request.page_size, request.page_token)?;
        Response::ok(ListServiceAccountsResponse {
            service_accounts,
            next_page_token,
            ..Default::default()
        })
    }

    async fn create_service_account(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateServiceAccountRequest>,
    ) -> ServiceResult<CreateServiceAccountResponse> {
        self.0.project(request.project)?;
        if request.name.is_empty() {
            return Err(invalid("name", "required"));
        }
        let account: ServiceAccount = from_json(self.0.example("service_account")?)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "service_account.create",
            ("service_account", &account.id, request.name),
            request.project,
            "",
        ));
        Response::ok(CreateServiceAccountResponse {
            service_account: account.into(),
            ..Default::default()
        })
    }
}
