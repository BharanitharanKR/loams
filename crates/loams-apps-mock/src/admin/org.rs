//! `loams.admin.v1.OrgService` (API2 Task 7): the org, members, invitations
//! and teams, from the console seed.

use std::sync::Arc;

use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};

use super::journal::event;
use super::{ConsoleApi, fix, from_json, invalid, not_found, page};
use crate::proto::loams::admin::v1::{
    AddTeamMemberRequest, AddTeamMemberResponse, CreateInvitationRequest, CreateInvitationResponse,
    CreateTeamRequest, CreateTeamResponse, GetOrgRequest, GetOrgResponse, GetTeamRequest,
    GetTeamResponse, Invitation, ListInvitationsRequest, ListInvitationsResponse,
    ListMembersRequest, ListMembersResponse, ListTeamsRequest, ListTeamsResponse, Member,
    Org as OrgMessage, OrgService, RemoveMemberRequest, RemoveMemberResponse, Team,
    UpdateOrgRequest, UpdateOrgResponse,
};

/// `loams.admin.v1.OrgService`.
pub(crate) struct Org(pub(crate) Arc<ConsoleApi>);

impl Org {
    fn org(&self) -> Result<OrgMessage, connectrpc::ConnectError> {
        from_json(self.0.get("org")?.clone())
    }

    fn is_member(&self, user: &str) -> Result<bool, connectrpc::ConnectError> {
        Ok(self
            .0
            .items("members")?
            .iter()
            .any(|m| m["user"]["id"] == user))
    }
}

impl OrgService for Org {
    async fn get_org(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, GetOrgRequest>,
    ) -> ServiceResult<GetOrgResponse> {
        Response::ok(GetOrgResponse {
            org: self.org()?.into(),
            ..Default::default()
        })
    }

    async fn update_org(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateOrgRequest>,
    ) -> ServiceResult<UpdateOrgResponse> {
        let request = request.to_owned_message();
        let mut org = self.org()?;
        let changes = request.org.as_option().cloned().unwrap_or_default();
        let paths = request
            .update_mask
            .as_option()
            .map(|mask| mask.paths.clone())
            .unwrap_or_default();
        for path in &paths {
            match path.as_str() {
                "name" => org.name.clone_from(&changes.name),
                "require_two_factor" | "requireTwoFactor" => {
                    org.require_two_factor = changes.require_two_factor;
                }
                "allowed_domains" | "allowedDomains" => {
                    org.allowed_domains.clone_from(&changes.allowed_domains);
                }
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
                "org.update",
                ("org", &org.id, &org.name),
                "",
                "",
            ));
        }
        // Like `PATCH /api/v1/org`, the change is answered but not kept.
        Response::ok(UpdateOrgResponse {
            org: org.into(),
            ..Default::default()
        })
    }

    async fn list_members(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListMembersRequest>,
    ) -> ServiceResult<ListMembersResponse> {
        let members = self
            .0
            .items("members")?
            .into_iter()
            .map(|mut m| {
                fix::member(&mut m);
                from_json::<Member>(m)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (members, next_page_token) = page(members, request.page_size, request.page_token)?;
        Response::ok(ListMembersResponse {
            members,
            next_page_token,
            ..Default::default()
        })
    }

    async fn remove_member(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RemoveMemberRequest>,
    ) -> ServiceResult<RemoveMemberResponse> {
        let user = request.user.to_owned();
        if user.is_empty() {
            return Err(invalid("user", "required"));
        }
        // Removing someone who is not a member is not an error (IDEMPOTENT).
        if self.is_member(&user)? {
            self.0.journal.record(event(
                &self.0,
                &ctx,
                "member.remove",
                ("user", &user, &user),
                "",
                "",
            ));
        }
        Response::ok(RemoveMemberResponse::default())
    }

    async fn list_invitations(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListInvitationsRequest>,
    ) -> ServiceResult<ListInvitationsResponse> {
        let invitations = self
            .0
            .items("invitations")?
            .into_iter()
            .map(|mut i| {
                fix::invitation(&mut i);
                from_json::<Invitation>(i)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (invitations, next_page_token) =
            page(invitations, request.page_size, request.page_token)?;
        Response::ok(ListInvitationsResponse {
            invitations,
            next_page_token,
            ..Default::default()
        })
    }

    async fn create_invitation(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateInvitationRequest>,
    ) -> ServiceResult<CreateInvitationResponse> {
        if request.email.is_empty() {
            return Err(invalid("email", "required"));
        }
        let mut invitation = self.0.example("invitation")?;
        fix::invitation(&mut invitation);
        let invitation: Invitation = from_json(invitation)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "invitation.create",
            ("invitation", &invitation.id, request.email),
            "",
            "",
        ));
        Response::ok(CreateInvitationResponse {
            invitation: invitation.into(),
            ..Default::default()
        })
    }

    async fn list_teams(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListTeamsRequest>,
    ) -> ServiceResult<ListTeamsResponse> {
        let teams = self
            .0
            .items("teams")?
            .into_iter()
            .map(from_json::<Team>)
            .collect::<Result<Vec<_>, _>>()?;
        let (teams, next_page_token) = page(teams, request.page_size, request.page_token)?;
        Response::ok(ListTeamsResponse {
            teams,
            next_page_token,
            ..Default::default()
        })
    }

    async fn create_team(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateTeamRequest>,
    ) -> ServiceResult<CreateTeamResponse> {
        if request.name.is_empty() {
            return Err(invalid("name", "required"));
        }
        let team: Team = from_json(self.0.example("team")?)?;
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "team.create",
            ("team", &team.id, request.name),
            "",
            "",
        ));
        Response::ok(CreateTeamResponse {
            team: team.into(),
            ..Default::default()
        })
    }

    async fn get_team(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GetTeamRequest>,
    ) -> ServiceResult<GetTeamResponse> {
        let team = request.team.to_owned();
        let mut detail = self
            .0
            .get("team_details")?
            .get(&team)
            .cloned()
            .ok_or_else(|| not_found("team", &team))?;
        fix::team_detail(&mut detail);
        Response::ok(from_json::<GetTeamResponse>(detail)?)
    }

    async fn add_team_member(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, AddTeamMemberRequest>,
    ) -> ServiceResult<AddTeamMemberResponse> {
        let (team, user) = (request.team.to_owned(), request.user.to_owned());
        if self.0.get("team_details")?.get(&team).is_none() {
            return Err(not_found("team", &team));
        }
        if user.is_empty() {
            return Err(invalid("user", "required"));
        }
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "team.member.add",
            ("team", &team, &team),
            "",
            "",
        ));
        Response::ok(AddTeamMemberResponse::default())
    }
}
