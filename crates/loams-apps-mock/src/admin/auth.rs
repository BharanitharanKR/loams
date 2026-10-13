//! `loams.auth.v1.AuthService` (API2 Task 6): sign-in providers, first-run
//! setup, sessions and OAuth consent, from the console seed.

use std::sync::Arc;

use connectrpc::{ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult};

use super::{ConsoleApi, SESSION_COOKIE, fix, from_json, invalid};
use crate::proto::loams::auth::v1::{
    AuthService, CompleteSetupRequest, CompleteSetupResponse, ConsentDecision,
    CreateSessionRequest, CreateSessionResponse, DecideConsentRequest, DecideConsentResponse,
    DeleteSessionRequest, DeleteSessionResponse, GetSessionRequest, GetSessionResponse,
    ListProvidersRequest, ListProvidersResponse, Session,
};
use crate::refuse;

/// The session cookie `CreateSession` and `CompleteSetup` set (§44 §5.2,
/// API2 Task 6). The value is fake; nothing reads it but [`has_session`].
fn set_cookie() -> String {
    format!("{SESSION_COOKIE}=mock-session; Path=/; HttpOnly; Secure; SameSite=Lax")
}

/// Whether the call carries a session: the cookie, or any bearer token (the
/// mock's bearer tokens are fake and checked only by the app protos).
fn has_session(ctx: &RequestContext) -> bool {
    let cookie = ctx
        .header(http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|cookies| {
            cookies
                .split(';')
                .any(|c| c.trim().starts_with(&format!("{SESSION_COOKIE}=")))
        });
    let bearer = ctx
        .header(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("Bearer ") && v.len() > "Bearer ".len());
    cookie || bearer
}

/// `loams.auth.v1.AuthService`.
pub(crate) struct Auth(pub(crate) Arc<ConsoleApi>);

impl Auth {
    fn session(&self) -> Result<Session, connectrpc::ConnectError> {
        let mut session = self.0.get("session")?.clone();
        fix::session(&mut session);
        from_json(session)
    }
}

impl AuthService for Auth {
    async fn list_providers(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, ListProvidersRequest>,
    ) -> ServiceResult<ListProvidersResponse> {
        let mut sign_in = self
            .0
            .get("instance")?
            .get("sign_in")
            .cloned()
            .unwrap_or_default();
        fix::sign_in(&mut sign_in);
        Response::ok(from_json::<ListProvidersResponse>(sign_in)?)
    }

    async fn complete_setup(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, CompleteSetupRequest>,
    ) -> ServiceResult<CompleteSetupResponse> {
        for (field, value) in [
            ("org_name", request.org_name),
            ("email", request.email),
            ("password", request.password),
        ] {
            if value.is_empty() {
                return Err(invalid(field, "required"));
            }
        }
        let session = self.session()?;
        Ok(Response::new(CompleteSetupResponse {
            session: session.into(),
            ..Default::default()
        })
        .with_header(http::header::SET_COOKIE, set_cookie()))
    }

    async fn get_session(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, GetSessionRequest>,
    ) -> ServiceResult<GetSessionResponse> {
        if !self.0.signed_in && !has_session(&ctx) {
            return Err(refuse(
                ErrorCode::Unauthenticated,
                "unauthenticated",
                "sign in to continue",
            ));
        }
        Response::ok(GetSessionResponse {
            session: self.session()?.into(),
            ..Default::default()
        })
    }

    async fn create_session(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, CreateSessionRequest>,
    ) -> ServiceResult<CreateSessionResponse> {
        if request.email.is_empty() {
            return Err(invalid("email", "required"));
        }
        if request.password.is_empty() {
            return Err(invalid("password", "required"));
        }
        Ok(Response::new(CreateSessionResponse {
            session: self.session()?.into(),
            ..Default::default()
        })
        .with_header(http::header::SET_COOKIE, set_cookie()))
    }

    async fn delete_session(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, DeleteSessionRequest>,
    ) -> ServiceResult<DeleteSessionResponse> {
        Ok(Response::new(DeleteSessionResponse::default()).with_header(
            http::header::SET_COOKIE,
            format!("{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax"),
        ))
    }

    async fn decide_consent(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, DecideConsentRequest>,
    ) -> ServiceResult<DecideConsentResponse> {
        let request = request.to_owned_message();
        match request.decision.as_known() {
            Some(ConsentDecision::CONSENT_DECISION_ALLOW) => {
                // The seed's canned redirect, as `POST /api/v1/oauth/consent`
                // answers.
                let consent = self.0.example("consent")?;
                Response::ok(from_json::<DecideConsentResponse>(consent)?)
            }
            Some(ConsentDecision::CONSENT_DECISION_DENY) => {
                if request.redirect_uri.is_empty() {
                    return Err(invalid("redirect_uri", "required"));
                }
                let mut to = url::Url::parse(&request.redirect_uri)
                    .map_err(|_| invalid("redirect_uri", "not a URL"))?;
                to.query_pairs_mut()
                    .append_pair("error", "access_denied")
                    .append_pair("state", &request.state);
                Response::ok(DecideConsentResponse {
                    redirect_to: to.into(),
                    ..Default::default()
                })
            }
            _ => Err(invalid("decision", "ALLOW or DENY")),
        }
    }
}
