//! `loams.auth.v1.AuthService` on the main port (API2 Task 6, server half):
//! sign-in providers, first-run setup, console sessions and OAuth consent.
//!
//! Sessions live in the principal store ([`crate::auth::directory`]): a
//! session is a random `ls_…` token, kept only as its SHA-256, answered once
//! in the `loams_session` cookie (`Path=/; HttpOnly; Secure; SameSite=Lax`,
//! ruling 6/7.9) and usable as a bearer too. The auth layer turns either into
//! a [`Principal`] of kind `Session`; a cookie-authenticated call that is not
//! a `GET` (nor `GetSession`) must carry the session's `X-CSRF-Token`.
//!
//! `ListProviders`, `CompleteSetup`, `CreateSession` and `DeleteSession` are
//! answered without a credential ([`crate::auth::PUBLIC_RPCS`]).
//! `CompleteSetup` runs once: from loopback when authentication is off, and
//! otherwise only with `LOAMS_SETUP_TOKEN`.
//!
//! The OAuth protocol endpoints stay HTTP and arrive with MT1's issuer
//! (D602); until then `DecideConsent` can deny (it only redirects back) but
//! not allow, which needs an issuer to mint the code.

#![allow(refining_impl_trait)]

use std::sync::Arc;

use connectrpc::{
    ConnectError, ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult,
};
use loams_proto::loams::admin::v1 as admin;
use loams_proto::loams::auth::v1::{
    AuthService, AuthServiceExt, CompleteSetupRequest, CompleteSetupResponse, ConsentDecision,
    CreateSessionRequest, CreateSessionResponse, DecideConsentRequest, DecideConsentResponse,
    DeleteSessionRequest, DeleteSessionResponse, GetSessionRequest, GetSessionResponse,
    ListProvidersRequest, ListProvidersResponse, Session,
};
use subtle::ConstantTimeEq;

use super::connect::refuse;
use super::connect_console::{event, invalid, slugify, unavailable};
use crate::auth::directory::{
    Directory, Doc, MemberRec, Role, SESSION_COOKIE, new_id, new_session_token, now_ms,
    session_token, timestamp, token_digest,
};
use crate::auth::{Principal, PrincipalKind, Principals, SetupGate};

/// The shortest password `CompleteSetup` takes.
const MIN_PASSWORD: usize = 8;

/// `loams.auth.v1.AuthService`.
struct Auth {
    principals: Principals,
}

/// The `Set-Cookie` that starts a session.
fn set_cookie(token: &str, directory: &Directory) -> String {
    format!(
        "{SESSION_COOKIE}={token}; Path=/; Max-Age={}; HttpOnly; Secure; SameSite=Lax",
        directory.session_ttl().as_secs()
    )
}

/// The `Set-Cookie` that ends one.
fn clear_cookie() -> String {
    format!("{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax")
}

fn no_session() -> ConnectError {
    refuse(
        ErrorCode::Unauthenticated,
        "unauthenticated",
        "no console session: sign in with AuthService/CreateSession",
        &[],
    )
}

/// The `Session` of `member` in `doc`.
fn session_of(doc: &Doc, member: &MemberRec, csrf: String, expires_ms: i64) -> Session {
    Session {
        user: member.user.clone().into(),
        org: doc.org.clone().unwrap_or_default().into(),
        role: member.role.to_proto().into(),
        csrf_token: csrf,
        expires_at: timestamp(expires_ms).into(),
        ..Default::default()
    }
}

/// The audit actor for a person signing in or out.
fn person(member: &MemberRec) -> crate::auth::authorizer::Actor {
    crate::auth::authorizer::Actor {
        kind: admin::SubjectKind::SUBJECT_KIND_USER,
        id: member.user.id.clone(),
        name: member.user.name.clone(),
        role: member.role,
        user: Some(member.user.id.clone()),
    }
}

/// An argon2id hash, off the async runtime.
async fn hash_password(password: String) -> Result<String, ConnectError> {
    tokio::task::spawn_blocking(move || crate::auth::api_key::hash_key(&password))
        .await
        .map_err(|err| unavailable(&format!("hashing the password: {err}")))
}

impl AuthService for Auth {
    async fn list_providers(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, ListProvidersRequest>,
    ) -> ServiceResult<ListProvidersResponse> {
        // Passwords are the engine's own; TOTP, passkeys and OIDC sign-in
        // arrive with MT1's issuer (the start endpoints are absent without
        // it, so no provider is listed).
        Response::ok(ListProvidersResponse {
            password: true,
            ..Default::default()
        })
    }

    async fn complete_setup(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, CompleteSetupRequest>,
    ) -> ServiceResult<CompleteSetupResponse> {
        let request = request.to_owned_message();
        match &self.principals.setup {
            SetupGate::Open => {}
            SetupGate::Token(token) => {
                if !bool::from(request.setup_token.as_bytes().ct_eq(token.as_bytes())) {
                    return Err(refuse(
                        ErrorCode::PermissionDenied,
                        "permission_denied",
                        "setup_token is not this engine's LOAMS_SETUP_TOKEN",
                        &[],
                    ));
                }
            }
            SetupGate::Closed => {
                return Err(refuse(
                    ErrorCode::FailedPrecondition,
                    "failed_precondition",
                    "first-run setup on an engine with authentication on needs LOAMS_SETUP_TOKEN; \
                     set it and restart",
                    &[],
                ));
            }
        }
        for (field, value) in [
            ("org_name", &request.org_name),
            ("email", &request.email),
            ("password", &request.password),
        ] {
            if value.trim().is_empty() {
                return Err(invalid(field, "required"));
            }
        }
        if !request.email.contains('@') {
            return Err(invalid("email", "not an email address"));
        }
        if request.password.chars().count() < MIN_PASSWORD {
            return Err(invalid("password", "at least 8 characters"));
        }
        let hash = hash_password(request.password.clone()).await?;
        let token = new_session_token();
        let directory = &self.principals.directory;
        let ttl = directory.session_ttl();
        let session = directory
            .update(|doc| {
                if doc.org.is_some() {
                    return Err(refuse(
                        ErrorCode::FailedPrecondition,
                        "failed_precondition",
                        "setup has already run",
                        &[],
                    ));
                }
                let now = now_ms();
                let org = admin::Org {
                    id: new_id("org"),
                    name: request.org_name.trim().to_owned(),
                    slug: slugify(&request.org_name),
                    created_at: timestamp(now).into(),
                    ..Default::default()
                };
                let name = match request.name.trim() {
                    "" => request.email.clone(),
                    name => name.to_owned(),
                };
                let owner = MemberRec {
                    user: admin::User {
                        id: new_id("usr"),
                        name,
                        email: request.email.trim().to_owned(),
                        created_at: timestamp(now).into(),
                        last_seen_at: timestamp(now).into(),
                        ..Default::default()
                    },
                    role: Role::Owner,
                    joined_at_ms: now,
                    password: hash.clone(),
                };
                doc.org = Some(org.clone());
                doc.members.push(owner.clone());
                doc.record(event(
                    &person(&owner),
                    "org.setup",
                    ("org", &org.id, &org.name),
                    "",
                    "",
                ));
                let (csrf, expires) = doc.start_session(&owner.user.id, &token, ttl);
                Ok(session_of(doc, &owner, csrf, expires))
            })
            .await?;
        Ok(Response::new(CompleteSetupResponse {
            session: session.into(),
            ..Default::default()
        })
        .with_header(http::header::SET_COOKIE, set_cookie(&token, directory)))
    }

    async fn get_session(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, GetSessionRequest>,
    ) -> ServiceResult<GetSessionResponse> {
        let principal = ctx.extensions().get::<Principal>();
        if principal.is_none_or(|p| p.kind != PrincipalKind::Session) {
            return Err(no_session());
        }
        let token = session_token(ctx.headers()).ok_or_else(no_session)?;
        let doc = self
            .principals
            .directory
            .read()
            .await
            .map_err(|err| unavailable(&err.0))?;
        let digest = token_digest(&token);
        let record = doc
            .sessions
            .iter()
            .find(|s| s.digest == digest)
            .ok_or_else(no_session)?;
        let member = doc.member(&record.user).ok_or_else(no_session)?;
        Response::ok(GetSessionResponse {
            session: session_of(&doc, member, record.csrf.clone(), record.expires_ms).into(),
            ..Default::default()
        })
    }

    async fn create_session(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, CreateSessionRequest>,
    ) -> ServiceResult<CreateSessionResponse> {
        let request = request.to_owned_message();
        if request.email.is_empty() {
            return Err(invalid("email", "required"));
        }
        if request.password.is_empty() {
            return Err(invalid("password", "required"));
        }
        let directory = &self.principals.directory;
        let member = directory
            .check_password(request.email.trim(), &request.password)
            .await
            .map_err(|err| unavailable(&err.0))?
            .ok_or_else(|| {
                refuse(
                    ErrorCode::Unauthenticated,
                    "unauthenticated",
                    "the email or the password is not right",
                    &[],
                )
            })?;
        let token = new_session_token();
        let ttl = directory.session_ttl();
        let session = directory
            .update(|doc| {
                // Still a member (removed between the check and now: no).
                let Some(at) = doc.members.iter().position(|m| m.user.id == member.user.id) else {
                    return Err(no_session());
                };
                doc.members[at].user.last_seen_at = timestamp(now_ms()).into();
                let current = doc.members[at].clone();
                doc.record(event(
                    &person(&current),
                    "session.create",
                    ("user", &current.user.id, &current.user.name),
                    "",
                    "",
                ));
                let (csrf, expires) = doc.start_session(&current.user.id, &token, ttl);
                Ok(session_of(doc, &current, csrf, expires))
            })
            .await?;
        Ok(Response::new(CreateSessionResponse {
            session: session.into(),
            ..Default::default()
        })
        .with_header(http::header::SET_COOKIE, set_cookie(&token, directory)))
    }

    async fn delete_session(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, DeleteSessionRequest>,
    ) -> ServiceResult<DeleteSessionResponse> {
        if let Some(token) = session_token(ctx.headers()) {
            let digest = token_digest(&token);
            self.principals
                .directory
                .update(|doc| {
                    let Some(at) = doc.sessions.iter().position(|s| s.digest == digest) else {
                        return Ok::<_, ConnectError>(());
                    };
                    let session = doc.sessions.remove(at);
                    if let Some(member) = doc.member(&session.user).cloned() {
                        doc.record(event(
                            &person(&member),
                            "session.delete",
                            ("user", &member.user.id, &member.user.name),
                            "",
                            "",
                        ));
                    }
                    Ok(())
                })
                .await?;
        }
        Ok(Response::new(DeleteSessionResponse::default())
            .with_header(http::header::SET_COOKIE, clear_cookie()))
    }

    async fn decide_consent(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, DecideConsentRequest>,
    ) -> ServiceResult<DecideConsentResponse> {
        let request = request.to_owned_message();
        match request.decision.as_known() {
            Some(ConsentDecision::CONSENT_DECISION_ALLOW) => Err(refuse(
                ErrorCode::FailedPrecondition,
                "failed_precondition",
                "this engine has no OAuth issuer to grant a code (MT1); only a denial is answered",
                &[],
            )),
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

/// Registers `AuthService` on `router`.
pub(super) fn register(router: connectrpc::Router, principals: &Principals) -> connectrpc::Router {
    Arc::new(Auth {
        principals: principals.clone(),
    })
    .register(router)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_session_cookie_has_the_ruled_attributes() {
        let directory = Directory::in_memory();
        let cookie = set_cookie("ls_x", &directory);
        for part in [
            "loams_session=ls_x",
            "Path=/",
            "HttpOnly",
            "Secure",
            "SameSite=Lax",
        ] {
            assert!(cookie.contains(part), "{cookie}");
        }
        assert!(clear_cookie().contains("Max-Age=0"));
    }
}
