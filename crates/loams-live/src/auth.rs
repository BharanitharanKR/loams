//! Who is calling Live (LV1 plan Task 14, design §45 §5).
//!
//! Live verifies no credential itself: the engine's authentication layer
//! (`crates/loams/src/auth`: console sessions, OIDC tokens, API keys,
//! tailnet users) admits the request in front of both Live services and
//! hands Live a [`Caller`] in the request's extensions, with what the
//! credential reaches (the engine principal's grant: org, environment, scopes,
//! expiry). Where a source names none of them, the caller has the
//! self-hosted default: the install's one org and environment, and every
//! scope. A request with no `Caller` (a server started without the engine's
//! layer, as the crate's own tests do) is [`Caller::unauthenticated`].

use std::collections::BTreeSet;

use connectrpc::RequestContext;

/// The id a request without a [`Caller`] is recorded as.
pub use crate::deploy::UNAUTHENTICATED;

/// What kind of credential admitted the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CallerKind {
    /// An OIDC or Loams token of a person.
    User,
    /// A console session.
    Session,
    /// An API key.
    ApiKey,
    /// A tailnet user, as `tailscale serve` named them.
    Tailscale,
    /// The engine runs without authentication, on loopback.
    DevLoopback,
    /// No authentication layer ran (a bare Live server).
    Unauthenticated,
}

impl CallerKind {
    /// The kind's name in logs and audit lines.
    pub fn as_str(self) -> &'static str {
        match self {
            CallerKind::User => "user",
            CallerKind::Session => "session",
            CallerKind::ApiKey => "api_key",
            CallerKind::Tailscale => "tailscale",
            CallerKind::DevLoopback => "dev_loopback",
            CallerKind::Unauthenticated => "unauthenticated",
        }
    }
}

/// Who is calling: the principal the engine's authentication layer
/// admitted, put in the request's extensions. It never holds a credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// The principal's id (an OIDC subject, `apikey:<id>`, a user id,
    /// `tailscale:<login>` or `dev-loopback`).
    pub id: String,
    pub kind: CallerKind,
    /// The org the credential belongs to, when its source names one.
    pub org: Option<String>,
    /// The environment the credential is bound to (`<project>/<env>`),
    /// when its source names one. Such a credential reaches only the app of
    /// that environment (LV1 plan Task 15).
    pub environment: Option<String>,
    /// The scopes it carries; `None` is every scope.
    pub scopes: Option<BTreeSet<String>>,
    /// When the credential expires, Unix ms, when known.
    pub expires_at_ms: Option<u64>,
}

impl Caller {
    /// A caller of `kind` with id `id` and the self-hosted default grant.
    pub fn new(id: impl Into<String>, kind: CallerKind) -> Self {
        Caller {
            id: id.into(),
            kind,
            org: None,
            environment: None,
            scopes: None,
            expires_at_ms: None,
        }
    }

    /// The caller of a request no authentication layer admitted.
    pub fn unauthenticated() -> Self {
        Caller::new(UNAUTHENTICATED, CallerKind::Unauthenticated)
    }

    /// The caller of `ctx`: the layer's [`Caller`], else
    /// [`unauthenticated`](Self::unauthenticated).
    pub fn of(ctx: &RequestContext) -> Self {
        ctx.extensions()
            .get::<Caller>()
            .cloned()
            .unwrap_or_else(Caller::unauthenticated)
    }

    /// Whether the credential carries `scope` (every scope when it names
    /// none).
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.as_ref().is_none_or(|s| s.contains(scope))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_default_caller_has_every_scope_and_no_environment() {
        let caller = Caller::new("apikey:01", CallerKind::ApiKey);
        assert!(caller.has_scope("live:deploy"));
        assert_eq!(caller.environment, None);
        let narrow = Caller {
            scopes: Some(["live:call".to_string()].into()),
            ..caller
        };
        assert!(narrow.has_scope("live:call") && !narrow.has_scope("live:admin"));
        assert_eq!(Caller::unauthenticated().id, UNAUTHENTICATED);
    }
}
