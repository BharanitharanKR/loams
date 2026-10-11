//! Who a connection is (design §50 §7, plan DD1 Task 7).
//!
//! A WebSocket upgrade carries `Authorization: Bearer <token>`. The daemon's
//! main token (the contents of `agentd.token`) gives role `owner`; a live
//! scoped run token, issued for one harness run, gives role `mcp` bound to
//! that run's session. Every comparison is constant time, and no token
//! value is ever logged, formatted by `Debug` or echoed in an error.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use base64::Engine as _;
use loams_agentd_proto::rpc::Role;
use subtle::ConstantTimeEq;

/// A token value. `Debug` never shows it.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// 32 bytes from the OS RNG, base64url without padding (43 characters).
    pub fn generate() -> std::io::Result<Self> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(Self(
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes),
        ))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Constant time in the contents. A length mismatch returns early, which
    /// reveals only the length (every token has the same public length).
    pub fn matches(&self, presented: &[u8]) -> bool {
        self.0.as_bytes().ct_eq(presented).into()
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// An authenticated connection's identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub role: Role,
    /// For role `mcp`: the run the token was issued for, and its session.
    pub run: Option<RunScope>,
}

impl Identity {
    pub fn owner() -> Self {
        Self {
            role: Role::Owner,
            run: None,
        }
    }
}

/// The run a scoped token belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunScope {
    pub run_id: String,
    /// The session (the fork's `chatId`) the run belongs to.
    pub session_id: String,
}

/// A scoped run token, handed to one harness run's MCP shim
/// (`LOAMS_AGENTD_RUN_TOKEN`). Valid until [`ScopedTokens::revoke`].
#[derive(Debug, Clone)]
pub struct RunToken {
    pub scope: RunScope,
    pub token: Secret,
}

/// The live scoped run tokens.
pub struct ScopedTokens {
    live: Mutex<HashMap<String, RunToken>>,
    /// Bumped on every revocation, so open connections can re-check.
    revoked: tokio::sync::watch::Sender<u64>,
}

impl Default for ScopedTokens {
    fn default() -> Self {
        Self {
            live: Mutex::default(),
            revoked: tokio::sync::watch::channel(0).0,
        }
    }
}

impl std::fmt::Debug for ScopedTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedTokens")
            .field("live", &self.lock().len())
            .finish()
    }
}

impl ScopedTokens {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, RunToken>> {
        self.live.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A new token for one run of `session_id`, valid until revoked.
    pub fn issue(&self, session_id: &str) -> std::io::Result<RunToken> {
        let mut id = [0u8; 16];
        getrandom::fill(&mut id).map_err(|error| std::io::Error::other(error.to_string()))?;
        let token = RunToken {
            scope: RunScope {
                run_id: format!("run-{}", hex(&id)),
                session_id: session_id.to_owned(),
            },
            token: Secret::generate()?,
        };
        self.lock()
            .insert(token.scope.run_id.clone(), token.clone());
        Ok(token)
    }

    /// The run is over: its token no longer authenticates, and connections
    /// made with it are refused on their next call.
    pub fn revoke(&self, run_id: &str) {
        if self.lock().remove(run_id).is_some() {
            self.revoked
                .send_modify(|count| *count = count.wrapping_add(1));
        }
    }

    /// Changes after each revocation.
    pub(crate) fn revocations(&self) -> tokio::sync::watch::Receiver<u64> {
        self.revoked.subscribe()
    }

    pub fn is_live(&self, run_id: &str) -> bool {
        self.lock().contains_key(run_id)
    }

    /// The scope of the live token equal to `presented`. Every live token is
    /// compared, in constant time each, whichever matches.
    fn find(&self, presented: &[u8]) -> Option<RunScope> {
        let live = self.lock();
        let mut found = None;
        for token in live.values() {
            if token.token.matches(presented) {
                found = Some(token.scope.clone());
            }
        }
        found
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The main token and the scoped run tokens a server accepts.
#[derive(Debug)]
pub struct Credentials {
    owner: Secret,
    scoped: Arc<ScopedTokens>,
}

impl Credentials {
    pub fn new(owner: Secret, scoped: Arc<ScopedTokens>) -> Self {
        Self { owner, scoped }
    }

    pub fn scoped(&self) -> &Arc<ScopedTokens> {
        &self.scoped
    }

    /// The identity an `Authorization` header value proves, if any. Only
    /// `Bearer <token>` (scheme case-insensitive, one space) is accepted.
    pub fn authenticate(&self, authorization: &[u8]) -> Option<Identity> {
        let presented = bearer(authorization)?;
        // Both checks always run, so timing does not tell owner from mcp.
        let owner = self.owner.matches(presented);
        let scoped = self.scoped.find(presented);
        if owner {
            return Some(Identity::owner());
        }
        scoped.map(|run| Identity {
            role: Role::Mcp,
            run: Some(run),
        })
    }

    /// Whether `identity` may still call: a scoped token dies with its run.
    pub fn still_valid(&self, identity: &Identity) -> bool {
        match &identity.run {
            Some(run) => self.scoped.is_live(&run.run_id),
            None => true,
        }
    }
}

/// The token of `Bearer <token>`.
fn bearer(value: &[u8]) -> Option<&[u8]> {
    const SCHEME: &[u8] = b"bearer ";
    if value.len() <= SCHEME.len() || !value[..SCHEME.len()].eq_ignore_ascii_case(SCHEME) {
        return None;
    }
    let token = &value[SCHEME.len()..];
    (!token.is_empty() && !token.contains(&b' ')).then_some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials() -> (Credentials, Arc<ScopedTokens>) {
        let scoped = Arc::new(ScopedTokens::new());
        (
            Credentials::new(Secret::new("owner-token"), scoped.clone()),
            scoped,
        )
    }

    #[test]
    fn the_main_token_is_owner() {
        let (credentials, _) = credentials();
        assert_eq!(
            credentials.authenticate(b"Bearer owner-token"),
            Some(Identity::owner())
        );
        assert_eq!(
            credentials.authenticate(b"bearer owner-token"),
            Some(Identity::owner())
        );
    }

    #[test]
    fn anything_else_is_nobody() {
        let (credentials, _) = credentials();
        for value in [
            &b""[..],
            b"Bearer",
            b"Bearer ",
            b"Bearer owner-toke",
            b"Bearer owner-tokenn",
            b"Bearer  owner-token",
            b"Bearer owner-token ",
            b"Basic owner-token",
            b"owner-token",
            b"Bearer OWNER-TOKEN",
        ] {
            assert_eq!(
                credentials.authenticate(value),
                None,
                "{}",
                String::from_utf8_lossy(value)
            );
        }
    }

    #[test]
    fn scoped_tokens_are_mcp_until_revoked() {
        let (credentials, scoped) = credentials();
        let run = scoped.issue("chat-1").unwrap();
        assert_eq!(run.token.expose().len(), 43);
        let identity = credentials
            .authenticate(format!("Bearer {}", run.token.expose()).as_bytes())
            .expect("a live run token authenticates");
        assert_eq!(identity.role, Role::Mcp);
        assert_eq!(identity.run.as_ref(), Some(&run.scope));
        assert_eq!(run.scope.session_id, "chat-1");
        assert!(credentials.still_valid(&identity));

        scoped.revoke(&run.scope.run_id);
        assert!(!credentials.still_valid(&identity));
        assert_eq!(
            credentials.authenticate(format!("Bearer {}", run.token.expose()).as_bytes()),
            None
        );
    }

    #[test]
    fn every_issue_is_a_new_token_and_run() {
        let scoped = ScopedTokens::new();
        let first = scoped.issue("chat").unwrap();
        let second = scoped.issue("chat").unwrap();
        assert_ne!(first.scope.run_id, second.scope.run_id);
        assert!(!first.token.matches(second.token.expose().as_bytes()));
    }

    #[test]
    fn debug_never_shows_a_token() {
        let scoped = Arc::new(ScopedTokens::new());
        let run = scoped.issue("chat").unwrap();
        let credentials = Credentials::new(Secret::new("owner-secret-value"), scoped.clone());
        let text = format!("{run:?} {credentials:?} {scoped:?}");
        assert!(!text.contains("owner-secret-value"), "{text}");
        assert!(!text.contains(run.token.expose()), "{text}");
    }
}
