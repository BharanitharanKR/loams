//! Authorization (§36 §10, D182, D397; plan GT1 Task 16, the morning
//! version's Task 18): what a credential may do with repositories, as a
//! [`GitGrant`], and the [`GitAuthorizer`] seam that turns a git token into
//! one.
//!
//! The namespace comes from the credential, never from the URL alone: a
//! request that names a repository its grant does not cover is answered as
//! if the repository did not exist ([`GitDenied::NotFound`]), so a token
//! cannot probe other tenants.

use std::fmt;

use async_trait::async_trait;

use crate::ids::{NamespaceId, Principal, RefName, RepoName, RepoPath, WsId};
use crate::reflog::RefUpdate;

/// A value that must never be printed: tokens, vended credentials, vault
/// values, data keys. `Debug` and `Display` print `[redacted]`.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
    /// Wraps `value`.
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// The value, for the one place that must use it.
    pub fn expose(&self) -> &T {
        &self.0
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

/// How much a grant may do with the repositories it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GitAccess {
    /// Clone, fetch, list refs, read history.
    Read,
    /// Also push (within the branch prefixes) and edit workspaces.
    Write,
    /// Also create, fork, delete and configure repositories.
    Admin,
}

impl GitAccess {
    /// `read`, `write` or `admin`.
    pub fn as_str(self) -> &'static str {
        match self {
            GitAccess::Read => "read",
            GitAccess::Write => "write",
            GitAccess::Admin => "admin",
        }
    }
}

/// Which repositories of the namespace a grant covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepoMatch {
    /// Every repository.
    All,
    /// These, by name.
    Names(Vec<RepoName>),
}

/// Which workspaces a grant may use (§19.4, Q750).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WsMatch {
    /// The principal's own.
    Own,
    /// Every workspace of the covered repositories.
    All,
    /// These.
    Ids(Vec<WsId>),
}

/// What a grant may do with one environment's values (§19.13.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EnvAccess {
    /// Read values.
    Read,
    /// Seal new values (with or without reading, Q760).
    Write,
    /// Also promote.
    Promote,
}

/// One environment the grant reaches (§19.13.7). Agents carry none unless
/// granted (D844).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvGrant {
    /// The environment's name.
    pub environment: String,
    /// What it may do there.
    pub access: EnvAccess,
}

/// The build cache's trust classes (§36 §8.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TrustClass {
    /// Written only by trusted CI.
    Trusted,
    /// Written by anyone with a grant; read by all.
    Untrusted,
    /// Private to its principal.
    Scratch,
}

/// What a credential may do (the plan's shared contract).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitGrant {
    /// The organisation (`local` until MT1).
    pub org: String,
    /// The one namespace the grant reaches (D182).
    pub namespace: NamespaceId,
    /// Who holds it.
    pub principal: Principal,
    /// Which repositories.
    pub repos: RepoMatch,
    /// How much.
    pub access: GitAccess,
    /// Refs a write may change: each update's name must start with one of
    /// them; empty is every ref.
    pub branch_prefixes: Vec<String>,
    /// Paths a write may change (D397); empty is the whole tree. Checked by
    /// workspaces and the receive path (GT4 cones).
    pub cones: Vec<RepoPath>,
    /// Which workspaces.
    pub workspaces: WsMatch,
    /// Which environments (§19.13.7); empty for agents unless granted.
    pub env: Vec<EnvGrant>,
    /// The build cache class, if any.
    pub cache_class: Option<TrustClass>,
    /// Whether the crates mirror may be read.
    pub registry_read: bool,
}

/// Why a grant refuses.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GitDenied {
    /// The repository is outside the grant: answered as not found, with no
    /// detail (D182).
    #[error("not found")]
    NotFound,
    /// The grant covers the repository with less access than needed.
    #[error("{need} access is needed; this credential has {have}", need = need.as_str(), have = have.as_str())]
    AccessRequired {
        /// What the call needs.
        need: GitAccess,
        /// What the grant has.
        have: GitAccess,
    },
    /// A ref the write would change is outside the branch prefixes.
    #[error("{name} is outside this credential's branch prefixes")]
    OutOfScope {
        /// The first such ref.
        name: RefName,
    },
}

impl GitGrant {
    /// A grant of `access` on every repository of `namespace` for
    /// `principal`, with no branch prefix, cone, environment or cache class.
    pub fn whole_namespace(
        org: impl Into<String>,
        namespace: NamespaceId,
        principal: Principal,
        access: GitAccess,
    ) -> Self {
        Self {
            org: org.into(),
            namespace,
            principal,
            repos: RepoMatch::All,
            access,
            branch_prefixes: Vec::new(),
            cones: Vec::new(),
            workspaces: if access >= GitAccess::Admin {
                WsMatch::All
            } else {
                WsMatch::Own
            },
            env: Vec::new(),
            cache_class: None,
            registry_read: false,
        }
    }

    /// Whether the grant reaches repository `repo` of `namespace` at all.
    pub fn covers(&self, namespace: NamespaceId, repo: &RepoName) -> bool {
        self.namespace == namespace
            && match &self.repos {
                RepoMatch::All => true,
                RepoMatch::Names(names) => names.contains(repo),
            }
    }

    /// Whether the grant reaches `namespace` (a namespace-wide call such as
    /// `ListRepos` or `CreateRepo` checks this, then the access).
    pub fn covers_namespace(&self, namespace: NamespaceId) -> bool {
        self.namespace == namespace
    }

    /// `need` on `repo` of `namespace`: `NotFound` outside the grant,
    /// `AccessRequired` below `need`.
    pub fn require(
        &self,
        namespace: NamespaceId,
        repo: &RepoName,
        need: GitAccess,
    ) -> Result<(), GitDenied> {
        if !self.covers(namespace, repo) {
            return Err(GitDenied::NotFound);
        }
        self.require_access(need)
    }

    /// `need`, whatever the repository.
    pub fn require_access(&self, need: GitAccess) -> Result<(), GitDenied> {
        if self.access < need {
            return Err(GitDenied::AccessRequired {
                need,
                have: self.access,
            });
        }
        Ok(())
    }

    /// A push of `updates` to `repo` (§36 §6.3 step 3, D397): write access
    /// and every ref inside the branch prefixes.
    pub fn check_push(
        &self,
        namespace: NamespaceId,
        repo: &RepoName,
        updates: &[RefUpdate],
    ) -> Result<(), GitDenied> {
        self.require(namespace, repo, GitAccess::Write)?;
        if self.branch_prefixes.is_empty() {
            return Ok(());
        }
        match updates.iter().find(|update| {
            !self
                .branch_prefixes
                .iter()
                .any(|prefix| update.name.as_str().starts_with(prefix.as_str()))
        }) {
            Some(update) => Err(GitDenied::OutOfScope {
                name: update.name.clone(),
            }),
            None => Ok(()),
        }
    }
}

/// Why a credential was not admitted.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GitAuthError {
    /// No credential, or one this authorizer does not know. Never carries
    /// the credential.
    #[error("a valid git token is required")]
    Unauthenticated,
    /// The credential could not be checked now.
    #[error("the credential could not be checked: {0}")]
    Unavailable(String),
}

/// Turns a git token into a grant (§36 §10). `LoopbackDevAuthorizer` (in
/// `loams`, a file of token hashes) serves until MT1, whose vended tokens
/// carry the grant as claims (Ruling 30).
#[async_trait]
pub trait GitAuthorizer: Send + Sync + fmt::Debug {
    /// The grant of `token`.
    async fn authorize(&self, token: &Secret<String>) -> Result<GitGrant, GitAuthError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_never_print() {
        let token = Secret::new("lgt_supersecret".to_string());
        assert_eq!(format!("{token:?}"), "[redacted]");
        assert_eq!(format!("{token}"), "[redacted]");
        assert_eq!(token.expose(), "lgt_supersecret");
    }
}
