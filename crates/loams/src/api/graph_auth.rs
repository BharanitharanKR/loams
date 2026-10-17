//! Who may do what with Loams Graph on the main port (GR1 Task 24, design §48 §11, D750): the
//! caller of a graph RPC, and the built-in RBAC that expands the principal store's roles into
//! the `graph` type's relations until MT1's OpenFGA replaces it behind
//! [`GraphAuthorizer`].
//!
//! A caller's relation on a namespace's graphs (every graph of a namespace alike):
//!
//! - **The operator** (the developer on loopback with `LOAMS_AUTH_MODE=off`, a key from
//!   `LOAMS_API_KEYS_FILE`, any API key before first-run setup): `admin`, and project admin.
//! - **An org `owner` or `admin`**: `admin` on every namespace, and project admin (they
//!   administer every project).
//! - **A `member`**, in a namespace that is a project's environment: the project role their
//!   grants give them, directly or through a team (`viewer` → `reader`, `developer` → `writer`,
//!   `admin` → `admin` and project admin); none without a grant. A service account's key reaches
//!   its own project's environments as a `writer`.
//! - **A `member`**, in a namespace that is no project's environment: `writer` (as `member`
//!   writes Loams Git repositories, GT1 Task 16).
//! - **Anyone else** (not in the org, no principal): nothing.
//!
//! A credential bound to an environment (`<project>/<environment>`) reaches only that
//! environment's namespace, and its scopes cap the relation (`graph:read|write|admin`); both are
//! [`loams_graph::authz::decide`]'s.

use std::sync::Arc;

use async_trait::async_trait;
use connectrpc::{ConnectError, ErrorCode};
use loams_graph::authz::{Caller, CallerKind, GraphAuthorizer, Relation, Standing};
use loams_proto::loams::admin::v1 as admin_pb;

use crate::auth::authorizer::{Actor, Denied};
use crate::auth::directory::{Doc, Role};
use crate::auth::{Grant, Principal, PrincipalKind, Principals};

/// The graph caller of an admitted principal (or of none: [`Caller::anonymous`]).
pub(crate) fn caller_of(principal: Option<&Principal>) -> Caller {
    let Some(principal) = principal else {
        return Caller::anonymous();
    };
    let kind = match principal.kind {
        PrincipalKind::User => CallerKind::User,
        PrincipalKind::Session => CallerKind::Session,
        PrincipalKind::ApiKey => CallerKind::ApiKey,
        PrincipalKind::Tailscale => CallerKind::Tailscale,
        PrincipalKind::DevLoopback => CallerKind::DevLoopback,
    };
    Caller {
        id: principal.subject.clone(),
        kind,
        name: principal.display_name.clone(),
        email: principal.email.clone(),
        environment: principal.grant.environment.clone(),
        scopes: principal.grant.scopes.clone(),
    }
}

/// The principal a caller came from, for the org authorizer.
fn principal_of(caller: &Caller) -> Option<Principal> {
    let kind = match caller.kind {
        CallerKind::User => PrincipalKind::User,
        CallerKind::Session => PrincipalKind::Session,
        CallerKind::ApiKey => PrincipalKind::ApiKey,
        CallerKind::Tailscale => PrincipalKind::Tailscale,
        CallerKind::DevLoopback => PrincipalKind::DevLoopback,
        CallerKind::Anonymous => return None,
    };
    Some(Principal {
        subject: caller.id.clone(),
        kind,
        display_name: caller.name.clone(),
        email: caller.email.clone(),
        grant: Grant {
            environment: caller.environment.clone(),
            scopes: caller.scopes.clone(),
            ..Grant::default()
        },
    })
}

/// The built-in RBAC over the principal store (module docs).
#[derive(Clone)]
pub(crate) struct DirectoryGraphAuthz {
    principals: Principals,
}

impl std::fmt::Debug for DirectoryGraphAuthz {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryGraphAuthz")
            .finish_non_exhaustive()
    }
}

impl DirectoryGraphAuthz {
    pub(crate) fn new(principals: Principals) -> Self {
        Self { principals }
    }

    /// The caller as the org sees them: `Ok(None)` for someone outside it, and the operator as
    /// an `owner`.
    async fn actor(&self, caller: &Caller) -> Result<Option<Actor>, ConnectError> {
        let Some(principal) = principal_of(caller) else {
            return Ok(None);
        };
        if principal.kind == PrincipalKind::DevLoopback {
            return Ok(Some(operator(&principal)));
        }
        match self
            .principals
            .authorizer
            .authorize(&principal, Role::Member)
            .await
        {
            Ok(actor) => Ok(Some(actor)),
            // Before first-run setup the only keys are the operator's own (as Loams Git rules).
            Err(Denied::SetupRequired) if principal.kind == PrincipalKind::ApiKey => {
                Ok(Some(operator(&principal)))
            }
            Err(Denied::SetupRequired | Denied::RoleRequired { .. }) => Ok(None),
            Err(Denied::Unavailable(err)) => {
                tracing::warn!(error = %err, "reading the principal store for a graph check failed");
                Err(unavailable())
            }
        }
    }
}

fn operator(principal: &Principal) -> Actor {
    Actor {
        kind: admin_pb::SubjectKind::SUBJECT_KIND_USER,
        id: principal.subject.clone(),
        name: principal.display_name.clone(),
        role: Role::Owner,
        user: None,
    }
}

fn unavailable() -> ConnectError {
    ConnectError::new(
        ErrorCode::Unavailable,
        "the principal store could not be read; retry",
    )
}

/// The project and environment whose namespace is `namespace`, if any.
fn environment_of<'d>(
    doc: &'d Doc,
    namespace: &str,
) -> Option<(
    &'d crate::auth::directory::ProjectRec,
    &'d admin_pb::Environment,
)> {
    doc.projects.iter().find_map(|project| {
        project
            .environments
            .iter()
            .find(|env| env.namespace == namespace)
            .map(|env| (project, env))
    })
}

/// Whether a credential bound to `environment` (`<project>/<environment>`, or a namespace's
/// name) reaches `namespace`.
fn reaches(doc: &Doc, environment: &str, namespace: &str) -> bool {
    if environment == namespace {
        return true;
    }
    let Some((project, env)) = environment.split_once('/') else {
        return false;
    };
    doc.project(project)
        .and_then(|p| p.environments.iter().find(|e| e.slug == env))
        .is_some_and(|e| e.namespace == namespace)
}

/// A member's strongest project role in `project`: their own grants and their teams'.
fn project_role(
    doc: &Doc,
    project: &crate::auth::directory::ProjectRec,
    user: &str,
) -> Option<admin_pb::ProjectRole> {
    project
        .grants
        .iter()
        .filter(|grant| {
            let Some(subject) = grant.subject.as_option() else {
                return false;
            };
            match subject.kind.as_known() {
                Some(admin_pb::SubjectKind::SUBJECT_KIND_USER) => subject.id == user,
                Some(admin_pb::SubjectKind::SUBJECT_KIND_TEAM) => doc
                    .team(&subject.id)
                    .is_some_and(|team| team.members.iter().any(|m| m == user)),
                _ => false,
            }
        })
        .filter_map(|grant| grant.role.as_known())
        .min_by_key(|role| match role {
            admin_pb::ProjectRole::PROJECT_ROLE_ADMIN => 0,
            admin_pb::ProjectRole::PROJECT_ROLE_DEVELOPER => 1,
            admin_pb::ProjectRole::PROJECT_ROLE_VIEWER => 2,
            _ => 3,
        })
}

/// The standing of `actor` (`principal` is its caller) on `namespace` in `doc` (module docs).
fn standing_in(doc: &Doc, caller: &Caller, actor: Option<&Actor>, namespace: &str) -> Standing {
    let place = environment_of(doc, namespace);
    let protected = place.is_some_and(|(_, env)| env.protected);
    let in_environment = caller
        .environment
        .as_deref()
        .is_none_or(|env| reaches(doc, env, namespace));
    let (relation, project_admin) = match actor {
        None => (None, false),
        Some(actor) if actor.role >= Role::Admin => (Some(Relation::Admin), true),
        Some(actor) => match place {
            None => (Some(Relation::Writer), false),
            Some((project, _)) => match &actor.user {
                Some(user) => match project_role(doc, project, user) {
                    Some(admin_pb::ProjectRole::PROJECT_ROLE_ADMIN) => {
                        (Some(Relation::Admin), true)
                    }
                    Some(admin_pb::ProjectRole::PROJECT_ROLE_DEVELOPER) => {
                        (Some(Relation::Writer), false)
                    }
                    Some(admin_pb::ProjectRole::PROJECT_ROLE_VIEWER) => {
                        (Some(Relation::Reader), false)
                    }
                    _ => (None, false),
                },
                // A service account's key: its own project's environments.
                None => {
                    let id = caller.id.strip_prefix("apikey:").unwrap_or(&caller.id);
                    let own = doc
                        .key(id)
                        .is_some_and(|key| key.project == project.project.slug);
                    (own.then_some(Relation::Writer), false)
                }
            },
        },
    };
    Standing {
        relation,
        protected,
        project_admin,
        in_environment,
    }
}

#[async_trait]
impl GraphAuthorizer for DirectoryGraphAuthz {
    async fn standing(
        &self,
        caller: &Caller,
        namespace: &str,
        _graph: Option<&str>,
    ) -> Result<Standing, ConnectError> {
        let actor = self.actor(caller).await?;
        let doc = self.principals.directory.read().await.map_err(|err| {
            tracing::warn!(error = %err.0, "reading the principal store for a graph check failed");
            unavailable()
        })?;
        Ok(standing_in(&doc, caller, actor.as_ref(), namespace))
    }

    /// Relations are namespace-wide here: one read answers every graph.
    async fn visible(
        &self,
        caller: &Caller,
        namespace: &str,
        graphs: &[String],
    ) -> Result<Vec<bool>, ConnectError> {
        let standing = self.standing(caller, namespace, None).await?;
        let visible = loams_graph::authz::decide(
            caller,
            &standing,
            loams_graph::authz::Need::At(Relation::Reader),
        )
        .is_ok();
        Ok(vec![visible; graphs.len()])
    }
}

/// The authorizer the server's graph services use.
pub(crate) fn authorizer(principals: &Principals) -> Arc<dyn GraphAuthorizer> {
    Arc::new(DirectoryGraphAuthz::new(principals.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::SetupGate;
    use crate::auth::directory::{Directory, KeyRec, MemberRec, ProjectRec, TeamRec};
    use loams_graph::authz::{Need, decide};

    fn user(id: &str) -> admin_pb::User {
        admin_pb::User {
            id: id.into(),
            name: id.into(),
            email: format!("{id}@example.com"),
            ..Default::default()
        }
    }

    fn member(id: &str, role: Role) -> MemberRec {
        MemberRec {
            user: user(id),
            role,
            joined_at_ms: 0,
            password: String::new(),
        }
    }

    fn grant(
        kind: admin_pb::SubjectKind,
        id: &str,
        role: admin_pb::ProjectRole,
    ) -> admin_pb::Grant {
        admin_pb::Grant {
            subject: admin_pb::SubjectRef {
                kind: kind.into(),
                id: id.into(),
                ..Default::default()
            }
            .into(),
            role: role.into(),
            ..Default::default()
        }
    }

    fn env(project: &str, slug: &str, namespace: &str, protected: bool) -> admin_pb::Environment {
        admin_pb::Environment {
            slug: slug.into(),
            project: project.into(),
            namespace: namespace.into(),
            protected,
            ..Default::default()
        }
    }

    /// An org with a project `shop` (environments `shop-dev`, and `shop-prod`, protected), its
    /// grants, a team, and a service account key of the project.
    async fn principals() -> Principals {
        use admin_pb::ProjectRole as R;
        use admin_pb::SubjectKind as K;
        let directory = Directory::in_memory();
        directory
            .update(|doc| {
                doc.org = Some(admin_pb::Org {
                    id: "org".into(),
                    slug: "acme".into(),
                    ..Default::default()
                });
                doc.members = vec![
                    member("own", Role::Owner),
                    member("viewer", Role::Member),
                    member("dev", Role::Member),
                    member("padmin", Role::Member),
                    member("teamed", Role::Member),
                    member("stranger", Role::Member),
                ];
                doc.teams = vec![TeamRec {
                    team: admin_pb::Team {
                        id: "tm_1".into(),
                        slug: "core".into(),
                        ..Default::default()
                    },
                    members: vec!["teamed".into()],
                }];
                doc.projects = vec![ProjectRec {
                    project: admin_pb::Project {
                        id: "pr_1".into(),
                        slug: "shop".into(),
                        ..Default::default()
                    },
                    grants: vec![
                        grant(K::SUBJECT_KIND_USER, "viewer", R::PROJECT_ROLE_VIEWER),
                        grant(K::SUBJECT_KIND_USER, "dev", R::PROJECT_ROLE_DEVELOPER),
                        grant(K::SUBJECT_KIND_USER, "padmin", R::PROJECT_ROLE_ADMIN),
                        grant(K::SUBJECT_KIND_TEAM, "tm_1", R::PROJECT_ROLE_DEVELOPER),
                    ],
                    environments: vec![
                        env("shop", "dev", "shop-dev", false),
                        env("shop", "prod", "shop-prod", true),
                    ],
                }];
                doc.keys = vec![KeyRec {
                    key: admin_pb::ApiKey {
                        id: "k_sa".into(),
                        principal: admin_pb::SubjectRef {
                            kind: K::SUBJECT_KIND_SERVICE_ACCOUNT.into(),
                            id: "sa_1".into(),
                            ..Default::default()
                        }
                        .into(),
                        ..Default::default()
                    },
                    project: "shop".into(),
                    hash: String::new(),
                    role: Role::Member,
                    idempotency_key: String::new(),
                    created_by: "own".into(),
                    revoked: false,
                }];
                Ok::<_, Denied>(())
            })
            .await
            .expect("seed the directory");
        Principals::new(directory, SetupGate::Closed)
    }

    fn session(id: &str) -> Caller {
        Caller::new(id, CallerKind::Session)
    }

    async fn relation(authz: &DirectoryGraphAuthz, caller: &Caller, ns: &str) -> Option<Relation> {
        let standing = authz
            .standing(caller, ns, Some("g"))
            .await
            .expect("standing");
        decide(caller, &standing, Need::At(Relation::Reader))
            .ok()
            .and(standing.relation)
            .filter(|_| standing.in_environment)
    }

    #[tokio::test]
    async fn org_and_project_roles_expand_to_graph_relations() {
        let authz = DirectoryGraphAuthz::new(principals().await);
        let cases = [
            ("own", "shop-prod", Some(Relation::Admin)),
            ("own", "elsewhere", Some(Relation::Admin)),
            ("viewer", "shop-dev", Some(Relation::Reader)),
            ("dev", "shop-dev", Some(Relation::Writer)),
            ("padmin", "shop-prod", Some(Relation::Admin)),
            ("teamed", "shop-dev", Some(Relation::Writer)),
            // A member with no grant in the project reaches none of its environments...
            ("stranger", "shop-dev", None),
            // ...and writes a namespace that is no project's.
            ("stranger", "elsewhere", Some(Relation::Writer)),
            ("nobody", "elsewhere", None),
        ];
        for (who, ns, expected) in cases {
            assert_eq!(
                relation(&authz, &session(who), ns).await,
                expected,
                "{who} on {ns}"
            );
        }
        assert_eq!(
            relation(&authz, &Caller::anonymous(), "elsewhere").await,
            None
        );
        let loopback = Caller::new("dev-loopback", CallerKind::DevLoopback);
        assert_eq!(
            relation(&authz, &loopback, "shop-prod").await,
            Some(Relation::Admin)
        );
        // The service account's key: its project's environments, and no other project's.
        let key = Caller::new("apikey:k_sa", CallerKind::ApiKey);
        assert_eq!(
            relation(&authz, &key, "shop-dev").await,
            Some(Relation::Writer)
        );
    }

    #[tokio::test]
    async fn protected_environments_and_project_admins() {
        let authz = DirectoryGraphAuthz::new(principals().await);
        let prod = authz
            .standing(&session("padmin"), "shop-prod", Some("g"))
            .await
            .expect("standing");
        assert!(prod.protected && prod.project_admin);
        let dev = authz
            .standing(&session("dev"), "shop-dev", Some("g"))
            .await
            .expect("standing");
        assert!(!dev.protected && !dev.project_admin);
        // An org owner administers every project.
        let owner = authz
            .standing(&session("own"), "shop-prod", None)
            .await
            .expect("standing");
        assert!(owner.project_admin);
    }

    #[tokio::test]
    async fn an_environment_bound_credential_reaches_only_its_namespace() {
        let authz = DirectoryGraphAuthz::new(principals().await);
        let bound = Caller {
            environment: Some("shop/dev".into()),
            ..session("own")
        };
        let at = |ns: &'static str| {
            let (authz, bound) = (authz.clone(), bound.clone());
            async move {
                authz
                    .standing(&bound, ns, Some("g"))
                    .await
                    .expect("standing")
                    .in_environment
            }
        };
        assert!(at("shop-dev").await);
        assert!(!at("shop-prod").await);
        assert!(!at("elsewhere").await);
        // A credential bound to a namespace by name reaches it (the LV1 T14-2 form).
        let named = Caller {
            environment: Some("elsewhere".into()),
            ..session("own")
        };
        assert!(
            authz
                .standing(&named, "elsewhere", None)
                .await
                .expect("standing")
                .in_environment
        );
    }

    #[test]
    fn a_principal_becomes_a_caller_with_its_grant() {
        let principal = Principal {
            subject: "apikey:k1".into(),
            kind: PrincipalKind::ApiKey,
            display_name: "ci".into(),
            email: None,
            grant: Grant {
                environment: Some("shop/dev".into()),
                scopes: Grant::scopes_of(["graph:read"]),
                ..Grant::default()
            },
        };
        let caller = caller_of(Some(&principal));
        assert_eq!(caller.kind, CallerKind::ApiKey);
        assert_eq!(caller.environment.as_deref(), Some("shop/dev"));
        assert_eq!(caller.scope_cap(), Some(Relation::Reader));
        assert_eq!(caller_of(None).kind, CallerKind::Anonymous);
        assert_eq!(
            principal_of(&caller).map(|p| p.subject),
            Some("apikey:k1".into())
        );
    }
}
