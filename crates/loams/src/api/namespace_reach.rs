//! Which namespaces a caller can reach, for `NamespaceService/ListNamespaces`
//! (the 0.1.0 console's Namespaces page, B7).
//!
//! The rules are the built-in RBAC's for a namespace's data (the ones
//! `graph_auth` expands into graph relations), at the weakest relation, so a
//! namespace is listed exactly when its data can be read:
//!
//! - **The operator** (the developer on loopback with `LOAMS_AUTH_MODE=off`,
//!   a key from `LOAMS_API_KEYS_FILE`, any API key before first-run setup) and
//!   an org **`owner` or `admin`**: every namespace.
//! - **A `member`**: every namespace that is no project's environment, and
//!   the environments of the projects their grants (direct or through a team)
//!   give them any role in. A service account's key: its own project's
//!   environments, and the namespaces that are no project's.
//! - **Anyone else** (outside the org, or no principal): none.
//!
//! A credential bound to an environment (`<project>/<environment>`, or a
//! namespace's name) reaches only that environment's namespace, whatever its
//! role.
//!
//! This module is not behind the `graph` feature: listing namespaces is part
//! of every variant, and `graph_auth` stays the graph's own.

use connectrpc::{ConnectError, ErrorCode};
use loams_proto::loams::admin::v1 as admin_pb;

use crate::auth::authorizer::Denied;
use crate::auth::directory::{Doc, ProjectRec, Role};
use crate::auth::{Principal, PrincipalKind, Principals};

/// Who is asking, as far as reaching a namespace goes.
enum Standing {
    /// Every namespace (the operator, an org owner or admin).
    All,
    /// A person in the org with this user id.
    Member(String),
    /// A service account's API key (its id, without `apikey:`).
    ServiceKey(String),
    /// No namespace.
    Nothing,
}

/// The namespaces of `names` that `principal` reaches, in their order
/// (module docs).
pub(crate) async fn reachable(
    principals: &Principals,
    principal: Option<&Principal>,
    names: Vec<String>,
) -> Result<Vec<String>, ConnectError> {
    let Some(principal) = principal else {
        return Ok(Vec::new());
    };
    let standing = standing(principals, principal).await?;
    if matches!(standing, Standing::Nothing) {
        return Ok(Vec::new());
    }
    if matches!(standing, Standing::All) && principal.grant.environment.is_none() {
        return Ok(names);
    }
    let doc = principals.directory.read().await.map_err(|err| {
        tracing::warn!(error = %err.0, "reading the principal store to list namespaces failed");
        unavailable()
    })?;
    Ok(names
        .into_iter()
        .filter(|ns| reaches_namespace(&doc, principal, &standing, ns))
        .collect())
}

/// The caller's standing in the org.
async fn standing(
    principals: &Principals,
    principal: &Principal,
) -> Result<Standing, ConnectError> {
    if principal.kind == PrincipalKind::DevLoopback {
        return Ok(Standing::All);
    }
    match principals
        .authorizer
        .authorize(principal, Role::Member)
        .await
    {
        Ok(actor) if actor.role >= Role::Admin => Ok(Standing::All),
        Ok(actor) => Ok(match actor.user {
            Some(user) => Standing::Member(user),
            None if principal.kind == PrincipalKind::ApiKey => {
                let id = principal
                    .subject
                    .strip_prefix("apikey:")
                    .unwrap_or(&principal.subject);
                Standing::ServiceKey(id.to_owned())
            }
            None => Standing::Nothing,
        }),
        // Before first-run setup the only keys are the operator's own.
        Err(Denied::SetupRequired) if principal.kind == PrincipalKind::ApiKey => Ok(Standing::All),
        Err(Denied::SetupRequired | Denied::RoleRequired { .. }) => Ok(Standing::Nothing),
        Err(Denied::Unavailable(err)) => {
            tracing::warn!(error = %err, "reading the principal store to list namespaces failed");
            Err(unavailable())
        }
    }
}

fn reaches_namespace(doc: &Doc, principal: &Principal, standing: &Standing, ns: &str) -> bool {
    if let Some(bound) = principal.grant.environment.as_deref()
        && !bound_reaches(doc, bound, ns)
    {
        return false;
    }
    let project = environment_project(doc, ns);
    match (standing, project) {
        (Standing::Nothing, _) => false,
        (Standing::All, _) | (_, None) => true,
        (Standing::Member(user), Some(project)) => has_project_role(doc, project, user),
        (Standing::ServiceKey(id), Some(project)) => doc
            .key(id)
            .is_some_and(|key| key.project == project.project.slug),
    }
}

/// The project whose environment `ns` is, if any.
fn environment_project<'d>(doc: &'d Doc, ns: &str) -> Option<&'d ProjectRec> {
    doc.projects
        .iter()
        .find(|project| project.environments.iter().any(|env| env.namespace == ns))
}

/// Whether a credential bound to `environment` (`<project>/<environment>`,
/// or a namespace's name) reaches `ns`.
fn bound_reaches(doc: &Doc, environment: &str, ns: &str) -> bool {
    if environment == ns {
        return true;
    }
    let Some((project, env)) = environment.split_once('/') else {
        return false;
    };
    doc.project(project)
        .and_then(|p| p.environments.iter().find(|e| e.slug == env))
        .is_some_and(|e| e.namespace == ns)
}

/// Whether `user` holds any project role in `project`, directly or through a
/// team.
fn has_project_role(doc: &Doc, project: &ProjectRec, user: &str) -> bool {
    use admin_pb::ProjectRole as R;
    project.grants.iter().any(|grant| {
        let held = matches!(
            grant.role.as_known(),
            Some(R::PROJECT_ROLE_ADMIN | R::PROJECT_ROLE_DEVELOPER | R::PROJECT_ROLE_VIEWER)
        );
        let Some(subject) = grant.subject.as_option() else {
            return false;
        };
        held && match subject.kind.as_known() {
            Some(admin_pb::SubjectKind::SUBJECT_KIND_USER) => subject.id == user,
            Some(admin_pb::SubjectKind::SUBJECT_KIND_TEAM) => doc
                .team(&subject.id)
                .is_some_and(|team| team.members.iter().any(|m| m == user)),
            _ => false,
        }
    })
}

fn unavailable() -> ConnectError {
    ConnectError::new(
        ErrorCode::Unavailable,
        "the principal store could not be read; retry",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::directory::{Directory, KeyRec, MemberRec, TeamRec};
    use crate::auth::{Grant, SetupGate};

    fn member(id: &str, role: Role) -> MemberRec {
        MemberRec {
            user: admin_pb::User {
                id: id.into(),
                name: id.into(),
                email: format!("{id}@example.com"),
                ..Default::default()
            },
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

    fn env(project: &str, slug: &str, namespace: &str) -> admin_pb::Environment {
        admin_pb::Environment {
            slug: slug.into(),
            project: project.into(),
            namespace: namespace.into(),
            ..Default::default()
        }
    }

    /// An org with projects `shop` (environments `shop-dev`, `shop-prod`) and
    /// `ops` (`ops-prod`), grants in `shop` only, and a `shop` service key.
    async fn principals(setup: bool) -> Principals {
        use admin_pb::ProjectRole as R;
        use admin_pb::SubjectKind as K;
        let directory = Directory::in_memory();
        if setup {
            directory
                .update(|doc| {
                    doc.org = Some(admin_pb::Org {
                        id: "org".into(),
                        slug: "acme".into(),
                        ..Default::default()
                    });
                    doc.members = vec![
                        member("own", Role::Owner),
                        member("adm", Role::Admin),
                        member("viewer", Role::Member),
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
                    doc.projects = vec![
                        ProjectRec {
                            project: admin_pb::Project {
                                id: "pr_1".into(),
                                slug: "shop".into(),
                                ..Default::default()
                            },
                            grants: vec![
                                grant(K::SUBJECT_KIND_USER, "viewer", R::PROJECT_ROLE_VIEWER),
                                grant(K::SUBJECT_KIND_TEAM, "tm_1", R::PROJECT_ROLE_DEVELOPER),
                            ],
                            environments: vec![
                                env("shop", "dev", "shop-dev"),
                                env("shop", "prod", "shop-prod"),
                            ],
                        },
                        ProjectRec {
                            project: admin_pb::Project {
                                id: "pr_2".into(),
                                slug: "ops".into(),
                                ..Default::default()
                            },
                            grants: vec![],
                            environments: vec![env("ops", "prod", "ops-prod")],
                        },
                    ];
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
        }
        Principals::new(directory, SetupGate::Closed)
    }

    fn all() -> Vec<String> {
        ["default", "ops-prod", "scratch", "shop-dev", "shop-prod"]
            .map(String::from)
            .to_vec()
    }

    fn principal(subject: &str, kind: PrincipalKind, environment: Option<&str>) -> Principal {
        Principal {
            subject: subject.into(),
            kind,
            display_name: subject.into(),
            email: None,
            grant: Grant {
                environment: environment.map(str::to_owned),
                ..Grant::default()
            },
        }
    }

    fn session(id: &str, environment: Option<&str>) -> Principal {
        principal(id, PrincipalKind::Session, environment)
    }

    async fn list(principals: &Principals, who: Option<&Principal>) -> Vec<&'static str> {
        let reached = reachable(principals, who, all()).await.expect("reachable");
        all()
            .into_iter()
            .zip(["default", "ops-prod", "scratch", "shop-dev", "shop-prod"])
            .filter(|(ns, _)| reached.contains(ns))
            .map(|(_, name)| name)
            .collect()
    }

    #[tokio::test]
    async fn the_authz_matrix() {
        let p = principals(true).await;
        let every = vec!["default", "ops-prod", "scratch", "shop-dev", "shop-prod"];
        let unowned = vec!["default", "scratch"];
        let cases: Vec<(&str, Option<Principal>, Vec<&str>)> = vec![
            ("the owner", Some(session("own", None)), every.clone()),
            ("an org admin", Some(session("adm", None)), every.clone()),
            (
                "sign-in off (dev-loopback)",
                Some(crate::auth::Auth::dev_loopback()),
                every.clone(),
            ),
            (
                "a key from the keys file",
                Some(principal("apikey:file", PrincipalKind::ApiKey, None)),
                every.clone(),
            ),
            (
                "a project viewer",
                Some(session("viewer", None)),
                vec!["default", "scratch", "shop-dev", "shop-prod"],
            ),
            (
                "a viewer bound to one environment",
                Some(session("viewer", Some("shop/dev"))),
                vec!["shop-dev"],
            ),
            (
                "a viewer bound to an environment of a project they hold no role in",
                Some(session("viewer", Some("ops/prod"))),
                vec![],
            ),
            (
                "the owner bound to a namespace by name",
                Some(session("own", Some("ops-prod"))),
                vec!["ops-prod"],
            ),
            (
                "a team member",
                Some(session("teamed", None)),
                vec!["default", "scratch", "shop-dev", "shop-prod"],
            ),
            (
                "a member with no grant",
                Some(session("stranger", None)),
                unowned.clone(),
            ),
            (
                "the project's service key",
                Some(principal("apikey:k_sa", PrincipalKind::ApiKey, None)),
                vec!["default", "scratch", "shop-dev", "shop-prod"],
            ),
            (
                "someone outside the org",
                Some(session("nobody", None)),
                vec![],
            ),
            ("anonymous", None, vec![]),
        ];
        for (who, principal, expected) in cases {
            assert_eq!(list(&p, principal.as_ref()).await, expected, "{who}");
        }
    }

    #[tokio::test]
    async fn before_setup_only_the_operator_lists() {
        let p = principals(false).await;
        let every = vec!["default", "ops-prod", "scratch", "shop-dev", "shop-prod"];
        assert_eq!(
            list(&p, Some(&crate::auth::Auth::dev_loopback())).await,
            every
        );
        assert_eq!(
            list(
                &p,
                Some(&principal("apikey:file", PrincipalKind::ApiKey, None))
            )
            .await,
            every
        );
        assert!(list(&p, Some(&session("own", None))).await.is_empty());
        assert!(list(&p, None).await.is_empty());
    }
}
