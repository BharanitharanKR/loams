//! Grants (plan GT1 Task 16): namespace and repository coverage, access
//! levels, branch prefixes, and secrets that never print.

use loams_git::{
    Expect, GitAccess, GitDenied, GitGrant, NamespaceId, ObjectId, Principal, RefName, RefUpdate,
    RepoMatch, RepoName, Secret,
};

fn grant(access: GitAccess) -> GitGrant {
    GitGrant::whole_namespace(
        "local",
        NamespaceId(1),
        Principal::parse("ci").expect("principal"),
        access,
    )
}

fn repo(name: &str) -> RepoName {
    RepoName::parse(name).expect("name")
}

fn update(name: &str) -> RefUpdate {
    RefUpdate {
        name: RefName::parse(name).expect("ref"),
        expect: Expect::Absent,
        new: Some(ObjectId::try_from(&[1u8; 20][..]).expect("oid")),
    }
}

#[test]
fn namespace_from_token_not_url() {
    let g = grant(GitAccess::Admin);
    assert!(g.covers(NamespaceId(1), &repo("web")));
    // Another namespace is not found, whatever the access.
    assert_eq!(
        g.require(NamespaceId(2), &repo("web"), GitAccess::Read),
        Err(GitDenied::NotFound)
    );
    assert!(!g.covers_namespace(NamespaceId(2)));
    // A grant naming repositories hides the others the same way.
    let named = GitGrant {
        repos: RepoMatch::Names(vec![repo("web")]),
        ..g
    };
    assert_eq!(
        named.require(NamespaceId(1), &repo("api"), GitAccess::Read),
        Err(GitDenied::NotFound)
    );
    assert_eq!(
        named.require(NamespaceId(1), &repo("web"), GitAccess::Admin),
        Ok(())
    );
}

#[test]
fn read_token_cannot_push() {
    let g = grant(GitAccess::Read);
    assert_eq!(
        g.require(NamespaceId(1), &repo("web"), GitAccess::Read),
        Ok(())
    );
    assert_eq!(
        g.check_push(NamespaceId(1), &repo("web"), &[update("refs/heads/main")]),
        Err(GitDenied::AccessRequired {
            need: GitAccess::Write,
            have: GitAccess::Read
        })
    );
}

#[test]
fn branch_prefix_scope_refuses_push() {
    let g = GitGrant {
        branch_prefixes: vec!["refs/heads/agent/".into()],
        ..grant(GitAccess::Write)
    };
    assert_eq!(
        g.check_push(
            NamespaceId(1),
            &repo("web"),
            &[
                update("refs/heads/agent/fix-1"),
                update("refs/heads/agent/fix-2")
            ]
        ),
        Ok(())
    );
    match g.check_push(
        NamespaceId(1),
        &repo("web"),
        &[update("refs/heads/agent/fix-1"), update("refs/heads/main")],
    ) {
        Err(GitDenied::OutOfScope { name }) => assert_eq!(name.as_str(), "refs/heads/main"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn admin_needed_for_delete_and_config() {
    for access in [GitAccess::Read, GitAccess::Write] {
        assert_eq!(
            grant(access).require(NamespaceId(1), &repo("web"), GitAccess::Admin),
            Err(GitDenied::AccessRequired {
                need: GitAccess::Admin,
                have: access
            })
        );
    }
    assert_eq!(
        grant(GitAccess::Admin).require(NamespaceId(1), &repo("web"), GitAccess::Admin),
        Ok(())
    );
}

#[test]
fn agent_grant_has_no_env_by_default() {
    // A grant built without an environment names none (D844, Q760's default).
    assert!(grant(GitAccess::Write).env.is_empty());
}

#[test]
fn token_never_logged() {
    let token = Secret::new("lgt_0123456789abcdef".to_string());
    for printed in [
        format!("{token:?}"),
        format!("{token}"),
        format!("{:?}", Some(&token)),
    ] {
        assert!(!printed.contains("lgt_"), "{printed}");
    }
}
