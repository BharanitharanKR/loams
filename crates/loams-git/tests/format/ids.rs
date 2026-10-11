//! Identifiers: one spelling each, and git's ref-name rules.

use std::process::Command;

use loams_git::{
    IdemKey, MountId, Principal, RefName, RepoId, RepoName, RepoPath, SecretId, Seq, SnapN,
    TraceParent, WsId, WsName,
};
use proptest::prelude::*;
use ulid::Ulid;

/// 40 names: (name, valid). Every valid one is a full ref under `refs/`.
const REF_NAMES: [(&str, bool); 40] = [
    ("refs/heads/main", true),
    ("refs/heads/feature/x-1", true),
    ("refs/tags/v1.0.0", true),
    ("refs/remotes/origin/main", true),
    ("refs/loams/ws/w01h0000000000000000000000", true),
    ("refs/heads/a.b", true),
    ("refs/heads/a_b-c+d", true),
    ("refs/heads/caf\u{e9}", true),
    ("refs/heads/@", true),
    ("refs/heads/a@b", true),
    ("refs/heads/x}y", true),
    ("refs/heads/UPPER", true),
    ("refs/heads/a/b/c/d/e/f", true),
    ("refs/notes/commits", true),
    ("refs/heads/a.lock.b", true),
    ("refs/heads/-dash", true),
    ("refs/heads/a..b", false),
    ("refs/heads/a.lock", false),
    ("refs/heads/.hidden", false),
    ("refs/heads/a/.b", false),
    ("refs/heads/a.", false),
    ("refs/heads/a/", false),
    ("refs/heads//a", false),
    ("refs/heads/a b", false),
    ("refs/heads/a~1", false),
    ("refs/heads/a^", false),
    ("refs/heads/a:b", false),
    ("refs/heads/a?b", false),
    ("refs/heads/a*b", false),
    ("refs/heads/a[b", false),
    ("refs/heads/a\\b", false),
    ("refs/heads/a@{1}", false),
    ("refs/heads/a\tb", false),
    ("refs/heads/a\u{7f}b", false),
    ("refs/heads/x.lock/y", false),
    ("refs/", false),
    ("HEAD", false),
    ("main", false),
    ("heads/main", false),
    ("", false),
];

fn git_accepts(name: &str) -> Option<bool> {
    let output = Command::new("git")
        .args(["check-ref-format", name])
        .output()
        .ok()?;
    Some(output.status.success())
}

#[test]
fn refname_validation_matches_git_check_ref_format() {
    let git_present = git_accepts("refs/heads/main").is_some();
    for (name, valid) in REF_NAMES {
        assert_eq!(
            RefName::parse(name).is_ok(),
            valid,
            "RefName::parse({name:?})"
        );
        // Under `refs/`, `RefName` and git agree. Outside it git also accepts
        // some names (`heads/main`), which `RefName` refuses by design.
        if git_present && name.starts_with("refs/") {
            assert_eq!(
                git_accepts(name),
                Some(valid),
                "git check-ref-format {name:?}"
            );
        }
    }
    if !git_present {
        eprintln!("git not found: checked the corpus against RefName only");
    }
}

#[test]
fn repo_id_charset() {
    for ok in [
        "r01h0000000000000000000000",
        "a",
        "0",
        "my-repo.v2_x",
        &"a".repeat(100),
    ] {
        assert_eq!(RepoId::parse(ok).unwrap().as_str(), ok);
    }
    for bad in [
        "",
        "-a",
        ".a",
        "_a",
        "A",
        "a/b",
        "a b",
        "ab\u{e9}",
        &"a".repeat(101),
    ] {
        assert!(RepoId::parse(bad).is_err(), "{bad:?}");
    }
    let id = RepoId::generate();
    assert_eq!(id.as_str().len(), 27);
    assert!(id.as_str().starts_with('r'));
    assert_eq!(RepoId::parse(id.as_str()), Ok(id.clone()));
    assert_eq!(id.as_str().parse::<RepoId>(), Ok(id));
}

#[test]
fn ws_id_charset() {
    let ulid = Ulid::from_parts(1_790_762_400_000, 42);
    let ws = WsId::from_ulid(ulid);
    let text = ws.as_str();
    assert_eq!(text.len(), 27);
    assert_eq!(text, format!("w{}", ulid.to_string().to_ascii_lowercase()));
    assert_eq!(WsId::parse(text), Ok(ws.clone()));
    assert_eq!(WsId::parse(WsId::generate().as_str()).map(|_| ()), Ok(()));

    let upper = format!("w{}", ulid);
    let refused = [
        "",
        "w",
        &text[..26],
        &format!("{text}0"),
        &upper,                         // uppercase ULID
        &text.replacen('w', "r", 1),    // another kind's prefix
        "wzzzzzzzzzzzzzzzzzzzzzzzzzz",  // more than 128 bits
        "w01h00000000000000000000000l", // Crockford alias 'l'
        "w01h0000000000000000000000u",  // 'u' is not base32
    ];
    for bad in refused {
        assert!(WsId::parse(bad).is_err(), "{bad:?}");
    }
    // The same rule, with its own letter, for mounts and secrets.
    let mount = MountId::from_ulid(ulid);
    assert!(mount.as_str().starts_with('m'));
    assert_eq!(MountId::parse(mount.as_str()), Ok(mount));
    assert!(MountId::parse(text).is_err());
    let secret = SecretId::from_ulid(ulid);
    assert!(secret.as_str().starts_with('s'));
    assert_eq!(SecretId::parse(secret.as_str()), Ok(secret));
    assert!(SecretId::parse(text).is_err());
}

#[test]
fn repo_name_rejects_dot_git_suffix() {
    assert!(RepoName::parse("loams").is_ok());
    assert!(RepoName::parse("loams.gitx").is_ok());
    assert!(RepoName::parse("git").is_ok());
    let err = RepoName::parse("loams.git").unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid repository name \"loams.git\": must not end in '.git'"
    );
    assert!(RepoName::parse(".git").is_err());
    assert!(RepoName::parse("Loams").is_err());
    assert!(RepoName::parse(&"a".repeat(100)).is_ok());
    assert!(RepoName::parse(&"a".repeat(101)).is_err());
    // Workspace names share the charset, without the suffix rule.
    assert!(WsName::parse("fix.git").is_ok());
    assert!(WsName::parse("Fix").is_err());
}

#[test]
fn other_ids_have_one_spelling() {
    assert!(IdemKey::parse(&"k".repeat(128)).is_ok());
    assert!(IdemKey::parse(&"k".repeat(129)).is_err());
    assert!(IdemKey::parse("").is_err());
    assert!(IdemKey::parse("A-z.0_9:x").is_ok());
    assert!(IdemKey::parse("a/b").is_err());

    assert!(Principal::parse("agent:42@example.com").is_ok());
    assert!(Principal::parse(&"p".repeat(256)).is_ok());
    assert!(Principal::parse(&"p".repeat(257)).is_err());
    for bad in ["", ".", "..", "a/b", "a\\b", "a b", "a\u{e9}"] {
        assert!(Principal::parse(bad).is_err(), "{bad:?}");
    }

    let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    assert!(TraceParent::parse(tp).is_ok());
    assert!(TraceParent::parse(&format!("01{}-extra", &tp[2..])).is_ok());
    for bad in [
        &format!("{tp}-x"),         // version 00 has four fields
        &format!("ff{}", &tp[2..]), // version ff is invalid
        &tp.to_uppercase(),         // lowercase only
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01", // zero trace id
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // zero parent id
        &tp[..54],
        &format!("{}\u{e9}", "0".repeat(54)), // byte 55 inside a character
        &format!("{}\u{e9}", &tp[..53]),
    ] {
        assert!(TraceParent::parse(bad).is_err(), "{bad:?}");
    }

    assert_eq!(RepoPath::parse("").unwrap(), RepoPath::root());
    assert!(RepoPath::parse("src/lib.rs").is_ok());
    for bad in [
        "/a",
        "a/",
        "a//b",
        "a/./b",
        "../a",
        ".git/config",
        "a/.GIT/b",
        "a\0b",
    ] {
        assert!(RepoPath::parse(bad).is_err(), "{bad:?}");
    }
    assert!(RepoPath::parse(&"a".repeat(4096)).is_ok());
    assert!(RepoPath::parse(&"a".repeat(4097)).is_err());
}

#[test]
fn seq_and_snap_n_count_from_zero() {
    assert_eq!(Seq::ZERO.next(), Seq(1));
    assert_eq!(SnapN::ZERO.next(), SnapN(1));
    assert_eq!(Seq(7).to_string(), "00000000000000000007");
    assert_eq!(SnapN(u64::MAX).to_string(), "18446744073709551615");
    assert!("7".parse::<Seq>().is_err());
    assert!("+0000000000000000007".parse::<Seq>().is_err());
    assert!("99999999999999999999".parse::<Seq>().is_err());
}

proptest! {
    #[test]
    fn seq_formats_as_twenty_digits_and_sorts_lexically(a in any::<u64>(), b in any::<u64>()) {
        let (sa, sb) = (Seq(a).to_string(), Seq(b).to_string());
        prop_assert_eq!(sa.len(), 20);
        prop_assert!(sa.bytes().all(|c| c.is_ascii_digit()));
        prop_assert_eq!(sa.cmp(&sb), a.cmp(&b));
        prop_assert_eq!(sa.parse::<Seq>(), Ok(Seq(a)));
        prop_assert_eq!(SnapN(a).to_string(), sa);
    }
}
