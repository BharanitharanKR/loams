//! Workspace snapshots (§36 §19.4.2; plan GT1 Task 22).

use std::process::{Command, Stdio};

use loams_git::format::FormatError;
use loams_git::format::pb;
use loams_git::format::snapshot::{
    SNAPSHOT_END, SNAPSHOT_MAGIC, decode_snapshot, encode_snapshot, set_changed_hint,
    snapshot_commit,
};
use loams_git::limits::{MAX_CHANGED_HINT, MAX_SNAPSHOT_BYTES};

use crate::fixtures::{golden, oid, pack_ref};

/// A snapshot with every field set.
pub fn golden_snapshot() -> pb::WorkspaceSnapshot {
    pb::WorkspaceSnapshot {
        ws_id: "w01j9z3m5q8x0000000000000001".into(),
        n: 7,
        object_format: pb::ObjectFormat::Sha1 as i32,
        repo_id: "r01j9z3m5q8x0000000000000000".into(),
        owner: "alice@example.com".into(),
        name: "fix-login".into(),
        track: "refs/heads/main".into(),
        base: oid(1),
        base_seq: 42,
        head: oid(2),
        head_ref: "refs/heads/fix-login".into(),
        refs: vec![pb::LocalRef {
            name: "refs/heads/fix-login".into(),
            oid: oid(2),
            upstream: "refs/heads/main".into(),
        }],
        index_tree: oid(3),
        worktree_tree: oid(4),
        snapshot_commit: oid(5),
        changed: vec![
            pb::ChangedPath {
                path: "src/login.rs".into(),
                kind: pb::changed_path::Kind::Modified as i32,
                staged: true,
            },
            pb::ChangedPath {
                path: "notes.txt".into(),
                kind: pb::changed_path::Kind::Untracked as i32,
                staged: false,
            },
        ],
        changed_overflow: false,
        packs: vec![pack_ref(0xab)],
        writer: "m01j9z3m5q8x0000000000000002".into(),
        principal: "alice@example.com".into(),
        created_unix_ms: 1_760_000_123_456,
        label: "before-refactor".into(),
        takeover: Some(pb::Takeover {
            from_mount: "m01j9z3m5q8x0000000000000003".into(),
            from_n: 6,
            reason: "idle".into(),
        }),
        fork: Some(pb::ForkOrigin {
            repo_id: "r01j9z3m5q8x0000000000000000".into(),
            ws_id: "w01j9z3m5q8x0000000000000009".into(),
            n: 3,
        }),
        conflicts: vec![pb::Conflict {
            path: "src/lib.rs".into(),
            base: oid(6),
            ours: oid(7),
            theirs: oid(8),
            mode: 0o100644,
        }],
    }
}

#[test]
fn snapshot_round_trip() {
    let snap = golden_snapshot();
    let bytes = encode_snapshot(&snap).expect("encode");
    assert_eq!(decode_snapshot(&bytes), Ok(snap.clone()));
    assert_eq!(
        encode_snapshot(&snap).expect("again"),
        bytes,
        "deterministic"
    );
    // Snapshot 0 of a new workspace.
    let zero = pb::WorkspaceSnapshot {
        ws_id: "w01j9z3m5q8x0000000000000001".into(),
        object_format: pb::ObjectFormat::Sha1 as i32,
        ..Default::default()
    };
    let bytes = encode_snapshot(&zero).expect("encode");
    assert_eq!(decode_snapshot(&bytes), Ok(zero));
}

/// `snap` with a label that makes its body `MAX_SNAPSHOT_BYTES + extra`.
fn sized(mut snap: pb::WorkspaceSnapshot, extra: u64) -> pb::WorkspaceSnapshot {
    const FRAME: u64 = 30;
    snap.label = String::new();
    let without = encode_snapshot(&snap).expect("encode").len() as u64 - FRAME;
    // The label (field 22) takes a two-byte tag and, at this size, a
    // three-byte length.
    snap.label = "x".repeat((MAX_SNAPSHOT_BYTES - without - 5 + extra) as usize);
    snap
}

#[test]
fn snapshot_over_one_mib_is_too_large() {
    let at = encode_snapshot(&sized(golden_snapshot(), 0)).expect("at the limit");
    assert_eq!(at.len() as u64 - 30, MAX_SNAPSHOT_BYTES);
    assert_eq!(decode_snapshot(&at).map(|s| s.n), Ok(7));
    assert!(matches!(
        encode_snapshot(&sized(golden_snapshot(), 1)),
        Err(FormatError::TooLarge { max, .. }) if max == MAX_SNAPSHOT_BYTES
    ));
}

#[test]
fn corrupt_snapshot_crc_is_error() {
    let bytes = encode_snapshot(&golden_snapshot())
        .expect("encode")
        .to_vec();
    for at in [0, 12, 20, bytes.len() - 10, bytes.len() - 1] {
        let mut bad = bytes.clone();
        bad[at] ^= 0x01;
        assert!(
            matches!(decode_snapshot(&bad), Err(FormatError::Corrupt { .. })),
            "byte {at}"
        );
    }
    for len in [0, 17, 29, bytes.len() - 1] {
        assert!(matches!(
            decode_snapshot(&bytes[..len]),
            Err(FormatError::Corrupt { .. })
        ));
    }
    let mut v2 = bytes;
    v2[8..10].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        decode_snapshot(&v2),
        Err(FormatError::UnknownVersion { found: 2 })
    );
}

#[test]
fn golden_snapshot_v1() {
    let fresh = encode_snapshot(&golden_snapshot())
        .expect("encode")
        .to_vec();
    let golden = golden("snapshot_v1.lws", &fresh);
    assert_eq!(&golden[..8], SNAPSHOT_MAGIC);
    assert_eq!(&golden[golden.len() - 8..], SNAPSHOT_END);
    assert_eq!(decode_snapshot(&golden), Ok(golden_snapshot()));
    assert_eq!(
        fresh, golden,
        "encoding the golden snapshot reproduces the file byte for byte"
    );
}

/// `git hash-object -t commit --stdin` of `body`, or `None` without git.
fn git_hash_commit(body: &[u8]) -> Option<String> {
    use std::io::Write as _;
    let dir = tempfile::tempdir().expect("dir");
    let mut child = Command::new("git")
        .args(["hash-object", "-t", "commit", "--stdin"])
        .current_dir(dir.path())
        .env("GIT_DIR", dir.path().join("none"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(body)
        .expect("write");
    let out = child.wait_with_output().expect("git");
    assert!(out.status.success());
    Some(
        String::from_utf8(out.stdout)
            .expect("utf-8")
            .trim()
            .to_owned(),
    )
}

#[test]
fn snapshot_commit_is_deterministic() {
    let snap = golden_snapshot();
    // Two hosts: the same state, written at another time of the same second
    // and with other host-local fields, gives the same commit.
    let mut other = snap.clone();
    other.created_unix_ms += 400;
    other.writer = "m01j9z3m5q8x0000000000000077".into();
    other.packs.clear();
    let (a, body) = snapshot_commit(&snap).expect("commit");
    let (b, _) = snapshot_commit(&other).expect("commit");
    assert_eq!(a, b);
    let text = String::from_utf8(body.to_vec()).expect("utf-8");
    assert_eq!(
        text,
        format!(
            "tree {}\nparent {}\nauthor Loams Snapshot <snapshot@loams.invalid> 1760000123 +0000\n\
             committer Loams Snapshot <snapshot@loams.invalid> 1760000123 +0000\n\n\
             loams snapshot w01j9z3m5q8x0000000000000001@7\n",
            "04".repeat(20),
            "02".repeat(20)
        )
    );
    if let Some(theirs) = git_hash_commit(&body) {
        assert_eq!(a.to_string(), theirs, "git hashes it the same");
    }
    // An unborn workspace has no parent; another n is another commit.
    let mut unborn = snap.clone();
    unborn.head.clear();
    let (c, body) = snapshot_commit(&unborn).expect("commit");
    assert!(!String::from_utf8_lossy(&body).contains("parent"));
    if let Some(theirs) = git_hash_commit(&body) {
        assert_eq!(c.to_string(), theirs);
    }
    let mut later = snap;
    later.n = 8;
    assert_ne!(snapshot_commit(&later).expect("commit").0, a);
}

#[test]
fn changed_hint_overflow_sets_flag() {
    let path = |i: usize| pb::ChangedPath {
        path: format!("f{i}"),
        kind: pb::changed_path::Kind::Added as i32,
        staged: false,
    };
    let mut snap = golden_snapshot();
    set_changed_hint(&mut snap, (0..MAX_CHANGED_HINT).map(path).collect());
    assert_eq!(snap.changed.len(), MAX_CHANGED_HINT);
    assert!(!snap.changed_overflow);
    set_changed_hint(&mut snap, (0..=MAX_CHANGED_HINT).map(path).collect());
    assert!(snap.changed.is_empty());
    assert!(snap.changed_overflow);
    // Back under the cap, the hint returns.
    set_changed_hint(&mut snap, vec![path(1)]);
    assert_eq!(snap.changed.len(), 1);
    assert!(!snap.changed_overflow);
}
