//! `WorkspaceStore` (§36 §19.4; plan GT1 Task 23): workspaces as
//! create-only snapshot chains on the bucket.
//!
//! `million_workspaces_listing_pages` creates `LOAMS_GIT_WS_SCALE`
//! workspaces (10,000 by default; the nightly sets 1,000,000).

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;

use loams_git::format::pb;
use loams_git::format::snapshot::decode_snapshot;
use loams_git::testing::ManualClock;
use loams_git::{
    BucketWorkspaceStore, CreateWs, MemWorkspaceStore, MountId, NamespaceId, ObjectId, Principal,
    RefName, RepoId, RepoPaths, Seq, SnapAppended, SnapN, WorkspaceStore, WsBase, WsError, WsId,
    WsName,
};
use loams_store::{Fault, Op, Store};
use support::faulty_store;

const NS: NamespaceId = NamespaceId(7);

fn repo() -> RepoId {
    RepoId::parse("r01j9z3m5q8x0000000000000001").expect("repo id")
}

fn paths() -> RepoPaths {
    RepoPaths::new(NS, &repo())
}

fn oid(byte: u8) -> ObjectId {
    ObjectId::from_bytes_or_panic(&[byte; 20])
}

fn principal(s: &str) -> Principal {
    Principal::parse(s).expect("principal")
}

fn base() -> WsBase {
    WsBase {
        track: RefName::parse("refs/heads/main").expect("ref"),
        commit: Some(oid(0xc1)),
        tree: oid(0x71),
        seq: Seq(42),
        head_ref: Some(RefName::parse("refs/heads/main").expect("ref")),
    }
}

fn spec(owner: &str, name: Option<&str>) -> CreateWs {
    CreateWs {
        repo: repo(),
        ws_id: WsId::generate(),
        owner: principal(owner),
        name: name.map(|n| WsName::parse(n).expect("name")),
        principal: principal(owner),
        writer: None,
        base: Some(base()),
    }
}

fn bucket(store: Store) -> BucketWorkspaceStore {
    BucketWorkspaceStore::new(store, NS).with_clock(Arc::new(ManualClock::new(1_760_000_000_000)))
}

/// Snapshot `n` of `prev`'s workspace, written by `writer`, with a tree
/// that tells writers apart.
fn next(prev: &pb::WorkspaceSnapshot, writer: &MountId, tag: u8) -> pb::WorkspaceSnapshot {
    pb::WorkspaceSnapshot {
        n: prev.n + 1,
        writer: writer.to_string(),
        worktree_tree: vec![tag; 20],
        created_unix_ms: prev.created_unix_ms + 1000,
        ..prev.clone()
    }
}

/// Saves `count` snapshots after `snap` with one writer.
async fn save_chain(
    store: &dyn WorkspaceStore,
    snap: &pb::WorkspaceSnapshot,
    count: u64,
) -> pb::WorkspaceSnapshot {
    let writer = MountId::generate();
    let mut current = snap.clone();
    for i in 0..count {
        let snap = next(&current, &writer, i as u8);
        assert_eq!(
            store.append(&snap).await.expect("append"),
            SnapAppended::Committed { n: SnapN(snap.n) }
        );
        current = snap;
    }
    current
}

#[tokio::test]
async fn create_is_one_put_unnamed_two_named() {
    let (faulty, store) = faulty_store();
    let ws = bucket(store.clone());

    let unnamed = spec("alice", None);
    let snap = ws.create(unnamed.clone()).await.expect("create");
    assert_eq!(faulty.calls(Op::Put), 1, "an unnamed workspace is one PUT");
    assert_eq!(faulty.calls(Op::PutCreate), 1);
    assert_eq!(snap.n, 0);
    assert_eq!(snap.ws_id, unnamed.ws_id.as_str());
    assert_eq!(snap.repo_id, repo().as_str());
    assert_eq!(snap.owner, "alice");
    assert_eq!(snap.name, "");
    assert_eq!(snap.track, "refs/heads/main");
    assert_eq!(snap.base, oid(0xc1).as_bytes());
    assert_eq!(snap.head, snap.base);
    assert_eq!(snap.base_seq, 42);
    assert_eq!(snap.head_ref, "refs/heads/main");
    assert_eq!(snap.index_tree, oid(0x71).as_bytes());
    assert_eq!(snap.worktree_tree, snap.index_tree);
    assert!(snap.packs.is_empty(), "snapshot 0 copies nothing");
    assert_eq!(snap.object_format, pb::ObjectFormat::Sha1 as i32);
    let (commit, _) = loams_git::format::snapshot::snapshot_commit(&snap).expect("commit");
    assert_eq!(snap.snapshot_commit, commit.as_bytes());

    // Stored where §19.4.1 says, and decodes to what create answered.
    let (bytes, _) = store
        .get(&paths().ws_snapshot(&unnamed.ws_id, SnapN::ZERO))
        .await
        .expect("snapshot 0");
    assert_eq!(decode_snapshot(&bytes).expect("decode"), *snap);

    let named = spec("alice", Some("feature-x"));
    ws.create(named.clone()).await.expect("create named");
    assert_eq!(faulty.calls(Op::Put), 3, "a named workspace is two PUTs");
    assert_eq!(faulty.calls(Op::PutCreate), 3);
    let (record, _) = store
        .get(&paths().ws_name(&principal("alice"), &WsName::parse("feature-x").unwrap()))
        .await
        .expect("name record");
    let record: serde_json::Value = serde_json::from_slice(&record).expect("json");
    assert_eq!(record["ws_id"], named.ws_id.as_str());
    assert_eq!(record["created_unix_ms"], 1_760_000_000_000_i64);
}

#[tokio::test]
async fn create_replay_returns_same_ws() {
    let (faulty, store) = faulty_store();
    let ws = bucket(store.clone());
    let spec = spec("alice", Some("replayed"));

    // Both writes land and both answers are lost: the resends are refused
    // as existing and read back.
    faulty.inject(Op::PutCreate, Fault::ErrorAfterApply);
    faulty.inject(Op::PutCreate, Fault::ErrorAfterApply);
    let first = ws.create(spec.clone()).await.expect("create");
    assert_eq!(first.ws_id, spec.ws_id.as_str());

    // A caller's own retry, later (another clock reading), answers the
    // stored snapshot 0.
    let later = BucketWorkspaceStore::new(store, NS)
        .with_clock(Arc::new(ManualClock::new(1_770_000_000_000)));
    let again = later.create(spec.clone()).await.expect("replay");
    assert_eq!(again, first);
    assert_eq!(
        ws.latest(&repo(), &spec.ws_id).await.expect("latest"),
        first
    );

    // The same id for another workspace is a conflict, not a replay.
    let mut other = spec.clone();
    other.name = None;
    other.owner = principal("bob");
    assert!(matches!(
        ws.create(other).await,
        Err(WsError::IdConflict { .. })
    ));
    // A conflicting spec with a name claims no name.
    let mut named = spec.clone();
    named.name = Some(WsName::parse("never-claimed").unwrap());
    assert!(matches!(
        ws.create(named.clone()).await,
        Err(WsError::IdConflict { .. })
    ));
    assert!(
        ws.list(&repo(), Some(&principal("alice")), None, 10)
            .await
            .expect("list")
            .iter()
            .all(|s| s.name.as_ref().map(WsName::as_str) != Some("never-claimed"))
    );

    // The in-memory store answers the same way.
    let mem = MemWorkspaceStore::new();
    let first = mem.create(spec.clone()).await.expect("create");
    assert_eq!(mem.create(spec.clone()).await.expect("replay"), first);
    assert!(matches!(
        mem.create(named).await,
        Err(WsError::IdConflict { .. })
    ));
    assert!(
        mem.list(&repo(), Some(&principal("alice")), None, 10)
            .await
            .expect("list")
            .iter()
            .all(|s| s.name.as_ref().map(WsName::as_str) != Some("never-claimed"))
    );
}

#[tokio::test]
async fn name_taken_is_already_exists() {
    for store in stores() {
        let first = spec("alice", Some("taken"));
        store.create(first.clone()).await.expect("create");
        let second = spec("alice", Some("taken"));
        match store.create(second.clone()).await {
            Err(WsError::WorkspaceNameTaken { name, holder }) => {
                assert_eq!(name.as_str(), "taken");
                assert_eq!(holder, first.ws_id);
            }
            other => panic!("expected WorkspaceNameTaken, got {other:?}"),
        }
        // The loser wrote no snapshot.
        assert!(matches!(
            store.latest(&repo(), &second.ws_id).await,
            Err(WsError::NotFound)
        ));
        // Names are per owner.
        store
            .create(spec("bob", Some("taken")))
            .await
            .expect("another owner's name");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_append_is_exclusive() {
    let store = Store::in_memory();
    let ws = bucket(store.clone());
    let zero = ws.create(spec("alice", None)).await.expect("create");
    let racers = (0..32u8).map(|tag| {
        let ws = bucket(store.clone());
        let snap = next(&zero, &MountId::generate(), tag);
        tokio::spawn(async move { (snap.clone(), ws.append(&snap).await) })
    });
    let mut committed = Vec::new();
    let mut fenced = Vec::new();
    for racer in racers {
        let (snap, appended) = racer.await.expect("join");
        match appended.expect("append") {
            SnapAppended::Committed { n } => {
                assert_eq!(n, SnapN(1));
                committed.push(snap);
            }
            SnapAppended::Fenced { existing } => fenced.push(existing),
        }
    }
    assert_eq!(committed.len(), 1, "exactly one writer saves snapshot 1");
    assert_eq!(fenced.len(), 31);
    for existing in &fenced {
        assert_eq!(**existing, committed[0], "every loser is told the winner");
    }
    let latest = ws
        .latest(&repo(), &WsId::parse(&zero.ws_id).unwrap())
        .await
        .expect("latest");
    assert_eq!(*latest, committed[0]);

    // The in-memory store has the same rule.
    let mem = Arc::new(MemWorkspaceStore::new());
    let zero = mem.create(spec("alice", None)).await.expect("create");
    let racers = (0..32u8).map(|tag| {
        let mem = mem.clone();
        let snap = next(&zero, &MountId::generate(), tag);
        tokio::spawn(async move { mem.append(&snap).await })
    });
    let mut wins = 0;
    for racer in racers {
        if let SnapAppended::Committed { .. } = racer.await.expect("join").expect("append") {
            wins += 1;
        }
    }
    assert_eq!(wins, 1);
}

#[tokio::test]
async fn lost_ack_save_is_committed() {
    let (faulty, store) = faulty_store();
    let ws = bucket(store);
    let zero = ws.create(spec("alice", None)).await.expect("create");
    let writer = MountId::generate();

    // Applied, and the answer lost: the resend is refused and the
    // read-back finds this writer's bytes.
    let one = next(&zero, &writer, 1);
    faulty.inject(Op::PutCreate, Fault::ErrorAfterApply);
    assert_eq!(
        ws.append(&one).await.expect("append"),
        SnapAppended::Committed { n: SnapN(1) }
    );

    // Applied, and reported as existing (a lost ack seen by a retry).
    let two = next(&one, &writer, 2);
    faulty.inject(Op::PutCreate, Fault::Precondition);
    assert_eq!(
        ws.append(&two).await.expect("append"),
        SnapAppended::Committed { n: SnapN(2) }
    );

    // Failed before it was applied: resent, and saved.
    let three = next(&two, &writer, 3);
    faulty.inject(Op::PutCreate, Fault::Error);
    assert_eq!(
        ws.append(&three).await.expect("append"),
        SnapAppended::Committed { n: SnapN(3) }
    );

    // A caller's own retry of a saved snapshot is committed too.
    assert_eq!(
        ws.append(&three).await.expect("append"),
        SnapAppended::Committed { n: SnapN(3) }
    );
    let latest = ws
        .latest(&repo(), &WsId::parse(&zero.ws_id).unwrap())
        .await
        .expect("latest");
    assert_eq!(*latest, three);
}

#[tokio::test]
async fn fenced_append_returns_existing() {
    for store in stores() {
        let zero = store.create(spec("alice", None)).await.expect("create");
        let a = next(&zero, &MountId::generate(), 0xa);
        let b = next(&zero, &MountId::generate(), 0xb);
        assert_eq!(
            store.append(&a).await.expect("a"),
            SnapAppended::Committed { n: SnapN(1) }
        );
        match store.append(&b).await.expect("b") {
            SnapAppended::Fenced { existing } => assert_eq!(*existing, a),
            other => panic!("expected Fenced, got {other:?}"),
        }
        // The same writer offering different bytes at a taken `n` is
        // fenced too: only the same bytes are its own.
        let a_changed = pb::WorkspaceSnapshot {
            label: "changed".into(),
            ..a.clone()
        };
        assert!(matches!(
            store.append(&a_changed).await.expect("a changed"),
            SnapAppended::Fenced { .. }
        ));
        // Snapshot 0 is never appended.
        assert!(matches!(
            store.append(&zero).await,
            Err(WsError::Invalid { .. })
        ));
    }
}

#[tokio::test]
async fn latest_uses_hint_then_probe() {
    let (faulty, store) = faulty_store();
    let ws = bucket(store);
    let zero = ws.create(spec("alice", None)).await.expect("create");
    let id = WsId::parse(&zero.ws_id).unwrap();
    let five = save_chain(&ws, &zero, 5).await;
    ws.publish_hint(&repo(), &id, SnapN(3)).await.expect("hint");

    let lists = faulty.calls(Op::List);
    let gets = faulty.calls(Op::Get);
    let latest = ws.latest(&repo(), &id).await.expect("latest");
    assert_eq!(*latest, five, "probed past the hint to the tail");
    assert_eq!(faulty.calls(Op::List), lists, "a usable hint needs no LIST");
    // The hint, snapshots 3, 4 and 5, and the miss at 6.
    assert_eq!(faulty.calls(Op::Get) - gets, 5);

    // A hint naming a missing snapshot (collected, or garbage) falls back
    // to listing.
    ws.publish_hint(&repo(), &id, SnapN(9)).await.expect("hint");
    assert_eq!(*ws.latest(&repo(), &id).await.expect("latest"), five);
    assert!(faulty.calls(Op::List) > lists);
}

#[tokio::test]
async fn latest_without_hint_lists() {
    let (faulty, store) = faulty_store();
    let ws = bucket(store.clone());
    let zero = ws.create(spec("alice", None)).await.expect("create");
    let id = WsId::parse(&zero.ws_id).unwrap();
    let last = save_chain(&ws, &zero, 1200).await;

    let lists = faulty.calls(Op::List);
    let gets = faulty.calls(Op::Get);
    let latest = ws.latest(&repo(), &id).await.expect("latest");
    assert_eq!(*latest, last);
    assert_eq!(
        faulty.calls(Op::List) - lists,
        2,
        "1,201 names take two pages"
    );
    // The missing hint, the listed tail, and the miss after it.
    assert_eq!(faulty.calls(Op::Get) - gets, 3);

    // An unreadable hint is no hint.
    store
        .put(&paths().ws_head(&id), "not json".into())
        .await
        .expect("garbage hint");
    assert_eq!(*ws.latest(&repo(), &id).await.expect("latest"), last);

    // A workspace with no snapshot is not found.
    assert!(matches!(
        ws.latest(&repo(), &WsId::generate()).await,
        Err(WsError::NotFound)
    ));
}

#[tokio::test]
async fn fork_is_one_put_and_shares_packs() {
    let (faulty, store) = faulty_store();
    let ws = bucket(store);
    let zero = ws.create(spec("alice", None)).await.expect("create");
    let source = WsId::parse(&zero.ws_id).unwrap();
    let mut two = save_chain(&ws, &zero, 1).await;
    two.n = 2;
    two.packs = vec![pb::PackRef {
        checksum: oid(0x9a).to_hex().to_string(),
        object_count: 3,
        ..Default::default()
    }];
    two.refs = vec![pb::LocalRef {
        name: "refs/heads/topic".into(),
        oid: vec![0x22; 20],
        upstream: String::new(),
    }];
    two.label = "before-refactor".into();
    ws.append(&two).await.expect("append");

    let puts = faulty.calls(Op::Put);
    let fork_spec = CreateWs {
        base: None,
        ..spec("bob", None)
    };
    let fork = ws
        .fork((&repo(), &source, SnapN(2)), fork_spec.clone())
        .await
        .expect("fork");
    assert_eq!(
        faulty.calls(Op::Put) - puts,
        1,
        "an unnamed fork is one PUT"
    );
    assert_eq!(faulty.calls(Op::Copy), 0, "no pack is copied");
    assert_eq!(fork.n, 0);
    assert_eq!(fork.ws_id, fork_spec.ws_id.as_str());
    assert_eq!(fork.owner, "bob");
    assert_eq!(fork.packs, two.packs, "the source's packs, by reference");
    assert_eq!(fork.refs, two.refs);
    assert_eq!(fork.worktree_tree, two.worktree_tree);
    assert_eq!(fork.head, two.head);
    assert_eq!(fork.track, two.track);
    assert_eq!(fork.label, "", "labels are not inherited");
    assert_eq!(
        fork.fork,
        Some(pb::ForkOrigin {
            repo_id: repo().to_string(),
            ws_id: source.to_string(),
            n: 2,
        })
    );
    let (commit, _) = loams_git::format::snapshot::snapshot_commit(&fork).expect("commit");
    assert_eq!(fork.snapshot_commit, commit.as_bytes());

    // A replay answers the same fork with no write.
    let puts = faulty.calls(Op::Put);
    let again = ws
        .fork((&repo(), &source, SnapN(2)), fork_spec.clone())
        .await
        .expect("replay");
    assert_eq!(again, fork);
    assert_eq!(faulty.calls(Op::Put), puts);

    // A named fork is two PUTs; a fork of a missing snapshot is not found;
    // a fork takes no base.
    let named = CreateWs {
        base: None,
        ..spec("bob", Some("fork-of-two"))
    };
    let puts = faulty.calls(Op::Put);
    ws.fork((&repo(), &source, SnapN(2)), named)
        .await
        .expect("named fork");
    assert_eq!(faulty.calls(Op::Put) - puts, 2);
    assert!(matches!(
        ws.fork(
            (&repo(), &source, SnapN(7)),
            CreateWs {
                base: None,
                ..spec("bob", None)
            }
        )
        .await,
        Err(WsError::NotFound)
    ));
    assert!(matches!(
        ws.fork((&repo(), &source, SnapN(2)), spec("bob", None))
            .await,
        Err(WsError::Invalid { .. })
    ));
}

#[tokio::test]
async fn salvage_is_stored_under_the_mount() {
    for store in stores() {
        let zero = store.create(spec("alice", None)).await.expect("create");
        let mount = MountId::generate();
        let unsaved = next(&zero, &mount, 7);
        let id = store.salvage(&unsaved, &mount).await.expect("salvage");
        let other = store.salvage(&unsaved, &mount).await.expect("salvage");
        assert_ne!(id, other, "each salvage has its own id");
    }
    let raw = Store::in_memory();
    let ws = bucket(raw.clone());
    let zero = ws.create(spec("alice", None)).await.expect("create");
    let mount = MountId::generate();
    let unsaved = next(&zero, &mount, 7);
    let id = ws.salvage(&unsaved, &mount).await.expect("salvage");
    let ws_id = WsId::parse(&zero.ws_id).unwrap();
    let (bytes, _) = raw
        .get(&paths().ws_salvage(&ws_id, &mount, id))
        .await
        .expect("salvage object");
    assert_eq!(decode_snapshot(&bytes).expect("decode"), unsaved);
}

#[tokio::test]
async fn listing_pages_skip_each_workspaces_objects() {
    let store = Store::in_memory();
    let ws = bucket(store);
    let mut ids = BTreeSet::new();
    for i in 0..1100 {
        let zero = ws.create(spec("alice", None)).await.expect("create");
        // Some workspaces hold more objects than a LIST page.
        if i % 500 == 0 {
            save_chain(&ws, &zero, 1001).await;
        }
        ids.insert(zero.ws_id.clone());
    }
    let listed = list_everything(&ws, None, 333).await;
    assert_eq!(listed, ids.into_iter().collect::<Vec<_>>());
}

/// Every workspace (or `owner`'s named ones) in pages of `page`, checking
/// that pages are full until the last.
async fn list_everything(
    store: &dyn WorkspaceStore,
    owner: Option<&Principal>,
    page: usize,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let got = store
            .list(&repo(), owner, after.as_deref(), page)
            .await
            .expect("list");
        assert!(got.len() <= page);
        out.extend(got.iter().map(|s| s.ws_id.to_string()));
        if got.len() < page {
            return out;
        }
        after = Some(got.last().expect("a full page").cursor().to_string());
    }
}

#[tokio::test]
async fn million_workspaces_listing_pages() {
    let scale: usize = std::env::var("LOAMS_GIT_WS_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    let store = MemWorkspaceStore::new();
    let alice = principal("alice");
    let mut named = Vec::new();
    let mut all = BTreeSet::new();
    for i in 0..scale {
        // One in a hundred is named; the rest are agents' unnamed ones.
        let name = (i % 100 == 0).then(|| format!("ws-{i:08}"));
        let snap = store
            .create(spec("alice", name.as_deref()))
            .await
            .expect("create");
        if name.is_some() {
            named.push(snap.ws_id.clone());
        }
        all.insert(snap.ws_id.clone());
    }
    let listed = list_everything(&store, None, 1000).await;
    assert_eq!(listed.len(), scale);
    assert_eq!(listed, all.into_iter().collect::<Vec<_>>());
    // Names `ws-<8 digits>` sort as they were made.
    let listed = list_everything(&store, Some(&alice), 1000).await;
    assert_eq!(listed, named);
}

#[tokio::test]
async fn named_listing_pages_through_name_records() {
    for store in stores() {
        let alice = principal("alice");
        let mut named = Vec::new();
        for i in 0..25 {
            let snap = store
                .create(spec("alice", Some(&format!("n{i:02}"))))
                .await
                .expect("create");
            named.push(snap.ws_id.clone());
            store.create(spec("alice", None)).await.expect("unnamed");
        }
        store.create(spec("bob", Some("n00"))).await.expect("bob");
        let listed = list_everything(store.as_ref(), Some(&alice), 4).await;
        assert_eq!(listed, named);
        let first = store
            .list(&repo(), Some(&alice), None, 1)
            .await
            .expect("list");
        assert_eq!(first[0].name.as_ref().map(WsName::as_str), Some("n00"));
        assert_eq!(first[0].created_unix_ms, Some(1_760_000_000_000));
        assert!(
            store
                .list(&repo(), Some(&alice), None, 0)
                .await
                .expect("list")
                .is_empty()
        );
    }
}

/// Both implementations, for the tests whose rules they share.
fn stores() -> Vec<Box<dyn WorkspaceStore>> {
    vec![
        Box::new(bucket(Store::in_memory())),
        Box::new(
            MemWorkspaceStore::new().with_clock(Arc::new(ManualClock::new(1_760_000_000_000))),
        ),
    ]
}

/// `ListSnapshots` and `ListSalvage` read these (Task 25): snapshot numbers
/// and salvage records in pages, on both stores.
#[tokio::test]
async fn snapshots_and_salvage_are_listed() {
    for store in stores() {
        let zero = store.create(spec("alice", None)).await.expect("create");
        let ws = WsId::parse(&zero.ws_id).expect("ws id");
        let last = save_chain(store.as_ref(), &zero, 4).await;
        let first = store
            .list_snapshots(&repo(), &ws, None, 2)
            .await
            .expect("list");
        assert_eq!(first, vec![SnapN(0), SnapN(1)]);
        let rest = store
            .list_snapshots(&repo(), &ws, Some(SnapN(1)), 10)
            .await
            .expect("list");
        assert_eq!(rest, vec![SnapN(2), SnapN(3), SnapN(4)]);
        assert!(
            store
                .list_snapshots(&repo(), &WsId::generate(), None, 10)
                .await
                .expect("list")
                .is_empty()
        );

        let (a, b) = (MountId::generate(), MountId::generate());
        let mut held = Vec::new();
        for (mount, tag) in [(&a, 1), (&b, 2), (&a, 3)] {
            let unsaved = next(&last, mount, tag);
            let id = store.salvage(&unsaved, mount).await.expect("salvage");
            held.push((
                loams_git::ws::SalvageRef {
                    mount: mount.clone(),
                    id,
                },
                unsaved,
            ));
        }
        held.sort_by(|x, y| x.0.cmp(&y.0));
        let page = store
            .list_salvage(&repo(), &ws, None, 2)
            .await
            .expect("list");
        let more = store
            .list_salvage(&repo(), &ws, page.last(), 2)
            .await
            .expect("list");
        let listed: Vec<_> = page.into_iter().chain(more).collect();
        let want: Vec<_> = held.iter().map(|(r, _)| r.clone()).collect();
        assert_eq!(listed, want);
        for (salvage, unsaved) in &held {
            let got = store
                .get_salvage(&repo(), &ws, salvage)
                .await
                .expect("get")
                .expect("held");
            assert_eq!(*got, *unsaved);
        }
        let absent = loams_git::ws::SalvageRef {
            mount: a.clone(),
            id: ulid::Ulid::generate(),
        };
        assert!(
            store
                .get_salvage(&repo(), &ws, &absent)
                .await
                .expect("get")
                .is_none()
        );
    }
}

/// The server's `WsLookup` (Task 25, ruling T24.6): a workspace's owner,
/// latest packs and fork sources; a deleted or unknown one is not found.
#[tokio::test]
async fn store_lookup_follows_forks_and_hides_deleted() {
    use loams_git::objects::WsLookup;
    use loams_git::ws::{DELETED_LABEL, StoreWsLookup};

    let raw = Store::in_memory();
    let ws = bucket(raw.clone());
    let lookup = StoreWsLookup::new(raw);
    let zero = ws.create(spec("alice", None)).await.expect("create");
    let source = WsId::parse(&zero.ws_id).expect("ws id");
    let pack = pb::PackRef {
        checksum: "ab".repeat(20),
        object_count: 1,
        pack_len: 32,
        idx_offset: 32,
        idx_len: 1072,
        has_bitmap: false,
    };
    let saved = pb::WorkspaceSnapshot {
        packs: vec![pack.clone()],
        ..next(&zero, &MountId::generate(), 1)
    };
    ws.append(&saved).await.expect("append");
    let view = lookup
        .workspace(NS, &repo(), &source)
        .await
        .expect("lookup")
        .expect("found");
    assert_eq!(view.owner, principal("alice"));
    assert_eq!(view.packs, vec![pack.clone()]);
    assert!(view.sources.is_empty());

    let fork = ws
        .fork(
            (&repo(), &source, SnapN(1)),
            CreateWs {
                base: None,
                ..spec("bob", None)
            },
        )
        .await
        .expect("fork");
    let forked = WsId::parse(&fork.ws_id).expect("ws id");
    let view = lookup
        .workspace(NS, &repo(), &forked)
        .await
        .expect("lookup")
        .expect("found");
    assert_eq!(view.owner, principal("bob"));
    assert_eq!(view.packs, vec![pack]);
    assert_eq!(view.sources, vec![source.clone()]);

    let deleted = pb::WorkspaceSnapshot {
        label: DELETED_LABEL.to_owned(),
        ..next(&saved, &MountId::generate(), 2)
    };
    ws.append(&deleted).await.expect("append");
    assert!(
        lookup
            .workspace(NS, &repo(), &source)
            .await
            .expect("lookup")
            .is_none()
    );
    assert!(
        lookup
            .workspace(NS, &repo(), &WsId::generate())
            .await
            .expect("lookup")
            .is_none()
    );
}
