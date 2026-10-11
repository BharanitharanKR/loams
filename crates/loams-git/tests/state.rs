//! The repository state machine and idempotency (§36 §4.4, §4.5; plan GT1
//! Task 8).

use std::collections::{BTreeMap, HashMap};

use loams_git::format::event::{TxnBody, TxnEvent, to_cloudevent};
use loams_git::format::pb;
use loams_git::format::segment::{WalBatch, WalSegment};
use loams_git::limits::IDEMPOTENCY_WINDOW;
use loams_git::state::pattern_matches;
use loams_git::{
    Check, Expect, IdemKey, NamespaceId, ObjectId, Principal, Receipt, RefName, RefTxn, RefUpdate,
    RejectReason, RepoId, RepoState, Seq, StateError,
};
use proptest::prelude::*;

const SHA1: pb::ObjectFormat = pb::ObjectFormat::Sha1;
const T0: i64 = 1_790_762_400_000;

fn window() -> i64 {
    IDEMPOTENCY_WINDOW.as_millis() as i64
}

fn oid(byte: u8) -> ObjectId {
    ObjectId::try_from(&[byte; 20][..]).expect("oid")
}

fn name(short: &str) -> RefName {
    RefName::parse(&format!("refs/heads/{short}")).expect("ref name")
}

fn update(short: &str, expect: Expect, new: Option<u8>) -> RefUpdate {
    RefUpdate {
        name: name(short),
        expect,
        new: new.map(oid),
    }
}

fn txn(key: &str, digest: u8, updates: Vec<RefUpdate>) -> RefTxn {
    RefTxn {
        key: IdemKey::parse(key).expect("key"),
        request_digest: [digest; 32],
        updates,
        packs: Vec::new(),
        principal: Principal::parse("alice").expect("principal"),
        scope: None,
        trace: None,
        push_options: Vec::new(),
    }
}

/// Applies an accepted transaction as the only entry of the next segment.
fn commit(state: &mut RepoState, txn: &RefTxn, now: i64) -> Seq {
    let seq = state.seq.next();
    state
        .apply_txn(&txn.to_pb(SHA1), &txn.key, txn.request_digest, seq, 0, now)
        .expect("apply");
    state.seq = seq;
    seq
}

fn event(key: &str, body: TxnBody, now: i64) -> TxnEvent {
    TxnEvent {
        ns: NamespaceId(7),
        repo: RepoId::parse("r01j9z3m5q8x0000000000000000").expect("repo"),
        tenant: "acme/prod".into(),
        key: IdemKey::parse(key).expect("key"),
        trace: None,
        time_unix_ms: now,
        body,
    }
}

fn segment(seq: Seq, events: &[TxnEvent]) -> WalSegment {
    WalSegment {
        seq,
        batch: WalBatch {
            batch_id: [seq.0 as u8; 16],
            events: events.iter().map(|e| to_cloudevent(e, seq)).collect(),
        },
        created_unix_ms: T0,
    }
}

fn ref_event(txn: &RefTxn, now: i64) -> TxnEvent {
    event(txn.key.as_str(), TxnBody::RefTxn(txn.to_pb(SHA1)), now)
}

/// The reference model: three refs, CAS semantics, one protected ref
/// (`c`: no delete, no force) and an idempotency window.
#[derive(Default)]
struct Model {
    refs: HashMap<u8, u8>,
    idem: HashMap<u8, (u8, u64, i64)>,
    seq: u64,
}

#[derive(Clone, Debug)]
struct Step {
    key: u8,
    digest: u8,
    updates: Vec<(u8, u8, u8)>, // (name, expect: 0..4 = oid, 4 = absent, 5 = any; new: 0 = delete)
    advance_ms: i64,
}

const NAMES: [&str; 3] = ["a", "b", "c"];

impl Model {
    fn decide(&self, step: &Step, now: i64) -> Check {
        if let Some(&(digest, seq, expires)) = self.idem.get(&step.key)
            && now < expires
        {
            return if digest == step.digest {
                Check::Replay(Receipt {
                    seq: Seq(seq),
                    index: 0,
                    replayed: true,
                })
            } else {
                Check::Mismatch
            };
        }
        let mut rejected = Vec::new();
        for (i, &(n, expect, new)) in step.updates.iter().enumerate() {
            let dups = step.updates.iter().filter(|u| u.0 == n).count();
            if dups > 1 {
                if step.updates[..i].iter().all(|u| u.0 != n) {
                    rejected.push((name(NAMES[n as usize]), RejectReason::InvalidName));
                }
                continue;
            }
            let current = self.refs.get(&n).copied();
            let reason = match expect {
                0..4 if current != Some(expect) => Some(RejectReason::Stale {
                    current: current.map(oid),
                }),
                4 if current.is_some() => Some(RejectReason::Exists),
                _ if n == 2 && (new == 0 || expect == 5) => Some(RejectReason::Protected),
                _ => None,
            };
            if let Some(reason) = reason {
                rejected.push((name(NAMES[n as usize]), reason));
            }
        }
        if rejected.is_empty() {
            Check::Accept
        } else {
            Check::Reject(rejected)
        }
    }

    fn apply(&mut self, step: &Step, now: i64) {
        self.seq += 1;
        for &(n, _, new) in &step.updates {
            if new == 0 {
                self.refs.remove(&n);
            } else {
                self.refs.insert(n, new);
            }
        }
        self.idem
            .insert(step.key, (step.digest, self.seq, now + window()));
    }
}

fn step_txn(step: &Step) -> RefTxn {
    let updates = step
        .updates
        .iter()
        .map(|&(n, expect, new)| {
            let expect = match expect {
                0..4 => Expect::Oid(oid(expect)),
                4 => Expect::Absent,
                _ => Expect::Any,
            };
            update(NAMES[n as usize], expect, (new != 0).then_some(new))
        })
        .collect();
    txn(&format!("key-{}", step.key), step.digest, updates)
}

fn protected_c() -> Vec<pb::Protection> {
    vec![pb::Protection {
        pattern: "refs/heads/c".into(),
        deny_delete: true,
        deny_force: true,
    }]
}

fn step_strategy() -> impl Strategy<Value = Step> {
    (
        0..5u8,
        0..2u8,
        prop::collection::vec((0..3u8, 0..6u8, 0..4u8), 1..4),
        0..=1_200_000i64,
    )
        .prop_map(|(key, digest, updates, advance_ms)| Step {
            key,
            digest,
            updates,
            advance_ms,
        })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// Random transactions through `check` and `apply_txn` agree with the
    /// reference model, and replaying the committed segments gives the same
    /// state as applying the transactions live.
    #[test]
    fn model_matches_reference(steps in prop::collection::vec(step_strategy(), 1..40)) {
        let mut model = Model::default();
        let mut state = RepoState::empty(SHA1);
        state.protections = protected_c();
        let mut replayed = state.clone();
        let mut now = T0;
        for step in &steps {
            now += step.advance_ms;
            let txn = step_txn(step);
            let got = state.check(&txn, now);
            prop_assert_eq!(&got, &model.decide(step, now), "step {:?}", step);
            if got == Check::Accept {
                let seq = commit(&mut state, &txn, now);
                model.apply(step, now);
                replayed
                    .apply_segment(&segment(seq, &[ref_event(&txn, now)]))
                    .expect("replay");
            }
            let refs: BTreeMap<RefName, ObjectId> = model
                .refs
                .iter()
                .map(|(&n, &o)| (name(NAMES[n as usize]), oid(o)))
                .collect();
            prop_assert_eq!(&state.refs, &refs);
            prop_assert_eq!(state.seq, Seq(model.seq));
        }
        prop_assert_eq!(replayed.to_checkpoint(now), state.to_checkpoint(now));
    }
}

#[test]
fn group_sees_earlier_txns() {
    let state = RepoState::empty(SHA1);
    let first = txn("k1", 1, vec![update("main", Expect::Absent, Some(1))]);
    let second = txn("k2", 2, vec![update("main", Expect::Oid(oid(1)), Some(2))]);
    assert_eq!(state.check(&first, T0), Check::Accept);

    // The group's working copy: the first transaction applied at index 0.
    let mut group = state.clone();
    group
        .apply_txn(
            &first.to_pb(SHA1),
            &first.key,
            first.request_digest,
            Seq(1),
            0,
            T0,
        )
        .expect("apply");
    assert_eq!(group.check(&second, T0), Check::Accept, "sees index 0");
    group
        .apply_txn(
            &second.to_pb(SHA1),
            &second.key,
            second.request_digest,
            Seq(1),
            1,
            T0,
        )
        .expect("apply");
    assert_eq!(group.refs[&name("main")], oid(2));

    // The state the group started from is untouched.
    assert_eq!(
        state.check(&second, T0),
        Check::Reject(vec![(name("main"), RejectReason::Stale { current: None })])
    );
    assert!(state.refs.is_empty());

    // A third transaction of the group replays the first's key.
    assert_eq!(
        group.check(&first, T0),
        Check::Replay(Receipt {
            seq: Seq(1),
            index: 0,
            replayed: true
        })
    );
}

#[test]
fn replay_inside_window_returns_receipt() {
    let mut state = RepoState::empty(SHA1);
    let push = txn("push-1", 7, vec![update("main", Expect::Absent, Some(1))]);
    let mut first = state.clone();
    first.seq = Seq(4);
    first
        .apply_txn(
            &push.to_pb(SHA1),
            &push.key,
            push.request_digest,
            Seq(5),
            2,
            T0,
        )
        .expect("apply");
    first.seq = Seq(5);
    state = first;
    let receipt = Receipt {
        seq: Seq(5),
        index: 2,
        replayed: true,
    };
    // Its expectation no longer holds, but the key answers first.
    assert_eq!(state.check(&push, T0), Check::Replay(receipt));
    assert_eq!(
        state.check(&push, T0 + window() - 1),
        Check::Replay(receipt),
        "the last ms of the window"
    );
}

#[test]
fn key_reuse_with_other_digest_is_mismatch() {
    let mut state = RepoState::empty(SHA1);
    let push = txn("push-1", 7, vec![update("main", Expect::Absent, Some(1))]);
    commit(&mut state, &push, T0);
    let other = txn("push-1", 8, vec![update("main", Expect::Absent, Some(1))]);
    assert_eq!(state.check(&other, T0 + 1), Check::Mismatch);
    let other_updates = txn("push-1", 7, vec![update("dev", Expect::Absent, Some(3))]);
    assert_eq!(
        state.check(&other_updates, T0 + 1),
        Check::Replay(Receipt {
            seq: Seq(1),
            index: 0,
            replayed: true
        }),
        "the digest, not the updates, binds the key"
    );
}

#[test]
fn after_window_old_oid_decides() {
    let mut state = RepoState::empty(SHA1);
    let create = txn("push-1", 7, vec![update("main", Expect::Absent, Some(1))]);
    commit(&mut state, &create, T0);
    let forward = txn(
        "push-2",
        8,
        vec![update("main", Expect::Oid(oid(1)), Some(2))],
    );
    commit(&mut state, &forward, T0 + 10);

    let expired = T0 + 10 + window();
    // A retry of the create after its window is a new transaction, and the
    // ref exists.
    assert_eq!(
        state.check(&create, expired),
        Check::Reject(vec![(name("main"), RejectReason::Exists)])
    );
    // A retry of the fast-forward finds main moved: never applied twice.
    assert_eq!(
        state.check(&forward, expired),
        Check::Reject(vec![(
            name("main"),
            RejectReason::Stale {
                current: Some(oid(2))
            }
        )])
    );
    // One ms earlier, the fast-forward's key still answers.
    assert!(matches!(
        state.check(&forward, expired - 1),
        Check::Replay(_)
    ));
}

#[test]
fn protected_ref_refuses_delete_and_force() {
    let mut state = RepoState::empty(SHA1);
    state.protections = vec![
        pb::Protection {
            pattern: "refs/heads/main".into(),
            deny_delete: true,
            deny_force: true,
        },
        pb::Protection {
            pattern: "refs/heads/release/*".into(),
            deny_delete: true,
            deny_force: false,
        },
    ];
    let setup = txn(
        "setup",
        0,
        vec![
            update("main", Expect::Absent, Some(1)),
            update("release/1", Expect::Absent, Some(1)),
            update("topic", Expect::Absent, Some(1)),
        ],
    );
    commit(&mut state, &setup, T0);
    let cases = [
        (update("main", Expect::Oid(oid(1)), None), false),
        (update("main", Expect::Any, Some(2)), false),
        (update("main", Expect::Oid(oid(1)), Some(2)), true),
        (update("release/1", Expect::Oid(oid(1)), None), false),
        (update("release/1", Expect::Any, Some(2)), true),
        (update("topic", Expect::Oid(oid(1)), None), true),
        (update("topic", Expect::Any, Some(2)), true),
    ];
    for (i, (case, allowed)) in cases.into_iter().enumerate() {
        let short = case.name.as_str()["refs/heads/".len()..].to_string();
        let check = state.check(&txn(&format!("k{i}"), 1, vec![case]), T0 + 1);
        if allowed {
            assert_eq!(check, Check::Accept, "case {i}");
        } else {
            assert_eq!(
                check,
                Check::Reject(vec![(name(&short), RejectReason::Protected)]),
                "case {i}"
            );
        }
    }
    assert!(pattern_matches("refs/heads/*", "refs/heads/a/b"));
    assert!(pattern_matches("refs/*/main", "refs/heads/main"));
    assert!(!pattern_matches("refs/heads/main", "refs/heads/main2"));
    assert!(!pattern_matches("refs/heads/a*a", "refs/heads/a"));
}

#[test]
fn duplicate_ref_in_one_txn_is_invalid() {
    let state = RepoState::empty(SHA1);
    let twice = txn(
        "k",
        1,
        vec![
            update("main", Expect::Absent, Some(1)),
            update("dev", Expect::Absent, Some(1)),
            update("main", Expect::Any, Some(2)),
        ],
    );
    assert_eq!(
        state.check(&twice, T0),
        Check::Reject(vec![(name("main"), RejectReason::InvalidName)])
    );
}

#[test]
fn checkpoint_round_trip_keeps_window_entries_only() {
    let mut state = RepoState::empty(SHA1);
    state.symrefs.insert("HEAD".into(), name("main"));
    state.protections = protected_c();
    state.parent = Some(pb::ForkParent {
        repo_id: "r01j9z3m5q8y0000000000000000".into(),
        seq: 9,
    });
    let mut pushes = Vec::new();
    for (i, at) in [0, window() / 2, window()].into_iter().enumerate() {
        let mut push = txn(
            &format!("push-{i}"),
            i as u8,
            vec![update(&format!("b{i}"), Expect::Absent, Some(i as u8 + 1))],
        );
        push.packs.push(pb::PackRef {
            checksum: format!("{:02x}", i).repeat(20),
            object_count: 1,
            ..Default::default()
        });
        commit(&mut state, &push, T0 + at);
        pushes.push(push);
    }
    // At this time push-0's window has closed and the others are open.
    let now = T0 + window() + 1;
    let checkpoint = state.to_checkpoint(now);
    assert_eq!(checkpoint.seq, 3);
    assert_eq!(
        checkpoint
            .idempotency
            .iter()
            .map(|e| e.key.as_str())
            .collect::<Vec<_>>(),
        ["push-1", "push-2"]
    );
    assert_eq!(checkpoint.packs.len(), 3);
    assert_eq!(state.to_checkpoint(now), checkpoint, "deterministic");

    let restored = RepoState::from_checkpoint(checkpoint.clone()).expect("restore");
    assert_eq!(restored.to_checkpoint(now), checkpoint);
    assert_eq!(restored.snapshot().refs, state.snapshot().refs);
    assert_eq!(restored.symrefs["HEAD"], name("main"));
    assert_eq!(restored.parent, state.parent);
    assert!(matches!(restored.check(&pushes[2], now), Check::Replay(_)));
    assert!(matches!(restored.check(&pushes[0], now), Check::Reject(_)));

    // Formats other than SHA-1, and malformed entries, are refused.
    let mut sha256 = checkpoint.clone();
    sha256.object_format = pb::ObjectFormat::Sha256 as i32;
    let err = RepoState::from_checkpoint(sha256).expect_err("sha256");
    assert_eq!(
        err.to_string(),
        "object format sha256 is not supported yet (Q388)"
    );
    let mut bad_digest = checkpoint.clone();
    bad_digest.idempotency[0].request_digest = vec![1; 31];
    assert!(matches!(
        RepoState::from_checkpoint(bad_digest),
        Err(StateError::Corrupt { seq: Seq(3), .. })
    ));
    let mut bad_ref = checkpoint;
    bad_ref.refs.insert("heads/x".into(), vec![1; 20]);
    assert!(matches!(
        RepoState::from_checkpoint(bad_ref),
        Err(StateError::Corrupt { .. })
    ));
}

#[test]
fn apply_segment_refuses_a_gap() {
    let mut state = RepoState::empty(SHA1);
    let push = txn("k1", 1, vec![update("main", Expect::Absent, Some(1))]);
    let two = segment(Seq(2), &[ref_event(&push, T0)]);
    assert_eq!(
        state.apply_segment(&two),
        Err(StateError::Gap {
            current: Seq(0),
            found: Seq(2)
        })
    );
    let one = segment(Seq(1), &[ref_event(&push, T0)]);
    state.apply_segment(&one).expect("seq 1 follows 0");
    assert_eq!(state.seq, Seq(1));
    assert_eq!(
        state.apply_segment(&one),
        Err(StateError::Gap {
            current: Seq(1),
            found: Seq(1)
        }),
        "a segment is applied once"
    );
}

/// A segment applies its events in order: ref transactions (with their
/// idempotency entries, from the digest the event carries), pack-set
/// changes and configuration changes.
#[test]
fn apply_segment_applies_every_event_kind() {
    let mut state = RepoState::empty(SHA1);
    let mut push = txn("push-1", 7, vec![update("main", Expect::Absent, Some(1))]);
    push.packs.push(pb::PackRef {
        checksum: "ab".repeat(20),
        object_count: 3,
        ..Default::default()
    });
    let repack = event(
        "01j9z3m5q8abcdefghjkmnpqrs",
        TxnBody::PackSet(pb::PackSetChange {
            added: vec![pb::PackRef {
                checksum: "cd".repeat(20),
                object_count: 3,
                ..Default::default()
            }],
            removed: vec!["ab".repeat(20)],
            ..Default::default()
        }),
        T0,
    );
    let config = event(
        "01j9z3m5q8abcdefghjkmnpqrt",
        TxnBody::Config(pb::ConfigChange {
            symrefs: vec![pb::SymRef {
                name: "HEAD".into(),
                target: "refs/heads/main".into(),
            }],
            protections: protected_c(),
        }),
        T0,
    );
    let later = txn("push-2", 8, vec![update("dev", Expect::Absent, Some(2))]);
    state
        .apply_segment(&segment(
            Seq(1),
            &[ref_event(&push, T0), repack, config, ref_event(&later, T0)],
        ))
        .expect("apply");
    assert_eq!(state.refs.len(), 2);
    assert_eq!(
        state
            .packs
            .iter()
            .map(|p| p.checksum.clone())
            .collect::<Vec<_>>(),
        ["cd".repeat(20)]
    );
    assert_eq!(state.symrefs["HEAD"], name("main"));
    assert_eq!(state.protections, protected_c());
    assert_eq!(
        state.check(&later, T0 + 1),
        Check::Replay(Receipt {
            seq: Seq(1),
            index: 3,
            replayed: true
        }),
        "a replayed segment restores its keys at their indexes"
    );
    assert_eq!(
        state.check(&txn("push-1", 9, vec![]), T0 + 1),
        Check::Mismatch
    );
}

#[test]
fn corrupt_segment_leaves_state_unchanged() {
    let mut state = RepoState::empty(SHA1);
    let good = txn("k1", 1, vec![update("main", Expect::Absent, Some(1))]);
    let mut no_digest = txn("k2", 1, vec![update("dev", Expect::Absent, Some(1))]).to_pb(SHA1);
    no_digest.request_digest.clear();
    let bad = event("k2", TxnBody::RefTxn(no_digest), T0);
    let before = state.clone();
    let err = state
        .apply_segment(&segment(Seq(1), &[ref_event(&good, T0), bad]))
        .expect_err("a ref transaction without its digest");
    assert!(
        matches!(err, StateError::Corrupt { seq: Seq(1), .. }),
        "{err:?}"
    );
    assert_eq!(state, before);

    let mut sha256 = good.to_pb(SHA1);
    sha256.object_format = pb::ObjectFormat::Sha256 as i32;
    let err = state
        .apply_segment(&segment(
            Seq(1),
            &[event("k3", TxnBody::RefTxn(sha256), T0)],
        ))
        .expect_err("another format");
    assert!(
        matches!(err, StateError::UnsupportedFormat { .. }),
        "{err:?}"
    );
    assert_eq!(state, before);
}
