//! Fixed transactions and the golden-file helper shared by the format tests.

use std::path::PathBuf;

use loams_git::format::event::{TxnBody, TxnEvent, to_cloudevent};
use loams_git::format::pb;
use loams_git::format::segment::WalBatch;
use loams_git::{IdemKey, NamespaceId, RepoId, Seq, TraceParent};

pub fn oid(byte: u8) -> Vec<u8> {
    vec![byte; 20]
}

pub fn ref_txn() -> pb::RefTransaction {
    pb::RefTransaction {
        updates: vec![
            pb::RefUpdate {
                name: "refs/heads/main".into(),
                expect: Some(pb::ref_update::Expect::OldOid(oid(1))),
                new_oid: oid(2),
            },
            pb::RefUpdate {
                name: "refs/heads/feature".into(),
                expect: Some(pb::ref_update::Expect::MustNotExist(true)),
                new_oid: oid(3),
            },
            pb::RefUpdate {
                name: "refs/tags/old".into(),
                expect: Some(pb::ref_update::Expect::Any(true)),
                new_oid: Vec::new(),
            },
        ],
        packs: vec![pack_ref(0xab)],
        principal: "alice@example.com".into(),
        push_options: vec!["ci.skip".into()],
        object_format: pb::ObjectFormat::Sha1 as i32,
    }
}

pub fn pack_ref(byte: u8) -> pb::PackRef {
    pb::PackRef {
        checksum: format!("{byte:02x}").repeat(20),
        object_count: 3,
        pack_len: 1234,
        idx_offset: 1234,
        idx_len: 1160,
        has_bitmap: false,
    }
}

pub fn event(key: &str, body: TxnBody, trace: bool) -> TxnEvent {
    TxnEvent {
        ns: NamespaceId(7),
        repo: RepoId::parse("r01j9z3m5q8x0000000000000000").expect("repo id"),
        tenant: "acme/prod".into(),
        key: IdemKey::parse(key).expect("key"),
        trace: trace.then(|| {
            TraceParent::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .expect("traceparent")
        }),
        time_unix_ms: 1_790_762_400_250,
        body,
    }
}

/// The golden segment's batch: one event of each type.
pub fn golden_batch(seq: Seq) -> WalBatch {
    let events = [
        event(
            "6d5b3c1f0e9a8b7c6d5b3c1f0e9a8b7c6d5b3c1f0e9a8b7c6d5b3c1f0e9a8b7c",
            TxnBody::RefTxn(ref_txn()),
            true,
        ),
        event(
            "01j9z3m5q8abcdefghjkmnpqrs",
            TxnBody::PackSet(pb::PackSetChange {
                added: vec![pack_ref(0xcd)],
                removed: vec!["ab".repeat(20)],
                midx: "midx/00000000000000000041.midx".into(),
                commit_graph: String::new(),
            }),
            false,
        ),
        event(
            "01j9z3m5q8abcdefghjkmnpqrt",
            TxnBody::Config(pb::ConfigChange {
                symrefs: vec![pb::SymRef {
                    name: "HEAD".into(),
                    target: "refs/heads/main".into(),
                }],
                protections: vec![pb::Protection {
                    pattern: "refs/heads/main".into(),
                    deny_delete: true,
                    deny_force: true,
                }],
            }),
            false,
        ),
    ];
    WalBatch {
        batch_id: *b"golden-batch-id!",
        events: events.iter().map(|e| to_cloudevent(e, seq)).collect(),
    }
}

/// The golden checkpoint: refs, symrefs, protections, packs, a fork parent
/// and an idempotency entry.
pub fn golden_checkpoint() -> pb::Checkpoint {
    pb::Checkpoint {
        seq: 42,
        object_format: pb::ObjectFormat::Sha1 as i32,
        refs: [
            ("refs/heads/main".to_string(), oid(2)),
            ("refs/heads/feature".to_string(), oid(3)),
            ("refs/tags/v1".to_string(), oid(4)),
        ]
        .into_iter()
        .collect(),
        symrefs: vec![pb::SymRef {
            name: "HEAD".into(),
            target: "refs/heads/main".into(),
        }],
        protections: vec![pb::Protection {
            pattern: "refs/heads/main".into(),
            deny_delete: true,
            deny_force: false,
        }],
        packs: vec![pack_ref(0xab), pack_ref(0xcd)],
        parent: Some(pb::ForkParent {
            repo_id: "r01j9z3m5q8y0000000000000000".into(),
            seq: 9,
        }),
        idempotency: vec![pb::IdemEntry {
            key: "push-1".into(),
            request_digest: vec![9; 32],
            seq: 41,
            index: 0,
            expires_unix_ms: 1_790_766_000_000,
        }],
    }
}

pub fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name)
}

/// The checked-in golden bytes. With `LOAMS_GIT_BLESS=1` the file is first
/// (re)written from `fresh`: only when a format version changes on purpose.
pub fn golden(name: &str, fresh: &[u8]) -> Vec<u8> {
    let path = golden_path(name);
    if std::env::var_os("LOAMS_GIT_BLESS").is_some() {
        std::fs::write(&path, fresh).expect("write golden file");
    }
    std::fs::read(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}
