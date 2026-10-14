//! Forwarding a commit to the repository's sequencer owner (§36 §4.4; plan
//! GT1 Task 14, Ruling 27): the postcard body of `POST
//! /internal/v1/git/commit` and its answer, and the [`SeqForwarder`] seam
//! the server implements over HTTP.
//!
//! A forwarded commit carries everything the owner's `BucketRefLog` needs
//! (the transaction or configuration change, with its idempotency key), so
//! a commit retried on another node after a lost forward is replayed there,
//! never applied twice.

use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message as _;
use serde::{Deserialize, Serialize};

use crate::format::pb;
use crate::ids::TraceParent;
use crate::ids::{IdemKey, NamespaceId, ObjectId, Principal, RefName, RepoId, RepoPath, Seq};
use crate::reflog::{Expect, Receipt, RefError, RefTxn, RefUpdate, RejectReason, Scope};

/// The internal route a forwarded commit is posted to.
pub const FORWARD_PATH: &str = "/internal/v1/git/commit";

/// The wire version of [`ForwardedTxn`] and [`ForwardedAnswer`].
const WIRE_VERSION: u32 = 1;

/// Posts a forwarded commit to the owner at `addr` and returns its answer
/// body; any transport failure is an `Err` (the caller falls back).
#[async_trait]
pub trait SeqForwarder: Send + Sync + fmt::Debug {
    /// `POST http://<addr>/internal/v1/git/commit` with `body`.
    async fn forward(&self, addr: &str, body: Bytes) -> Result<Bytes, String>;
}

/// What a sequencer commits: a ref transaction or a configuration change.
#[derive(Clone, Debug, PartialEq)]
pub enum SeqCommit {
    /// `RefLog::commit`.
    Ref(RefTxn),
    /// `BucketRefLog::commit_config`.
    Config {
        /// The idempotency key.
        key: IdemKey,
        /// The change.
        change: pb::ConfigChange,
    },
}

/// A forwarded commit, as posted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ForwardedTxn {
    v: u32,
    ns: u64,
    tenant: String,
    repo: String,
    commit: WireCommit,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireCommit {
    Ref(WireRefTxn),
    /// The change, protobuf-encoded.
    Config {
        key: String,
        change: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireRefTxn {
    key: String,
    request_digest: [u8; 32],
    updates: Vec<WireUpdate>,
    /// Each `PackRef`, protobuf-encoded.
    packs: Vec<Vec<u8>>,
    principal: String,
    scope: Option<WireScope>,
    trace: Option<String>,
    push_options: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireScope {
    repo: String,
    commit: Vec<u8>,
    cones: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireUpdate {
    name: String,
    expect: WireExpect,
    new: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireExpect {
    Oid(Vec<u8>),
    Absent,
    Any,
}

/// The owner's answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum ForwardedAnswer {
    Committed {
        seq: u64,
        index: u32,
        replayed: bool,
    },
    Rejected(Vec<(String, WireReject)>),
    IdempotencyMismatch,
    TooLarge,
    Unavailable,
    Invalid(String),
    NotFound,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireReject {
    Stale(Option<Vec<u8>>),
    Exists,
    Protected,
    OutOfScope(String),
    InvalidName,
}

/// A request that could not be decoded.
#[derive(Debug, thiserror::Error)]
#[error("malformed forwarded commit: {0}")]
pub struct WireError(pub String);

fn bad(what: impl fmt::Display) -> WireError {
    WireError(what.to_string())
}

fn oid(bytes: &[u8]) -> Result<ObjectId, WireError> {
    ObjectId::try_from(bytes).map_err(bad)
}

/// The body of a forwarded commit of `repo` in `ns` by `tenant`.
pub fn encode_request(ns: NamespaceId, tenant: &str, repo: &RepoId, commit: &SeqCommit) -> Bytes {
    let commit = match commit {
        SeqCommit::Ref(txn) => WireCommit::Ref(WireRefTxn {
            key: txn.key.as_str().to_string(),
            request_digest: txn.request_digest,
            updates: txn
                .updates
                .iter()
                .map(|u| WireUpdate {
                    name: u.name.as_str().to_string(),
                    expect: match u.expect {
                        Expect::Oid(oid) => WireExpect::Oid(oid.as_bytes().to_vec()),
                        Expect::Absent => WireExpect::Absent,
                        Expect::Any => WireExpect::Any,
                    },
                    new: u.new.map(|oid| oid.as_bytes().to_vec()),
                })
                .collect(),
            packs: txn.packs.iter().map(|p| p.encode_to_vec()).collect(),
            principal: txn.principal.as_str().to_string(),
            scope: txn.scope.as_ref().map(|s| WireScope {
                repo: s.repo.as_str().to_string(),
                commit: s.commit.as_bytes().to_vec(),
                cones: s.cones.iter().map(|c| c.as_str().to_string()).collect(),
            }),
            trace: txn.trace.as_ref().map(|t| t.as_str().to_string()),
            push_options: txn.push_options.clone(),
        }),
        SeqCommit::Config { key, change } => WireCommit::Config {
            key: key.as_str().to_string(),
            change: change.encode_to_vec(),
        },
    };
    let request = ForwardedTxn {
        v: WIRE_VERSION,
        ns: ns.0,
        tenant: tenant.to_string(),
        repo: repo.as_str().to_string(),
        commit,
    };
    // Plain structs of strings, bytes and integers: encoding cannot fail.
    Bytes::from(postcard::to_stdvec(&request).expect("postcard encodes plain data"))
}

/// A decoded forwarded commit: the repository, its tenant, and the commit.
pub fn decode_request(body: &[u8]) -> Result<(NamespaceId, String, RepoId, SeqCommit), WireError> {
    let request: ForwardedTxn = postcard::from_bytes(body).map_err(bad)?;
    if request.v != WIRE_VERSION {
        return Err(bad(format!("wire version {}", request.v)));
    }
    let repo = RepoId::parse(&request.repo).map_err(bad)?;
    let commit = match request.commit {
        WireCommit::Config { key, change } => SeqCommit::Config {
            key: IdemKey::parse(&key).map_err(bad)?,
            change: pb::ConfigChange::decode(change.as_slice()).map_err(bad)?,
        },
        WireCommit::Ref(txn) => {
            let updates = txn
                .updates
                .into_iter()
                .map(|u| {
                    Ok(RefUpdate {
                        name: RefName::parse(&u.name).map_err(bad)?,
                        expect: match u.expect {
                            WireExpect::Oid(bytes) => Expect::Oid(oid(&bytes)?),
                            WireExpect::Absent => Expect::Absent,
                            WireExpect::Any => Expect::Any,
                        },
                        new: u.new.as_deref().map(oid).transpose()?,
                    })
                })
                .collect::<Result<Vec<_>, WireError>>()?;
            let packs = txn
                .packs
                .iter()
                .map(|p| pb::PackRef::decode(p.as_slice()).map_err(bad))
                .collect::<Result<Vec<_>, _>>()?;
            let scope = txn
                .scope
                .map(|s| {
                    Ok::<_, WireError>(Scope {
                        repo: RepoId::parse(&s.repo).map_err(bad)?,
                        commit: oid(&s.commit)?,
                        cones: s
                            .cones
                            .iter()
                            .map(|c| RepoPath::parse(c).map_err(bad))
                            .collect::<Result<_, _>>()?,
                    })
                })
                .transpose()?;
            SeqCommit::Ref(RefTxn {
                key: IdemKey::parse(&txn.key).map_err(bad)?,
                request_digest: txn.request_digest,
                updates,
                packs,
                principal: Principal::parse(&txn.principal).map_err(bad)?,
                scope,
                trace: txn
                    .trace
                    .as_deref()
                    .map(TraceParent::parse)
                    .transpose()
                    .map_err(bad)?,
                push_options: txn.push_options,
            })
        }
    };
    Ok((NamespaceId(request.ns), request.tenant, repo, commit))
}

/// The owner's answer to a forwarded commit, as sent back.
pub fn encode_answer(answer: &Result<Receipt, RefError>) -> Bytes {
    let wire = match answer {
        Ok(receipt) => ForwardedAnswer::Committed {
            seq: receipt.seq.0,
            index: receipt.index,
            replayed: receipt.replayed,
        },
        Err(RefError::Rejected(rejected)) => ForwardedAnswer::Rejected(
            rejected
                .iter()
                .map(|(name, reason)| {
                    let reason = match reason {
                        RejectReason::Stale { current } => {
                            WireReject::Stale(current.map(|oid| oid.as_bytes().to_vec()))
                        }
                        RejectReason::Exists => WireReject::Exists,
                        RejectReason::Protected => WireReject::Protected,
                        RejectReason::OutOfScope { path } => {
                            WireReject::OutOfScope(path.as_str().to_string())
                        }
                        RejectReason::InvalidName => WireReject::InvalidName,
                    };
                    (name.as_str().to_string(), reason)
                })
                .collect(),
        ),
        Err(RefError::IdempotencyMismatch) => ForwardedAnswer::IdempotencyMismatch,
        Err(RefError::TooLarge) => ForwardedAnswer::TooLarge,
        Err(RefError::Invalid { reason }) => ForwardedAnswer::Invalid(reason.clone()),
        Err(RefError::NotFound) => ForwardedAnswer::NotFound,
        Err(RefError::State(err)) => ForwardedAnswer::Invalid(err.to_string()),
        // The owner could not learn the outcome or reach the WAL: the
        // caller retries with the same key, which replays a landed commit.
        Err(RefError::Unavailable | RefError::Wal(_)) => ForwardedAnswer::Unavailable,
    };
    Bytes::from(postcard::to_stdvec(&wire).expect("postcard encodes plain data"))
}

/// The answer an owner sent back.
pub fn decode_answer(body: &[u8]) -> Result<Result<Receipt, RefError>, WireError> {
    let answer: ForwardedAnswer = postcard::from_bytes(body).map_err(bad)?;
    Ok(match answer {
        ForwardedAnswer::Committed {
            seq,
            index,
            replayed,
        } => Ok(Receipt {
            seq: Seq(seq),
            index,
            replayed,
        }),
        ForwardedAnswer::Rejected(rejected) => Err(RefError::Rejected(
            rejected
                .into_iter()
                .map(|(name, reason)| {
                    let reason = match reason {
                        WireReject::Stale(current) => RejectReason::Stale {
                            current: current.as_deref().map(oid).transpose()?,
                        },
                        WireReject::Exists => RejectReason::Exists,
                        WireReject::Protected => RejectReason::Protected,
                        WireReject::OutOfScope(path) => RejectReason::OutOfScope {
                            path: RepoPath::parse(&path).map_err(bad)?,
                        },
                        WireReject::InvalidName => RejectReason::InvalidName,
                    };
                    Ok((RefName::parse(&name).map_err(bad)?, reason))
                })
                .collect::<Result<Vec<_>, WireError>>()?,
        )),
        ForwardedAnswer::IdempotencyMismatch => Err(RefError::IdempotencyMismatch),
        ForwardedAnswer::TooLarge => Err(RefError::TooLarge),
        ForwardedAnswer::Unavailable => Err(RefError::Unavailable),
        ForwardedAnswer::Invalid(reason) => Err(RefError::Invalid { reason }),
        ForwardedAnswer::NotFound => Err(RefError::NotFound),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid_of(n: u8) -> ObjectId {
        ObjectId::try_from(&[n; 20][..]).expect("oid")
    }

    #[test]
    fn requests_round_trip() {
        let repo = RepoId::parse("r01jtest000000000000000000").expect("repo");
        let txn = SeqCommit::Ref(RefTxn {
            key: IdemKey::parse("git-push:abc").expect("key"),
            request_digest: [9; 32],
            updates: vec![
                RefUpdate {
                    name: RefName::parse("refs/heads/main").expect("ref"),
                    expect: Expect::Oid(oid_of(1)),
                    new: Some(oid_of(2)),
                },
                RefUpdate {
                    name: RefName::parse("refs/heads/gone").expect("ref"),
                    expect: Expect::Any,
                    new: None,
                },
            ],
            packs: vec![pb::PackRef {
                checksum: "ab".repeat(20),
                object_count: 3,
                pack_len: 100,
                idx_offset: 100,
                idx_len: 1100,
                has_bitmap: false,
            }],
            principal: Principal::parse("t@example.com").expect("principal"),
            scope: Some(Scope {
                repo: repo.clone(),
                commit: oid_of(3),
                cones: vec![RepoPath::parse("src/lib").expect("path")],
            }),
            trace: Some(
                TraceParent::parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
                    .expect("trace"),
            ),
            push_options: vec!["ci.skip".into()],
        });
        let body = encode_request(NamespaceId(7), "local/7", &repo, &txn);
        let (ns, tenant, back_repo, back) = decode_request(&body).expect("decode");
        assert_eq!(
            (ns, tenant.as_str(), &back_repo),
            (NamespaceId(7), "local/7", &repo)
        );
        assert_eq!(back, txn);
        let config = SeqCommit::Config {
            key: IdemKey::parse("cfg").expect("key"),
            change: pb::ConfigChange {
                symrefs: vec![pb::SymRef {
                    name: "HEAD".into(),
                    target: "refs/heads/main".into(),
                }],
                protections: Vec::new(),
                request_digest: vec![1; 32],
            },
        };
        let body = encode_request(NamespaceId(7), "local/7", &repo, &config);
        assert_eq!(decode_request(&body).expect("decode").3, config);
        assert!(decode_request(b"\xff\x00garbage").is_err());
    }

    #[test]
    fn answers_round_trip() {
        let receipt = Receipt {
            seq: Seq(12),
            index: 3,
            replayed: true,
        };
        match decode_answer(&encode_answer(&Ok(receipt))).expect("decode") {
            Ok(back) => assert_eq!(back, receipt),
            Err(err) => panic!("{err}"),
        }
        let rejected = RefError::Rejected(vec![
            (
                RefName::parse("refs/heads/a").expect("ref"),
                RejectReason::Stale {
                    current: Some(oid_of(4)),
                },
            ),
            (
                RefName::parse("refs/heads/b").expect("ref"),
                RejectReason::Protected,
            ),
        ]);
        match decode_answer(&encode_answer(&Err(rejected))).expect("decode") {
            Err(RefError::Rejected(list)) => {
                assert_eq!(list.len(), 2);
                assert_eq!(
                    list[0].1,
                    RejectReason::Stale {
                        current: Some(oid_of(4))
                    }
                );
                assert_eq!(list[1].1, RejectReason::Protected);
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            decode_answer(&encode_answer(&Err(RefError::Unavailable))).expect("decode"),
            Err(RefError::Unavailable)
        ));
        assert!(matches!(
            decode_answer(&encode_answer(&Err(RefError::IdempotencyMismatch))).expect("decode"),
            Err(RefError::IdempotencyMismatch)
        ));
    }
}
