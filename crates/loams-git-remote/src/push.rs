//! `push` (old GT1 Task 9 step 6, §36 §6.3 as the helper uses it): check
//! each refspec, pack the new objects once with `git pack-objects`, upload
//! one `.lpk`, and commit one ref transaction (an atomic push) or one per
//! ref (committed together, so they share segments) through this
//! process's own `BucketRefLog`.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use futures::future::join_all;
use loams_git::format::pb;
use loams_git::limits::{MAX_PACK_BYTES, MAX_REFS_PER_PUSH};
use loams_git::{
    BlobStore, BucketRefLog, Expect, IdemKey, LpkWriter, ObjectId, Principal, Receipt, RefError,
    RefLog, RefName, RefSnapshot, RefTxn, RefUpdate, RejectReason,
};
use sha2::{Digest, Sha256};

use crate::HelperError;
use crate::local::Local;

/// Commits answered `Unavailable` (the outcome was lost) are repeated with
/// the same key this many times; a repeat of a landed commit replays it.
const COMMIT_ATTEMPTS: u32 = 5;

/// One `push [+]<src>:<dst>` line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushSpec {
    /// `+`: update even when it is not a fast-forward.
    pub force: bool,
    /// The local ref or object; `None` deletes `dst`.
    pub src: Option<String>,
    /// The remote ref.
    pub dst: String,
}

impl PushSpec {
    /// Parses the argument of a `push` line.
    pub fn parse(arg: &str) -> Result<Self, HelperError> {
        let (force, rest) = match arg.strip_prefix('+') {
            Some(rest) => (true, rest),
            None => (false, arg),
        };
        let (src, dst) = rest
            .split_once(':')
            .ok_or_else(|| HelperError::Protocol(format!("push {arg}: expected <src>:<dst>")))?;
        if dst.is_empty() {
            return Err(HelperError::Protocol(format!(
                "push {arg}: empty destination"
            )));
        }
        Ok(Self {
            force,
            src: (!src.is_empty()).then(|| src.to_string()),
            dst: dst.to_string(),
        })
    }
}

/// How a push runs.
#[derive(Clone, Debug, Default)]
pub struct PushOptions {
    /// All refs or none (`git push --atomic`).
    pub atomic: bool,
    /// Check, but write nothing (`git push --dry-run`; see [`dry_run`]).
    pub dry_run: bool,
    /// `git push -o`.
    pub push_options: Vec<String>,
    /// Progress on stderr.
    pub progress: bool,
    /// Test hook (feature `faults`): every first answer of a commit is
    /// dropped as if lost, so the commit is repeated and replayed.
    pub lose_answers: bool,
}

/// One checked ref update, or why it was refused before any write.
#[derive(Clone, Debug)]
enum Checked {
    Update(RefUpdate),
    /// Already where the push would put it.
    UpToDate,
    Refused(String),
}

/// The answer for each `dst`, in the order git sent them: `None` is `ok`,
/// `Some(reason)` is `error <dst> <reason>`.
pub type Statuses = Vec<(String, Option<String>)>;

/// Checks every spec against `snapshot` (Ruling 5: fast-forward checks
/// run here, with the local graph).
fn check(
    local: &Local,
    snapshot: &RefSnapshot,
    specs: &[PushSpec],
) -> Result<Vec<(String, Checked)>, HelperError> {
    // The remote tips the local repository has: fast-forward checks need
    // them, and they are the bases the pack may leave out.
    let remote: Vec<ObjectId> = snapshot.refs.values().copied().collect();
    let present = local.present(&remote)?;
    let mut out = Vec::with_capacity(specs.len());
    for spec in specs {
        let Ok(name) = RefName::parse(&spec.dst) else {
            out.push((
                spec.dst.clone(),
                Checked::Refused("invalid ref name".into()),
            ));
            continue;
        };
        let current = snapshot.refs.get(&name).copied();
        let checked = match &spec.src {
            None => match current {
                None => Checked::Refused("remote ref does not exist".into()),
                Some(old) => Checked::Update(RefUpdate {
                    name,
                    expect: if spec.force {
                        Expect::Any
                    } else {
                        Expect::Oid(old)
                    },
                    new: None,
                }),
            },
            Some(src) => {
                let new = local.rev_parse(src)?;
                match current {
                    Some(old) if old == new => Checked::UpToDate,
                    None => Checked::Update(RefUpdate {
                        name,
                        expect: Expect::Absent,
                        new: Some(new),
                    }),
                    Some(old) => {
                        let ff = present.contains(&old) && local.is_ancestor(&old, &new)?;
                        // A tag that exists is never moved without force, as git
                        // refuses it.
                        let is_tag = name.as_str().starts_with("refs/tags/");
                        if ff && !is_tag {
                            // A forced fast-forward is still an ordinary
                            // update, so protections against force do not
                            // refuse it.
                            Checked::Update(RefUpdate {
                                name,
                                expect: Expect::Oid(old),
                                new: Some(new),
                            })
                        } else if spec.force {
                            Checked::Update(RefUpdate {
                                name,
                                expect: Expect::Any,
                                new: Some(new),
                            })
                        } else if is_tag {
                            Checked::Refused("already exists".into())
                        } else if !present.contains(&old) {
                            Checked::Refused("fetch first".into())
                        } else {
                            Checked::Refused("non-fast-forward".into())
                        }
                    }
                }
            }
        };
        out.push((spec.dst.clone(), checked));
    }
    Ok(out)
}

/// The words git's transport helper understands for a refusal.
fn reason_text(reason: &RejectReason) -> String {
    match reason {
        RejectReason::Stale { .. } => "fetch first".into(),
        RejectReason::Exists => "already exists".into(),
        RejectReason::Protected => "protected".into(),
        RejectReason::OutOfScope { path } => format!("out of scope: {path}"),
        RejectReason::InvalidName => "invalid ref name".into(),
    }
}

/// The request digest and idempotency key of one transaction (§36 §4.5,
/// Ruling 6): the digest is SHA-256 over the pack checksum and the sorted
/// (name, expect, new) triples, and the key binds it to the principal.
fn derive_key(
    principal: &Principal,
    pack: Option<&str>,
    updates: &[RefUpdate],
) -> (IdemKey, [u8; 32]) {
    let mut sorted: Vec<&RefUpdate> = updates.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let mut body = Sha256::new();
    body.update(b"git-remote-loams push v1\0");
    body.update(pack.unwrap_or("").as_bytes());
    for update in sorted {
        body.update(b"\0");
        body.update(update.name.as_str().as_bytes());
        match update.expect {
            Expect::Oid(oid) => body.update(format!("\0oid:{oid}")),
            Expect::Absent => body.update(b"\0absent"),
            Expect::Any => body.update(b"\0any"),
        }
        match update.new {
            Some(oid) => body.update(format!("\0new:{oid}")),
            None => body.update(b"\0delete"),
        }
    }
    let digest: [u8; 32] = body.finalize().into();
    let mut key = Sha256::new();
    key.update(principal.as_str().as_bytes());
    key.update(b"\0");
    key.update(digest);
    let key = format!("git-push:{}", hex::encode(key.finalize()));
    // 73 bytes of `[a-z0-9:-]`: within `IdemKey`'s rules.
    (IdemKey::parse(&key).expect("a valid key"), digest)
}

/// Commits `txn`, repeating it with the same key while the outcome is
/// lost (`Unavailable`): a repeat of a commit that landed is answered with
/// its first receipt (`replayed`).
async fn commit(log: &BucketRefLog, txn: RefTxn, lose_answers: bool) -> Result<Receipt, RefError> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let answer = match log.commit(txn.clone()).await {
            // The test hook: the answer of a landed commit is lost once.
            Ok(_) if lose_answers && attempt == 1 => Err(RefError::Unavailable),
            other => other,
        };
        match answer {
            Err(RefError::Unavailable) if attempt < COMMIT_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt))).await;
            }
            other => return other,
        }
    }
}

/// Runs one batch of push lines against `log` (whose state is the
/// remote's) and answers each destination.
pub async fn push(
    log: &BucketRefLog,
    blobs: &dyn BlobStore,
    local: &Local,
    principal: &Principal,
    specs: &[PushSpec],
    options: &PushOptions,
) -> Result<Statuses, HelperError> {
    let snapshot = log
        .snapshot(loams_git::ReadAt::Latest)
        .await
        .map_err(|err| HelperError::store("read the remote refs", err))?;
    let checked = check(local, &snapshot, specs)?;
    let refused = checked
        .iter()
        .any(|(_, c)| matches!(c, Checked::Refused(_)));
    // An atomic push with a refused ref writes nothing.
    if options.atomic && refused {
        return Ok(checked
            .into_iter()
            .map(|(dst, c)| match c {
                Checked::Refused(reason) => (dst, Some(reason)),
                _ => (dst, Some("atomic push failed".into())),
            })
            .collect());
    }
    let updates: Vec<&RefUpdate> = checked
        .iter()
        .filter_map(|(_, c)| match c {
            Checked::Update(update) => Some(update),
            _ => None,
        })
        .collect();
    let mut answers: BTreeMap<String, Option<String>> = BTreeMap::new();
    if !updates.is_empty() {
        let pack = upload_pack(blobs, local, &snapshot, &updates, options.progress).await?;
        let checksum = pack.as_ref().map(|p| p.checksum.clone());
        let txn_of = |group: Vec<RefUpdate>| {
            let (key, request_digest) = derive_key(principal, checksum.as_deref(), &group);
            RefTxn {
                key,
                request_digest,
                updates: group,
                packs: pack.iter().cloned().collect(),
                principal: principal.clone(),
                scope: None,
                trace: None,
                push_options: options.push_options.clone(),
            }
        };
        let txns: Vec<RefTxn> = if options.atomic {
            vec![txn_of(updates.iter().map(|u| (*u).clone()).collect())]
        } else {
            updates.iter().map(|u| txn_of(vec![(*u).clone()])).collect()
        };
        let results = join_all(
            txns.iter()
                .map(|txn| commit(log, txn.clone(), options.lose_answers)),
        )
        .await;
        let mut landed = false;
        for (txn, result) in txns.iter().zip(results) {
            let names = txn.updates.iter().map(|u| u.name.as_str().to_string());
            match result {
                Ok(receipt) => {
                    landed = true;
                    if receipt.replayed {
                        eprintln!("loams: replayed the push committed at seq {}", receipt.seq);
                    }
                    answers.extend(names.map(|name| (name, None)));
                }
                Err(RefError::Rejected(rejected)) => {
                    let reasons: BTreeMap<String, String> = rejected
                        .iter()
                        .map(|(name, reason)| (name.as_str().to_string(), reason_text(reason)))
                        .collect();
                    for name in names {
                        let reason = reasons
                            .get(&name)
                            .cloned()
                            .unwrap_or_else(|| "atomic push failed".into());
                        answers.insert(name, Some(reason));
                    }
                }
                Err(err) => {
                    let reason = err.to_string();
                    answers.extend(names.map(|name| (name, Some(reason.clone()))));
                }
            }
        }
        // This repository has every object of the pack it pushed: a later
        // fetch need not download it.
        if landed && let Some(checksum) = checksum {
            local.record_packs(&[checksum])?;
        }
    }
    Ok(checked
        .into_iter()
        .map(|(dst, c)| match c {
            Checked::Refused(reason) => (dst, Some(reason)),
            Checked::UpToDate => (dst, None),
            Checked::Update(update) => {
                let answer = answers
                    .remove(update.name.as_str())
                    .unwrap_or_else(|| Some("not committed".into()));
                (dst, answer)
            }
        })
        .collect())
}

/// Refuses a push of more than `MAX_REFS_PER_PUSH` (4096) refs (§36 §6.3)
/// before anything is read or written.
pub fn check_ref_count(specs: &[PushSpec]) -> Result<(), HelperError> {
    if specs.len() > MAX_REFS_PER_PUSH {
        return Err(HelperError::Protocol(format!(
            "a push may update at most {MAX_REFS_PER_PUSH} refs; this one names {}",
            specs.len()
        )));
    }
    Ok(())
}

/// `git push --dry-run`: the checks of [`push`] against `snapshot`, with
/// no write.
pub fn dry_run(
    local: &Local,
    snapshot: &RefSnapshot,
    specs: &[PushSpec],
) -> Result<Statuses, HelperError> {
    Ok(check(local, snapshot, specs)?
        .into_iter()
        .map(|(dst, checked)| match checked {
            Checked::Refused(reason) => (dst, Some(reason)),
            Checked::UpToDate | Checked::Update(_) => (dst, None),
        })
        .collect())
}

/// Packs the objects of the new tips the remote lacks, uploads them as one
/// `.lpk`, and returns its `PackRef`; `None` when there is nothing to send.
async fn upload_pack(
    blobs: &dyn BlobStore,
    local: &Local,
    snapshot: &RefSnapshot,
    updates: &[&RefUpdate],
    progress: bool,
) -> Result<Option<pb::PackRef>, HelperError> {
    let tips: Vec<ObjectId> = updates
        .iter()
        .filter_map(|u| u.new)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let remote: Vec<ObjectId> = snapshot.refs.values().copied().collect();
    let mut exclude: Vec<ObjectId> = local.present(&remote)?.into_iter().collect();
    exclude.sort();
    let Some((pack, idx)) = local.pack(&tips, &exclude, progress)? else {
        return Ok(None);
    };
    if pack.len() as u64 > MAX_PACK_BYTES {
        return Err(HelperError::Protocol(format!(
            "the pack is {} bytes; a push may upload at most {MAX_PACK_BYTES}",
            pack.len()
        )));
    }
    let (id, pack_ref, bundle) = LpkWriter::from_parts(pack, idx)
        .map_err(|err| HelperError::store("build the pack bundle", err))?;
    if progress {
        eprintln!(
            "loams: uploading pack {id} ({} objects, {} bytes)",
            pack_ref.object_count,
            bundle.len()
        );
    }
    blobs
        .put(&id, bundle)
        .await
        .map_err(|err| HelperError::store(format!("upload pack {id}"), err))?;
    Ok(Some(pack_ref))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_push_lines() {
        assert_eq!(
            PushSpec::parse("refs/heads/a:refs/heads/b").expect("parse"),
            PushSpec {
                force: false,
                src: Some("refs/heads/a".into()),
                dst: "refs/heads/b".into()
            }
        );
        assert_eq!(
            PushSpec::parse("+HEAD:refs/heads/b").expect("parse"),
            PushSpec {
                force: true,
                src: Some("HEAD".into()),
                dst: "refs/heads/b".into()
            }
        );
        assert_eq!(
            PushSpec::parse(":refs/heads/gone").expect("parse").src,
            None
        );
        assert!(PushSpec::parse("refs/heads/a").is_err());
        assert!(PushSpec::parse("refs/heads/a:").is_err());
    }

    #[test]
    fn keys_bind_principal_pack_and_sorted_updates() {
        let oid = |n: u8| ObjectId::try_from(&[n; 20][..]).expect("oid");
        let update = |name: &str, n: u8| RefUpdate {
            name: RefName::parse(name).expect("name"),
            expect: Expect::Absent,
            new: Some(oid(n)),
        };
        let alice = Principal::parse("alice").expect("principal");
        let bob = Principal::parse("bob").expect("principal");
        let ab = [update("refs/heads/a", 1), update("refs/heads/b", 2)];
        let ba = [update("refs/heads/b", 2), update("refs/heads/a", 1)];
        let (k1, d1) = derive_key(&alice, Some("aa"), &ab);
        let (k2, d2) = derive_key(&alice, Some("aa"), &ba);
        assert_eq!((k1.clone(), d1), (k2, d2), "order does not matter");
        let (k3, d3) = derive_key(&bob, Some("aa"), &ab);
        assert_ne!(k1, k3, "the principal is in the key");
        assert_eq!(d1, d3, "but not in the request digest");
        let (k4, _) = derive_key(&alice, Some("bb"), &ab);
        assert_ne!(k1, k4, "the pack is in the key");
    }
}
