//! `git-wal-bench`: drives `BucketRefLog` directly (no git processes) and
//! prints one JSON line with commits per second, commit and push latency,
//! the mean group size, and store PUTs and GETs per commit (design §36
//! §4.4; plan GT1 Task 11, the morning version's Task 13).
//!
//! ```text
//! git-wal-bench --store <url> [--opt key=value]... --repos <n> --pushers <p>
//!   --duration <s> --pack-bytes <b> [--put-delay-ms <d>] [--sorted-listing]
//! ```
//!
//! `--put-delay-ms` adds that latency to every segment append and pack
//! upload, the model of a remote store's PUT. Pusher `j` updates its own
//! branch of repository `j % repos`, uploading a pack of `--pack-bytes`
//! first when that is not 0.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use loams_git::format::pb;
use loams_git::wal::{Appended, Hint, WalError, WalStore};
use loams_git::{
    BlobId, BlobStore, BucketRefLog, Expect, IdemKey, NamespaceId, ObjectId, Principal, RefLog,
    RefName, RefTxn, RefUpdate, RepoId, RepoPaths, Seq, SequencerConfig, StoreBlobStore,
    StoreWalStore, SystemClock, WalBatch, WalSegment, create_repo,
};
use loams_store::{FaultyStore, Op, Store};
use rand::{Rng, RngCore};

#[derive(Debug, Parser)]
#[command(name = "git-wal-bench", about = "Measures the Loams Git bucket WAL")]
struct Args {
    /// The store URL (`memory:///`, `file:///dir`, `s3://bucket/prefix`, …).
    #[arg(long)]
    store: String,
    /// A backend option, `key=value` (for example `aws_endpoint=http://…`).
    #[arg(long = "opt")]
    opts: Vec<String>,
    /// The store lists keys in order (memory, GCS, Azure, AWS S3).
    #[arg(long)]
    sorted_listing: bool,
    /// Repositories.
    #[arg(long, default_value_t = 1)]
    repos: usize,
    /// Concurrent pushers.
    #[arg(long, default_value_t = 32)]
    pushers: usize,
    /// Seconds to run.
    #[arg(long, default_value_t = 30)]
    duration: u64,
    /// Bytes of the pack each push uploads (0: none).
    #[arg(long, default_value_t = 0)]
    pack_bytes: usize,
    /// Latency added to every segment append and pack upload, in ms.
    #[arg(long, default_value_t = 0)]
    put_delay_ms: u64,
}

/// A WAL whose appends take `delay` longer.
#[derive(Debug)]
struct Delayed {
    inner: StoreWalStore,
    delay: Duration,
}

#[async_trait]
impl WalStore for Delayed {
    async fn append(&self, expect_seq: Seq, batch: &WalBatch) -> Result<Appended, WalError> {
        tokio::time::sleep(self.delay).await;
        self.inner.append(expect_seq, batch).await
    }

    async fn read_from(&self, from: Seq, max: usize) -> Result<Vec<WalSegment>, WalError> {
        self.inner.read_from(from, max).await
    }

    async fn hint(&self) -> Result<Hint, WalError> {
        self.inner.hint().await
    }

    async fn publish_hint(&self, hint: &Hint) -> Result<(), WalError> {
        self.inner.publish_hint(hint).await
    }

    async fn put_checkpoint(&self, checkpoint: &pb::Checkpoint) -> Result<(), WalError> {
        self.inner.put_checkpoint(checkpoint).await
    }

    async fn latest_checkpoint(
        &self,
        at_or_below: Seq,
    ) -> Result<Option<pb::Checkpoint>, WalError> {
        self.inner.latest_checkpoint(at_or_below).await
    }
}

fn random_hex(bytes: usize) -> String {
    let mut raw = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut raw);
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

fn random_oid() -> ObjectId {
    let mut raw = [0u8; 20];
    rand::rng().fill_bytes(&mut raw);
    ObjectId::try_from(&raw[..]).expect("20 bytes")
}

/// The `q` quantile of sorted `values`, in ms.
fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let at = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[at]
}

/// One pusher's loop; returns (commit ms, push ms) per acknowledged push
/// and the count of refused or failed ones.
async fn pusher(
    id: usize,
    log: BucketRefLog,
    blobs: Arc<StoreBlobStore>,
    deadline: Instant,
    pack_bytes: usize,
    delay: Duration,
) -> (Vec<(f64, f64)>, u64) {
    let name = RefName::parse(&format!("refs/heads/p{id}")).expect("ref");
    let principal = Principal::parse(&format!("pusher-{id}")).expect("principal");
    let mut current: Option<ObjectId> = None;
    let mut done = Vec::new();
    let mut failed = 0;
    while Instant::now() < deadline {
        let pushed = Instant::now();
        let mut packs = Vec::new();
        if pack_bytes > 0 {
            let mut body = vec![0u8; pack_bytes];
            rand::rng().fill_bytes(&mut body);
            let id = BlobId::parse(&random_hex(20)).expect("blob id");
            tokio::time::sleep(delay).await;
            if let Err(err) = blobs.put(&id, Bytes::from(body)).await {
                eprintln!("pusher {id}: uploading pack {}: {err}", id.as_str());
                failed += 1;
                continue;
            }
            packs.push(pb::PackRef {
                checksum: id.as_str().to_string(),
                object_count: 1,
                pack_len: pack_bytes as u64,
                idx_offset: pack_bytes as u64,
                idx_len: 0,
                has_bitmap: false,
            });
        }
        let new = random_oid();
        let mut digest = [0u8; 32];
        rand::rng().fill_bytes(&mut digest);
        let txn = RefTxn {
            key: IdemKey::parse(&format!("b{}", random_hex(12))).expect("key"),
            request_digest: digest,
            updates: vec![RefUpdate {
                name: name.clone(),
                expect: current.map_or(Expect::Absent, Expect::Oid),
                new: Some(new),
            }],
            packs,
            principal: principal.clone(),
            scope: None,
            trace: None,
            push_options: Vec::new(),
        };
        let committing = Instant::now();
        match log.commit(txn).await {
            Ok(_) => {
                current = Some(new);
                done.push((
                    committing.elapsed().as_secs_f64() * 1e3,
                    pushed.elapsed().as_secs_f64() * 1e3,
                ));
            }
            Err(err) => {
                failed += 1;
                eprintln!("pusher {id}: {err}");
            }
        }
    }
    (done, failed)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let opts: Vec<(String, String)> = args
        .opts
        .iter()
        .map(|opt| {
            let (key, value) = opt.split_once('=').expect("--opt takes key=value");
            (key.to_string(), value.to_string())
        })
        .collect();
    let base = Store::from_url(&args.store, opts).expect("open the store");
    let counted = Arc::new(FaultyStore::new(Arc::clone(base.inner())));
    let store = Store::new(counted.clone()).with_sorted_listing(args.sorted_listing);
    let delay = Duration::from_millis(args.put_delay_ms);
    let run = random_hex(4);
    let ns = NamespaceId(rand::rng().random_range(1..u64::from(u32::MAX)));

    let mut logs = Vec::new();
    let mut blob_stores = Vec::new();
    for i in 0..args.repos {
        let repo = RepoId::parse(&format!("rbench{run}{i}")).expect("repo id");
        let paths = RepoPaths::new(ns, &repo);
        let wal = Arc::new(Delayed {
            inner: StoreWalStore::new(store.clone(), paths.clone()),
            delay,
        });
        create_repo(wal.as_ref(), pb::ObjectFormat::Sha1)
            .await
            .expect("create a repository");
        let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths));
        let config = SequencerConfig::new(format!("bench/{ns}"), ns, repo);
        let log = BucketRefLog::open(wal, blobs.clone(), config, Arc::new(SystemClock))
            .await
            .expect("open a repository");
        logs.push(log);
        blob_stores.push(blobs);
    }

    let (puts, gets) = (counted.calls(Op::Put), counted.calls(Op::Get));
    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration);
    let tasks: Vec<_> = (0..args.pushers)
        .map(|j| {
            let repo = j % args.repos;
            tokio::spawn(pusher(
                j,
                logs[repo].clone(),
                blob_stores[repo].clone(),
                deadline,
                args.pack_bytes,
                delay,
            ))
        })
        .collect();
    let mut commit_ms = Vec::new();
    let mut push_ms = Vec::new();
    let mut failed = 0;
    for task in tasks {
        let (done, errors) = task.await.expect("pusher");
        failed += errors;
        for (commit, push) in done {
            commit_ms.push(commit);
            push_ms.push(push);
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let (puts, gets) = (counted.calls(Op::Put) - puts, counted.calls(Op::Get) - gets);
    let segments: u64 = logs.iter().map(|log| log.seq().0).sum();
    for log in logs {
        log.shutdown().await;
    }
    commit_ms.sort_by(f64::total_cmp);
    push_ms.sort_by(f64::total_cmp);
    let commits = commit_ms.len() as f64;
    let per_commit = |n: u64| {
        if commits > 0.0 {
            n as f64 / commits
        } else {
            0.0
        }
    };
    let line = serde_json::json!({
        "store": args.store,
        "repos": args.repos,
        "pushers": args.pushers,
        "duration_s": args.duration,
        "pack_bytes": args.pack_bytes,
        "put_delay_ms": args.put_delay_ms,
        "commits": commit_ms.len(),
        "failed": failed,
        "commits_per_s": commits / elapsed,
        "commit_p50_ms": quantile(&commit_ms, 0.5),
        "commit_p99_ms": quantile(&commit_ms, 0.99),
        "push_p50_ms": quantile(&push_ms, 0.5),
        "push_p99_ms": quantile(&push_ms, 0.99),
        "segments": segments,
        "mean_group": if segments > 0 { commits / segments as f64 } else { 0.0 },
        "puts_per_commit": per_commit(puts),
        "gets_per_commit": per_commit(gets),
    });
    println!("{line}");
}
