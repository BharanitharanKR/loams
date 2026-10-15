//! `BlobStore` and pack bundles (§36 §5.3, D390; plan GT1 Task 6, Ruling 1).

use std::io::{self, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use loams_fs::{Fs, FsPath, MemFs};
use loams_git::blob::{DEFAULT_MEMORY_SPILL_BYTES, read_idx, read_pack};
use loams_git::limits::MULTIPART_THRESHOLD;
use loams_git::{
    BlobError, BlobId, BlobStore, LpkWriter, NamespaceId, ObjectId, Put, RepoId, RepoPaths,
    StoreBlobStore,
};
use loams_store::{Fault, FaultyStore, Op, Store};
use object_store::memory::InMemory;

const MIB: usize = 1024 * 1024;

fn paths() -> RepoPaths {
    RepoPaths::new(
        NamespaceId(7),
        &RepoId::parse("r01j9z3m5q8x0000000000000000").expect("repo id"),
    )
}

/// A blob store over a fault-injecting in-memory store.
fn faulty() -> (Arc<FaultyStore>, StoreBlobStore) {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faulty.clone()).with_sorted_listing(true);
    (faulty, StoreBlobStore::new(store, paths()))
}

fn id(byte: u8) -> BlobId {
    BlobId::from_checksum(&ObjectId::try_from(&[byte; 20][..]).expect("oid"))
}

/// Deterministic bytes, so a slice can be checked without keeping a copy.
fn pattern(len: usize) -> Bytes {
    (0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>().into()
}

fn chunked(bytes: Bytes, chunk: usize) -> BoxStream<'static, io::Result<Bytes>> {
    let chunks: Vec<io::Result<Bytes>> = (0..bytes.len())
        .step_by(chunk)
        .map(|at| Ok(bytes.slice(at..bytes.len().min(at + chunk))))
        .collect();
    stream::iter(chunks).boxed()
}

/// A pack of `count` objects and its version 2 idx, with consistent counts
/// and checksums (the object data itself is filler).
fn synthetic_pack(count: u8) -> (Bytes, Bytes) {
    let trailer = [0x5a; 20];
    let mut pack = BytesMut::new();
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2u32.to_be_bytes());
    pack.extend_from_slice(&u32::from(count).to_be_bytes());
    pack.extend_from_slice(&[7; 64]);
    pack.extend_from_slice(&trailer);
    let names: Vec<[u8; 20]> = (1..=count).map(|i| [i; 20]).collect();
    let mut idx = BytesMut::new();
    idx.extend_from_slice(&[0xff, b't', b'O', b'c']);
    idx.extend_from_slice(&2u32.to_be_bytes());
    for byte in 0..=255u8 {
        let n = names.iter().filter(|name| name[0] <= byte).count() as u32;
        idx.extend_from_slice(&n.to_be_bytes());
    }
    for name in &names {
        idx.extend_from_slice(name);
    }
    for _ in &names {
        idx.extend_from_slice(&0u32.to_be_bytes());
    }
    for (i, _) in names.iter().enumerate() {
        idx.extend_from_slice(&(12 + i as u32).to_be_bytes());
    }
    idx.extend_from_slice(&trailer);
    idx.extend_from_slice(&[0x33; 20]);
    (pack.freeze(), idx.freeze())
}

#[tokio::test]
async fn put_is_create_only_and_idempotent() {
    let (faulty, blobs) = faulty();
    let id = id(1);
    let body = Bytes::from_static(b"pack bytes");
    assert_eq!(
        blobs.put(&id, body.clone()).await.expect("put"),
        Put::Created
    );
    assert_eq!(
        blobs.put(&id, body.clone()).await.expect("put"),
        Put::Existed
    );
    assert_eq!(faulty.calls(Op::PutCreate), 2, "both puts are create-only");
    assert_eq!(faulty.calls(Op::Put), 2, "and nothing else wrote");
    assert_eq!(
        blobs.head(&id).await.expect("head"),
        Some(body.len() as u64)
    );
    assert_eq!(
        blobs
            .get_range(&id, 0..body.len() as u64)
            .await
            .expect("read"),
        body
    );
    assert_eq!(blobs.head(&self::id(2)).await.expect("head"), None);
    assert_eq!(
        blobs.path(&id),
        format!("{}{id}.lpk", paths().pack_prefix()),
        "blobs are the repository's pack objects"
    );
}

#[tokio::test]
async fn put_existing_with_other_length_is_mismatch() {
    let (_, blobs) = faulty();
    let id = id(1);
    blobs
        .put(&id, Bytes::from_static(b"ten bytes!"))
        .await
        .expect("put");
    let err = blobs
        .put(&id, Bytes::from_static(b"eleven bytes"))
        .await
        .expect_err("another length under the same name");
    assert!(
        matches!(&err, BlobError::Mismatch { id: got, stored: 10, offered: 12 } if *got == id),
        "{err:?}"
    );
    assert_eq!(blobs.head(&id).await.expect("head"), Some(10), "unchanged");
}

#[tokio::test]
async fn lost_ack_put_returns_existed_on_retry() {
    let (faulty, blobs) = faulty();
    let body = Bytes::from_static(b"pack bytes");

    // The first PUT is applied and its answer lost; the retry finds the name.
    faulty.inject(Op::PutCreate, Fault::ErrorAfterApply);
    assert_eq!(
        blobs.put(&id(1), body.clone()).await.expect("put"),
        Put::Existed
    );
    assert_eq!(faulty.calls(Op::PutCreate), 2);
    assert_eq!(blobs.head(&id(1)).await.expect("head"), Some(10));

    // A PUT that failed before it was applied is simply resent.
    faulty.inject(Op::PutCreate, Fault::Error);
    assert_eq!(
        blobs.put(&id(2), body.clone()).await.expect("put"),
        Put::Created
    );

    // Three retries, then the error.
    for _ in 0..4 {
        faulty.inject(Op::PutCreate, Fault::Error);
    }
    let before = faulty.calls(Op::PutCreate);
    let err = blobs
        .put(&id(3), body)
        .await
        .expect_err("every attempt fails");
    assert!(matches!(err, BlobError::Store(_)), "{err:?}");
    assert_eq!(faulty.calls(Op::PutCreate) - before, 4);
    assert_eq!(blobs.head(&id(3)).await.expect("head"), None);
}

#[tokio::test]
async fn get_range_reads_idx_section() {
    let (faulty, blobs) = faulty();
    let (pack, idx) = synthetic_pack(3);
    let (id, pack_ref, bundle) = LpkWriter::from_parts(pack.clone(), idx.clone()).expect("bundle");
    assert_eq!(id.as_str(), "5a".repeat(20));
    assert_eq!(pack_ref.checksum, id.as_str());
    assert_eq!(pack_ref.object_count, 3);
    assert_eq!(pack_ref.pack_len, pack.len() as u64);
    assert_eq!(pack_ref.idx_offset, pack.len() as u64);
    assert_eq!(pack_ref.idx_len, idx.len() as u64);
    assert_eq!(blobs.put(&id, bundle).await.expect("put"), Put::Created);

    let gets = faulty.calls(Op::Get);
    assert_eq!(read_idx(&blobs, &pack_ref).await.expect("idx"), idx);
    assert_eq!(faulty.calls(Op::Get) - gets, 1, "the idx is one range read");
    assert_eq!(read_pack(&blobs, &pack_ref).await.expect("pack"), pack);

    // An idx that does not belong to the pack is refused.
    let (_, other_idx) = synthetic_pack(4);
    assert!(matches!(
        LpkWriter::from_parts(pack.clone(), other_idx),
        Err(BlobError::InvalidPack(_))
    ));
    let mut foreign = BytesMut::from(&idx[..]);
    let at = foreign.len() - 40;
    foreign[at] ^= 1;
    assert!(matches!(
        LpkWriter::from_parts(pack, foreign.freeze()),
        Err(BlobError::InvalidPack(_))
    ));
}

#[tokio::test]
async fn range_past_end_is_out_of_range() {
    let (faulty, blobs) = faulty();
    let id = id(1);
    blobs.put(&id, pattern(100)).await.expect("put");

    assert_eq!(
        blobs.get_range(&id, 90..100).await.expect("tail"),
        pattern(100).slice(90..100)
    );
    assert_eq!(
        blobs.get_range(&id, 100..100).await.expect("empty"),
        Bytes::new()
    );
    // With no retry, an out-of-range read is one GET, or a GET and a HEAD
    // when the store refuses a range that starts past the end; an empty
    // range is a HEAD.
    let gets = faulty.calls(Op::Get);
    for range in [90..101, 100..110, 150..160, 101..101] {
        let err = blobs
            .get_range(&id, range.clone())
            .await
            .expect_err("outside the blob");
        assert!(
            matches!(&err, BlobError::OutOfRange { range: got, len: 100, .. } if *got == range),
            "{range:?}: {err:?}"
        );
    }
    assert_eq!(
        faulty.calls(Op::Get) - gets,
        1 + 2 + 2 + 1,
        "a range error is not retried"
    );
    let gets = faulty.calls(Op::Get);
    assert!(matches!(
        blobs.get_range(&self::id(2), 0..1).await,
        Err(BlobError::NotFound { .. })
    ));
    assert_eq!(faulty.calls(Op::Get) - gets, 1, "a missing blob is one GET");
}

/// Runs git in `dir`; `None` when git is not installed.
fn git(dir: &Path, args: &[&str], stdin: Option<&[u8]>) -> Option<Vec<u8>> {
    let mut child = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .ok()?;
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(input)
            .expect("write stdin");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("git runs");
    assert!(out.status.success(), "git {args:?} failed");
    Some(out.stdout)
}

#[tokio::test]
async fn lpk_from_git_pack_round_trips() {
    let dir = tempfile::Builder::new()
        .prefix("blob-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let path = dir.path();
    if git(path, &["init", "-q", "."], None).is_none() {
        eprintln!("git not found: lpk_from_git_pack_round_trips skipped");
        return;
    }
    for (i, text) in ["one\n", "one\ntwo\n", "one\ntwo\nthree\n"]
        .iter()
        .enumerate()
    {
        std::fs::write(path.join("a.txt"), text).expect("write");
        std::fs::write(path.join(format!("f{i}.txt")), text.repeat(50)).expect("write");
        git(path, &["add", "."], None);
        git(
            path,
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.com",
                "commit",
                "-q",
                "-m",
                text,
            ],
            None,
        );
    }
    let head = git(path, &["rev-parse", "HEAD"], None).expect("rev-parse");
    let printed = git(path, &["pack-objects", "--revs", "out"], Some(&head)).expect("pack");
    let printed = String::from_utf8(printed).expect("utf-8");
    let printed = printed.trim();
    let pack = std::fs::read(path.join(format!("out-{printed}.pack"))).expect("pack");
    let idx = std::fs::read(path.join(format!("out-{printed}.idx"))).expect("idx");

    let (_, blobs) = faulty();
    let (id, pack_ref, bundle) =
        LpkWriter::from_parts(pack.clone().into(), idx.clone().into()).expect("bundle");
    assert_eq!(id.as_str(), printed, "named by the pack checksum");
    assert_eq!(blobs.put(&id, bundle).await.expect("put"), Put::Created);
    let read_back_pack = read_pack(&blobs, &pack_ref).await.expect("pack");
    let read_back_idx = read_idx(&blobs, &pack_ref).await.expect("idx");
    assert_eq!(read_back_pack, pack);
    assert_eq!(read_back_idx, idx);

    // git accepts the sections as a pack and its idx.
    let out = path.join("verify");
    std::fs::create_dir(&out).expect("mkdir");
    std::fs::write(out.join(format!("pack-{id}.pack")), &read_back_pack).expect("write");
    std::fs::write(out.join(format!("pack-{id}.idx")), &read_back_idx).expect("write");
    let verified = git(
        path,
        &["verify-pack", "-v", &format!("verify/pack-{id}.idx")],
        None,
    )
    .expect("verify-pack");
    let verified = String::from_utf8(verified).expect("utf-8");
    assert!(
        verified.contains(&format!("verify/pack-{id}.pack: ok")),
        "{verified}"
    );
    assert!(verified.contains("non delta: "), "{verified}");
    assert_eq!(
        pack_ref.object_count,
        verified
            .lines()
            .filter(|line| line.len() > 40 && line.as_bytes()[40] == b' ')
            .count() as u64,
        "the PackRef counts every object"
    );
}

/// Exactly the multipart threshold is one create-only PUT; one byte more is
/// a multipart upload, from a stream spilled to the configured `Fs`.
#[tokio::test]
async fn large_blob_uses_multipart() {
    let (faulty, blobs) = faulty();
    let spill: Arc<MemFs> = Arc::new(MemFs::new(u64::MAX));
    let spill_dir = FsPath::new("spill").expect("path");
    let blobs = blobs.with_spill(spill.clone(), spill_dir.clone());
    let threshold = MULTIPART_THRESHOLD as usize;

    let at_limit = pattern(threshold);
    assert_eq!(
        blobs.put(&id(1), at_limit).await.expect("put"),
        Put::Created
    );
    assert_eq!(faulty.calls(Op::PutCreate), 1);
    assert_eq!(faulty.calls(Op::PutMultipart), 0);

    let large = pattern(threshold + 1);
    let len = large.len() as u64;
    let put = blobs
        .put_stream(&id(2), chunked(large.clone(), MIB), len)
        .await
        .expect("put_stream");
    assert_eq!(put, Put::Created);
    assert_eq!(faulty.calls(Op::PutCreate), 1, "no create-only PUT");
    assert_eq!(faulty.calls(Op::PutMultipart), 1);
    assert_eq!(blobs.head(&id(2)).await.expect("head"), Some(len));
    for range in [0..MIB as u64, len - 10..len, 40_000_000..40_000_100] {
        let (start, end) = (range.start as usize, range.end as usize);
        assert_eq!(
            blobs.get_range(&id(2), range).await.expect("read"),
            large.slice(start..end)
        );
    }
    assert!(
        spill
            .list(&spill_dir, None, 10)
            .await
            .expect("list")
            .is_empty(),
        "the spill directory is removed"
    );

    // An existing blob is not uploaded again.
    let again = blobs
        .put_stream(&id(2), chunked(large.clone(), MIB), len)
        .await
        .expect("put_stream");
    assert_eq!(again, Put::Existed);
    assert_eq!(faulty.calls(Op::PutMultipart), 1);

    // A lost completion is retried from the spill file.
    faulty.inject(Op::PutMultipart, Fault::ErrorAfterApply);
    let put = blobs
        .put_stream(&id(3), chunked(large.clone(), 3 * MIB), len)
        .await
        .expect("put_stream");
    assert_eq!(put, Put::Created);
    assert_eq!(faulty.calls(Op::PutMultipart), 3);
    assert_eq!(
        blobs.get_range(&id(3), len - 5..len).await.expect("read"),
        large.slice(large.len() - 5..)
    );

    // `put` of large bytes is a multipart upload too.
    assert_eq!(
        blobs.put(&id(4), large.clone()).await.expect("put"),
        Put::Created
    );
    assert_eq!(faulty.calls(Op::PutMultipart), 4);
    assert_eq!(faulty.calls(Op::PutCreate), 1);

    // A body shorter than declared writes nothing.
    let short = large.slice(..large.len() - 1);
    let err = blobs
        .put_stream(&id(5), chunked(short, MIB), len)
        .await
        .expect_err("short body");
    assert!(
        matches!(err, BlobError::LengthMismatch { declared, got, .. } if declared == len && got == len - 1),
        "{err:?}"
    );
    assert_eq!(blobs.head(&id(5)).await.expect("head"), None);
}

/// Ruling 1: until Q385 confirms that RustFS completes a multipart upload
/// atomically, a store marked so refuses blobs over 64 MiB.
#[tokio::test]
async fn rustfs_large_blob_refused_until_q385() {
    let (faulty, blobs) = faulty();
    let blobs = blobs.with_atomic_multipart(false);
    let threshold = MULTIPART_THRESHOLD as usize;

    assert_eq!(
        blobs
            .put(&id(1), pattern(threshold))
            .await
            .expect("at the limit"),
        Put::Created
    );
    let err = blobs
        .put(&id(2), pattern(threshold + 1))
        .await
        .expect_err("one byte past it");
    assert!(
        matches!(err, BlobError::TooLarge { len, max, .. } if len == MULTIPART_THRESHOLD + 1 && max == MULTIPART_THRESHOLD),
        "{err:?}"
    );
    // Refused before the body is read.
    let unread: BoxStream<'static, io::Result<Bytes>> =
        stream::once(async { panic!("the body must not be read") }).boxed();
    let err = blobs
        .put_stream(&id(3), unread, MULTIPART_THRESHOLD + 1)
        .await
        .expect_err("refused");
    assert!(matches!(err, BlobError::TooLarge { .. }), "{err:?}");
    assert_eq!(faulty.calls(Op::PutMultipart), 0);
    assert_eq!(blobs.head(&id(2)).await.expect("head"), None);
    assert_eq!(blobs.head(&id(3)).await.expect("head"), None);
}

/// The default spill is memory with a bound: a streamed body larger than it
/// is refused before it is read, instead of filling the process's memory.
#[tokio::test]
async fn default_spill_is_bounded() {
    let (faulty, blobs) = faulty();
    let unread: BoxStream<'static, io::Result<Bytes>> =
        stream::once(async { panic!("the body must not be read") }).boxed();
    let err = blobs
        .put_stream(&id(1), unread, DEFAULT_MEMORY_SPILL_BYTES + 1)
        .await
        .expect_err("over the default spill");
    assert!(
        matches!(err, BlobError::Spill(loams_fs::FsError::TooLarge(_))),
        "{err:?}"
    );
    assert_eq!(faulty.calls(Op::PutMultipart), 0);
}

/// Q385, run by hand against RustFS: a multipart upload of a blob over
/// 64 MiB while a reader polls it. The reader must see either no object or
/// the whole object, never a part.
///
/// ```text
/// LOAMS_GIT_Q385_S3=http://127.0.0.1:9000/<bucket> LOAMS_GIT_Q385_KEY=<key> \
///   LOAMS_GIT_Q385_SECRET=<secret> cargo test -p loams-git --test blob -- --ignored q385
/// ```
#[tokio::test]
#[ignore = "needs a RustFS endpoint (LOAMS_GIT_Q385_S3)"]
async fn q385_rustfs_multipart_is_atomic() {
    let Ok(endpoint) = std::env::var("LOAMS_GIT_Q385_S3") else {
        eprintln!("LOAMS_GIT_Q385_S3 not set: skipped");
        return;
    };
    let (base, bucket) = endpoint.rsplit_once('/').expect("<endpoint>/<bucket>");
    let key = std::env::var("LOAMS_GIT_Q385_KEY").expect("LOAMS_GIT_Q385_KEY");
    let secret = std::env::var("LOAMS_GIT_Q385_SECRET").expect("LOAMS_GIT_Q385_SECRET");
    let store = Store::from_url(
        &format!("s3://{bucket}"),
        [
            ("aws_endpoint", base.to_string()),
            ("aws_access_key_id", key),
            ("aws_secret_access_key", secret),
            ("aws_region", "us-east-1".to_string()),
            ("aws_allow_http", "true".to_string()),
            ("aws_virtual_hosted_style_request", "false".to_string()),
        ],
    )
    .expect("store");
    let tries: usize = std::env::var("LOAMS_GIT_Q385_TRIES")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(200);
    let len = MULTIPART_THRESHOLD as usize + 16 * MIB;
    let body = pattern(len);
    let mut partial = 0usize;
    let mut whole = 0usize;
    let mut absent = 0usize;
    let mut rounds = 0usize;
    while absent + whole + partial < tries {
        rounds += 1;
        let blobs = StoreBlobStore::new(store.clone(), paths());
        let id = BlobId::parse(&format!("{:040x}", rounds + 0xabc0)).expect("id");
        let path = blobs.path(&id);
        let upload = {
            let (blobs, id, body) = (blobs.clone(), id.clone(), body.clone());
            tokio::spawn(async move { blobs.put(&id, body).await })
        };
        while !upload.is_finished() {
            match store.get(&path).await {
                Ok((bytes, _)) if bytes == body => whole += 1,
                Ok((bytes, _)) => {
                    eprintln!("partial object: {} of {len} bytes", bytes.len());
                    partial += 1;
                }
                Err(loams_store::StoreError::NotFound { .. }) => absent += 1,
                Err(err) => panic!("poll: {err}"),
            }
        }
        upload.await.expect("join").expect("upload");
        store.delete(&path).await.expect("delete");
    }
    eprintln!(
        "Q385: {rounds} uploads of {len} bytes, {} polls: {absent} absent, {whole} whole, {partial} partial",
        absent + whole + partial
    );
    assert_eq!(partial, 0, "a reader saw part of a multipart upload");
}
