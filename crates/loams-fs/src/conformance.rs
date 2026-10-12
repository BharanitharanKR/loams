//! The semantics every [`Fs`] backend gives (§36 §17.1), held by the same
//! cases on each.
//!
//! [`fs_conformance!`](crate::fs_conformance) expands one `#[tokio::test]`
//! per case for a factory expression that yields `(Arc<dyn Fs>, guard)`,
//! evaluated once per case; the guard (a temporary directory, say) lives
//! until the case ends:
//!
//! ```ignore
//! loams_fs::fs_conformance!(mem, (std::sync::Arc::new(loams_fs::MemFs::new(1 << 20)) as _, ()));
//! ```
//!
//! [`run`] runs every case on one backend, each under its own directory.
//! Cases a backend's [`FsCaps`](crate::FsCaps) rule out check that the
//! operation answers `Unsupported` instead.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::join_all;

use crate::{Fs, FsError, FsPath, FsPathError, MAX_PATH_BYTES, SpillDir, WriteMode};

/// Expands one `#[tokio::test]` per conformance case into `mod $backend`.
#[macro_export]
macro_rules! fs_conformance {
    ($backend:ident, $factory:expr) => {
        mod $backend {
            #[allow(unused_imports)]
            use super::*;
            $crate::fs_conformance!(@cases $factory;
                read_after_write,
                create_new_race_one_winner,
                range_read_boundaries,
                list_pagination,
                rename_is_atomic,
                append_extends_and_creates,
                max_file_is_enforced,
                path_rejects_dot_segments_and_over_1024_bytes,
                spill_dir_removed_on_drop
            );
        }
    };
    (@cases $factory:expr; $($case:ident),* $(,)?) => {
        $(
            #[::tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() {
                let (fs, _guard): (::std::sync::Arc<dyn $crate::Fs>, _) = $factory;
                $crate::conformance::$case(fs, $crate::FsPath::root()).await;
            }
        )*
    };
}

/// Runs every case on `fs`, each under `conformance/<case>/`.
pub async fn run(fs: Arc<dyn Fs>) {
    let dir = |case: &str| path(&format!("conformance/{case}"));
    read_after_write(fs.clone(), dir("read_after_write")).await;
    create_new_race_one_winner(fs.clone(), dir("create_new_race_one_winner")).await;
    range_read_boundaries(fs.clone(), dir("range_read_boundaries")).await;
    list_pagination(fs.clone(), dir("list_pagination")).await;
    rename_is_atomic(fs.clone(), dir("rename_is_atomic")).await;
    append_extends_and_creates(fs.clone(), dir("append_extends_and_creates")).await;
    max_file_is_enforced(fs.clone(), dir("max_file_is_enforced")).await;
    path_rejects_dot_segments_and_over_1024_bytes(
        fs.clone(),
        dir("path_rejects_dot_segments_and_over_1024_bytes"),
    )
    .await;
    spill_dir_removed_on_drop(fs, dir("spill_dir_removed_on_drop")).await;
}

fn path(s: &str) -> FsPath {
    FsPath::new(s).expect("conformance paths are valid")
}

fn under(dir: &FsPath, child: &str) -> FsPath {
    dir.join(child).expect("conformance paths are valid")
}

/// A write is visible to the next read, an overwrite replaces, and a removed
/// file is gone (removing it again is `Ok`).
pub async fn read_after_write(fs: Arc<dyn Fs>, dir: FsPath) {
    let file = under(&dir, "a/b/file");
    assert!(matches!(fs.read(&file).await, Err(FsError::NotFound(p)) if p == file));
    assert_eq!(fs.stat(&file).await.expect("stat"), None);

    fs.write(&file, Bytes::from_static(b"one"), WriteMode::Overwrite)
        .await
        .expect("first write");
    assert_eq!(
        fs.read(&file).await.expect("read"),
        Bytes::from_static(b"one")
    );
    let meta = fs.stat(&file).await.expect("stat").expect("a file");
    assert_eq!(meta.len, 3);
    let now = crate::now_unix_ms();
    assert!(
        (now - 60_000..=now + 60_000).contains(&meta.modified_unix_ms),
        "{meta:?}"
    );

    fs.write(&file, Bytes::from_static(b"second"), WriteMode::Overwrite)
        .await
        .expect("overwrite");
    assert_eq!(
        fs.read(&file).await.expect("read"),
        Bytes::from_static(b"second")
    );
    assert_eq!(fs.stat(&file).await.expect("stat").expect("a file").len, 6);

    let empty = under(&dir, "empty");
    fs.write(&empty, Bytes::new(), WriteMode::CreateNew)
        .await
        .expect("empty file");
    assert_eq!(fs.read(&empty).await.expect("read"), Bytes::new());

    fs.remove(&file).await.expect("remove");
    assert!(matches!(fs.read(&file).await, Err(FsError::NotFound(_))));
    assert_eq!(fs.stat(&file).await.expect("stat"), None);
    fs.remove(&file)
        .await
        .expect("removing a missing file is Ok");

    // A directory is not a file.
    assert_eq!(fs.stat(&under(&dir, "a")).await.expect("stat"), None);
    assert!(matches!(
        fs.read(&FsPath::root()).await,
        Err(FsError::NotFound(_) | FsError::Unsupported(_))
    ));
}

/// Of many concurrent `CreateNew` writes of one path, exactly one wins, and
/// the file holds the winner's bytes in full.
pub async fn create_new_race_one_winner(fs: Arc<dyn Fs>, dir: FsPath) {
    let file = under(&dir, "contended");
    let bodies: Vec<Bytes> = (0..16u8).map(|i| Bytes::from(vec![i; 4096])).collect();
    let results = join_all(
        bodies
            .iter()
            .map(|body| fs.write(&file, body.clone(), WriteMode::CreateNew)),
    )
    .await;
    let winners: Vec<usize> = results
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.is_ok().then_some(i))
        .collect();
    assert_eq!(winners.len(), 1, "{results:?}");
    for result in &results {
        assert!(
            matches!(result, Ok(()))
                || matches!(result, Err(FsError::AlreadyExists(p)) if p == &file),
            "{result:?}"
        );
    }
    assert_eq!(fs.read(&file).await.expect("read"), bodies[winners[0]]);

    // A later CreateNew still loses; an Overwrite still wins.
    assert!(matches!(
        fs.write(&file, Bytes::from_static(b"late"), WriteMode::CreateNew)
            .await,
        Err(FsError::AlreadyExists(_))
    ));
    fs.write(&file, Bytes::from_static(b"late"), WriteMode::Overwrite)
        .await
        .expect("overwrite");
    assert_eq!(
        fs.read(&file).await.expect("read"),
        Bytes::from_static(b"late")
    );
}

/// Ranges at the edges of a file read exactly; a range past the end or an
/// inverted one is an error, never a short read.
pub async fn range_read_boundaries(fs: Arc<dyn Fs>, dir: FsPath) {
    let file = under(&dir, "ranged");
    let data: Bytes = (0..1000u32)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>()
        .into();
    fs.write(&file, data.clone(), WriteMode::Overwrite)
        .await
        .expect("write");
    for range in [
        0..0,
        0..1,
        0..1000,
        999..1000,
        1000..1000,
        250..750,
        500..500,
    ] {
        let got = fs.read_range(&file, range.clone()).await.expect("in range");
        let (start, end) = (range.start as usize, range.end as usize);
        assert_eq!(got, data.slice(start..end), "{range:?}");
    }
    #[allow(clippy::reversed_empty_ranges)]
    let inverted = 10..5;
    for range in [500..1001, 1000..1001, 1001..1002, inverted] {
        assert!(
            fs.read_range(&file, range.clone()).await.is_err(),
            "{range:?} should fail"
        );
    }
    assert!(matches!(
        fs.read_range(&under(&dir, "missing"), 0..1).await,
        Err(FsError::NotFound(_))
    ));
}

/// Listing is sorted, segment-based, strictly after `after`, resumable, and
/// holds files only.
pub async fn list_pagination(fs: Arc<dyn Fs>, dir: FsPath) {
    let names = [
        "d/k07",
        "d/k01",
        "d/sub/x",
        "d/k03",
        "d/k10",
        "d/k02",
        "d/sub/deeper/y",
        "d/k05",
        "d/k04",
        "d/k09",
        "d/k08",
        "d/k06",
    ];
    for name in names {
        fs.write(
            &under(&dir, name),
            Bytes::from_static(b"12"),
            WriteMode::Overwrite,
        )
        .await
        .expect("write");
    }
    // Siblings that share a string prefix but not a segment.
    fs.write(
        &under(&dir, "dx/k00"),
        Bytes::from_static(b"1"),
        WriteMode::Overwrite,
    )
    .await
    .expect("write");
    fs.write(
        &under(&dir, "d0"),
        Bytes::from_static(b"1"),
        WriteMode::Overwrite,
    )
    .await
    .expect("write");

    let listed_dir = under(&dir, "d");
    let mut expected: Vec<FsPath> = names.iter().map(|n| under(&dir, n)).collect();
    expected.sort();

    for limit in [1, 3, 5, 12, 50] {
        let mut seen = Vec::new();
        let mut after: Option<FsPath> = None;
        loop {
            let page = fs
                .list(&listed_dir, after.as_ref(), limit)
                .await
                .expect("list");
            assert!(page.len() <= limit);
            if page.is_empty() {
                break;
            }
            for (p, meta) in &page {
                assert_eq!(meta.len, 2, "{p}");
            }
            let paths: Vec<FsPath> = page.into_iter().map(|(p, _)| p).collect();
            let mut sorted = paths.clone();
            sorted.sort();
            assert_eq!(paths, sorted, "page not sorted");
            after = paths.last().cloned();
            seen.extend(paths);
        }
        assert_eq!(seen, expected, "limit {limit}");
    }

    // `after` need not name a file; a missing directory lists nothing.
    let page = fs
        .list(&listed_dir, Some(&under(&dir, "d/k05x")), 2)
        .await
        .expect("list");
    let got: Vec<FsPath> = page.into_iter().map(|(p, _)| p).collect();
    assert_eq!(got, [under(&dir, "d/k06"), under(&dir, "d/k07")]);
    assert!(
        fs.list(&under(&dir, "none"), None, 10)
            .await
            .expect("list")
            .is_empty()
    );
    assert!(
        fs.list(&listed_dir, None, 0)
            .await
            .expect("list")
            .is_empty()
    );

    // The root lists everything, including these files.
    let all = fs
        .list(&FsPath::root(), None, usize::MAX)
        .await
        .expect("list");
    assert!(all.iter().any(|(p, _)| p == &under(&dir, "d0")));
}

/// Rename replaces its target atomically: concurrent readers of the target
/// see the old bytes or the new ones, whole, never a missing file. Backends
/// without atomic rename answer `Unsupported`.
pub async fn rename_is_atomic(fs: Arc<dyn Fs>, dir: FsPath) {
    let target = under(&dir, "target");
    let staged = under(&dir, "staging/next");
    let old = Bytes::from(vec![b'o'; 64 * 1024]);
    let new = Bytes::from(vec![b'n'; 64 * 1024]);
    fs.write(&target, old.clone(), WriteMode::Overwrite)
        .await
        .expect("write");
    fs.write(&staged, new.clone(), WriteMode::Overwrite)
        .await
        .expect("write");

    if !fs.caps().atomic_rename {
        assert!(matches!(
            fs.rename(&staged, &target).await,
            Err(FsError::Unsupported(_))
        ));
        return;
    }

    let readers = async {
        let mut reads = 0;
        for _ in 0..200 {
            let got = fs.read(&target).await.expect("the target never disappears");
            assert!(
                got == old || got == new,
                "a torn read of {} bytes",
                got.len()
            );
            reads += 1;
            tokio::task::yield_now().await;
        }
        reads
    };
    let rename = async {
        tokio::time::sleep(Duration::from_millis(2)).await;
        fs.rename(&staged, &target).await
    };
    let (reads, renamed) = tokio::join!(readers, rename);
    renamed.expect("rename");
    assert_eq!(reads, 200);
    assert_eq!(fs.read(&target).await.expect("read"), new);
    assert!(matches!(fs.read(&staged).await, Err(FsError::NotFound(_))));

    // Renaming a missing file is NotFound, and renaming creates parents.
    assert!(matches!(
        fs.rename(&staged, &target).await,
        Err(FsError::NotFound(p)) if p == staged
    ));
    let moved = under(&dir, "elsewhere/deep/moved");
    fs.rename(&target, &moved)
        .await
        .expect("rename to a new directory");
    assert_eq!(fs.read(&moved).await.expect("read"), new);
}

/// Append creates a missing file, extends an existing one and returns the
/// new length. Backends without append answer `Unsupported`.
pub async fn append_extends_and_creates(fs: Arc<dyn Fs>, dir: FsPath) {
    let log = under(&dir, "journal");
    if !fs.caps().append {
        assert!(matches!(
            fs.append(&log, Bytes::from_static(b"x")).await,
            Err(FsError::Unsupported(_))
        ));
        return;
    }
    assert_eq!(
        fs.append(&log, Bytes::from_static(b"abc"))
            .await
            .expect("append"),
        3
    );
    assert_eq!(
        fs.append(&log, Bytes::from_static(b"de"))
            .await
            .expect("append"),
        5
    );
    assert_eq!(fs.append(&log, Bytes::new()).await.expect("append"), 5);
    assert_eq!(
        fs.read(&log).await.expect("read"),
        Bytes::from_static(b"abcde")
    );
    assert_eq!(fs.stat(&log).await.expect("stat").expect("a file").len, 5);
}

/// A write or append past `max_file` is `TooLarge` and changes nothing.
/// Runs only where `max_file` is small enough to reach in a test.
pub async fn max_file_is_enforced(fs: Arc<dyn Fs>, dir: FsPath) {
    let max = fs.caps().max_file;
    if max > 64 * 1024 * 1024 {
        return;
    }
    let max = usize::try_from(max).expect("checked above");
    let file = under(&dir, "big");
    fs.write(&file, Bytes::from(vec![1u8; max]), WriteMode::Overwrite)
        .await
        .expect("a file of exactly max_file bytes");
    assert!(matches!(
        fs.write(&file, Bytes::from(vec![2u8; max + 1]), WriteMode::Overwrite).await,
        Err(FsError::TooLarge(n)) if n == max as u64 + 1
    ));
    assert_eq!(fs.read(&file).await.expect("read").len(), max);
    if fs.caps().append {
        assert!(matches!(
            fs.append(&file, Bytes::from_static(b"x")).await,
            Err(FsError::TooLarge(_))
        ));
        assert_eq!(
            fs.stat(&file).await.expect("stat").expect("a file").len,
            max as u64
        );
    }
}

/// Paths are relative, with no empty, `.` or `..` segments, and at most
/// 1,024 bytes; a valid path round-trips through a write.
pub async fn path_rejects_dot_segments_and_over_1024_bytes(fs: Arc<dyn Fs>, dir: FsPath) {
    for bad in [
        "", "/abs", "a//b", "a/", "./a", "a/./b", "a/..", "../x", "a\0b",
    ] {
        assert!(FsPath::new(bad).is_err(), "{bad:?} should be refused");
    }
    assert!(matches!(
        FsPath::new("a/../b"),
        Err(FsPathError::BadSegment(_))
    ));
    let longest = "x".repeat(MAX_PATH_BYTES);
    assert!(FsPath::new(longest.as_str()).is_ok());
    assert!(matches!(
        FsPath::new(format!("{longest}y")),
        Err(FsPathError::TooLong(1025))
    ));
    for good in [".hidden", "a.b/..c/d..", "spaces in name", "ünïcode"] {
        let p = under(&dir, good);
        fs.write(&p, Bytes::from_static(b"ok"), WriteMode::Overwrite)
            .await
            .unwrap_or_else(|e| panic!("{good:?}: {e}"));
        assert_eq!(fs.read(&p).await.expect("read"), Bytes::from_static(b"ok"));
    }
}

/// A spill directory's files are removed when it is dropped (in a spawned
/// task: poll until they are gone) or closed.
pub async fn spill_dir_removed_on_drop(fs: Arc<dyn Fs>, dir: FsPath) {
    let parent = under(&dir, "spill");
    let spill = SpillDir::new(fs.clone(), &parent).expect("spill dir");
    let other = SpillDir::new(fs.clone(), &parent).expect("spill dir");
    assert_ne!(
        spill.dir(),
        other.dir(),
        "each request gets its own directory"
    );
    assert!(spill.dir().is_under(&parent));
    for name in ["pack", "idx/part-1", "idx/part-2"] {
        fs.write(
            &spill.path(name).expect("path"),
            Bytes::from_static(b"spilled"),
            WriteMode::Overwrite,
        )
        .await
        .expect("write");
    }
    fs.write(
        &other.path("kept").expect("path"),
        Bytes::from_static(b"other"),
        WriteMode::Overwrite,
    )
    .await
    .expect("write");
    let spilled = spill.dir().clone();
    assert_eq!(fs.list(&spilled, None, 10).await.expect("list").len(), 3);

    drop(spill);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if fs.list(&spilled, None, 10).await.expect("list").is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "spill dir {spilled} not removed after drop"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Only the dropped directory went.
    assert_eq!(fs.list(other.dir(), None, 10).await.expect("list").len(), 1);

    let other_dir = other.dir().clone();
    other.close().await.expect("close");
    assert!(
        fs.list(&other_dir, None, 10)
            .await
            .expect("list")
            .is_empty()
    );
}
