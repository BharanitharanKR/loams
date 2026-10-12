//! The conformance suite on `NativeFs`, in a directory under cargo's target
//! tmpdir (never the RAM `/tmp`), plus what only a disk backend does.

use std::sync::Arc;

use bytes::Bytes;
use loams_fs::{Fs, FsPath, NativeFs, WriteMode, conformance};

fn native() -> (Arc<dyn Fs>, tempfile::TempDir) {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a temporary directory");
    let fs = NativeFs::new(dir.path().join("root"), true).expect("a native fs");
    (Arc::new(fs), dir)
}

loams_fs::fs_conformance!(native_fs, crate::native());

#[tokio::test]
async fn every_case_runs_on_one_native_fs() {
    let (fs, _dir) = native();
    conformance::run(fs).await;
}

#[tokio::test]
async fn files_survive_reopening_the_root() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let path = FsPath::new("cache/ab/cdef").unwrap();
    {
        let fs = NativeFs::new(dir.path().join("root"), true).unwrap();
        assert!(fs.caps().survives_restart);
        fs.write(&path, Bytes::from_static(b"kept"), WriteMode::CreateNew)
            .await
            .unwrap();
    }
    let reopened = NativeFs::new(dir.path().join("root"), true).unwrap();
    assert_eq!(
        reopened.read(&path).await.unwrap(),
        Bytes::from_static(b"kept")
    );
}

#[tokio::test]
async fn staging_files_are_never_listed_and_are_swept_on_open() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let root = dir.path().join("root");
    let fs = NativeFs::new(&root, false).unwrap();
    fs.write(
        &FsPath::new("a").unwrap(),
        Bytes::from_static(b"1"),
        WriteMode::Overwrite,
    )
    .await
    .unwrap();
    // A write that crashed half way leaves a staging file behind.
    let staging = std::fs::read_dir(root.join("staging")).unwrap().count();
    assert_eq!(staging, 0, "a finished write leaves no staging file");
    std::fs::write(root.join("staging").join("crashed-write"), b"partial").unwrap();

    let listed = fs.list(&FsPath::root(), None, 10).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0, FsPath::new("a").unwrap());

    drop(fs);
    let _reopened = NativeFs::new(&root, false).unwrap();
    assert_eq!(std::fs::read_dir(root.join("staging")).unwrap().count(), 0);
}
