//! The conformance suite on `MemFs`, the suite's reference backend.

use std::sync::Arc;

use loams_fs::{Fs, MemFs, conformance};

fn mem() -> (Arc<dyn Fs>, ()) {
    (Arc::new(MemFs::new(1024 * 1024)), ())
}

loams_fs::fs_conformance!(mem_fs, crate::mem());

#[tokio::test]
async fn every_case_runs_on_one_mem_fs() {
    conformance::run(mem().0).await;
}

#[tokio::test]
async fn mem_fs_reports_its_caps() {
    let caps = MemFs::new(77).caps();
    assert!(caps.append && caps.atomic_rename && !caps.survives_restart);
    assert_eq!(caps.max_file, 77);
}
