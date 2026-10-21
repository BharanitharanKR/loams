//! iceberg-rust 0.10.1, the newest release (2026-08-01).
const VERSION: &str = "0.10.1 (crates.io)";
const VERSION_TAG: &str = "rel";
include!("../../reader-common/probe.rs");

fn task_deletes(t: &iceberg::scan::FileScanTask) -> usize {
    t.deletes.len()
}
