//! iceberg-rust main, pinned at 000e791 (2026-10-21).
const VERSION: &str = "main @ 000e791d5229 (git)";
const VERSION_TAG: &str = "main";
include!("../../reader-common/probe.rs");

fn task_deletes(t: &iceberg::scan::FileScanTask) -> usize {
    t.deletes().len()
}
