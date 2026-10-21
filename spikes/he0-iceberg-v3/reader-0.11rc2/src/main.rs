//! iceberg-rust v0.11.0-rc.2 (commit 8590485), the 0.11.x release branch.
const VERSION: &str = "0.11.0-rc.2 @ 859048563bc7 (git)";
const VERSION_TAG: &str = "rc2";
include!("../../reader-common/probe.rs");

fn task_deletes(t: &iceberg::scan::FileScanTask) -> usize {
    t.deletes().len()
}
