//! diff.proto: checkout diffs and a file's two sides.

use loams_agentd_proto as rs;

use crate::proto::loams::agentd::v1 as pb;

wire_struct!(rs::DiffFileSummary => pb::DiffFileSummary {
    path => path,
    old_path => old_path,
    status => status,
    additions => additions,
    deletions => deletions,
    binary => binary,
});

wire_struct!(rs::CheckoutDiff => pb::CheckoutDiff {
    checkout_id => checkout_id,
    device_id => device_id,
    cwd => cwd,
    patch => patch,
    files => files,
    additions => additions,
    deletions => deletions,
    truncated => truncated,
    checksum => checksum,
    updated_at => updated_at_ms,
});

wire_struct!(rs::CheckoutFileDiffText => pb::CheckoutFileDiffText {
    diff_checksum => diff_checksum,
    old_text => old_text,
    new_text => new_text,
    old_content_hash => old_content_hash,
    new_content_hash => new_content_hash,
    binary => binary,
    truncated => truncated,
    stale => stale,
});
