//! threads.proto's checkpoints (G6, G7; SD1 Task 10).

use loams_agentd_proto::checkpoints as rs_checkpoints;

use crate::proto::loams::agentd::v1 as pb;

wire_struct!(rs_checkpoints::Checkpoint => pb::Checkpoint {
    turn_id => turn_id,
    git_ref => r#ref,
    created_at_ms => created_at_ms,
    files_changed => files_changed,
});

wire_struct!(rs_checkpoints::CheckpointList => pb::ListCheckpointsResponse {
    checkpoints => checkpoints,
});

wire_struct!(rs_checkpoints::ListCheckpointsParams => pb::ListCheckpointsRequest {
    chat_id => chat_id,
});

wire_enum!(rs_checkpoints::CheckpointScope => pb::CheckpointScope {
    Worktree => CHECKPOINT_SCOPE_WORKTREE,
    WorktreeAndTranscript => CHECKPOINT_SCOPE_WORKTREE_AND_TRANSCRIPT,
});

wire_struct!(rs_checkpoints::RestoreCheckpointParams => pb::RestoreCheckpointRequest {
    chat_id => chat_id,
    turn_id => turn_id,
    expected_checksum => expected_checksum,
    scope => scope,
});

wire_struct!(rs_checkpoints::RestoreCheckpointReply => pb::RestoreCheckpointResponse {
    restored_to => restored_to,
    forked_chat_id => forked_chat_id,
});
