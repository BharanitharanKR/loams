//! notifications.proto (G19) and threads.proto's usage (G20; SD1 Task 14).

use loams_agentd_proto::notify as rs_notify;
use loams_agentd_proto::usage as rs_usage;

use crate::proto::loams::agentd::v1 as pb;

wire_enum!(rs_notify::NotificationKind => pb::NotificationKind {
    TurnDone => NOTIFICATION_KIND_TURN_DONE,
    Approval => NOTIFICATION_KIND_APPROVAL,
    Question => NOTIFICATION_KIND_QUESTION,
    Error => NOTIFICATION_KIND_ERROR,
    GroupReady => NOTIFICATION_KIND_GROUP_READY,
    CheckFailed => NOTIFICATION_KIND_CHECK_FAILED,
});

wire_struct!(rs_notify::Notification => pb::Notification {
    id => id,
    kind => kind,
    chat_id => chat_id,
    group_id => group_id,
    title => title,
    created_at_ms => created_at_ms,
});

wire_struct!(rs_usage::GetThreadUsageParams => pb::GetThreadUsageRequest {
    chat_id => chat_id,
});

wire_struct!(rs_usage::TurnUsage => pb::TurnUsage {
    turn_id => turn_id,
    input_tokens => input_tokens,
    output_tokens => output_tokens,
    cache_tokens => cache_tokens,
    cost_usd => cost_usd,
    duration_ms => duration_ms,
});

wire_struct!(rs_usage::UsageTotals => pb::UsageTotals {
    input_tokens => input_tokens,
    output_tokens => output_tokens,
    cache_tokens => cache_tokens,
    cost_usd => cost_usd,
    duration_ms => duration_ms,
});

wire_struct!(rs_usage::ThreadUsage => pb::GetThreadUsageResponse {
    turns => turns,
    totals => totals as default_msg,
});
