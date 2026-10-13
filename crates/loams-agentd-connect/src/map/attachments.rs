//! attachments.proto and preview.proto.

use loams_agentd_proto as rs;

use crate::proto::loams::agentd::v1 as pb;

wire_struct!(rs::AttachmentChunk => pb::AttachmentChunk {
    name => name,
    mime_type => mime_type,
    data => data,
    next_offset => next_offset,
    done => done,
});

wire_struct!(rs::ToolOutputChunk => pb::ToolOutputChunk {
    text => text,
    offset => offset,
    len => len,
    total => total,
    truncated => truncated,
    eof => eof,
});

wire_struct!(rs::PreviewService => pb::PreviewServer {
    id => id,
    project_id => project_id,
    project_name => project_name,
    project_cwd => project_cwd,
    device_id => device_id,
    device_name => device_name,
    hostname => hostname,
    name => name,
    port => port,
    pid => pid,
    cwd => cwd,
    started_at => started_at_ms,
    loams_desktop_owned => loams_desktop_owned,
});

wire_struct!(rs::PreviewSnapshot => pb::PreviewSnapshot {
    services => services,
    proxy_port => proxy_port,
    error => error,
    project_name => project_name,
    remote => remote,
});
