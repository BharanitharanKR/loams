//! loams-agentd-connect — `loams.agentd.v1`, the daemon's Connect surface for
//! Loams Studio (design §53 §6.1–§6.2, D907, D908; plan SD1 Tasks 4 and 5).
//!
//! The daemon keeps its ndjson wire (DD1 Ruling 2); this crate serves the
//! same engine a second way:
//!
//! - [`services::Agentd`] implements every service. Each handler maps its
//!   request to the ndjson twin's params ([`map`]), calls the engine's own
//!   dispatch with them ([`engine`]) and maps the twin's result back, so the
//!   two wires answer alike ([`twins`] names each method's twin).
//! - [`listener`] serves them on a loopback port behind the ndjson
//!   listener's rules (§50 §7): the owner token, `Host: 127.0.0.1:<port>`, and
//!   a 403 for any `Origin`.

/// The generated messages, service traits and clients of `loams.agentd.v1`,
/// with the `loams.errors.v1` and `loams.options.v1` they use.
#[allow(
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    clippy::all
)]
pub mod proto {
    connectrpc::include_generated!();
}

pub mod engine;
pub mod error;
pub mod listener;
pub mod map;
pub mod services;
pub mod twins;

pub use listener::{ConnectListener, LoopbackOptions};
pub use services::{Agentd, HostInfo};

/// The `FileDescriptorSet` of `loams.agentd.v1` (with its imports), as
/// `protoc` wrote it at build time.
pub const DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/agentd_descriptor.bin"));

/// The daemon feature this surface adds to `Hello` and `GetHost` (SD1 Shared
/// contracts).
pub const FEATURE: &str = "studio.v1";

/// The daemon feature of the worktree list and the git writes (§53 §6.5 G1–G5,
/// G21; SD1 Task 9).
pub const GIT_WRITE_FEATURE: &str = "git.write";
