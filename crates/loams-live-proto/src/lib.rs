//! The `loams.live.v1` protocol of Loams Live (design §20 §4.3, §5.3, §7;
//! D121): values and stored documents (`value.proto`), the `LiveService`
//! sync API (`live.proto`), the `LiveAdminService` admin API (`admin.proto`,
//! LV1 plan Task 10), and two files internal to the server: the
//! commit journal (`journal.proto`) and the table and index catalog records
//! (`catalog.proto`). `loams::live::worker::v1` is the internal protocol
//! between the server and its sandboxed function workers
//! (`proto/loams/live/worker/v1/worker.proto`, LV1 plan Ruling 5).
//!
//! Everything is generated at build time from `proto/loams/live/v1/` by
//! `connectrpc-build`: buffa message types (with their borrowed views and
//! the proto3 JSON mapping, in which 64-bit integers are strings), the
//! `LiveService` server trait, and `LiveServiceClient` behind the `client`
//! feature. Oneof enums live under `loams::live::v1::__buffa::oneof`. The
//! TypeScript client is generated from the same files
//! (`sdks/live-typescript/src/gen/`).

// The generated service marker and server types derive no `Debug`.
#[allow(missing_debug_implementations)]
mod generated {
    connectrpc::include_generated!();
}

pub use generated::*;

/// The wire-format `FileDescriptorSet` of every generated file and its
/// imports, for the Live listener's `grpc.reflection.v1` (LV1 plan Task
/// 10). The server lists only `LiveService` and `LiveAdminService`.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/loams_live_descriptor.bin"));
