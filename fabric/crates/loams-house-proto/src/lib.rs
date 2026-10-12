//! `loams.house.v1`, the Connect API of Loams House (design §49 §18, D778; HS1
//! Task 8), generated at build time from `proto/loams/house/v1/house.proto` at
//! the repository root (see `build.rs`): buffa messages with the proto3 JSON
//! mapping, the connect-rust `HouseService` trait the front implements
//! (`loams_house::api`) and, with the `client` feature, `HouseServiceClient`.
//!
//! The packages sit at `loams_house_proto::loams::<pkg>::v1`, the path every
//! other proto crate of the repository uses, so a message is reached the same
//! way whichever crate holds it. `loams.operations.v1`, `loams.options.v1` and
//! `loams.errors.v1` come along: the House imports the first two, and its
//! errors carry the third's `ErrorInfo`.

#[allow(missing_debug_implementations, missing_docs, unreachable_pub)]
mod generated {
    connectrpc::include_generated!();
}

pub use generated::*;

/// The well-known types the generated messages name by path
/// (`google::protobuf::Timestamp`), so the front builds them from the same
/// crate the generated code does.
pub use buffa_types::google;
