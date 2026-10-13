//! Generates the `loams.flow.v1` messages from `fabric/proto/loams/flow/v1/`
//! with connect-rust's code generator and the system `protoc`, as
//! `crates/loams-live-proto/build.rs` does for `proto/loams/live/v1/`
//! (R1 plan Task 7).
//!
//! With the `graph` feature it also generates `loams.graph.v1` (Grafeo's
//! Connect API, design §48 §8) from the **repository root's** `proto/` tree,
//! the cross-workspace build CN2 owner ruling OR-20 asks for and spike §8
//! found possible: `fabric/crates/loams-house-proto/build.rs` already does the
//! same for `loams.house.v1`. `graph.proto` imports `loams.errors.v1`,
//! `loams.operations.v1` and `loams.options.v1`, so those are generated with
//! it (CN2 Task 21, ruling R21.4).

/// The `loams.graph.v1` files and their imports, under the root `proto/`.
const GRAPH_FILES: &[&str] = &[
    "loams/options/v1/options.proto",
    "loams/errors/v1/errors.proto",
    "loams/operations/v1/operations.proto",
    "loams/graph/v1/graph.proto",
];

fn main() {
    // Read at run time, not with `env!`: the shared target directory reuses a
    // compiled build script across worktrees, and a path baked in at compile
    // time would point into whichever worktree compiled it first (the reason
    // `loams-house-proto/build.rs` gives).
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("cargo sets CARGO_MANIFEST_DIR for build scripts");
    // The proto root is the workspace's `fabric/proto`, a sibling of the engine
    // workspace's root `proto/` rather than a subtree of it: the `fabric/`
    // workspace is a separate cargo workspace (FL1 plan Task 1), and this crate
    // sits at `fabric/crates/loams-flow-proto`, so its root is two levels up.
    let flow_root = format!("{manifest_dir}/../../proto");
    // The manifest types of CN1 Task 1. CN1 Task 3 adds `flow`, which owns
    // `FlowService`; the list grows there.
    let mut files: Vec<String> = ["connector", "instance"]
        .iter()
        .map(|name| format!("{flow_root}/loams/flow/v1/{name}.proto"))
        .collect();
    let mut includes = vec![flow_root];
    if std::env::var_os("CARGO_FEATURE_GRAPH").is_some() {
        // `fabric/crates/loams-flow-proto` -> the repository root's `proto/`.
        let root = format!("{manifest_dir}/../../../proto");
        files.extend(GRAPH_FILES.iter().map(|f| format!("{root}/{f}")));
        includes.push(root);
    }
    for file in &files {
        println!("cargo:rerun-if-changed={file}");
    }
    println!("cargo:rerun-if-env-changed=CARGO_MANIFEST_DIR");
    if let Err(err) = connectrpc_build::Config::new()
        .files(&files)
        .includes(&includes)
        .include_file("_connectrpc.rs")
        .gate_client_feature(true)
        .compile()
    {
        panic!("generating the loams.flow.v1 code failed (is protoc installed?): {err:#}");
    }
}
