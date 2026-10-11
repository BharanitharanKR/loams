//! Generates `loams.house.v1` from the repository's root `proto/` tree with
//! connect-rust's code generator and the system `protoc`, as the engine
//! workspace's `crates/loams-proto/build.rs` does for the other `loams.*.v1`
//! packages (HS1 Task 8).
//!
//! `house.proto` imports `loams.operations.v1` (`RestoreTable` answers an
//! `Operation`) and `loams.options.v1` (the service's module option), so both
//! are generated here too; `loams.errors.v1` is generated because every House
//! error carries its `ErrorInfo`. The `fabric/` workspace is a separate cargo
//! workspace, so these types are generated once in it, here, as `loams-proto`
//! generates them once in the engine's.

/// The proto files, as paths under the repository's `proto/` directory.
const FILES: &[&str] = &[
    "loams/options/v1/options.proto",
    "loams/errors/v1/errors.proto",
    "loams/operations/v1/operations.proto",
    "loams/house/v1/house.proto",
];

fn main() {
    // Read at run time, not with `env!`: the shared target directory reuses a
    // compiled build script across worktrees, and a path baked in at compile
    // time would point into whichever worktree compiled it first (the reason
    // `loams-proto/build.rs` gives).
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("cargo sets CARGO_MANIFEST_DIR for build scripts");
    // `fabric/crates/loams-house-proto` -> the repository root's `proto/`.
    let root = format!("{manifest_dir}/../../../proto");
    let files: Vec<String> = FILES.iter().map(|f| format!("{root}/{f}")).collect();
    for file in &files {
        println!("cargo:rerun-if-changed={file}");
    }
    println!("cargo:rerun-if-env-changed=CARGO_MANIFEST_DIR");
    if let Err(err) = connectrpc_build::Config::new()
        .files(&files)
        .includes(&[root])
        .include_file("_connectrpc.rs")
        .gate_client_feature(true)
        .compile()
    {
        panic!("generating the loams.house.v1 code failed (is protoc installed?): {err:#}");
    }
}
