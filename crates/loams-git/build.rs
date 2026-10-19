//! Compiles the crate-local `loams.git.v1` on-disk format protos with the
//! system `protoc` (plan GT1 Task 5, R0.5).

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/loams/git/v1/wal.proto");
    println!("cargo:rerun-if-changed=proto/loams/git/v1/derived.proto");
    println!("cargo:rerun-if-changed=proto/loams/git/v1/workspace.proto");
    prost_build::Config::new()
        // `Checkpoint.refs` is a map: name order makes a checkpoint's bytes
        // deterministic, which the golden files pin.
        .btree_map(["."])
        .compile_protos(
            &[
                "proto/loams/git/v1/wal.proto",
                "proto/loams/git/v1/derived.proto",
                "proto/loams/git/v1/workspace.proto",
            ],
            &["proto"],
        )
}
