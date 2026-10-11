//! Compiles the vendored CloudEvents protobuf schema (`io.cloudevents.v1`)
//! with the system `protoc` when the `protobuf` feature is on (plan GT1
//! Task 3, ruling R0.4). The schema stays crate-local: it is upstream's, not
//! ours, and is not held to the workspace's buf lint rules.

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    #[cfg(feature = "protobuf")]
    {
        println!("cargo:rerun-if-changed=proto/io/cloudevents/v1/cloudevents.proto");
        prost_build::Config::new()
            // A map's entries come out in name order, so a converted event's
            // extension attributes are ordered and a batch's bytes are
            // deterministic.
            .btree_map(["."])
            .compile_protos(&["proto/io/cloudevents/v1/cloudevents.proto"], &["proto"])?;
    }
    Ok(())
}
