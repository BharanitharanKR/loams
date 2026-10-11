fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `io.cloudevents.v1` is generated once, by `loams-cloudevents` (plan GT1
    // Task 3); its schema stays there and is only an include path here.
    println!("cargo:rerun-if-changed=../loams-cloudevents/proto");
    tonic_prost_build::configure()
        // A map's entries come out in name order, so a converted event's
        // extension attributes are ordered.
        .btree_map(".")
        .extern_path(".io.cloudevents.v1", "::loams_cloudevents::protobuf::pb")
        .compile_protos(
            &["proto/loams/stream/v1/stream.proto"],
            &["proto", "../loams-cloudevents/proto"],
        )?;
    Ok(())
}
