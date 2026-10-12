fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../../crates/loams-stream-grpc/proto/loams/stream/v1/stream.proto";
    let include = "../../../crates/loams-stream-grpc/proto";
    // stream.proto imports io/cloudevents/v1/cloudevents.proto, whose schema
    // lives with loams-cloudevents (GT1 Task 3); main.rs includes the
    // generated io.cloudevents.v1 module itself.
    let cloudevents = "../../../crates/loams-cloudevents/proto";
    tonic_prost_build::configure().compile_protos(&[proto], &[include, cloudevents])?;
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed={include}");
    println!("cargo:rerun-if-changed={cloudevents}");
    Ok(())
}
