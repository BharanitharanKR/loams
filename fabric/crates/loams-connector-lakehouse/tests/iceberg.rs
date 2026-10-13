//! The native Iceberg raw-topic sink (OR-8) waits on the owner's R0.15
//! decision (R17.1): it refuses to start, naming the ruling.

use std::sync::Arc;

use loams_connector_lakehouse::iceberg_sink::IcebergSink;
use loams_connector_sdk::{
    ConnectorContext, ConnectorFactory, ErrorClass, InstanceKey, Scrubber, StaticSecrets,
};

fn ctx() -> ConnectorContext {
    ConnectorContext::new(
        InstanceKey::new(7, "raw"),
        serde_json::json!({"catalog": {"uri": "http://lakekeeper:8181/catalog"}}),
        Arc::new(StaticSecrets::new()),
        Scrubber::new(),
    )
}

#[test]
fn iceberg_sink_refuses_to_start_naming_r0_15() {
    let Err(err) = IcebergSink.sink(&ctx()) else {
        panic!("the Iceberg sink must refuse to start");
    };
    assert_eq!(err.class, ErrorClass::Fatal);
    assert!(err.message.contains("R0.15"), "{}", err.message);
    assert!(err.message.contains("R17.1"), "{}", err.message);
    assert!(IcebergSink.source(&ctx()).expect("no source").is_none());
}

/// The plan's test. It needs the sink, which waits on the owner's R0.15
/// decision (R17.1); its body is written when the decision lands.
#[test]
#[ignore = "CN2 R17.1: the Iceberg sink waits on the owner's R0.15 decision"]
fn iceberg_sink_snapshot_checkpoint_exactly_once() {
    assert!(IcebergSink.sink(&ctx()).is_ok());
}
