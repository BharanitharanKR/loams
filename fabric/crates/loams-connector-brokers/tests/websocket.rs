//! `websocket` against its fixture (systems.toml: frames recorded from a
//! public RFC 6455 server, replayed by this family's own replayer, because
//! the kit's FixtureProxy is HTTP only). No recording exists yet (R18.11),
//! so the kit reports the suites as skipped; the connector's behaviour is
//! covered by the unit tests in `src/websocket.rs` against an in-process
//! server, which never count toward `preview`.

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
mod common;

use common::*;
use loams_connector_brokers::websocket;
use loams_flow_conformance::{ServiceKind, Suite};

const SERVICES: &[ServiceKind] = &[ServiceKind::Fixture {
    dir: "conformance/connectors/fixtures/websocket",
    recorded_from: "WebSocket (frames recorded from a public RFC 6455 server)",
}];

fn plan() -> Plan {
    Plan {
        services: SERVICES,
        factory: websocket::factory(),
        configs: Box::new(|_| unreachable!("the fixture has no recording yet")),
        setup: Box::new(|_| Box::pin(async { Err("no recording".to_owned()) })),
        subscribe_first: false,
        auth: vec![],
        redelivery: std::time::Duration::ZERO,
    }
}

#[tokio::test]
async fn websocket_suites_against_the_fixture() {
    let h = BrokerHarness::new("websocket", plan());
    let report = h
        .run(
            websocket::ID,
            &[
                Suite::Contract,
                Suite::Roundtrip,
                Suite::Backpressure,
                Suite::Auth,
            ],
        )
        .await;
    eprintln!("{report}");
    assert!(
        report.green() || report.skipped().len() == report.results.len(),
        "{report}"
    );
}

#[test]
fn websocket_declares_not_resumable() {
    let spec = spec(websocket::ID);
    let doc = spec.to_json().unwrap();
    assert_eq!(doc["capabilities"]["source"]["resumable"], false);
    assert_eq!(doc["capabilities"]["delivery"]["source"], "at_most_once");
    assert!(!websocket::declared().source_resumable);
}
