//! The suites, run against the SDK's reference connector (the kit's
//! self-test), and the rules that keep a report honest.
// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]

mod common;

use common::{StubHarness, opts, reference_spec, registry};
use loams_connector_sdk::{Profile, ProfileKind, SystemRef};
use loams_flow::AuthMethod;
use loams_flow_conformance::reference::{ReferenceHarness, SERVICES};
use loams_flow_conformance::{
    FaultPoint, RunEnv, Suite, SuiteOutcome, SuiteReport, SuiteResult, capability_checks, run_with,
    unchecked_capabilities,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_at_every_fault_point() {
    let spec = reference_spec();
    let env = ReferenceHarness::run_env();

    // The transactional map sink: a crash at every fault point of the chain
    // counter → Fabric → map leaves the target equal to the input exactly.
    let h = ReferenceHarness::new(true);
    let report = run_with(&env, &opts(), &spec, None, &h, &[Suite::ExactlyOnce]).await;
    assert!(report.green(), "{report}");
    let notes = &report.result(Suite::ExactlyOnce).unwrap().notes;
    for p in FaultPoint::ALL {
        assert!(
            notes
                .iter()
                .any(|n| n.starts_with(&format!("{p:?}: ")) && n.ends_with("exactly once")),
            "{p:?} did not run: {notes:?}"
        );
    }

    // The control: the same chain with the plain (at-least-once) map sink
    // replays after a crash, and the suite catches the duplicate.
    let plain = ReferenceHarness::new(false);
    let report = run_with(&env, &opts(), &spec, None, &plain, &[Suite::ExactlyOnce]).await;
    match &report.result(Suite::ExactlyOnce).unwrap().outcome {
        SuiteOutcome::Failed(m) => assert!(m.contains("after a crash at"), "{m}"),
        other => panic!("an at-least-once sink passed exactly-once: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_connector_passes_its_suites() {
    let spec = reference_spec();
    let env = ReferenceHarness::run_env();
    let h = ReferenceHarness::new(true);
    let suites = [
        Suite::Contract,
        Suite::Roundtrip,
        Suite::KillRestart,
        Suite::DupCheck,
        Suite::Backpressure,
        Suite::Auth,
    ];
    let report = run_with(&env, &opts(), &spec, None, &h, &suites).await;
    assert!(report.green(), "{report}");
    assert!(report.proves(&suites));
    assert_eq!(report.system.as_deref(), Some("Reference connector"));
    // The contract proved `sink.transactional` (exactly-once) in small,
    // because this run did not request that suite on its own.
    let contract = &report.result(Suite::Contract).unwrap().notes;
    assert!(
        contract
            .iter()
            .any(|n| n.contains("sink.transactional") && n.contains("exactly-once passed in small")),
        "{contract:?}"
    );
    // The at-least-once sink keeps duplicates within two copies.
    let plain = ReferenceHarness::new(false);
    let mut spec = reference_spec();
    spec.capabilities.delivery.sink = loams_flow::Delivery::AtLeastOnce;
    if let Some(sink) = spec.capabilities.sink.as_mut() {
        sink.transactional = false;
    }
    let report = run_with(
        &env,
        &opts(),
        &spec,
        None,
        &plain,
        &[Suite::KillRestart, Suite::DupCheck],
    )
    .await;
    assert!(report.green(), "{report}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skipped_suite_never_counts_as_green() {
    // A report with a skipped suite is not green, and does not prove it.
    let report = SuiteReport {
        connector: "x".to_owned(),
        system: Some("X".to_owned()),
        results: vec![
            SuiteResult {
                suite: Suite::Contract,
                outcome: SuiteOutcome::Passed,
                notes: vec![],
            },
            SuiteResult {
                suite: Suite::Roundtrip,
                outcome: SuiteOutcome::Skipped("no service".to_owned()),
                notes: vec![],
            },
        ],
    };
    assert!(!report.green());
    assert!(report.proves(&[Suite::Contract]));
    assert!(!report.proves(&[Suite::Roundtrip]));
    assert!(
        !report.proves(&[Suite::Bulk]),
        "an absent suite proves nothing"
    );
    let empty = SuiteReport {
        connector: "x".to_owned(),
        system: None,
        results: vec![],
    };
    assert!(!empty.green());

    // No stack is up: every suite is skipped, none is green.
    let spec = reference_spec();
    let mut env: RunEnv = ReferenceHarness::run_env();
    env.vars.clear();
    let report = run_with(
        &env,
        &opts(),
        &spec,
        None,
        &StubHarness(SERVICES),
        &[Suite::Contract, Suite::Roundtrip],
    )
    .await;
    assert_eq!(report.skipped(), vec![Suite::Contract, Suite::Roundtrip]);
    assert!(!report.green());
    match &report.results[0].outcome {
        SuiteOutcome::Skipped(m) => assert!(m.contains("LOAMS_CONNECTORS_P2_STACK"), "{m}"),
        other => panic!("{other:?}"),
    }

    // The service is up, but the harness cannot run the suite (the reference
    // connector has no bulk path, and no basic auth): skipped, not green.
    let env = ReferenceHarness::run_env();
    let mut spec = reference_spec();
    spec.auth = vec![AuthMethod::Basic];
    let h = ReferenceHarness::new(true);
    let report = run_with(&env, &opts(), &spec, None, &h, &[Suite::Bulk, Suite::Auth]).await;
    assert_eq!(report.skipped(), vec![Suite::Bulk, Suite::Auth], "{report}");
    assert!(!report.green());

    // A declared capability whose suite cannot run keeps the contract from
    // passing: contract exercises every declared capability.
    let mut spec = reference_spec();
    spec.capabilities.bulk.arrow = true;
    let report = run_with(&env, &opts(), &spec, None, &h, &[Suite::Contract]).await;
    match &report.result(Suite::Contract).unwrap().outcome {
        SuiteOutcome::Skipped(m) => assert!(m.contains("bulk.arrow"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert!(!report.green());
}

#[test]
fn every_declared_capability_has_a_test() {
    let reg = registry();
    let mut problems = Vec::new();
    let mut preview = 0;
    for spec in reg.all() {
        if !matches!(spec.status.slug(), "preview" | "stable") {
            continue;
        }
        preview += 1;
        let unchecked = unchecked_capabilities(spec, None);
        if !unchecked.is_empty() {
            problems.push(format!("{}: no check for {unchecked:?}", spec.id));
        }
        // R-CN2-5: a preview manifest runs the contract, which exercises
        // every declared capability.
        if !spec.conformance.iter().any(|s| s.slug() == "contract") {
            problems.push(format!("{}: preview without the contract suite", spec.id));
        }
    }
    assert!(preview > 0);
    assert_eq!(problems, Vec::<String>::new());

    // The mapping itself: each capability goes to the suite that proves it.
    let pg = reg.get("postgresql").unwrap();
    let checks = capability_checks(pg, None);
    let suite_of = |cap: &str| checks.iter().find(|c| c.capability == cap).map(|c| c.suite);
    assert_eq!(suite_of("sink.transactional"), Some(Suite::ExactlyOnce));
    assert_eq!(suite_of("source.resumable"), Some(Suite::KillRestart));
    assert_eq!(suite_of("bulk.arrow"), Some(Suite::Bulk));
    assert_eq!(suite_of("auth.basic"), Some(Suite::Auth));
    assert_eq!(suite_of("sink.upsert"), Some(Suite::DupCheck));
    let mysql_cdc = reg.get("debezium-mysql").unwrap();
    assert!(
        capability_checks(mysql_cdc, None)
            .iter()
            .any(|c| c.capability == "source.cdc" && c.suite == Suite::Cdc)
    );

    // A target profile's narrowing removes the capability, and its check.
    let profile = Profile {
        id: "aurora".to_owned(),
        kind: ProfileKind::Target,
        module: "postgres".to_owned(),
        system: SystemRef {
            name: "Aurora PostgreSQL".to_owned(),
            service: serde_json::json!({}),
        },
        defaults: serde_json::json!({}),
        narrows: vec![
            "capabilities.sink.transactional".to_owned(),
            "bulk".to_owned(),
        ],
        http: None,
    };
    let narrowed = capability_checks(pg, Some(&profile));
    assert!(
        !narrowed
            .iter()
            .any(|c| c.capability == "sink.transactional")
    );
    assert!(!narrowed.iter().any(|c| c.capability == "bulk.arrow"));
}
