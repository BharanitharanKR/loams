//! What the family's service tests share: the gate (a suite needs its
//! compose stack, or the nightly secrets), and the check of a suite report.

#![allow(dead_code)]

use loams_connector_kv::harness::{ChainHarness, KvSystem, proposed_manifest};
use loams_flow_conformance::{Suite, SuiteReport};

/// Whether `var` is set: the CI shard sets `LOAMS_CONNECTORS_P2_STACK` once
/// the `connectors-p2` services are up, `LOAMS_CONNECTORS_CI_ONLY_STACK` for
/// `connectors-ci-only`. Locally neither is set and nothing is pulled or run.
pub fn up(var: &str) -> bool {
    std::env::var(var).is_ok_and(|v| !v.is_empty())
}

/// Skip with a note.
pub fn skip(test: &str, why: &str) {
    eprintln!("{test}: skipped ({why})");
}

/// Run `suites` of the proposed manifest `id` through `h`, and check the
/// report: green when `live`, else every suite skipped (never green).
pub async fn run_suites<S: KvSystem>(
    id: &str,
    h: &ChainHarness<S>,
    suites: &[Suite],
    live: bool,
) -> SuiteReport {
    let (spec, profile) = proposed_manifest(id).unwrap_or_else(|e| panic!("{id}: {e:?}"));
    let report = loams_flow_conformance::run(&spec, profile.as_ref(), h, suites).await;
    eprintln!("{report}");
    if live {
        assert!(report.green(), "{id} suites are not green:\n{report}");
    } else {
        assert_eq!(
            report.skipped().len(),
            report.results.len(),
            "{id}: without its service every suite is skipped:\n{report}"
        );
        assert!(!report.green());
    }
    report
}

/// The suites a proposed manifest declares.
pub fn declared(id: &str) -> Vec<Suite> {
    let (spec, _) = proposed_manifest(id).unwrap_or_else(|e| panic!("{id}: {e:?}"));
    Suite::declared_by(&spec)
}
