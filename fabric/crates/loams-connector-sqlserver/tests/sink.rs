//! The exactly-once MERGE sink against real SQL Server 2025 Developer
//! (compose `sqlserver`, `connectors-ci-only`), gated on
//! `LOAMS_CONNECTORS_CI_ONLY_STACK`; and Azure SQL with an Entra ID token,
//! nightly, gated on `LOAMS_AZURE_SQL_URL`.

mod common;

use common::{azure, ci_only, opts, spec};
use loams_connector_sqlserver::Target;
use loams_connector_sqlserver::harness::{Endpoint, Login, SqlHarness};
use loams_flow_conformance::{ConnectorHarness, RunEnv, Suite, run_with};

/// A crash at every fault point of the sink (mid-batch, after prepare,
/// after the commit but before the Fabric commit): the target holds every
/// record exactly once, counted by a trigger on every application.
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_merge_exactly_once_under_crash() {
    if !ci_only() {
        return;
    }
    let h = SqlHarness::sink(Target::SqlServer, "eo", true);
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("sql-server"),
        None,
        &h,
        &[Suite::ExactlyOnce, Suite::KillRestart, Suite::Roundtrip],
    )
    .await;
    assert!(report.green(), "{report}");
}

/// The control: the plain (at-least-once) sink must *fail* exactly-once
/// under the same crashes, or the trigger proves nothing.
#[tokio::test(flavor = "multi_thread")]
async fn plain_merge_sink_is_not_exactly_once() {
    if !ci_only() {
        return;
    }
    let h = SqlHarness::sink(Target::SqlServer, "alo", false);
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("sql-server"),
        None,
        &h,
        &[Suite::ExactlyOnce],
    )
    .await;
    assert!(
        !report.green(),
        "an at-least-once sink passed exactly-once: {report}"
    );
}

/// TDS 8.0 `strict` against the container: TLS (rustls, ALPN `tds/8.0`)
/// before the login. The container makes a self-signed certificate at start
/// (SQL Server 2022 on Linux refuses strict logins whatever its certificate,
/// so the estate runs 2025, R12.15), and the test trusts it explicitly.
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_strict_tls_login() {
    if !ci_only() {
        return;
    }
    let mut ep = Endpoint::from_env(Target::SqlServer).expect("endpoint");
    ep.encrypt = "strict".into();
    ep.trust_server_certificate = true;
    let h = SqlHarness::sink(Target::SqlServer, "tls", true).with_endpoint(ep);
    h.reset().await.expect("reset over strict TLS");
    h.authenticate("basic")
        .await
        .expect("login over strict TLS");
}

/// Azure SQL with a service principal's Entra ID token (client
/// credentials), fetched through the SDK's HTTP client: the sink's
/// exactly-once and roundtrip suites, and the `aad` auth suite.
#[tokio::test(flavor = "multi_thread")]
async fn azure_sql_entra_token_nightly() {
    if !azure() {
        return;
    }
    let h = SqlHarness::sink(Target::AzureSql, "entra", true);
    let ep = h.endpoint().expect("LOAMS_AZURE_SQL_URL parses");
    assert!(
        matches!(ep.login, Login::Entra { .. }),
        "LOAMS_AZURE_SQL_URL must name a service principal (?tenant=)"
    );
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("azure-sql"),
        None,
        &h,
        &[Suite::Auth, Suite::Roundtrip, Suite::ExactlyOnce],
    )
    .await;
    assert!(report.green(), "{report}");
}
