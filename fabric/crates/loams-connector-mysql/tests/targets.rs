//! Amazon Aurora MySQL, nightly against a real cluster (OR-3, OR-23):
//! `LOAMS_AURORA_MYSQL_URL` (`mysql://user:password@host:port/database`)
//! and the AWS credentials for IAM tokens. Skipped without them.

mod common;

use std::sync::Arc;

use common::{gate, opts, spec};
use loams_connector_mysql::harness::{Endpoint, MyHarness};
use loams_connector_mysql::{MysqlFactory, Target};
use loams_connector_sdk::{ConnectorContext, InstanceKey, Scrubber, StaticSecrets};
use loams_flow_conformance::{RunEnv, Suite, run_with};
use serde_json::json;

/// An IAM token, minted locally (R10.7's presigner, OR-6), logs in through
/// `mysql_clear_password` over TLS. `LOAMS_AURORA_MYSQL_IAM_USER` names the
/// IAM-enabled account (default: the URL's user).
#[tokio::test(flavor = "multi_thread")]
async fn aurora_mysql_iam_token_nightly() {
    if !gate(&[
        "LOAMS_AURORA_MYSQL_URL",
        "LOAMS_AWS_ACCESS_KEY_ID",
        "LOAMS_AWS_SECRET_ACCESS_KEY",
        "LOAMS_AWS_REGION",
    ]) {
        return;
    }
    let url = std::env::var("LOAMS_AURORA_MYSQL_URL").expect("url");
    let ep = Endpoint::from_url(&url).expect("a mysql:// URL");
    let var = |k: &str| std::env::var(k).expect(k);
    let user = std::env::var("LOAMS_AURORA_MYSQL_IAM_USER")
        .ok()
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| ep.user.clone());
    let secrets = StaticSecrets::new()
        .with("aws-key", var("LOAMS_AWS_ACCESS_KEY_ID").into_bytes())
        .with(
            "aws-secret",
            var("LOAMS_AWS_SECRET_ACCESS_KEY").into_bytes(),
        );
    let config = json!({
        "host": ep.host, "port": ep.port, "user": user, "database": ep.database,
        "auth": "iam", "tls": {"enabled": true, "verify_full": false},
        "aws": {"region": var("LOAMS_AWS_REGION"), "access_key_id": "aws-key", "secret_access_key": "aws-secret"},
        "tables": ["t"],
    });
    let ctx = ConnectorContext::new(
        InstanceKey::new(1, "aurora-iam"),
        config,
        Arc::new(secrets),
        Scrubber::new(),
    );
    let cfg = MysqlFactory::new(Target::Aurora)
        .config(&ctx)
        .expect("config");
    let mut conn = loams_connector_mysql::conn::connect(&cfg, &ctx)
        .await
        .expect("an IAM login");
    let one = conn.query("SELECT 1").await.expect("query").scalar();
    assert_eq!(one.as_deref(), Some("1"));
    conn.quit().await;
    // The nightly also runs the source suites on the real cluster.
    let h = MyHarness::cdc(Target::Aurora, "aurora");
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("aurora"),
        None,
        &h,
        &[Suite::KillRestart, Suite::Cdc],
    )
    .await;
    assert!(report.green(), "{report}");
}
