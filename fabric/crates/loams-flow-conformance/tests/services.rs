//! OR-2 and OR-3 over the estate: every suite runs against the manifest's
//! real system (never a Loams module standing in for it), a target's suite
//! runs against the target, every P2 manifest names its real system, and the
//! compose estate is pinned and split by licence.
// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;

use common::{StubHarness, opts, registry};
use loams_connector_sdk::{Profile, ProfileKind, SystemRef};
use loams_flow::manifest::repository_root;
use loams_flow_conformance::services::{
    ComposeEstate, LoamsModules, ServiceDecl, Stack, SystemEntry, SystemsCatalog,
};
use loams_flow_conformance::{
    Estate, RunEnv, ServiceKind, Suite, SuiteOutcome, check_services, harness_problems, run_with,
};

fn estate() -> Estate {
    Estate::load(&repository_root()).expect("systems.toml and compose.yaml load")
}

fn entry(systems: &[&str], services: Vec<ServiceDecl>, extra: Vec<ServiceDecl>) -> SystemEntry {
    SystemEntry {
        systems: systems.iter().map(|s| (*s).to_owned()).collect(),
        family: "search".to_owned(),
        services,
        extra_targets: extra,
        harness: None,
        notes: Vec::new(),
    }
}

fn container(compose: &str, system: &str) -> ServiceDecl {
    ServiceDecl::Container {
        compose: compose.to_owned(),
        system: system.to_owned(),
    }
}

const QDRANT: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "qdrant",
    system: "Qdrant",
}];
const LOAMS_QDRANT: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "loams-collections",
    system: "Loams collections (Qdrant API)",
}];
const LOAMS_HOUSE: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "loams-house",
    system: "Loams House",
}];

#[tokio::test]
async fn suite_service_is_the_real_system() {
    // The checked-in catalog is consistent: no Loams module stands in for an
    // external system, and every service is one of its entry's systems.
    let real = estate();
    assert_eq!(real.catalog.problems(), Vec::<String>::new());

    // A catalog where Qdrant's suite may also run, after the real one,
    // against the collections' Qdrant API.
    let mut catalog = SystemsCatalog {
        loams_modules: LoamsModules {
            systems: vec![
                "Loams collections (Qdrant API)".to_owned(),
                "Loams House".to_owned(),
            ],
            compose_prefix: "loams-".to_owned(),
        },
        systems: Default::default(),
    };
    catalog.systems.insert(
        "qdrant".to_owned(),
        entry(
            &["Qdrant"],
            vec![container("qdrant", "Qdrant")],
            vec![container(
                "loams-collections",
                "Loams collections (Qdrant API)",
            )],
        ),
    );
    assert_eq!(catalog.problems(), Vec::<String>::new());

    assert_eq!(
        check_services("qdrant", None, QDRANT, &catalog).as_deref(),
        Ok("Qdrant")
    );
    // The extra target alone never counts as the real system.
    let err = check_services("qdrant", None, LOAMS_QDRANT, &catalog).unwrap_err();
    assert!(
        err.iter().any(|e| e.contains("extra targets never count")),
        "{err:?}"
    );
    // A Loams module that is not even declared.
    let err = check_services("qdrant", None, LOAMS_HOUSE, &catalog).unwrap_err();
    assert!(err.iter().any(|e| e.contains("not declared")), "{err:?}");
    // A manifest with no entry has no real system to run against.
    assert!(check_services("clickhouse", None, LOAMS_HOUSE, &catalog).is_err());

    // A catalog that lists a Loams module as the real service is refused.
    catalog.systems.insert(
        "clickhouse".to_owned(),
        entry(
            &["ClickHouse"],
            vec![container("loams-house", "Loams House")],
            vec![],
        ),
    );
    let problems = catalog.problems();
    assert!(
        problems
            .iter()
            .any(|p| p.contains("Loams module standing in")),
        "{problems:?}"
    );
    // ... and a harness using it fails every suite, before any suite runs:
    // it is a failure, not a skip.
    let mut env = common_env(catalog);
    env.vars
        .insert(Stack::ConnectorsP2.env_var().to_owned(), "1".to_owned());
    let mut spec = registry().get("clickhouse").unwrap().clone();
    spec.id = "clickhouse".to_owned();
    let report = run_with(
        &env,
        &opts(),
        &spec,
        None,
        &StubHarness(LOAMS_HOUSE),
        &[Suite::Contract, Suite::Roundtrip],
    )
    .await;
    assert!(!report.green());
    assert_eq!(report.failed(), vec![Suite::Contract, Suite::Roundtrip]);
    match &report.results[0].outcome {
        SuiteOutcome::Failed(m) => assert!(m.contains("Loams module"), "{m}"),
        other => panic!("expected a failure, got {other:?}"),
    }
}

fn common_env(catalog: SystemsCatalog) -> RunEnv {
    let compose = ComposeEstate::parse(
        "services:\n  qdrant: { image: x, profiles: [connectors-p2] }\n  loams-house: { image: y, profiles: [connectors-p2] }\n",
    )
    .unwrap();
    RunEnv {
        root: repository_root(),
        catalog,
        compose,
        vars: Default::default(),
    }
}

fn redpanda_profile(system: &str, compose: &str) -> Profile {
    Profile {
        id: "redpanda".to_owned(),
        kind: ProfileKind::Target,
        module: "kafka".to_owned(),
        system: SystemRef {
            name: system.to_owned(),
            service: serde_json::json!({ "compose": compose }),
        },
        defaults: serde_json::json!({}),
        narrows: Vec::new(),
        http: None,
    }
}

const KAFKA: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "kafka",
    system: "Apache Kafka",
}];
const REDPANDA: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "redpanda",
    system: "Redpanda",
}];

#[test]
fn target_suite_runs_against_target_system() {
    let catalog = estate().catalog;
    let profile = redpanda_profile("Redpanda", "redpanda");

    // Redpanda is served by the Kafka family, but its suite runs against
    // Redpanda, not against Kafka.
    assert_eq!(
        check_services("redpanda", Some(&profile), REDPANDA, &catalog).as_deref(),
        Ok("Redpanda")
    );
    let err = check_services("redpanda", Some(&profile), KAFKA, &catalog).unwrap_err();
    assert!(
        err.iter().any(|e| e.contains("compose service redpanda")),
        "{err:?}"
    );
    assert!(err.iter().any(|e| e.contains("not declared")), "{err:?}");

    // A target profile that names another system than the manifest's.
    let wrong = redpanda_profile("Apache Kafka", "kafka");
    let err = check_services("redpanda", Some(&wrong), KAFKA, &catalog).unwrap_err();
    assert!(
        err.iter().any(|e| e.contains("not this manifest's")),
        "{err:?}"
    );

    // Every target the catalog declares runs against its own system: no
    // entry borrows another entry's service.
    for (id, e) in &catalog.systems {
        for s in &e.services {
            assert!(
                e.systems
                    .iter()
                    .any(|sys| loams_flow_conformance::services::system_matches(s.system(), sys)),
                "{id}: {s}"
            );
        }
    }
}

#[test]
fn every_p2_preview_manifest_has_a_harness() {
    let estate = estate();
    let reg = registry();
    let mut problems = Vec::new();
    let mut p2 = 0;
    for spec in reg.all() {
        if spec.priority.slug() != "P2" {
            continue;
        }
        p2 += 1;
        // Every P2 manifest names its real system now, so a family task
        // cannot make one `preview` against an undeclared service.
        problems.extend(harness_problems(spec, &estate.catalog, &estate.root));
    }
    assert!(p2 >= 70, "only {p2} P2 manifests");
    assert_eq!(problems, Vec::<String>::new());

    // A P2 manifest marked `preview` without a harness in an existing family
    // crate is refused.
    let mut spec = reg.get("redpanda").unwrap().clone();
    spec.status = loams_flow::Status::Preview;
    let problems = harness_problems(&spec, &estate.catalog, &estate.root);
    assert!(
        problems.iter().any(|p| p.contains("names no harness")),
        "{problems:?}"
    );
    let mut catalog = estate.catalog.clone();
    catalog.systems.get_mut("redpanda").unwrap().harness =
        Some("loams_connector_brokers::Redpanda".to_owned());
    let problems = harness_problems(&spec, &catalog, &estate.root);
    assert!(
        problems
            .iter()
            .any(|p| p.contains("not in its family crate")),
        "{problems:?}"
    );
}

/// The images OR-3 and the dependency spike (§7) mark `ci-service`:
/// proprietary, source-available or under a EULA.
const CI_SERVICE_REPOS: &[&str] = &[
    "redpandadata/redpanda",
    "mcr.microsoft.com/azure-messaging/eventhubs-emulator",
    "mcr.microsoft.com/azure-messaging/servicebus-emulator",
    "mcr.microsoft.com/mssql/server",
    "gvenzl/oracle-free",
    "cockroachdb/cockroach",
    "mongo",
    "redis",
    "amazon/dynamodb-local",
    "docker.elastic.co/elasticsearch/elasticsearch",
    "splunk/splunk",
    "gcr.io/google.com/cloudsdktool/google-cloud-cli",
    "minio/minio",
];

fn repo_of(image: &str) -> &str {
    let name_tag = image.split('@').next().unwrap_or(image);
    name_tag.rsplit_once(':').map_or(name_tag, |(r, _)| r)
}

#[test]
fn compose_estate_is_pinned_and_split() {
    let estate = estate();
    let compose = &estate.compose;
    assert!(compose.services.len() >= 30, "{}", compose.services.len());
    for (name, s) in &compose.services {
        // Pinned by digest, with the tag kept for humans.
        let (name_tag, digest) = s
            .image
            .split_once('@')
            .unwrap_or_else(|| panic!("{name}: {} is not pinned by digest", s.image));
        assert!(name_tag.contains(':'), "{name}: no tag in {}", s.image);
        let hex = digest.strip_prefix("sha256:").unwrap_or("");
        assert!(
            hex.len() == 64
                && hex
                    .chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "{name}: bad digest {digest}"
        );
        // Never in the default profile: every service names a CN2 profile.
        assert!(!s.profiles.is_empty(), "{name} runs in the default profile");
        for p in &s.profiles {
            assert!(
                matches!(p.as_str(), "connectors-p2" | "connectors-ci-only"),
                "{name}: unknown profile {p}"
            );
        }
        assert!(s.licence.is_some(), "{name}: no io.loams.licence label");
        // OR-3: a ci-service image runs only in connectors-ci-only, and the
        // label agrees with the list of proprietary images.
        let proprietary = CI_SERVICE_REPOS.contains(&repo_of(&s.image));
        assert_eq!(
            s.ci_service, proprietary,
            "{name}: io.loams.ci-service label disagrees with OR-3 for {}",
            s.image
        );
        if s.ci_service {
            assert_eq!(
                s.profiles,
                vec!["connectors-ci-only".to_owned()],
                "{name}: a ci-service image outside connectors-ci-only"
            );
        } else {
            assert!(
                s.profiles.iter().any(|p| p == "connectors-p2"),
                "{name}: an open image outside connectors-p2"
            );
        }
    }
    // Every container and emulator systems.toml names is in the estate.
    let mut missing = BTreeSet::new();
    for (id, e) in &estate.catalog.systems {
        for s in e.services.iter().chain(&e.extra_targets) {
            if let Some(c) = s.compose_service()
                && !estate.catalog.is_loams_module(s)
                && !compose.services.contains_key(c)
            {
                missing.insert(format!("{id}: {c}"));
            }
        }
    }
    assert_eq!(missing, BTreeSet::new());
}

#[test]
fn nightly_secrets_are_wired_into_the_nightly_job() {
    let estate = estate();
    let wf = std::fs::read_to_string(repository_root().join(".github/workflows/fabric.yml"))
        .expect("fabric.yml");
    let doc: serde_norway::Value = serde_norway::from_str(&wf).expect("fabric.yml parses");
    let env = doc["jobs"]["connectors-nightly"]["env"]
        .as_mapping()
        .expect("connectors-nightly has an env block");
    let wired: BTreeSet<&str> = env.keys().filter_map(|k| k.as_str()).collect();
    let mut missing = BTreeSet::new();
    for (id, e) in &estate.catalog.systems {
        for s in &e.services {
            if let ServiceDecl::Nightly { env, .. } = s {
                for v in env {
                    if !wired.contains(v.as_str()) {
                        missing.insert(format!("{id}: {v}"));
                    }
                }
            }
        }
    }
    assert_eq!(missing, BTreeSet::new());
}

#[test]
fn services_are_unavailable_until_their_stack_is_up() {
    let estate = estate();
    let mut env = RunEnv::from_estate(estate, Default::default());
    let svc = ServiceKind::Container {
        compose_service: "rabbitmq",
        system: "RabbitMQ",
    };
    let why = env.available(&svc).unwrap_err();
    assert!(why.contains("LOAMS_CONNECTORS_P2_STACK"), "{why}");
    env.vars
        .insert("LOAMS_CONNECTORS_P2_STACK".to_owned(), "1".to_owned());
    assert_eq!(env.available(&svc), Ok(()));
    // A ci-service image needs the CI-only stack, not the P2 one.
    let ms = ServiceKind::Container {
        compose_service: "sqlserver",
        system: "SQL Server",
    };
    assert!(
        env.available(&ms)
            .unwrap_err()
            .contains("LOAMS_CONNECTORS_CI_ONLY_STACK")
    );
    // A fixture with no recording yet is not a service, and neither is one
    // that holds only synthetic fixtures (CN2 Task 20, R20.1).
    let fx = ServiceKind::Fixture {
        dir: "conformance/connectors/fixtures/stripe",
        recorded_from: "Stripe",
    };
    let why = env.available(&fx).unwrap_err();
    assert!(why.contains("only synthetic"), "{why}");
    let none = ServiceKind::Fixture {
        dir: "conformance/connectors/fixtures/openai",
        recorded_from: "OpenAI",
    };
    assert!(env.available(&none).is_err());
    let night = ServiceKind::Nightly {
        env: &["LOAMS_STRIPE_API_KEY"],
        system: "Stripe",
    };
    assert!(env.available(&night).is_err());
}
