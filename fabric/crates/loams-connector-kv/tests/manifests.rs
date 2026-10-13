//! The family's manifests and harnesses, checked without any service: the
//! proposed manifests (ruling R14.1) validate, name this crate's factories
//! and their profile, and every harness runs against services
//! `conformance/connectors/systems.toml` declares for its manifest (OR-2).

mod common;

use loams_connector_kv::harness::cassandra::Cassandra;
use loams_connector_kv::harness::dynamodb::{DynamoDb, DynamoDbStreams};
use loams_connector_kv::harness::redis::{Streams, ValkeyKeys};
use loams_connector_kv::harness::{KvSystem, proposed_manifest};
use loams_flow_conformance::{Estate, Suite, check_services, required_suites};

const IDS: &[&str] = &[
    "valkey",
    "redis-streams",
    "dynamodb",
    "dynamodb-streams",
    "cassandra",
];

#[test]
fn proposed_manifests_validate() {
    let ids: Vec<&str> = loams_connector_kv::factories()
        .iter()
        .map(|f| f.id())
        .collect();
    for id in IDS {
        let (spec, profile) = proposed_manifest(id).unwrap_or_else(|e| panic!("{id}: {e:?}"));
        assert_eq!(spec.id, *id);
        assert!(ids.contains(id), "{id} has a factory");
        assert!(
            spec.runtime.reference.starts_with("loams_connector_kv::"),
            "{id}: {}",
            spec.runtime.reference
        );
        assert_eq!(
            profile.is_some(),
            *id == "valkey",
            "{id}: only Valkey is a target profile"
        );
        // Every suite a declared capability needs is in the manifest's list,
        // so CI runs each on its own rather than in small under `contract`.
        let declared = Suite::declared_by(&spec);
        for s in required_suites(&spec, profile.as_ref()) {
            assert!(
                declared.contains(&s),
                "{id}: {s} is needed by a capability but not declared"
            );
        }
    }
}

#[test]
fn harness_services_are_declared_for_their_manifests() {
    let estate = Estate::load_default().expect("the estate");
    let check = |sys: &dyn KvSystemDyn| {
        let (spec, profile) = proposed_manifest(sys.id()).expect("manifest");
        check_services(&spec.id, profile.as_ref(), sys.services(), &estate.catalog)
            .unwrap_or_else(|e| panic!("{}: {e:?}", sys.id()));
    };
    check(&ValkeyKeys::new());
    check(&Streams::valkey());
    check(&Streams::redis());
    check(&DynamoDb::new());
    check(&DynamoDbStreams::new());
    check(&Cassandra::new());
    let entries: Vec<_> = IDS
        .iter()
        .map(|id| estate.catalog.get(id).expect("entry"))
        .collect();
    for e in entries {
        assert_eq!(e.family, "kv");
        assert!(
            e.harness
                .as_deref()
                .is_some_and(|h| h.starts_with("loams_connector_kv::harness::")),
            "{:?}",
            e.harness
        );
    }
}

/// Object-safe view of a system for the check above.
trait KvSystemDyn {
    fn id(&self) -> &'static str;
    fn services(&self) -> &'static [loams_flow_conformance::ServiceKind];
}

impl<S: KvSystem> KvSystemDyn for S {
    fn id(&self) -> &'static str {
        KvSystem::id(self)
    }
    fn services(&self) -> &'static [loams_flow_conformance::ServiceKind] {
        KvSystem::services(self)
    }
}
