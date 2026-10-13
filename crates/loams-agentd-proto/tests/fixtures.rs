//! Canonical fixtures for the wire types (design §50 §8.3, plan DD1 Task 9).
//!
//! `support::write_fixtures` writes `fixtures/*.json` from the typed
//! constructors in `support/fixtures.rs` (the `export_bindings_fixtures` test,
//! so `cargo test -p loams-agentd-proto export_bindings` regenerates both the
//! TypeScript and the fixtures); `fixtures_roundtrip` checks that the committed
//! files are current and that every fixture deserializes and re-serializes
//! to identical canonical JSON; `fixtures_cover_every_type` that every type
//! of the method table, and every variant of the tagged unions the UI
//! switches on, has one.

#![allow(clippy::unwrap_used)]

mod support;

use std::collections::BTreeSet;

use loams_agentd_proto::rpc::METHODS;

#[test]
fn export_bindings_fixtures() {
    support::write_fixtures();
}

#[test]
fn fixtures_roundtrip() {
    let diff = support::diff(&support::fixture_files(), support::committed_fixtures());
    assert!(
        diff.is_empty(),
        "crates/loams-agentd-proto/fixtures is out of date; run \
         `cargo test -p loams-agentd-proto export_bindings`:\n  {}",
        diff.join("\n  ")
    );
    for fixture in support::fixtures::all() {
        let again = (fixture.roundtrip)(&fixture.value).unwrap_or_else(|e| {
            panic!(
                "{} does not parse as {}: {e}",
                fixture.name, fixture.ts_type
            )
        });
        assert_eq!(
            support::canonical(&again),
            support::canonical(&fixture.value),
            "{} changes on a round trip",
            fixture.name
        );
    }
}

/// The tags of a union's variants, read from its TypeScript declaration.
fn tags(declaration: &str, tag: &str) -> BTreeSet<String> {
    let needle = format!("\"{tag}\": \"");
    let mut out = BTreeSet::new();
    let mut rest = declaration;
    while let Some(at) = rest.find(&needle) {
        rest = &rest[at + needle.len()..];
        out.insert(rest[..rest.find('"').unwrap()].to_owned());
    }
    out
}

fn covered<T: ts_rs::TS + 'static>(tag: &str) {
    let cfg = support::ts_config(std::path::Path::new("."));
    let name = T::name(&cfg);
    let expected = tags(&T::decl(&cfg), tag);
    assert!(!expected.is_empty(), "{name} has no `{tag}` variants");
    let present: BTreeSet<String> = support::fixtures::all()
        .iter()
        .filter(|f| f.ts_type == name)
        .filter_map(|f| f.value[tag].as_str().map(str::to_owned))
        .collect();
    let missing: Vec<_> = expected.difference(&present).collect();
    assert!(
        missing.is_empty(),
        "{name} variants without a fixture: {missing:?}"
    );
}

#[test]
fn fixtures_cover_every_type() {
    let names: BTreeSet<String> = support::fixtures::all()
        .iter()
        .map(|f| f.ts_type.clone())
        .collect();
    let missing: BTreeSet<&str> = METHODS
        .iter()
        .flat_map(|m| [m.params, m.result])
        .filter(|ty| !names.contains(*ty))
        .collect();
    assert!(
        missing.is_empty(),
        "method types without a fixture: {missing:?}"
    );

    covered::<loams_agentd_proto::AgentEvent>("type");
    covered::<loams_agentd_proto::ToolCall>("kind");
    covered::<loams_agentd_proto::MessagePart>("kind");
    covered::<loams_agentd_proto::SessionCommandPayload>("kind");
    covered::<loams_agentd_proto::MutateParams>("op");
}
