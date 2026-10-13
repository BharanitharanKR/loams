//! The crate's dependency graph, read from `fabric/Cargo.lock` (what
//! `cargo tree -p loams-connector-protocols -e normal` walks, without running
//! cargo inside a test; ruling R21.11).
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};

/// name → dependency names, for every package in the lock.
fn lock() -> BTreeMap<String, Vec<String>> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock");
    let text = std::fs::read_to_string(path).unwrap();
    let mut out = BTreeMap::new();
    for pkg in text.split("[[package]]").skip(1) {
        let name = pkg
            .lines()
            .find_map(|l| l.strip_prefix("name = \""))
            .unwrap()
            .trim_end_matches('"')
            .to_owned();
        let deps: Vec<String> = pkg
            .split("dependencies = [")
            .nth(1)
            .map(|d| {
                d.split(']')
                    .next()
                    .unwrap_or("")
                    .lines()
                    .filter_map(|l| l.trim().strip_prefix('"'))
                    .map(|l| {
                        l.trim_end_matches(['"', ','])
                            .split(' ')
                            .next()
                            .unwrap()
                            .to_owned()
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.entry(name).or_insert_with(Vec::new).extend(deps);
    }
    out
}

/// Every crate reachable from this one (dev-dependencies included, which
/// over-approximates the shipped graph: fine for a ban list).
fn closure() -> BTreeSet<String> {
    let lock = lock();
    let mut seen = BTreeSet::new();
    let mut todo = vec!["loams-connector-protocols".to_owned()];
    while let Some(n) = todo.pop() {
        if seen.insert(n.clone())
            && let Some(deps) = lock.get(&n)
        {
            todo.extend(deps.iter().cloned());
        }
    }
    seen
}

#[test]
fn no_bolt_dependency() {
    let all = closure();
    assert!(
        all.contains("prost-reflect") && all.contains("snap"),
        "the walk found the graph"
    );
    // D634 (c): Neo4j only over its HTTP Query API.
    let bolt: Vec<_> = all
        .iter()
        .filter(|n| n.contains("bolt") || n.starts_with("neo4rs") || n.starts_with("neo4j"))
        .collect();
    assert!(bolt.is_empty(), "a Bolt client is in the graph: {bolt:?}");
}

#[test]
fn every_connection_goes_through_the_sdk() {
    // R21.1, R21.2: no HTTP or gRPC stack that dials outside the SDK's
    // egress check, and no vendor SDK.
    let all = closure();
    let banned: Vec<_> = all
        .iter()
        .filter(|n| {
            matches!(
                n.as_str(),
                "reqwest" | "tonic" | "tokio-tungstenite" | "tungstenite" | "async-openai"
            ) || n.starts_with("aws-sdk-")
                || n.starts_with("aws-config")
        })
        .collect();
    assert!(banned.is_empty(), "{banned:?}");
}
