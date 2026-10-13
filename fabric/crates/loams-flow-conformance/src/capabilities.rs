//! Which suite proves which declared capability (D353 rule 3, CN1's
//! `every_declared_capability_has_a_test`, extended by CN2 to every
//! `preview` manifest).
//!
//! The manifest is read through its JSON form, not its Rust fields, so a
//! capability a later schema adds (CN2 Task 1's `sink.reply`, say) shows up
//! here as *unchecked* until the kit registers a check for it, rather than
//! being silently ignored.

use std::collections::BTreeSet;

use loams_connector_sdk::Profile;
use loams_flow::ConnectorSpec;
use serde_json::Value;

use crate::Suite;

/// One declared capability and the suite that proves it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CapabilityCheck {
    /// A capability path: `source.cdc`, `sink.transactional`,
    /// `delivery.sink.exactly_once`, `bulk.arrow`, `auth.basic`, …
    pub capability: String,
    pub suite: Suite,
}

/// The registered checks: a capability path (or a path prefix ending in
/// `.`) and its suite. Every path a manifest declares must match one.
const CHECKS: &[(&str, Suite)] = &[
    ("source.streaming", Suite::Roundtrip),
    ("source.batch", Suite::Roundtrip),
    ("source.webhook", Suite::Roundtrip),
    ("source.cdc", Suite::Cdc),
    ("source.resumable", Suite::KillRestart),
    ("sink.streaming", Suite::Roundtrip),
    ("sink.batch", Suite::Roundtrip),
    ("sink.transactional", Suite::ExactlyOnce),
    ("sink.upsert", Suite::DupCheck),
    ("sink.delete", Suite::Roundtrip),
    ("sink.idempotent", Suite::DupCheck),
    // CN2 Task 1 (OR-19): one reply event per input, read back.
    ("sink.reply", Suite::Roundtrip),
    ("delivery.source.at_most_once", Suite::Contract),
    ("delivery.source.at_least_once", Suite::KillRestart),
    ("delivery.source.effectively_once", Suite::DupCheck),
    ("delivery.source.exactly_once", Suite::ExactlyOnce),
    ("delivery.sink.at_most_once", Suite::Contract),
    ("delivery.sink.at_least_once", Suite::KillRestart),
    ("delivery.sink.effectively_once", Suite::DupCheck),
    ("delivery.sink.exactly_once", Suite::ExactlyOnce),
    ("bulk.arrow", Suite::Bulk),
    ("schema.evolution.", Suite::SchemaEvolution),
    ("backpressure.", Suite::Backpressure),
    ("auth.none", Suite::Contract),
    ("auth.", Suite::Auth),
];

fn check_for(path: &str) -> Option<Suite> {
    CHECKS
        .iter()
        .find(|(p, _)| {
            if p.ends_with('.') {
                path.starts_with(p)
            } else {
                path == *p
            }
        })
        .map(|(_, s)| *s)
}

/// Whether a profile narrows the capability away (`narrows` holds paths,
/// with or without a leading `capabilities.`; a path also narrows what is
/// below it).
fn narrowed(path: &str, profile: Option<&Profile>) -> bool {
    profile.is_some_and(|p| {
        p.narrows.iter().any(|n| {
            let n = n.strip_prefix("capabilities.").unwrap_or(n);
            path == n
                || path
                    .strip_prefix(n)
                    .is_some_and(|rest| rest.starts_with('.'))
        })
    })
}

/// Every capability the manifest declares, as paths, minus what the profile
/// narrows.
pub fn declared_capabilities(spec: &ConnectorSpec, profile: Option<&Profile>) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(doc) = spec.to_json() else {
        return out;
    };
    let caps = &doc["capabilities"];
    let mut directions = Vec::new();
    for dir in ["source", "sink"] {
        if let Some(obj) = caps.get(dir).and_then(Value::as_object) {
            directions.push(dir);
            for (k, v) in obj {
                if v.as_bool() == Some(true) {
                    out.push(format!("{dir}.{k}"));
                }
            }
        }
    }
    if let Some(delivery) = caps.get("delivery").and_then(Value::as_object) {
        for (dir, v) in delivery {
            if directions.contains(&dir.as_str())
                && let Some(slug) = v.as_str()
            {
                out.push(format!("delivery.{dir}.{slug}"));
            }
        }
    }
    if caps["bulk"]["arrow"].as_bool() == Some(true) {
        out.push("bulk.arrow".to_owned());
    }
    if let Some(ev) = caps["schema"]["evolution"].as_str()
        && ev != "none"
    {
        out.push(format!("schema.evolution.{ev}"));
    }
    if let Some(bp) = caps["backpressure"].as_str() {
        out.push(format!("backpressure.{bp}"));
    }
    if let Some(auth) = doc["auth"].as_array() {
        for a in auth.iter().filter_map(Value::as_str) {
            out.push(format!("auth.{a}"));
        }
    }
    out.retain(|p| !narrowed(p, profile));
    out
}

/// Each declared capability with the suite that proves it. A capability with
/// no registered check is left out (see [`unchecked_capabilities`]).
pub fn capability_checks(spec: &ConnectorSpec, profile: Option<&Profile>) -> Vec<CapabilityCheck> {
    declared_capabilities(spec, profile)
        .into_iter()
        .filter_map(|capability| {
            check_for(&capability).map(|suite| CapabilityCheck { capability, suite })
        })
        .collect()
}

/// The declared capabilities the kit has no check for. Empty for every
/// manifest that may be `preview`.
pub fn unchecked_capabilities(spec: &ConnectorSpec, profile: Option<&Profile>) -> Vec<String> {
    declared_capabilities(spec, profile)
        .into_iter()
        .filter(|c| check_for(c).is_none())
        .collect()
}

/// The suites a manifest's declared capabilities need, plus `contract`.
pub fn required_suites(spec: &ConnectorSpec, profile: Option<&Profile>) -> BTreeSet<Suite> {
    let mut s: BTreeSet<Suite> = capability_checks(spec, profile)
        .into_iter()
        .map(|c| c.suite)
        .collect();
    s.insert(Suite::Contract);
    s
}
