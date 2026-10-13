//! The TypeScript contract (design §50 §8, plan DD1 Task 9).
//!
//! - `export_bindings` writes `web/packages/agentd-client/src/gen/**` and
//!   `src/fixtures.gen.ts`. Run it after any wire change:
//!   `cargo test -p loams-agentd-proto export_bindings`.
//! - `ts_bindings_are_fresh` regenerates under the target directory and fails
//!   when the committed files differ (it compares against the files as they
//!   were before `export_bindings` ran in this process).
//! - `methods_unique_and_complete` checks the method table against the
//!   dispatch arms of the engine and the daemon.
//! - `ts_requests_parse` parses every request the TypeScript builders wrote.

#![allow(clippy::unwrap_used)]

mod support;

use std::collections::BTreeSet;

use loams_agentd_proto::rpc::{METHODS, Method, MethodVisitor, methods, visit_methods};

#[test]
fn export_bindings() {
    // Read the committed state first, so `ts_bindings_are_fresh` in this
    // process compares against what was there before.
    support::committed_bindings();
    let files = support::generate();
    support::write_tree(
        &support::client_dir().join("src"),
        |rel| rel.starts_with("gen/") || rel == "fixtures.gen.ts",
        &files,
    );
}

#[test]
fn ts_bindings_are_fresh() {
    let committed = support::committed_bindings();
    let generated = support::generate();
    let diff = support::diff(&generated, committed);
    assert!(
        diff.is_empty(),
        "web/packages/agentd-client/src is out of date; run \
         `cargo test -p loams-agentd-proto export_bindings` and commit the result:\n  {}",
        diff.join("\n  ")
    );
}

/// The `methods::X` names a dispatch function matches on.
fn dispatched(source: &str, function: &str) -> BTreeSet<String> {
    let start = source
        .find(function)
        .unwrap_or_else(|| panic!("{function} not found"));
    let body = &source[start..];
    let body = &body[..body.find("\n#[cfg(test)]").unwrap_or(body.len())];
    let mut names = BTreeSet::new();
    let mut rest = body;
    while let Some(at) = rest.find("methods::") {
        rest = &rest[at + "methods::".len()..];
        let konst: String = rest
            .chars()
            .take_while(|c| c.is_ascii_uppercase() || *c == '_')
            .collect();
        let after = rest[konst.len()..].trim_start();
        if !konst.is_empty() && (after.starts_with("=>") || after.starts_with('|')) {
            names.insert(konst);
        }
    }
    names
}

/// `ListHarnesses` → `LIST_HARNESSES`.
fn constant(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

#[test]
fn methods_unique_and_complete() {
    let names: Vec<&str> = METHODS.iter().map(|m| m.name).collect();
    let unique: BTreeSet<&str> = names.iter().copied().collect();
    assert_eq!(unique.len(), names.len(), "duplicate method names");
    assert_eq!(methods::HELLO, "Hello");
    assert_eq!(methods::SHUTDOWN, "Shutdown");

    // The engine's dispatch arms, plus the two methods the daemon serves in
    // front of it (`Hello` in hello.rs, `Shutdown` in server.rs).
    let root = support::repo_root().join("crates");
    let read = |path: &str| std::fs::read_to_string(root.join(path)).expect(path);
    let mut arms = dispatched(
        &read("loams-agentd-sessions/src/rpc.rs"),
        "impl RpcService for EngineRpc",
    );
    // The engine's match has no arm for the daemon's own methods.
    assert!(!arms.contains("HELLO") && !arms.contains("SHUTDOWN"));
    let hello = read("loams-agentd/src/hello.rs");
    assert!(
        hello.contains("method != methods::HELLO"),
        "hello.rs serves Hello"
    );
    let server = read("loams-agentd/src/server.rs");
    assert!(
        server.contains("method != methods::SHUTDOWN"),
        "server.rs serves Shutdown"
    );
    arms.insert("HELLO".into());
    arms.insert("SHUTDOWN".into());

    let table: BTreeSet<String> = METHODS.iter().map(|m| constant(m.name)).collect();
    let unserved: Vec<_> = table.difference(&arms).collect();
    let unlisted: Vec<_> = arms.difference(&table).collect();
    assert!(
        unserved.is_empty(),
        "in METHODS but not dispatched: {unserved:?}"
    );
    assert!(
        unlisted.is_empty(),
        "dispatched but not in METHODS: {unlisted:?}"
    );

    // A dispatch on a string literal would bypass the table.
    let engine = read("loams-agentd-sessions/src/rpc.rs");
    for name in &names {
        assert!(
            !engine.contains(&format!("\"{name}\" =>")),
            "{name} is dispatched by a string literal"
        );
    }
}

#[test]
fn ts_requests_parse() {
    let path = support::client_dir().join("test/requests.json");
    let text = std::fs::read_to_string(&path).expect("read requests.json");
    let requests: Vec<serde_json::Value> = serde_json::from_str(&text).expect("requests.json");

    struct Parse<'a> {
        requests: &'a [serde_json::Value],
        seen: BTreeSet<&'static str>,
        failures: Vec<String>,
    }
    impl MethodVisitor for Parse<'_> {
        fn visit<M: Method>(&mut self) {
            for request in self.requests {
                if request["method"] != M::NAME {
                    continue;
                }
                self.seen.insert(M::NAME);
                if let Err(e) = serde_json::from_value::<M::Params>(request["params"].clone()) {
                    self.failures
                        .push(format!("{}: {e}: {}", M::NAME, request["params"]));
                }
            }
        }
    }
    let mut parse = Parse {
        requests: &requests,
        seen: BTreeSet::new(),
        failures: Vec::new(),
    };
    visit_methods(&mut parse);
    assert!(parse.failures.is_empty(), "{:#?}", parse.failures);

    let unknown: Vec<_> = requests
        .iter()
        .filter(|r| !METHODS.iter().any(|m| r["method"] == m.name))
        .collect();
    assert!(
        unknown.is_empty(),
        "requests for unknown methods: {unknown:?}"
    );
    let missing: Vec<_> = METHODS
        .iter()
        .filter(|m| !parse.seen.contains(m.name))
        .map(|m| m.name)
        .collect();
    assert!(missing.is_empty(), "no TypeScript request for: {missing:?}");
}
