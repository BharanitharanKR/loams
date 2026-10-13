//! `loams.repos.v1` (GT1 Task 12; design §36 §19.10, plan "Public API"):
//! the descriptor carries `RepoService` as the plan's contract writes it,
//! every mutation takes an `idempotency_key`, every read of one repository
//! takes a `consistency_token`, every list pages as AP0 says, and every
//! reason the package names is registered.
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::method_options::IdempotencyLevel;
use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto};

const PACKAGE: &str = "loams.repos.v1";
const SERVICE: &str = "RepoService";

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = workspace().join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn descriptor_set() -> FileDescriptorSet {
    FileDescriptorSet::decode(loams_proto::FILE_DESCRIPTOR_SET).expect("the descriptor set decodes")
}

fn files(set: &FileDescriptorSet) -> Vec<&FileDescriptorProto> {
    let files: Vec<_> = set.file.iter().filter(|f| f.package() == PACKAGE).collect();
    assert!(
        !files.is_empty(),
        "{PACKAGE} is not in loams-proto's descriptor set (crates/loams-proto/build.rs FILES)"
    );
    files
}

fn methods(set: &FileDescriptorSet) -> Vec<MethodDescriptorProto> {
    files(set)
        .into_iter()
        .flat_map(|f| f.service.iter())
        .find(|s| s.name() == SERVICE)
        .unwrap_or_else(|| panic!("no {SERVICE} in {PACKAGE}"))
        .method
        .clone()
}

/// Every message by its fully qualified name, nested ones included.
fn messages(set: &FileDescriptorSet) -> BTreeMap<String, DescriptorProto> {
    fn walk(prefix: &str, m: &DescriptorProto, out: &mut BTreeMap<String, DescriptorProto>) {
        let name = format!("{prefix}.{}", m.name());
        for nested in &m.nested_type {
            walk(&name, nested, out);
        }
        out.insert(name, m.clone());
    }
    let mut out = BTreeMap::new();
    for f in &set.file {
        for m in &f.message_type {
            walk(&format!(".{}", f.package()), m, &mut out);
        }
    }
    out
}

fn read_only(m: &MethodDescriptorProto) -> bool {
    m.options
        .as_ref()
        .is_some_and(|o| o.idempotency_level() == IdempotencyLevel::NoSideEffects)
}

/// The RPCs of the plan's contract, each with whether it reads only
/// (`NO_SIDE_EFFECTS`) and whether it streams.
const CONTRACT: &[(&str, bool, bool)] = &[
    ("CreateRepo", false, false),
    ("GetRepo", true, false),
    ("ListRepos", true, false),
    ("ForkRepo", false, false),
    ("DeleteRepo", false, false),
    ("UpdateRepoConfig", false, false),
    ("ListRefs", true, false),
    ("CompactRepo", false, false),
    ("WatchRepo", true, true),
];

/// Reads that do not address one repository, so have no seq to pin.
const CATALOG_READS: &[&str] = &["ListRepos"];

/// The package's four files are registered, in one package.
#[test]
fn the_package_has_its_four_files() {
    let set = descriptor_set();
    let names: BTreeSet<&str> = files(&set).iter().map(|f| f.name()).collect();
    let want: BTreeSet<&str> = [
        "loams/repos/v1/repos.proto",
        "loams/repos/v1/workspaces.proto",
        "loams/repos/v1/objects.proto",
        "loams/repos/v1/env.proto",
    ]
    .into_iter()
    .collect();
    assert_eq!(names, want);
}

/// The service is the contract's, RPC for RPC.
#[test]
fn the_service_is_the_shared_contract() {
    let set = descriptor_set();
    let got: BTreeMap<String, (bool, bool)> = methods(&set)
        .iter()
        .map(|m| {
            (
                m.name().to_string(),
                (read_only(m), m.server_streaming() || m.client_streaming()),
            )
        })
        .collect();
    let want: BTreeMap<String, (bool, bool)> = CONTRACT
        .iter()
        .map(|(n, r, s)| (n.to_string(), (*r, *s)))
        .collect();
    assert_eq!(got, want);
}

/// Every RPC that is not `NO_SIDE_EFFECTS` takes `string idempotency_key =
/// 15` (AP0 rules).
#[test]
fn every_mutation_has_idempotency_key() {
    let set = descriptor_set();
    let msgs = messages(&set);
    let mut checked = 0;
    for m in methods(&set).iter().filter(|m| !read_only(m)) {
        let input = &msgs[m.input_type()];
        let field = input
            .field
            .iter()
            .find(|f| f.number() == 15)
            .unwrap_or_else(|| panic!("{}: {} has no field 15", m.name(), input.name()));
        assert_eq!(field.name(), "idempotency_key", "{}", m.name());
        assert_eq!(field.r#type(), Type::String, "{}", m.name());
        assert_eq!(field.label(), Label::Optional, "{}: not repeated", m.name());
        assert!(!field.proto3_optional(), "{}: a plain string", m.name());
        checked += 1;
    }
    assert_eq!(checked, CONTRACT.iter().filter(|(_, r, _)| !r).count());
}

/// Every read of one repository takes `string consistency_token = 14` and
/// answers a `consistency_token` (§36 §4.6); the catalog read takes none.
#[test]
fn reads_take_consistency_token() {
    let set = descriptor_set();
    let msgs = messages(&set);
    let mut checked = 0;
    for m in methods(&set).iter().filter(|m| read_only(m)) {
        let input = &msgs[m.input_type()];
        let token = input.field.iter().find(|f| f.number() == 14);
        if CATALOG_READS.contains(&m.name()) {
            assert!(token.is_none(), "{} reads no seq", m.name());
            continue;
        }
        let token = token.unwrap_or_else(|| panic!("{}: no field 14", m.name()));
        assert_eq!(token.name(), "consistency_token", "{}", m.name());
        assert_eq!(token.r#type(), Type::String, "{}", m.name());
        let output = &msgs[m.output_type()];
        let answered = output
            .field
            .iter()
            .find(|f| f.name() == "consistency_token")
            .unwrap_or_else(|| panic!("{}: {} answers no token", m.name(), output.name()));
        assert_eq!(answered.r#type(), Type::String, "{}", m.name());
        checked += 1;
    }
    assert_eq!(checked, 3, "GetRepo, ListRefs and WatchRepo");
}

/// AP0 pagination: every `List*` takes `int32 page_size` and `string
/// page_token`, and answers one repeated resource and `next_page_token`.
#[test]
fn list_rpcs_page_as_ap0_says() {
    let set = descriptor_set();
    let msgs = messages(&set);
    let field = |m: &DescriptorProto, name: &str| {
        m.field
            .iter()
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("{} has no {name}", m.name()))
            .clone()
    };
    let lists: Vec<MethodDescriptorProto> = methods(&set)
        .into_iter()
        .filter(|m| m.name().starts_with("List"))
        .collect();
    assert_eq!(lists.len(), 2);
    for m in &lists {
        let req = &msgs[m.input_type()];
        assert_eq!(
            field(req, "page_size").r#type(),
            Type::Int32,
            "{}",
            m.name()
        );
        assert_eq!(
            field(req, "page_token").r#type(),
            Type::String,
            "{}",
            m.name()
        );
        let resp = &msgs[m.output_type()];
        assert_eq!(
            field(resp, "next_page_token").r#type(),
            Type::String,
            "{}",
            m.name()
        );
        let repeated = resp
            .field
            .iter()
            .filter(|f| f.label() == Label::Repeated && f.r#type() == Type::Message)
            .count();
        assert_eq!(repeated, 1, "{}", m.name());
    }
}

/// `DeleteRepo` and `CompactRepo` answer an `Operation` whose kind the
/// proto's comment lists.
#[test]
fn slow_rpcs_answer_an_operation() {
    let text = read("proto/loams/repos/v1/repos.proto");
    for kind in ["repo.delete", "repo.compact"] {
        assert!(text.contains(&format!("`{kind}`")), "no {kind}");
    }
    let set = descriptor_set();
    let msgs = messages(&set);
    for rpc in ["DeleteRepo", "CompactRepo"] {
        let m = methods(&set).into_iter().find(|m| m.name() == rpc).unwrap();
        let out = &msgs[m.output_type()];
        assert!(
            out.field
                .iter()
                .any(|f| f.type_name() == ".loams.operations.v1.Operation"),
            "{rpc}"
        );
    }
}

/// The snake_case words in backticks on the lines of `text` that `take`
/// selects, minus field names.
fn backticked_reasons(text: &str, take: impl Fn(&str) -> bool) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in text.lines().filter(|l| take(l)) {
        for (i, tok) in line.split('`').enumerate() {
            let snake = !tok.is_empty()
                && tok.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                && tok.contains('_');
            if i % 2 == 1 && snake && !tok.ends_with("_key") && !tok.ends_with("_token") {
                out.insert(tok.to_string());
            }
        }
    }
    out
}

/// The reasons the package names: the proto's Errors bullet and §36
/// §19.10's "New reasons" line.
fn package_reasons() -> BTreeSet<String> {
    let proto = read("proto/loams/repos/v1/repos.proto");
    let start = proto
        .find("// - **Errors**")
        .expect("the proto has an Errors bullet");
    let end = proto[start..]
        .find("\npackage ")
        .map_or(proto.len(), |e| start + e);
    let mut reasons = backticked_reasons(&proto[start..end], |_| true);
    let design = read("docs/design/36-loams-git.md");
    reasons.extend(backticked_reasons(&design, |l| {
        l.starts_with("New reasons:")
    }));
    reasons
}

/// Every reason the package names is a row of `docs/api/reasons.md`, and
/// the plan's and §19.10's lists are all named (all registered now, before
/// any RPC raises them).
#[test]
fn reasons_registered() {
    let reasons = package_reasons();
    for must in [
        "repo_not_found",
        "repo_already_exists",
        "repo_deleting",
        "ref_stale",
        "ref_protected",
        "out_of_scope",
        "object_format_unsupported",
        "fork_too_deep",
        "quota_exceeded",
        "sequencer_unavailable",
        "workspace_not_found",
        "workspace_fenced",
        "workspace_read_only",
        "workspace_name_taken",
        "object_missing",
        "secret_detected",
        "env_unsealed",
        "env_access_denied",
        "env_foreign_reference",
    ] {
        assert!(reasons.contains(must), "{must} is not named: {reasons:?}");
    }
    let registry: BTreeSet<String> = read("docs/api/reasons.md")
        .lines()
        .filter_map(|l| l.strip_prefix("| `"))
        .filter_map(|l| l.split('`').next())
        .map(str::to_string)
        .collect();
    let missing: Vec<_> = reasons.difference(&registry).collect();
    assert!(
        missing.is_empty(),
        "not in docs/api/reasons.md: {missing:?}"
    );
}

/// The route map's planned `loams.repos.v1` section: every row names an RPC
/// of the service, and every RPC has a row.
#[test]
fn every_rpc_has_a_planned_console_route() {
    let text = read("docs/api/route-map.md");
    let start = text
        .find("## Planned: `loams.repos.v1`")
        .expect("docs/api/route-map.md has the planned loams.repos.v1 section");
    let end = text[start + 1..]
        .find("\n## ")
        .map_or(text.len(), |e| start + 1 + e);
    let prefix = format!("`{PACKAGE}.{SERVICE}/");
    let mut routed = BTreeSet::new();
    let mut pairs = BTreeSet::new();
    for line in text[start..end].lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() < 5 || !cells[1].starts_with("`/v1/namespaces/{ns}/repos") {
            continue;
        }
        assert!(
            pairs.insert((cells[1].to_string(), cells[2].to_string())),
            "route {} {} is listed twice",
            cells[2],
            cells[1]
        );
        assert!(
            ["GET", "POST", "PATCH", "DELETE"].contains(&cells[2]),
            "bad method in {line}"
        );
        let rpc = cells[3]
            .strip_prefix(&prefix)
            .and_then(|r| r.strip_suffix('`'))
            .unwrap_or_else(|| panic!("no {prefix}…` RPC in {line}"));
        routed.insert(rpc.to_string());
    }
    let all: BTreeSet<String> = CONTRACT.iter().map(|(n, _, _)| n.to_string()).collect();
    assert_eq!(routed, all);
}
