//! Per-graph authorization (GR1 Task 24, design §48 §11, D750), against the `GraphAuthorizer`
//! trait with the built-in RBAC ([`StaticRbac`]) until MT1's interceptor lands: every RPC's check
//! (§48 §11.1's table), the environment fence, the scopes, protected environments, `ListGraphs`'
//! filter, and the audit of every refusal.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use connectrpc::{ConnectError, ErrorCode};
use loams_common::meta::MetaStore;
use loams_graph::Engine;
use loams_graph::authz::{self, Caller, CallerKind, Relation, StaticRbac};
use loams_graph::catalog::GraphCatalog;
use loams_graph::service::admin::GraphAdmin;
use loams_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock};
use loams_proto::loams::graph::v1 as pb;
use loams_store::Store;
use pb::__buffa::oneof::value::Kind;
use tempfile::TempDir;

const NS: &str = "acme";

/// A single-node metastore, a bucket and a data directory, for the life of a test.
struct Fixture {
    _node: MetaNode,
    meta: Arc<dyn MetaStore>,
    store: Store,
    data_dir: TempDir,
    _meta_dir: TempDir,
}

impl Fixture {
    async fn start() -> Self {
        let meta_dir = TempDir::new().expect("temp dir");
        let clock = Arc::new(SystemClock);
        let mut config = MetaConfig::new(1, meta_dir.path(), Store::in_memory());
        config.clock = clock.clone();
        let node = MetaNode::start(config, &Router::new())
            .await
            .expect("start meta");
        node.initialize([1]).await.expect("initialize");
        node.wait_for_leader(Duration::from_secs(30))
            .await
            .expect("leader");
        let client = MetaClient::new(node.clone(), vec![], clock, MetaClientConfig::default());
        Self {
            _node: node,
            meta: Arc::new(client),
            store: Store::in_memory(),
            data_dir: TempDir::new().expect("data dir"),
            _meta_dir: meta_dir,
        }
    }

    /// The runtime's own admin (no caller: Loams' internal calls), with `rbac` deciding RPCs.
    fn admin(&self, rbac: StaticRbac) -> GraphAdmin {
        GraphAdmin::new(
            Arc::new(Engine::with_data_dir(self.data_dir.path())),
            GraphCatalog::new(self.meta.clone(), self.store.clone()),
        )
        .with_authorizer(Arc::new(rbac))
    }
}

fn reason(err: &ConnectError) -> String {
    use base64::Engine as _;
    use buffa::Message as _;
    let Some(detail) = err
        .details
        .iter()
        .find(|d| d.type_url == "loams.errors.v1.ErrorInfo")
    else {
        return String::new();
    };
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(detail.value.as_deref().unwrap_or_default())
        .expect("unpadded base64");
    loams_proto::loams::errors::v1::ErrorInfo::decode_from_slice(&bytes)
        .expect("an ErrorInfo")
        .reason
}

fn assert_denied<T: std::fmt::Debug>(result: Result<T, ConnectError>, what: &str) {
    let err = result.expect_err(&format!("{what} is refused"));
    assert_eq!(err.code, ErrorCode::PermissionDenied, "{what}: {err:?}");
    assert_eq!(reason(&err), authz::DENIED, "{what}: {err:?}");
}

fn person(id: &str) -> Caller {
    Caller::new(id, CallerKind::Session)
}

fn scoped(id: &str, scopes: &[&str]) -> Caller {
    Caller {
        scopes: Some(
            scopes
                .iter()
                .map(ToString::to_string)
                .collect::<BTreeSet<_>>(),
        ),
        ..Caller::new(id, CallerKind::ApiKey)
    }
}

fn create(ns: &str, name: &str) -> pb::CreateGraphRequest {
    pb::CreateGraphRequest {
        namespace: ns.to_string(),
        name: name.to_string(),
        ..Default::default()
    }
}

fn get(ns: &str, name: &str) -> pb::GetGraphRequest {
    pb::GetGraphRequest {
        namespace: ns.to_string(),
        name: name.to_string(),
        ..Default::default()
    }
}

fn delete(ns: &str, name: &str) -> pb::DeleteGraphRequest {
    pb::DeleteGraphRequest {
        namespace: ns.to_string(),
        name: name.to_string(),
        ..Default::default()
    }
}

fn update(ns: &str, name: &str) -> pb::UpdateGraphRequest {
    pb::UpdateGraphRequest {
        graph: pb::Graph {
            namespace: ns.to_string(),
            name: name.to_string(),
            replicas: 0,
            ..Default::default()
        }
        .into(),
        update_mask: buffa_types::google::protobuf::FieldMask {
            paths: vec!["replicas".to_string()],
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn list(ns: &str) -> pb::ListGraphsRequest {
    pb::ListGraphsRequest {
        namespace: ns.to_string(),
        ..Default::default()
    }
}

fn statement(ns: &str, graph: &str, text: &str) -> pb::ExecuteRequest {
    pb::ExecuteRequest {
        namespace: ns.to_string(),
        graph: graph.to_string(),
        statement: text.to_string(),
        ..Default::default()
    }
}

fn batch(ns: &str, graph: &str, texts: &[&str]) -> pb::ExecuteBatchRequest {
    pb::ExecuteBatchRequest {
        namespace: ns.to_string(),
        graph: graph.to_string(),
        statements: texts
            .iter()
            .map(|text| pb::Statement {
                statement: (*text).to_string(),
                ..Default::default()
            })
            .collect(),
        atomic: true,
        ..Default::default()
    }
}

fn explain(ns: &str, graph: &str, text: &str) -> pb::ExplainRequest {
    pb::ExplainRequest {
        namespace: ns.to_string(),
        graph: graph.to_string(),
        statement: text.to_string(),
        ..Default::default()
    }
}

fn stream(ns: &str, graph: &str, text: &str) -> pb::ExecuteStreamRequest {
    pb::ExecuteStreamRequest {
        request: statement(ns, graph, text).into(),
        ..Default::default()
    }
}

/// The graph's node count and engine epoch: what a write would change.
async fn state(admin: &GraphAdmin, ns: &str, graph: &str) -> (i64, u64) {
    let rows = admin
        .execute(statement(ns, graph, "MATCH (n) RETURN count(n) AS c"))
        .await
        .expect("count");
    let count = match &rows.rows.as_option().expect("rows").rows[0].values[0].kind {
        Some(Kind::Int64(n)) => *n,
        other => panic!("{other:?}"),
    };
    let epoch = admin
        .open(ns, graph)
        .await
        .expect("open")
        .current_epoch()
        .as_u64();
    (count, epoch)
}

/// A graph `name` in `ns` with two people, made by Loams itself (no caller).
async fn seeded(admin: &GraphAdmin, ns: &str, name: &str) {
    admin.create_graph(create(ns, name)).await.expect("create");
    admin
        .execute(statement(
            ns,
            name,
            "INSERT (:Person {name: 'a'})-[:KNOWS]->(:Person {name: 'b'})",
        ))
        .await
        .expect("seed");
}

/// One case of the classify corpus (`conformance/graph/gql/classify/*.gql`): its text, and
/// whether the guard or the engine reads it as more than a read.
struct Case {
    file: String,
    text: String,
    writes: bool,
}

fn corpus() -> Vec<Case> {
    let dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance/graph/gql/classify");
    let mut out = Vec::new();
    for file in ["writes.gql", "ddl.gql", "refused.gql", "reads.gql"] {
        let text = std::fs::read_to_string(dir.join(file)).expect("a corpus file");
        for block in text.split("\n\n").filter(|b| !b.trim().is_empty()) {
            let body: Vec<&str> = block
                .lines()
                .filter(|line| !line.starts_with("#! "))
                .collect();
            let text = body.join("\n");
            let guard = loams_graph::classify::classify(&text, pb::QueryLanguage::Gql);
            let engine = loams_graph::classify::engine_classify(&text).ok();
            let writes = guard != loams_graph::Access::Read
                || engine.is_some_and(|e| e != loams_graph::Access::Read);
            out.push(Case {
                file: file.to_string(),
                text,
                writes,
            });
        }
    }
    out
}

/// Review Focus 3: a `reader` sends every write of the corpus (and the comment-hidden ones, the
/// procedure calls, a Cypher `MERGE`) through every statement RPC. Each is `PERMISSION_DENIED`
/// (or the gate's own refusal), and nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_cannot_write_any_corpus_write() {
    let fixture = Fixture::start().await;
    let admin = fixture.admin(StaticRbac::new().grant("reader", NS, Relation::Reader));
    seeded(&admin, NS, "kg").await;
    let reader = admin.for_caller(person("reader"));
    let mut writes = 0;
    for case in corpus() {
        let what = format!("{}: {}", case.file, case.text);
        // A read transaction may advance the engine's epoch, so the state is taken per case.
        let before = state(&admin, NS, "kg").await;
        let outcomes = [
            reader
                .execute(statement(NS, "kg", &case.text))
                .await
                .map(drop),
            reader
                .execute_batch(batch(NS, "kg", &["MATCH (n) RETURN n", &case.text]))
                .await
                .map(drop),
            reader
                .execute_stream(stream(NS, "kg", &case.text))
                .await
                .map(drop),
            reader
                .explain(explain(NS, "kg", &case.text))
                .await
                .map(drop),
        ];
        for outcome in outcomes {
            if case.writes {
                assert_denied(outcome, &what);
            } else if let Err(err) = outcome {
                // A read may still fail (a gate refusal, a parse error), but never as a denial.
                assert_ne!(reason(&err), authz::DENIED, "{what}: {err:?}");
            }
        }
        if case.writes {
            writes += 1;
            assert_eq!(
                state(&admin, NS, "kg").await,
                before,
                "{what} changed the graph"
            );
        }
    }
    let before = state(&admin, NS, "kg").await.0;
    assert!(writes >= 30, "only {writes} writing cases");
    // A Cypher MERGE on a GQL-only graph: refused for its access before its language.
    let mut cypher = statement(NS, "kg", "MERGE (:Person {name: 'c'})");
    cypher.language = pb::QueryLanguage::Cypher.into();
    assert_denied(reader.execute(cypher).await, "a Cypher MERGE");
    // The reader reads.
    reader
        .execute(statement(NS, "kg", "MATCH (n:Person) RETURN n.name"))
        .await
        .expect("a read");
    reader.get_graph(get(NS, "kg")).await.expect("GetGraph");
    reader
        .get_schema(pb::GetSchemaRequest {
            namespace: NS.into(),
            name: "kg".into(),
            ..Default::default()
        })
        .await
        .expect("GetSchema");
    assert_eq!(state(&admin, NS, "kg").await.0, before);
}

/// A `writer` writes but runs no DDL; an `admin` passes authorization (and DDL is then refused
/// by the engine path until R24.6).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_cannot_ddl() {
    let fixture = Fixture::start().await;
    let admin = fixture.admin(
        StaticRbac::new()
            .grant("writer", NS, Relation::Writer)
            .grant("boss", NS, Relation::Admin),
    );
    seeded(&admin, NS, "kg").await;
    let writer = admin.for_caller(person("writer"));
    let boss = admin.for_caller(person("boss"));
    writer
        .execute(statement(NS, "kg", "INSERT (:Person {name: 'w'})"))
        .await
        .expect("a writer writes");
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/graph/gql/classify/ddl.gql");
    let text = std::fs::read_to_string(dir).expect("ddl.gql");
    let mut checked = 0;
    for block in text.split("\n\n").filter(|b| !b.trim().is_empty()) {
        if !block.contains("engine=admin") {
            continue;
        }
        let ddl: Vec<&str> = block.lines().filter(|l| !l.starts_with("#! ")).collect();
        let ddl = ddl.join("\n");
        assert_denied(
            writer.execute(statement(NS, "kg", &ddl)).await,
            &format!("writer: {ddl}"),
        );
        assert_denied(
            writer
                .execute_batch(batch(NS, "kg", &["INSERT (:Q)", &ddl]))
                .await,
            &format!("writer batch: {ddl}"),
        );
        let err = boss
            .execute(statement(NS, "kg", &ddl))
            .await
            .expect_err("DDL is not served yet");
        assert_eq!(err.code, ErrorCode::FailedPrecondition, "{ddl}: {err:?}");
        assert_eq!(reason(&err), "graph_statement_not_allowed", "{ddl}");
        checked += 1;
    }
    assert!(checked >= 3, "only {checked} DDL cases");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_cannot_delete_graph() {
    let fixture = Fixture::start().await;
    let admin = fixture.admin(StaticRbac::new().grant("writer", NS, Relation::Writer));
    seeded(&admin, NS, "kg").await;
    let writer = admin.for_caller(person("writer"));
    assert_denied(writer.delete_graph(delete(NS, "kg")).await, "DeleteGraph");
    assert_denied(writer.update_graph(update(NS, "kg")).await, "UpdateGraph");
    assert_denied(writer.create_graph(create(NS, "more")).await, "CreateGraph");
    admin
        .get_graph(get(NS, "kg"))
        .await
        .expect("the graph is still there");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_can_delete() {
    let fixture = Fixture::start().await;
    let admin = fixture.admin(StaticRbac::new().grant("boss", NS, Relation::Admin));
    let boss = admin.for_caller(person("boss"));
    // An admin of the namespace creates (the namespace with it), updates and deletes.
    boss.create_graph(create(NS, "kg")).await.expect("create");
    boss.update_graph(update(NS, "kg")).await.expect("update");
    boss.delete_graph(delete(NS, "kg")).await.expect("delete");
    let err = boss.get_graph(get(NS, "kg")).await.expect_err("gone");
    assert_eq!(err.code, ErrorCode::NotFound);
}

/// A credential bound to an environment reaches only that environment's namespace, whatever its
/// roles elsewhere; and a caller with no relation learns nothing of a graph's existence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_namespace_denied() {
    let fixture = Fixture::start().await;
    let rbac = StaticRbac::new()
        .grant("bound", NS, Relation::Admin)
        .grant("bound", "other", Relation::Admin)
        .environment("shop/dev", NS);
    let admin = fixture.admin(rbac);
    seeded(&admin, NS, "kg").await;
    seeded(&admin, "other", "kg").await;
    let bound = admin.for_caller(Caller {
        environment: Some("shop/dev".to_string()),
        ..person("bound")
    });
    bound
        .execute(statement(NS, "kg", "MATCH (n) RETURN n"))
        .await
        .expect("its own namespace");
    bound.list_graphs(list(NS)).await.expect("its own list");
    let read = "MATCH (n) RETURN n";
    assert_denied(
        bound.execute(statement("other", "kg", read)).await,
        "Execute",
    );
    assert_denied(
        bound.execute_batch(batch("other", "kg", &[read])).await,
        "ExecuteBatch",
    );
    assert_denied(
        bound
            .execute_stream(stream("other", "kg", read))
            .await
            .map(drop),
        "ExecuteStream",
    );
    assert_denied(bound.explain(explain("other", "kg", read)).await, "Explain");
    assert_denied(bound.get_graph(get("other", "kg")).await, "GetGraph");
    assert_denied(bound.list_graphs(list("other")).await, "ListGraphs");
    assert_denied(
        bound.create_graph(create("other", "new")).await,
        "CreateGraph",
    );
    assert_denied(
        bound.delete_graph(delete("other", "kg")).await,
        "DeleteGraph",
    );
    // No relation: a graph that exists and one that does not are refused alike.
    let stranger = admin.for_caller(person("stranger"));
    assert_denied(stranger.get_graph(get(NS, "kg")).await, "an existing graph");
    assert_denied(stranger.get_graph(get(NS, "nope")).await, "a missing graph");
    // An anonymous caller reaches nothing.
    let anonymous = admin.for_caller(Caller::anonymous());
    assert_denied(
        anonymous.execute(statement(NS, "kg", read)).await,
        "anonymous",
    );
}

/// Agent scopes narrow a relation (§48 §11.2 item 3): `graph:read` reads only, and a credential
/// with scopes but no `graph:*` one reaches no graph.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_scope_graph_read_blocks_writes() {
    let fixture = Fixture::start().await;
    let rbac = StaticRbac::new()
        .grant("agent", NS, Relation::Admin)
        .grant("other-agent", NS, Relation::Admin)
        .grant("writer-agent", NS, Relation::Admin);
    let admin = fixture.admin(rbac);
    seeded(&admin, NS, "kg").await;
    let before = state(&admin, NS, "kg").await;
    let agent = admin.for_caller(scoped("agent", &[authz::SCOPE_READ]));
    agent
        .execute(statement(NS, "kg", "MATCH (n) RETURN n"))
        .await
        .expect("graph:read reads");
    agent.list_graphs(list(NS)).await.expect("graph:read lists");
    for write in [
        "INSERT (:X)",
        "MATCH (n) SET n.x = 1",
        "MATCH (n) DETACH DELETE n",
    ] {
        assert_denied(agent.execute(statement(NS, "kg", write)).await, write);
        assert_denied(
            agent
                .execute_batch(batch(NS, "kg", &["MATCH (n) RETURN n", write]))
                .await,
            write,
        );
    }
    assert_denied(agent.delete_graph(delete(NS, "kg")).await, "DeleteGraph");
    assert_eq!(state(&admin, NS, "kg").await, before);
    // A scope for another product: no graph at all.
    let other = admin.for_caller(scoped("other-agent", &["live:call", "collections:read"]));
    assert_denied(
        other
            .execute(statement(NS, "kg", "MATCH (n) RETURN n"))
            .await,
        "no graph scope",
    );
    assert!(
        other
            .list_graphs(list(NS))
            .await
            .expect("list")
            .graphs
            .is_empty(),
        "no graph is visible without a graph scope"
    );
    // graph:write writes but does not administer.
    let writer = admin.for_caller(scoped("writer-agent", &[authz::SCOPE_WRITE]));
    writer
        .execute(statement(NS, "kg", "INSERT (:Y)"))
        .await
        .expect("graph:write writes");
    assert_denied(
        writer.delete_graph(delete(NS, "kg")).await,
        "graph:write deletes",
    );
}

/// In a protected environment, deleting or reconfiguring a graph needs the project `admin`
/// role, not only `admin` on the graph; writing does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_environment_requires_project_admin() {
    let fixture = Fixture::start().await;
    let rbac = StaticRbac::new()
        .grant("ops", "prod", Relation::Admin)
        .grant("lead", "prod", Relation::Admin)
        .project_admin("lead", "prod")
        .grant("ops", "staging", Relation::Admin)
        .protect("prod");
    let admin = fixture.admin(rbac);
    seeded(&admin, "prod", "kg").await;
    seeded(&admin, "staging", "kg").await;
    let ops = admin.for_caller(person("ops"));
    ops.execute(statement("prod", "kg", "INSERT (:X)"))
        .await
        .expect("an admin writes in prod");
    assert_denied(ops.delete_graph(delete("prod", "kg")).await, "DeleteGraph");
    assert_denied(ops.update_graph(update("prod", "kg")).await, "UpdateGraph");
    ops.delete_graph(delete("staging", "kg"))
        .await
        .expect("an unprotected environment needs only admin");
    let lead = admin.for_caller(person("lead"));
    lead.update_graph(update("prod", "kg"))
        .await
        .expect("the project admin updates");
    lead.delete_graph(delete("prod", "kg"))
        .await
        .expect("the project admin deletes");
}

/// `ListGraphs` answers only the graphs the caller may read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_filters_invisible_graphs() {
    let fixture = Fixture::start().await;
    let rbac = StaticRbac::new()
        .grant_graph("bob", NS, "g1", Relation::Reader)
        .grant_graph("bob", NS, "g3", Relation::Writer)
        .grant("all", NS, Relation::Reader);
    let admin = fixture.admin(rbac);
    for name in ["g1", "g2", "g3", "g4"] {
        admin.create_graph(create(NS, name)).await.expect("create");
    }
    let names = |r: pb::ListGraphsResponse| -> Vec<String> {
        r.graphs.into_iter().map(|g| g.name).collect()
    };
    let bob = admin.for_caller(person("bob"));
    assert_eq!(
        names(bob.list_graphs(list(NS)).await.expect("bob")),
        ["g1", "g3"]
    );
    let all = admin.for_caller(person("all"));
    assert_eq!(
        names(all.list_graphs(list(NS)).await.expect("all")),
        ["g1", "g2", "g3", "g4"]
    );
    let stranger = admin.for_caller(person("stranger"));
    assert!(
        names(stranger.list_graphs(list(NS)).await.expect("stranger")).is_empty(),
        "a stranger sees no graph"
    );
    // A per-graph relation is the graph's own: bob reads g1, not g2.
    bob.get_graph(get(NS, "g1")).await.expect("g1");
    assert_denied(bob.get_graph(get(NS, "g2")).await, "g2");
}

/// Every refusal writes one audit line naming the caller, the RPC and the object, and is
/// counted; an allowed lifecycle change is audited too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denials_are_audited() {
    let log = captured();
    let fixture = Fixture::start().await;
    let id = format!("auditor-{}", ulid::Ulid::generate());
    let admin = fixture.admin(StaticRbac::new().grant(&id, NS, Relation::Reader));
    seeded(&admin, NS, "kg").await;
    let caller = admin.for_caller(person(&id));
    let before = authz::denials();
    assert_denied(
        caller.execute(statement(NS, "kg", "INSERT (:X)")).await,
        "a write",
    );
    assert_denied(caller.delete_graph(delete(NS, "kg")).await, "a delete");
    assert!(authz::denials() >= before + 2, "denials are counted");
    let text = String::from_utf8(log.0.lock().expect("the log").clone()).expect("utf-8");
    let mine: Vec<&str> = text.lines().filter(|l| l.contains(&id)).collect();
    assert_eq!(mine.len(), 2, "one line per denial: {mine:#?}");
    for (line, rpc) in mine.iter().zip(["Execute", "DeleteGraph"]) {
        assert!(line.contains("audit=true"), "{line}");
        assert!(line.contains("outcome=\"denied\""), "{line}");
        assert!(line.contains(&format!("rpc=\"{rpc}\"")), "{line}");
        assert!(line.contains(&format!("object=graph:{NS}/kg")), "{line}");
        assert!(
            !line.contains("INSERT"),
            "no statement text in an audit line: {line}"
        );
    }
    // An allowed delete is audited as allowed.
    let boss_id = format!("boss-{}", ulid::Ulid::generate());
    let admin = fixture.admin(StaticRbac::new().grant(&boss_id, NS, Relation::Admin));
    admin
        .for_caller(person(&boss_id))
        .delete_graph(delete(NS, "kg"))
        .await
        .expect("delete");
    let text = String::from_utf8(log.0.lock().expect("the log").clone()).expect("utf-8");
    assert!(
        text.lines()
            .any(|l| l.contains(&boss_id) && l.contains("outcome=\"allowed\"")),
        "the allowed delete is audited"
    );
}

/// A statement the keyword guard and the engine read differently is logged as
/// `graph_classifier_disagreement` (literals redacted) and counted (§48 §11.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classifier_disagreement_is_logged_and_counted() {
    let log = captured();
    let fixture = Fixture::start().await;
    let admin = fixture.admin(StaticRbac::new());
    seeded(&admin, NS, "kg").await;
    let before = authz::classifier_disagreements();
    // The guard reads the property name `set` as the keyword; the engine reads a read.
    admin
        .execute(statement(
            NS,
            "kg",
            "MATCH (n) WHERE n.name = 'secret-literal' RETURN n.set AS s",
        ))
        .await
        .expect("a read");
    assert!(authz::classifier_disagreements() > before, "counted");
    let text = String::from_utf8(log.0.lock().expect("the log").clone()).expect("utf-8");
    assert!(text.contains("graph_classifier_disagreement"), "logged");
    assert!(!text.contains("secret-literal"), "literals are redacted");
}

/// The log lines every test of this binary writes.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("the log").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn captured() -> &'static Captured {
    static LOG: OnceLock<Captured> = OnceLock::new();
    LOG.get_or_init(|| {
        let log = Captured::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::set_global_default(subscriber).expect("one global subscriber");
        log
    })
}
