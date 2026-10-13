//! The `pg-control` role of the `loams` binary (PG2 Task 9, design §46 §4
//! and §6.1): `loams.postgres.v1` on its own API listener, authorized and
//! audited, beside the upcall listener; and what the default build leaves
//! out.
//!
//! The cases that start `pg-control` need the feature `postgres`
//! (`cargo test -p loams --features postgres --test pg_control`); the rest
//! run in every build.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::process::Command;

use loams::{Server, ServerConfig};
use prost::Message as _;
use serde_json::{Value, json};
use tempfile::TempDir;

/// The workspace root.
fn workspace() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `cargo tree -p loams -e normal` with `features`, as text.
fn cargo_tree(features: &[&str]) -> String {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let mut command = Command::new(cargo);
    command
        .current_dir(workspace())
        .args([
            "tree",
            "-p",
            "loams",
            "-e",
            "normal",
            "--locked",
            "--offline",
        ])
        .args(["--prefix", "none", "--no-default-features", "--features"])
        .arg(features.join(","));
    let out = command.output().expect("cargo tree runs");
    assert!(
        out.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8")
}

/// The packages of a `cargo tree --prefix none` listing.
fn packages(tree: &str) -> BTreeSet<String> {
    tree.lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_owned)
        .collect()
}

/// Global Constraints, Review Focus 8: the default build links no Postgres
/// control-plane code.
#[test]
fn default_features_exclude_postgres() {
    let default = packages(&cargo_tree(&["default"]));
    assert!(default.contains("loams"), "the listing parsed: {default:?}");
    for name in ["loams-pg-control", "loams-postgres"] {
        assert!(!default.contains(name), "{name} is in the default build");
    }
    // The control: the feature does bring them in, so the check above reads
    // a real listing.
    let with = packages(&cargo_tree(&["default", "postgres"]));
    assert!(with.contains("loams-pg-control"), "{with:?}");
    assert!(with.contains("loams-postgres"), "{with:?}");
}

/// The length-prefixed frames of a body.
fn frames(body: &[u8]) -> Vec<(u8, &[u8])> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 5 <= body.len() {
        let len =
            u32::from_be_bytes([body[at + 1], body[at + 2], body[at + 3], body[at + 4]]) as usize;
        let end = at + 5 + len;
        assert!(end <= body.len(), "a frame runs past the body");
        out.push((body[at], &body[at + 5..end]));
        at = end;
    }
    out
}

/// Reflection's service list at `base`, through a gRPC-Web `ListServices`.
async fn reflected_services(base: &str) -> BTreeSet<String> {
    // ServerReflectionRequest { list_services: "" } = field 7, empty string.
    let request = [0x3a, 0x00];
    let mut framed = vec![0u8];
    framed.extend_from_slice(&u32::try_from(request.len()).expect("small").to_be_bytes());
    framed.extend_from_slice(&request);
    let response = reqwest::Client::new()
        .post(format!(
            "{base}/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
        ))
        .header("content-type", "application/grpc-web+proto")
        .body(framed)
        .send()
        .await
        .expect("reflection");
    let body = response.bytes().await.expect("body");
    let message = frames(&body)
        .into_iter()
        .find(|(flags, _)| flags & 0x80 == 0)
        .expect("a reflection answer")
        .1
        .to_vec();
    #[derive(Clone, PartialEq, prost::Message)]
    struct ServiceResponse {
        #[prost(string, tag = "1")]
        name: String,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    struct ListServiceResponse {
        #[prost(message, repeated, tag = "1")]
        service: Vec<ServiceResponse>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    struct Answer {
        #[prost(message, optional, tag = "6")]
        list_services_response: Option<ListServiceResponse>,
    }
    Answer::decode(message.as_slice())
        .expect("decodes")
        .list_services_response
        .expect("a list")
        .service
        .into_iter()
        .map(|s| s.name)
        .collect()
}

/// A Connect unary call with a JSON body and optional bearer token.
async fn connect(base: &str, rpc: &str, body: &Value, bearer: Option<&str>) -> (u16, Value) {
    let mut request = reqwest::Client::new()
        .post(format!("{base}{rpc}"))
        .header("content-type", "application/json")
        .body(body.to_string());
    if let Some(token) = bearer {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = request.send().await.expect("connect call");
    let status = response.status().as_u16();
    let text = response.text().await.expect("a body");
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

/// The reflection services every listener with reflection adds, and health.
const PROTOCOL_SERVICES: [&str; 3] = [
    "grpc.health.v1.Health",
    "grpc.reflection.v1.ServerReflection",
    "grpc.reflection.v1alpha.ServerReflection",
];

/// R1.4: reflection lists the services the process serves (or stubs with a
/// typed refusal: the catalogue's), plus health and reflection themselves,
/// never a package that is only in the descriptor set.
#[tokio::test]
async fn reflection_lists_only_served_services() {
    let dir = TempDir::new().expect("temp dir");
    let mut config = ServerConfig::new(dir.path());
    config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.reflection = true;
    let server = Server::start(config).await.expect("start");
    let base = format!("http://{}", server.local_addr());

    let (status, info) = connect(
        &base,
        "/loams.instance.v1.InstanceService/GetInstance",
        &json!({}),
        None,
    )
    .await;
    assert_eq!(status, 200, "{info}");
    let mut expected: BTreeSet<String> = info["services"]
        .as_array()
        .expect("services")
        .iter()
        .flat_map(|row| row["services"].as_array().cloned().unwrap_or_default())
        .filter_map(|s| s.as_str().map(str::to_owned))
        .collect();
    expected.extend(PROTOCOL_SERVICES.map(str::to_owned));
    let listed = reflected_services(&base).await;
    assert_eq!(listed, expected);
    for unserved in [
        "loams.postgres.v1.PostgresService",
        "loams.operations.v1.OperationsService",
    ] {
        assert!(!listed.contains(unserved), "{unserved} listed: {listed:?}");
    }
    server.shutdown().await.expect("shutdown");

    #[cfg(feature = "postgres")]
    {
        let pg = postgres::Running::start(|_| {}).await;
        let listed = reflected_services(&pg.base).await;
        let mut expected: BTreeSet<String> = [
            "loams.postgres.v1.PostgresService",
            "loams.operations.v1.OperationsService",
        ]
        .map(str::to_owned)
        .into();
        expected.extend(PROTOCOL_SERVICES.map(str::to_owned));
        assert_eq!(listed, expected);
        pg.shutdown().await;
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use std::sync::Arc;

    use loams::pg_control::{PgAuth, PgControl, PgControlConfig};
    use loams_pg_control::audit::{AuditOutcome, MemoryAuditSink};
    use loams_pg_control::authz::{
        Actor, Grant, GrantObject, KeysFile, PrincipalKind, Relation, mint_api_key,
    };

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD_NO_PAD;
    use buffa::Message as _;
    use loams_proto::loams::errors::v1::ErrorInfo;

    use super::*;

    /// The `ErrorInfo.reason` of a Connect JSON error body.
    fn reason(error: &Value) -> String {
        let detail = &error["details"][0];
        assert_eq!(detail["type"], "loams.errors.v1.ErrorInfo", "{error}");
        let bytes = STANDARD_NO_PAD
            .decode(detail["value"].as_str().expect("an encoded detail"))
            .expect("base64");
        ErrorInfo::decode_from_slice(&bytes)
            .expect("ErrorInfo")
            .reason
    }

    pub(super) struct Running {
        pub base: String,
        pub upcall: String,
        pub control: PgControl,
        pub audit: Arc<MemoryAuditSink>,
        _dir: TempDir,
    }

    impl Running {
        pub async fn start(edit: impl FnOnce(&mut PgControlConfig)) -> Self {
            let dir = TempDir::new().expect("temp dir");
            Self::start_in(dir, edit).await
        }

        pub async fn start_in(dir: TempDir, edit: impl FnOnce(&mut PgControlConfig)) -> Self {
            let audit = Arc::new(MemoryAuditSink::default());
            let mut config = PgControlConfig::new(dir.path());
            config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
            config.upcall_listen = SocketAddr::from(([127, 0, 0, 1], 0));
            config.reflection = true;
            // Nothing answers there: the service's writes do not need Neon,
            // and the reconciler retries with backoff.
            config.pageserver = "http://127.0.0.1:9".parse().expect("a URL");
            config.wal = "http://127.0.0.1:9".parse().expect("a URL");
            config.audit = audit.clone();
            // The API alone: no compute runtime (the endpoint reconciler is
            // tested in loams-pg-control on fakes).
            config.computes = None;
            edit(&mut config);
            let control = PgControl::start(config).await.expect("pg-control starts");
            Running {
                base: format!("http://{}", control.api_addr()),
                upcall: format!("http://{}", control.upcall_addr()),
                control,
                audit,
                _dir: dir,
            }
        }

        pub async fn shutdown(self) -> TempDir {
            self.control.shutdown().await;
            self._dir
        }

        pub async fn call(&self, rpc: &str, body: Value, bearer: Option<&str>) -> (u16, Value) {
            connect(
                &self.base,
                &format!("/loams.postgres.v1.PostgresService/{rpc}"),
                &body,
                bearer,
            )
            .await
        }
    }

    /// A keys file in `dir` with these keys; answers its path.
    fn keys_file(dir: &std::path::Path, keys: &KeysFile) -> std::path::PathBuf {
        let path = dir.join("keys.json");
        std::fs::write(&path, serde_json::to_vec(keys).expect("json")).expect("write keys");
        path
    }

    fn grant(relation: Relation, namespace: &str) -> Grant {
        Grant {
            relation,
            on: GrantObject::Namespace(namespace.into()),
        }
    }

    /// R8.7, R8.9: the API listener serves no upcall route; the upcall
    /// listener does.
    #[tokio::test]
    async fn api_listener_serves_no_upcall() {
        let pg = Running::start(|_| {}).await;
        let client = reqwest::Client::new();
        let spec = "/compute/api/v2/computes/cmp-01K7AAAAAAAAAAAAAAAAAAAAAA/spec";
        let r = client
            .get(format!("{}{spec}", pg.base))
            .send()
            .await
            .expect("get");
        assert_eq!(r.status().as_u16(), 404);
        let r = client
            .put(format!("{}/notify-attach", pg.base))
            .header("authorization", "Bearer token")
            .body("{}")
            .send()
            .await
            .expect("put");
        assert_eq!(r.status().as_u16(), 404);
        // The upcall listener serves the spec route: no token is a 401, not
        // a missing route.
        let r = client
            .get(format!("{}{spec}", pg.upcall))
            .send()
            .await
            .expect("get");
        assert_eq!(r.status().as_u16(), 401);
        pg.shutdown().await;
    }

    /// R8.9: the signing key is the secret store's, and a restart keeps it.
    #[tokio::test]
    async fn signing_key_survives_a_restart() {
        let pg = Running::start(|_| {}).await;
        let kid = pg.control.signing_kid().to_owned();
        let dir = pg.shutdown().await;
        let again = Running::start_in(dir, |_| {}).await;
        assert_eq!(again.control.signing_kid(), kid);
        again.shutdown().await;
    }

    #[tokio::test]
    async fn viewer_cannot_create_branch() {
        let dir = TempDir::new().expect("temp dir");
        let (alice, alice_entry) = mint_api_key(
            "alice",
            PrincipalKind::User,
            Vec::new(),
            vec![grant(Relation::Editor, "acme")],
        )
        .expect("minted");
        let (bob, bob_entry) = mint_api_key(
            "bob",
            PrincipalKind::User,
            Vec::new(),
            vec![grant(Relation::Viewer, "acme")],
        )
        .expect("minted");
        let path = keys_file(
            dir.path(),
            &KeysFile {
                keys: vec![alice_entry, bob_entry],
            },
        );
        let pg = Running::start_in(dir, |c| c.auth = PgAuth::ApiKeys(path)).await;
        let (alice, bob) = (alice.expose().clone(), bob.expose().clone());

        let (status, created) = pg
            .call(
                "CreateProject",
                json!({"namespace": "acme", "name": "shop", "idempotencyKey": "k1"}),
                Some(&alice),
            )
            .await;
        assert_eq!(status, 200, "{created}");
        let project = created["project"]["id"].as_str().expect("an id").to_owned();

        let (status, refused) = pg
            .call(
                "CreateBranch",
                json!({"namespace": "acme", "projectId": project, "name": "dev", "idempotencyKey": "k2"}),
                Some(&bob),
            )
            .await;
        assert_eq!(status, 403, "{refused}");
        assert_eq!(refused["code"], "permission_denied");
        assert_eq!(reason(&refused), "permission_denied");

        // A viewer reads.
        let (status, got) = pg
            .call(
                "GetProject",
                json!({"namespace": "acme", "projectId": project}),
                Some(&bob),
            )
            .await;
        assert_eq!(status, 200, "{got}");
        assert_eq!(got["project"]["name"], "shop");
        // And the editor creates the branch.
        let (status, branch) = pg
            .call(
                "CreateBranch",
                json!({"namespace": "acme", "projectId": project, "name": "dev", "idempotencyKey": "k2"}),
                Some(&alice),
            )
            .await;
        assert_eq!(status, 200, "{branch}");
        assert_eq!(branch["branch"]["state"], "BRANCH_STATE_CREATING");
        assert_eq!(branch["operation"]["kind"], "postgres.branch.create");

        // No credential, or another namespace: refused.
        let (status, e) = pg
            .call("ListProjects", json!({"namespace": "acme"}), None)
            .await;
        assert_eq!(
            (status, reason(&e).as_str()),
            (401, "unauthenticated"),
            "{e}"
        );
        let (status, e) = pg
            .call("ListProjects", json!({"namespace": "other"}), Some(&alice))
            .await;
        assert_eq!(
            (status, reason(&e).as_str()),
            (403, "permission_denied"),
            "{e}"
        );
        pg.shutdown().await;
    }

    #[tokio::test]
    async fn agent_cannot_hold_admin() {
        // A keys file granting an agent admin does not load.
        let dir = TempDir::new().expect("temp dir");
        let (_, agent_admin) = mint_api_key(
            "bot",
            PrincipalKind::Agent,
            Vec::new(),
            vec![grant(Relation::Admin, "acme")],
        )
        .expect("minted");
        let path = keys_file(
            dir.path(),
            &KeysFile {
                keys: vec![agent_admin],
            },
        );
        let mut config = PgControlConfig::new(dir.path());
        config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
        config.upcall_listen = SocketAddr::from(([127, 0, 0, 1], 0));
        config.auth = PgAuth::ApiKeys(path);
        let e = PgControl::start(config).await.expect_err("refused");
        assert!(e.to_string().contains("agents cannot hold admin"), "{e}");

        // A person with admin, acting through an agent, has no admin: the
        // protection a person sets, the agent cannot lift.
        let dir = TempDir::new().expect("temp dir");
        let (alice, alice_entry) = mint_api_key(
            "alice",
            PrincipalKind::User,
            Vec::new(),
            vec![grant(Relation::Admin, "acme")],
        )
        .expect("minted");
        let (via_bot, via_bot_entry) = mint_api_key(
            "alice",
            PrincipalKind::User,
            vec![Actor {
                subject: "bot".into(),
                kind: PrincipalKind::Agent,
            }],
            Vec::new(),
        )
        .expect("minted");
        let path = keys_file(
            dir.path(),
            &KeysFile {
                keys: vec![alice_entry, via_bot_entry],
            },
        );
        let pg = Running::start_in(dir, |c| c.auth = PgAuth::ApiKeys(path)).await;
        let (alice, via_bot) = (alice.expose().clone(), via_bot.expose().clone());
        let (status, created) = pg
            .call(
                "CreateProject",
                json!({"namespace": "acme", "name": "shop", "idempotencyKey": "k1"}),
                Some(&alice),
            )
            .await;
        assert_eq!(status, 200, "{created}");
        let project = created["project"]["id"].as_str().expect("an id").to_owned();
        let (status, branch) = pg
            .call(
                "CreateBranch",
                json!({"namespace": "acme", "projectId": project, "name": "prod",
                       "protected": true, "idempotencyKey": "k2"}),
                Some(&alice),
            )
            .await;
        assert_eq!(status, 200, "{branch}");
        let branch_id = branch["branch"]["id"].as_str().expect("an id").to_owned();
        let unprotect = |key: &str| {
            json!({"namespace": "acme", "projectId": project,
                   "branch": {"id": branch_id, "protected": false},
                   "updateMask": "protected", "idempotencyKey": key})
        };
        let (status, e) = pg
            .call("UpdateBranch", unprotect("k3"), Some(&via_bot))
            .await;
        assert_eq!(status, 403, "{e}");
        assert_eq!(reason(&e), "permission_denied");
        let (status, updated) = pg.call("UpdateBranch", unprotect("k4"), Some(&alice)).await;
        assert_eq!(status, 200, "{updated}");
        assert_ne!(updated["branch"]["protected"], true, "{updated}");

        // The audit names the person and the agent that acted for them.
        let refused = pg
            .audit
            .events()
            .into_iter()
            .find(|e| e.rpc.ends_with("/UpdateBranch") && e.outcome != AuditOutcome::Ok)
            .expect("the agent's attempt is audited");
        assert_eq!(refused.principal, "alice");
        assert_eq!(refused.act.len(), 1);
        assert_eq!(refused.act[0].kind, PrincipalKind::Agent);
        pg.shutdown().await;
    }

    /// The `PostgresService` methods that are not `NO_SIDE_EFFECTS`.
    fn mutations() -> Vec<String> {
        let set = prost_types::FileDescriptorSet::decode(loams_proto::FILE_DESCRIPTOR_SET)
            .expect("descriptor set");
        let service = set
            .file
            .iter()
            .filter(|f| f.package() == "loams.postgres.v1")
            .flat_map(|f| f.service.iter())
            .find(|s| s.name() == "PostgresService")
            .expect("PostgresService");
        service
            .method
            .iter()
            .filter(|m| {
                m.options.as_ref().is_none_or(|o| {
                    o.idempotency_level()
                        != prost_types::method_options::IdempotencyLevel::NoSideEffects
                })
            })
            .map(|m| m.name().to_owned())
            .collect()
    }

    /// D221: every mutation, applied or refused, is one audit event naming
    /// the principal; no event holds a secret.
    #[tokio::test]
    async fn every_mutation_emits_audit_event() {
        let pg = Running::start(|_| {}).await;
        let (status, created) = pg
            .call(
                "CreateProject",
                json!({"namespace": "acme", "name": "shop", "idempotencyKey": "k1"}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{created}");
        let project = created["project"]["id"].as_str().expect("an id").to_owned();
        let (status, role) = pg
            .call(
                "CreateRole",
                json!({"namespace": "acme", "projectId": project, "name": "app", "idempotencyKey": "k2"}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{role}");
        let password = role["password"].as_str().expect("the password").to_owned();
        assert!(password.len() >= 32);

        let methods = mutations();
        assert!(methods.len() >= 20, "{methods:?}");
        for method in &methods {
            // Whatever the answer (most are refused: no such branch, or not
            // built yet), the call is audited.
            let body = json!({"namespace": "acme", "projectId": project, "idempotencyKey": format!("audit-{method}")});
            let (status, answer) = pg.call(method, body, None).await;
            assert!(answer.is_object(), "{method}: {status} {answer}");
        }

        let events = pg.audit.events();
        for method in &methods {
            let rpc = format!("/loams.postgres.v1.PostgresService/{method}");
            let event = events
                .iter()
                .rev()
                .find(|e| e.rpc == rpc)
                .unwrap_or_else(|| panic!("no audit event for {method}"));
            assert_eq!(event.principal, "dev", "{event:?}");
            assert_eq!(event.namespace, "acme", "{event:?}");
            assert_eq!(
                event.idempotency_key.as_deref(),
                Some(format!("audit-{method}").as_str())
            );
        }
        let create = events
            .iter()
            .find(|e| e.rpc.ends_with("/CreateProject"))
            .expect("the create");
        assert_eq!(create.outcome, AuditOutcome::Ok);
        assert_eq!(create.project_id.as_deref(), Some(project.as_str()));
        assert_eq!(
            create.operation_id.as_deref(),
            created["operation"]["id"].as_str()
        );
        let role_event = events
            .iter()
            .find(|e| e.rpc.ends_with("/CreateRole"))
            .expect("the role");
        assert_eq!(
            role_event.target.get("name").map(String::as_str),
            Some("app")
        );
        let all = serde_json::to_string(&events).expect("json");
        assert!(
            !all.contains(&password),
            "an audit event holds the password"
        );
        // Reads are not mutations: no event.
        let before = pg.audit.events().len();
        let (status, _) = pg
            .call("ListProjects", json!({"namespace": "acme"}), None)
            .await;
        assert_eq!(status, 200);
        assert_eq!(pg.audit.events().len(), before);
        pg.shutdown().await;
    }

    /// The service answers with the proto's shapes: a replay is the first
    /// answer, the operation is readable, a role's password once.
    #[tokio::test]
    async fn postgres_service_round_trips() {
        let pg = Running::start(|_| {}).await;
        let create = json!({"namespace": "acme", "name": "shop", "historyRetention": "86400s", "idempotencyKey": "k1"});
        let (status, first) = pg.call("CreateProject", create.clone(), None).await;
        assert_eq!(status, 200, "{first}");
        let (status, again) = pg.call("CreateProject", create, None).await;
        assert_eq!(status, 200, "{again}");
        assert_eq!(first["operation"]["id"], again["operation"]["id"]);
        let project = &first["project"];
        assert_eq!(project["state"], "PROJECT_STATE_CREATING");
        assert_eq!(project["pgVersion"], 17);
        assert_eq!(project["historyRetention"], "86400s");
        assert_eq!(project["walPool"], "default");
        let id = project["id"].as_str().expect("an id").to_owned();

        let (status, op) = connect(
            &pg.base,
            "/loams.operations.v1.OperationsService/GetOperation",
            &json!({"operationId": first["operation"]["id"]}),
            None,
        )
        .await;
        assert_eq!(status, 200, "{op}");
        assert_eq!(op["operation"]["kind"], "postgres.project.create");
        assert_eq!(op["operation"]["target"]["project"], id.as_str());

        let (status, branches) = pg
            .call(
                "ListBranches",
                json!({"namespace": "acme", "projectId": id}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{branches}");
        let main = &branches["branches"][0];
        assert_eq!(main["name"], "main");
        assert_eq!(main["isDefault"], true);

        let role =
            json!({"namespace": "acme", "projectId": id, "name": "app", "idempotencyKey": "k2"});
        let (status, issued) = pg.call("CreateRole", role.clone(), None).await;
        assert_eq!(status, 200, "{issued}");
        assert_eq!(issued["role"]["name"], "app");
        let (status, replay) = pg.call("CreateRole", role, None).await;
        assert_eq!(status, 400, "{replay}");
        assert_eq!(reason(&replay), "secret_already_issued");

        let (status, e) = pg
            .call(
                "GetProject",
                json!({"namespace": "acme", "projectId": "prj-nope"}),
                None,
            )
            .await;
        assert_eq!(status, 400, "{e}");
        assert_eq!(reason(&e), "invalid_argument");
        let (status, e) = pg
            .call(
                "UpgradeProject",
                json!({"namespace": "acme", "projectId": id, "idempotencyKey": "k3"}),
                None,
            )
            .await;
        assert_eq!(status, 501, "{e}");
        assert_eq!(reason(&e), "not_implemented");
        pg.shutdown().await;
    }

    /// The endpoint RPCs (Task 11) with the proto's shapes: a create, its
    /// read and listing, an update of its settings, a start's operation
    /// naming the endpoint; a second read-write endpoint, a setting outside
    /// the allow-list and a promotion (Task 16) are refused.
    #[tokio::test]
    async fn endpoint_rpcs_round_trip() {
        let pg = Running::start(|_| {}).await;
        let (status, created) = pg
            .call(
                "CreateProject",
                json!({"namespace": "acme", "name": "shop", "idempotencyKey": "k1"}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{created}");
        let project = created["project"]["id"].as_str().expect("an id").to_owned();
        let main = created["project"]["defaultBranchId"]
            .as_str()
            .expect("main")
            .to_owned();

        let create = json!({
            "namespace": "acme", "projectId": project, "type": "ENDPOINT_TYPE_READ_WRITE",
            "minCu": 0.5, "maxCu": 2.0, "suspendTimeout": "600s",
            "pgSettings": {"work_mem": "64MB"}, "idempotencyKey": "e1"
        });
        let (status, ep) = pg.call("CreateEndpoint", create.clone(), None).await;
        assert_eq!(status, 200, "{ep}");
        let endpoint = &ep["endpoint"];
        let id = endpoint["id"].as_str().expect("an id").to_owned();
        assert!(id.starts_with("ep-"));
        assert_eq!(endpoint["branchId"], main.as_str());
        assert_eq!(endpoint["state"], "ENDPOINT_STATE_IDLE");
        assert_eq!(endpoint["minCu"], 0.5);
        assert_eq!(endpoint["maxCu"], 2.0);
        assert_eq!(endpoint["suspendTimeout"], "600s");
        assert_eq!(endpoint["poolMode"], "POOL_MODE_TRANSACTION");
        assert_eq!(endpoint["pgSettings"]["work_mem"], "64MB");
        let (_, replay) = pg.call("CreateEndpoint", create, None).await;
        assert_eq!(replay["endpoint"]["id"], id.as_str());

        let (status, e) = pg
            .call(
                "CreateEndpoint",
                json!({"namespace": "acme", "projectId": project, "type": "ENDPOINT_TYPE_READ_WRITE", "idempotencyKey": "e2"}),
                None,
            )
            .await;
        assert_eq!(status, 409, "{e}");
        assert_eq!(reason(&e), "endpoint_exists_for_branch");
        let (status, e) = pg
            .call(
                "CreateEndpoint",
                json!({"namespace": "acme", "projectId": project, "type": "ENDPOINT_TYPE_READ_ONLY",
                       "pgSettings": {"shared_preload_libraries": "evil"}, "idempotencyKey": "e3"}),
                None,
            )
            .await;
        assert_eq!(status, 400, "{e}");
        assert_eq!(reason(&e), "invalid_argument");

        let (status, got) = pg
            .call(
                "GetEndpoint",
                json!({"namespace": "acme", "projectId": project, "endpointId": id}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{got}");
        assert_eq!(got["endpoint"]["id"], id.as_str());
        let (status, listed) = pg
            .call(
                "ListEndpoints",
                json!({"namespace": "acme", "projectId": project, "branchId": main}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{listed}");
        assert_eq!(listed["endpoints"].as_array().map(Vec::len), Some(1));

        let (status, updated) = pg
            .call(
                "UpdateEndpoint",
                json!({"namespace": "acme", "projectId": project,
                       "endpoint": {"id": id, "pgSettings": {"statement_timeout": "30s"}},
                       "updateMask": "pgSettings", "idempotencyKey": "u1"}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{updated}");
        assert_eq!(
            updated["endpoint"]["pgSettings"],
            json!({"statement_timeout": "30s"})
        );

        let (status, started) = pg
            .call(
                "StartEndpoint",
                json!({"namespace": "acme", "projectId": project, "endpointId": id, "idempotencyKey": "s1"}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{started}");
        let op = &started["operation"];
        assert_eq!(op["kind"], "postgres.endpoint.start");
        assert_eq!(op["target"]["endpoint"], id.as_str());
        assert_eq!(op["target"]["branch"], main.as_str());

        let (status, e) = pg
            .call(
                "RestartEndpoint",
                json!({"namespace": "acme", "projectId": project, "endpointId": id, "promote": true, "idempotencyKey": "r1"}),
                None,
            )
            .await;
        assert_eq!(status, 501, "{e}");
        assert_eq!(reason(&e), "not_implemented");

        let (status, deleted) = pg
            .call(
                "DeleteEndpoint",
                json!({"namespace": "acme", "projectId": project, "endpointId": id, "idempotencyKey": "d1"}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{deleted}");
        assert_eq!(deleted["operation"]["kind"], "postgres.endpoint.delete");
        pg.shutdown().await;
    }

    /// `WatchProject` streams a snapshot first (Connect server streaming,
    /// JSON envelopes).
    #[tokio::test]
    async fn watch_project_streams_a_snapshot() {
        let pg = Running::start(|_| {}).await;
        let (status, created) = pg
            .call(
                "CreateProject",
                json!({"namespace": "acme", "name": "shop", "idempotencyKey": "k1"}),
                None,
            )
            .await;
        assert_eq!(status, 200, "{created}");
        let project = created["project"]["id"].as_str().expect("an id").to_owned();
        let body = json!({"namespace": "acme", "projectId": project}).to_string();
        let mut framed = vec![0u8];
        framed.extend_from_slice(&u32::try_from(body.len()).expect("small").to_be_bytes());
        framed.extend_from_slice(body.as_bytes());
        let mut response = reqwest::Client::new()
            .post(format!(
                "{}/loams.postgres.v1.PostgresService/WatchProject",
                pg.base
            ))
            .header("content-type", "application/connect+json")
            .body(framed)
            .send()
            .await
            .expect("a stream");
        assert_eq!(response.status().as_u16(), 200);
        let mut buf = Vec::new();
        let first = loop {
            let chunk = tokio::time::timeout(std::time::Duration::from_secs(10), response.chunk())
                .await
                .expect("a frame within 10 s")
                .expect("reads")
                .expect("not the end");
            buf.extend_from_slice(&chunk);
            // The first frame, once it is whole.
            if buf.len() >= 5 {
                let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                if buf.len() >= 5 + len {
                    let message = &buf[5..5 + len];
                    assert_eq!(
                        buf[0],
                        0,
                        "a message, not the end: {}",
                        String::from_utf8_lossy(message)
                    );
                    break serde_json::from_slice::<Value>(message).expect("json");
                }
            }
        };
        assert_eq!(first["snapshot"]["project"]["id"], project.as_str());
        assert_eq!(
            first["snapshot"]["branches"].as_array().map(Vec::len),
            Some(1)
        );
        assert!(
            first["cursor"]
                .as_str()
                .is_some_and(|c| c.starts_with("w1."))
        );
        drop(response);
        pg.shutdown().await;
    }

    /// R-REL.1: the compute runner answers its health unauthenticated,
    /// refuses every other call without its token, and refuses an image
    /// outside its list with its token.
    #[tokio::test]
    async fn pg_runner_holds_the_runtime_behind_its_token() {
        use loams::pg_control::{ComputesConfig, PgRunner, PgRunnerConfig};
        use loams_pg_control::runtime::compose::ComposeBin;
        use loams_pg_control::secrets::Secret;

        let dir = TempDir::new().expect("temp dir");
        let runner = PgRunner::start(PgRunnerConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            token: Secret::new("runner-token".into()),
            computes: ComputesConfig {
                // Never run: every call below is refused first.
                compose_bin: Some(ComposeBin {
                    program: "/bin/false".into(),
                    args: Vec::new(),
                }),
                control_plane_uri: Some("http://pg-control:7690".into()),
                ..ComputesConfig::default()
            },
            data_dir: dir.path().into(),
        })
        .await
        .expect("the runner starts");
        let pinned = loams_pg_control::runtime::images::pinned().expect("pins");
        assert_eq!(
            runner.images().iter().collect::<BTreeSet<_>>(),
            pinned.values().collect::<BTreeSet<_>>()
        );
        let base = format!("http://{}", runner.addr());
        let client = reqwest::Client::new();
        let health = client
            .get(format!("{base}/health"))
            .send()
            .await
            .expect("get");
        assert_eq!(health.status().as_u16(), 200);
        let launch = json!({
            "compute_id": "cmp-01K7AAAAAAAAAAAAAAAAAAAAAA",
            "binding": {
                "namespace": "acme",
                "project_id": "prj-01K7AAAAAAAAAAAAAAAAAAAAAA",
                "endpoint_id": "ep-01K7AAAAAAAAAAAAAAAAAAAAAA",
            },
            "address": { "host": "127.0.0.1", "port": 55500 },
            "image": "docker.io/library/alpine@sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "cu": 4,
            "upcall_token": "a.b.c",
        });
        let anonymous = client
            .post(format!("{base}/v1/start"))
            .json(&launch)
            .send()
            .await
            .expect("post");
        assert_eq!(anonymous.status().as_u16(), 401);
        let refused = client
            .post(format!("{base}/v1/start"))
            .bearer_auth("runner-token")
            .json(&launch)
            .send()
            .await
            .expect("post");
        assert_eq!(refused.status().as_u16(), 400);
        let body: Value = refused.json().await.expect("json");
        assert!(
            body["error"]["InvalidArgument"]
                .as_str()
                .is_some_and(|m| m.contains("not one the runner runs")),
            "{body}"
        );
        runner.shutdown().await;
    }

    /// The runner does not know where pg-control's upcall listener is.
    #[tokio::test]
    async fn pg_runner_needs_a_control_plane_uri() {
        use loams::pg_control::{ComputesConfig, PgRunner, PgRunnerConfig};
        use loams_pg_control::secrets::Secret;

        let dir = TempDir::new().expect("temp dir");
        let e = PgRunner::start(PgRunnerConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            token: Secret::new("runner-token".into()),
            computes: ComputesConfig::default(),
            data_dir: dir.path().into(),
        })
        .await
        .expect_err("refused");
        assert!(e.to_string().contains("--compute-control-plane-uri"), "{e}");
    }

    /// With a compute runner, pg-control starts without any compose
    /// command or container socket of its own.
    #[tokio::test]
    async fn pg_control_starts_with_a_compute_runner() {
        use loams::pg_control::{ComputesConfig, RunnerClient};
        use loams_pg_control::secrets::Secret;

        let pg = Running::start(|config| {
            config.computes = Some(ComputesConfig {
                // Not looked for: the runner runs compose.
                compose_bin: None,
                runner: Some(RunnerClient {
                    url: "http://127.0.0.1:9".parse().expect("a URL"),
                    token: Secret::new("runner-token".into()),
                }),
                ..ComputesConfig::default()
            });
        })
        .await;
        pg.shutdown().await;
    }

    /// `--pg-authz dev` is for a loopback listener only.
    #[tokio::test]
    async fn dev_authz_refuses_a_public_listener() {
        let dir = TempDir::new().expect("temp dir");
        let mut config = PgControlConfig::new(dir.path());
        config.listen = SocketAddr::from(([0, 0, 0, 0], 0));
        config.upcall_listen = SocketAddr::from(([127, 0, 0, 1], 0));
        let e = PgControl::start(config).await.expect_err("refused");
        assert!(e.to_string().contains("loopback"), "{e}");
    }
}
