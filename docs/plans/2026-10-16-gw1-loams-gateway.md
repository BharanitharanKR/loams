# GW1 — Loams Gateway Implementation Plan (the Postgres front door)

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task, test first. Each task lists:
> - its files;
> - the interfaces it must produce;
> - the tests that must exist and pass before it is done (exact function names);
> - its steps;
> - its commit message.
>
> Where this plan gives exact names, paths, ports, flags or defaults, use them verbatim. Where it gives a contract and named tests, write the code to that contract, and record any deviation in "Rulings made during execution" at the end of this file.
>
> **Status: Planned** (2026-10-16). Track GW, design [§57](../design/57-loams-gateway.md) (D1010–D1024, Q895–Q904). The owner's decision of 2026-10-16 fixes one thing: Loams builds its own gateway crate instead of deploying Envoy or Traefik, and its first job is PG2 Task 22's SNI listener. This plan builds the Postgres front door (GW1a), wires it for PG2 Task 22 (GW1b) and hardens it (GW1c). The `http`, `llm` and `mcp` modules (§57 §12) get plans of their own. This GW1 is unrelated to the protocol gateway's old plans, which moved to `loams-platform` (D1024).

**Goal:** a client of `<ns>.<region>.pg.<base-domain>` reaches that namespace's PgDog through one regional listener, whether it negotiates TLS with `SSLRequest` (spliced, TLS end to end to PgDog) or directly (`sslnegotiation=direct`, terminated and re-encrypted); a client without SNI reaches it on the namespace's dedicated port; plaintext is refused; routes follow `pg-control`'s records without a restart.

**Architecture** (§57 §5, §7, §8):
- **`crates/loams-gateway`** (new, Apache-2.0, `publish = false`): a library and the binary `loams-gateway`.
  - The core: `config`, `routes`, `reload`, `listeners`, `sniff/{pg,tls}`, `tls`, `io`, `admin`, `observe`.
  - The module `pg/{mod,front,upstream}`.
- **`pg-control`** (PG2's crate) gains the dedicated-port records and the routes renderer (GW1b).
- **Deploy:** `deploy/release/Containerfile.loams-gateway`, the single-node compose service, and the Helm templates next to PG2 Task 22's PgDog templates.

**Tech Stack:**
- Rust 1.97 (workspace `rust-version`), edition 2024, workspace lints.
- Workspace dependencies only for GW1a: `tokio`, `rustls` 0.23 (`ring`), `tokio-rustls` 0.26, `hyper` 1 and `hyper-util` 0.1 (admin), `serde`, `toml`, `clap`, `sha2`, `hex`, `thiserror`, `tracing`, `tracing-subscriber`; `rcgen` 0.14 and `tempfile` in tests. **No Pingora** in GW1 (D1013: the `http` plan adds it).
- Integration: the pinned PgDog image of `deploy/pgdog/compose.yaml` (v0.1.60, R18.1), `postgres:17.11`, and libpq 17's `psql` from that image as the reference client.

**Spec:** [§57](../design/57-loams-gateway.md) (all of it); [§46](../design/46-loams-postgres-production.md) §8; [PG2](2026-10-08-pg2-postgres-production.md) rulings R18–R21 and R22.0.

## Global Constraints

- **Worktree and branches.** Work in `~/Documents/Ostriumlabs/loams-wt/gw1`, branches `gw1-t<N>` cut from a fresh `origin/dev`. **Tasks 1–7 land as one pull request** (the working Postgres route; they are not useful apart). Every later task is its own pull request into `dev`, with one CodeRabbit round, merged with `gh pr merge <n> -R ostrium-labs/loams --admin --merge` only on a green `CI required`. Sign every commit (`git commit -s`). Never `git stash`; never commit `.superpowers/`. Commit areas: `gateway`, `pg`, `deploy`, `ci`, `docs`.
- **Rust builds** use the shared target directory. Never set `CARGO_TARGET_DIR`, never build in `/tmp`. Build the touched crate (`cargo test -p loams-gateway`), not the workspace.
- **Containers** (PgDog, Postgres) run in tests marked `#[ignore]`, run by the `gateway.yml` workflow and locally with `IT_CONTAINER_ENGINE=podman`. Remove only containers the test created.
- **The PgDog licence boundary** (§46 §8.3, R19): no PgDog crate, source or text. The image is read from `deploy/pgdog/compose.yaml`, never written into another file (R19.3's check).
- **No tenant labels on metrics, no billing names** (D1021, D548). Per-namespace numbers only through `ConnectionObserver`.
- **Pins.** A new dependency must be at least 14 days old, pass `cargo deny`, and carry the `ci:upgrade` label on its pull request.

## Review Focus

1. **Plaintext reaches PgDog or a client.** Expected: never; a plaintext startup is refused, and the gateway closes if PgDog answers `N`. Tests: Task 4 `plaintext_startup_refused`, `upstream_n_closes`; Task 7 `it_plaintext_refused`.
2. **A client reaches another namespace's PgDog.** Expected: never; SNI and ports map to exactly one namespace, and an unknown SNI is refused. Tests: Task 2 `duplicate_hostname_rejected`; Task 4 `unknown_sni_gets_unrecognized_name`; Task 6 `dedicated_port_ignores_sni`; Task 11 `it_kind_two_namespaces_isolated`.
3. **A reload drops live connections or installs a bad table.** Expected: neither. Tests: Task 2 `invalid_file_keeps_old_table`; Task 6 `reload_keeps_live_connection`, `removed_port_stops_accepting`.
4. **The splice changes the TLS session.** Expected: the client sees PgDog's certificate and handshake unchanged. Tests: Task 4 `splice_presents_upstream_certificate`; Task 7 `it_sslrequest_verify_full`.
5. **Direct TLS is accepted without ALPN, or re-encrypted without verifying PgDog.** Expected: neither. Tests: Task 5 `direct_without_alpn_refused`, `upstream_certificate_verified`.
6. **Unbounded pre-auth input.** Expected: the limits of D1020. Tests: Task 4 `oversized_client_hello_refused`, `handshake_deadline`.

## File structure

```
crates/loams-gateway/                                       Tasks 1–8, 12–14
  Cargo.toml
  src/lib.rs  config.rs  routes.rs  reload.rs  listeners.rs  tls.rs  io.rs  admin.rs  observe.rs  main.rs
  src/sniff/{mod.rs,pg.rs,tls.rs}
  src/pg/{mod.rs,front.rs,upstream.rs}
  tests/{routes.rs,pg_front.rs,listeners.rs,admin.rs,it_pgdog.rs,common/mod.rs}
  fuzz/                                                     Task 14
crates/loams-pg-control/src/gateway/{mod.rs,ports.rs,render.rs}   Task 9
crates/loams-pg-control/tests/{gateway_render.rs,golden/gateway/*.toml}
deploy/release/Containerfile.loams-gateway                  Task 8
deploy/loams-postgres-single/ (the gateway service)         Task 10 (with PG2 Task 56)
deploy/helm/loams-postgres/templates/gateway-*.yaml         Task 11 (with PG2 Tasks 22, 48)
.github/workflows/gateway.yml                               Task 7
```

## Shared contracts

```rust
// config.rs: the static config (§57 §9.3); every field also a flag.
pub struct GatewayConfig { pub admin: AdminConfig, pub routes: RoutesConfig, pub pg: PgConfig }
pub struct PgConfig {
    pub listen: SocketAddr,                 // 0.0.0.0:5432
    pub dedicated_bind: IpAddr,             // 0.0.0.0
    pub dedicated_ports: RangeInclusive<u16>, // 16000..=17999
    pub tls_mode: TlsMode,                  // Auto | Terminate | Passthrough
    pub handshake_timeout: Duration,        // 10 s
    pub max_connections: usize,             // 20 000
    pub drain_timeout: Duration,            // 60 s
    pub certs: Vec<CertConfig>,             // names, cert, key (PEM files)
    pub upstream_tls: Option<UpstreamTlsConfig>, // ca (PEM file)
}

// routes.rs
pub struct RouteTable { pub generation: String, pub pg: PgRoutes }
pub struct PgRoutes { pub default_namespace: Option<String>, pub namespaces: Vec<PgNamespace> }
pub struct PgNamespace {
    pub name: String, pub hostnames: Vec<String>, pub port: Option<u16>,
    pub upstreams: Vec<String>, pub upstream_server_name: Option<String>,
}
impl RouteTable {
    pub fn parse(text: &str, dedicated: &RangeInclusive<u16>) -> Result<RouteTable, RouteError>;
    pub fn by_hostname(&self, sni: &str) -> Option<&PgNamespace>;
    pub fn by_port(&self, port: u16) -> Option<&PgNamespace>;
}

// reload.rs
pub type RouteRx = tokio::sync::watch::Receiver<Arc<RouteTable>>;
pub async fn watch_file(path: PathBuf, every: Duration, dedicated: RangeInclusive<u16>,
                        tx: watch::Sender<Arc<RouteTable>>, shutdown: impl Future);

// sniff/pg.rs
pub enum Opening { SslRequest, GssEncRequest, Cancel([u8; 16]), Startup, Tls, Unknown }
pub async fn sniff_opening<R: AsyncRead + Unpin>(io: &mut R, buf: &mut Vec<u8>) -> io::Result<Opening>;

// sniff/tls.rs
pub struct ClientHello { pub server_name: Option<String>, pub alpn: Vec<Vec<u8>> }
pub async fn read_client_hello<R: AsyncRead + Unpin>(io: &mut R, buf: &mut Vec<u8>, limit: usize)
    -> Result<ClientHello, SniffError>;   // buf keeps every byte read

// io.rs
pub struct Prefixed<IO> { /* replays `prefix`, then reads `inner` */ }

// pg/upstream.rs
pub async fn connect_pgdog(ns: &PgNamespace, health: &Ejections, timeout: Duration)
    -> Result<(TcpStream, SocketAddr), UpstreamError>;   // ordered, SSLRequest sent, `S` read

// pg/front.rs
pub enum Entry { Shared, Dedicated(u16) }
pub struct PgFrontDoor { /* config, routes, server TLS, upstream TLS, ejections, observer */ }
impl PgFrontDoor { pub async fn serve(self: Arc<Self>, stream: TcpStream, entry: Entry); }

// observe.rs
pub trait ConnectionObserver: Send + Sync {
    fn on_open(&self, _namespace: &str, _entry: &Entry, _path: Path) {}
    fn on_close(&self, _namespace: &str, _bytes_in: u64, _bytes_out: u64, _duration: Duration) {}
}
pub enum Path { Spliced, Terminated }
```

---

## GW1a — The Postgres front door (Tasks 1–8)

### Task 1: The crate, the static config and the admin listener

**Files:** `crates/loams-gateway/{Cargo.toml,src/lib.rs,src/config.rs,src/admin.rs,src/main.rs}`, `tests/admin.rs`.

**Interfaces:** `GatewayConfig::from_toml(&str)` with §57 §9.3's defaults; `admin::serve(listener, state, shutdown)` answering `GET /healthz` (200), `GET /readyz` (200 once a route table is loaded, else 503), `GET /v1/routes` (`{"generation": …, "pg_namespaces": n}`). The binary: `loams-gateway --config <path>`.

Tests: `config_defaults_match_the_design`, `config_rejects_unknown_fields`, `readyz_waits_for_routes`, `routes_endpoint_shows_generation_only`.

Commit `feat(gateway): loams-gateway crate, config and admin listener`.

### Task 2: The route table and hot reload

**Files:** `src/routes.rs`, `src/reload.rs`, `tests/routes.rs`.

**Contract:** §57 §8.1–§8.2. Validation: names `[a-z0-9-]{1,63}`; hostnames lower-cased, unique across namespaces; ports unique and inside `dedicated_ports`; at least one upstream (`host:port`); `default_namespace` names a namespace. The generation is `sha256:<hex>` of the file without its `generation` line; a present `generation` must match. The poller re-reads every `poll_interval_ms` by digest and on `SIGHUP`; an invalid file is logged and ignored.

Tests: `parse_minimal_and_full`, `duplicate_hostname_rejected`, `duplicate_port_rejected`, `port_outside_range_rejected`, `generation_mismatch_rejected`, `hostname_lookup_is_case_insensitive`, `invalid_file_keeps_old_table`, `changed_file_is_swapped`, `sighup_reloads_at_once`.

Commit `feat(gateway): route table with validated hot reload`.

### Task 3: Upstreams: ordered choice, ejection and the PgDog handshake

**Files:** `src/pg/upstream.rs` (tests inline and in `tests/pg_front.rs`).

**Contract:** D1017. Resolve each upstream host to all of its addresses, sorted; try them in order; a failed connect ejects that address for 2 s; an ejected address is tried only when every address is ejected. After connecting, send `SSLRequest` and require `S` (`UpstreamError::NoTls` on `N`).

Tests: `first_healthy_upstream_is_used`, `failed_upstream_is_ejected_then_retried`, `all_ejected_still_tries`, `upstream_n_closes`.

Commit `feat(gateway): ordered PgDog upstreams with passive ejection`.

### Task 4: Sniffing, refusals and the splice

**Files:** `src/sniff/{mod.rs,pg.rs,tls.rs}`, `src/io.rs`, `src/pg/front.rs`, `tests/pg_front.rs`.

**Contract:** §57 §7.2 steps 1, 2, 4–7, and §7.4. The ClientHello is parsed by `rustls::server::Acceptor` over the kept bytes. Limits (D1020): 64 KiB ClientHello, 10 KiB startup packet, the handshake deadline over everything before the relay starts. Cancel: on a dedicated port, write the 16 bytes to every resolved address of the namespace; on the shared port, drop and count (Q895).

Tests (a fake PgDog in-process: rustls server with its own CA, no ALPN, echoing after TLS):
- `sslrequest_spliced_by_sni`
- `splice_presents_upstream_certificate` (the client trusts only the upstream's CA and succeeds)
- `unknown_sni_gets_unrecognized_name`
- `no_sni_uses_default_namespace`
- `plaintext_startup_refused` (`FATAL`, SQLSTATE `28000`)
- `gssenc_answered_n_then_ssl`
- `bytes_after_sslrequest_closed`
- `oversized_client_hello_refused`
- `handshake_deadline`
- `cancel_on_shared_port_dropped`

Commit `feat(gateway): postgres sniffing, refusals and the TLS splice`.

### Task 5: Direct TLS: terminate and re-encrypt

**Files:** `src/tls.rs`, `src/pg/front.rs`, `tests/pg_front.rs`.

**Contract:** §57 §7.2 step 3 and §7.3. `tls::SniResolver` picks a certificate by exact name or one wildcard label; the server config offers ALPN `postgresql` only. The upstream client config trusts `upstream_tls.ca` and verifies `upstream_server_name` (default the namespace's first hostname). `tls_mode`: `auto` (default, Q903), `terminate` (also for `SSLRequest`), `passthrough` (direct refused with `no_application_protocol`). A config with `auto` or `terminate` and no certificate fails at start.

Tests: `direct_tls_terminated_and_reencrypted`, `direct_without_alpn_refused`, `upstream_certificate_verified` (an upstream with a certificate from another CA is refused), `terminate_mode_terminates_sslrequest`, `passthrough_mode_refuses_direct`, `wildcard_certificate_selected`.

Commit `feat(gateway): direct tls terminated with alpn postgresql`.

### Task 6: Listeners: the shared port and dedicated ports

**Files:** `src/listeners.rs`, `src/lib.rs` (`Gateway::run`), `tests/listeners.rs`.

**Contract:** §57 §7.5 and §8.2. The shared listener is bound at start. After each route swap, the reconciler binds every route `port` not yet bound (on `dedicated_bind`), stops accepting on ports no longer in the table, and keeps in-flight connections. A bind failure is logged, shown on `/readyz` as `degraded` (200 with a body; the shared port still serves) and retried on the next reload. `max_connections` is a semaphore over every listener. `SIGTERM` stops accepting and waits up to `drain_timeout` for relays.

Tests: `dedicated_port_routes_without_sni`, `dedicated_port_ignores_sni`, `new_port_bound_on_reload`, `removed_port_stops_accepting`, `reload_keeps_live_connection`, `connection_cap_enforced`, `cancel_on_dedicated_port_reaches_every_replica`.

Commit `feat(gateway): dedicated namespace ports reconciled on reload`.

### Task 7: The integration test against the pinned PgDog, and its CI job

**Files:** `tests/it_pgdog.rs`, `tests/common/mod.rs`, `.github/workflows/gateway.yml`.

**Contract:** a real PgDog (the pin in `deploy/pgdog/compose.yaml`) with TLS (`tls_certificate`, `tls_private_key`, `tls_client_required = true`) and a `postgres:17.11` backend, behind an in-process gateway on random ports. Certificates from `rcgen`: one CA, PgDog's certificate for `*.gw.pg.test`, the gateway's for the same names. libpq 17's `psql` (from the `postgres:17.11` image, host network) is the client, with `hostaddr=127.0.0.1` and `host=acme.gw.pg.test`.

Tests (all `#[ignore]`; `IT_CONTAINER_ENGINE` is `docker` by default):
- `it_sslrequest_verify_full`: `sslmode=verify-full`, default negotiation; spliced.
- `it_direct_tls`: `sslmode=require sslnegotiation=direct`; terminated.
- `it_dedicated_port_without_sni`: `sslsni=0` on the namespace's port.
- `it_plaintext_refused`: `sslmode=disable` gets the `28000` error.
- `it_unknown_sni_refused`: `host=other.gw.pg.test`.

`gateway.yml` runs on pull requests and pushes that touch `crates/loams-gateway/**`, `deploy/pgdog/compose.yaml` or itself: fmt-independent `cargo test -p loams-gateway` and clippy, then the ignored tests with Docker on `ubuntu-24.04`.

Commit `test(gateway): psql through the gateway to the pinned pgdog`.

### Task 8: The image and the drain

**Files:** `deploy/release/Containerfile.loams-gateway`, `deploy/release/build.sh` (the image name), `.github/workflows/release-images.yml` (matrix).

**Contract:** a static `loams-gateway` binary on a distroless base like the other images, user 65532, `EXPOSE 5432`, entrypoint `loams-gateway --config /etc/loams-gateway/gateway.toml`. `ghcr.io/ostrium-labs/loams-gateway` joins the release matrix (D1019).

Tests: `build.sh loams-gateway --load` in the `gateway.yml` job (no push); `sigterm_drains_then_exits` in `tests/listeners.rs`.

Commit `deploy(gateway): the loams-gateway image`.

## GW1b — Wiring for PG2 Task 22 (Tasks 9–11)

### Task 9: `pg-control`: dedicated-port records and the routes renderer

**Files:** `crates/loams-pg-control/src/gateway/{mod.rs,ports.rs,render.rs}`, the namespace setting `dedicated_port` (proto field on the namespace's Postgres settings, Task 1's rules), `tests/gateway_render.rs`, `tests/golden/gateway/*.toml`.

**Contract:** D1016, D1018. `ports::allocate(ns)` takes the lowest free port in the range under a CAS on `G/<ns>` and the range index; `release(ns)` keeps a tombstone for 24 h. `render::routes(region, namespaces, settings) -> RenderedRoutes { text, generation }`, byte-identical for identical records. A writer per runtime: `DirSink`-style atomic file for compose, a ConfigMap update for Kubernetes, kicked by the same store watch as `PgDogPusher` (R21.8).

Tests: `render_is_deterministic`, `render_golden_three_namespaces`, `port_allocated_lowest_free`, `port_not_reused_within_24h`, `concurrent_allocations_unique`, `rendered_routes_parse_in_the_gateway` (a dev-dependency on `loams-gateway`).

Commit `feat(pg): gateway routes and dedicated ports from pg-control`.

### Task 10: Compose and single node

**Files:** `deploy/loams-postgres-single/compose.yaml` (the gateway service; with PG2 Task 56, or a minimal file if Task 56 has not landed, recorded as a ruling), `deploy/loams-postgres-single/gateway.toml`.

**Contract:** §57 §9.2: one gateway publishing 5432, the routes directory shared with `pg-control`, `default_namespace` set; only with Postgres (Q904).

Tests: a compose smoke in `gateway.yml`: `psql "host=localhost sslmode=require"` reaches the single namespace.

Commit `deploy(gateway): single-node compose service`.

### Task 11: Helm templates and the two-namespace isolation test

**Files:** `deploy/helm/loams-postgres/templates/gateway-{deployment,service,networkpolicy,pdb,certificate,configmap}.yaml` (in Task 48's chart; if it has not landed, a chart `deploy/helm/loams-gateway`, recorded as a ruling), goldens, and PG2 Task 22's `it_kind_two_namespaces_isolated`.

**Contract:** §57 §9.1. The Service lists 5432 and each allocated port from the rendered values; NetworkPolicy egress to `loams-pg-*` port 6432 and DNS only; PgDog namespaces admit 6432 only from the gateway's pods and `pg-control`.

Tests: `gateway_manifests_golden`, `kubeconform_passes`, `it_kind_two_namespaces_isolated` (CI only).

Commit `deploy(gateway): helm templates for the postgres front door`.

## GW1c — Hardening (Tasks 12–14)

### Task 12: PROXY protocol v2 and handshake caps per address

**Files:** `src/listeners.rs`, `src/config.rs` (`proxy_protocol = "off" | "v2"` per listener). Tests: `proxy_v2_sets_client_address`, `proxy_header_refused_when_off`, `per_address_handshake_cap`. Q901's default. Commit `feat(gateway): proxy protocol v2 and per-address caps`.

### Task 13: Metrics, the observer and spans

**Files:** `src/observe.rs`, `src/admin.rs` (`/metrics`). The series of §57 §11, no tenant label. Tests: `metrics_have_no_tenant_label`, `observer_sees_open_and_close`, `outcomes_counted`. Commit `feat(gateway): metrics, spans and the connection observer`.

### Task 14: Fuzzing

**Files:** `crates/loams-gateway/fuzz/` with targets `sniff_opening` and `client_hello_replay`; the nightly fuzz job. Tests: each target runs 60 s in the nightly without a crash. Commit `test(gateway): fuzz the pre-auth parsers`.

## Exit criteria for the Postgres front door

- Tasks 1–7 merged: `it_sslrequest_verify_full`, `it_direct_tls`, `it_dedicated_port_without_sni`, `it_plaintext_refused` and `it_unknown_sni_refused` green in `gateway.yml`.
- Tasks 9–11 merged: PG2 Task 22's `sni_route_per_namespace_golden` and `it_kind_two_namespaces_isolated` pass through the gateway.
- Tasks 12–14 before GA (PG2 Task 53's security review covers the gateway).

## Rulings made during execution

(none yet)
