# Loams Flow connectors: threat model

> **Status:** CN2 Task 25 (2026-10-13). Covers native connectors (`loams-connector-sdk` and the `loams-connector-<family>` crates, run by `NativeRuntime` inside `loams-fabric flow`) and the fallback runtimes of OR-4 (Camel `loams-connect`, Debezium Server, Iggy connector plugins). Design: [§33](../design/33-connectors.md); plan: [CN2](../plans/2026-10-10-cn2-connectors.md), Task 25 and rulings R25.1–R25.10.

## 1. Assets and actors

| Asset | Where it lives |
|---|---|
| Connector credentials (database passwords, API tokens, cloud keys, TLS keys) | Kubernetes `Secret`s in the instance's namespace (`KubeSecretStore`), or `deploy/fabric/secrets/` mode 0600 in dev (`FileSecretStore`) |
| Tenant data in flight | Source → `ingest` → Iggy → sink; checkpoints in `_fabric.flow_positions` |
| The cluster | The Kubernetes API, the Loams control-plane Services, other tenants' pods |
| The cloud account | Instance metadata endpoints (169.254.169.254, `fd00:ec2::254`, `metadata.google.internal`, 100.100.100.200, Azure WireServer 168.63.129.16) |

| Actor | Can do |
|---|---|
| A namespace member | Create instances: choose a connector, write its config (hosts, URLs, profile, overlay), and reference the namespace's secrets by name |
| A namespace admin | Set the namespace's `EgressPolicy` allowlist; create `Secret`s |
| An external system | Answer a connector: DNS answers, HTTP redirects, error messages, CDC payloads, webhook deliveries |
| A supply-chain attacker | Publish a malicious crate version or image tag |

Out of scope: a compromised node or kubelet, a compromised Loams operator account, and the external system's own security.

## 2. Tenant isolation

- **Native connectors** run as tokio tasks inside `flow` pods, shared across namespaces. Each instance has its own `ConnectorContext`: its own `SecretResolver` (scoped to one namespace by `NamespaceResolver`; a reference cannot name another namespace), its own `EgressCheck`, and its own memory `Budget` and rate limiter (Task 4, R4.7), so one tenant cannot exhaust another's memory or upstream quota. Per-namespace budgets cap the sum (Task 27).
- **Fallback pods** run per namespace group (Task 26). Their isolation is the pod boundary: a NetworkPolicy rendered from the namespace's `EgressPolicy` (`render_network_policy`), only the referenced `Secret` keys mounted, read-only, mode 0400 (`secret_mounts`), and no service-account token mounted (a requirement on Task 26's rendering, R25.9).
- **Residual risk.** A memory-safety bug in a native family crate would cross tenants inside one `flow` pod. Mitigations: `unsafe_code = "forbid"` in every Loams crate, protocol crates audited by `cargo deny` (D359), and a split of `flow` pods per namespace group if the platform needs a hard boundary (Task 26 may render it).

## 3. SSRF, DNS rebinding and redirects (Review Focus 9)

An instance config names hosts. Without a guard, a member could point an HTTP profile at the metadata endpoint, the Kubernetes API or another tenant's database, and read the answer back through the Fabric.

- **Default deny** (`EgressPolicy::for_cluster`): unspecified, loopback, link-local, the cloud metadata endpoints above, multicast and broadcast, the cluster's pod and Service CIDRs, the Loams control-plane Services; by name, `localhost`, `metadata*`, `instance-data*`, `kubernetes*`, `*.svc` and `*.cluster.local`. Private RFC 1918 ranges outside the cluster stay allowed: on-premises databases are the point of many connectors.
- **At validation** (`check_config`), every host the config names: host keys, `host:port` lists, URLs and multi-host URLs, and any URL anywhere in the config. Non-canonical IP literals (`127.1`, `2130706433`, `0x7f.0.0.1`) and `file:`/`unix:` URLs are refused. Secret paths hold secret names and are skipped. Errors name the host and the config path, never a URL's userinfo.
- **At connect, after DNS**, on every connection: `sdk::net::connect` resolves through the check's resolver hook and checks every address before dialling. A name that resolved to a public address at validation and to 169.254.169.254 later (DNS rebinding) is refused on the connection that would use it (`egress_guard_rechecks_after_dns`). IPv4-mapped, IPv4-compatible and NAT64 (`64:ff9b::/96`) IPv6 addresses are checked as the IPv4 address they carry.
- **Redirects.** `HttpClient::send` follows none. `send_following` treats each hop as a new request through the same checks, refuses `https` → `http`, drops `Authorization`, `Cookie` and `Proxy-Authorization` on a cross-origin hop, and bounds the hop count (`redirect_rechecked`).
- **Admin allowlist.** `allow_cidrs` re-opens an address range; `allow_hosts` lifts only the name check. An allowlisted name is still checked after DNS, so allowlisting a name never allowlists what it may later resolve to (R25.2).
- **Fallback pods** cannot be hooked; the NetworkPolicy enforces the CIDR half. Names cannot be expressed in a NetworkPolicy, so fallback instances rely on validation for the name half (R25.4). A cluster that resolves DNS through a node-local cache on a link-local address must allowlist that address.
- **Residual risk.** A connector whose protocol hands out further addresses (a Kafka broker's advertised listeners, a MongoDB replica set's member list, a Cassandra peer table) makes the driver connect to addresses the config never named. Family crates must route those connections through `sdk::net` (Global Constraints); a family whose protocol crate dials on its own must take a custom connector or resolver hook that calls `EgressCheck` (Tasks 9, 14, 16 check this in review).

## 4. Secret exposure (Review Focus 8)

- **At rest:** only in a Kubernetes `Secret` (or a 0600 dev file). Never in a manifest, an instance spec, a profile, a rendered ConfigMap, a NetworkPolicy, a Deployment, a checkpoint, a metric label or an error (`no_secret_in_rendered_kube_objects`). Fallback configs reference secrets as mounted files (`config_with_secret_files`).
- **In memory:** `Secret` is zeroed on drop; its `Debug` is `<redacted>` and it has no `Display`. `KubeSecretStore` zeroes the decoded values and the parsed `data` map; the raw HTTP body is a shared `Bytes` that cannot be zeroed and is dropped right after parsing (residual).
- **In native logs:** the SDK never formats a `Secret`; `ScrubbingResolver` records each resolved secret so the pipeline scrubs positions, errors, DLQ records, observer messages and status (R4.5).
- **In fallback logs:** the fallbacks are told to mask what they can (Camel `logMask`, R25.7); the supervisor then masks known secret option names and URL passwords, and drops and counts any line that still holds a resolved secret's bytes, raw, base64 or percent-encoded (`no_secret_in_fallback_logs`).
- **Kubernetes API access:** the `flow` service account needs `get` on `secrets` in the namespaces it serves, and nothing else on `Secret`s (no `list`, no `watch`); Task 26's chart is to render that Role per namespace (R25.9).
- **Residual risk.** A secret transformed beyond those encodings (hex, a hash, a substring) can still reach a fallback's log; a secret shorter than four bytes is not scrubbed (it would redact ordinary text and is not a credential).

## 5. Webhook forgery and replay

Requirements on the webhook sources (CN1's webhooks ★ and the HTTP-API profiles' `webhook` block, Tasks 8 and 20), which are not built yet:
- verify the provider's signature (`hmac-sha256` and the provider schemes of Task 20) over the raw body, with a constant-time compare, before parsing anything;
- refuse a delivery older than the provider's timestamp tolerance (default 5 min), and use the delivery id as the event id, so `ingest`'s `(source, id)` dedup drops a replay inside the window.
- Endpoints stay on loopback (D111) until Q30's auth plan opens remote ingress (Task 28, OR-17).

## 6. Malicious profile or overlay content

A Camel overlay or a user-supplied route runs inside a fallback pod, where some endpoints and expression languages execute commands, read files (including mounted secrets) or instantiate classes. `check_overlay` refuses, anywhere in the document (`overlay_refuses_bean_exec_file_schemes`):
- endpoint schemes `exec:`, `file:`, `bean:`, `class:`, `language:`, `jndi:`, `ssh:`, `scp:`, `docker:`, `stub:` and `ref:`;
- bean and class references `#bean:`, `#class:`, `#type:` and `#valueAs(`;
- property functions that read the pod: `{{env:`, `{{sys:`, `{{file:`, `{{bean:`;
- `beans:` entries with a `type` (or `builderClass`);
- scripting expression languages (Groovy, Java/jOOR, JavaScript, MVEL, OGNL, SpEL, Python, Kotlin, `script`).
Task 22 adds the positive list of languages (`simple`, `constant`, `header`, `jq`, `jsonpath`, `jsonata`). HTTP-API profiles (Task 7) are data run by the native HTTP engine: their `base_url` goes through the egress guard like any config host.

## 7. CDC privilege scope

| System | Role the connector needs | Never grant |
|---|---|---|
| Postgres | `REPLICATION` login plus `SELECT` on the published tables; the publication created by an owner | superuser, `pg_read_server_files`, `pg_execute_server_program` |
| MySQL / MariaDB | `REPLICATION SLAVE`, `REPLICATION CLIENT`, `SELECT` on the captured schemas | `SUPER`, `FILE`, `PROCESS` beyond the above |
| SQL Server | `db_datareader` plus `SELECT` on the `cdc` schema | `sysadmin`, `db_owner` |
| MongoDB | `read` on the watched databases plus `changeStream` | `root`, `clusterAdmin` |
| Sinks | write on the target tables, plus the checkpoint table for exactly-once (OR-7) | DDL beyond declared schema evolution (Task 5) |

The guides (Tasks 10–16) document these roles; the suites run with them, not with an administrator.

## 8. Supply chain

- Crates: `cargo deny check licenses bans sources advisories` in CI (D359, Task 3); every new crate at least 14 days old, its version and date in the commit and in `cn2-dependency-spike.md`; protocol crates confined to their family crate (OR-6).
- Images: pinned by digest; `ci-service` and `user-supplied` components never in an image Loams builds (Task 3's `no_ci_only_or_user_supplied_in_images`).
- Fallback runtimes: Camel, Debezium Server and the Iggy runtime pinned by digest (OR-4); no Loams-written Java (`no_java_sources_in_tree`).

## 9. Tests

| Threat | Test |
|---|---|
| SSRF to metadata, loopback and the cluster | `egress_guard_refuses_metadata_loopback_and_cluster` |
| DNS rebinding | `egress_guard_rechecks_after_dns` |
| Redirects | `redirect_rechecked` |
| Fallback pod egress | `network_policy_rendered_from_policy` |
| Secrets in fallback logs | `no_secret_in_fallback_logs` |
| Secrets in rendered objects | `no_secret_in_rendered_kube_objects` |
| Executable overlays | `overlay_refuses_bean_exec_file_schemes` |
| Production secret store | `kube_secret_store_reads_secret`, `file_secret_store_reads_owner_only_files` |
| Secrets in positions and errors | Task 4 `no_secret_in_position_or_error` |
