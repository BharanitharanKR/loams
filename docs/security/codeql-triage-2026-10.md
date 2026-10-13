# CodeQL triage — October 2026

Snapshot of the 200 open code-scanning alerts on 2026-10-13 (all reported on
`main`, which matched `dev` at the time). Follow-up to
[the CodeQL alerts plan](../plans/2026-10-02-ops-codeql-alerts.md) (#298).

This PR fixes the alerts marked **fixed**. Nothing was dismissed: dismissal is a
repository-admin action. A maintainer should dismiss the rest with the GitHub
reason in brackets ("false positive" or "used in tests") and the one-line reason
given here, after the CodeQL run on the merged PR confirms the fixed ones closed.

| Verdict | Alerts |
|---|---|
| Fixed in this PR | 8 |
| Test-only (dismiss: used in tests) | 154 |
| False positive / accepted by design (dismiss: false positive) | 38 |
| **Total** | **200** |

## Fixed

| Rule | Alert | Location | Fix |
|---|---|---|---|
| js/polynomial-redos | #32 | `plugins/apps/server/src/index.ts` `normalizeOrigin` | The `Origin` header is attacker-controlled; trailing `/` are trimmed with a scan instead of `/\/+$/`. |
| js/polynomial-redos | #79 | `plugins/core/host/src/auth/tokens.ts` `readBearerToken` | The `Authorization` header is attacker-controlled; `\s+(\S.*)` replaces the ambiguous `\s+(.+)`. |
| js/polynomial-redos | #36 | `plugins/packages/plugin-glitchtip-adapter/src/types.ts` | Upstream `Link` header; the link-value regex is anchored at `^<`. |
| js/polynomial-redos | #39 | `plugins/packages/plugin-matomo-adapter/src/service.ts` | Upstream report text; a number may only start at the head of a digit run (`(?<!\d)`). |
| js/polynomial-redos | #245 | `web/packages/charts/src/compile.ts` | Spec `chartType`; whitespace is collapsed before the ` chart`/` plot` suffix is stripped. |
| js/prototype-pollution-utility | #82 | `plugins/core/dashboard-ui/src/theme/ink.ts` `setPath` | Refuses `__proto__`, `constructor` and `prototype` path segments. |
| py/insecure-protocol | #85 | `scripts/sqlgate/capture/proxy.py` | Fixture-capture TLS context pins `minimum_version` to TLS 1.2. |
| rust/uncontrolled-allocation-size | #125 | `crates/loams-qdrant/src/writes.rs` `update` | `planned` grows as operations are planned instead of `Vec::with_capacity(client count)` (plan ruling 8). |

#32 and #79 take request headers and are the material ones. #82 and #125 were not
reachable with hostile input (static customizer paths; an already-deserialised,
body-limited list) and are hardening; #85 is a fixture-capture tool.

Every rewritten regex was checked against the original on all short strings
over the relevant alphabet (identical results) and runs in linear time on
200,000-character adversarial inputs.

## Test-only (dismiss as "used in tests")

| Rule | Count | Alerts | Reason |
|---|---|---|---|
| rust/hard-coded-cryptographic-value | 132 | #86–#105, #118–#121, #152–#153, #156–#157, #159–#231, #236–#239, #241–#243, #246–#269 | Fixture passwords and keys in `crates/loams-sqlgate/tests/**` and `fabric/crates/loams-house-worker/tests/**`. |
| rust/hard-coded-cryptographic-value | 5 | #144–#148 | `#[cfg(test)]` module of `crates/loams-agentd-proto/src/sidebar_pins.rs` (order-key nonces, not keys). |
| rust/hard-coded-cryptographic-value | 1 | #143 | `#[cfg(test)]` module of `crates/loams-agentd-link/src/auth/oidc.rs` (PKCE test verifier). |
| rust/hard-coded-cryptographic-value | 6 | #110–#115 | `#[cfg(test)]` modules of `fabric/crates/loams-house/src/{auth,config}.rs`. |
| rust/hard-coded-cryptographic-value | 2 | #116, #117 | `fabric/crates/loams-house/src/fuzz.rs`, compiled only with the `test-hooks` feature (fuzz harness). |
| rust/cleartext-logging | 6 | #131, #132, #133, #134, #154, #272 | Assertion messages in tests (`tests/`, `provider_tests.rs`, `#[cfg(test)]` modules); test credentials only. |
| rust/cleartext-transmission | 1 | #274 | `crates/loams/tests/auth.rs` helper calling a local test server. |
| go/unvalidated-url-redirection | 1 | #31 | `apps/mobile/mock` dev mock IdP; redirecting to the caller's `redirect_uri` is what it imitates. |

## False positive / accepted by design (dismiss as "false positive")

| Rule | Count | Alerts | Reason |
|---|---|---|---|
| js/polynomial-redos | 9 | #35, #37, #40, #41, #42, #44, #77, #80, #78 | `/\/+$/` on operator configuration (`baseUrl`, `apiPrefix`, `CORS_ALLOWED_ORIGINS`), not on request data; flagged only because the functions are exported. |
| js/polynomial-redos | 1 | #84 | `apps/desktop-electron` `isSingleStatement`: SQL the local user typed into their own desktop app; worst case is a self-inflicted slow check. |
| js/stack-trace-exposure | 2 | #45, #81 | Only `err.message` is returned, never a stack; surfacing handler messages to the admin console is intentional and asserted by `plugins/core/host/tests/api.spec.ts`. |
| js/cors-misconfiguration-for-credentials | 1 | #47 | The origin is reflected only after an exact match against an explicit allowlist; `*` is refused by `resolveAllowedOrigins`. |
| js/incomplete-url-substring-sanitization | 1 | #83 | `events` is an array, so `includes` is exact element equality, not a substring check. |
| rust/hard-coded-cryptographic-value | 2 | #141, #142 | `crates/loams-agentd-doc/src/registry.rs`: resets an ID counter to `0`. |
| rust/hard-coded-cryptographic-value | 1 | #155 | `crates/loams-sqlgate/src/auth.rs`: fixed decoy password hashed for unknown users so they cost the same as known ones (timing equalisation). |
| rust/hard-coded-cryptographic-value | 4 | #106–#109 | `fabric/crates/loams-house/src/auth.rs`: empty-string defaults for absent credentials and the unknown-user dummy (same timing equalisation). |
| rust/cleartext-logging | 2 | #127, #128 | `sidebar_pins.rs`: inserts a session ID into a list; nothing is logged. |
| rust/cleartext-logging | 1 | #273 | `loams api-key new` prints the new key to stdout once, by design; only its hash is stored. |
| rust/cleartext-logging | 2 | #276, #277 | `pg_control_cli.rs` prints listen addresses, not secrets. |
| rust/cleartext-transmission | 6 | #135–#140 | Provider usage/profile calls go to `https://` endpoints; plain `http` exists only for loopback test servers (`allow_loopback_http`). |
| rust/weak-sensitive-data-hashing | 2 | #149, #150 | SHA-256 fingerprints of high-entropy provider API keys/tokens used as account identifiers, not password storage. |
| rust/weak-sensitive-data-hashing | 2 | #123, #124 | `crates/loams-sqlgate/src/codec/auth.rs`: MySQL `caching_sha2_password` is protocol-mandated SHA-256. |
| rust/weak-sensitive-data-hashing | 1 | #244 | `fabric/crates/loams-house/src/config.rs`: dev-only `password_sha256` user map (ClickHouse `password_sha256_hex` shape); API keys replace it (Q30, see FL2 plan). Accepted, not a fix target here. |
| py/clear-text-logging-sensitive-data | 1 | #275 | `scripts/ci/no-metering.py` prints a file name and a matched billing word; "private" is a package-privacy flag, not a secret. |

## Test paths in CodeQL configuration

The project's documented approach (plan #298, Task 3) is to dismiss verified
test and non-security findings with a specific reason, not to change scanning
configuration, so no config is applied here. CodeQL currently runs as GitHub
**default setup** (there is no CodeQL workflow in `.github/workflows`), which does
not read a config file. If maintainers want test fixtures excluded permanently,
that needs advanced setup with a workflow and a `.github/codeql/codeql-config.yml`
such as:

```yaml
paths-ignore:
  - "**/tests/**"
  - "**/*.spec.ts"
  - "**/*.test.ts"
  - "apps/mobile/mock/**"
```

This would not cover `#[cfg(test)]` modules inside `src/`, which still need
dismissal. Note that the 132 test-fixture crypto alerts reappeared under new
numbers after the earlier dismissals recorded in the plan, so dismissal alone
may need repeating when fixture lines move.
