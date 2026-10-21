# MK1 — The Loams Agent Marketplace

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task, test first. Where this plan gives exact names, paths, fields or tests, use them verbatim. Record any deviation in "Rulings made during execution" at the end of this file.
>
> **Status: Approved stub** (owner, 2026-10-21, all defaults). Track MK, design [§73](../design/73-agent-marketplace.md) (D1445–D1479, Q1122–Q1144), with [§72](../design/72-software-factory-loop.md) for the factory agents that are its first listings. This plan specifies **MK0** (Mastra first, first-party only) task by task. MK1 (any framework, third-party), MK2 (more targets and kinds) and MK3 (paid listings, only if the owner decides) are listed at milestone level.

**Goal (MK0):** a Loams Desktop user opens the Marketplace page, finds a first-party Mastra agent, an ACP or built-in harness, or an MCP server, sees exactly what it will be allowed to do, installs it, and uses it in Studio; the listing came from a signed static index whose entries were checked for provenance and passed the Core level of the agent package conformance. The schema, the runtime contract and the conformance runner are framework-neutral from the first commit, even though MK0 lists only Mastra agents.

**Architecture** (§73 §3–§9):
- **`ostrium-labs/loams-marketplace`** (new public repository, created by the owner): listings, schemas, CI, the index build and signing.
- **`schemas/marketplace/listing.v1.json`** and **`proto/loams/marketplace/v1/listing.proto`** in this repository: the manifest's two forms, kept equal by a test.
- **`agents/loams-listing`** (TypeScript CLI, a member of the `agents/*` workspace that FT0 Task 1 adds): `lint`, `from-package` (turn a `loams.agent` block into `listing.json`), `index build`.
- **`crates/loams-agentd-marketplace`** (new): sources, index verification, revocations, install planning, installers for the `node` runtime, harness descriptors and MCP servers, the environment contract (§73 §3.3), and the conformance runner.
- **`proto/loams/agentd/v1/marketplace.proto`**: `MarketplaceService`.
- **`ostrium-labs/loams-mastra`**: `packages/agent-host` (`@loams/agent-host`) and `packages/acp` (`@loams/mastra-acp`, shared with FT0 Task 2).
- **`web/plugins/marketplace`** (`@loams/plugin-marketplace`, modes console and studio) and **`loams marketplace`** in the CLI.

**Tech stack:** Rust 1.97 (workspace dependencies only; `sigstore` verification through the crate the release pipeline already uses, else a ruling in Task 0); TypeScript and Node ≥ 22.13; `@mastra/*` at `loams-mastra`'s pinned set; GitHub Actions on GitHub-hosted runners for the index.

**Spec:** [§73](../design/73-agent-marketplace.md) (all of it); [§53](../design/53-studio-ade.md) §6; [§60](../design/60-places-and-harness-composition.md); [§65](../design/65-loams-operator.md) for the conformance shape; the draft [publishing guide](../marketplace/publishing.md).

## Global Constraints

- **Worktrees and branches.** `~/Documents/Ostriumlabs/loams-wt/mk0-<task>`; one PR per task into `dev`, or into `loams-mastra` / `loams-marketplace` `main` where the task says. `git commit -s`.
- **Light Rust builds.** `cargo build -p loams-agentd-marketplace`; never `--workspace` locally; never build in `/tmp`.
- **Metadata only.** The index never contains package bytes.
- **No code at install.** npm installs run with scripts off; anything that needs a script is refused.
- **Nothing before consent.** No Place, role, key, file or process is created before the person confirms the generated install plan.
- **First-party only in MK0.** The index CI refuses namespaces other than `ostrium` and `loams` until MK1.
- **No telemetry.** Neither agentd nor the index reports installs.

## Review Focus

1. **An unsigned or wrongly signed index is accepted.** Expected: refused. Tests: `index_signature_required`, `index_identity_pinned`.
2. **An artifact whose integrity or provenance does not match is installed.** Expected: refused. Tests: `integrity_mismatch_refused`, `provenance_repo_mismatch_refused`.
3. **An agent gets anything outside the environment contract.** Expected: never. Test: `agent_env_is_exactly_the_contract`.
4. **An undeclared tool runs.** Expected: refused. Tests: `undeclared_acp_tool_refused`, `undeclared_mcp_tool_hidden`.
5. **A revoked version keeps running.** Expected: stopped within one check. Test: `disable_revocation_stops_agent`.
6. **Something is created before consent.** Expected: never. Test: `plan_install_creates_nothing`.

## MK0 — Mastra first, first-party only (Tasks 0–11)

### Task 0 — Rulings

Check and record: the owner has created `ostrium-labs/loams-marketplace` (Q1124); `@loams/mastra-resonate` 0.1.0 is published with provenance; the Rust Sigstore verification library and its licence; how npm provenance is verified offline from a package's attestations; ACP's permission request as agentd's ACP harness maps it today; the GitHub and Plane MCP servers' licences, transports and `server.json` (Q1129); how agentd's `AcpAgentSpec` can be registered at run time (today it is a static table).

Files: this plan. Tests: none. Gate: rulings recorded.

### Task 1 — Listing schema v1

Files: `schemas/marketplace/listing.v1.json` (every field of §73 §3.6, with `runtime.kind` enumerating `node`, `python`, `oci`, `binary`; `surfaces` requiring `acp` or `a2a` for agents; `pricing` const `free`); `proto/loams/marketplace/v1/listing.proto`; fixtures under `schemas/marketplace/testdata/` (valid agent, harness and MCP-server listings; invalid ones for each rule).

Tests: `listing_schema_accepts_fixtures`, `listing_schema_rejects_each_invalid_fixture`, `proto_and_schema_agree`.

Commit: `marketplace: listing schema v1 (MK0 Task 1, D1455)`.

### Task 2 — `loams-listing` CLI

Files: `agents/loams-listing/` with `lint <listing.json>`, `from-package <dir>` (reads `package.json` `loams.agent`), `index build <listings-dir> --out <dir>`.

Tests: `from_package_round_trips`, `lint_reports_each_rule`, `index_build_is_deterministic`.

Commit: `marketplace: the loams-listing CLI (MK0 Task 2)`.

### Task 3 — The index repository (in `loams-marketplace`)

Files: layout of §73 §5; `.github/workflows/validate.yml` (on pull requests: lint, namespace check (first-party only), artifact resolution, integrity and provenance check, LAPC Core run from Task 8, a report comment); `.github/workflows/publish.yml` (on `main`: `index build`, Sigstore keyless signing of `index.json` and `revocations.json`, publish to GitHub Pages; a release artifact for `loams-site`).

Tests: workflow self-tests on fixture listings, `unsigned_publish_fails`.

Commit (in `loams-marketplace`): `index: validate and publish a signed static index (MK0 Task 3, D1460)`.

### Task 4 — `MarketplaceService` and sources

Files: `proto/loams/agentd/v1/marketplace.proto` (`ListSources`, `AddSource`, `RemoveSource`, `Search`, `GetListing`, `PlanInstall`, `Install`, `Uninstall`, `ListInstalled`, `CheckUpdates`, `Update`); `crates/loams-agentd-marketplace/src/{sources,index,verify,revocations}.rs`; the official source pinned to the publish workflow's identity by default.

Tests: `index_signature_required`, `index_identity_pinned`, `search_filters_by_kind_framework_level`, `revocations_checked_on_start`.

Commit: `agentd: MarketplaceService, sources and signed index verification (MK0 Task 4, D1462)`.

### Task 5 — Install planning and the environment contract

Files: `crates/loams-agentd-marketplace/src/{plan,contract}.rs`: `PlanInstall` returns the consent plan (Place preview, tools by risk, egress, dependencies, secrets, models, budgets, licence, tier, provenance, version age) and creates nothing; `contract.rs` builds the §73 §3.3 environment (token file rotation every 10 minutes for 15-minute tokens; a Postgres role per install when declared; proxied MCP config).

Tests: `plan_install_creates_nothing`, `plan_lists_every_declared_permission`, `agent_env_is_exactly_the_contract`, `token_file_rotates`.

Commit: `agentd: install plans and the agent environment contract (MK0 Task 5, D1453, D1466)`.

### Task 6 — Installers: `node` agents, harness descriptors, MCP servers

Files: `crates/loams-agentd-marketplace/src/install/{node,harness,mcp}.rs`: npm tarball fetched by integrity, lockfile install with scripts off, into `<data>/agents/<id>/<version>`; provenance verified; harness descriptors registered as `AcpAgentSpec` entries at run time (`HarnessId::Marketplace(listing_id)`); MCP servers registered for harness runs and agents with secrets in the vault; uninstall keeps user data.

Tests: `integrity_mismatch_refused`, `provenance_repo_mismatch_refused`, `install_scripts_refused`, `harness_descriptor_appears_in_list_harnesses`, `mcp_server_available_to_threads`, `uninstall_keeps_data`.

Commit: `agentd: installers for node agents, harnesses and MCP servers (MK0 Task 6, D1464, D1473)`.

### Task 7 — `@loams/agent-host` (in `loams-mastra`)

Files: `packages/agent-host/`: loads `createAgent(ctx)`; builds `ctx` from the environment contract (`PostgresStore` and `PgVector` on `LOAMS_PG_URL`, `@mastra/qdrant` on `LOAMS_QDRANT_URL`, the Resonate engine on `LOAMS_DURABLE_URL`, `MCPClient` from `LOAMS_MCP_CONFIG`); registers only declared tools; serves ACP through `@loams/mastra-acp`.

Tests: `host_loads_starter_agent`, `host_registers_only_declared_tools`, `host_reads_only_contract_env`, `host_exits_on_sigterm`.

Commit (in `loams-mastra`): `agent-host: run a packaged Mastra agent under the Loams contract (MK0 Task 7, D1456)`.

### Task 8 — LAPC Core runner

Files: `crates/loams-agentd-marketplace/src/conformance/`: `loams agent conformance <package|listing>` runs LAPC-C1 to C9 (§73 §9) in a sandboxed process (process isolation and an egress-recording proxy in MK0), with a scripted model endpoint and a fake MCP server; writes a JSON report; `--sign` signs it with the CI identity.

Tests: one passing and one failing fixture agent per requirement (`lapc_c4_undeclared_tool_fails`, `lapc_c5_egress_fails`, `lapc_c7_hang_fails`, …).

Commit: `agentd: the agent package conformance runner, Core level (MK0 Task 8, D1469)`.

### Task 9 — Console and Studio

Files: `web/plugins/marketplace/` (`modes: ["console","studio"]`): browse with filters, the listing page, the consent screen from `PlanInstall`, installed agents with updates and revocation banners; Studio's harness picker "Add a harness…".

Tests: component tests for the consent screen (every permission shown), a Playwright check in `studio-e2e`: install the fixture agent and open a thread with it.

Commit: `console: the marketplace plugin and consent screen (MK0 Task 9, D1465)`.

### Task 10 — CLI

Files: `loams marketplace search|info|install|uninstall|list|update|source` through agentd.

Tests: CLI integration tests against a fixture index.

Commit: `cli: loams marketplace (MK0 Task 10)`.

### Task 11 — First-party listings and docs

Listings in `loams-marketplace`: `ostrium/loams-bot` and the factory's agents (after FT0 Task 6), `ostrium/loams-starter`, `ostrium/support-agent` (MA1 Task 43), the built-in harness descriptors, and the MCP servers of Task 0's ruling. Docs: `docs/marketplace/README.md` (status **experimental**, first-party only); a note at the top of `docs/marketplace/publishing.md` pointing to §73; the plans README row.

Tests: every listing passes `validate.yml` and LAPC Core.

Commit: `marketplace: first-party listings and docs (MK0 Task 11)`.

## MK1 — Any framework, third-party (milestone level)

The `python` (wheels only, `uv`, hashes) and `binary` runtimes; agentd's A2A bridge from `loams-agentd-link`; the model proxy (§73 §3.4) required for third-party agents; LAPC Compatible; `ostrium-labs/loams-agent-templates` and conformance cells for Pydantic AI, LangGraph, the OpenAI Agents SDK and Google ADK (Q1133); Resonate's Python SDK cell against Loams Durable (Q1143); namespace verification (GitHub OIDC, DNS, HTTP); the scanning pipeline (§73 §10.2); verified and community tiers; revocation runbook; third-party ACP harnesses such as fx; everything third-party in SB1's `linux` sandbox; the publishing guide rewritten. Estimate: 12 engineer-weeks.

## MK2 — More targets and kinds (milestone level)

The `oci` runtime; the BYOC `agents` profile; app BYOC agents on §71's contract; `loams-registry` with a publish API and MCP-Registry v0.1 compatibility; a curated MCP Registry import; templates, workflows, console plugins, connectors and factory apps as kinds; the Jailer and the gateway's `mcp` module as enforcement points. Estimate: 9 engineer-weeks.

## MK3 — Paid listings (only if the owner decides)

Through the Meter's user meters (D1138). Not designed.

## Rulings made during execution

None yet.
