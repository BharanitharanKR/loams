# Loams v0.1.0-preview.1: run the whole stack yourself

Status: **preview**, v0.1.0-preview.1, first published 2026-10-12. Everything listed as
**Preview** in the [README's feature table](../../README.md#features) ships in this release.
Preview means built, tested in CI and usable, but not production ready: APIs, on-disk formats,
flags and image layouts can still change without a migration path, and parts of the security
model are not hardened yet (see [Security model](#security-model)).

v0.1.0-preview.1 ships one path: **your own Tailscale tailnet plus our hosted console at
`https://console.loams.dev`**. You run the Loams stack; we host the console and, for the
optional `oidc` mode, the auth service.

| Piece | Who runs it | Where |
|---|---|---|
| The engine stack: `loams`, TiDB on TiKV and PD, the Postgres stack | **You** | Docker Compose on your laptop, or on your own VMs (BYOC) with `deploy/release/byoc/install.sh` over SSH |
| The private network between your devices and the stack | **You** | Your own Tailscale tailnet |
| The React console (static files) | Ostrium | `https://console.loams.dev` |
| The auth service (OIDC issuer), only for `oidc` mode | Ostrium | `https://auth.loams.dev` |

The engine is never exposed on the public internet. Your browser reaches it over the tailnet,
through the Tailscale client on your own device.

**Coming soon, not in this release:** the console on your own Vercel or Cloudflare project,
self-service OIDC clients (`auth.loams.dev/register`), Headscale networks, one-click OAuth
deploy, and House/Analytics wired to the engine.

## Architecture

```mermaid
flowchart LR
  subgraph hosted["Hosted by Ostrium"]
    console["React console at console.loams.dev<br/>static assets"]
    auth["Auth service (oidc mode only)<br/>auth.loams.dev"]
  end

  subgraph device["Your device"]
    browser["Browser"]
    tsclient["Tailscale client"]
  end

  coord["Tailscale coordination"]

  subgraph stack["Your engine stack: Docker Compose, local or on your VMs (BYOC)"]
    ts["tailscale sidecar<br/>TS_HOSTNAME=loams<br/>tailscale serve"]
    loams["loams<br/>engine"]
    tidb["tidb"]
    tikv["tikv"]
    pd["pd"]
    pg["Postgres stack<br/>deploy/loams-postgres-dev"]
  end

  browser -- "1. load the console (HTTPS)" --> console
  browser -. "2. sign in (oidc mode only)" .-> auth
  browser -- "3. API calls" --> tsclient
  tsclient -- "WireGuard, tailnet only" --> ts
  ts -- "127.0.0.1:7781, identity headers" --> loams
  loams -. "oidc mode: verify JWT against JWKS (cached)" .-> auth
  loams --> tikv
  tidb --> tikv
  tikv --> pd
  loams --> pg
  tsclient -.-> coord
  ts -.-> coord
```

1. The browser loads the console's static files from `console.loams.dev`. The console holds
   no data and no secrets.
2. In the default `tailscale` mode there is no separate sign-in: the console calls your
   engine and Tailscale tells the engine who you are. In `oidc` mode the console signs you
   in with Authorization Code + PKCE against the auth service; tokens live in memory only,
   never in `localStorage`.
3. The console calls your engine at `LOAMS_ENGINE_URL`, a tailnet URL such as
   `https://loams.<tailnet>.ts.net`. The request leaves your browser through your device's
   Tailscale client and reaches the stack's `tailscale` sidecar. Off the tailnet the URL does
   not resolve and nothing answers.
4. In `oidc` mode the engine verifies the token against the issuer's JWKS (fetched outbound,
   cached). The auth service never connects to your stack, and never sees your data.

## What is in the stack

The compose file is [`deploy/release/compose.yaml`](../../deploy/release/compose.yaml), with
its settings in `deploy/release/.env.example`.

| Service | Image | Role |
|---|---|---|
| `pd` | PD | TiKV's placement driver |
| `tikv` | TiKV | Metadata, Live transactions, TiDB's storage |
| `tidb` | `ghcr.io/ostrium-labs/tidb:v8.5.8-loams.1` | Loams SQL (MySQL compatible) |
| `loams` | `ghcr.io/ostrium-labs/loams` | The engine, built with `live,live-tikv,durable,graph,tikv,pgwire,mysql-wire,stream-grpc` |
| `house` (profile `house`) | `ghcr.io/ostrium-labs/loams-fabric` | Loams House (the front, with `loams-house-worker` inside). Not started by default and not wired to the engine in this release: the engine answers `house_not_configured` |
| Postgres stack | pinned in `deploy/pins/loams-postgres-images.toml` | Included from [`deploy/loams-postgres-dev`](../../deploy/loams-postgres-dev/README.md) |
| `console` (profile `console`, optional) | static console | A local copy of the console, for local trials; the supported console is the hosted one |
| `tailscale` | `tailscale/tailscale` | The stack's only door: joins the tailnet as `loams` and serves the engine there |
| `headscale` (profile `headscale`) | Headscale | Coming soon, unsupported in this release (see [the tailnet](#the-tailnet)) |

`ghcr.io/ostrium-labs/loams-agentd` (the agent daemon) is published too, and is optional.
Images are tagged `0.1.0-preview.1` and the rolling `:preview`, never `:latest`. The compose
file pins `0.1.0-preview.1` through `LOAMS_VERSION`. The patched TiDB image
`ghcr.io/ostrium-labs/tidb:v8.5.8-loams.1` is public and amd64 only.

Only the `tailscale` service is reachable from outside the compose network. Nothing binds
`0.0.0.0` on the host by default.

## Quick start: fully local

You need Docker with the Compose plugin (v2.20.3 or later, for `include`), several GB of free
memory (TiKV, TiDB and the Postgres stack are the large ones), and the Tailscale client on the
machine whose browser you will use.

```sh
git clone https://github.com/ostrium-labs/loams.git
cd loams/deploy/release
cp .env.example .env
```

Edit `.env`:

- `TS_AUTHKEY`: an auth key from your Tailscale admin console (Settings, Keys). A tagged,
  non-ephemeral, single-use key is the right shape; see [the tailnet](#the-tailnet).
- `TS_HOSTNAME=loams`: the stack's name on the tailnet. Keep it unless you run two stacks.
- `LOAMS_AUTH_MODE`: `tailscale` by default, which needs nothing more. Set
  `LOAMS_TAILSCALE_ALLOWED_USERS` to narrow who is admitted. See [Auth modes](#auth-modes).

Then start it:

```sh
docker compose up -d
docker compose ps                                   # every service running
docker compose exec tailscale tailscale status      # the stack is on your tailnet
```

The engine's URL is `https://<TS_HOSTNAME>.<tailnet>.ts.net`, by default
`https://loams.<tailnet>.ts.net`.

Enable **MagicDNS** and **HTTPS certificates** for your tailnet (Tailscale admin console, DNS),
so the sidecar can serve `https://loams.<tailnet>.ts.net`. Open
[console.loams.dev](https://console.loams.dev) in a browser on a device that is on the same
tailnet, and enter that URL as your engine.

From the CLI or an SDK, set `LOAMS_AUTH_MODE=apikey`, create a key with `loams auth new-key`
and use the same tailnet URL.

To stop, `docker compose down`. Your data stays in the compose volumes; `docker compose
down -v` deletes it.

### Without a tailnet

For a laptop-only trial with nothing on any network, set `LOAMS_AUTH_MODE=off` and use the
optional local `console` service. `off` is accepted only for loopback clients: the engine
refuses every request that does not come from loopback, so this mode cannot be reached over
the tailnet. Do not use it on a shared machine.

## BYOC: your own VM over SSH

The stack is the same on a VM: one VM for a trial, or the services split across VMs that share a
tailnet. The VM needs no inbound port other than SSH, and SSH itself can move onto the tailnet
once the stack is up.

From a checkout on your own machine, `deploy/release/byoc/install.sh` does it over SSH: it
installs Docker if missing, copies the compose bundle, writes `.env` (asking for your
Tailscale auth key) and starts the stack.

```sh
deploy/release/byoc/install.sh -i ~/.ssh/id_ed25519 you@your-vm
```

By default it starts `loams`, `tailscale` and `tidb`; `--all` starts every service,
the Postgres stack included, and `--version` picks the image tag (default `0.1.0-preview.1`).
`byoc/uninstall.sh` removes it. To do it by hand instead, copy `deploy/release` to the VM,
`cp .env.example .env`, set `TS_AUTHKEY` and run `docker compose up -d`.

Your device then reaches the engine at `https://loams.<tailnet>.ts.net`, exactly as in the
local quick start. Nothing on the VM listens on its public address.

### The tailnet

**Tailscale.** Create an auth key in the admin console (Settings, Keys), tagged for example
`tag:loams`, and put it in `TS_AUTHKEY`. Add a policy rule that lets your own devices reach
`tag:loams` on port 443, and nothing else reach it. Leave `TS_LOGIN_SERVER` empty.

**Headscale: coming soon, unsupported in this release.** The `headscale` profile and
`TS_LOGIN_SERVER` exist in the compose file, but Headscale issues no `*.ts.net` HTTPS
certificates, so the hosted console (HTTPS) cannot call the engine through it: the browser
blocks it as mixed content. Use Tailscale for v0.1.0-preview.1.

## The console on your own Vercel or Cloudflare: coming soon

Hosting the console on your own Vercel or Cloudflare project is not part of v0.1.0-preview.1.
Use `https://console.loams.dev`. Likewise not in this release: self-service OIDC clients at
`auth.loams.dev/register` and one-click OAuth deploy.

## Security model

**What protects the engine in this preview:**

- **Tailnet only.** The engine has no public address. Only the `tailscale` sidecar is
  reachable from outside the compose network, and only for tailnet members your tailnet
  policy allows. Nothing binds `0.0.0.0` on the host by default.
- **Authentication on every request**, chosen by `LOAMS_AUTH_MODE`.
- **The auth service never reaches your stack.** In `oidc` mode the engine fetches the
  issuer's JWKS outbound; the auth service sees sign-ins, not data.
- **No tokens in browser storage.** In `oidc` mode access tokens are kept in memory.

### Auth modes

| `LOAMS_AUTH_MODE` | Who it accepts | Settings |
|---|---|---|
| `tailscale` (default) | The Tailscale identity that `tailscale serve` adds. The engine trusts it only on the dedicated listener `LOAMS_TAILSCALE_LISTEN` (`127.0.0.1:7781`), which is the sidecar's serve target; every other listener ignores those headers, so a forged one on the main port gets a 401. Browser requests are limited by CORS to `https://console.loams.dev` (or `LOAMS_CORS_ORIGINS`) | `LOAMS_TAILSCALE_LISTEN`, `LOAMS_TAILSCALE_ALLOWED_USERS` (logins; empty admits every user your tailnet policy lets reach the node) |
| `oidc` | Bearer JWTs from the issuer (`https://auth.loams.dev` or your own), RS256 or EdDSA, verified against its JWKS (cached) | `LOAMS_OIDC_ISSUER`, `LOAMS_OIDC_AUDIENCE=loams-console`, and one of `LOAMS_OIDC_ALLOWED_EMAILS`, `_SUBJECTS` or `_GROUPS`: the engine refuses to start without one |
| `apikey` | API keys for the CLI and SDKs; the file holds only hashes. Create one with `loams auth new-key` | `LOAMS_API_KEYS_FILE` |
| `off` | Loopback clients only; every other client is refused | none |

The compose default is `tailscale`.

**What is not hardened yet:**

- **Authorization is coarse.** A valid token or key can do anything the engine can do; there
  are no per-namespace roles or tenant quotas yet (planned).
- **Keep the other listeners inside the stack.** The tailnet sidecar serves the engine's main
  port. The Qdrant, Elasticsearch, Flight SQL, Postgres-wire and MySQL-wire listeners, Resonate
  and the House's ClickHouse HTTP interface stay inside the compose network unless you publish
  them. Do not publish them in this preview: the auth modes above are specified for the main
  port, and the other listeners are not all reviewed against them.
- **Traffic inside the compose network is plain text**, between `loams`, the House, TiDB, TiKV,
  PD and the Postgres stack. The tailnet encrypts the hop from your device to the stack.
- **The Postgres development stack uses default credentials** and `fsync = off` in its
  compute spec (see its [README](../../deploy/loams-postgres-dev/README.md)).
- **No backups or upgrades are automated.** Back up the compose volumes yourself; a later
  preview may need a fresh stack.
- **No external security review** has been done.

Report anything you find through [SECURITY.md](../../SECURITY.md), not in a public issue.

## Known limitations

- **SDKs.** Use the SDKs from the repository (`sdks/<language>`); none is published to a package
  registry yet. Supported in this preview, each tested in CI against a live server: TypeScript,
  Python, Go, Rust, C#, C++ and Swift. Coming soon: Java, Kotlin, Ruby, PHP, Dart and Objective-C.

- **Loams Postgres:** the control plane runs (PG2 Tasks 1 to 9: projects, branches, roles,
  databases, the reconcilers and the Neon upcall API), but computes and endpoints are not
  provisioned through it yet. In this preview the Postgres stack runs Neon's storage and
  computes from `deploy/loams-postgres-dev`, and tenants, timelines and computes are created
  by hand as its README shows. Its images are pinned to Neon's last public build until the
  fork's own builds (NF1 Task 14).
- **Loams Graph:** durability is partial. Writes reach the Loams log before they are
  acknowledged (GR1 Task 11, [#424](https://github.com/ostrium-labs/loams/pull/424)), but
  crash recovery (GR1 Task 14) is not done. Keep a copy of graph data you cannot recreate.
- **Loams Live:** a change made on one node can take up to 60 seconds to reach live queries
  on another node (cross-node pickup). The deploy flow (versioned deployments with compare-and-swap activation) is in
  progress ([#419](https://github.com/ostrium-labs/loams/pull/419)). Documents, transactions,
  indexes and live queries work.
- **Loams House:** over the ClickHouse HTTP interface, a result larger than one engine block
  (about 65 000 rows) in a format with a header or footer (JSON, CSVWithNames, Parquet, Arrow)
  comes back malformed (HS1 ruling R8.4); the fix is pending. The `loams.house.v1` API, which the
  console uses, is not affected.
- **Console:** several pages (Jobs, Connectors, SQL, Postgres control plane, Live
  deployments) still run against mocked clients and show demonstration data, not your stack.
  House and Analytics are not wired to the engine in this preview (`--profile house` exists,
  but the engine answers `house_not_configured`).
- **Durable on TiKV** is in progress: Loams Durable keeps its state in SQLite on the `loams`
  service's data.
- **Not in this release:** Loams Git, connectors and jobs; Headscale networks; the console on
  your own Vercel or Cloudflare; self-service OIDC clients; one-click OAuth deploy.
- **Browsers on Chromium** may ask for permission the first time a page on a public origin
  calls an address on your tailnet (Local Network Access). Allow it for the console's origin.
- **No upgrade path between previews.** Formats can change; read the
  [changelog](../../CHANGELOG.md) before pulling a new `:preview`.

## Reporting issues

- **Bugs and rough edges:** open an issue at
  [github.com/ostrium-labs/loams/issues](https://github.com/ostrium-labs/loams/issues). Include
  the image tags (`docker compose images`), your `LOAMS_AUTH_MODE`, your Tailscale setup, and the output of `docker compose logs <service>` for the failing service.
  Remove tokens, API keys and `TS_AUTHKEY` from anything you paste.
- **Security issues:** privately, through [SECURITY.md](../../SECURITY.md).
- **Questions and ideas:** GitHub Discussions on the same repository.

## Related

- [Publishing](publishing.md): how the images reach `ghcr.io/ostrium-labs`.
- [Private networking (§43)](../design/43-private-networking.md) and
  [multi-tenant BYOC control plane (§41)](../design/41-multitenant-byoc-control-plane.md): where
  the managed BYOC path goes after the preview.
