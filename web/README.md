![Loams — Your data. Your bucket.](../docs/assets/loams-banner.svg)

# web

The Loams console and its design system (design [§19](../docs/design/19-console-identity-and-agents.md)).

| Package | What |
|---|---|
| `packages/ui` (`@loams/ui`) | Tokens, the mark and wordmark, grain textures and React primitives: buttons, status tags, cards, empty states, fields, tables, stats, notices, dialogs, snippets, meters, avatars, formatters. Plain CSS with `loams-` classes; works in Vite and Next.js |
| `apps/console` (`@loams/console`) | One single-page app for OSS, Loams Cloud and BYOC, served at the base path's root (`/ui/`). It reads `InstanceService/GetInstance` to know its edition and the APIs the server serves, and `AuthService/ListProviders` for its sign-in methods |

### The cordis console (design §37 §5, AP1a)

The console is a [cordis](https://github.com/cordiverse/cordis) v4 application: a small host boots a cordis context from a plugin catalog, and everything else is a plugin with a manifest, declared services, slots and permissions. The original React console's pages moved into plugins (AP1a Task 5, API2 Task 10); its old addresses (`/ui/teams`, `/ui/projects/<p>/environments/<e>`, `/ui/cordis.html`) land on the same pages' hash routes (`/ui/#/teams`).

| Package | What |
|---|---|
| `packages/cordis` (`@loams/cordis`) | The only import of cordis (pinned `4.0.0-rc.10`) and its patch log |
| `packages/slots` (`@loams/slots`) | Typed `single`, `list` and `keyed` slots, the React renderer, per-entry error boundaries |
| `packages/console-host` (`@loams/console-host`) | Boot, the `loams.yml` catalog (no YAML tags, so no `!!js`), manifests (`loams.plugin` in package.json, `plugin-manifest.schema.json`), trust tiers, the guard proxy, permissions, the sandbox bridge; `./testing` has an in-memory app-protos mock |
| `packages/platform-web` (`@loams/platform-web`) | `platform` and `transport` in a browser |
| `packages/proto` (`@loams/proto`) | Generated clients for the app protos (AP0) |
| `packages/admin-ui` (`@loams/admin-ui`) | What the console's own pages share: page heads, the audit table, the activity chart, labels for the `loams.admin.v1` enums, the pages' styles; `./testing` has an in-memory `loams.auth.v1` and `loams.admin.v1` |
| `plugins/*` | `shell` (layout, nav, `router`), `rpc` (`rpc.*` clients, gated on `api_versions`), `identity` (`session`, with the console's sign-in gate), the console's own pages (`account`: sign-in, setup, consent; `org`: overview, teams, members, settings; `projects`: projects, environments, access and the project switcher; `agents`; `audit`, live), `stack-status`, `namespaces`, `approvals`, and `sandbox` (the in-frame runtime) |
| `examples/plugin-hello` | A third-party plugin that runs in a sandboxed frame |
| `apps/console/catalog/base.yml` | The `oss` plugin set |

```bash
cargo run -p loams-apps-mock                 # the console API and the app protos on :8084
pnpm dev                                     # then open http://localhost:5173/ui/ (proxied to the mock)
                                             # http://localhost:5173/ui/?demo is the in-browser demo, no server
VITE_LOAMS_APPS_URL=http://127.0.0.1:8084 VITE_LOAMS_DEV_BEARER=mock-access-usr_omar pnpm dev
                                             # straight at the mock with a bearer token
pnpm test                                    # Vitest, every package
```

Third-party plugins are off unless the instance sets the `console.third_party_plugins` feature (the demo mock does): until the unified auth plan can vend attenuated tokens there is no server-side boundary for them. `window.loamsConsole` in the browser console shows the plugin table and `pending()`.

The console speaks Connect: `loams.auth.v1` and `loams.admin.v1` (API2 Tasks 6, 7) through `@loams/proto` and `@connectrpc/connect-query`, over the console's one transport. Until the server serves them (API2 Task 7's server half), [`loams-apps-mock`](../crates/loams-apps-mock) serves them from the console seed on `:8084`, beside the app protos; the seed itself lives in [`loams-console-mock`](../crates/loams-console-mock). The console's pages appear only where `GetInstance.api_versions` names those packages.

## Develop

The pnpm workspace is rooted at the repository root: its `package.json` pins pnpm `11.27.1`, and `pnpm-workspace.yaml` and `pnpm-lock.yaml` are shared by all workspace packages. Prefer running `pnpm install --frozen-lockfile` from the repository root; `pnpm install` from `web/` also discovers the root workspace and uses that same lockfile. The `web/` package scripts and directory layout are unchanged; run the web commands below from `web/`.

```bash
cargo run -p loams-apps-mock          # the console API mock on :8084 (add --signed-out for the sign-in screens)
cd web && pnpm install && pnpm dev       # the console on http://localhost:5173/ui/
```

`pnpm dev` proxies the Connect paths (`/loams.<package>.<Service>/<Method>`), `/api`, `/v1`, `/.well-known`, `/health` and `/ready` to the mock. To use a real engine instead, set `LOAMS_API=http://127.0.0.1:8080`; `LOAMS_UI_BASE=/` serves the console at the root instead of `/ui/`.

## Check and build

```bash
pnpm lint          # Biome
pnpm typecheck     # tsc, every package
pnpm test          # Vitest, every package
pnpm build         # @loams/ui to packages/ui/dist, the console to apps/console/dist
```

The console builds for the base path `/ui`, bundles its fonts and assets, and loads nothing from the internet, so an air-gapped install works. The engine will embed `apps/console/dist` behind a `console` feature (design §19 §3).

## `@loams/ui` outside this workspace

```ts
import '@loams/ui/styles.css';
import { Button, Card, Logo } from '@loams/ui';
```

Put the class `dark` on `<html>` for dark mode, and set `--loams-font-sans` and `--loams-font-mono` to your loaded Archivo and Martian Mono. Inside the workspace the package resolves to its TypeScript sources; `pnpm --filter @loams/ui publish` publishes the built `dist` (`publishConfig`).

## Fonts

The console bundles Archivo and Martian Mono from Fontsource, both under the SIL Open Font License 1.1 (see `NOTICE`).

## Deploy the console (hosted, Cloudflare, Vercel) — preview

Loams ships as a preview. Ostrium hosts only the console ([console.loams.dev](https://console.loams.dev)) and the auth service; everything else runs on your machine or your own VMs (Docker Compose, `deploy/release/compose.yaml`). The browser talks to **your** engine over your tailnet (Tailscale; Headscale is coming soon): the device running the browser needs the Tailscale client, logged in to the engine's tailnet. The engine is never exposed on the public internet. Deploying the same console on your own Cloudflare or Vercel account is coming soon.

The hosted console is the cordis console (`cordis.html`) served at `/`. On first load it:

1. shows **Connect your engine** on first run (and whenever the API key is needed): the person picks how to sign in and, unless the deploy fixes one, enters the tailnet URL (`https://loams.<tailnet>.ts.net`); **Test connection** calls `GET /health`. The engine URL and the chosen mode are remembered in this browser (neither is a credential);
2. signs in the chosen way (below);
3. boots the console against that engine (no cookies); the header shows who the engine is talking to.

| Mode | Sign-in |
|---|---|
| `tailscale` (default) | No sign-in screen. The engine identifies the caller from the tailnet; the header shows the user `AuthService/GetSession` returns. |
| `oidc` | A Loams account: Authorization Code + PKCE at `https://auth.loams.dev` with the `loams-console` client, sent as `Authorization: Bearer <token>`. Tokens stay in memory; a reload renews silently with `prompt=none`, a refresh token is used while the page lives; nothing that grants access goes to `localStorage` or `sessionStorage`. Offered when the deploy has a client. |
| `apikey` | The connect screen requires an engine API key on every load, kept in the tab's memory only and sent as the bearer. |

`LOAMS_AUTH_MODE` sets the default. Any other value fails the deploy (`config.mjs`), and a `config.json` with an unknown mode, or `oidc` without a client, stops at a screen that says so; the console never falls back to another mode on its own.

**Coming soon** (shown as such on the connect screen, not linked): deploying this console to your own Vercel or Cloudflare account, self-service client registration at auth.loams.dev (`https://auth.loams.dev/register`, URL to confirm), and Headscale networks.

Pages whose backend the engine serves (Live, Graph, Analytics/House, Status, Namespaces, Approvals) are on; pages that still run on built-in sample data (Deployments, Live deployments, SQL, Metrics, Pipelines, Git) appear only with the **Demo data** toggle in the header. Every page shows the **Preview** badge.

Runtime config is written at deploy time from the environment by `apps/console/deploy/config.mjs` (and by the stage step), never baked into the bundle:

| Variable | Meaning |
|---|---|
| `LOAMS_AUTH_MODE` | the default mode: `tailscale` (default), `oidc` or `apikey`; anything else is an error. |
| `LOAMS_AUTH_CLIENT_ID` | the console's public client id at the issuer (`loams-console`; redirect URI: the console's origin + `/`). With it, `oidc` is offered; required for `LOAMS_AUTH_MODE=oidc`. |
| `LOAMS_AUTH_ISSUER` | the issuer, default `https://auth.loams.dev`. Your own IdP is accepted here but not offered in the UI yet (coming soon). |
| `LOAMS_AUTH_SCOPE` | optional, default `openid profile email offline_access`. |
| `LOAMS_ENGINE_URL` | optional: fixes the engine (skips the connect screen and adds its origin to the CSP). |

`config.json` keys: `hosted`, `engineUrl`, `authMode`, `authIssuer`, `authClientId`, `authScope`. The engine-embedded console keeps serving `{}` (same origin, cookie session).

The stage step injects a Content-Security-Policy `<meta>`: no inline scripts in the cordis console, no eval, `connect-src` limited to the console itself, `https://*.ts.net` (port 443, where `tailscale serve` publishes the engine), the issuer and the fixed engine. Headscale networks are coming soon (Headscale issues no HTTPS certificates, so such an engine needs its own certificate and a console deployed with `LOAMS_ENGINE_URL` set to it).

The engine must answer CORS for the console's origin, and the issuer must answer CORS on its discovery and token endpoints (the browser exchanges the code itself).

### Cloudflare

Ostrium deploys console.loams.dev this way. Deploying to your own Cloudflare account (`--env byoc`) is coming soon.

Workers static assets, configured in [`apps/console/wrangler.jsonc`](apps/console/wrangler.jsonc): assets only, SPA fallback, free plan.

```bash
pnpm build
LOAMS_AUTH_CLIENT_ID=loams-console \
  node apps/console/deploy/stage.mjs     # dist/ -> dist-cloudflare/: /, ui/, config.json, _headers, _redirects
cd apps/console && wrangler deploy        # console.loams.dev
cd apps/console && wrangler deploy --env byoc   # your account, on workers.dev (coming soon)
node deploy/smoke.mjs https://console.loams.dev
```

`.github/workflows/console-deploy.yml` deploys on every push to `main` that touches `web/` (or the root workspace configuration, or the workflow), and on demand. It needs `LOAMS_AUTH_CLIENT_ID` (variable, `loams-console`; `LOAMS_AUTH_MODE` and `LOAMS_AUTH_ISSUER` default to `tailscale` and `https://auth.loams.dev`) plus `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` in the `production` environment; without the secrets it builds and stages, then skips the deploy.

### Vercel (coming soon)

Deploying the console to your own Vercel account is coming soon: the configuration below is kept but not yet supported or advertised in the console. [`apps/console/vercel.json`](apps/console/vercel.json): set the project's Root Directory to `web/apps/console` and the environment variables above. It installs from the workspace, runs `pnpm build` and `stage.mjs --target vercel` into `dist-vercel/`, rewrites unknown paths to the console and sends the security headers. Vercel headers are static, so its CSP names `https://*.ts.net` and `https://auth.loams.dev`; the stage step fails the build when the configured issuer or engine is missing from it — edit `connect-src` in `vercel.json` for another issuer.

`node --test apps/console/deploy/*.test.mjs` tests the config, stage and smoke scripts, `wrangler.jsonc` and `vercel.json`. Sandbox frames receive their own response CSP with `sandbox allow-scripts`; HTML paths stay exact so their security headers apply without canonical redirects.
