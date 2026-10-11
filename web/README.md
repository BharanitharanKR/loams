![Loams — Your data. Your bucket.](../docs/assets/loams-banner.svg)

# web

The Loams console and its design system (design [§19](../docs/design/19-console-identity-and-agents.md)).

| Package | What |
|---|---|
| `packages/ui` (`@loams/ui`) | Tokens, the mark and wordmark, grain textures and React primitives: buttons, status tags, cards, empty states, fields, tables, stats, notices, dialogs, snippets, meters, avatars, formatters. Plain CSS with `loams-` classes; works in Vite and Next.js |
| `apps/console` (`@loams/console`) | One single-page app for OSS, Loams Cloud and BYOC. It reads `GET /api/v1/instance` to know its edition and sign-in methods |

### The cordis console (design §37 §5, AP1a)

The console is becoming a [cordis](https://github.com/cordiverse/cordis) v4 application: a small host boots a cordis context from a plugin catalog, and everything else is a plugin with a manifest, declared services, slots and permissions. Until today's pages move into plugins (AP1a Task 5) it is served beside the current console at `/ui/cordis.html`.

| Package | What |
|---|---|
| `packages/cordis` (`@loams/cordis`) | The only import of cordis (pinned `4.0.0-rc.10`) and its patch log |
| `packages/slots` (`@loams/slots`) | Typed `single`, `list` and `keyed` slots, the React renderer, per-entry error boundaries |
| `packages/console-host` (`@loams/console-host`) | Boot, the `loams.yml` catalog (no YAML tags, so no `!!js`), manifests (`loams.plugin` in package.json, `plugin-manifest.schema.json`), trust tiers, the guard proxy, permissions, the sandbox bridge; `./testing` has an in-memory app-protos mock |
| `packages/platform-web` (`@loams/platform-web`) | `platform` and `transport` in a browser |
| `packages/proto` (`@loams/proto`) | Generated clients for the app protos (AP0) |
| `plugins/*` | `shell` (layout, nav, `router`), `rpc` (`rpc.*` clients, gated on `api_versions`), `identity` (`session`), `stack-status`, `namespaces`, `approvals`, and `sandbox` (the in-frame runtime) |
| `examples/plugin-hello` | A third-party plugin that runs in a sandboxed frame |
| `apps/console/catalog/base.yml` | The `oss` plugin set |

```bash
pnpm dev                                     # then open http://localhost:5173/ui/cordis.html (demo mode, no server)
cargo run -p loams-apps-mock                 # or against the app-protos mock on :8084:
VITE_LOAMS_APPS_URL=http://127.0.0.1:8084 VITE_LOAMS_DEV_BEARER=mock-access-usr_omar pnpm dev
pnpm test                                    # Vitest, every package
```

Third-party plugins are off unless the instance sets the `console.third_party_plugins` feature (the demo mock does): until the unified auth plan can vend attenuated tokens there is no server-side boundary for them. `window.loamsConsole` in the browser console shows the plugin table and `pending()`.

The console speaks the contract in [`api/console/openapi.json`](../api/console/openapi.json). Until the gateway implements it (M2), [`loams-apps-mock`](../crates/loams-apps-mock) serves it with seed data on `:8084`, beside the app protos. The contract and seed themselves live in [`loams-console-mock`](../crates/loams-console-mock), which is a library now: `loams-apps-mock` mounts its routes over axum, so one listener answers both surfaces.

## Develop

The pnpm workspace is rooted at the repository root: its `package.json` pins pnpm `11.27.1`, and `pnpm-workspace.yaml` and `pnpm-lock.yaml` are shared by all workspace packages. Prefer running `pnpm install --frozen-lockfile` from the repository root; `pnpm install` from `web/` also discovers the root workspace and uses that same lockfile. The `web/` package scripts and directory layout are unchanged; run the web commands below from `web/`.

```bash
cargo run -p loams-apps-mock          # the console API mock on :8084 (add --signed-out for the sign-in screens)
cd web && pnpm install && pnpm dev       # the console on http://localhost:5173/ui/
```

`pnpm dev` proxies `/api`, `/v1`, `/.well-known`, `/health` and `/ready` to the mock. To use a real engine instead, set `LOAMS_API=http://127.0.0.1:8080`.

After changing the contract, regenerate the console's types:

```bash
pnpm gen:api
```

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

Loams ships as a preview. Ostrium hosts only the console ([console.loams.dev](https://console.loams.dev)) and the auth service; everything else runs on your machine or your own VMs (Docker Compose, `deploy/release/compose.yaml`). The browser talks to **your** engine over your tailnet (Tailscale, or a self-hosted Headscale): the device running the browser needs the Tailscale client, logged in to the engine's tailnet. The engine is never exposed on the public internet. You can also deploy the same console on your own Cloudflare or Vercel account.

The hosted console is the cordis console (`cordis.html`) served at `/`. On first load it:

1. signs in the way the deploy's `LOAMS_AUTH_MODE` says (below);
2. shows **Connect your engine** unless the deploy fixes one: enter the tailnet URL (`https://loams.<tailnet>.ts.net`), **Test connection** calls `GET /health`, and the URL is remembered in this browser;
3. boots the console against that engine (no cookies); the header shows who the engine is talking to.

| `LOAMS_AUTH_MODE` | Sign-in |
|---|---|
| `oidc` (default) | Authorization Code + PKCE against `LOAMS_AUTH_ISSUER` (default `https://auth.loams.dev`, or your own IdP), sent as `Authorization: Bearer <token>`. Tokens stay in memory; a reload renews silently with `prompt=none`, a refresh token is used while the page lives; nothing that grants access goes to `localStorage` or `sessionStorage`. |
| `tailscale` | No sign-in screen. The engine identifies the caller from the tailnet; the header shows the user `GET /api/v1/session` returns. |
| `apikey` | The connect screen requires an engine API key on every load, kept in the tab's memory only and sent as the bearer. |

Any other value fails the deploy (`config.mjs`), and a `config.json` with an unknown mode, or `oidc` without a client, stops at a screen that explains the three modes; the console never falls back to another mode on its own.

A console you deploy yourself (Cloudflare, Vercel) in `oidc` mode needs its own client id at auth.loams.dev: register it at <https://auth.loams.dev/register> (to confirm) with the console's exact origin + `/` as the redirect URI.

Pages whose backend the engine serves (Live, Graph, Analytics/House, Status, Namespaces, Approvals) are on; pages that still run on built-in sample data (Deployments, Live deployments, SQL, Metrics, Pipelines, Git) appear only with the **Demo data** toggle in the header. Every page shows the **Preview** badge.

Runtime config is written at deploy time from the environment by `apps/console/deploy/config.mjs` (and by the stage step), never baked into the bundle:

| Variable | Meaning |
|---|---|
| `LOAMS_AUTH_MODE` | `oidc` (default), `tailscale` or `apikey`; anything else is an error. |
| `LOAMS_AUTH_ISSUER` | `oidc`: the issuer, default `https://auth.loams.dev`. |
| `LOAMS_AUTH_CLIENT_ID` | `oidc`, required: the console's public client id at the issuer (redirect URI: the console's origin + `/`). |
| `LOAMS_AUTH_SCOPE` | optional, default `openid profile email offline_access`. |
| `LOAMS_ENGINE_URL` | optional: fixes the engine (skips the connect screen and adds its origin to the CSP). |

`config.json` keys: `hosted`, `engineUrl`, `authMode`, `authIssuer`, `authClientId`, `authScope`. The engine-embedded console keeps serving `{}` (same origin, cookie session).

The stage step injects a Content-Security-Policy `<meta>`: no inline scripts in the cordis console, no eval, `connect-src` limited to the console itself, `https://*.ts.net` (port 443, where `tailscale serve` publishes the engine), the issuer and the fixed engine. Headscale issues no HTTPS certificates, so a Headscale engine needs its own certificate and a console deployed with `LOAMS_ENGINE_URL` set to it.

The engine must answer CORS for the console's origin, and the issuer must answer CORS on its discovery and token endpoints (the browser exchanges the code itself).

### Cloudflare

Workers static assets, configured in [`apps/console/wrangler.jsonc`](apps/console/wrangler.jsonc): assets only, SPA fallback, free plan.

```bash
pnpm build
LOAMS_AUTH_MODE=oidc LOAMS_AUTH_CLIENT_ID=loams-console \
  node apps/console/deploy/stage.mjs     # dist/ -> dist-cloudflare/: /, ui/, config.json, _headers, _redirects
cd apps/console && wrangler deploy        # console.loams.dev
cd apps/console && wrangler deploy --env byoc   # your account, on workers.dev
node deploy/smoke.mjs https://console.loams.dev
```

`.github/workflows/console-deploy.yml` deploys on every push to `main` that touches `web/` (or the root workspace configuration, or the workflow), and on demand. It deploys in `oidc` mode only and needs `LOAMS_AUTH_CLIENT_ID` (variable; `LOAMS_AUTH_MODE` and `LOAMS_AUTH_ISSUER` default to `oidc` and `https://auth.loams.dev`) plus `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` in the `production` environment; without the secrets it builds and stages, then skips the deploy.

### Vercel

[`apps/console/vercel.json`](apps/console/vercel.json): set the project's Root Directory to `web/apps/console` and the environment variables above. It installs from the workspace, runs `pnpm build` and `stage.mjs --target vercel` into `dist-vercel/`, rewrites unknown paths to the console and sends the security headers. Vercel headers are static, so its CSP names `https://*.ts.net` and `https://auth.loams.dev`; the stage step fails the build when the configured issuer or engine is missing from it — edit `connect-src` in `vercel.json` for another issuer.

`node --test apps/console/deploy/*.test.mjs` tests the config, stage and smoke scripts, `wrangler.jsonc` and `vercel.json`. Sandbox frames receive their own response CSP with `sandbox allow-scripts`; HTML paths stay exact so their security headers apply without canonical redirects.
