# A remote agentd for Studio in the browser

Loams Studio in the browser console works against a `loams-agentd` you run on a machine you control: a dev box, a VM, a container or a workspace sandbox (design [§53](../../design/53-studio-ade.md) §6.3, D909). One remote agentd serves one person (D921). The browser reaches it over TLS with your own short-lived access token, and the daemon's own token never leaves the machine.

> **Status.** The remote listener, its checks and previews through it are on `dev` (SD1 Tasks 7 and 26). Three things are **pending**:
> - the browser console's Content-Security-Policy for your host ([below](#the-consoles-content-security-policy), SD1 Task 30);
> - a sign-in that issues the agentd token, so for now you paste one (MT1);
> - Studio in the browser without the `studio` instance feature (SD1 Task 32; see [Getting started](getting-started.md#in-a-browser)).

## What `serve --remote` opens

```sh
loams-agentd serve --remote \
  --listen 0.0.0.0:8443 \
  --tls-cert /etc/loams-agentd-tls/cert.pem --tls-key /etc/loams-agentd-tls/key.pem \
  --issuer https://auth.example.com/application/o/loams-agentd/ \
  --audience loams-agentd \
  --subject <your sub> \
  --allow-origin https://console.example.com \
  --config /var/lib/loams-agentd/config.toml
```

`serve` runs the daemon as `run` does (the ndjson listener, the loopback Connect listener and everything else) and adds the remote listener.

**Flags and config:**
- **Every flag is required**, except `--allow-origin`, which you repeat once per console origin. A missing flag exits 4 and names it (`loams-agentd: serve --remote needs --tls-cert`). So does a certificate, key, issuer or origin that can't be used. Nothing has a default that opens the daemon up.
- **The same settings can live in `config.toml`.** Add a `[remote]` table with `enabled = true` and the keys `listen`, `tls_cert`, `tls_key`, `issuer`, `audience`, `subject` and `allow_origins`. Then plain `loams-agentd run` opens the listener too. Without `enabled = true` the listener stays off, and with it every key but `allow_origins` is required. On Windows, use `config.toml`: the service re-runs its worker without the flags.
- **`--config` names the daemon's directory.** The daemon writes its lock, token, discovery file, store and logs beside `config.toml`, so that directory must be writable. `LOAMS_AGENTD_CONFIG` overrides `--config`.

**What the listener serves:**
- **Only `loams.agentd.v1`**, Connect over HTTP/1.1 and HTTP/2 (ALPN `h2` and `http/1.1`, TLS 1.2 and 1.3), plus the [preview paths](#previews). The ndjson wire, `Hello` and `Shutdown` stay on the loopback listener, which still refuses browsers (D788). `agentd.json` doesn't publish the remote port.
- **Each request is checked in this order** before any handler runs:
  1. **Origin.** The request has no `Origin` header (curl, a CLI), or its origin is exactly one of yours. Any other origin gets a 403 `origin_not_allowed` with no CORS headers.
  2. **Preflight.** An `OPTIONS` preflight from one of your origins is answered here, before authentication.
  3. **Token.** `Authorization: Bearer <access token>`, a JWT signed by the issuer with RS256 or EdDSA (HMAC and `none` are refused). The daemon finds the issuer's keys through discovery, caches them, and refetches at most once a minute for an unknown key. It checks `iss`, `exp` and `nbf` with 60 s of leeway, and requires `aud` to contain the audience. A token that doesn't verify gets a 401 `unauthenticated`. If no signing keys can be fetched and none are cached, the answer is a 503.
  4. **Subject.** `sub` must equal `--subject`. A valid token for anyone else gets a 403 `forbidden_role`.
  5. **Role.** Your browser session runs in the role `studio`. It can call every Studio method except the harness installers and updaters (`InstallHarness`, `CancelInstall`, `ApplyHarnessUpdate`, `CancelHarnessUpdate`) and the agent account logins and switches (`StartAgentLogin` and its siblings, `ActivateAgentAccount`, `ForgetAgentAccount`), Q808. Do those on the machine itself.
- **CORS answers only your origins.** It allows the Connect headers and `Authorization`, exposes `grpc-status`, `grpc-message` and `grpc-status-details-bin`, caches preflights for 7200 s, and never sends `Access-Control-Allow-Credentials`. The console sends a bearer token, never a cookie. An origin must be `https://host[:port]` (`http://` only on loopback), never `*`, `null` or a path.

A quick check from another shell (with a self-signed certificate, hence `-k`):

```sh
# A preflight from your console's origin: 204 and the CORS headers.
curl -sk -D - -o /dev/null -X OPTIONS \
  -H 'Origin: https://console.example.com' -H 'Access-Control-Request-Method: POST' \
  https://devbox.example.com:8443/loams.agentd.v1.StudioService/GetHost
# Another origin: 403, no CORS headers. No token: 401 unauthenticated.
curl -sk -X POST -H 'Content-Type: application/json' -d '{}' \
  https://devbox.example.com:8443/loams.agentd.v1.StudioService/GetHost
```

These answers were checked against a debug build on 2026-10-15, with a stand-in issuer; a valid-token call needs a real issuer and was not run.

## Reaching it: a tailnet, or a port

The default (Q804) is one of these:
- **A tailnet** (§43). Join the machine to your tailnet and listen on its tailnet address, or on `0.0.0.0` with the host firewall closed to everything but the tailnet. The tailnet's HTTPS certificates (for example from `tailscale cert`) work as `--tls-cert` and `--tls-key`.
- **A direct TLS port.** Use a certificate from a public CA for the machine's name. Open only that port.

An outbound tunnel through the Loams gateway is an open question (Q804); SD1 doesn't build one.

A self-signed certificate works, but the browser has to trust it first. Open the host's URL once and accept it, or install your own CA. When you register the host in Studio, note the certificate's SHA-256 fingerprint so you can compare it with what the browser shows:

```sh
openssl x509 -in cert.pem -noout -fingerprint -sha256
```

## A container recipe

The release builds an image, `ghcr.io/ostrium-labs/loams-agentd`, from [`deploy/release/Containerfile.loams-agentd`](../../../deploy/release/Containerfile.loams-agentd) (`deploy/release/build.sh loams-agentd` builds it locally). Its entry point is `loams-agentd`, it runs as uid 10001, and it sets `LOAMS_AGENTD_CONFIG=/etc/loams-agentd/config.toml`, which overrides `--config`. **It holds no harness CLIs:** build your own image from it with the agents you use (Claude Code, Codex, opencode…) installed.

Run it with only your project directories mounted (§53 §8.2). Worktrees keep agents' edits apart, but they don't contain processes: an agent with a shell can read anything the daemon's user can.

```sh
# The daemon writes its token, lock and store beside config.toml, so its directory is a
# writable volume; the TLS files are mounted read-only somewhere else.
podman run -d --name loams-agentd \
  -v loams-agentd-etc:/etc/loams-agentd \
  -v loams-agentd-home:/home/loams \
  -v ~/src:/home/loams/src \
  -v ./tls:/etc/loams-agentd-tls:ro \
  -p 8443:8443 \
  <your image, FROM ghcr.io/ostrium-labs/loams-agentd, with your harness CLIs> \
  serve --remote --listen 0.0.0.0:8443 \
    --tls-cert /etc/loams-agentd-tls/cert.pem --tls-key /etc/loams-agentd-tls/key.pem \
    --issuer https://auth.example.com/application/o/loams-agentd/ \
    --audience loams-agentd --subject <your sub> \
    --allow-origin https://console.example.com \
    --config /etc/loams-agentd/config.toml
```

**Unverified:** this recipe has not been run end to end against the published image. `deploy/release/compose.yaml`'s optional `agentd` service (`--profile agentd`) runs the same image with `run`, not `serve --remote`.

## The Authentik application

- Create an OAuth2/OpenID provider and application for the agentd, for example with the slug `loams-agentd`. Its tokens must carry `aud: loams-agentd`; Authentik puts the provider's client id in `aud`, so name the client `loams-agentd`.
- The issuer is then `https://auth.example.com/application/o/loams-agentd/`. The daemon reads `/.well-known/openid-configuration` under it, which names the JWKS at `/application/o/loams-agentd/jwks/`.
- Your subject is the `sub` Authentik puts in your tokens. With the default subject mode, that is a hashed user id; read it from one of your tokens' claims.
- Keep the signing key RS256 (Authentik's default) or EdDSA.

**Unverified:** these Authentik steps were not tried against a live Authentik. The daemon's tests use a stand-in issuer (discovery and a JWKS on loopback).

**How the console gets the token.** It will get an access token for the audience `loams-agentd` through its own sign-in (MT1): an RFC 8693 token exchange, or a second client. Until MT1 lands, **Settings › Agentd hosts** asks you to paste a token per host, under a "Development sign-in" warning. The token stays in that tab's memory and is gone when you reload.

## In Studio

1. Open Studio, then **Settings** (Mod+,). In the browser the page has an **Agentd hosts** card. Add the host's name and its `https://` URL (`http://` only for `localhost`), and optionally the certificate fingerprint as a note. The list is kept in this browser, under `loams.studio.hosts`; it doesn't follow you to another browser.
2. Paste an access token for the host.
3. Studio uses the first host you add; **Use** switches hosts. A link can name a host as `?host=<id>`, by its registered id only; a URL in `?host=` is ignored.

Without a host, or without a token for the selected host, Studio's home shows **Connect an agentd host**.

## The console's Content-Security-Policy

**Pending (SD1 Task 30, ruling T26-9).** The browser console only fetches from and frames the origins its Content-Security-Policy names, and today none of the policies on `dev` names your agentd host:

| Console | `connect-src` (Connect calls) | `frame-src` (previews) |
|---|---|---|
| The production build (`vite.config.ts`'s `<meta>`), as the engine serves it | `'self'` | `'self'` |
| The hosted console (`deploy/stage.mjs`, Cloudflare) | `'self'`, `https://*.ts.net`, the issuer and a fixed engine (`LOAMS_ENGINE_URL`) | `'self'` |
| Vercel (`vercel.json`) | `'self'`, `https://*.ts.net`, `https://auth.loams.dev` | `'self'` |
| The dev server (`pnpm dev`) | no policy | no policy |

So, on `dev`:
- **The dev server works** with any host.
- **The hosted and Vercel consoles reach an agentd on a `*.ts.net` name on port 443 only.** A source without a port matches only the scheme's default port, so `https://devbox.<tailnet>.ts.net:8443` is blocked. Listen on 443 (`--listen <tailnet address>:443`) to use them. Previews stay blocked by `frame-src 'self'`.
- **The engine-served console reaches no remote agentd.**

The fix (ruling T26-9) is that a deployment serves a policy naming the configured agentd origins in both `connect-src` and `frame-src`, as §39 does for the factory apps. Task 30 is making that change; this section will name its setting once it is on `dev`. The desktop is not affected: it reaches its local daemon through Electron main, and its preview is a native view.

## Previews

Dev-server previews work through the remote listener (Q806, SD1 Task 26). The daemon's own preview proxy listens on loopback only (`http://<name>.<project>.localhost:7331`), so the remote listener forwards `/preview/…` to it:

- **The console asks for a ticket.** It `POST`s `/preview/-/ticket` with its bearer token, from one of your origins, with the body `{"preview": "<name>.<project>"}`. The answer is a path `/preview/pt_<64 hex>/`: a random capability for that one preview, held in memory (at most 64), lasting until the token expires and at most 8 hours. The console frames that path. A frame cannot send an `Authorization` header, and the listener sets no cookies.
- **A CLI or curl** can call `/preview/<name>.<project>/…` with the bearer token instead.
- **Every preview answer** carries `Content-Security-Policy: frame-ancestors <your --allow-origin list>` (`'none'` without one), refusals included, and `Referrer-Policy: same-origin`, so a ticket never leaves in a `Referer`. The token is never forwarded to the dev server, and the daemon never logs the path.
- **The prefix is stripped** (`/preview/<x>/src/main.tsx` reaches the dev server as `/src/main.tsx`, with `X-Forwarded-Prefix`), and a `Location` header is moved back under it. An app that uses root-relative URLs (`/assets/…`) needs its dev server's base path set to the prefix, as behind any path proxy. HMR WebSockets pass through.
- **Cookies a dev server sets** land on the agentd's origin, where every preview of the same daemon can see them. That is acceptable for one person's daemon (D921), and a reason not to share one.

If the daemon answers the ticket request with 404, 501 or 503 (an older daemon, or previews off), the Preview tab shows how to forward the preview proxy's port instead: `ssh -N -L 7331:localhost:7331 <host>`. See [Preview](preview.md).

## Approvals

A remote daemon can be reached over the network, so with the remote listener on, a thread that is given no mode is **supervised**: every tool call waits for you (`[approvals] default_mode` overrides this). Your browser session can answer approvals and set a thread's mode, since the `studio` role includes the approval methods. An approval nobody answers is denied after `[approvals] timeout_secs` (1800 s). See [Approvals](approvals.md).
