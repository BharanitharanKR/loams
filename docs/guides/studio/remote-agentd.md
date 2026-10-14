# A remote agentd for Studio in the browser (draft)

> **Draft** (SD1 Task 7). Task 31 finishes this guide. Studio in the browser is still behind `flags.features.studio` until SD1 Task 32.

Loams Studio in the browser console works against a `loams-agentd` you run on a machine you control: a dev box, a VM, a container or a workspace sandbox (design §53 §6.3, D909). One remote agentd serves one person (D921). The browser reaches it over TLS with your own short-lived access token. The daemon's own token never leaves the machine.

## What `serve --remote` opens

```sh
loams-agentd serve --remote \
  --listen 0.0.0.0:8443 \
  --tls-cert /etc/loams-agentd/cert.pem --tls-key /etc/loams-agentd/key.pem \
  --issuer https://auth.example.com/application/o/loams-agentd/ \
  --audience loams-agentd \
  --subject <your sub> \
  --allow-origin https://console.example.com \
  --config /var/lib/loams-agentd/config.toml
```

- **Every flag is required**, except `--allow-origin`, which you repeat once per console origin. A missing flag exits 4 and names it, and so does a certificate or key that can't be used. Nothing has a default that opens the daemon up.
- **The same settings can live in `config.toml`.** Add a `[remote]` table with `enabled = true` and the keys `listen`, `tls_cert`, `tls_key`, `issuer`, `audience`, `subject` and `allow_origins`. Without `enabled = true` the listener stays off. When both are given, the flags win.
- **The listener serves only `loams.agentd.v1`**, Connect over HTTP/1.1 and HTTP/2. The ndjson wire, `Hello` and `Shutdown` stay on the loopback listener, which still refuses browsers (D788).
- **Each request is checked before any handler runs:**
  - Origin: the request has no `Origin` header, or its origin is one of yours. Any other origin gets a 403 and no CORS headers.
  - Token: `Authorization: Bearer <access token>`, a JWT signed by the issuer. The daemon reads the issuer's keys through discovery and refetches them at most once a minute for an unknown key. It checks `iss`, `exp` and `nbf` with 60 s of leeway, and requires `aud` to contain `loams-agentd`.
  - Subject: `sub` must equal `--subject`. A valid token for anyone else gets a 403.
- **Your browser session runs in the role `studio`.** It can call every Studio method except the harness installers and updaters and the agent account logins (Q808). Do those from the machine itself.
- **CORS answers only your origins.** It allows only the Connect headers and `Authorization`, and never sends `Access-Control-Allow-Credentials`. The console sends a bearer token, never a cookie.

## Reaching it: a tailnet, or a port

The default (Q804) is one of these:
- **A tailnet** (§43). Join the machine to your tailnet and listen on its tailnet address, or on `0.0.0.0` with the host firewall closed to everything but the tailnet. The tailnet's HTTPS certificates (for example `tailscale cert`) work as `--tls-cert` and `--tls-key`.
- **A direct TLS port.** Use a certificate from a public CA for the machine's name. Open only that port.

An outbound tunnel through the Loams gateway is an open question (Q804); SD1 doesn't build one.

A self-signed certificate works, but the browser has to trust it first. Open the host's URL once and accept it, or install your own CA. When you register the host in Studio, note the certificate's SHA-256 fingerprint so you can compare it with what the browser shows:

```sh
openssl x509 -in cert.pem -noout -fingerprint -sha256
```

## A container recipe

Run the daemon with only your project directories mounted (§53 §8.2). Worktrees keep agents' edits apart, but they don't contain processes: an agent with a shell can read anything the daemon's user can.

```sh
podman run -d --name loams-agentd \
  -v ~/src:/home/dev/src \
  -v loams-agentd-data:/var/lib/loams-agentd \
  -v ./certs:/etc/loams-agentd:ro \
  -p 8443:8443 \
  <an image with loams-agentd and your harness CLIs> \
  loams-agentd serve --remote --listen 0.0.0.0:8443 \
    --tls-cert /etc/loams-agentd/cert.pem --tls-key /etc/loams-agentd/key.pem \
    --issuer https://auth.example.com/application/o/loams-agentd/ \
    --audience loams-agentd --subject <your sub> \
    --allow-origin https://console.example.com \
    --config /var/lib/loams-agentd/config.toml
```

The image isn't published yet. Task 31 adds one, or a recipe that builds it.

## The Authentik application

- Create an OAuth2/OpenID provider and application for the agentd, for example with the slug `loams-agentd`. Its tokens then carry `aud: loams-agentd` (Authentik puts the client id in `aud`).
- The issuer is `https://auth.example.com/application/o/loams-agentd/`. The daemon finds its keys through `/.well-known/openid-configuration`, which names `/application/o/loams-agentd/jwks/`.
- Your subject is the `sub` Authentik puts in your tokens. With the default subject mode, that is a hashed user id; read it from a token's claims.
- Keep the signing key RS256 (Authentik's default) or EdDSA. The daemon refuses HMAC and `none`.

**How the console gets the token.** It will get an access token for the audience `loams-agentd` through its own sign-in (MT1): an RFC 8693 token exchange, or a second client. Until MT1 lands, Settings › Agentd hosts asks you to paste a token, under a "Development sign-in" warning. The pasted token stays in that tab's memory and is never stored.

## In Studio

1. Open **Studio › Settings › Agentd hosts** and add the host's name and `https://` URL (`http://` only for localhost). Optionally add the certificate fingerprint note. The list is kept in this browser under `loams.studio.hosts`.
2. Studio uses the first host you add. **Use** switches hosts. A link can name a host as `?host=<id>`, by its id only; a URL in `?host=` is ignored.
3. Without a token for the selected host, Studio shows "Connect an agentd host".

## Approvals

A remote daemon can be reached over the network, so threads started from the browser default to **supervised** approvals: every tool call waits for you (§53 §8.1, D911). SD1 Task 13 lands supervised mode; until then the harnesses auto-approve, as on the desktop.

## Previews

Previews through the remote listener (`/preview/…`, behind the same token; Q806) come with SD1 Task 26.
