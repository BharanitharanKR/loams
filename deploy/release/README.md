# Loams preview: run it yourself

Everything in the preview runs on your machine or on VMs you own. Ostrium hosts
two things only: the console (a React app) and the auth service. You can serve
the console yourself instead, from your own Vercel or Cloudflare project, or
from this stack (`--profile console`).

```
 browser ── console (hosted, or yours) ── auth.loams.dev (OIDC, PKCE)
    │
    └─ your device's Tailscale client ══ tailnet ══ tailscale sidecar ─┐
                                                         (loams netns) │
                                       loams engine :8080 ◄────────────┘
                                       house (loams-fabric) :8123
                                       pd · tikv · tidb · Postgres stack
```

The engine is never on the public internet: the only way in is the Tailscale
sidecar, which joins your tailnet (Tailscale, or Headscale through
`TS_LOGIN_SERVER`) and proxies `https://loams.<tailnet>.ts.net` to the engine.
No service binds a host port on `0.0.0.0` by default.

## Images

Built by [`build.sh`](build.sh) from `Containerfile.<name>` and published to
`ghcr.io/ostrium-labs` as `:<version>` and `:preview`, for linux/amd64 and
linux/arm64. Each runs as uid 10001 on debian-slim, with tini as PID 1 and a
healthcheck.

| Image | Contains |
|---|---|
| `loams` | the engine, features `live,live-tikv,durable,graph,tikv,pgwire,mysql-wire,stream-grpc` |
| `loams-fabric` | `loams-fabric house` and `loams-house-worker`, with the digest-pinned libchdb |
| `loams-agentd` | the agent daemon (optional) |

```sh
deploy/release/build.sh loams                        # both platforms, local manifest list
deploy/release/build.sh --platforms linux/amd64 loams
deploy/release/build.sh --version 0.1.0-preview.1 --push   # CI on a tag
```

## On this machine

```sh
cd deploy/release
cp .env.example .env            # set TS_AUTHKEY; see the comments
docker compose up -d loams tailscale     # engine, pd, tikv and the sidecar
docker compose up -d                     # plus house, tidb and the Postgres stack
docker compose logs tailscale            # the node name, or a login URL
```

Set the console's `LOAMS_ENGINE_URL` to `https://loams.<tailnet>.ts.net`.
Profiles add `headscale`, `console` (the console from `LOAMS_CONSOLE_DIR`, with
`/config.json` built from `LOAMS_ENGINE_URL`, `LOAMS_AUTH_ISSUER` and
`LOAMS_AUTH_CLIENT_ID`) and `agentd`. Stop with `docker compose --profile '*'
down`; add `--volumes` to delete the data.

### Auth

The engine reads `LOAMS_AUTH_MODE` (`oidc`, `apikey`, or `off`, which serves
loopback only), `LOAMS_OIDC_ISSUER` and `LOAMS_OIDC_AUDIENCE` (JWTs checked
against the issuer's JWKS) and `LOAMS_API_KEYS_FILE`, which compose points at
`secrets/api-keys`. The default is OIDC against `https://auth.loams.dev` plus
API keys for the CLI and SDKs.

### Headscale

```sh
# .env: HEADSCALE_SERVER_URL=https://hs.example.com, TS_LOGIN_SERVER=the same,
#       TS_SERVE_MODE=http, HEADSCALE_BIND=0.0.0.0 (behind TLS) to let devices join
docker compose --profile headscale up -d headscale
docker compose exec headscale headscale users create loams
docker compose exec headscale headscale users list            # its id
docker compose exec headscale headscale preauthkeys create --user <id> --reusable
# put the key in TS_AUTHKEY, then
docker compose up -d loams tailscale
```

Headscale issues no HTTPS certificates, so the engine is served over plain
HTTP on the tailnet (`TS_SERVE_MODE=http`, inside WireGuard). A console served
over HTTPS cannot call it (mixed content); serve the console over HTTP from the
tailnet or this machine.

## On your own VM (BYOC)

```sh
deploy/release/byoc/install.sh ubuntu@203.0.113.10            # asks for TS_AUTHKEY
deploy/release/byoc/install.sh --headscale https://hs.example.com ubuntu@vm
deploy/release/byoc/uninstall.sh ubuntu@203.0.113.10          # --purge deletes the data
```

`install.sh` installs Docker if it is missing (get.docker.com), copies this
directory and `deploy/loams-postgres-dev` to `~/loams`, writes `.env` with mode
0600 and starts `loams house tailscale tidb` (`--all` for everything). The VM
needs SSH and outbound internet only.

## Known gaps in the preview

- House workers run without the netns sandbox (`--sandbox none`): it needs
  `CAP_SYS_ADMIN` in a container. The container is their boundary.
- Live (7710), durable (8001), and the MySQL and Postgres wire listeners of the
  engine are loopback only and are not forwarded over the tailnet.
- TiDB starts with a passwordless root and the Postgres stack with default
  credentials; neither is forwarded over the tailnet by default.
- `compute1` needs `TENANT_ID` and `TIMELINE_ID` (see
  [`../loams-postgres-dev/README.md`](../loams-postgres-dev/README.md)).
