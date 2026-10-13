# Loams preview: run it yourself

Everything in the preview runs on your machine or on VMs you own. Ostrium hosts
two things only: the console (a React app) and the auth service. You can serve
the console yourself instead, from your own Vercel or Cloudflare project, or
from this stack (`--profile console`).

```
 browser ── console (hosted, or yours) ── auth.loams.dev (OIDC, PKCE)
    │
    └─ your device's Tailscale client ══ tailnet ══ tailscale sidecar ─┐
                                    (loams netns: engine + sidecar only)│
                         loams engine, tailscale listener :7781 ◄───────┘
    ─────────────── compose network ───────────────
      pd · tikv · tidb · house · agentd · Postgres stack
```

The engine is never on the public internet: the only way in is the Tailscale
sidecar, which joins your own Tailscale tailnet and proxies `https://loams.<tailnet>.ts.net` to the engine's
tailscale listener (`LOAMS_TAILSCALE_LISTEN`, `127.0.0.1:7781`). Only the
engine and the sidecar share that network namespace; every other service is on
the compose network and cannot reach the engine's loopback listeners.
No service binds a host port on `0.0.0.0` by default.

## Images

Built by [`build.sh`](build.sh) from `Containerfile.<name>` and published to
`ghcr.io/ostrium-labs` as `:<version>` and `:preview`, for linux/amd64 and
linux/arm64. Each runs as uid 10001 on debian-slim, with tini as PID 1 and a
healthcheck.

| Image | Contains |
|---|---|
| `loams` | the engine, features `live,live-tikv,durable,graph,tikv,pgwire,mysql-wire,stream-grpc` |
| `loams-fabric` | `loams-fabric house` and `loams-house-worker`, with the digest-pinned libchdb; workers sealed with `--sandbox pods` by default |
| `loams-agentd` | the agent daemon (optional) |

```sh
deploy/release/build.sh loams                        # both platforms, local manifest list
deploy/release/build.sh --platforms linux/amd64 loams
deploy/release/build.sh --version 0.1.0-preview.2 --push   # CI on a tag
```

## On this machine

```sh
cd deploy/release
cp .env.example .env            # set TS_AUTHKEY; see the comments
docker compose up -d loams tailscale     # engine, pd, tikv and the sidecar
docker compose up -d                     # plus tidb and the Postgres stack
docker compose logs tailscale            # the node name, or a login URL
```

Set the console's `LOAMS_ENGINE_URL` to `https://loams.<tailnet>.ts.net`.
Profiles add `headscale` (coming soon, unsupported in 0.1.0-preview.2), `console` (the console from `LOAMS_CONSOLE_DIR`, with
`/config.json` built from `LOAMS_ENGINE_URL`, `LOAMS_AUTH_ISSUER` and
`LOAMS_AUTH_CLIENT_ID`), `agentd` and `house` (below). Stop with `docker compose --profile '*'
down`; add `--volumes` to delete the data.

### House

```sh
docker compose --profile house up -d house house-tailscale
docker compose exec house cat /var/lib/loams-house/house.key   # user `default`'s password
curl -u "default:$PASSWORD" https://loams-house.<tailnet>.ts.net/ --data-binary 'SELECT 1'
```

The House listens on its loopback only; `house-tailscale`, in its network
namespace, serves it on the tailnet as `HOUSE_TS_HOSTNAME` (`loams-house`), with
`HOUSE_TS_AUTHKEY` (or a reusable `TS_AUTHKEY`). It is on the `house` network,
which nothing else joins. Workers run sealed: `HOUSE_SANDBOX=pods` (default;
Landlock with TCP rules on Linux 6.7 or later, and seccomp) or `netns` (adds a
user and network namespace per worker; needs unprivileged user namespaces in the
container, as Podman allows). chDB's access control (R-ACL.1) and the statement
checks are always on.

### Auth

The engine reads `LOAMS_AUTH_MODE` (`tailscale`, `oidc`, `apikey`, or `off`,
which serves loopback only). The default is `tailscale`: the engine trusts the
`Tailscale-User-Login` headers that the sidecar's `tailscale serve` proxy adds,
on `LOAMS_TAILSCALE_LISTEN` (`127.0.0.1:7781`) only. That listener is the serve
target and nothing else, and no container but the sidecar shares the engine's
network namespace. `oidc` checks JWTs from
`LOAMS_OIDC_ISSUER` (default `https://auth.loams.dev`) against its JWKS with
audience `LOAMS_OIDC_AUDIENCE`; `apikey` checks hashed keys in
`LOAMS_API_KEYS_FILE`, which compose points at `secrets/api-keys`. See
`.env.example`.

### Headscale (coming soon, unsupported in 0.1.0-preview.2)

0.1.0-preview.2 supports one setup: your own Tailscale tailnet and the hosted
console. Headscale issues no `ts.net` HTTPS certificates, so the hosted console
(served over HTTPS) cannot reach an engine on a Headscale tailnet. The profile
stays in the bundle for a later release; the steps below are unsupported.

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
deploy/release/byoc/uninstall.sh ubuntu@203.0.113.10          # --purge deletes the data
```

`install.sh` installs Docker if it is missing (get.docker.com), copies this
directory and `deploy/loams-postgres-dev` to `~/loams`, writes `.env` with mode
0600 and starts `loams tailscale tidb` (`--all` for everything). The VM
needs SSH and outbound internet only.

## Known gaps in the preview

- The engine does not reach the House yet. The House serves loopback plaintext
  only until TLS (D111, HS1 Task 20) and may not share the engine's network
  namespace, so `loams.house.v1` answers `house_not_configured`; use the House's
  own tailnet URL (`--profile house`).
- `HOUSE_SANDBOX=netns` fails under Docker's default seccomp profile (no
  unprivileged user namespaces); keep `pods` there. On a kernel without Landlock
  TCP rules (before 6.7) the workers refuse to boot.
- Live (7710), durable (8001), and the MySQL and Postgres wire listeners of the
  engine are loopback only and are not forwarded over the tailnet.
- TiDB starts with a passwordless root and the Postgres stack with default
  credentials; neither is forwarded over the tailnet by default.
- `compute1` needs `TENANT_ID` and `TIMELINE_ID` (see
  [`../loams-postgres-dev/README.md`](../loams-postgres-dev/README.md)).
