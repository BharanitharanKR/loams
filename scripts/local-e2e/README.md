# local-e2e: the release stack on one machine, with real data

The owner's pre-0.1.0 check (2026-10-17): run the release compose stack
(`deploy/release/compose.yaml`) locally from this checkout, seed every product
with public data, and test the console and the desktop app against it.

Nothing here changes the release bundle. The scripts copy it to
`$E2E_HOME/stack` (as `byoc/install.sh` copies it to a VM) and add
`compose.override.yaml` from this directory.

## Where things live

| What | Where |
|---|---|
| Everything local-e2e keeps (`E2E_HOME`) | `/mnt/Projects/loams-e2e` |
| Its own Podman store and API | `$E2E_HOME/podman/storage`, socket `$XDG_RUNTIME_DIR/loams-e2e-podman.sock`, unit `loams-e2e-podman` |
| The stack directory (bundle + `.env` + `secrets/` + override) | `$E2E_HOME/stack/release` |
| Downloads | `$E2E_HOME/data` (the worktree's `../local-e2e-data` links there) |
| Binaries and images | `$E2E_HOME/bin`; `localhost/loams-e2e/{loams,loams-fabric,loams-agentd,loams-pg-control}:0.1.0` |
| Logs and seed checks | `$E2E_HOME/logs` (`seed-<product>.json`) |

The store is separate from your default Podman store (`/home` is nearly full):
use `source scripts/local-e2e/env.sh` and then `epodman ps`, `ecompose ps`,
`ecompose logs -f loams`. Plain `podman ps` does not show these containers.

## Ports (all on 127.0.0.1)

| Port | Service |
|---|---|
| 18080 | engine main port: REST, Connect, gRPC-Web, Live, the House link |
| 18082 | Flight SQL |
| 16333 / 16334 | Qdrant REST / gRPC |
| 19200 | Elasticsearch |
| 18001 | Durable (Resonate protocol) |
| 15432 / 13306 | Postgres wire / MySQL wire over collections (namespace `e2e`) |
| 18123 | House, ClickHouse HTTP (user `default`, password: `ecompose exec house cat /var/lib/loams-house/house.key`) |
| 14000 | TiDB (Loams SQL), root without a password |
| 7680 | pg-control (`loams.postgres.v1`), API key in `$E2E_HOME/pg-api-key` |
| 55500-55999 | Postgres computes |
| 19000 | RustFS S3 (`rustfsadmin` / `rustfsadmin`), buckets `loams-engine`, `neon` |
| 5190 | console dev server (the engine-served edition: base.yml pages) |
| 5192 | the hosted web edition (deploy/stage.mjs output, as console.loams.dev serves it) |
| 3000 | the compose `console` service (broken in 0.1.0, see the report) |
| 17443 | agentd for the web console (HTTPS, self-signed) |

## How it runs without Tailscale

`LOAMS_AUTH_MODE=off`, the documented laptop-only mode
(docs/release/preview.md, "Without a tailnet"). The engine then refuses to
start unless every listener is loopback, so the override adds
`loams-forward`, a socat sidecar in the engine's network namespace (where the
Tailscale sidecar would be), which forwards the published 127.0.0.1 host
ports to the engine's loopback listeners. The House gets the same
(`house-forward`). Every caller is `dev-loopback`; `HOUSE_PRINCIPALS` gives it
the House. Containers on the compose network can reach the forwarder too, so
never run this on a shared machine.

The engine's bucket is S3: `s3://loams-engine/stack` on RustFS (from the
included Postgres stack; `e2e-bucket` creates the bucket), with the `AWS_*`
settings in `secrets/engine.env` (`AWS_ENDPOINT_URL=http://rustfs:9000`). A
stack made before PR #755 (B1) used `file:///var/lib/loams/bucket`; `up.sh`
switches its `.env` over, and the old data is not moved, so re-seed (`down.sh
--purge` or remove the volumes first). Git repositories live in the same bucket
under `ns/<namespace-id>/repos/<repo>`; `seed/git.sh` pushes there from the host
through RustFS's port 19000.

## Steps

```sh
cd ~/Documents/Ostriumlabs/loams-wt/local-e2e
pnpm install                                   # console, desktop, SDK, Playwright
scripts/local-e2e/fetch-data.sh                # ~600 MB of public data
scripts/local-e2e/build-binaries.sh            # release binaries in rust:1.97.1-slim-trixie (~1.5 h cold)
scripts/local-e2e/build-images.sh              # the four images, from those binaries
scripts/local-e2e/up.sh                        # the stack, with --profile house and postgres-control
scripts/local-e2e/seed-all.sh                  # every product (or name some: seed-all.sh graph house)
scripts/local-e2e/agentd.sh web                # agentd for the web console's Studio (debug build)
scripts/local-e2e/console.sh                   # dev server, http://127.0.0.1:5190/
scripts/local-e2e/console.sh hosted            # hosted web edition, http://127.0.0.1:5192/
node scripts/local-e2e/console-setup.mjs       # first-run setup (org + owner) through the console
pnpm --filter @loams/console build && scripts/local-e2e/desktop.sh   # start [dev|preview]; the app starts agentd itself
node scripts/local-e2e/console-crawl.mjs --base http://127.0.0.1:5192/ --out <dir>  # screenshot + report per page
node scripts/local-e2e/console-interact.mjs --out <dir>   # Analytics, Graph, Live, Studio (hosted edition)
node scripts/local-e2e/desktop-crawl.mjs --out <dir>      # every desktop page (its own userData)
```

The console's first-run owner is `owner@e2e.local`; the password is in
`$E2E_HOME/console-owner.env` (mode 0600). For Studio in the hosted edition, add
the host `https://127.0.0.1:17443` in Studio > Settings with the token in
`$E2E_HOME/agentd/web/token` (accept the self-signed certificate at that URL
first in your own browser). The pasted token lives in the tab's memory only.

Restarting the engine: `ecompose rm -sf loams-forward && ecompose up -d loams loams-forward`
(Podman refuses to recreate a container that another one shares the network
namespace of).

Stop: `scripts/local-e2e/down.sh` (data kept). Remove everything:
`scripts/local-e2e/down.sh --purge`.

### Images

`deploy/release/build.sh` compiles inside `podman build` with one cache mount
per image, which this machine has no disk for, so `build-binaries.sh` runs the
same build stage (`images/Containerfile.builder`: the Rust image, packages and
`cargo build --locked --release` commands of `deploy/release/Containerfile.*`)
with one bind-mounted target, and `build-images.sh` puts the binaries into the
release runtime stages (`images/Containerfile.*`, verbatim except for the
`COPY` of the binaries). A host build does not work: it links the host's glibc
2.44, whose libm symbols trixie's 2.41 lacks.

## Seed data

| Product | Source | Size | Script |
|---|---|---|---|
| Collections, hybrid search, Qdrant, ES, Flight SQL, pg/mysql wire | DBpedia entities with OpenAI text-embedding-3-small 512-d vectors (Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K, Hugging Face) | 100k rows, 364 MB | `seed/collections.py` |
| Streams and links | GH Archive, 2024-01-15 15:00 UTC | 136 MB gz | `seed/streams.py` |
| Graph | the same hour's actor-repository graph; SNAP email-Eu-core | 1005 nodes, 25571 edges | `seed/graph.py` |
| Live | the same hour (first 400 events), TypeScript SDK | | `seed/live.ts` |
| Loams SQL (TiDB), Loams Postgres | Chinook 1.4.5 (lerocha/chinook-database) | 11 tables, ~15k rows | `seed/relational.py` |
| House | NYC TLC yellow taxi, January 2024 | 2.96M rows, 50 MB parquet | `seed/house.py` |
| Durable | an order workflow's promises, tasks, schedules | | `seed/durable.py` |
| Git | a repository made by the script, pushed with git-remote-loams | | `seed/git.sh` |

Downloads are data: each in its own directory, read with `python -I`, never
executed. The Chinook SQL files are split into statements and sent through the
drivers, and refused if they hold a client meta-command or a server-side file
or program statement.
