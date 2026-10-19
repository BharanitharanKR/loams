# PgDog in Loams Postgres

PgDog is Loams Postgres's router: TLS, SCRAM, pooling, routing by database
name and wake-on-connect (design §46 §8). This directory holds what Loams
needs to run it unmodified:

- `LICENSE-NOTICE.md` and `LICENSE.AGPL-3.0`: the notice and licence text
  every bundle that references the PgDog image carries.
- `compose.yaml` and `scratch/`: PG2 Task 18's scratch stack, used to check
  PgDog's behaviour (the plan's Task 18 rulings). It is never deployed.

Loams renders PgDog's two configuration files, `pgdog.toml` and `users.toml`
(PG2 Task 20), pushes them, and sends `RELOAD` (Task 21). The pinned version
is recorded in `LICENSE-NOTICE.md` and in the root `LICENSES.md`.

## The licence boundary (design §46 §8.3, verbatim)

PgDog is **AGPL-3.0** (`LICENSE` is the GNU AGPL v3 text, no CLA; verified 2026-09-29, §28 §8; PG2 Task 18 checks again and records the pinned version). Loams uses it **as an unmodified, separate process**:

- No PgDog crate in any `Cargo.lock`; `deny.toml` already rejects AGPL. No PgDog source in any Loams repository. `conformance/router` stores no PgDog text (D318).
- No patches. A needed fix goes upstream (with the owner's approval) or is worked around in configuration. If neither works, the fallback is used (§28 §8: Neon's proxy, PgBouncer).
- No PgDog plugins: a plugin loaded into the PgDog process would form one program with it.
- Images are pinned by digest from `ghcr.io/pgdogdev/pgdog`. Wherever Loams **distributes** a bundle that includes the image reference (BYOC chart, the single-node compose file, the desktop's stack), it ships PgDog's licence text and a link to the exact upstream source tag, recorded in `LICENSES.md` and `deploy/pgdog/README.md`. The licence-check job (D-SC-6) checks deploy images.
- PgDog's closed Enterprise Edition is not used.

## What CI checks

`scripts/ci/pgdog-licence.sh` (its `--self-test` too) fails when a lockfile
or a vendored directory holds a PgDog package, when a file outside
`deploy/pgdog`, `deploy/helm/loams-postgres` and
`deploy/loams-postgres-single` (and the router conformance harness,
`scripts/router/inventory`, which is never distributed) references the image,
when a reference has no `@sha256` digest, or when such a bundle lacks
`LICENSE-NOTICE.md`. It also runs `scripts/spec/provenance.sh`.

## The pinned version

PgDog v0.1.60, `ghcr.io/pgdogdev/pgdog@sha256:25d1908886f595a2e3b00ec59783326712e3c0f75f91d29f032ff712b30f2266`,
source at `https://github.com/pgdogdev/pgdog/tree/v0.1.60`.
