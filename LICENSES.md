# Third-party programs Loams runs

Loams' own licence is in `LICENSE`, and code derived from other projects is
listed in `NOTICE`. This file lists programs Loams **runs as separate,
unmodified processes** and references in the bundles it distributes. They are
not part of Loams' code, and their licences do not extend to it.

| Program | Licence | Version and source | Where Loams references it | Notice |
|---|---|---|---|---|
| PgDog | AGPL-3.0, unmodified | `v0.1.60`, `https://github.com/pgdogdev/pgdog/tree/v0.1.60`; image `ghcr.io/pgdogdev/pgdog@sha256:25d1908886f595a2e3b00ec59783326712e3c0f75f91d29f032ff712b30f2266` | `deploy/pgdog`, `deploy/helm/loams-postgres`, `deploy/loams-postgres-single` (design §46 §8.3) | `deploy/pgdog/LICENSE-NOTICE.md` |
