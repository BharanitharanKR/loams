# PgDog licence notice

Loams Postgres runs **PgDog** as its connection router. PgDog is a separate
program by PgDog, Inc. and its contributors, licensed under the **GNU Affero
General Public License, version 3** (AGPL-3.0). It is not part of Loams'
code, and Loams does not modify it.

| | |
|---|---|
| Program | PgDog, `https://github.com/pgdogdev/pgdog` |
| Version | `v0.1.60` (git `0d040f9`) |
| Image | `ghcr.io/pgdogdev/pgdog@sha256:25d1908886f595a2e3b00ec59783326712e3c0f75f91d29f032ff712b30f2266` (multi-arch index) |
| Corresponding source | `https://github.com/pgdogdev/pgdog/tree/v0.1.60` |
| Licence | AGPL-3.0; the text is `LICENSE.AGPL-3.0` beside this file, PgDog's `LICENSE` at that tag |
| Modifications | None. Loams only writes its configuration files and sends admin commands |

The AGPL-3.0 grants you the right to obtain PgDog's complete corresponding
source for the version you run, at the link above. Every Loams bundle that
references the PgDog image (the single-node compose file, the Helm chart, the
desktop's stack) carries this notice and the licence text.
