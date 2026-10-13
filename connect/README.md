# connect/

The Camel routes of `loams-connect`, the **fallback** runtime (CN2 plan,
ruling OR-4). No P1 or P2 connector runs on Camel by default: Camel serves
only the rows OR-4 names, each a manifest `runtime.fallback` block (or a
`fallback-only:` note) with the task that replaces it. CN1 Task 12 builds
`loams-connect` and the route templates; CN2 Task 22 adds the fallback
overlays under `routes/overlays/` and the catalog snapshot under
`connectors/vendor/`.

Loams writes no Java (owner, 2026-10-01). Route YAML may use only the
languages `simple`, `constant`, `header`, `jq`, `jsonpath` and `jsonata`, and
no `beans:` entry with a `type:` or `#class:` reference.

## The Camel bump procedure (fallbacks only)

Camel is pinned to one version (4.22.1 today; CN1 Ruling 7, R0.24), in
`.github/workflows/fabric.yml`'s `CAMEL_CATALOG_URL` and the files
`scripts/connectors/camel_bump.py` lists under `pins`. A bump is a pull request
that moves every pin and passes every step below. Camel stays on Camel Main:
Q352's Quarkus measurement is dropped, since Camel is fallback-only.

1. **Plan.** `python3 scripts/connectors/camel_bump.py --to X.Y.Z --dry-run`
   lists the steps, the pinned files and the fallback manifests, and says
   which steps are `ready`, `deferred` (to the task that owns their inputs)
   or `manual`. A change of Camel's major version is refused: that is a plan
   change, not a bump.
2. **Check in CI.** Run the `Fabric` workflow by hand (`workflow_dispatch`)
   with `camel_version = X.Y.Z`. Its `camel-bump` job downloads that
   version's `camel-catalog` from Maven Central, checks it against Central's
   SHA-1, and runs every ready step:
   - `catalog-snapshot`: regenerate `connectors/vendor/camel-catalog-X.Y.Z.json.zst`
     (Task 22's `camel_manifest.py --snapshot --version X.Y.Z`);
   - `catalog-drift`: `camel_catalog.py --check --tag X.Y.Z`, the Camel column
     of design §33 Appendix A against the new catalog;
   - `directions-match-catalog`: `camel_direction_matches_catalog`, every Camel
     direction or fallback against the catalog's consumer and producer flags;
   - `validate-fallback-routes`: `camel validate` over `connect/routes/`, in the
     Camel image of the new version;
   - `fallback-suites`: the suites of every manifest with a Camel runtime or
     fallback.
   The job stays red while any step is deferred: a bump is complete only when
   every step ran.
3. **Re-check `camel-iggy`** (manual, ruling R0.25): read the new version's
   offset commit. While it commits at poll time (or never), no fallback
   delivers through it; fallbacks deliver to the Fabric through `ingest`.
   Record the finding in `docs/plans/cn2-dependency-spike.md` §6.
4. **Open the pull request** with the pins moved, the regenerated snapshot,
   the licence stanza in `connectors/licences.toml`, and the job's run linked.
   The image digest of the Camel runtime moves in the same pull request.

Until CN2 Task 22 lands, the snapshot, direction, route and suite steps are
deferred, so a bump cannot be completed: only the catalog drift check runs
(ruling R27.6).
