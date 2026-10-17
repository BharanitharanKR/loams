# Publishing Loams: registries, secrets and the release path

Status: **0.1.0 publishes container images and the seven supported SDKs.** Since
v0.1.0-preview.1 (2026-10-12), CI publishes the release images to
`ghcr.io/ostrium-labs` on tags; see [Container images](#container-images-ghcrio).
From 0.1.0, [`.github/workflows/sdk-release.yml`](../../.github/workflows/sdk-release.yml)
publishes the TypeScript, Python, Rust, C#, Go, Swift and C++ SDKs as `0.1.0`, each
registry once its `SDK_PUBLISH_<REGISTRY>` variable is on. <!-- verify after publish -->
It is written for
`ostrium-labs/loams`; the decisions it follows are in
[`docs/design/13-decision-log.md`](../design/13-decision-log.md).

## What exists today, and what does not

| Registry | State | What |
|---|---|---|
| **npm** | Publishing, `sdk-release.yml` | `@loams/client` and `@loams/live`, `0.1.0` (dist-tag `latest`) |
| **PyPI** | Publishing, `sdk-release.yml` | `loams` `0.1.0` |
| **crates.io** | Publishing, `sdk-release.yml` | The SDK `loams` with `loams-proto` and `loams-live-proto`, `0.1.0`. The other `loams-*` crates are verified by [`.github/workflows/release-crates.yml`](../../.github/workflows/release-crates.yml) and upload only when a maintainer dispatches it inside the `crates-io` environment |
| **NuGet** | Publishing, `sdk-release.yml` | `Loams` `0.1.0` |
| **Go modules** | Publishing, `sdk-release.yml` | `loams.dev/go`, tag `sdks/go/v0.1.0`, through `proxy.golang.org`; needs the `go-import` tag on `loams.dev` (see [`sdks/go/README.md`](../../sdks/go/README.md#publishing)) |
| **SwiftPM** | The release tag | `https://github.com/ostrium-labs/loams`, version `0.1.0` |
| **GitHub release assets** | Publishing | The C++ SDK as `loams-cpp-0.1.0.tar.gz`, the `loams` CLI packages and Loams Desktop |
| Maven Central | Not yet | Java and Kotlin SDKs build and pass conformance in `sdks/`, and are not published (coming soon) |
| **GitHub Packages (`ghcr.io`)** | **Publishing** | `loams`, `loams-fabric`, `loams-agentd` and `loams-pg-control` publish to `ghcr.io/ostrium-labs` on tags. See [Container images](#container-images-ghcrio) |

<!-- verify after publish: each "Publishing" row above serves 0.1.0 -->

A rule for later: a registry gets a workflow when something in this repository
is ready to be installed from it, not when the issue lists it.

## Container images (`ghcr.io`)

The release images, the contract the compose stack in
[`deploy/release/compose.yaml`](../../deploy/release/compose.yaml) and
[Running Loams](preview.md) rely on:

| Image | Containerfile | Contents |
|---|---|---|
| `ghcr.io/ostrium-labs/loams` | `deploy/release/Containerfile.loams` | The engine, built with the features `live,live-tikv,durable,graph,tikv,pgwire,mysql-wire,stream-grpc` |
| `ghcr.io/ostrium-labs/loams-fabric` | `deploy/release/Containerfile.loams-fabric` | The House front, with `loams-house-worker` inside |
| `ghcr.io/ostrium-labs/loams-agentd` | `deploy/release/Containerfile.loams-agentd` | The agent daemon (optional) |

- **Build.** `deploy/release/build.sh` builds every image from its
  `deploy/release/Containerfile.<name>`, locally with Docker or Podman and in CI the
  same way.
- **Publish.** CI publishes on tags only, never from a pull request, with the
  workflow's own `GITHUB_TOKEN` (`packages: write`); there is no stored registry
  credential.
- **Tags.** Every image gets `:<version>` (for example `0.1.0-preview.1`, no `v` prefix) and
  the rolling `:preview`, which moves with each prerelease (`-preview.N`, `-rc.N`). A
  stable version (no `-`, for example `0.1.0`) gets `:<version>`, `:<major>.<minor>` and
  `:latest` instead, and is a normal GitHub release (the repository's "Latest"); a
  prerelease is a GitHub pre-release and never `:latest`. The rule is
  [`scripts/release/channel.sh`](../../scripts/release/channel.sh). Deployments that must
  not move pin `:<version>`.
- **Images built elsewhere.** `ghcr.io/ostrium-labs/tidb:v8.5.8-loams.1` is built by
  [`scripts/sqldb/tidb-image/build.sh`](../../scripts/sqldb/tidb-image/build.sh) from an
  annotated patch-queue tag, is public, amd64 only, and is pinned in
  [`release/sqldb-images.toml`](../../release/sqldb-images.toml), and the Postgres stack's images
  are pinned by digest in
  [`deploy/pins/loams-postgres-images.toml`](../../deploy/pins/loams-postgres-images.toml).
- **Not yet.** No attestation or cosign signature is attached to the images in
  0.1.0, and the Loams images are not promised to be multi-architecture.

### Third-party pins (design §54 §9.2)

PD, TiKV and TiDB (`deploy/release/.env.example`, `release/sqldb-images.toml`), the Loams Postgres images
(`deploy/pins/loams-postgres-images.toml`), chDB (`loams-chdb-sys`) and Grafeo (`Cargo.toml`) follow these rules:
- A pin changes in its own pull request into `dev`. It names the old and new versions and the component's own
  upgrade notes.
- The pull request carries the `ci:upgrade` label, so the upgrade gate runs on it. The repository checks fail a
  pin change without the label.
- The newest release's `[release.components]` in `release/upgrade-path.toml` equals the pin files, and no
  component goes down against the previous release that lists components.
- A bump across a component's major needs a `point_of_no_return` migration entry in that release, because its
  files do not go back.
- `scripts/release/check-upgrade-path.sh` checks the last two rules.

## What crates.io actually publishes

The crate list is declared in the workflow's `CRATES` and checked by
[`scripts/ci/publishable-crates.py`](../../scripts/ci/publishable-crates.py),
which derives the same list from the manifests and fails the run when the two
disagree. A crate is publishable when its manifest does not say
`publish = false` and none of its normal dependencies is itself unpublishable;
dev-dependencies do not count, because cargo strips them from a published
manifest.

Two crates are excluded by a `publish = false` this change adds, and both are
structural, not a judgement about their API:

- `loams`: its optional `loams-tikv`, `loams-durable` and `loams-meta-tikv`
  dependencies appear in the published manifest, and all three are
  unpublishable. `loams-tikv` and `loams-durable` depend on git revisions of
  private forks, which no registry can resolve. Uploading the binary today
  would produce a crate nobody can build.
- `loams-safekeeper`: same shape through its `tikv` feature and `loams-tikv`.

Both flags are one line each, and both come off in the change that publishes
the binary (D294's install, published as `loams-server`, D1106) or the first release after the forks
are vendored. This page is where that promise is written down; the crate
comments say it in place.

## Before the first publish

1. **Claim the namespace.** crates.io rejects a crate whose name is taken. The
   `loams` and `loams-*` names are free as of 2026-10-03 (the sparse index
   answers 404 for `loams`, `loams-common`, `loams-collection` and
   `loams-store`), but crates.io does not reserve them: whoever uploads
   `loams-store` first owns it, permanently.
2. **Reserve every name, by hand, with a token.** crates.io's trusted publishing
   only issues a token for a crate that already exists, so the first upload of
   each of the 18 crates is a manual `cargo publish` from a workstation with a
   freshly generated token (`cargo login`, scope `publish-write`, then revoke).
   Do this once per crate, lowest dependency first. No token is stored in this
   repository at any point.
3. **Configure the trusted publisher** on crates.io, for each crate: repository
   `ostrium-labs/loams`, workflow `release-crates.yml`, environment `crates-io`.
   From then on CI mints its own short-lived token through
   `rust-lang/crates-io-auth-action` and no credential is stored in GitHub.
4. **Create the GitHub environment** `crates-io` with required reviewers, so an
   upload waits for a second maintainer, and restrict it to the protected tags
   (`v*`). The environment name is part of the trusted-publisher configuration,
   so a token cannot be minted outside it.
5. **Add the missing package metadata.** Every crate carries a description and
   the Apache-2.0 licence, but none declares `repository`, `homepage`,
   `documentation` or `keywords`; `cargo publish --dry-run` warns about it and
   the verify job prints a warning per missing field. Add them in
   `[workspace.package]` and inherit them per crate, with at most five
   keywords.
6. **Set the release branch variable** if releases are not tagged on the
   repository's default branch: a repository variable `RELEASE_BRANCH`. Without
   it the guard requires the tag to be on the default branch, which is the
   integration branch here.
7. **Bump the workspace version in a PR, merge it, then tag.** The guard
   compares the tag against `[workspace.package] version` and refuses a tag that
   does not match, so a mis-tagged release cannot reach the registry.

Secrets, environments and variables this path uses, in full:

| Name | Kind | Needed by | Value |
|---|---|---|---|
| `crates-io` | environment (required reviewers, protected tags `v*`) | the publish job | none; it is a gate, not a secret |
| `RELEASE_BRANCH` | repository variable (optional) | the guard, verify and publish jobs | the branch releases are tagged on; defaults to the default branch |
| `RUNNER_MAIN` | repository variable (optional, pre-existing) | verify and publish | a runner label for the long build |

There is no other secret. In particular there is no `CARGO_REGISTRY_TOKEN`
secret, no npm token, no PyPI token and no Maven GPG key: the only credential
is the OIDC token GitHub mints for this workflow and crates.io revokes when the
job ends.

## How a release runs

1. Merge the version bump. `dev` is the integration branch.
2. Tag `vX.Y.Z` and publish a GitHub Release for it. That fires the workflow's
   `release` trigger, which runs the guard and `verify`: the tag is a real
   release, it is on the release branch at the workspace version, the crate list
   matches the manifests, and every crate packages and compiles the way crates.io
   will compile it. **Nothing is uploaded by this step.**
3. Dispatch `Release crates` with `tag: vX.Y.Z` and `publish: true`. The
   same guard and verify run again, the `crates-io` environment asks for a
   reviewer's approval, and the crates upload in dependency order.
4. A failure part way through is safe to repeat: cargo refuses a version that is
   already published, and the next dispatch continues with the crates that are
   not.

## Why a fork cannot reach the registry

- The only triggers are a published GitHub Release and a manual dispatch.
  Neither can be produced by a pull request, and nothing here uses
  `pull_request`, `pull_request_target` or `workflow_run`.
- A manual dispatch needs write access to the repository, which a fork does not
  have, and its `publish` input defaults to `false` anyway.
- The code that runs is checked out from the release branch, never from the tag
  and never from a pull request head, and the checkout keeps no credentials in
  `.git/config`.
- Every step of the guard reads the tag, the branch head and the release, so a
  tag pushed to a branch that is not the release branch is refused.
- The upload token is not a stored secret: it is minted from this workflow's
  OIDC identity, and crates.io only honours it for this repository, this
  workflow and this environment.

## Related

- [`docs/marketplace/publishing.md`](../marketplace/publishing.md) is a
  different thing: listing a third-party project in the Loams Cloud
  marketplace, not publishing Loams itself.
- D292 (cargo-dist for binaries and GitHub Releases), D294 (`cargo install
  loams`), D298 (the CLI track) and D400 (the `loams` package namespace).
- D220 and D552 (the open-core boundary). A publish path may not add a
  billing-grade name or a private package to anything it uploads.
