# Local CI hosts

Two laptops on the owner's tailnet run Loams CI; GitHub-hosted runners are kept for releases
(owner, 2026-10-24). The guide is [docs/guides/local-ci.md](../../docs/guides/local-ci.md).

| File | What |
|---|---|
| `setup-host.sh` | One idempotent command that sets a host up (`sudo deploy/ci-hosts/setup-host.sh --help`) |
| `versions.env` | Pinned versions and SHA-256 digests of everything downloaded |
| `lib/` | The pieces `setup-host.sh` sources: the `ci` user, the private CA, NativeLink, sccache |
| `sccache-evict.sh` | Least-recently-used eviction for sccache's store (run by a timer) |

What `setup-host.sh --role primary` sets up today:

- the `ci` user (no sudo, locked password, home mode 700, subordinate ids for rootless podman);
- a private CA (key in the owner's `~/.config/loams-ci/ca`, mode 600) and a client certificate for
  the owner (`~/.config/loams-ci/tls`);
- NativeLink as `loams-nativelink.service` (user `ci`): CAS + action cache with LRU eviction under
  `/mnt/Projects/ci/nativelink` (30 GiB cap), the scheduler and one worker. It listens on
  127.0.0.1 (plain gRPC) and on the Tailscale address (mutual TLS) only, ports 50051 (clients)
  and 50061 (workers, health), and opens those two ports on `tailscale0` in ufw;
- sccache 0.18.0 in `/opt/loams-ci/bin`, and its shared store: dufs (WebDAV) as
  `loams-sccache-store.service` (user `ci`) on 127.0.0.1 and the Tailscale address, port 50081,
  password-protected, under `/mnt/Projects/ci/sccache` with a 10 GiB cap enforced every 10 minutes
  by `loams-sccache-evict.timer`. sccache has no Remote Execution API backend, so it cannot share
  NativeLink's CAS. Clients load `/etc/loams-ci/sccache.env` (the runners) or
  `~/.config/loams-ci/sccache.env` (the owner, opt-in); `~/.cargo/config.toml` is not touched.

Secrets are generated on the host, kept in files of mode 600 (or 640 for group `ci`), and never
printed or committed.
