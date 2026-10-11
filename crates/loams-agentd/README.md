# loams-agentd

`loams-agentd` is Loams Desktop's per-user agent daemon. It is headless (no GUI crate on any target, D782) and runs:
- the sessions engine;
- the agent harnesses: Claude Code, Codex, opencode, Pi, and the ACP agents Devin, Grok, Hermes, Antigravity and Loams Bot;
- local preview routing;
- the MCP shim that the engine injects into every harness run.

Clients reach it over an ndjson RPC on a loopback WebSocket. When DD1 is complete, the daemon will also supervise `loams dev` and the compose stacks, run Loams' own agent loop, and keep agent turns durable across restarts. Electron is its client.

The design is [§50, the Loams Desktop agent daemon](../../docs/design/50-loams-desktop-daemon.md). The plan is [DD1](../../docs/plans/2026-10-09-dd1-desktop-daemon.md).

## Running it from a checkout

The command-line contract (§50, plan DD1 "Shared contracts") is:

```sh
mkdir -p .agentd-dev && touch .agentd-dev/config.toml
cargo run -p loams-agentd -- run --child --config .agentd-dev/config.toml
```

`config.toml` has the keys listed in the plan's Shared contracts; every key is optional, so an empty file works. The daemon keeps its other files in the config's directory, so give it a directory of its own:
- `agentd.lock`, the single-instance lock;
- `agentd.token`, the RPC token (0600, new at every start);
- `agentd.json`, the discovery file (protocol, version, pid, port, start time and mode; removed on a clean exit);
- `store/`, the sessions engine's data;
- `adapters/`, the harnesses' managed ACP adapter installs;
- `logs/agentd.log`, the daemon's own log (a supervisor's capture of stdout and stderr goes to `logs/agentd.stdout.log`).

The RPC listens on `127.0.0.1` on an ephemeral port, which `agentd.json` names. `--child` (Loams Desktop starts and stops the daemon) or `--service` (the per-user service) sets the mode that `Hello` reports; without either flag, `[daemon] mode` in the config decides. `LOAMS_AGENTD_CONFIG` overrides `--config`.

Every WebSocket upgrade must carry `Authorization: Bearer <agentd.token>` and no `Origin`, with `Host: 127.0.0.1:<port>`; anything else is refused (401 or 403) before the RPC is served, and `Hello` must be the first call on a connection (design §50 §7).

Exit codes: 0 stopped, 3 already running (a second `run` on the same directory prints `already running (pid …)`), 4 invalid config (the message names the key), 70 internal error.

The other subcommands are `-- status --config <file>`, `-- version` and `-- mcp`. The `mcp` subcommand is the stdio shim; the daemon injects it into each harness run with a token scoped to that run (`LOAMS_AGENTD_RUN_TOKEN`, `LOAMS_AGENTD_PORT`, `LOAMS_AGENTD_SESSION_ID`), so it is not useful on its own.

Build and test one crate at a time with `-p`, for example `cargo test -p loams-agentd`. Use the shared target directory (never set `CARGO_TARGET_DIR`). `cargo clippy -p 'loams-agentd*' --all-targets -- -D warnings` runs the lint gate, and `scripts/ci/agentd-deps.sh` runs the headless guard.

## Provenance

The daemon began as **zeron** (https://github.com/zeronsh/zeron), MIT, "Copyright (c) 2026 Wing". Zeron came to Loams through the Loams Desktop fork, `ostrium-labs/loams-desktop`, which keeps zeron's history:
1. The fork's tracked files were imported into `apps/desktop/native` from fork commit `c9205f8f949711c3315af551c804c56452d2fbd7`.
2. DD1 Task 1 moved the headless crates into the root workspace as `crates/loams-agentd*` (D783).
3. GPUI, the headed mode, the edge, WorkOS, Cursor, self-update and push were deleted (D781, D782).

[`import-provenance.json`](import-provenance.json) records:
- the source commit;
- the SHA-256 of every file as it was imported;
- the fork's dependency closure;
- under `moved_to_root_workspace`, where each fork crate went and its licence.

Files kept with this crate:
- [`LICENSE`](LICENSE): zeron's MIT licence, unmodified.
- [`NOTICE`](NOTICE): the attribution and licence scope.
- [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md): third-party assets bundled with the daemon (none today; zeron's GUI entries were removed with the GUI).
- [`SCOPED_NOTICE.md`](SCOPED_NOTICE.md): the original import notice.

## Licences

Every `loams-agentd*` crate is listed here. The workspace test `licence_fields_are_set` (`tests/workspace.rs`) reads `cargo metadata` and checks each crate against this table:
- each crate's `license` matches its row;
- a `zeron` crate is MIT;
- a `Loams Desktop` or `new` crate is Apache-2.0;
- a `new` crate takes `license.workspace = true`.

Only the `new` rows may name a crate that does not exist yet. A crate added to the workspace must be added to this table.

| Crate | Licence | Origin | Notes |
|---|---|---|---|
| `loams-agentd` | MIT | zeron | The binary, from the fork's `apps/loams-desktop`, rewritten headless |
| `loams-agentd-sessions` | MIT | zeron | From the fork's `engine` |
| `loams-agentd-harness` | MIT | zeron | From the fork's `harness` |
| `loams-agentd-proto` | MIT | zeron | From the fork's `proto`. It also holds the brand strings of the fork's Apache-2.0 `loams-desktop-brand`; the Loams Authors place those lines under MIT (T0-10a) |
| `loams-agentd-rpc` | MIT | zeron | From the fork's `rpc` |
| `loams-agentd-doc` | MIT | zeron | From the fork's `doc` |
| `loams-agentd-store` | MIT | zeron | From the fork's `sync` |
| `loams-agentd-mcp` | MIT | zeron | From the fork's `mcp` |
| `loams-agentd-preview` | MIT | zeron | From the fork's `preview` |
| `loams-agentd-link` | Apache-2.0 | Loams Desktop | From the fork's `loams-desktop-link`, written by the Loams Authors; its own `LICENSE` |
| `loams-agentd-llm` | Apache-2.0 | new | Model providers (DD1 Task 18) |
| `loams-agentd-loop` | Apache-2.0 | new | The native loop, tool registry, approvals and scrubbing (DD1 Tasks 19, 24, 25) |
| `loams-agentd-tools` | Apache-2.0 | new | Loams and SQL tools (DD1 Tasks 20, 21) |
| `loams-agentd-factory` | Apache-2.0 | new | Factory ops (DD1 Task 23) |
| `loams-agentd-supervisor` | Apache-2.0 | new | The engine and stack supervisor (DD1 Tasks 14, 15) |

Changes that the Loams Authors make to the MIT crates are released under those crates' MIT licence.
