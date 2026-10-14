# Loams Studio: threat model

> **Status:** SD1 Task 30 (2026-10-15), reviewed against `dev` after Tasks 0–28 and Task 13 (#598) merged. Covers Studio (`@loams/plugin-studio`, `@loams/studio-ui`), the daemon surfaces it added (`loams-agentd-connect`: the loopback and remote Connect listeners, preview tickets, the twins table and roles; the git writes, checkpoints, review, search, run groups, approvals, notifications and usage in `loams-agentd-sessions` and `loams-agentd-harness`), Electron main's agentd proxy, preview view and editor launcher, and the browser console's deployment. Design: [§53](../design/53-studio-ade.md) §6 and §8; plan: [SD1](../plans/2026-10-13-sd1-studio-ade.md), Task 30 and rulings T30-1…T30-18.

## 1. Assets and actors

| Asset | Where it lives |
|---|---|
| The daemon's owner token | `agentd.token` (0600, owner-only directory), read only by Electron main and the CLI |
| The person's OIDC access token for `loams-agentd` | The browser tab's memory (development: pasted per host, T7-11) |
| The person's code, checkouts and git remotes | The machine or VM the daemon runs on |
| Forge credentials (Forgejo token, `gh` login) | The daemon's keyring and `gh`'s own store (T9-3); never sent to a client |
| Code execution | Every harness run, terminal and project action runs as the daemon's user |

| Actor | Can do |
|---|---|
| A network attacker | Reach the remote listener's TCP port (`serve --remote`); send any bytes |
| A malicious web page | Run script in the person's browser; send requests to any origin, including loopback |
| A compromised console renderer | Call whatever the renderer may call (`renderer: true`) through Electron's proxy |
| An agent (model output, tool output, a prompt-injected repository) | Write files, print to terminals, emit markdown and diffs, call the MCP shim (role `mcp`) |
| A dev server's page | Run script in a preview frame or the desktop preview view |
| A supply-chain attacker | Publish a malicious npm or crate version; alter the T3 Code archive |

Out of scope: a compromised OS account, a compromised identity provider, and the harness CLIs' own security.

## 2. The remote listener (§53 §6.3)

- **Network attacker.** TLS 1.2/1.3 only (rustls on ring). Every request goes origin → preflight → OIDC token → role, in that order (T7-4); unauthenticated requests never reach a handler or a body decoder. The JWT verifier accepts RS256 and EdDSA only, with the alg matched to the key's family, required `exp`/`iss`/`aud`/`sub`, and one enrolled `sub` (T7-1). The `jwt` fuzz target covers it (§9).
- **Resource exhaustion.** Connections are capped at 256 and refused credentials waiting out the failure delay at 32; one more is a 429 at once (T30-2, fixed: before, both were unbounded). connectrpc's default message and body limits apply after authentication.
- **A stolen token** is the person: it can do what Studio does, including terminals and resolving approvals (I-2). Its lifetime is the issuer's; preview tickets never outlive it (T26-1).
- **A wrong origin** gets a 403 and no CORS header; origins match exactly, never `*` or `null`; no credentials mode (T7-3).
- **DNS rebinding.** A rebinding page's requests carry its own `Origin`, which is refused; TLS fails for a name the certificate does not cover. The preview path's "same origin" rule compares `Origin` with `https://<Host>`, which only a name the certificate covers can satisfy.
- **The `studio` role** is the Connect method set minus the installers and account logins (Q808); `Shutdown`, `Hello` and the key methods are not on the surface. Paths are matched exactly; anything else is `unknown_method` (T7-5, `paths_are_read_strictly`).

## 3. The loopback listener and the desktop proxy (§53 §6.2)

- A browser never reaches the loopback listener: any `Origin` is a 403, `Host` must be `127.0.0.1:<port>`, the peer must be loopback, and the owner token is compared in constant time (T5-3). A cross-site `fetch` cannot add the bearer token.
- **Renderer compromise.** Electron main forwards only exact `renderer: true` paths (`allowlist.gen.ts`), drops `Origin`, `Referer`, `Cookie` and the renderer's `Authorization`, adds the token, and strips any response header that echoes it (T5-6). The token never reaches the renderer. A compromised renderer can still do anything the renderer may (I-7): that is by design, and `agentd:git-write` is hygiene in the console's realm, not a boundary (T6-3).
- The preview view (`persist:studio-preview`) and the factory views are other sessions: the `loams-app` handler is installed on the console's session only, so their pages cannot reach the agentd proxy.

## 4. Untrusted content (vendored-code XSS paths, D919)

- **Markdown:** react-markdown + remark-gfm, no rehype-raw (raw HTML becomes text), rehype-sanitize with http(s) and relative links and `blob:` images only; the components check again and open http(s) through the platform's opener, which main checks again. No remote image is ever requested.
- **Diffs and file views:** `@pierre/diffs` 1.5.1 on the shadow-root path only (escaped tokens); no `prerenderedHTML`, SSR, marker `{html}` or `allowDangerousHtml` anywhere in Studio (checked by search). Code blocks are Shiki tokens rendered as React text.
- **Terminal output** goes to xterm.js only; links open on Ctrl/Cmd+click, http(s) through the platform, paths in the file viewer. **Pastes** now drop ESC and the other control characters before they reach the shell, so a copied snippet cannot end a bracketed paste early (`ESC [201~`) and run the rest, or send ^C/^D (T30-5, fixed).
- No `innerHTML`, `insertAdjacentHTML` or `dangerouslySetInnerHTML` in `@loams/studio-ui`, `@loams/plugin-studio` or `@loams/agentd-client`.

## 5. Supervised mode (§53 §8.1, Task 13)

- **Who decides.** `ResolveApproval`, `WatchApprovals` and `SetThreadApprovalMode` are role `owner`: the MCP shim can never answer, watch or switch a thread (T13-6). The remote `studio` role may, as §53 §8.1 says (I-2).
- **Timeouts deny** (`[approvals] timeout_secs`, 30 min); a turn that ends denies what it still waits for; an ask outside a turn is denied (T13-7). Pending approvals survive a restart (T13-8).
- **Per driver.** Claude: `--permission-mode default`, each `can_use_tool` waits. Codex: `on-request` with the thread's sandbox. opencode: forwards its permission event and is told `once`. ACP: forwards `session/request_permission`; **asks during `session/new`/`load` and the session setup calls are now rejected in a supervised thread** instead of auto-accepted (T30-6, fixed). Pi cannot ask, so a supervised Pi thread is refused (T13-10).
- **Escapes, fixed here:** a fork hung under another thread took that thread's or the default mode (T30-7); a side-chat chain deeper than the 16-parent walk fell back to the default, and each side chat's own run can make 8 more (T30-8); a thread started from the project page's composer (and the run group's judge) never set the mode, so on a desktop daemon it ran at the `full-access` default (T30-9, **high**).
- **Residual:** the daemon's default is `full-access` unless the remote listener is on (T13-3, I-1); a thread created by another client (the dock, the CLI) without a mode runs full access.

## 6. Git write misuse (§53 §8.2, Task 9)

- **The jail:** a `cwd` must be a known checkout, a `repoPath` a registered repository; paths are relative, literal pathspecs, no `..`, and now never inside `.git` (T30-10). `AddRepo` lets the owner register any repository: the jail protects against a confused client and the MCP shim, not against the owner role.
- **Destructive writes** need the confirmed checksum or head; pushes are never plain-forced; merges stop clean on conflicts; none is in the `mcp` role (Review Focus 3).
- **`CloneRepo`:** only remote git URLs; now also no ssh host or user that starts with `-`, parsed past a password (T30-11), and no clone folder of `.` or `..` (T30-12).
- **Forge credentials** go only to the configured base URL over https, with redirects off; a remote's owner or repository of `.`/`..` can no longer move the request to another API path of that host (T30-13).
- **Review comments:** `PostReviewToChangeRequest` now refuses when the checkout's diff can't be read rather than posting unchecked anchors (T30-14). The watch's `outdated` flag follows the diff sync's debounced publish; it is eventually consistent and display-only, so the timing T12's parity note saw is not a correctness issue (T30-15).

## 7. The preview proxy (Q806, Task 26)

- Remote: `/preview/…` behind the same origin and token checks; frames use a 256-bit ticket (≤ token `exp`, ≤ 8 h, ≤ 64 live), `frame-ancestors` limited to the console origins, `Referrer-Policy: same-origin`, the bearer token never forwarded. Unknown tickets take the failure delay and its bound (T30-2).
- The dev server's pages share the agentd's origin: cookies they set are visible to the person's other previews (T26-3, I-5). The API refuses that origin and takes bearer tokens only.
- Desktop: one sandboxed `WebContentsView` with no preload, its own partition, loopback-only navigation; other http(s) opens in the browser (I-6).
- **The browser console's CSP** now names the agentd hosts a deployment lists (`LOAMS_AGENTD_ORIGINS`) in `connect-src` and `frame-src` (T30-3).

## 8. Supply chain

- Vendored T3 Code: fetched by `vendor.mjs` into a scratch directory, SHA-256 pinned, never executed; headers and provenance rows enforced (`vendored_headers_present`, `provenance_hashes_match`, `no_effect_imports`).
- npm: the licence gate passed (316 production packages for the console, desktop and `@loams/studio-ui`); `pnpm audit --prod` reported no known vulnerabilities on 2026-10-15. Crates: `cargo deny --locked check` passed (advisories, bans, licences, sources).
- New crates in this task: none (`async-trait` and `tower` are new edges of crates already in the lock).

## 9. Fuzzing

`crates/loams-agentd-connect/fuzz` (cargo-fuzz, `agentd-connect-fuzz` in `ci.yml`, 60 s each in the full tier and the nightly), beside DD1's `crates/loams-agentd-rpc/fuzz` (`frame`, `handshake`):

| Target | Input | Invariant |
|---|---|---|
| `connect_decode` | Any `loams.agentd.v1` method, any codec (Connect/gRPC/gRPC-Web, proto/JSON, GET, gzip), any body, through the real router and handlers over a recording fake engine | No panic or hang; a handler calls only its own ndjson twin (and the right `Mutate` op or command kind), with params that parse as that method's `Params`; a gap never calls the engine |
| `jwt` | A bearer token against a fixed Ed25519 key set | No panic; anything accepted is EdDSA-signed for the enrolled subject, audience and issuer, with the `exp` the verifier answered |
| `ndjson_params` | JSON params for any ndjson method | Decoding never panics; what decodes re-encodes the same |
| `path_jail` | A string | `checkout_relative` and `WorkspaceRelativePath` never accept an absolute path, `..` or `.git`; an accepted clone URL is no option, helper, local URL or ssh option, and its folder is never `.`/`..`; a ref-safe chat id is one ref component; a Forgejo API URL stays under the repository; review text quotes to one line with every quote escaped |

The invariants run on their seeds under stable in `cargo test` (`fuzz::tests`); 60 000 random mutations of the ndjson fixtures ran clean locally under stable. The `path_jail` invariant found the password-in-URL case of T30-11 while it was being written.

## 10. Open issues

| # | Issue | Severity | Owner |
|---|---|---|---|
| I-1 | The daemon's default mode is `full-access` unless the remote listener is on; any client that creates a thread without `SetThreadApprovalMode` runs it unsupervised | Medium | Owner decision (Q803): make `supervised` the default, or require the mode at creation |
| I-2 | The remote `studio` role may resolve approvals and switch a thread to full access; a stolen remote token can approve its own runs | Info (by design, §53 §8.1) | — |
| I-3 | The verifier does not check the token type (`typ`/`azp`): an ID token minted for the `loams-agentd` client is accepted like an access token | Low | MT1 |
| I-4 | With no cached keys and the issuer down, each request triggers a (serialized, 5 s) JWKS fetch | Low | Follow-up |
| I-5 | Previews of one daemon share an origin and its cookies (T26-3) | Low (accepted) | — |
| I-6 | A dev server's page in the desktop preview can navigate to any loopback service, and open any http(s) URL in the browser without a gesture | Low | Follow-up |
| I-7 | A compromised renderer has the owner's daemon rights apart from the token itself, `Shutdown` and `Hello` (terminals included) | Info (by design) | — |
| I-8 | A shared hosted console (console.loams.dev) can't list every person's agentd host; `*.ts.net` is in `connect-src` but not `frame-src` | Medium (availability) | Q804 / the user-settings API |
| I-9 | `outdated` misses a line whose text changed but stays in a hunk, and a comment anchored on the working tree is posted at the same line of the pull request's head (T11-1) | Low | Follow-up |
| I-10 | An `agentd-data` volume made before T30-16 has no `/home/loams/agentd`; Docker creates it root-owned when mounting `config.toml`, and the daemon can't start until it is chowned or the volume recreated | Low (upgrade) | Release notes |
