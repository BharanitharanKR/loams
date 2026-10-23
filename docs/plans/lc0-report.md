# LC0 — Loams Cloud on TiDB X: spike report

Status: **in progress** · 2026-10-23. Track LC, design [§78](../design/78-loams-cloud-on-tidb-x.md), plan [LC1](2026-10-23-lc1-loams-cloud.md) (LC0 Tasks 0–7).

## 4. Task 4: TiDB Cloud Zero for try-out SQL

Research only. No agent created a Zero database. The one manual creation and claim is the owner's (steps below).

| Question | Answer | Source |
|---|---|---|
| Is there an API, or only the SKILL? | **An API.** `POST https://zero.tidbapi.com/v1beta1/instances`, with an optional body `{"tag":"…"}`. "No API key, token, or Authorization header is required." It returns `connection{host, port 4000, username, password}`, a `connectionString`, `claimInfo.claimUrl` and `expiresAt`. There is no renewal call. The SKILL is a prompt that tells an agent to call this API | [SKILL.md](https://zero.tidbcloud.com/SKILL.md) v0.1.2; [zero.tidbcloud.com](https://zero.tidbcloud.com/) FAQ (V) |
| Can a third party create Zero databases for its users, programmatically? | **Technically, yes:** one unauthenticated `fetch`, and PingCAP pitches it for "automation", "MCP tooling" and "your app or agent". **The terms don't cover it.** There are no Zero-specific terms. The public-preview terms allow use "solely for internal evaluation purposes", if Zero counts as a preview (it isn't labelled one). The Cloud Services Agreement (updated 2026-09-01) bars "service bureau, outsourcing" use (§3.2(e)) and resale (§3.2(g)) | [Preview terms](https://www.pingcap.com/legal/tidb-cloud-public-preview-terms-and-conditions/); [CSA](https://www.pingcap.com/legal/tidb-cloud-services-agreement/) (V) |
| Rate limits | **None published.** Each database gets the Starter free quota plus a hard $1 spending limit, then refuses new connections. It lives 30 days | FAQ, SKILL.md (V); the rate limits are not verified |
| How does a claim work? | The user opens `claimUrl`, signs in to TiDB Cloud, and clicks Claim. The database becomes a Starter instance in their organization, with "schema, and data … exactly the same", and uses one of that organization's 5 free instances. **The claim link is a bearer link:** whoever opens it first claims the database. After expiry the link stops working and the data is removed | [Launch blog](https://www.pingcap.com/blog/tidb-cloud-zero-public-preview/) (V) |

**Is Q1264's Zero option allowed and practical?** **It isn't allowed as things stand. Q1264 resolves to Ostrium's cells** (§2).
- The terms don't cover third-party provisioning, and the service-bureau clause may apply.
- There are no published rate limits and no SLA. The API is v1beta1.
- Ostrium would hold each user's credentials and a bearer claim link until the user claims.

Zero stays attractive, because Ostrium would hold no tenant SQL at all. Revisit if PingCAP confirms in writing (the product page invites partnership questions at zero@pingcap.com).

**The owner's manual run** (pending):
1. On your own machine: `curl -sS -X POST https://zero.tidbapi.com/v1beta1/instances -H 'Content-Type: application/json' -d '{"tag":"loams-lc0-owner"}' > zero.json`. Note `expiresAt`.
2. Connect with `mysql --ssl-mode=REQUIRED` and the `connectionString`. Create a table, insert a row, and run `SELECT tidb_version()` and `SHOW GRANTS`.
3. Open `claimInfo.claimUrl`, sign in to the no-card LC0 organization, and click **Claim**. Run the activation instruction it gives.
4. Confirm the instance shows up as Starter and the row survived. Record whether the host or credentials changed, whether the claim used one of the 5 free slots, and the spending limit it got.
5. Delete the instance. Optionally, email zero@pingcap.com to ask whether Loams Cloud may create Zero databases for its users and hand them the claim link, and what rate limits apply.

## 7. Task 6: the company-device check

**Not run.** It needs a managed-like macOS or Windows laptop: a user account without admin rights, behind an HTTPS proxy with a re-signing CA. No agent has one. The table is §78 §10's, row by row, with what LC0 could establish without that laptop and the check the owner runs. Every row's verdict waits for that run.

| §10 row | Established without the laptop | The check on the laptop |
|---|---|---|
| Admin rights: none, a per-user service (D784) | — | Install Loams Desktop's per-user daemon as the standard user. Pass: no UAC, sudo or Authorization Services prompt, and a LaunchAgent or HKCU Run entry rather than a system service |
| Network client: outbound 443 only | Task 3: the control plane is a Worker on `workers.dev`, reached over HTTPS and WSS on 443 only | Set `HTTPS_PROXY` to the re-signing proxy (for example mitmproxy, its CA in the OS store). Pass: the daemon's WebSocket and HTTPS calls go through the proxy and succeed |
| MySQL over TLS on 4000 through the proxy, or the HTTP fallback | Task 1: MySQL/TLS on 4000 needs a raw TCP path. An HTTP proxy carries it only by `CONNECT` to port 4000, which most corporate proxies refuse (they allow 443). Task 3: the HTTP driver (`@tidbcloud/serverless`) works over 443 | Run `mysql --ssl-mode=VERIFY_IDENTITY -h gateway01.us-east-1.prod.aws.tidbcloud.com -P 4000` through the proxy, then the HTTP-driver call. Expected: MySQL fails and HTTP passes. Then the fallback must be automatic |
| The corporate CA from the OS store | — | With the proxy re-signing, the daemon must trust its CA from the OS store (Keychain, the Windows store) without a flag, and pin only `auth.loams.dev`'s issuer, never a leaf |
| Inbound: none, except loopback | — | `lsof -iTCP -sTCP:LISTEN` (macOS) or `Get-NetTCPConnection -State Listen` (Windows). Pass: only `127.0.0.1` or `::1` |
| Existing corporate Tailscale untouched; `tsnet` joins a second tailnet | Report §4.3: a device is on one tailnet at a time (V) | With the system Tailscale client on tailnet A, start a test daemon with `tsnet` on tailnet B (`TS_AUTHKEY` for B). Pass: the system client stays on A, the daemon's node appears in B, and the two reach their own peers only |
| Data leaving the device: off for `--work` | — | Enrol with `--work`. Pass: no transcript, workspace or SQL upload is attempted (proxy log) |
| Background activity, telemetry | — | No wake locks; the tray icon shows running agents; no telemetry hosts in the proxy log |
| Removal | — | `loams cloud device remove`, then uninstall. Pass: the per-user files listed in §50 §5 are gone and the login item is removed |
| MDM key `CloudSync=off` | — | Deferred to LC2 (§10) |

**What blocks it:** the per-user daemon exists in Loams Desktop (D784), but `loams cloud device add` and the device link are LC2 work. Before LC2 the laptop run can check only the rows that need no control plane: admin rights, the proxy and CA for HTTPS, MySQL versus HTTP through the proxy, the listeners, `tsnet`, and removal. **Recommendation:** run those six on the owner's work-like laptop before LC2's device link is built, and the rest as part of LC2's exit.
