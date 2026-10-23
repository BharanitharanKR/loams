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
