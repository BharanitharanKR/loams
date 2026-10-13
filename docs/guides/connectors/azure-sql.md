# Azure SQL Database

Azure SQL Database runs on the native SQL Server connector ([sql-server.md](sql-server.md)) through the target profile `connectors/profiles/azure-sql.yaml` (CN2 Task 12, OR-2). Everything in that guide applies; this page lists what differs.

**Tested against:** a real Azure SQL database, nightly (`connectors-nightly`, repository secret `LOAMS_AZURE_SQL_URL`). Azure SQL has no image, so SQL Server 2022 Developer in compose is an extra target and never the system under test. The connector becomes `preview` only once its suites are green, and `stable` after 14 consecutive green nightlies (OR-23).

- **Login.** The profile defaults to `auth: entra`, a Microsoft Entra ID service principal. The connector fetches a token for `https://database.windows.net/.default` with the client-credentials grant from `<entra.authority>/<tenant_id>/oauth2/v2.0/token`. The request goes through the SDK's HTTP client, so the egress policy applies. The token is cached until five minutes before it expires, and dropped when the server refuses a login. The client secret is a secret name; the profile and the instance spec never hold it. `auth: entra-token` takes a token the runtime keeps fresh instead (a managed identity), and `auth: sql` takes a SQL login.
- **Encryption.** `encrypt: strict` (TDS 8.0) is the only mode Azure SQL accepts. The profile sets it, and the connector refuses Entra ID without it.
- **Gateway redirects.** Under the *redirect* connection policy, the gateway hands the client the database node's address. The connector opens that second connection through `sdk::net::connect` as well, so the egress policy sees it.
- **CDC** needs a database on the S3 tier or above (or vCore). Its scheduler runs capture and cleanup without SQL Agent, and `msdb.dbo.cdc_jobs` is not readable, so set `retention_minutes` for the retention metric (the default assumes three days).
- **Narrowings:** none.
