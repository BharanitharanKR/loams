# Amazon Aurora (PostgreSQL) connector

Aurora PostgreSQL runs on the native Postgres family through the target profile `aurora-postgres` (`connectors/profiles/aurora-postgres.yaml`). Batch reads, pgoutput CDC and the exactly-once sink behave as in the [PostgreSQL guide](postgresql.md). The MySQL engine is CN2 Task 11's `aurora-mysql`.

**Tested against:** a real Aurora PostgreSQL cluster, nightly (`LOAMS_AURORA_POSTGRES_URL`, OR-3). Aurora has no container image. PostgreSQL in compose is an extra target only and never counts for Aurora.

## Differences from PostgreSQL

- **TLS.** The profile defaults to `sslmode: require`. For `verify-full`, set `ca_pem` to the Amazon RDS CA bundle, which is not in the webpki roots.
- **IAM authentication** (`auth: iam`). The connector mints an RDS IAM token (a SigV4-presigned `rds-db:connect` URL, valid 15 minutes) for every connection and uses it as the password. The instance names its AWS credentials by secret:

  ```yaml
  auth: iam
  user: loams_cdc            # a database user granted rds_iam
  aws:
    region: eu-west-1
    access_key_id: aurora-key-id          # secret names, never values
    secret_access_key: aurora-secret-key
    session_token: aurora-session-token   # optional
  ```

- **CDC.** Enable `rds.logical_replication = 1` in the cluster parameter group, and grant the user `rds_replication`.

No narrowings are known; the nightly suite adds any it finds to the profile with a `narrows:` note.
