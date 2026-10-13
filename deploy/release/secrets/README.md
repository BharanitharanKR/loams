# secrets/

Mounted read-only into the engine at `/etc/loams/secrets`. Nothing here is
committed except this file (see `.gitignore`).

- `api-keys`: the hashed API keys for `LOAMS_AUTH_MODE=apikey` and for the CLI
  and SDKs beside OIDC (`LOAMS_API_KEYS_FILE`). Its format belongs to the
  engine's auth module.
- `engine.env`: optional extra environment for the engine, one `KEY=value` per
  line, such as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` and
  `AWS_ENDPOINT_URL` for an `s3://` `LOAMS_BUCKET`.
- `pg-api-keys.json`: the API keys of `pg-control` (`--profile postgres-control`),
  `{"keys": [<entry>, …]}`, each entry made by `loams pg-api-key` (see the
  release README). It holds the keys' hashes, not the keys. `pg-control`
  reads it at `/etc/loams-pg-control/api-keys.json`; the engine does not.
