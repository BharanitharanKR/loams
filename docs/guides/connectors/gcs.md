# Google Cloud Storage

Native, `loams_connector_objects::gcs`, over the GCS JSON API (`object_store`
0.13.2, `gcp`). Source and sink behave as the [S3 connector's](s3.md).

| | |
|---|---|
| Auth | `auth.service_account_key` (the name of a secret holding the JSON key); absent: the pod's metadata server |
| Conditional publish | `x-goog-if-generation-match: 0` |
| Notifications | Pub/Sub (`OBJECT_FINALIZE`, `OBJECT_DELETE`), nightly; the subscription client is the cloud family's (CN2 R13.4) |
| Tested against | fake-gcs-server 1.56.1 in PR CI; a real bucket nightly |

`endpoint` points the client at an emulator (fake-gcs-server); with no key the
client then sends no OAuth token.
