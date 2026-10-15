# Azure Blob Storage

Native, `loams_connector_objects::azure_blob`, over the Blob REST API
(`object_store` 0.13.2, `azure`). Source and sink behave as the
[S3 connector's](s3.md); `bucket` (or `container`) names the container and
`account` the storage account.

| | |
|---|---|
| Auth | `auth.access_key` (Shared Key), else `auth.sas_token`, else `auth.bearer_token` (Entra ID) — all secret names |
| Conditional publish | `If-None-Match: *` |
| Notifications | Event Grid `BlobCreated` / `BlobDeleted`, either schema, nightly (CN2 R13.4) |
| Tested against | Azurite 3.37.0 in PR CI; a real account nightly |

A SAS token travels in URLs, so the family strips every URL's query string
from its error messages.
