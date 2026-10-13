# ADLS Gen2

Native, `loams_connector_objects::adls_gen2`. Listing, reading and writing use
the Blob API, as [Azure Blob](azure-blob.md) does. With `hns: true` (an account
with a hierarchical namespace) the sink publishes each rolled file by writing
it under `_staging/` and making it visible with an **atomic DFS rename** that
refuses an existing destination (`x-ms-rename-source`, `If-None-Match: *`),
because `object_store` renames by copy and delete (R0.21).

| | |
|---|---|
| Config | as Azure Blob, plus `filesystem` (alias of `bucket`), `hns`, `dfs_endpoint` (default `https://<account>.dfs.core.windows.net`) |
| DFS auth | Shared Key (HMAC-SHA256, version `2023-11-03`), SAS, or Entra ID bearer |
| Tested against | flat Blob operations on Azurite in PR CI; the DFS rename on a real HNS account nightly (`adls_hns_rename_nightly`). Azurite has no hierarchical namespace |
| Narrowings | until the nightly is green: HNS rename (R0.21) |

PR CI checks the DFS requests' shape against a local endpoint
(`dfs_rename_request_shape`), which does not count toward the suite; fixtures
recorded from a real account replace it when Task 6's proxy exists.
