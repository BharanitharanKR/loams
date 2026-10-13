# Cloudflare R2

R2 runs the [S3 connector's](s3.md) code with the `cloudflare-r2` target
profile (`loams_connector_objects::s3`, `runtime.profile: cloudflare-r2`).

| | |
|---|---|
| Endpoint | `https://<account-id>.r2.cloudflarestorage.com`, `region: auto` (the profile's default) |
| Source | listing (polling); **no event notifications** |
| Sink | as S3, published with `If-None-Match: *`, which R2 documents for `PutObject` |
| Tested against | fixtures recorded from real R2 in PR CI (once recorded), a real bucket nightly |
| Narrowings | `source.streaming` (no notifications) |

Unverified until the first nightly records it: that a conflicting
conditional PUT answers 412 (R0.22). Until R2's suite is green the manifest
stays `planned` (CN2 R13.5).
