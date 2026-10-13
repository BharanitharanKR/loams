# MinIO / RustFS

Both run the [S3 connector's](s3.md) code with the `minio-rustfs` target
profile (`loams_connector_objects::s3`, `runtime.profile: minio-rustfs`):
`endpoint` names the server, `allow_http` only for a development server.

| | |
|---|---|
| Tested against | RustFS 1.0.1 (Apache-2.0) in PR CI (`connectors-p2`, `objects` shard) |
| MinIO | no community image exists any more (R0.28); its half of `rustfs_and_minio_suites` runs when the `connectors-ci-only` job provides an image built from the last source release (`RELEASE.2025-10-15T17-29-55Z`, AGPL, never shipped). Until then MinIO is "deployments as of 2025-10, untested" |
| Source | listing; bucket notifications are not wired (webhook delivery, a later change) |
| Sink | as S3, with `If-None-Match: *` |

A server that ignores `If-None-Match` silently overwrites; the suite's
`conditional_put_refuses_overwrite` proves the server refuses. Set
`sink.conditional_put: false` only for a server that rejects the header, which
makes the sink at-least-once.
