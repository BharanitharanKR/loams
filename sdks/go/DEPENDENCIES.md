# Go SDK dependencies

Pinned by `go.mod` and `go.sum`. Everything here is Apache-2.0 or BSD-3-Clause:
**no AGPL, and no PgDog** (the licenses `docs/design/13-decision-log.md` D64 and
D600 rule out).

| Module | Version | Licence | Why it is here |
|---|---|---|---|
| [`connectrpc.com/connect`](https://github.com/connectrpc/connect-go) | v1.21.0 | Apache-2.0 | The Connect client (D604, D612: Go is Connect, not gRPC). It is the one library the Go SDK is built on: the protocol, the typed `*connect.Request`/`*connect.Response`, the server-stream reader, and the error decoding that makes `reason` reachable. |
| [`google.golang.org/protobuf`](https://github.com/protocolbuffers/protobuf-go) | v1.36.11 | BSD-3-Clause | The protobuf runtime: the generated messages, `proto.Clone`, and the descriptor walk `DeclaresIdempotencyKey` uses to read `idempotency_key` off a **schema** rather than off an object (R3). A direct dependency because the SDK reads descriptors; connect-go depends on it too. |
| [`golang.org/x/net`](https://pkg.go.dev/golang.org/x/net) | v0.55.0 | BSD-3-Clause | `http2.Transport`, for `TransportConfig.UseHTTP2` — the in-cluster caller that wants gRPC or Connect over one multiplexed connection. Optional in the sense that the default HTTP/1.1 path does not touch it. |
| `golang.org/x/text` | v0.37.0 | BSD-3-Clause | Indirect, required by `golang.org/x/net`. Listed for the licence audit, not chosen. |

## Versions

The two generation-relevant modules match `scripts/sdk/pins.lock`, which
`scripts/sdk/check-pins.sh` enforces: `connectrpc.com/connect` v1.21.0 and
`google.golang.org/protobuf` v1.36.11, with `buf.gen.go.yaml` pinning the
matching remote plugins (`protocolbuffers/go:v1.36.11`, `connectrpc/go:v1.21.0`).
The checked-in stubs under `gen/loams/` were regenerated with those plugins, so
`buf generate` reproduces the committed output rather than a diff.

- **The floor is Go 1.25.** `go.mod` says `go 1.25.0`, which connect-go v1.20.0
  and later require; the earlier v1.19.2 pin existed only to keep a 1.24 floor
  (Q732), and `go.mod` had already moved past it.
- **`connect-go` v2 is not taken.** v2.0.0 (2026-10-07) is under 14 days old on
  2026-10-13 and changes the import path; it is its own PR.
- **`golang.org/x/net` is v0.55.0.** Later releases require a newer Go than the
  floor.

## What is deliberately *not* a dependency

- **`github.com/google/uuid`** — a UUIDv7 is thirty lines and the SDK ships it
  (`UUIDv7`). A dependency for thirty lines is a licence to audit forever, and
  the alternatives (`gofrs/uuid`, `oklog/ulid`) would each make the thirteen SDKs
  produce different keys for the same logical call.
- **An Arrow library (`apache/arrow-go/v2`)** — `loams.bulk` over Arrow Flight is
  not implemented, because the write RPCs it would bind to arrive with API1 Tasks
  3 and 4 and there is no Flight SQL proto in this repository to generate from.
  Adding a large dependency for an API that does not exist yet would be the worst
  of both.
- **An HTTP/2 wrapper (`imroc/req)`** — `golang.org/x/net` and the standard
  library are enough for what a Connect client does.
