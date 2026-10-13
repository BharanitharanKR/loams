# Prometheus

Prometheus runs on the native protocols family (`runtime.ref:
loams_connector_protocols::prometheus`; CN2 Task 21). Both directions speak
[remote write 1.0](https://prometheus.io/docs/specs/prw/remote_write_spec/): snappy
(block format) compressed `prometheus.WriteRequest` protobuf.

| | |
|---|---|
| Tested against | Prometheus v3.15.0 (`prom/prometheus`, Apache-2.0) in CI's `connectors-p2` stack, as receiver of the sink and as sender to the receiver (`deploy/fabric/emulators/prometheus.yml`) |
| Source | a remote-write receiver on `ingest`'s listener |
| Sink | remote write |
| Auth | `basic`, `key` (bearer) on the sink; the receiver is loopback-only until Q30 (D111, OR-17) |

## Receiver (source)

`POST /v1/namespaces/{ns}/fabric/prometheus/{instance}/write`. Each series in a request becomes
one `io.loams.dev.flow.prometheus.metric.v1` event: `data` is `{labels, samples: [{value,
timestamp}]}`, `subject` is `name{label="value",…}`, and the id is a digest of the labels and
samples, so a request Prometheus resends is deduplicated by `ingest`.

The receiver answers only after the Fabric acknowledged: 204, or 503 on a transient Fabric
error (Prometheus retries), 429 with `Retry-After` when throttled, 400 for a body that is not
remote write 1.0 (Prometheus drops it), 415 for remote write 2.0. Until FL1's `ingest` exists,
the runtime mounts `RemoteWriteReceiver::receive` itself (ruling R21.7).

Point Prometheus at it:

```yaml
remote_write:
  - url: http://loams-fabric:8080/v1/namespaces/1/fabric/prometheus/prom/write
```

## Sink

Metric events (the receiver's shape, or one sample as `{labels, value, timestamp}`; `NaN`,
`+Inf`, `-Inf` as text) are sent in requests of at most `batch` series, labels sorted. A
missing timestamp is now. Prometheus accepts an identical sample twice, so a retried batch is
harmless; an out-of-order sample is a 400 and goes to the DLQ.

```yaml
url: https://prometheus.internal/api/v1/write
basic: { username: loams, password: prom-password }   # or bearer: prom-token
batch: 500
```
