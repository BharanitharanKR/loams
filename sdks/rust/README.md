# The Loams SDK for Rust (`loams`)

**0.1.0.** `loams` 0.1.0 tracks Loams 0.1.0. In 0.x the API may change between
minor versions; `loams = "0.1"` stays on the 0.1 line.

One `Loams` client with namespaced modules over the unified Connect API,
generated from the protos in [`proto/`](https://github.com/ostrium-labs/loams/tree/dev/proto).

```toml
[dependencies]
loams = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,ignore
use std::sync::Arc;
use loams::{ApiKey, CallOptionsOverrides, GetInstanceRequest, Loams, TransportOptions};

let loams = Loams::connect(TransportOptions::new("http://127.0.0.1:8080"))?
    .with_auth(Arc::new(ApiKey::new(std::env::var("LOAMS_API_KEY").unwrap_or_default())?));

// The first call any client makes: what this instance is, and no auth.
let info = loams.instance().get_instance(GetInstanceRequest::default(), CallOptionsOverrides::new()).await?;
println!("{} serves {:?}", info.name, info.api_versions);
```

## Building

The generated message and client code comes from `loams-proto` and
`loams-live-proto`, which generate it at build time with `protoc`. Install it
first (`apt install protobuf-compiler`, `brew install protobuf`, or a release
from <https://github.com/protocolbuffers/protobuf/releases>).

The default `tls` feature brings rustls with the webpki roots, for `https://`
endpoints. A loopback `http://` endpoint, which is what `loams dev` serves,
needs none of it: `default-features = false`.

## Licence

Apache-2.0. Source, issues and the other SDKs:
<https://github.com/ostrium-labs/loams/tree/dev/sdks>.
