![Loams — Your data. Your bucket.](docs/assets/loams-banner.svg)

# Loams

**One bucket, every index: hybrid retrieval on object storage, with reactive data and durable agent runs beside it.**

Loams is an open-source, AI-native data platform in Rust. It keeps your data in *your* object-storage bucket (S3, GCS, Azure Blob, RustFS or a local directory) in open formats: Lance and Tantivy today, Iceberg next. Compute is stateless and holds only caches.

> **Preview (v0.1.0-preview.2).** You run the stack yourself with Docker Compose, locally or on your own VMs, over your own Tailscale tailnet, and use the hosted console at [console.loams.dev](https://console.loams.dev). Not production ready: APIs, formats and flags can change between previews.

[Run the preview](docs/release/preview.md) · [Documentation](docs/README.md) · [Website](https://loams.dev) · [API reference](https://loams.dev/api-docs) · [Contributing](CONTRIBUTING.md)

## Why Loams

A typical AI application runs Elasticsearch, Qdrant, a graph database, Kafka and a workflow engine: five stateful clusters, several copies of the same data, and retrieval glued together in application code. Loams replaces them with one engine on one bucket:

- **Your bucket is the source of truth.** Object-storage prices, no 3× block-storage replication; losing a node loses no data.
- **The log is the spine.** Every write lands in a log; collections are materializations of it, and every write returns a consistency token later reads can wait on.
- **Hybrid retrieval in one plan.** Vectors, BM25 and filters fused with RRF in one DataFusion query, with hot tiers in RAM and NVMe.
- **Drop-in APIs.** Existing Qdrant and Elasticsearch clients (and LangChain and LlamaIndex on top of them) work unmodified, plus Arrow Flight SQL.
- **More than retrieval.** Reactive documents (Live), graphs (Graph), analytics (House), MySQL and Postgres, and durable agent runs (Durable), on the same storage.

Read the [pitch](docs/design/00-pitch.md) and the [architecture](docs/design/01-architecture.md).

<p align="center">
  <img src="docs/assets/architecture.svg" alt="Loams architecture: clients reach a stateless gateway and router; behind it the retrieval engine, Live, Durable, the runtime and jobs, and Postgres; TiKV holds metadata and transactions; object storage holds all data in open formats." width="100%">
</p>

## What ships in the preview

| | |
|---|---|
| **Retrieval engine** | Collections, hybrid search, SQL, streams and links; native REST, Arrow Flight SQL, Qdrant and Elasticsearch APIs; Postgres and MySQL wire |
| **Loams Live** | A reactive document database: transactions, indexes and live queries |
| **Loams Graph** | GQL graphs, with writes on the Loams log and crash recovery |
| **Loams House** | ClickHouse-compatible analytics with engine-level access control (opt-in profile) |
| **Loams SQL / Postgres** | MySQL on a patched TiDB; Postgres on our Neon fork, storage on the bucket |
| **Loams Durable** | Durable promises and tasks for agent runs (an embedded Resonate server) |
| **Console, desktop, CLI** | Hosted web console; Loams Desktop and the `loams` CLI for Linux, macOS and Windows |
| **Loams Studio** | An agentic development environment in the console's Studio mode: coding agents (Claude Code, Codex, opencode and others) in threads with a git worktree each, diff review, git actions, terminals, previews and parallel runs, all through the `loams-agentd` daemon. On by default in Loams Desktop; in the browser it runs against a remote `loams-agentd` and is behind a feature flag ([guides](docs/guides/studio/index.md)) |
| **SDKs** | TypeScript, Python, Go, Rust, C#, C++ and Swift (more languages coming) |

**Coming next:** Loams Git (Git on the bucket), first-party connectors, graph expansion for GraphRAG, Iceberg analytics, Kafka wire, functions and jobs. The full status table is in [Running the preview](docs/release/preview.md); the plan is in the [roadmap](docs/design/12-roadmap-testing-risks.md).

## Quick start

Run the whole stack (needs Docker Compose and a [Tailscale](https://tailscale.com) auth key):

```sh
git clone https://github.com/ostrium-labs/loams.git
cd loams/deploy/release
cp .env.example .env        # set TS_AUTHKEY
docker compose up -d
```

Then open [console.loams.dev](https://console.loams.dev) on a device on the same tailnet and enter `https://loams.<your-tailnet>.ts.net`. Details, BYOC over SSH and auth modes: [Running the preview](docs/release/preview.md).

Or build just the engine and run it on one machine:

```sh
cargo run --release -p loams -- dev    # native API on 127.0.0.1:8080, Qdrant on :6333, Elasticsearch on :9200
```

Examples for the native API, Qdrant clients, Flight SQL and reading the bucket directly are in the [engine quick start](docs/engine-quickstart.md).

## Repository

The engine crates, the web console, Loams Desktop, the mobile apps, SDKs, protos, connectors and deploy files all live here; see the [project layout](docs/project-layout.md) and the [monorepo guide](docs/monorepo.md). Design documents are in [`docs/design`](docs/design/README.md) and task-by-task plans in [`docs/plans`](docs/plans/README.md).

## Contributing and community

Bug reports, client compatibility reports, docs fixes, tests and code are all welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md) and issues labelled [`good first issue`](https://github.com/ostrium-labs/loams/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22); open an issue before changing a format, protocol or design decision.

[Discussions](https://github.com/ostrium-labs/loams/discussions) · [Issues](https://github.com/ostrium-labs/loams/issues) · [Security](SECURITY.md) · [Code of Conduct](CODE_OF_CONDUCT.md) · [Governance](GOVERNANCE.md) · [Ecosystem](ECOSYSTEM.md) · [Privacy](PRIVACY.md)

## License

[Apache License 2.0](LICENSE). Loams builds on DataFusion, Lance, Tantivy, Quickwit, qdrant-edge, SlateDB, openraft, TiKV, TiDB, Neon, Grafeo, chDB and Resonate; attributions are in [NOTICE](NOTICE). Everything needed to self-host Loams is open source here; only metering, billing and the commercial APIs are proprietary (open core).
