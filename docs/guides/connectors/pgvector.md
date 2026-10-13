# pgvector connector

pgvector is PostgreSQL with the `vector` extension, served by the native Postgres family through the target profile `pgvector`. Everything in the [PostgreSQL guide](postgresql.md) applies. The difference is the type:

| Postgres | Arrow (bulk path) | JSON (CDC events, sink input) |
|---|---|---|
| `vector(n)` | `FixedSizeList<Float32>(n)` | an array of `n` numbers |
| `vector` (no dimension) | `List<Float32>` | an array |

Batch reads decode pgvector's binary form, and the sink writes a vector as its text form (`[1,2.5,3]`) cast to the column's type, so an Arrow batch read from one table can be written to another unchanged (`pgvector_roundtrip_fixed_size_list`). `halfvec` and `sparsevec` are not mapped and pause the instance.

**Tested against:** `pgvector/pgvector:0.8.6-pg17` (PostgreSQL licence, compose service `pgvector`, profile `connectors-p2`). Loams Postgres may be tested as an extra target after it, never instead of it (OR-2).
