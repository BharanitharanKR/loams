# Graph adjacency index, version 1

A Loams Graph's **adjacency index** is the CSR/CSC of the graph at one offset `N` of its implicit
stream (`_graph.<gr_id>`): every record below `N`, and none at or above it. It is what the graph
algorithms (`pagerank`, `wcc`, `leiden`) run on. Design §48 §26.3 (D1063, D1064), GR1 Task 22.
The codec is `loams_graph::adjacency::format`.

The index is **derived state**. The graph is its stream (I4); deleting an index loses nothing,
and a reader that cannot read one builds it again from the engine. A file is published at
`ns/<ns_id>/graphs/<gr_id>/adj/<N as 20 digits>-<sha256>.adj` with a create-only PUT and named
by the graph manifest's `adjacency` entry, whose `format` is `loams.graph.adjacency/1`.

The `.bin` files here are frozen. `crates/loams-graph/tests/adjacency.rs`
(`adjacency_format_golden_vectors`) decodes each one to the index it names and encodes that index
back to the same bytes. A change that breaks either is a new version, not an edit. Add a case with
`UPDATE_GOLDEN=1 cargo test -p loams-graph --test adjacency adjacency_format_golden_vectors`.

| File | Index |
|---|---|
| `empty.bin` | A graph with no vertices at offset 0 |
| `small.bin` | Five vertices (integer, string and no keys; two labels, one vertex with both), seven edges of two types: parallel edges, a self-loop, weights |
| `isolated.bin` | Vertices and no edges |

## Canonical form

One graph state at one offset has exactly one encoding, however the index was built (a scan of
the engine, or the previous index patched with the stream's records):

- Vertices are in **element-id order**. A vertex's **ordinal** is its rank in that order.
- The edge-type table and the label table hold **exactly the names in use**, each strictly
  increasing by UTF-8 bytes. A type id or a label id is a rank in its table.
- A vertex's label ids are strictly increasing.
- Each vertex's run, in the CSR (out-edges, `other` = destination) and in the CSC (in-edges,
  `other` = source), is strictly increasing by `(type id, other ordinal, edge id)`.
- The CSC is exactly the CSR's transpose. Every edge id appears once.
- An edge's weight is its `weight` property when that is an integer or a finite float, else 1.
  It is never NaN or infinite.

A decoder refuses every other byte string: a wrong magic, a version it does not read, a bad
CRC-32C, a table or run out of order, an unused name, an id or ordinal out of range, a CSC that
is not the transpose, a key tag other than 0, 1 or 2, or bytes after the CRC. Every count is
checked against the bytes left before anything is allocated, so decoding is bounded.

## Layout

All integers are little-endian and fixed width, except the graph id.

```
file        = magic:8 ("LOAMSADJ")  version:u32 (= 1)  graph:id  covered_offset:u64
              edge_types:names  labels:names
              vertex_count:u64  vertex × vertex_count
              edge_count:u64
              csr_offsets:u64 × (vertex_count + 1)  csr_entry × edge_count
              csc_offsets:u64 × (vertex_count + 1)  csc_entry × edge_count
              crc32c:u32                -- CRC-32C (Castagnoli) of every byte before it
id          = 16 bytes: the graph's ULID, big-endian (the gr_<ULID> on the wire)
names       = count:u32  (len:u32 utf8 × len) × count
vertex      = element_id:u64  label_count:u32  label_id:u32 × label_count  key
key         = 0                         -- no _key
            | 1 value:i64               -- an integer _key
            | 2 len:u32 utf8 × len      -- a string _key
csr_entry   = type_id:u32  other_ordinal:u32  edge_id:u64  weight:f64 (IEEE 754 bits)
csc_entry   = type_id:u32  other_ordinal:u32  edge_id:u64
```

- `csr_offsets[0]` is 0, the offsets never decrease, and the last is `edge_count`. Vertex `v`'s
  run is `entries[offsets[v] .. offsets[v + 1]]`. The same holds for the CSC.
- The CSC carries no weights: an in-edge's weight is its CSR entry's.
- A file's SHA-256 (lowercase hex) is its name and the manifest entry's `sha256`; a reader checks
  it before decoding.

## What changes it

The index is built from the stream's `GraphChangeSet` records (`conformance/graph/records/v1`):
`UpsertNode` (labels and `_key`), `DeleteNode`, `UpsertEdge` (type, ends, `weight`),
`DeleteEdge`, and a `SetProps` that sets or removes a vertex's `_key` or an edge's `weight`. Any
other property change, and every `SchemaChange`, leaves it as it is.
