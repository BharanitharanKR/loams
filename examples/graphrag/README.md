# GraphRAG on Loams

A small, complete GraphRAG pipeline on Loams Graph (design §07 §8, §48 §10), in one Python
file with no dependencies beyond the standard library:

1. **Chunk** the documents of `corpus/` into paragraphs.
2. **Extract** entities and relations from each chunk.
3. **Write** them to three collections: `chunks`, `entities` (one per entity, keyed by its
   name's slug, with a vector of its name) and `relations` (`src`, `dst`, `rel_type`, and
   the chunk it was read from).
4. **Link** a graph over them: a LINKED Loams Graph whose vertices are `entities` and
   whose edges are `relations`, typed by `rel_type`. Loams keeps it in step with the
   collections; nothing writes to the graph directly.
5. **Retrieve** with one `Search` request: the question's vector finds seed entities, and
   the `expand` stage walks two hops from them in the graph.
6. **Answer** from the retrieved paths, citing the chunks the facts came from.

The graph's mapping is §07 §2.1's declaration:

```sql
CREATE GRAPH kg
  VERTEX entity  KEY (_pk)                                  FROM COLLECTION entities
  EDGE   RELATED SOURCE entity(src) DESTINATION entity(dst) TYPE FROM (rel_type) FROM COLLECTION relations;
```

and the retrieval is one request to `POST /v1/namespaces/<ns>/query`:

```json
{
  "collection": "entities",
  "retrievers": [{"vector": {"field": "embedding", "query": [...], "k": 3}}],
  "limit": 3,
  "score_threshold": 0.2,
  "expand": {"graph": "kg", "seed_label": "entity", "seeds": 3, "hops": 2,
             "direction": "both", "select": {"source": "all"}}
}
```

## Run it

```sh
cargo run -p loams --features graph -- dev        # Loams on http://127.0.0.1:8080
python3 examples/graphrag/graphrag.py demo        # index corpus/ and answer questions.json
python3 examples/graphrag/graphrag.py ask "Who leads Wren?"
```

`demo` prints, for each question, the answer, the facts on the path that led to it, and
their source chunks:

```text
Q: What language is the project that Ada Park leads written in?
A: Rust
   - Ada Park leads Kite
   - Kite is written in Rust
   sources: people#1, kite#0
```

Run it again with another `--namespace`: each run creates its namespace.

## What is deliberately simple

The example must give the same answers on every machine, so CI can check them. Three
parts are stand-ins, each behind a small protocol so a real one drops in:

| Part | Here | In an application |
|---|---|---|
| `Extractor` | `PatternExtractor`: `<Entity> <phrase> <Entity>` clauses, an entity being a run of capitalized words | An LLM prompted for entities and relations (Microsoft GraphRAG, LightRAG) |
| `Embedder` | `HashingEmbedder`: each word hashed to one of 256 dimensions, normalized | A sentence-embedding model |
| `Answerer` | `PathAnswerer`: the far end of the longest expansion path whose edge types the question's words ask for | An LLM given the same facts and chunks |

The corpus is fictional and written for this example (Apache-2.0, as Loams).

## Tests

```sh
python3 -m unittest discover -s examples/graphrag/tests -v            # offline tests
LOAMS_URL=http://127.0.0.1:8080 python3 -m unittest discover -s examples/graphrag/tests -v
```

With `LOAMS_URL`, `test_example_runs_end_to_end_in_ci` indexes the corpus into a fresh
namespace and checks every question's answer, facts and sources against `questions.json`,
twice. CI's Graph job runs it against `loams dev`.

The retrieval benchmark harness that measures this pipeline is
[`bench/graph/graphrag/`](../../bench/graph/graphrag/).
