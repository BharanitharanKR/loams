#!/usr/bin/env python3
"""GraphRAG on Loams: chunk -> entities and relations -> collections -> linked graph ->
Search with `expand` -> answer (GR1 Task 23; design §07 §8, §48 §10).

The pipeline, end to end:

1. **Chunk** each document of a corpus into paragraph chunks.
2. **Extract** entities and relations from each chunk. The default extractor is a small,
   deterministic pattern matcher (`<Entity> leads <Entity>.`), so the example runs in CI
   with no model; an LLM extractor implements the same `Extractor` protocol.
3. **Embed** chunks and entity names with a fixed hashing embedder (no model, no network,
   the same vectors on every machine); a real embedder implements the same `Embedder`
   protocol.
4. **Write** them to three collections of one namespace: `chunks`, `entities` and
   `relations` (the layout of §07 §6's adapters).
5. **Link** a graph over the collections: a LINKED Loams Graph whose vertices are the
   `entities` documents and whose edges are the `relations` documents, typed by their
   `rel_type` field (§07 §2.1). Loams keeps the graph in step with the collections.
6. **Retrieve** with one Search request: the question's vector finds seed entities, and
   the `expand` stage walks up to two hops from them in the graph (§07 §5.2).
7. **Answer** from the retrieved paths. The default answerer is extractive and
   deterministic; an LLM answerer implements the same `Answerer` protocol and gets the
   same context (facts and their source chunks).

Only the Python standard library is used. Run against a local `loams dev` built with the
`graph` feature:

    cargo run -p loams --features graph -- dev
    python3 examples/graphrag/graphrag.py demo
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Protocol, Sequence

HERE = Path(__file__).resolve().parent
DEFAULT_CORPUS = HERE / "corpus"
DEFAULT_QUESTIONS = HERE / "questions.json"
DEFAULT_URL = "http://127.0.0.1:8080"

# The graph's names (the mapping below uses them).
GRAPH = "kg"
VERTEX_LABEL = "entity"
EDGE_NAME = "RELATED"

# ---------------------------------------------------------------------------------------
# Text: tokens, chunks
# ---------------------------------------------------------------------------------------

STOPWORDS = frozenset(
    """a an and are as at be by does did do for from has have he her his how in into is it
    its of on or she that the their them they this to was were what when where which who
    whom whose why with""".split()
)

_WORD = re.compile(r"[A-Za-z0-9]+")


def tokens(text: str) -> list[str]:
    """Lower-cased word tokens without stopwords."""
    return [w for w in (m.group(0).lower() for m in _WORD.finditer(text)) if w not in STOPWORDS]


def slug(name: str) -> str:
    """An entity's key: its name, lower-cased, words joined by `-`."""
    return "-".join(m.group(0).lower() for m in _WORD.finditer(name))


@dataclass(frozen=True)
class Chunk:
    id: str  # `<doc>#<n>`
    doc: str
    text: str


def chunk_document(doc: str, text: str) -> list[Chunk]:
    """One chunk per paragraph; headings are dropped, whitespace inside a paragraph is
    folded to single spaces."""
    chunks = []
    for paragraph in re.split(r"\n\s*\n", text):
        lines = [line for line in paragraph.strip().splitlines() if not line.startswith("#")]
        body = " ".join(" ".join(lines).split())
        if body:
            chunks.append(Chunk(id=f"{doc}#{len(chunks)}", doc=doc, text=body))
    return chunks


def load_corpus(directory: Path) -> list[Chunk]:
    """Every `*.md` document of `directory`, in name order, chunked."""
    chunks: list[Chunk] = []
    for path in sorted(directory.glob("*.md")):
        chunks.extend(chunk_document(path.stem, path.read_text(encoding="utf-8")))
    return chunks


# ---------------------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Relation:
    src: str  # entity name
    rel_type: str
    dst: str
    chunk: str  # the chunk it was read from

    @property
    def id(self) -> str:
        """`src--TYPE--dst`, from the endpoint keys (§07 §6's `src ⟂ type ⟂ dst`)."""
        return f"{slug(self.src)}--{self.rel_type}--{slug(self.dst)}"


class Extractor(Protocol):
    def extract(self, chunk: Chunk) -> list[Relation]: ...


# Relation phrases, the type each becomes, and the question words that ask for it.
RELATION_PHRASES: dict[str, tuple[str, frozenset[str]]] = {
    "leads": ("LEADS", frozenset({"lead", "leads", "led", "leader"})),
    "maintains": ("MAINTAINS", frozenset({"maintain", "maintains", "maintained", "maintainer"})),
    "works with": ("WORKS_WITH", frozenset({"works", "work", "colleague", "colleagues"})),
    "is written in": ("WRITTEN_IN", frozenset({"written", "language"})),
    "depends on": ("DEPENDS_ON", frozenset({"depend", "depends", "dependency"})),
    "is part of": ("PART_OF", frozenset({"part", "team", "belong", "belongs"})),
    "uses": ("USES", frozenset({"use", "uses", "used", "using"})),
}
QUESTION_WORDS: dict[str, frozenset[str]] = {t: words for t, words in RELATION_PHRASES.values()}
TYPE_PHRASE: dict[str, str] = {t: phrase for phrase, (t, _) in RELATION_PHRASES.items()}

_ENTITY = r"[A-Z][A-Za-z0-9]*(?: [A-Z][A-Za-z0-9]*)*"
_CLAUSE = re.compile(
    rf"^(?P<src>{_ENTITY}) (?P<phrase>{'|'.join(sorted(map(re.escape, RELATION_PHRASES), key=len, reverse=True))}) (?P<dst>{_ENTITY})\b"
)


class PatternExtractor:
    """Reads `<Entity> <phrase> <Entity>` clauses, an entity being a run of capitalized
    words. Deterministic, so CI needs no model; swap in an LLM extractor for real text."""

    def extract(self, chunk: Chunk) -> list[Relation]:
        relations = []
        for sentence in re.split(r"(?<=[.!?])\s+", chunk.text):
            for clause in re.split(r",\s*(?:and\s+)?|;\s*", sentence.rstrip(".!?")):
                match = _CLAUSE.match(clause.strip())
                if match:
                    rel_type, _ = RELATION_PHRASES[match["phrase"]]
                    relations.append(Relation(match["src"], rel_type, match["dst"], chunk.id))
        return relations


# ---------------------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------------------


class Embedder(Protocol):
    dim: int

    def embed(self, text: str) -> list[float]: ...


class HashingEmbedder:
    """A fixed, model-free embedder: each token is hashed (BLAKE2b) to a dimension and a
    sign, and the vector is L2-normalized. Texts that share words are similar; the same
    text has the same vector on every machine."""

    def __init__(self, dim: int = 256) -> None:
        self.dim = dim

    def embed(self, text: str) -> list[float]:
        vector = [0.0] * self.dim
        for token in tokens(text):
            digest = hashlib.blake2b(token.encode(), digest_size=8).digest()
            index = int.from_bytes(digest[:4], "little") % self.dim
            vector[index] += 1.0 if digest[4] & 1 else -1.0
        norm = math.sqrt(sum(v * v for v in vector))
        return [round(v / norm, 6) for v in vector] if norm else vector


# ---------------------------------------------------------------------------------------
# The index: what goes into the collections
# ---------------------------------------------------------------------------------------


@dataclass
class Entity:
    name: str
    chunks: list[str] = field(default_factory=list)

    @property
    def key(self) -> str:
        return slug(self.name)


@dataclass
class Index:
    chunks: list[Chunk]
    entities: dict[str, Entity]  # by key
    relations: list[Relation]  # unique by id, first mention wins

    def chunk_entities(self, chunk_ids: Iterable[str]) -> list[str]:
        """The entities the chunks mention, in chunk order, each once."""
        out: list[str] = []
        for chunk in chunk_ids:
            for key, entity in self.entities.items():
                if chunk in entity.chunks and key not in out:
                    out.append(key)
        return out


def build_index(chunks: Sequence[Chunk], extractor: Extractor) -> Index:
    entities: dict[str, Entity] = {}
    relations: dict[str, Relation] = {}
    for chunk in chunks:
        for relation in extractor.extract(chunk):
            for name in (relation.src, relation.dst):
                entity = entities.setdefault(slug(name), Entity(name))
                if chunk.id not in entity.chunks:
                    entity.chunks.append(chunk.id)
            relations.setdefault(relation.id, relation)
    return Index(list(chunks), entities, list(relations.values()))


# ---------------------------------------------------------------------------------------
# Loams over HTTP (REST for collections and search, Connect JSON for graphs)
# ---------------------------------------------------------------------------------------


class LoamsError(RuntimeError):
    def __init__(self, status: int, body: object) -> None:
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


class Loams:
    """A minimal client: JSON POSTs and GETs, no dependencies."""

    def __init__(self, url: str = DEFAULT_URL, timeout: float = 30.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    def call(self, method: str, path: str, body: object | None = None) -> object:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            self.url + path, data=data, method=method, headers={"content-type": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                text = response.read().decode()
        except urllib.error.HTTPError as err:
            text = err.read().decode()
            try:
                detail: object = json.loads(text)
            except ValueError:
                detail = text
            raise LoamsError(err.code, detail) from None
        return json.loads(text) if text else None

    def post(self, path: str, body: object) -> object:
        return self.call("POST", path, body)

    def connect(self, rpc: str, body: object) -> object:
        """A Connect unary call with a JSON body (`/<package>.<Service>/<Method>`)."""
        return self.post("/" + rpc, body)


def _keyword(name: str) -> dict:
    return {"name": name, "source_path": name, "kind": "keyword", "indexed": True, "fast": True}


def _schema(fields: list[dict], vectors: list[dict]) -> dict:
    return {"fields": fields, "vectors": vectors, "sparse_vectors": [], "dynamic": "ignore", "max_fields": 100}


def graph_mapping() -> dict:
    """The LINKED graph over the collections (§07 §2.1, as the Connect API takes it):

        CREATE GRAPH kg
          VERTEX entity  KEY (_pk)                                  FROM COLLECTION entities
          EDGE   RELATED SOURCE entity(src) DESTINATION entity(dst) TYPE FROM (rel_type) FROM COLLECTION relations;
    """
    return {
        "vertices": [{"label": VERTEX_LABEL, "collection": "entities", "keyField": "_pk"}],
        "edges": [
            {
                "type": EDGE_NAME,
                "collection": "relations",
                "sourceLabel": VERTEX_LABEL,
                "sourceField": "src",
                "destinationLabel": VERTEX_LABEL,
                "destinationField": "dst",
                "typeField": "rel_type",
            }
        ],
    }


def write_index(loams: Loams, namespace: str, index: Index, embedder: Embedder, link_timeout: float = 60.0) -> None:
    """Creates the namespace, the three collections and the linked graph, writes the
    documents, and waits until the graph holds every relation."""
    loams.post("/v1/namespaces", {"name": namespace})
    vector = [{"name": "embedding", "dim": embedder.dim, "distance": "cosine"}]
    collections = {
        "chunks": _schema([_keyword("doc")], vector),
        "entities": _schema([_keyword("name")], vector),
        "relations": _schema([_keyword("src"), _keyword("dst"), _keyword("rel_type")], []),
    }
    for name, schema in collections.items():
        loams.post(f"/v1/namespaces/{namespace}/collections", {"name": name, "schema": schema, "partitions": 1})

    def write(collection: str, ops: list[dict]) -> None:
        loams.post(f"/v1/namespaces/{namespace}/collections/{collection}/documents", {"ops": ops})

    write(
        "chunks",
        [
            {"upsert": {"id": c.id, "source": {"doc": c.doc, "text": c.text}, "vectors": {"embedding": embedder.embed(c.text)}}}
            for c in index.chunks
        ],
    )
    # Vertices first (§07 §7), though the link would make stubs for edges written first.
    write(
        "entities",
        [
            {"upsert": {"id": e.key, "source": {"name": e.name, "chunks": e.chunks}, "vectors": {"embedding": embedder.embed(e.name)}}}
            for e in index.entities.values()
        ],
    )
    write(
        "relations",
        [
            {"upsert": {"id": r.id, "source": {"src": slug(r.src), "dst": slug(r.dst), "rel_type": r.rel_type, "chunk": r.chunk}}}
            for r in index.relations
        ],
    )
    loams.connect(
        "loams.graph.v1.GraphAdminService/CreateGraph",
        {"namespace": namespace, "name": GRAPH, "mode": "GRAPH_MODE_LINKED", "mapping": graph_mapping()},
    )
    wait_for_links(loams, namespace, len(index.relations), link_timeout)


def wait_for_links(loams: Loams, namespace: str, edges: int, timeout: float) -> None:
    """The links apply on the server's schedule: poll the graph's edge count with GQL."""
    deadline = time.monotonic() + timeout
    last: object = None
    while True:
        try:
            answer = loams.connect(
                "loams.graph.v1.GraphService/Execute",
                {"namespace": namespace, "graph": GRAPH, "statement": "MATCH ()-[e]->() RETURN count(e)"},
            )
            last = answer
            value = answer["rows"]["rows"][0]["values"][0]["int64"]  # type: ignore[index]
            if int(value) >= edges:
                return
        except (LoamsError, KeyError, IndexError, TypeError) as err:
            last = err
        if time.monotonic() > deadline:
            raise TimeoutError(f"the graph's links did not apply {edges} edges in {timeout} s: {last}")
        time.sleep(0.2)


# ---------------------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Step:
    """One hop of a path: from `via` over `edge` (a relation id) to `key`."""

    via: str
    edge_type: str
    edge: str
    key: str


@dataclass
class Retrieval:
    seeds: list[str]  # seed entity keys, best first
    neighbors: list[dict]  # the expand stage's neighbors, in answer order, with `seed` added
    names: dict[str, str]  # entity key -> name, for every key retrieved

    @property
    def entities(self) -> list[str]:
        """Every entity retrieved, seeds first, each once."""
        out = list(self.seeds)
        for n in self.neighbors:
            if n["key"] not in out:
                out.append(n["key"])
        return out

    def path(self, seed: str, key: str) -> list[Step]:
        """The path the expansion took from `seed` to `key` (empty for the seed)."""
        by_key = {n["key"]: n for n in self.neighbors if n["seed"] == seed}
        steps: list[Step] = []
        while key != seed and key in by_key:
            n = by_key[key]
            steps.append(Step(n["via"], n["edge_type"], n["edge"], key))
            key = n["via"]
        return list(reversed(steps))


def _key(value: object) -> str:
    """A key as the REST API answers it: a string, or an integer as its digits."""
    return value if isinstance(value, str) else json.dumps(value)


def search_body(question_vector: list[float], seeds: int = 3, hops: int = 2, score_threshold: float = 0.2) -> dict:
    """The one Search request: vector seeds on `entities`, then `expand` in the graph.
    Undirected (`both`), as LightRAG's graph is (§07 §6)."""
    return {
        "collection": "entities",
        "retrievers": [{"vector": {"field": "embedding", "query": question_vector, "k": seeds, "params": {"exact": True}}}],
        "limit": seeds,
        "score_threshold": score_threshold,
        "select": {"source": "all", "vectors": [], "fields": []},
        "expand": {
            "graph": GRAPH,
            "seed_label": VERTEX_LABEL,
            "seeds": seeds,
            "hops": hops,
            "direction": "both",
            "limit_per_seed": 50,
            "limit": 100,
            "select": {"source": "all", "vectors": [], "fields": []},
        },
    }


def retrieve(loams: Loams, namespace: str, embedder: Embedder, question: str, seeds: int = 3, hops: int = 2) -> Retrieval:
    answer = loams.post(f"/v1/namespaces/{namespace}/query", search_body(embedder.embed(question), seeds, hops))
    seed_keys: list[str] = []
    neighbors: list[dict] = []
    names: dict[str, str] = {}
    for hit in answer["hits"]:  # type: ignore[index]
        seed = _key(hit.get("id", hit.get("pk")))
        seed_keys.append(seed)
        names[seed] = (hit.get("source") or {}).get("name", seed)
        for n in hit.get("neighbors") or []:
            entry = {
                "seed": seed,
                "key": _key(n["key"]),
                "hop": n["hop"],
                "via": _key(n["via"]),
                "edge_type": n["edge_type"],
                "edge": _key(n["edge"]),
                "score": n["score"],
            }
            neighbors.append(entry)
            names[entry["key"]] = (n.get("source") or {}).get("name", entry["key"])
    return Retrieval(seed_keys, neighbors, names)


def retrieve_naive(loams: Loams, namespace: str, embedder: Embedder, question: str, k: int = 3) -> list[str]:
    """The naive reference pipeline (§07 §9): vector search over the chunks, no graph.
    Answers the top `k` chunk ids; `Index.chunk_entities` names the entities they mention."""
    answer = loams.post(
        f"/v1/namespaces/{namespace}/query",
        {
            "collection": "chunks",
            "retrievers": [{"vector": {"field": "embedding", "query": embedder.embed(question), "k": k, "params": {"exact": True}}}],
            "limit": k,
            "select": {"source": "all", "vectors": [], "fields": []},
        },
    )
    return [_key(h.get("id", h.get("pk"))) for h in answer["hits"]]  # type: ignore[index]


# ---------------------------------------------------------------------------------------
# Answering
# ---------------------------------------------------------------------------------------


@dataclass
class Answer:
    text: str  # the answer, or "" when none was found
    facts: list[str]  # the supporting path as sentences
    chunks: list[str]  # the chunks the supporting relations were read from


class Answerer(Protocol):
    def answer(self, question: str, retrieval: Retrieval, relations: dict[str, dict]) -> Answer: ...


def fact(relation: dict, names: dict[str, str]) -> str:
    src, dst = relation["src"], relation["dst"]
    return f"{names.get(src, src)} {TYPE_PHRASE.get(relation['rel_type'], relation['rel_type'])} {names.get(dst, dst)}"


class PathAnswerer:
    """Extractive and deterministic. The question's words name relation types ("written"
    asks for WRITTEN_IN, "leads" for LEADS); the answer is the far end of the longest
    expansion path whose every edge is a distinct asked-for type, ending at an entity the
    question does not name; the earliest such path in the expansion's own order (best seed
    first) breaks ties. An LLM answerer would get the same context."""

    def answer(self, question: str, retrieval: Retrieval, relations: dict[str, dict]) -> Answer:
        words = set(tokens(question))
        asked = {t for t, qw in QUESTION_WORDS.items() if words & qw}
        best: tuple[str, list[Step]] | None = None
        for n in retrieval.neighbors:
            if set(tokens(retrieval.names.get(n["key"], n["key"]))) <= words:
                continue  # an entity the question names is not its answer
            steps = retrieval.path(n["seed"], n["key"])
            types = [s.edge_type for s in steps]
            if not steps or len(set(types)) != len(types) or not set(types) <= asked:
                continue
            if best is None or len(steps) > len(best[1]):
                best = (n["key"], steps)
        if best is None:
            return Answer("", [], [])
        key, steps = best
        used = [relations[s.edge] for s in steps if s.edge in relations]
        chunks: list[str] = []
        for r in used:
            if r.get("chunk") and r["chunk"] not in chunks:
                chunks.append(r["chunk"])
        return Answer(retrieval.names.get(key, key), [fact(r, retrieval.names) for r in used], chunks)


def fetch_relations(loams: Loams, namespace: str, ids: Iterable[str]) -> dict[str, dict]:
    """The relation documents the expansion walked, by id."""
    wanted = sorted(set(ids))
    if not wanted:
        return {}
    answer = loams.post(
        f"/v1/namespaces/{namespace}/query",
        {"collection": "relations", "filter": {"ids": wanted}, "limit": len(wanted), "select": {"source": "all", "vectors": [], "fields": []}},
    )
    return {_key(h.get("id", h.get("pk"))): h["source"] for h in answer["hits"]}  # type: ignore[index]


# ---------------------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------------------


@dataclass
class GraphRAG:
    loams: Loams
    namespace: str = "graphrag"
    embedder: Embedder = field(default_factory=HashingEmbedder)
    extractor: Extractor = field(default_factory=PatternExtractor)
    answerer: Answerer = field(default_factory=PathAnswerer)

    def index(self, corpus: Path = DEFAULT_CORPUS) -> Index:
        built = build_index(load_corpus(corpus), self.extractor)
        write_index(self.loams, self.namespace, built, self.embedder)
        return built

    def retrieve(self, question: str) -> Retrieval:
        return retrieve(self.loams, self.namespace, self.embedder, question)

    def ask(self, question: str) -> Answer:
        retrieval = self.retrieve(question)
        relations = fetch_relations(self.loams, self.namespace, (n["edge"] for n in retrieval.neighbors))
        return self.answerer.answer(question, retrieval, relations)


def load_questions(path: Path = DEFAULT_QUESTIONS) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default=DEFAULT_URL, help=f"Loams' HTTP address (default {DEFAULT_URL})")
    parser.add_argument("--namespace", default="graphrag")
    sub = parser.add_subparsers(dest="command", required=True)
    index_cmd = sub.add_parser("index", help="index a corpus of *.md files")
    index_cmd.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    ask_cmd = sub.add_parser("ask", help="answer one question")
    ask_cmd.add_argument("question")
    demo = sub.add_parser("demo", help="index the example corpus and answer its questions")
    demo.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    demo.add_argument("--questions", type=Path, default=DEFAULT_QUESTIONS)
    args = parser.parse_args(argv)

    rag = GraphRAG(Loams(args.url), args.namespace)
    if args.command in ("index", "demo"):
        built = rag.index(args.corpus)
        print(f"indexed {len(built.chunks)} chunks, {len(built.entities)} entities, {len(built.relations)} relations")
    questions = [{"question": args.question}] if args.command == "ask" else []
    if args.command == "demo":
        questions = load_questions(args.questions)
    for q in questions:
        answer = rag.ask(q["question"])
        print(f"Q: {q['question']}\nA: {answer.text or '(no answer)'}")
        for f in answer.facts:
            print(f"   - {f}")
        if answer.chunks:
            print(f"   sources: {', '.join(answer.chunks)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
