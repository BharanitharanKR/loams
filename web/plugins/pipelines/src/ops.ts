// Pure edits on the builder graph. The page and the canvas only ever change the graph
// through these, so each is tested on its own.

import type { Direction, PipelineGraph, PipelineNode, TransformKind } from './model.js';
import { canConnect } from './schema.js';

export type NewNode =
  | { kind: Direction; ref: string; label: string }
  | { kind: 'transform'; ref: TransformKind; label: string };

const COLUMN = { source: 0, transform: 1, sink: 2 } as const;

/** A fresh id: the ref, then the first free counter (`kafka-1`, `kafka-2`, …). */
export function nextId(graph: PipelineGraph, ref: string): string {
  const base = ref.replace(/[^a-z0-9]+/gi, '-').toLowerCase();
  const taken = new Set(graph.nodes.map((n) => n.id));
  let i = 1;
  while (taken.has(`${base}-${i}`)) i++;
  return `${base}-${i}`;
}

/** Where a palette click puts a node: its kind's column, below what is there. */
export function defaultPosition(graph: PipelineGraph, kind: NewNode['kind']): { x: number; y: number } {
  const inColumn = graph.nodes.filter((n) => n.kind === kind).length;
  return { x: COLUMN[kind] * 300, y: inColumn * 140 };
}

export function addNode(
  graph: PipelineGraph,
  item: NewNode,
  position = defaultPosition(graph, item.kind),
): { graph: PipelineGraph; id: string } {
  const id = nextId(graph, item.ref);
  const sameRef = graph.nodes.filter((n) => n.ref === item.ref).length;
  const node: PipelineNode = {
    id,
    kind: item.kind,
    ref: item.ref,
    label: sameRef ? `${item.label} ${sameRef + 1}` : item.label,
    position,
    config: {},
    secrets: {},
  };
  return { graph: { ...graph, nodes: [...graph.nodes, node] }, id };
}

export function updateNode(graph: PipelineGraph, id: string, patch: Partial<PipelineNode>): PipelineGraph {
  // The id, kind and ref are the node's identity; a patch never changes them.
  const { id: _i, kind: _k, ref: _r, ...rest } = patch;
  return { ...graph, nodes: graph.nodes.map((n) => (n.id === id ? { ...n, ...rest } : n)) };
}

export function moveNode(graph: PipelineGraph, id: string, position: { x: number; y: number }): PipelineGraph {
  if (!Number.isFinite(position.x) || !Number.isFinite(position.y)) return graph;
  return updateNode(graph, id, { position });
}

export function removeNodes(graph: PipelineGraph, ids: readonly string[]): PipelineGraph {
  const gone = new Set(ids);
  return {
    ...graph,
    nodes: graph.nodes.filter((n) => !gone.has(n.id)),
    edges: graph.edges.filter((e) => !gone.has(e.source) && !gone.has(e.target)),
  };
}

export function connect(
  graph: PipelineGraph,
  source: string,
  target: string,
): { graph: PipelineGraph; error?: string } {
  const ok = canConnect(graph, source, target);
  if (!ok.ok) return { graph, error: ok.reason };
  const id = `${source}->${target}`;
  return { graph: { ...graph, edges: [...graph.edges, { id, source, target }] } };
}

export function disconnect(graph: PipelineGraph, edgeIds: readonly string[]): PipelineGraph {
  const gone = new Set(edgeIds);
  return { ...graph, edges: graph.edges.filter((e) => !gone.has(e.id)) };
}
