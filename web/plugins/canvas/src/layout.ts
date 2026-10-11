// A small layered (Sugiyama-style) layout, left to right, written here so the canvas adds
// no layout dependency: break cycles, rank by longest path, order each layer by barycentre
// sweeps, then place layers in columns with each column centred on the tallest one.

import type { Position } from './client.js';

export const NODE_WIDTH = 232;
export const NODE_HEIGHT = 112;

export interface LayoutOptions {
  nodeWidth?: number;
  nodeHeight?: number;
  /** Horizontal gap between layers. */
  rankGap?: number;
  /** Vertical gap between nodes in a layer. */
  nodeGap?: number;
  /** Ordering sweeps; more untangles more, at more cost. */
  sweeps?: number;
}

export interface LayoutEdge {
  source: string;
  target: string;
}

/**
 * Positions (top-left corners) for every id in `ids`. Edges naming an unknown id are
 * ignored. Deterministic: the same input always gives the same output, and ties keep
 * `ids` order.
 */
export function layeredLayout(
  ids: readonly string[],
  edges: readonly LayoutEdge[],
  opts: LayoutOptions = {},
): Record<string, Position> {
  const width = opts.nodeWidth ?? NODE_WIDTH;
  const height = opts.nodeHeight ?? NODE_HEIGHT;
  const rankGap = opts.rankGap ?? 96;
  const nodeGap = opts.nodeGap ?? 40;
  const sweeps = opts.sweeps ?? 4;

  const known = new Set(ids);
  const index = new Map(ids.map((id, i) => [id, i]));
  const out = new Map<string, string[]>(ids.map((id) => [id, []]));
  for (const e of edges) {
    if (e.source === e.target || !known.has(e.source) || !known.has(e.target)) continue;
    const list = out.get(e.source) as string[];
    if (!list.includes(e.target)) list.push(e.target);
  }

  // 1. Break cycles: a DFS in `ids` order drops each edge back onto the stack.
  const dag = acyclic(ids, out);

  // 2. Rank by longest path from a source.
  const incoming = new Map<string, string[]>(ids.map((id) => [id, []]));
  for (const [s, ts] of dag) for (const t of ts) (incoming.get(t) as string[]).push(s);
  const rank = new Map<string, number>();
  const rankOf = (id: string, seen = new Set<string>()): number => {
    const done = rank.get(id);
    if (done !== undefined) return done;
    if (seen.has(id)) return 0;
    seen.add(id);
    let r = 0;
    for (const p of incoming.get(id) as string[]) r = Math.max(r, rankOf(p, seen) + 1);
    rank.set(id, r);
    return r;
  };
  for (const id of ids) rankOf(id);

  // 3. Layers, ordered by barycentre of neighbours in the previous (or next) layer.
  const depth = Math.max(0, ...rank.values()) + 1;
  const layers: string[][] = Array.from({ length: depth }, () => []);
  for (const id of ids) (layers[rank.get(id) as number] as string[]).push(id);
  const pos = new Map<string, number>();
  const reindex = (layer: string[]) => layer.forEach((id, i) => pos.set(id, i));
  layers.forEach(reindex);
  const sortBy = (layer: string[], neighbours: (id: string) => string[]) => {
    const key = new Map<string, number>();
    for (const id of layer) {
      const ns = neighbours(id).filter((n) => pos.has(n));
      key.set(
        id,
        ns.length ? ns.reduce((a, n) => a + (pos.get(n) as number), 0) / ns.length : (pos.get(id) as number),
      );
    }
    layer.sort(
      (a, b) =>
        (key.get(a) as number) - (key.get(b) as number) ||
        (index.get(a) as number) - (index.get(b) as number),
    );
    reindex(layer);
  };
  for (let s = 0; s < sweeps; s++) {
    for (let r = 1; r < depth; r++) {
      sortBy(layers[r] as string[], (id) =>
        (incoming.get(id) as string[]).filter((p) => rank.get(p) === r - 1),
      );
    }
    for (let r = depth - 2; r >= 0; r--) {
      sortBy(layers[r] as string[], (id) =>
        (dag.get(id) as string[]).filter((c) => rank.get(c) === r + 1),
      );
    }
  }

  // 4. Coordinates: columns by rank, each column centred on the tallest.
  const tallest = Math.max(...layers.map((l) => l.length), 1);
  const span = (n: number) => n * height + Math.max(0, n - 1) * nodeGap;
  const result: Record<string, Position> = {};
  layers.forEach((layer, r) => {
    const top = (span(tallest) - span(layer.length)) / 2;
    layer.forEach((id, i) => {
      result[id] = { x: r * (width + rankGap), y: top + i * (height + nodeGap) };
    });
  });
  return result;
}

function acyclic(ids: readonly string[], out: Map<string, string[]>): Map<string, string[]> {
  const dag = new Map<string, string[]>(ids.map((id) => [id, []]));
  const state = new Map<string, 1 | 2>(); // 1 on the stack, 2 done
  const visit = (id: string) => {
    state.set(id, 1);
    for (const t of out.get(id) as string[]) {
      const st = state.get(t);
      if (st === 1) continue; // a back edge: dropped
      (dag.get(id) as string[]).push(t);
      if (st === undefined) visit(t);
    }
    state.set(id, 2);
  };
  for (const id of ids) if (!state.has(id)) visit(id);
  return dag;
}

/** A free spot for a new node: one row below everything already placed, in column 0. */
export function nextFreePosition(
  placed: readonly Position[],
  opts: Pick<LayoutOptions, 'nodeHeight' | 'nodeGap'> = {},
): Position {
  if (placed.length === 0) return { x: 0, y: 0 };
  const height = opts.nodeHeight ?? NODE_HEIGHT;
  const gap = opts.nodeGap ?? 40;
  const minX = Math.min(...placed.map((p) => p.x));
  const maxY = Math.max(...placed.map((p) => p.y));
  return { x: minX, y: maxY + height + gap };
}
