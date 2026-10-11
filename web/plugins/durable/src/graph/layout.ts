// A layered layout for the run DAG: a step's column is its longest path from
// the workflow, its row its order within that column. Pure, so it is tested
// without a canvas, and it also gives the keyboard its neighbours.

import { edgesOf, type RunGraph } from './model.js';

export const COL_W = 240;
export const ROW_H = 112;

export interface Placed {
  id: string;
  col: number;
  row: number;
  x: number;
  y: number;
}

export interface Layout {
  placed: Map<string, Placed>;
  columns: string[][];
  edges: [string, string][];
}

export function layoutRun(run: RunGraph): Layout {
  const edges = edgesOf(run);
  const incoming = new Map<string, string[]>();
  for (const [a, b] of edges) incoming.set(b, [...(incoming.get(b) ?? []), a]);
  const col = new Map<string, number>();
  const visiting = new Set<string>();
  const depth = (id: string): number => {
    const known = col.get(id);
    if (known !== undefined) return known;
    if (visiting.has(id)) return 0; // a cycle in bad data: break it rather than recurse forever
    visiting.add(id);
    const d = Math.max(-1, ...(incoming.get(id) ?? []).map(depth)) + 1;
    visiting.delete(id);
    col.set(id, d);
    return d;
  };
  const order = [...run.steps].sort(
    (a, b) => a.createdAt - b.createdAt || a.id.localeCompare(b.id),
  );
  for (const s of order) depth(s.id);
  const columns: string[][] = [];
  for (const s of order) {
    const c = col.get(s.id) ?? 0;
    const column = columns[c] ?? [];
    column.push(s.id);
    columns[c] = column;
  }
  const tallest = Math.max(1, ...columns.map((c) => c?.length ?? 0));
  const placed = new Map<string, Placed>();
  columns.forEach((ids, c) => {
    const offset = ((tallest - ids.length) * ROW_H) / 2;
    ids.forEach((id, row) => {
      placed.set(id, { id, col: c, row, x: c * COL_W, y: offset + row * ROW_H });
    });
  });
  return { placed, columns, edges };
}

export type Direction = 'left' | 'right' | 'up' | 'down';

/** The step the arrow key moves to from `id`, or `id` itself at an edge of the graph. */
export function neighbour(layout: Layout, id: string, dir: Direction): string {
  const here = layout.placed.get(id);
  if (!here) return id;
  if (dir === 'up' || dir === 'down') {
    const column = layout.columns[here.col] ?? [];
    const next = column[here.row + (dir === 'up' ? -1 : 1)];
    return next ?? id;
  }
  const linked =
    dir === 'left'
      ? layout.edges.filter(([, b]) => b === id).map(([a]) => a)
      : layout.edges.filter(([a]) => a === id).map(([, b]) => b);
  if (linked.length) {
    // The linked step nearest this one's row, so moving right then left returns.
    return linked.reduce((best, cand) => {
      const p = layout.placed.get(cand);
      const q = layout.placed.get(best);
      return p && q && Math.abs(p.y - here.y) < Math.abs(q.y - here.y) ? cand : best;
    });
  }
  const column = layout.columns[here.col + (dir === 'left' ? -1 : 1)];
  if (!column?.length) return id;
  return column[Math.min(here.row, column.length - 1)] ?? id;
}
