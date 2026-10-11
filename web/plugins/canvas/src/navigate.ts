// Keyboard navigation between canvas nodes: an arrow key moves focus to the nearest node
// in that direction, preferring nodes straight ahead over nodes off to the side.

import type { Position } from './client.js';

export type Direction = 'left' | 'right' | 'up' | 'down';

export interface Placed {
  id: string;
  position: Position;
}

export const ARROW_DIRECTIONS: Readonly<Record<string, Direction>> = {
  ArrowLeft: 'left',
  ArrowRight: 'right',
  ArrowUp: 'up',
  ArrowDown: 'down',
};

/**
 * The id of the nearest node from `fromId` in `dir`, or undefined when none lies that way.
 * A candidate must be ahead (its offset along `dir` positive); among those the score is the
 * distance ahead plus twice the sideways offset, so a node in line wins over a nearer one
 * off to the side. Ties go to the earlier node in `nodes`.
 */
export function nearestInDirection(
  nodes: readonly Placed[],
  fromId: string,
  dir: Direction,
): string | undefined {
  const from = nodes.find((n) => n.id === fromId);
  if (!from) return undefined;
  let best: { id: string; score: number } | undefined;
  for (const n of nodes) {
    if (n.id === fromId) continue;
    const dx = n.position.x - from.position.x;
    const dy = n.position.y - from.position.y;
    const ahead = dir === 'right' ? dx : dir === 'left' ? -dx : dir === 'down' ? dy : -dy;
    const side = dir === 'left' || dir === 'right' ? Math.abs(dy) : Math.abs(dx);
    if (ahead <= 0) continue;
    const score = ahead + 2 * side;
    if (!best || score < best.score) best = { id: n.id, score };
  }
  return best?.id;
}

/** Nodes in reading order (top to bottom, then left to right), for Home/End. */
export function readingOrder(nodes: readonly Placed[]): string[] {
  return [...nodes]
    .sort((a, b) => a.position.y - b.position.y || a.position.x - b.position.x)
    .map((n) => n.id);
}
