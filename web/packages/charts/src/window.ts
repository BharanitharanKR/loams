/**
 * The sliding time window, as a pure reducer.
 *
 * Live charts append batches as they arrive and keep only what falls inside
 * `windowMs` of the newest sample. The window is anchored on the data, not the
 * wall clock, so a stalled stream holds its last picture instead of scrolling
 * into emptiness, and the reducer stays deterministic under test.
 */
import type { Row, SeriesPoint } from './types';

/** Hard cap on retained rows, whatever the window says, so a burst cannot grow memory without bound. */
export const DEFAULT_MAX_ROWS = 5_000;

export interface WindowOptions {
  /** Width of the window in milliseconds. Zero or less keeps every row (still capped by `maxRows`). */
  windowMs: number;
  /** The field holding each row's time. */
  timeField: string;
  /** Anchor the window here instead of on the newest row (epoch ms). */
  now?: number;
  /** Keep at most this many rows, newest first. */
  maxRows?: number;
}

/**
 * Read a row's time as epoch milliseconds.
 *
 * Accepts a finite number, a `Date`, or a string `Date.parse` understands.
 * Anything else is "no time", and such rows are never trimmed by the window.
 */
export function timeOf(value: unknown): number | undefined {
  if (typeof value === 'number') return Number.isFinite(value) ? value : undefined;
  if (value instanceof Date) {
    const t = value.getTime();
    return Number.isFinite(t) ? t : undefined;
  }
  if (typeof value === 'string' && value !== '') {
    const t = Date.parse(value);
    return Number.isFinite(t) ? t : undefined;
  }
  return undefined;
}

/** The newest time in `rows`, or undefined when no row carries one. */
export function latestTime(rows: readonly Row[], timeField: string): number | undefined {
  let latest: number | undefined;
  for (const row of rows) {
    const t = timeOf(row[timeField]);
    if (t !== undefined && (latest === undefined || t > latest)) latest = t;
  }
  return latest;
}

/**
 * Append `incoming` to `rows` and trim to the window.
 *
 * Returns a new array; neither input is mutated. Rows stay in time order: a
 * batch that arrives out of order is merged by a stable sort, so equal times
 * keep their arrival order. Rows without a readable time sort last and survive
 * the time trim, but still count toward `maxRows`.
 */
export function appendWindow<T extends Row>(
  rows: readonly T[],
  incoming: readonly T[],
  options: WindowOptions,
): T[] {
  const { windowMs, timeField, now, maxRows = DEFAULT_MAX_ROWS } = options;
  let merged: T[] = rows.concat(incoming);

  if (incoming.length > 0 && !isSorted(merged, timeField)) {
    merged = merged
      .map((row, index) => ({ row, index, t: timeOf(row[timeField]) }))
      .sort((a, b) => {
        if (a.t === undefined || b.t === undefined) {
          if (a.t === b.t) return a.index - b.index;
          return a.t === undefined ? 1 : -1;
        }
        return a.t - b.t || a.index - b.index;
      })
      .map((entry) => entry.row);
  }

  if (windowMs > 0) {
    const anchor = now ?? latestTime(merged, timeField);
    if (anchor !== undefined) {
      const start = anchor - windowMs;
      merged = merged.filter((row) => {
        const t = timeOf(row[timeField]);
        return t === undefined || (t >= start && t <= anchor);
      });
    }
  }

  if (maxRows >= 0 && merged.length > maxRows) merged = merged.slice(merged.length - maxRows);
  return merged;
}

/** {@link appendWindow} for metric points, whose time is always `t`. */
export function windowPoints(
  points: readonly SeriesPoint[],
  incoming: readonly SeriesPoint[],
  windowMs: number,
  now?: number,
): SeriesPoint[] {
  return appendWindow(points as readonly Row[], incoming as readonly Row[], {
    windowMs,
    timeField: 't',
    now,
  }) as unknown as SeriesPoint[];
}

/** The [start, end] the chart's time axis should show for these rows. */
export function windowBounds(
  rows: readonly Row[],
  timeField: string,
  windowMs: number,
): { start: number; end: number } | undefined {
  const end = latestTime(rows, timeField);
  if (end === undefined || windowMs <= 0) return undefined;
  return { start: end - windowMs, end };
}

function isSorted(rows: readonly Row[], timeField: string): boolean {
  let previous = Number.NEGATIVE_INFINITY;
  let sawUntimed = false;
  for (const row of rows) {
    const t = timeOf(row[timeField]);
    if (t === undefined) {
      sawUntimed = true;
      continue;
    }
    if (sawUntimed || t < previous) return false;
    previous = t;
  }
  return true;
}
