// Turning the client's point streams into what @loams/charts consumes.

import type { MetricsClient, SeriesPoint } from './client.js';

export type Row = { t: number; v: number; series: string };

/** A point as a chart row; an unsplit metric's one series is named after the panel. */
export function toRow(p: SeriesPoint, name: string): Row {
  return { t: p.t, v: p.v, series: p.series ?? name };
}

/**
 * An iterable that owns an AbortController per iteration: the consumer stopping (`return()`,
 * as `for await ... break` or an unmounting chart does) aborts the underlying request, so
 * no timer outlives its panel. Each iteration is independent, so a remount starts afresh.
 */
export function abortable<T>(make: (signal: AbortSignal) => AsyncIterable<T>): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]() {
      const ctl = new AbortController();
      const it = make(ctl.signal)[Symbol.asyncIterator]();
      return {
        next: () => it.next(),
        return: (value?: unknown) => {
          ctl.abort();
          return it.return
            ? it.return(value as T)
            : Promise.resolve({ done: true as const, value: value as T });
        },
      };
    },
  };
}

export interface PanelSource {
  metric: string;
  rangeMs: number;
  stepMs: number;
  /** 0: no live stream. */
  intervalMs: number;
}

/** The live rows after the backfill, or undefined when refresh is off. */
export function liveRows(
  client: MetricsClient,
  src: PanelSource,
  name: string,
): AsyncIterable<Row[]> | undefined {
  if (src.intervalMs <= 0) return undefined;
  return abortable(async function* (signal) {
    for await (const points of client.stream({ ...src, signal })) {
      yield points.map((p) => toRow(p, name));
    }
  });
}

/** Backfill then live points, as one stream: what a stat tile's `useMetricStream` reads. */
export function backfillThenLive(
  client: MetricsClient,
  src: PanelSource,
): AsyncIterable<SeriesPoint[]> {
  return abortable(async function* (signal) {
    yield await client.query({ ...src, signal });
    if (src.intervalMs <= 0) return;
    yield* client.stream({ ...src, signal });
  });
}

/** The newest value of each series, in first-seen order. */
export function latestBySeries(points: readonly SeriesPoint[]): { series?: string; v: number }[] {
  const last = new Map<string | undefined, { t: number; v: number }>();
  for (const p of points) {
    const prev = last.get(p.series);
    if (!prev || p.t >= prev.t) last.set(p.series, { t: p.t, v: p.v });
  }
  return [...last.entries()].map(([series, { v }]) =>
    series === undefined ? { v } : { series, v },
  );
}
