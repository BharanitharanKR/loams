// A stand-in for @loams/charts in jsdom (no canvas, no ECharts): the same exports and props,
// drawn as plain elements a test can find by their accessible names.

import { useEffect, useState } from 'react';

export type SeriesPoint = { t: number; v: number; series?: string };

export function LiveChart(p: {
  spec: { chartType: string };
  data: readonly Record<string, unknown>[];
  stream?: AsyncIterable<Record<string, unknown>[]>;
  windowMs?: number;
  height?: number;
  ariaLabel: string;
}) {
  return (
    <figure
      aria-label={p.ariaLabel}
      data-chart-type={p.spec.chartType}
      data-rows={p.data.length}
      data-live={p.stream ? 'yes' : 'no'}
      data-window={p.windowMs}
    />
  );
}

export function Sparkline(p: { points: readonly SeriesPoint[]; ariaLabel: string; height?: number }) {
  return <svg role="img" aria-label={p.ariaLabel} data-points={p.points.length} />;
}

export function useMetricStream(
  source: () => AsyncIterable<SeriesPoint[]>,
  windowMs: number,
): SeriesPoint[] {
  const [points, setPoints] = useState<SeriesPoint[]>([]);
  useEffect(() => {
    const it = source()[Symbol.asyncIterator]();
    let live = true;
    (async () => {
      for (;;) {
        const r = await it.next();
        if (!live || r.done) return;
        setPoints((prev) => {
          const all = [...prev, ...r.value];
          const end = Math.max(...all.map((x) => x.t));
          return all.filter((x) => x.t >= end - windowMs);
        });
      }
    })();
    return () => {
      live = false;
      void it.return?.();
    };
  }, [source, windowMs]);
  return points;
}
