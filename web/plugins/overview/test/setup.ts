// jsdom has no layout, canvas or ResizeObserver. React Flow needs the
// observer and DOMMatrixReadOnly to mount; ECharts needs a canvas. The chart
// package is replaced by plain stand-ins that render what a screen reader
// would get (the label and the row count), so these tests check the wiring,
// not ECharts.

import { vi } from 'vitest';

class RO {
  observe() {}
  unobserve() {}
  disconnect() {}
}
const g = globalThis as Record<string, unknown>;
g.ResizeObserver ??= RO;
g.DOMMatrixReadOnly ??= class {
  m22: number;
  constructor(t?: string) {
    const scale = t?.match(/scale\(([1-9.])\)/)?.[1];
    this.m22 = scale ? Number(scale) : 1;
  }
};
const css = g.CSS as { escape?: (s: string) => string } | undefined;
if (!css?.escape) g.CSS = { ...(css ?? {}), escape: (s: string) => s.replace(/["\\]/g, '\\$&') };

vi.mock('@loams/charts', async () => {
  const { createElement, useEffect, useState } = await import('react');
  type P = { t: number; v: number; series?: string };
  function useMetricStream(source: () => AsyncIterable<P[]>, windowMs: number): P[] {
    const [pts, setPts] = useState<P[]>([]);
    useEffect(() => {
      const it = source()[Symbol.asyncIterator]();
      let live = true;
      (async () => {
        for (;;) {
          const r = await it.next();
          if (!live || r.done) return;
          setPts((cur) => {
            const all = [...cur, ...r.value];
            const end = all[all.length - 1]?.t ?? 0;
            return all.filter((p) => p.t >= end - windowMs);
          });
        }
      })();
      return () => {
        live = false;
        void it.return?.();
      };
    }, [source, windowMs]);
    return pts;
  }
  function Sparkline(p: { points: readonly P[]; ariaLabel: string }) {
    return createElement('figure', {
      'aria-label': p.ariaLabel,
      'data-sparkline': p.points.length,
    });
  }
  function LiveChart(p: {
    spec: { chartType: string };
    data: readonly Record<string, unknown>[];
    ariaLabel: string;
  }) {
    return createElement('figure', {
      'aria-label': p.ariaLabel,
      'data-chart': p.spec.chartType,
      'data-rows': p.data.length,
    });
  }
  return { useMetricStream, Sparkline, LiveChart };
});
