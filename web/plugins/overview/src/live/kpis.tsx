// KPI stat cards: the latest value, its change over the window, and a
// Sparkline of the window. Each card reads one KPI's history, then its stream.

import { Sparkline, useMetricStream } from '@loams/charts';
import { Card, formatBytes, formatNumber, Stat } from '@loams/ui';
import { useCallback } from 'react';
import { startWith } from './ticker.js';
import type { Kpi, OverviewMetricsClient, SeriesPoint } from './types.js';

export const KPI_WINDOW_MS = 5 * 60_000;

export function formatKpi(kpi: Pick<Kpi, 'unit'>, v: number): string {
  switch (kpi.unit) {
    case 'rate':
      return `${formatNumber(Math.round(v))}/s`;
    case 'percent':
      return `${v.toFixed(2)}%`;
    case 'ms':
      return `${Math.round(v)} ms`;
    case 'bytes':
      return formatBytes(v);
  }
}

/** The change from the window's first point to its last, as a signed percent. */
export function kpiDelta(points: readonly SeriesPoint[]): number | undefined {
  if (points.length < 2) return undefined;
  const first = points[0]?.v ?? 0;
  const last = points[points.length - 1]?.v ?? 0;
  if (first === 0) return undefined;
  return ((last - first) / first) * 100;
}

function Delta({ kpi, delta }: { kpi: Kpi; delta: number | undefined }) {
  if (delta === undefined) return null;
  const up = delta >= 0;
  const good = kpi.better === 'higher' ? up : !up;
  const text = `${up ? '+' : '-'}${Math.abs(delta).toFixed(1)}%`;
  return (
    <span
      className={good ? 'text-[var(--op-grow-deep)]' : 'text-[var(--op-danger)]'}
      aria-label={`${up ? 'Up' : 'Down'} ${Math.abs(delta).toFixed(1)} percent over 5 minutes`}
    >
      {/* The arrow is decoration; the label above says the direction in words. */}
      <span aria-hidden="true">{up ? '↑' : '↓'} </span>
      {text}
    </span>
  );
}

export function KpiCard({ kpi, client }: { kpi: Kpi; client: OverviewMetricsClient }) {
  const source = useCallback(
    () => startWith(() => client.kpiHistory(kpi.id), client.kpiStream(kpi.id)),
    [client, kpi.id],
  );
  const points = useMetricStream(source, KPI_WINDOW_MS);
  const last = points[points.length - 1];
  return (
    <div data-kpi={kpi.id}>
      <Card>
        <div className="flex flex-col gap-2">
          <Stat
            label={kpi.label}
            value={last ? formatKpi(kpi, last.v) : 'Loading'}
            detail={<Delta kpi={kpi} delta={kpiDelta(points)} />}
          />
          <Sparkline points={points} ariaLabel={`${kpi.label}, last 5 minutes`} height={36} />
        </div>
      </Card>
    </div>
  );
}

export function KpiStrip({ kpis, client }: { kpis: Kpi[]; client: OverviewMetricsClient }) {
  return (
    <ul
      className="grid gap-3 list-none p-0 m-0 grid-cols-[repeat(auto-fit,minmax(13rem,1fr))]"
      aria-label="Key metrics"
    >
      {kpis.map((k) => (
        <li key={k.id}>
          <KpiCard kpi={k} client={client} />
        </li>
      ))}
    </ul>
  );
}
