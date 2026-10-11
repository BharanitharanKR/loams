/**
 * A gallery of every chart this package draws, on mock data.
 *
 * Mount it in any React page (`import { ChartGallery } from '@loams/charts/gallery'`)
 * to eyeball light and dark, reduced motion and the screen-reader table. It is
 * a separate entry so the console does not ship it.
 */
import { type ReactNode, useMemo, useState } from 'react';
import { LiveChart } from './live-chart';
import { mockMetricStream, mockRowStream, seedMetricPoints } from './mock';
import { Sparkline } from './sparkline';
import type { ChartSpec } from './types';
import { useMetricStream } from './use-metric-stream';

const WINDOW_MS = 60_000;

const lineSpec: ChartSpec = {
  chartType: 'Line Chart',
  title: 'Request rate',
  encodings: { x: { field: 't', type: 'temporal' }, y: { field: 'v' }, color: { field: 'series' } },
};
const areaSpec: ChartSpec = {
  chartType: 'Area Chart',
  title: 'Storage written',
  encodings: { x: { field: 't', type: 'temporal' }, y: { field: 'v' } },
};
const barSpec: ChartSpec = {
  chartType: 'Stacked Bar Chart',
  title: 'Compute hours by region',
  encodings: { x: 'day', y: 'hours', color: 'region' },
};
const pieSpec: ChartSpec = {
  chartType: 'Donut Chart',
  title: 'Spend by service',
  encodings: { x: 'service', y: 'usd' },
};

const BAR_ROWS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].flatMap((day, i) => [
  { day, region: 'eu-west', hours: 40 + i * 6 },
  { day, region: 'us-east', hours: 55 - i * 3 },
  { day, region: 'ap-south', hours: 18 + (i % 2) * 9 },
]);
const PIE_ROWS = [
  { service: 'Compute', usd: 412 },
  { service: 'Storage', usd: 188 },
  { service: 'Egress', usd: 74 },
];

const SERIES = ['p50', 'p95', 'p99'] as const;

function Panel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section
      aria-label={title}
      style={{ border: '1px solid var(--op-rule)', background: 'var(--op-surface)', padding: 16, minWidth: 0 }}
    >
      <h2 style={{ margin: '0 0 12px', fontSize: 13, fontWeight: 600, color: 'var(--op-ink)' }}>{title}</h2>
      {children}
    </section>
  );
}

function SparkRow({ name, seed }: { name: string; seed: number }) {
  const points = useMetricStream(() => mockMetricStream({ intervalMs: 1_000, seed }), WINDOW_MS);
  return (
    <div style={{ display: 'grid', gridTemplateColumns: '6rem 1fr', alignItems: 'center', gap: 12 }}>
      <span style={{ fontSize: 12, color: 'var(--op-muted)' }}>{name}</span>
      <Sparkline points={points} ariaLabel={`${name}, last minute`} height={28} />
    </div>
  );
}

export function ChartGallery() {
  const [dark, setDark] = useState(() =>
    typeof document === 'undefined' ? false : document.documentElement.classList.contains('dark'),
  );
  const toggleTheme = () => {
    const next = !dark;
    document.documentElement.classList.toggle('dark', next);
    setDark(next);
  };

  const latencySeed = useMemo(
    () => seedMetricPoints(WINDOW_MS, 1_000, { series: SERIES, seed: 11, base: 120, amplitude: 60 }),
    [],
  );
  const latencyStream = useMemo(
    () => mockRowStream({ series: SERIES, seed: 11, base: 120, amplitude: 60, intervalMs: 1_000 }),
    [],
  );
  const writeSeed = useMemo(() => seedMetricPoints(WINDOW_MS, 2_000, { seed: 3 }), []);
  const writeStream = useMemo(() => mockRowStream({ seed: 3, intervalMs: 2_000 }), []);

  return (
    <main style={{ background: 'var(--op-bg)', color: 'var(--op-ink)', padding: 16, minHeight: '100%' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
        <h1 style={{ fontSize: 16, margin: 0 }}>@loams/charts gallery</h1>
        <button type="button" onClick={toggleTheme} aria-pressed={dark}>
          Dark mode
        </button>
      </header>
      <div
        style={{
          display: 'grid',
          gap: 16,
          marginTop: 16,
          gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 360px), 1fr))',
        }}
      >
        <Panel title="Live line, three series, 60 s window">
          <LiveChart
            spec={lineSpec}
            data={latencySeed}
            stream={latencyStream}
            windowMs={WINDOW_MS}
            ariaLabel="Request latency p50, p95 and p99 in milliseconds over the last minute"
          />
        </Panel>
        <Panel title="Live area, one series">
          <LiveChart
            spec={areaSpec}
            data={writeSeed}
            stream={writeStream}
            windowMs={WINDOW_MS}
            ariaLabel="Storage written per two seconds over the last minute"
          />
        </Panel>
        <Panel title="Static stacked bar">
          <LiveChart spec={barSpec} data={BAR_ROWS} ariaLabel="Compute hours per day, stacked by region" />
        </Panel>
        <Panel title="Static donut">
          <LiveChart spec={pieSpec} data={PIE_ROWS} ariaLabel="Spend in US dollars by service" />
        </Panel>
        <Panel title="Sparklines">
          <div style={{ display: 'grid', gap: 8 }}>
            <SparkRow name="CPU" seed={21} />
            <SparkRow name="Memory" seed={22} />
            <SparkRow name="Queries" seed={23} />
          </div>
        </Panel>
        <Panel title="Unsupported type (falls back to the message and table)">
          <LiveChart
            spec={{ chartType: 'Network Graph', encodings: {} }}
            data={PIE_ROWS}
            ariaLabel="A graph spec, which belongs to @loams-core/flows"
          />
        </Panel>
      </div>
    </main>
  );
}

export default ChartGallery;
