// Fixture-backed `OverviewMetricsClient`: seeded, smooth-ish series so the
// dashboard moves like a real one without any server. Every stream is a
// `ticker`, so it emits one point per interval and stops on `return()`.
// Values are illustrative only; the page labels them "Sample data".

import { rng } from '@loams/ui';
import { ticker } from './ticker.js';
import type {
  AlertItem,
  Deployment,
  Kpi,
  KpiId,
  OverviewMetricsClient,
  Product,
  Resource,
  SeriesPoint,
  Topology,
  TrafficSample,
  UsageSample,
} from './types.js';

export const KPIS: Kpi[] = [
  { id: 'requests', label: 'Requests', unit: 'rate', better: 'higher' },
  { id: 'errors', label: 'Error rate', unit: 'percent', better: 'lower' },
  { id: 'latency', label: 'p95 latency', unit: 'ms', better: 'lower' },
  { id: 'storage', label: 'Storage', unit: 'bytes', better: 'lower' },
];

export const PRODUCTS: Product[] = ['Postgres', 'WeSQL', 'Streams', 'Durable', 'Live'];

const GB = 1e9;

/** Baseline and swing per KPI. */
const KPI_SHAPE: Record<KpiId, { base: number; swing: number; noise: number }> = {
  requests: { base: 420, swing: 90, noise: 30 },
  errors: { base: 0.6, swing: 0.3, noise: 0.15 },
  latency: { base: 48, swing: 12, noise: 6 },
  storage: { base: 38 * GB, swing: 0.4 * GB, noise: 0.05 * GB },
};

/** Per-product usage baselines: CPU cores, memory bytes, storage bytes. */
const USAGE_BASE: Record<Product, Record<Resource, number>> = {
  Postgres: { cpu: 1.6, memory: 3.2 * GB, storage: 18 * GB },
  WeSQL: { cpu: 1.1, memory: 2.4 * GB, storage: 11 * GB },
  Streams: { cpu: 0.8, memory: 1.5 * GB, storage: 6 * GB },
  Durable: { cpu: 0.4, memory: 0.9 * GB, storage: 2 * GB },
  Live: { cpu: 0.3, memory: 0.6 * GB, storage: 1 * GB },
};

const seedOf = (s: string) => [...s].reduce((h, c) => (h * 31 + c.charCodeAt(0)) >>> 0, 7);

/** A deterministic wave: slow sine, plus seeded jitter, never below zero. */
function wave(key: string, base: number, swing: number, noise: number) {
  const r = rng(seedOf(key));
  return (t: number) =>
    Math.max(0, base + swing * Math.sin(t / 90_000 + seedOf(key)) + noise * (r() * 2 - 1));
}

export interface MockOptions {
  /** Milliseconds between streamed points. */
  intervalMs?: number;
  /** How many history points each snapshot returns. */
  historyPoints?: number;
  /** Clock; tests pass a fixed one. */
  now?: () => number;
}

export function mockOverviewMetrics(opts: MockOptions = {}): OverviewMetricsClient {
  const intervalMs = opts.intervalMs ?? 2_000;
  const points = opts.historyPoints ?? 60;
  const now = opts.now ?? Date.now;
  const history = (step: (t: number) => void) => {
    const end = now();
    for (let k = points - 1; k >= 0; k--) step(end - k * intervalMs);
  };

  const kpiWave = (id: KpiId) => {
    const s = KPI_SHAPE[id];
    return wave(`kpi:${id}`, s.base, s.swing, s.noise);
  };
  const trafficWaves = {
    req: wave('traffic:req', 420, 90, 30),
    err: wave('traffic:err', 2.5, 1.5, 0.8),
  };
  const traffic = (t: number): TrafficSample[] => [
    { t, series: 'Requests/s', v: round(trafficWaves.req(t)) },
    { t, series: 'Errors/s', v: round(trafficWaves.err(t)) },
  ];
  const usageWaves = (resource: Resource) =>
    PRODUCTS.map((product) => {
      const base = USAGE_BASE[product][resource];
      return { product, f: wave(`usage:${product}:${resource}`, base, base * 0.15, base * 0.05) };
    });
  const usage = (resource: Resource, ws: ReturnType<typeof usageWaves>, t: number) =>
    ws.map(({ product, f }): UsageSample => ({ t, product, resource, v: round(f(t)) }));

  return {
    sample: true,
    kpis: async () => KPIS,
    async kpiHistory(id) {
      const f = kpiWave(id);
      const out: SeriesPoint[] = [];
      history((t) => out.push({ t, v: round(f(t)) }));
      return out;
    },
    kpiStream(id) {
      const f = kpiWave(id);
      return ticker(intervalMs, () => {
        const t = now();
        return [{ t, v: round(f(t)) }];
      });
    },
    async trafficHistory() {
      const out: TrafficSample[] = [];
      history((t) => out.push(...traffic(t)));
      return out;
    },
    trafficStream: () => ticker(intervalMs, () => traffic(now())),
    async usageHistory(resource) {
      const ws = usageWaves(resource);
      const out: UsageSample[] = [];
      history((t) => out.push(...usage(resource, ws, t)));
      return out;
    },
    usageStream(resource) {
      const ws = usageWaves(resource);
      return ticker(intervalMs, () => usage(resource, ws, now()));
    },
    deployments: async (limit) => deploymentFixtures(now()).slice(0, limit),
    alerts: async () => alertFixtures(now()),
    topology: async () => TOPOLOGY,
  };
}

const round = (v: number) => Math.round(v * 100) / 100;

const MIN = 60_000;

export function deploymentFixtures(now: number): Deployment[] {
  return [
    {
      id: 'dep-6',
      service: 'api-gateway',
      version: 'v2.14.0',
      state: 'building',
      at: now - 2 * MIN,
      by: 'ci',
    },
    {
      id: 'dep-5',
      service: 'orders-db',
      version: 'pg-17.2',
      state: 'live',
      at: now - 26 * MIN,
      by: 'ana',
    },
    {
      id: 'dep-4',
      service: 'events',
      version: 'v0.9.3',
      state: 'failed',
      at: now - 74 * MIN,
      by: 'ci',
    },
    {
      id: 'dep-3',
      service: 'worker',
      version: 'v1.8.1',
      state: 'rolled-back',
      at: now - 3 * 60 * MIN,
      by: 'li',
    },
    {
      id: 'dep-2',
      service: 'api-gateway',
      version: 'v2.13.2',
      state: 'live',
      at: now - 9 * 60 * MIN,
      by: 'ci',
    },
    {
      id: 'dep-1',
      service: 'sessions',
      version: 'v3.0.0',
      state: 'live',
      at: now - 26 * 60 * MIN,
      by: 'sam',
    },
  ];
}

export function alertFixtures(now: number): AlertItem[] {
  return [
    {
      id: 'al-3',
      title: 'Error rate above 2% for 5 minutes',
      service: 'events',
      severity: 'critical',
      state: 'firing',
      since: now - 11 * MIN,
    },
    {
      id: 'al-2',
      title: 'Replication lag over 30s',
      service: 'orders-db',
      severity: 'warning',
      state: 'acknowledged',
      since: now - 48 * MIN,
    },
    {
      id: 'al-1',
      title: 'Disk 80% full',
      service: 'object-store',
      severity: 'info',
      state: 'resolved',
      since: now - 5 * 60 * MIN,
    },
  ];
}

export const TOPOLOGY: Topology = {
  services: [
    { id: 'gw', name: 'api-gateway', kind: 'gateway', health: 'healthy', col: 0, row: 1 },
    { id: 'worker', name: 'worker', kind: 'compute', health: 'healthy', col: 1, row: 0 },
    { id: 'events', name: 'events', kind: 'stream', health: 'degraded', col: 1, row: 2 },
    { id: 'orders', name: 'orders-db', kind: 'database', health: 'healthy', col: 2, row: 0 },
    { id: 'sessions', name: 'sessions', kind: 'database', health: 'healthy', col: 2, row: 1 },
    { id: 'store', name: 'object-store', kind: 'storage', health: 'healthy', col: 2, row: 2 },
  ],
  links: [
    { from: 'gw', to: 'worker' },
    { from: 'gw', to: 'events' },
    { from: 'gw', to: 'sessions' },
    { from: 'worker', to: 'orders' },
    { from: 'events', to: 'store' },
    { from: 'events', to: 'worker' },
  ],
};
