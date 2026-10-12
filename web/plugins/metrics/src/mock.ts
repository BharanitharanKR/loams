// The fixture-backed MetricsClient: the default dashboards, an in-memory save with the
// server's version check, and metric series drawn from a deterministic function of
// (metric, series, time) so a test, a screenshot and a reload all see the same curve.
// `stream` emits the newest points on an interval, as the real watch stream will.

import type { DashboardSpec } from '@loams-core/types';
import { metricDef } from './catalog.js';
import {
  type MetricQuery,
  type MetricStreamQuery,
  type MetricsClient,
  type SeriesPoint,
  VersionConflict,
} from './client.js';
import { DEFAULT_DASHBOARDS } from './defaults.js';

/** The label values each split produces. */
export const SERIES: Record<string, readonly string[]> = {
  function: ['checkout', 'resize-image', 'send-email'],
  namespace: ['default', 'billing', 'analytics'],
  compute: ['main', 'reporting'],
  state: ['up', 'suspended'],
  quantile: ['p50', 'p95'],
};

interface Profile {
  base: number;
  /** Fraction of `base` the slow wave swings. */
  amp: number;
  /** Fraction of `base` the jitter swings. */
  noise: number;
  int?: boolean;
  /** Per-series scale; the first series is 1. */
  scale?: readonly number[];
}

const PROFILES: Record<string, Profile> = {
  loams_function_invocations_total: { base: 42, amp: 0.35, noise: 0.15, scale: [1, 0.45, 0.2] },
  loams_function_duration_ms_p95: { base: 180, amp: 0.2, noise: 0.25, scale: [1, 2.4, 0.6] },
  loams_function_cpu_seconds_total: { base: 1.6, amp: 0.3, noise: 0.1, scale: [1, 0.5, 0.35] },
  loams_function_memory_bytes: {
    base: 512 * 1024 * 1024,
    amp: 0.1,
    noise: 0.04,
    int: true,
    scale: [1, 0.6, 0.4],
  },
  loams_function_egress_bytes_total: {
    base: 3 * 1024 * 1024,
    amp: 0.4,
    noise: 0.2,
    scale: [1, 0.3, 0.7],
  },
  loams_postgres_computes: { base: 4, amp: 0.4, noise: 0.2, int: true, scale: [1, 1.5] },
  loams_postgres_wal_lag_bytes: {
    base: 256 * 1024,
    amp: 0.6,
    noise: 0.4,
    int: true,
    scale: [1, 0.25],
  },
  loams_postgres_connections: { base: 24, amp: 0.3, noise: 0.1, int: true, scale: [1, 0.35] },
  loams_sql_wake_latency_ms: { base: 45, amp: 0.15, noise: 0.2, scale: [1, 4.2] },
  loams_sql_active_keyspaces: { base: 9, amp: 0.3, noise: 0.15, int: true },
  loams_sql_queries_per_second: { base: 320, amp: 0.4, noise: 0.12 },
  loams_live_invocations_total: { base: 75, amp: 0.3, noise: 0.15 },
  loams_live_errors_total: { base: 0.6, amp: 0.5, noise: 0.8 },
  loams_live_worker_restarts_total: { base: 0.4, amp: 0.2, noise: 1.2, int: true },
  loams_house_queries_total: { base: 12, amp: 0.35, noise: 0.2 },
  loams_house_bytes_scanned_total: {
    base: 80 * 1024 * 1024,
    amp: 0.45,
    noise: 0.3,
    int: true,
  },
};

const FALLBACK: Profile = { base: 10, amp: 0.3, noise: 0.1 };
const PERIOD_MS = 30 * 60_000;

/** FNV-1a, then a xorshift step: a stable value in [0, 1) for a key. */
function hash01(key: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < key.length; i += 1) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  h ^= h << 13;
  h ^= h >>> 17;
  h ^= h << 5;
  return (h >>> 0) / 0x1_0000_0000;
}

/** The series a metric splits into; one unnamed series when it does not split. */
export function seriesOf(metric: string): readonly (string | undefined)[] {
  const split = metricDef(metric)?.splitBy;
  return split ? (SERIES[split] ?? [undefined]) : [undefined];
}

/** The value of one series of a metric at time `t`, deterministic in its arguments. */
export function mockValue(metric: string, series: string | undefined, index: number, t: number) {
  const p = PROFILES[metric] ?? FALLBACK;
  const base = p.base * (p.scale?.[index] ?? 1);
  const phase = hash01(`${metric}|${series ?? ''}`) * Math.PI * 2;
  const wave = Math.sin((2 * Math.PI * t) / PERIOD_MS + phase);
  const jitter = hash01(`${metric}|${series ?? ''}|${Math.floor(t / 1000)}`) * 2 - 1;
  const v = Math.max(0, base * (1 + p.amp * wave + p.noise * jitter));
  return p.int ? Math.round(v) : Math.round(v * 1000) / 1000;
}

export function mockPointsAt(metric: string, t: number): SeriesPoint[] {
  return seriesOf(metric).map((series, i) => {
    const v = mockValue(metric, series, i, t);
    return series === undefined ? { t, v } : { t, v, series };
  });
}

function aborted(signal?: AbortSignal): Error {
  return signal?.reason instanceof Error
    ? signal.reason
    : new DOMException('Aborted', 'AbortError');
}

/** Resolves after `ms`, or at once when the signal aborts. */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      signal?.removeEventListener('abort', done);
      resolve();
    }
    signal?.addEventListener('abort', done, { once: true });
  });
}

export interface MockOptions {
  /** The clock; tests pin it. */
  now?: () => number;
  /** Dashboards the server already holds (replacing the defaults with the same id). */
  saved?: readonly DashboardSpec[];
  /** Answers after this long, so loading states show; 0 in tests. */
  latencyMs?: number;
}

export interface MockMetricsClient extends MetricsClient {
  /** Every save the mock accepted, in order. */
  readonly saves: DashboardSpec[];
}

export function createMockMetricsClient(opts: MockOptions = {}): MockMetricsClient {
  const now = opts.now ?? (() => Date.now());
  const latency = opts.latencyMs ?? 0;
  const store = new Map<string, DashboardSpec>();
  for (const d of DEFAULT_DASHBOARDS) store.set(d.id, structuredClone(d));
  for (const d of opts.saved ?? []) store.set(d.id, structuredClone(d));
  const saves: DashboardSpec[] = [];

  const wait = async (signal?: AbortSignal) => {
    if (latency > 0) await sleep(latency, signal);
    if (signal?.aborted) throw aborted(signal);
  };

  return {
    saves,

    async listDashboards(signal) {
      await wait(signal);
      return [...store.values()].map((d) => structuredClone(d));
    },

    async saveDashboard(spec, baseVersion) {
      await wait();
      const current = store.get(spec.id);
      const actual = current?.version ?? 0;
      if (current && actual !== baseVersion) throw new VersionConflict(baseVersion, actual);
      const saved = structuredClone({ ...spec, version: baseVersion + 1 });
      store.set(saved.id, saved);
      saves.push(saved);
      return structuredClone(saved);
    },

    async query({ metric, rangeMs, stepMs, signal }: MetricQuery) {
      await wait(signal);
      const end = Math.floor(now() / stepMs) * stepMs;
      const out: SeriesPoint[] = [];
      for (let t = end - rangeMs; t <= end; t += stepMs) out.push(...mockPointsAt(metric, t));
      return out;
    },

    stream({ metric, intervalMs, signal }: MetricStreamQuery): AsyncIterable<SeriesPoint[]> {
      return {
        async *[Symbol.asyncIterator]() {
          while (!signal?.aborted) {
            await sleep(intervalMs, signal);
            if (signal?.aborted) return;
            yield mockPointsAt(metric, now());
          }
        },
      };
    },
  };
}
