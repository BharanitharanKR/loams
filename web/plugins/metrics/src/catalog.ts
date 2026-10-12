// The metrics the page can chart. No metrics service has a proto yet, so this catalog is the
// typed contract the client (src/client.ts) and its mock (src/mock.ts) share: a panel names
// a metric by `id`, and the client streams `SeriesPoint`s for it, one series per label value.

export type MetricUnit = 'count' | 'per_second' | 'bytes' | 'bytes_per_second' | 'ms' | 'seconds';

export type MetricGroup = 'usage' | 'postgres' | 'sql' | 'live' | 'house';

export interface MetricDef {
  /** The exported metric name, e.g. `loams_function_invocations_total`. */
  id: string;
  label: string;
  group: MetricGroup;
  unit: MetricUnit;
  /** What one series is split by (`function`, `namespace`, `compute`, ...), or none. */
  splitBy?: string;
  description: string;
}

export const GROUP_LABELS: Record<MetricGroup, string> = {
  usage: 'Usage',
  postgres: 'Postgres',
  sql: 'Loams SQL',
  live: 'Live',
  house: 'House',
};

export const METRICS: readonly MetricDef[] = [
  // Usage: per-namespace and per-function, from the loams_function_* family.
  {
    id: 'loams_function_invocations_total',
    label: 'Invocations by function',
    group: 'usage',
    unit: 'per_second',
    splitBy: 'function',
    description: 'Function invocations per second, split by function.',
  },
  {
    id: 'loams_function_duration_ms_p95',
    label: 'Duration p95 by function',
    group: 'usage',
    unit: 'ms',
    splitBy: 'function',
    description: '95th percentile wall time of a function invocation.',
  },
  {
    id: 'loams_function_cpu_seconds_total',
    label: 'CPU seconds by namespace',
    group: 'usage',
    unit: 'seconds',
    splitBy: 'namespace',
    description: 'CPU seconds used per second by functions, split by namespace.',
  },
  {
    id: 'loams_function_memory_bytes',
    label: 'Memory by namespace',
    group: 'usage',
    unit: 'bytes',
    splitBy: 'namespace',
    description: 'Resident memory of running function instances, split by namespace.',
  },
  {
    id: 'loams_function_egress_bytes_total',
    label: 'Egress by namespace',
    group: 'usage',
    unit: 'bytes_per_second',
    splitBy: 'namespace',
    description: 'Bytes sent out of functions per second, split by namespace.',
  },
  // Postgres.
  {
    id: 'loams_postgres_computes',
    label: 'Computes up / suspended',
    group: 'postgres',
    unit: 'count',
    splitBy: 'state',
    description: 'Postgres computes by state: up or suspended.',
  },
  {
    id: 'loams_postgres_wal_lag_bytes',
    label: 'WAL lag',
    group: 'postgres',
    unit: 'bytes',
    splitBy: 'compute',
    description: 'Bytes of WAL not yet persisted by the safekeepers, per compute.',
  },
  {
    id: 'loams_postgres_connections',
    label: 'Connections',
    group: 'postgres',
    unit: 'count',
    splitBy: 'compute',
    description: 'Open client connections, per compute.',
  },
  // Loams SQL.
  {
    id: 'loams_sql_wake_latency_ms',
    label: 'Wake latency',
    group: 'sql',
    unit: 'ms',
    splitBy: 'quantile',
    description: 'Time from a query against a suspended keyspace to its first row: p50 and p95.',
  },
  {
    id: 'loams_sql_active_keyspaces',
    label: 'Active keyspaces',
    group: 'sql',
    unit: 'count',
    description: 'Keyspaces with a running compute.',
  },
  {
    id: 'loams_sql_queries_per_second',
    label: 'QPS',
    group: 'sql',
    unit: 'per_second',
    description: 'Statements executed per second.',
  },
  // Live.
  {
    id: 'loams_live_invocations_total',
    label: 'Invocations',
    group: 'live',
    unit: 'per_second',
    description: 'Live handler invocations per second.',
  },
  {
    id: 'loams_live_errors_total',
    label: 'Errors',
    group: 'live',
    unit: 'per_second',
    description: 'Live handler invocations that failed, per second.',
  },
  {
    id: 'loams_live_worker_restarts_total',
    label: 'Worker restarts',
    group: 'live',
    unit: 'count',
    description: 'Live workers restarted in each interval.',
  },
  // House.
  {
    id: 'loams_house_queries_total',
    label: 'Queries',
    group: 'house',
    unit: 'per_second',
    description: 'House queries per second.',
  },
  {
    id: 'loams_house_bytes_scanned_total',
    label: 'Bytes scanned',
    group: 'house',
    unit: 'bytes_per_second',
    description: 'Bytes House scanned per second.',
  },
];

const BY_ID = new Map(METRICS.map((m) => [m.id, m]));

export function metricDef(id: string): MetricDef | undefined {
  return BY_ID.get(id);
}

const UNIT_SUFFIX: Record<MetricUnit, string> = {
  count: '',
  per_second: '/s',
  bytes: '',
  bytes_per_second: '/s',
  ms: ' ms',
  seconds: ' s',
};

function bytes(n: number): string {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let v = Math.abs(n);
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${n < 0 ? '-' : ''}${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
}

/** A value in its metric's unit, for stat tiles, tooltips and the table fallback. */
export function formatValue(v: number, unit: MetricUnit): string {
  if (!Number.isFinite(v)) return '-';
  if (unit === 'bytes' || unit === 'bytes_per_second') return `${bytes(v)}${UNIT_SUFFIX[unit]}`;
  const abs = Math.abs(v);
  const text =
    abs >= 1_000_000
      ? `${(v / 1_000_000).toFixed(1)}M`
      : abs >= 10_000
        ? `${(v / 1000).toFixed(1)}k`
        : Number.isInteger(v)
          ? String(v)
          : v.toFixed(abs >= 100 ? 0 : abs >= 10 ? 1 : 2);
  return `${text}${UNIT_SUFFIX[unit]}`;
}
