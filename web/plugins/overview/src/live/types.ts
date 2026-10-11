// The live home dashboard's data, as a typed client. No proto describes
// metrics, deployments, alerts or the service topology yet, so the Overview
// reads them through this interface; `mockOverviewMetrics` (./mock.ts) is the
// fixture-backed implementation until a server one exists.

import type { SeriesPoint } from '@loams/charts';

export type { SeriesPoint };

export type KpiId = 'requests' | 'errors' | 'latency' | 'storage';

export type Kpi = {
  id: KpiId;
  label: string;
  /** How a value of this KPI reads: `rate` is per second, `percent` is 0-100. */
  unit: 'rate' | 'percent' | 'ms' | 'bytes';
  /** Lower is better for errors and latency; the delta's tone follows this. */
  better: 'higher' | 'lower';
};

/** One request-rate sample: total and failed requests per second at `t`. */
export type TrafficSample = {
  t: number;
  series: 'Requests/s' | 'Errors/s';
  v: number;
};

export type Product = 'Postgres' | 'WeSQL' | 'Streams' | 'Durable' | 'Live';
export type Resource = 'cpu' | 'memory' | 'storage';

/** One usage sample: how much of `resource` a `product` used at `t`. */
export type UsageSample = {
  t: number;
  product: Product;
  resource: Resource;
  /** CPU in cores, memory and storage in bytes. */
  v: number;
};

export type DeploymentState = 'live' | 'building' | 'failed' | 'rolled-back';

export type Deployment = {
  id: string;
  service: string;
  version: string;
  state: DeploymentState;
  /** Epoch ms. */
  at: number;
  by: string;
};

export type Severity = 'critical' | 'warning' | 'info';

export type AlertItem = {
  id: string;
  title: string;
  service: string;
  severity: Severity;
  state: 'firing' | 'acknowledged' | 'resolved';
  /** Epoch ms. */
  since: number;
};

export type ServiceKind = 'database' | 'stream' | 'compute' | 'gateway' | 'storage';
export type ServiceHealth = 'healthy' | 'degraded' | 'down';

export type ServiceNode = {
  id: string;
  name: string;
  kind: ServiceKind;
  health: ServiceHealth;
  /** Layout hint: column and row in a small grid; the mini canvas spaces them. */
  col: number;
  row: number;
};

export type ServiceLink = {
  from: string;
  to: string;
};

export type Topology = {
  services: ServiceNode[];
  links: ServiceLink[];
};

/**
 * Where the live dashboard reads from. Snapshots are promises; streams are
 * async iterables the caller stops with `return()` (React unmount does this).
 */
export interface OverviewMetricsClient {
  /** True when this client serves fixtures; the page says so. */
  readonly sample: boolean;
  kpis(): Promise<Kpi[]>;
  /** Recent history for one KPI, oldest first. */
  kpiHistory(id: KpiId): Promise<SeriesPoint[]>;
  /** New KPI points as they arrive. */
  kpiStream(id: KpiId): AsyncIterable<SeriesPoint[]>;
  trafficHistory(): Promise<TrafficSample[]>;
  trafficStream(): AsyncIterable<TrafficSample[]>;
  usageHistory(resource: Resource): Promise<UsageSample[]>;
  usageStream(resource: Resource): AsyncIterable<UsageSample[]>;
  deployments(limit: number): Promise<Deployment[]>;
  alerts(): Promise<AlertItem[]>;
  topology(): Promise<Topology>;
}
