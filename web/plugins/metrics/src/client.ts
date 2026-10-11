// The metrics page's client. No metrics service has a proto in @loams/proto yet, so this is
// a typed interface the page codes against, and src/mock.ts is the fixture-backed
// implementation the plugin uses until the server lands. A Connect client over the
// console transport replaces the mock behind the same interface.

import type { DashboardSpec } from '@loams-core/types';
import type { SeriesPoint } from '@loams/charts';

export type { SeriesPoint };

export interface MetricQuery {
  /** A metric id from the catalog (src/catalog.ts). */
  metric: string;
  /** How far back from now. */
  rangeMs: number;
  /** One point per series per step. */
  stepMs: number;
  signal?: AbortSignal;
}

export interface MetricStreamQuery extends MetricQuery {
  /** How often the stream emits the newest points. */
  intervalMs: number;
}

export interface MetricsClient {
  /** The saved dashboards, the defaults included (a saved copy replaces its default). */
  listDashboards(signal?: AbortSignal): Promise<DashboardSpec[]>;
  /**
   * Saves a dashboard. `baseVersion` is the version the edit started from; the server
   * refuses with `VersionConflict` when someone saved since, and otherwise returns the
   * saved spec at `baseVersion + 1`.
   */
  saveDashboard(spec: DashboardSpec, baseVersion: number): Promise<DashboardSpec>;
  /** The points over the last `rangeMs`, oldest first. */
  query(q: MetricQuery): Promise<SeriesPoint[]>;
  /** The newest points every `intervalMs`, until the signal aborts or the consumer stops. */
  stream(q: MetricStreamQuery): AsyncIterable<SeriesPoint[]>;
}

export class VersionConflict extends Error {
  constructor(
    readonly expected: number,
    readonly actual: number,
  ) {
    super(`Someone saved this dashboard since you opened it (version ${actual}, not ${expected}).`);
    this.name = 'VersionConflict';
  }
}

/** A failure as one line for the page. */
export function failureText(e: unknown): string {
  if (e instanceof Error) return e.message;
  return String(e);
}
