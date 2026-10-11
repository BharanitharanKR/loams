// The Pipelines page's client contract. There is no loams.flow proto in @loams/proto
// yet (CN2 Task 1 writes the manifest proto; the pipeline service follows), so this is a
// typed interface the page is written against, and `createMockPipelinesClient`
// (mock.ts) serves it from fixtures until the Connect service exists.

import type { ConnectorDef } from './model.js';
import type { PipelineSpec } from './spec.js';
import type { Issue } from './validate.js';

/** One sample of a metric: epoch ms and value. A type alias so it is a plain record. */
export type MetricPoint = { t: number; v: number; series?: string };

/**
 * The two per-node series the page draws, after CN2 Task 24's metric families:
 * `throughput` is the rate of `loams_flow_connector_events_total` (events/s), `lag` is
 * `loams_flow_connector_lag_seconds`.
 */
export type Metric = 'throughput' | 'lag';

export type DeploymentState = 'deploying' | 'running' | 'stopped' | 'failed';

export interface Deployment {
  id: string;
  name: string;
  state: DeploymentState;
  /** The spec's version as the server stored it. */
  revision: number;
  message?: string;
}

export interface PipelinesClient {
  /** Source and sink connectors, by family. */
  catalog(signal?: AbortSignal): Promise<readonly ConnectorDef[]>;
  /** The server's checks, on top of the page's own. */
  validate(spec: PipelineSpec, signal?: AbortSignal): Promise<readonly Issue[]>;
  /**
   * Deploys the spec. `secrets` maps each `secretRef` in the spec to its value; the
   * server writes them to its secret store and never returns them.
   */
  deploy(spec: PipelineSpec, secrets: ReadonlyMap<string, string>): Promise<Deployment>;
  stop(deploymentId: string): Promise<Deployment>;
  /** A live series for one node; each iteration yields the points since the last. */
  metrics(deploymentId: string, nodeId: string, metric: Metric): AsyncIterable<MetricPoint[]>;
}

/** A refused call, as the page shows it. */
export function failureMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
