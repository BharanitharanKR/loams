// The typed client for connector instances (CN2: docs/plans/2026-10-10-cn2-connectors.md).
//
// No `loams.flow.v1.FlowService` exists in the tree yet (CN1 Task 3 owns `flow.proto`; only
// `connector.proto` and `instance.proto` are in `fabric/proto`), so these types are written by hand
// to the shapes the plan fixes: `Instance` (instance.proto), `Direction`, `FabricBinding`, the SDK's
// per-split checkpoints, `ErrorClass` (Task 4) and the connector metrics (lag, throughput). When the
// generated `@loams/proto` client lands, this interface is implemented over it and the pages do not
// change. Until then the plugin runs against `createMockFlowClient` (mock-flow.ts).
//
// Secrets are write-only: `createInstance` takes secret values once, keyed by dotted field path,
// and nothing the client returns ever carries a value, only the secret's name (`SecretRef`).

export type Direction = 'source' | 'sink';

/** Native = a first-party Rust connector on `loams-connector-sdk` (OR-1); fallback = Camel,
 * Debezium Server or an Iggy upstream plugin, declared with the task that replaces it (OR-4). */
export type RuntimeClass = 'native' | 'fallback';

export type InstanceState = 'running' | 'paused' | 'starting' | 'failed' | 'stopped';

/** The SDK's error classes (CN2 Task 4). */
export type ErrorClass =
  | 'retryable'
  | 'throttled'
  | 'poison'
  | 'auth'
  | 'fatal'
  | 'position_lost'
  | 'schema_incompatible';

export type FabricBinding =
  | { kind: 'topic'; topic: string; partitions?: number }
  | { kind: 'table'; table: string; partitions?: number }
  | { kind: 'collection'; collection: string };

export interface InstanceSummary {
  name: string;
  connector: string;
  direction: Direction;
  runtime: RuntimeClass;
  state: InstanceState;
  /** Events per second over the last minute. */
  eventsPerSec: number;
  /** Seconds behind the source (or the Fabric, for a sink). */
  lagSeconds: number;
  version: number;
}

/** One split's committed position (a Kafka partition, a CDC slot, a key range). */
export interface SplitCheckpoint {
  split: string;
  /** `Position::display`: never carries a secret (OR / Global constraints). */
  position: string;
  epoch: number;
  committedAt: string;
  lagSeconds: number;
}

export interface ThroughputPoint {
  at: string;
  eventsPerSec: number;
  bytesPerSec: number;
}

export interface InstanceError {
  at: string;
  class: ErrorClass;
  message: string;
  split?: string;
  /** For `position_lost`, the re-snapshot hint. */
  hint?: string;
}

export interface InstanceDetail extends InstanceSummary {
  connectorMajor: number;
  fabric: FabricBinding;
  delivery: 'at_least_once' | 'effectively_once' | 'exactly_once';
  /** Config with secrets as `${secret:<path>}` placeholders. */
  config: Record<string, unknown>;
  /** Secret names only. */
  secrets: string[];
  checkpoints: SplitCheckpoint[];
  throughput: ThroughputPoint[];
  errors: InstanceError[];
  dlqCount: number;
  lastError?: InstanceError;
}

export interface CreateInstanceRequest {
  connector: string;
  name: string;
  direction: Direction;
  runtime: RuntimeClass;
  fabric: FabricBinding;
  /** Config with secret fields as placeholders (form.ts `buildConfig`). */
  config: Record<string, unknown>;
  /** Write-only: secret values by dotted path, sent once and never returned. */
  secrets: Record<string, string>;
  enabled: boolean;
}

export type FlowResult<T> = { ok: true; value: T } | { ok: false; code: string; message: string };

export interface FlowClient {
  listInstances(): Promise<InstanceSummary[]>;
  getInstance(name: string): Promise<FlowResult<InstanceDetail>>;
  createInstance(req: CreateInstanceRequest): Promise<FlowResult<InstanceSummary>>;
  pause(name: string): Promise<FlowResult<InstanceSummary>>;
  resume(name: string): Promise<FlowResult<InstanceSummary>>;
}

/** Instance names: the namespace object rule (lowercase, digits, dashes; 1..63). */
export const NAME_RE = /^[a-z][a-z0-9-]{0,62}$/;

export const formatLag = (s: number): string =>
  s < 1 ? '<1 s' : s < 60 ? `${Math.round(s)} s` : s < 3600 ? `${Math.round(s / 60)} min` : `${(s / 3600).toFixed(1)} h`;
