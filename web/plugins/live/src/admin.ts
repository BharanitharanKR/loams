// The Live admin client the Deployments and Functions views use.
//
// LV1 Task 10 adds `loams.live.v1.LiveAdminService { Deploy; ListDeployments;
// GetDeployment; ActivateDeployment; GetIndexStatus; TailLogs }`. It is not in
// proto/loams/live yet, so there are no generated stubs: this file defines the
// typed interface the views need, shaped after the plan's records (Tasks 4-7,
// 10), and `createMockAdminApi` serves it from fixtures. When the service lands,
// a `createLiveAdminApi(transport)` over `@loams/proto` replaces the mock in
// index.tsx and nothing in the views changes.

import type { Json } from './value.js';

/** `DeploymentState` (Task 6). `degraded`: five worker crashes in a minute (Task 5). */
export type DeploymentState =
  | 'pending'
  | 'validating'
  | 'backfilling'
  | 'active'
  | 'superseded'
  | 'failed'
  | 'degraded';

/** `DeploymentRecord` (Task 6), as the page needs it. Ids are ULIDs. */
export interface Deployment {
  id: string;
  /** Hex of the bundle's SHA-256. */
  bundleSha256: string;
  createdAtMs: number;
  createdBy: string;
  message: string;
  parent?: string;
  state: DeploymentState;
  /** `Failed { reason }`. */
  failureReason?: string;
  /** Set when this deployment was activated by a rollback (Task 7). */
  rolledBackFrom?: string;
}

/** The validator vocabulary of Task 4 (`v.null|int64|...|id(table)`). */
export type Validator =
  | { kind: 'null' | 'int64' | 'float64' | 'boolean' | 'string' | 'bytes' | 'any' }
  | { kind: 'array'; items: Validator }
  | { kind: 'object'; fields: { [name: string]: Validator } }
  | { kind: 'literal'; value: Json }
  | { kind: 'union'; members: Validator[] }
  | { kind: 'optional'; inner: Validator }
  | { kind: 'id'; table: string };

/** `FunctionMeta` (worker.proto, Task 4). `args` is absent when the function declares none. */
export interface FunctionInfo {
  path: string;
  kind: 'query' | 'mutation';
  visibility: 'public' | 'internal';
  args?: Validator;
}

export interface DeploymentDetail {
  deployment: Deployment;
  functions: FunctionInfo[];
}

/** One worker (one per node, app and deployment, Task 5). */
export interface WorkerHealth {
  node: string;
  deploymentId: string;
  degraded: boolean;
  crashesLastMinute: number;
  restarts: number;
  lastCrashMs?: number;
  /** The last failure reason, e.g. `live_worker_crashed`; never a document value. */
  lastReason?: string;
}

export type LogLevel = 'log' | 'debug' | 'info' | 'warn' | 'error';

/** One `logs::Ring` line (Task 10): `{ts, request_id, function, level, line}`. */
export interface LogLine {
  ts: number;
  requestId: string;
  function: string;
  level: LogLevel;
  line: string;
  truncated?: boolean;
}

/** `TailLogs{follow, since_ms, function?}` (Task 10). */
export interface TailLogsRequest {
  follow: boolean;
  sinceMs?: number;
  function?: string;
}

/** The deploy CAS lost (Task 6): `FAILED_PRECONDITION`, reason `live_deploy_conflict`. */
export class DeployConflictError extends Error {
  readonly reason = 'live_deploy_conflict';
  constructor(message = 'Another deployment became active first. Reload and try again.') {
    super(message);
    this.name = 'DeployConflictError';
  }
}

export interface LiveAdminApi {
  /** True while the API is served from fixtures rather than the engine. */
  readonly mocked: boolean;
  /** Newest first. */
  listDeployments(signal?: AbortSignal): Promise<Deployment[]>;
  getDeployment(id: string, signal?: AbortSignal): Promise<DeploymentDetail>;
  /** Activate an earlier (or later) deployment; a rollback is this (Task 7). */
  activateDeployment(id: string, expectedActive: string | undefined): Promise<Deployment>;
  workerHealth(signal?: AbortSignal): Promise<WorkerHealth[]>;
  tailLogs(req: TailLogsRequest, signal: AbortSignal): AsyncIterable<LogLine>;
}

/** A validator as source text, in the `v.*` builder form the SDK uses. */
export function validatorText(v: Validator | undefined): string {
  if (!v) return '(none)';
  switch (v.kind) {
    case 'array':
      return `v.array(${validatorText(v.items)})`;
    case 'object': {
      const entries = Object.entries(v.fields);
      if (entries.length === 0) return 'v.object({})';
      return `v.object({ ${entries.map(([k, f]) => `${k}: ${validatorText(f)}`).join(', ')} })`;
    }
    case 'literal':
      return `v.literal(${JSON.stringify(v.value)})`;
    case 'union':
      return `v.union(${v.members.map(validatorText).join(', ')})`;
    case 'optional':
      return `v.optional(${validatorText(v.inner)})`;
    case 'id':
      return `v.id(${JSON.stringify(v.table)})`;
    default:
      return `v.${v.kind}()`;
  }
}
