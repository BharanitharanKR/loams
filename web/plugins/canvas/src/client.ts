// The project canvas's client. No loams.*.v1 proto describes a project's services yet, so
// the page talks to this typed interface; `createMockCanvasClient` (mock.ts) backs it with a
// seeded project until a server contract lands.

import type { SeriesPoint } from '@loams/charts';

/** Every kind of service a project canvas can hold. */
export type ServiceKind =
  | 'postgres-branch'
  | 'sql-database'
  | 'live-deployment'
  | 'graph'
  | 'house'
  | 'jobs-queue'
  | 'connector'
  | 'git-repo';

/** A service's live state, as its badge shows it. */
export type ServiceStatus = 'healthy' | 'deploying' | 'degraded' | 'failed' | 'stopped';

/** What an edge between two services means. */
export type EdgeKind = 'depends-on' | 'data-flow';

export interface Position {
  x: number;
  y: number;
}

export interface Service {
  id: string;
  kind: ServiceKind;
  name: string;
  status: ServiceStatus;
  /** Kind-specific facts the side panel lists (region, version, branch, ...), in order. */
  details: readonly { label: string; value: string }[];
  /** The console path of this service's product page; defaults to its kind's page. */
  href?: string;
}

/**
 * `depends-on`: `source` needs `target` (Live -> Postgres).
 * `data-flow`: data moves from `source` to `target` (connector source -> sink).
 */
export interface ServiceEdge {
  id: string;
  source: string;
  target: string;
  kind: EdgeKind;
  /** A verb read from the source's side ("builds", "streams to"); data flows only. */
  label?: string;
}

export interface Project {
  id: string;
  name: string;
  services: readonly Service[];
  edges: readonly ServiceEdge[];
  /** Saved canvas positions by service id; a service without one is auto-laid out. */
  positions: Readonly<Record<string, Position>>;
}

export interface StatusUpdate {
  serviceId: string;
  status: ServiceStatus;
}

export interface CreateServiceInput {
  kind: ServiceKind;
  name: string;
  /** Optional: an existing service the new one connects from (source -> new). */
  connectFrom?: string;
}

export interface CreatedService {
  service: Service;
  edges: readonly ServiceEdge[];
}

export interface CanvasClient {
  getProject(projectId: string, signal?: AbortSignal): Promise<Project>;
  createService(projectId: string, input: CreateServiceInput): Promise<CreatedService>;
  /** Merges `positions` into the project's saved positions. */
  savePositions(projectId: string, positions: Readonly<Record<string, Position>>): Promise<void>;
  /** Status changes, as they happen, until `signal` aborts. */
  watchStatus(projectId: string, signal: AbortSignal): AsyncIterable<StatusUpdate[]>;
  /** The service's key metric, a batch of points at a time (see `metricFor` in kinds.ts). */
  metricStream(projectId: string, serviceId: string): AsyncIterable<SeriesPoint[]>;
}

/** Whether `name` is acceptable for a new service: 1-63 chars of letters, digits, - and _. */
export function validServiceName(name: string): string | undefined {
  const n = name.trim();
  if (!n) return 'Give the service a name.';
  if (n.length > 63) return 'Use at most 63 characters.';
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(n))
    return 'Use letters, digits, - and _, starting with a letter or digit.';
  return undefined;
}

/** A failure's message for a notice, never a stack. */
export function errorText(e: unknown): string {
  if (e instanceof Error && e.message) return e.message;
  return 'Something went wrong.';
}
