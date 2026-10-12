// A fixture-backed CanvasClient: one seeded project ("storefront") whose services report
// status changes and emit metric points on an interval. Deterministic for a given seed, so
// tests can assert on it; the page uses it until a project service contract exists.

import type { SeriesPoint } from '@loams/charts';
import {
  type CanvasClient,
  type CreatedService,
  type CreateServiceInput,
  type Position,
  type Project,
  type Service,
  type ServiceEdge,
  type ServiceKind,
  type ServiceStatus,
  type StatusUpdate,
  validServiceName,
} from './client.js';
import { KINDS } from './kinds.js';

export const SEED_PROJECT_ID = 'storefront';

export interface MockOptions {
  /** How often streams emit, in ms. */
  intervalMs?: number;
  /** Points back-filled when a metric stream opens. */
  backfill?: number;
  /** The rng seed. */
  seed?: number;
  /** The clock, for tests. */
  now?: () => number;
  /** Delay before each request resolves, in ms. */
  latencyMs?: number;
}

/** The seeded project, fresh each call. */
export function seedProject(): Project {
  const s = (
    id: string,
    kind: ServiceKind,
    name: string,
    status: ServiceStatus,
    details: [string, string][],
  ): Service => ({
    id,
    kind,
    name,
    status,
    details: details.map(([label, value]) => ({ label, value })),
  });
  const services: Service[] = [
    s('repo-storefront', 'git-repo', 'storefront', 'healthy', [
      ['Default branch', 'main'],
      ['Last push', '12 min ago'],
    ]),
    s('live-web', 'live-deployment', 'web', 'healthy', [
      ['Revision', 'r184'],
      ['Replicas', '3'],
      ['Built from', 'storefront@main'],
    ]),
    s('live-api', 'live-deployment', 'api', 'deploying', [
      ['Revision', 'r92 → r93'],
      ['Replicas', '4'],
      ['Built from', 'storefront@main'],
    ]),
    s('pg-main', 'postgres-branch', 'main', 'healthy', [
      ['Compute', '2 CU'],
      ['Postgres', '17'],
      ['Autosuspend', '5 min'],
    ]),
    s('pg-preview-42', 'postgres-branch', 'preview-42', 'stopped', [
      ['Parent', 'main'],
      ['Compute', '0.25 CU (suspended)'],
    ]),
    s('sql-orders', 'sql-database', 'orders', 'healthy', [
      ['Engine', 'MySQL 8.0 compatible'],
      ['Tables', '24'],
    ]),
    s('graph-recs', 'graph', 'recommendations', 'healthy', [
      ['Nodes', '1.2M'],
      ['Edges', '8.4M'],
    ]),
    s('house-analytics', 'house', 'analytics', 'degraded', [
      ['Tables', '41'],
      ['Note', 'Compaction behind by 18 min'],
    ]),
    s('jobs-emails', 'jobs-queue', 'emails', 'healthy', [
      ['Workers', '2'],
      ['Retry policy', '5 × exponential'],
    ]),
    s('conn-orders-cdc', 'connector', 'orders-cdc', 'healthy', [
      ['Source', 'orders (Loams SQL)'],
      ['Sink', 'analytics (House)'],
      ['Mode', 'Change data capture'],
    ]),
  ];
  const e = (
    source: string,
    target: string,
    kind: ServiceEdge['kind'],
    label?: string,
  ): ServiceEdge => ({
    id: `${source}->${target}`,
    source,
    target,
    kind,
    label,
  });
  return {
    id: SEED_PROJECT_ID,
    name: 'Storefront',
    services,
    edges: [
      e('repo-storefront', 'live-web', 'data-flow', 'builds'),
      e('repo-storefront', 'live-api', 'data-flow', 'builds'),
      e('live-web', 'live-api', 'depends-on'),
      e('live-api', 'pg-main', 'depends-on'),
      e('live-api', 'sql-orders', 'depends-on'),
      e('live-api', 'graph-recs', 'depends-on'),
      e('live-api', 'jobs-emails', 'depends-on'),
      e('pg-main', 'pg-preview-42', 'data-flow', 'branches to'),
      e('sql-orders', 'conn-orders-cdc', 'data-flow', 'streams to'),
      e('conn-orders-cdc', 'house-analytics', 'data-flow', 'sinks into'),
    ],
    positions: {},
  };
}

/** mulberry32: a small, seeded, deterministic rng in [0, 1). */
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function hash(text: string): number {
  let h = 2166136261;
  for (let i = 0; i < text.length; i++) h = Math.imul(h ^ text.charCodeAt(i), 16777619);
  return h >>> 0;
}

/** Each kind's baseline and spread, so sparklines look like their metric. */
const METRIC_SHAPE: Readonly<Record<ServiceKind, { base: number; spread: number; min: number }>> = {
  'postgres-branch': { base: 38, spread: 6, min: 0 },
  'sql-database': { base: 420, spread: 60, min: 0 },
  'live-deployment': { base: 180, spread: 30, min: 0 },
  graph: { base: 64, spread: 12, min: 0 },
  house: { base: 22, spread: 8, min: 0 },
  'jobs-queue': { base: 30, spread: 10, min: 0 },
  connector: { base: 900, spread: 120, min: 0 },
  'git-repo': { base: 3, spread: 1.5, min: 0 },
};

/** Waits `ms`, or less if `signal` aborts first. */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const t = setTimeout(done, ms);
    function done() {
      clearTimeout(t);
      signal?.removeEventListener('abort', done);
      resolve();
    }
    signal?.addEventListener('abort', done, { once: true });
  });
}

const slug = (name: string) =>
  name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '') || 'service';

export interface MockCanvasClient extends CanvasClient {
  /** The stored project, for test assertions. */
  peek(projectId?: string): Project;
}

export function createMockCanvasClient(opts: MockOptions = {}): MockCanvasClient {
  const intervalMs = opts.intervalMs ?? 2000;
  const backfill = opts.backfill ?? 30;
  const seed = opts.seed ?? 7;
  const now = opts.now ?? (() => Date.now());
  const latency = opts.latencyMs ?? 0;

  const projects = new Map<string, Project>([[SEED_PROJECT_ID, seedProject()]]);
  const pending: StatusUpdate[] = [];
  let created = 0;

  const get = (id: string): Project => {
    const p = projects.get(id);
    if (!p) throw new Error(`No project named ${id}.`);
    return p;
  };
  const clone = <T>(v: T): T => JSON.parse(JSON.stringify(v)) as T;
  const settle = async <T>(v: T): Promise<T> => {
    if (latency > 0) await sleep(latency);
    return v;
  };
  const setStatus = (p: Project, serviceId: string, status: ServiceStatus) => {
    projects.set(p.id, {
      ...p,
      services: p.services.map((s) => (s.id === serviceId ? { ...s, status } : s)),
    });
  };

  return {
    peek: (projectId = SEED_PROJECT_ID) => clone(get(projectId)),

    async getProject(projectId) {
      return settle(clone(get(projectId)));
    },

    async createService(projectId, input: CreateServiceInput): Promise<CreatedService> {
      const p = get(projectId);
      const bad = validServiceName(input.name);
      if (bad) throw new Error(bad);
      const name = input.name.trim();
      if (p.services.some((s) => s.kind === input.kind && s.name === name)) {
        throw new Error(`A ${KINDS[input.kind].label.toLowerCase()} named ${name} already exists.`);
      }
      if (input.connectFrom && !p.services.some((s) => s.id === input.connectFrom)) {
        throw new Error('The service to connect from no longer exists.');
      }
      created += 1;
      const service: Service = {
        id: `${input.kind}-${slug(name)}-${created}`,
        kind: input.kind,
        name,
        status: 'deploying',
        details: [{ label: 'Created', value: 'just now' }],
      };
      const edges: ServiceEdge[] = input.connectFrom
        ? [
            {
              id: `${input.connectFrom}->${service.id}`,
              source: input.connectFrom,
              target: service.id,
              kind: input.kind === 'connector' ? 'data-flow' : 'depends-on',
            },
          ]
        : [];
      projects.set(projectId, {
        ...p,
        services: [...p.services, service],
        edges: [...p.edges, ...edges],
      });
      // A new service finishes deploying on the next status tick.
      pending.push({ serviceId: service.id, status: 'healthy' });
      return settle(clone({ service, edges }));
    },

    async savePositions(projectId, positions) {
      const p = get(projectId);
      const next: Record<string, Position> = { ...p.positions };
      for (const [id, pos] of Object.entries(positions)) {
        if (!p.services.some((s) => s.id === id)) continue;
        next[id] = { x: Math.round(pos.x), y: Math.round(pos.y) };
      }
      projects.set(projectId, { ...p, positions: next });
      await settle(undefined);
    },

    async *watchStatus(projectId, signal): AsyncGenerator<StatusUpdate[]> {
      const random = rng(seed ^ hash(`status:${projectId}`));
      // A service flipped off healthy, to restore on the next tick.
      let flipped: StatusUpdate | undefined;
      while (!signal.aborted) {
        await sleep(intervalMs, signal);
        if (signal.aborted) return;
        const p = get(projectId);
        const batch: StatusUpdate[] = pending.splice(0);
        if (flipped) {
          batch.push(flipped);
          flipped = undefined;
        } else {
          const live = p.services.filter((s) => s.status === 'healthy');
          const pick = live[Math.floor(random() * live.length)];
          if (pick && random() < 0.5) {
            batch.push({ serviceId: pick.id, status: random() < 0.5 ? 'deploying' : 'degraded' });
            flipped = { serviceId: pick.id, status: 'healthy' };
          }
        }
        for (const u of batch) setStatus(get(projectId), u.serviceId, u.status);
        if (batch.length) yield batch;
      }
    },

    async *metricStream(projectId, serviceId): AsyncGenerator<SeriesPoint[]> {
      const service = get(projectId).services.find((s) => s.id === serviceId);
      if (!service) return;
      const shape = METRIC_SHAPE[service.kind];
      const random = rng(seed ^ hash(`metric:${projectId}:${serviceId}`));
      let v = shape.base;
      const step = () => {
        v += (random() - 0.5) * shape.spread + (shape.base - v) * 0.1;
        v = Math.max(shape.min, v);
        return Math.round(v * 10) / 10;
      };
      const t0 = now();
      const first: SeriesPoint[] = [];
      for (let i = backfill - 1; i >= 0; i--) first.push({ t: t0 - i * intervalMs, v: step() });
      yield first;
      for (;;) {
        await sleep(intervalMs);
        yield [{ t: now(), v: step() }];
      }
    },
  };
}
