// A fixture-backed LiveAdminApi, used until LiveAdminService (LV1 Task 10) is
// served. State is per instance and in memory: activating a deployment here
// changes nothing on any engine.

import {
  DeployConflictError,
  type Deployment,
  type DeploymentDetail,
  type FunctionInfo,
  type LiveAdminApi,
  type LogLine,
  type TailLogsRequest,
  type WorkerHealth,
} from './admin.js';

export interface AdminFixtures {
  deployments: Deployment[];
  functions: { [deploymentId: string]: FunctionInfo[] };
  workers: WorkerHealth[];
  logs: LogLine[];
}

const T0 = 1_760_000_000_000;

const BASE_FUNCTIONS: FunctionInfo[] = [
  {
    path: 'messages:list',
    kind: 'query',
    visibility: 'public',
    args: {
      kind: 'object',
      fields: {
        channel: { kind: 'id', table: 'channels' },
        limit: { kind: 'optional', inner: { kind: 'int64' } },
      },
    },
  },
  {
    path: 'messages:send',
    kind: 'mutation',
    visibility: 'public',
    args: {
      kind: 'object',
      fields: {
        channel: { kind: 'id', table: 'channels' },
        body: { kind: 'string' },
      },
    },
  },
  { path: 'channels:all', kind: 'query', visibility: 'public' },
];

const NEXT_FUNCTIONS: FunctionInfo[] = [
  ...BASE_FUNCTIONS,
  {
    path: 'messages:react',
    kind: 'mutation',
    visibility: 'public',
    args: {
      kind: 'object',
      fields: {
        message: { kind: 'id', table: 'messages' },
        emoji: {
          kind: 'union',
          members: [
            { kind: 'literal', value: 'up' },
            { kind: 'literal', value: 'down' },
          ],
        },
      },
    },
  },
  {
    path: 'internal/cleanup:prune',
    kind: 'mutation',
    visibility: 'internal',
    args: { kind: 'object', fields: { olderThanMs: { kind: 'float64' } } },
  },
];

export function defaultFixtures(): AdminFixtures {
  const d1 = '01K7A0000000000000000000D1';
  const d2 = '01K7A0000000000000000000D2';
  const d3 = '01K7A0000000000000000000D3';
  const d4 = '01K7A0000000000000000000D4';
  return {
    deployments: [
      {
        id: d4,
        bundleSha256: 'c4'.repeat(32),
        createdAtMs: T0 + 3 * 3_600_000,
        createdBy: 'dev@example.test',
        message: 'Tighten message length validator',
        parent: d3,
        state: 'failed',
        failureReason: 'live_schema_violation: 3 documents in messages fail the new validator',
      },
      {
        id: d3,
        bundleSha256: 'b3'.repeat(32),
        createdAtMs: T0 + 2 * 3_600_000,
        createdBy: 'dev@example.test',
        message: 'Add reactions',
        parent: d2,
        state: 'active',
      },
      {
        id: d2,
        bundleSha256: 'a2'.repeat(32),
        createdAtMs: T0 + 3_600_000,
        createdBy: 'dev@example.test',
        message: 'Paginate messages:list',
        parent: d1,
        state: 'superseded',
      },
      {
        id: d1,
        bundleSha256: '91'.repeat(32),
        createdAtMs: T0,
        createdBy: 'dev@example.test',
        message: 'Initial deployment',
        state: 'superseded',
      },
    ],
    functions: {
      [d1]: BASE_FUNCTIONS.slice(1),
      [d2]: BASE_FUNCTIONS,
      [d3]: NEXT_FUNCTIONS,
      [d4]: NEXT_FUNCTIONS,
    },
    workers: [
      { node: 'node-1', deploymentId: d3, degraded: false, crashesLastMinute: 0, restarts: 0 },
      {
        node: 'node-2',
        deploymentId: d3,
        degraded: true,
        crashesLastMinute: 5,
        restarts: 7,
        lastCrashMs: T0 + 2 * 3_600_000 + 60_000,
        lastReason: 'live_worker_crashed',
      },
    ],
    logs: [
      line(0, 'r-0001', 'messages:send', 'info', 'sending to channel'),
      line(1, 'r-0002', 'messages:list', 'debug', 'page size 50'),
      line(2, 'r-0003', 'messages:react', 'warn', 'reaction on a deleted message ignored'),
      line(3, 'r-0004', 'messages:send', 'error', 'Uncaught Error: body too long'),
      line(4, 'r-0005', 'messages:list', 'log', 'cursor resumed'),
    ],
  };
}

function line(
  i: number,
  requestId: string,
  fn: string,
  level: LogLine['level'],
  text: string,
): LogLine {
  return { ts: T0 + 2 * 3_600_000 + i * 1000, requestId, function: fn, level, line: text };
}

const abortError = () => new DOMException('aborted', 'AbortError');

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(abortError());
    const t = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      reject(abortError());
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

const FOLLOW_LINES: Array<Pick<LogLine, 'function' | 'level' | 'line'>> = [
  { function: 'messages:list', level: 'debug', line: 'page size 50' },
  { function: 'messages:send', level: 'info', line: 'sending to channel' },
  { function: 'channels:all', level: 'log', line: 'listing channels' },
];

export function createMockAdminApi(
  fixtures: AdminFixtures = defaultFixtures(),
  opts: { followEveryMs?: number; now?: () => number } = {},
): LiveAdminApi {
  const followEveryMs = opts.followEveryMs ?? 1500;
  const now = opts.now ?? Date.now;
  let deployments = fixtures.deployments.map((d) => ({ ...d }));
  const workers = fixtures.workers.map((w) => ({ ...w }));
  let seq = fixtures.logs.length;

  const find = (id: string) => {
    const d = deployments.find((x) => x.id === id);
    if (!d) throw new Error(`deployment ${id} not found`);
    return d;
  };

  return {
    mocked: true,
    async listDeployments() {
      return [...deployments].sort((a, b) => b.createdAtMs - a.createdAtMs);
    },
    async getDeployment(id): Promise<DeploymentDetail> {
      return { deployment: { ...find(id) }, functions: fixtures.functions[id] ?? [] };
    },
    async activateDeployment(id, expectedActive) {
      const target = find(id);
      const active = deployments.find((d) => d.state === 'active' || d.state === 'degraded');
      if ((active?.id ?? undefined) !== expectedActive) throw new DeployConflictError();
      if (target.state === 'failed') {
        throw new Error('A failed deployment cannot be activated.');
      }
      if (active && active.id === id) return { ...target };
      deployments = deployments.map((d) => {
        if (d.id === id) return { ...d, state: 'active', rolledBackFrom: active?.id };
        if (active && d.id === active.id) return { ...d, state: 'superseded' };
        return d;
      });
      // A fresh worker per node comes up healthy on the new deployment.
      for (const w of workers) {
        w.deploymentId = id;
        w.degraded = false;
        w.crashesLastMinute = 0;
      }
      return { ...find(id) };
    },
    async workerHealth() {
      return workers.map((w) => ({ ...w }));
    },
    async *tailLogs(req: TailLogsRequest, signal: AbortSignal) {
      const match = (l: LogLine) =>
        (!req.function || l.function === req.function) &&
        (req.sinceMs === undefined || l.ts >= req.sinceMs);
      for (const l of fixtures.logs) {
        if (signal.aborted) return;
        if (match(l)) yield l;
      }
      if (!req.follow) return;
      let i = 0;
      while (!signal.aborted) {
        try {
          await sleep(followEveryMs, signal);
        } catch {
          return;
        }
        const base = FOLLOW_LINES[i++ % FOLLOW_LINES.length];
        if (!base) return;
        seq += 1;
        const l: LogLine = {
          ...base,
          ts: now(),
          requestId: `r-${String(seq).padStart(4, '0')}`,
        };
        if (match(l)) yield l;
      }
    },
  };
}
