// A fixture-backed DeployClient. It simulates the pipeline on one interval timer that
// runs while anyone is watching: running stages advance, some validations fail, new
// deployments start, rollbacks run, promotions move along and carry secret events
// (key names only). Seeded, so tests see the same sequence every time.

import type { SeriesPoint } from '@loams/charts';
import { rng } from '@loams/ui';
import { deriveState } from './model.js';
import {
  type DeployClient,
  type DeployEvent,
  type DeployMetric,
  type Deployment,
  type EnvName,
  type LogLine,
  PROMOTION_STEPS,
  type Promotion,
  type PromotionStepKind,
  type SecretEvent,
  STAGE_KINDS,
  type Stage,
  type StageKind,
} from './types.js';

export interface MockOptions {
  /** How often the simulation advances and metric streams emit. */
  intervalMs?: number;
  seed?: number;
  now?: () => number;
}

const SERVICES = ['api', 'web', 'billing-worker', 'ingest', 'search'];
const AUTHORS = ['ana', 'kofi', 'mei', 'ravi', 'sam'];
const MINUTE = 60_000;

// ---- fixtures ----------------------------------------------------------------------------

function stages(states: Partial<Record<StageKind, Partial<Stage>>>, t0: number): Stage[] {
  let t = t0;
  return STAGE_KINDS.map((kind) => {
    const s: Stage = { kind, state: 'pending', progress: 0, ...states[kind] };
    if (s.state !== 'pending' && s.state !== 'skipped' && s.startedAt === undefined) {
      s.startedAt = t;
      t += 20_000 + kind.length * 4_000;
      if (s.state !== 'running') {
        s.finishedAt = t;
        s.progress = 1;
      }
    }
    return s;
  });
}

export function fixtureDeployments(now: number): Deployment[] {
  const running = stages(
    { build: { state: 'succeeded' }, upload: { state: 'running', progress: 0.4 } },
    now - 50_000,
  );
  const live = stages(
    {
      build: { state: 'succeeded' },
      upload: { state: 'succeeded' },
      validate: { state: 'succeeded' },
      activate: { state: 'succeeded' },
    },
    now - 30 * MINUTE,
  );
  const failed = stages(
    {
      build: { state: 'succeeded' },
      upload: { state: 'succeeded' },
      validate: { state: 'failed', message: 'Health check /ready returned 503 three times' },
      activate: { state: 'skipped' },
    },
    now - 2 * 60 * MINUTE,
  );
  return [
    {
      id: 'dep-1042',
      service: 'api',
      environment: 'dev',
      version: 'v1.42.0',
      previousVersion: 'v1.41.3',
      commit: '9f2c1ab',
      author: 'mei',
      createdAt: now - 50_000,
      state: deriveState(running),
      stages: running,
    },
    {
      id: 'dep-1041',
      service: 'web',
      environment: 'main',
      version: 'v3.8.1',
      previousVersion: 'v3.8.0',
      commit: '41d07e2',
      author: 'kofi',
      createdAt: now - 30 * MINUTE,
      state: deriveState(live),
      stages: live,
    },
    {
      id: 'dep-1040',
      service: 'billing-worker',
      environment: 'dev',
      version: 'v0.19.0',
      previousVersion: 'v0.18.4',
      commit: 'c3e9b70',
      author: 'ravi',
      createdAt: now - 2 * 60 * MINUTE,
      state: deriveState(failed),
      stages: failed,
    },
  ];
}

type Planned = Record<PromotionStepKind, Omit<SecretEvent, 'id' | 'at' | 'step'>[]>;

function plan(p: Partial<Planned>): Planned {
  return Object.fromEntries(PROMOTION_STEPS.map((k) => [k, p[k] ?? []])) as Planned;
}

const key = (kind: SecretEvent['kind'], k: string, env: EnvName) => ({ kind, key: k, env });

export function fixturePromotions(now: number): { promotion: Promotion; planned: Planned }[] {
  const done = (kind: PromotionStepKind, at: number, actor: string, ref?: string) => ({
    kind,
    state: 'succeeded' as const,
    at,
    actor,
    ref,
  });
  const flowA: Planned = plan({
    promote_dev: [
      key('promoted', 'STRIPE_WEBHOOK_SECRET', 'dev'),
      key('added', 'FEATURE_LEDGER_V2', 'dev'),
    ],
    promote_main: [
      key('promoted', 'STRIPE_WEBHOOK_SECRET', 'main'),
      key('rotated', 'DATABASE_PASSWORD', 'main'),
      key('added', 'FEATURE_LEDGER_V2', 'main'),
    ],
  });
  const flowB: Planned = plan({
    promote_dev: [key('rotated', 'SEARCH_API_KEY', 'dev'), key('removed', 'LEGACY_TOKEN', 'dev')],
    promote_main: [
      key('rotated', 'SEARCH_API_KEY', 'main'),
      key('removed', 'LEGACY_TOKEN', 'main'),
    ],
  });
  const a: Promotion = {
    id: 'prm-88',
    workspace: 'ws/mei-ledger-v2',
    title: 'Ledger v2 behind a flag',
    steps: [
      done('workspace', now - 90 * MINUTE, 'mei'),
      done('review', now - 60 * MINUTE, 'kofi', 'review 312'),
      done('merge_dev', now - 40 * MINUTE, 'mei', '7b1e0c4'),
      done('promote_dev', now - 38 * MINUTE, 'loams'),
      { kind: 'merge_main', state: 'running', at: now - 30_000, actor: 'mei' },
      { kind: 'promote_main', state: 'pending' },
    ],
    secretEvents: [],
  };
  const b: Promotion = {
    id: 'prm-87',
    workspace: 'ws/ravi-search-keys',
    title: 'Rotate the search key, drop the legacy token',
    steps: PROMOTION_STEPS.map((kind, i) =>
      done(kind, now - (300 - i * 20) * MINUTE, i % 2 ? 'loams' : 'ravi'),
    ),
    secretEvents: [],
  };
  // Events for steps already done are already on the promotion.
  for (const [p, planned] of [
    [a, flowA],
    [b, flowB],
  ] as const) {
    for (const step of p.steps) {
      if (step.state !== 'succeeded') continue;
      for (const [i, e] of planned[step.kind].entries()) {
        p.secretEvents.push({
          ...e,
          id: `${p.id}-${step.kind}-${i}`,
          step: step.kind,
          at: step.at ?? now,
        });
      }
      planned[step.kind] = [];
    }
  }
  return [
    { promotion: a, planned: flowA },
    { promotion: b, planned: flowB },
  ];
}

const LOG_TEXT: Record<StageKind, string[]> = {
  build: [
    'Resolving dependencies',
    'Compiling 214 units',
    'Image layers cached: 11 of 14',
    'Built',
  ],
  upload: ['Pushing artifact', 'Verifying digest', 'Uploaded'],
  validate: ['Starting a canary instance', 'Waiting for /ready', 'Running smoke checks'],
  activate: ['Shifting traffic 10%', 'Shifting traffic 50%', 'Shifting traffic 100%', 'Active'],
  rollback: ['Restoring the previous version', 'Shifting traffic back', 'Rolled back'],
};

// ---- streaming helpers ---------------------------------------------------------------------

/** A push queue drained by an async iterator that ends when `signal` aborts. */
function channel<T>(signal: AbortSignal) {
  const queue: T[] = [];
  let wake: (() => void) | undefined;
  const push = (v: T) => {
    queue.push(v);
    wake?.();
    wake = undefined;
  };
  // One listener for the channel's life, not one per idle wait.
  signal.addEventListener('abort', () => wake?.(), { once: true });
  async function* iterate(): AsyncGenerator<T> {
    while (!signal.aborted) {
      const next = queue.shift();
      if (next !== undefined) {
        yield next;
        continue;
      }
      await new Promise<void>((resolve) => {
        wake = resolve;
      });
    }
  }
  return { push, iterate };
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const clone = <T>(v: T): T => structuredClone(v);

// ---- the mock ------------------------------------------------------------------------------

export interface MockDeployClient extends DeployClient {
  /** Advances the simulation one step; the timer calls it, tests may too. */
  tick(): void;
}

export function createMockDeployClient(opts: MockOptions = {}): MockDeployClient {
  const intervalMs = opts.intervalMs ?? 1000;
  const now = opts.now ?? Date.now;
  const random = rng(opts.seed ?? 7);
  let deployments = fixtureDeployments(now());
  const failing = new Set<string>();
  const promotions = fixturePromotions(now());
  let serial = 1043;
  let ticks = 0;

  const deploySubs = new Set<(e: DeployEvent) => void>();
  const promoSubs = new Set<(p: Promotion[]) => void>();
  let timer: ReturnType<typeof setInterval> | undefined;

  const emit = (e: DeployEvent) => {
    for (const fn of deploySubs) fn(clone(e));
  };
  const emitPromotions = () => {
    const snap = promotions.map((p) => clone(p.promotion));
    for (const fn of promoSubs) fn(snap);
  };
  const retime = () => {
    const watching = deploySubs.size + promoSubs.size > 0;
    if (watching && !timer) timer = setInterval(tick, intervalMs);
    if (!watching && timer) {
      clearInterval(timer);
      timer = undefined;
    }
  };

  function setStage(d: Deployment, stage: Stage) {
    d.stages = d.stages.map((s) => (s.kind === stage.kind ? stage : s));
    d.state = deriveState(d.stages);
    emit({ kind: 'stage', deploymentId: d.id, stage, state: d.state });
  }

  function advance(d: Deployment) {
    const t = now();
    const running = d.stages.find((s) => s.state === 'running');
    if (!running) return;
    const progress = Math.min(1, running.progress + 0.15 + random() * 0.25);
    if (progress < 1) {
      setStage(d, { ...running, progress });
      return;
    }
    if (running.kind === 'validate' && failing.has(d.id)) {
      setStage(d, {
        ...running,
        progress: 1,
        state: 'failed',
        finishedAt: t,
        message: 'Smoke check POST /v1/ping timed out after 30 s',
      });
      const activate = d.stages.find((s) => s.kind === 'activate');
      if (activate) setStage(d, { ...activate, state: 'skipped' });
      return;
    }
    setStage(d, { ...running, progress: 1, state: 'succeeded', finishedAt: t });
    if (running.kind === 'rollback') return;
    const next = d.stages.find((s) => s.kind !== 'rollback' && s.state === 'pending');
    if (next) setStage(d, { ...next, state: 'running', startedAt: t, progress: 0 });
  }

  function startDeployment() {
    const t = now();
    const service = SERVICES[Math.floor(random() * SERVICES.length)] ?? 'api';
    const prior = deployments.find((d) => d.service === service);
    const minor = Math.floor(random() * 40);
    const d: Deployment = {
      id: `dep-${serial++}`,
      service,
      environment: random() < 0.6 ? 'dev' : 'main',
      version: `v1.${minor}.${serial % 10}`,
      previousVersion: prior?.version,
      commit: Math.floor(random() * 0xfffffff)
        .toString(16)
        .padStart(7, '0'),
      author: AUTHORS[Math.floor(random() * AUTHORS.length)] ?? 'ana',
      createdAt: t,
      state: 'running',
      stages: STAGE_KINDS.map((kind) =>
        kind === 'build'
          ? { kind, state: 'running', progress: 0, startedAt: t }
          : { kind, state: 'pending', progress: 0 },
      ),
    };
    if (random() < 0.2) failing.add(d.id);
    deployments = [d, ...deployments].slice(0, 8);
    emit({ kind: 'deployment', deployment: d });
  }

  function advancePromotions() {
    let changed = false;
    for (const entry of promotions) {
      const p = entry.promotion;
      const i = p.steps.findIndex((s) => s.state === 'running');
      const step = p.steps[i];
      if (!step) continue;
      const t = now();
      const ref = step.ref ?? (step.kind.startsWith('merge') ? 'e4a90d1' : undefined);
      p.steps[i] = { ...step, state: 'succeeded', at: t, ref };
      for (const [n, e] of entry.planned[step.kind].entries()) {
        p.secretEvents.push({ ...e, id: `${p.id}-${step.kind}-${n}`, step: step.kind, at: t });
      }
      entry.planned[step.kind] = [];
      const next = p.steps[i + 1];
      if (next) p.steps[i + 1] = { ...next, state: 'running', at: t, actor: 'loams' };
      changed = true;
    }
    if (changed) emitPromotions();
  }

  function tick() {
    ticks++;
    for (const d of deployments)
      if (d.state === 'running' || d.state === 'rolling_back') advance(d);
    if (ticks % 8 === 0) startDeployment();
    if (ticks % 5 === 0) advancePromotions();
  }

  function metricValue(metric: DeployMetric, r: () => number, prev: number) {
    if (metric === 'frequency') return Math.max(1, Math.round(prev + (r() - 0.5) * 4));
    return Math.max(0, Math.min(60, Math.round((prev + (r() - 0.5) * 8) * 10) / 10));
  }
  const series = (metric: DeployMetric) => (metric === 'frequency' ? 'deploys/h' : 'failure %');
  const start = (metric: DeployMetric) => (metric === 'frequency' ? 9 : 12);

  return {
    tick,

    async *watchDeployments(signal) {
      const ch = channel<DeployEvent>(signal);
      deploySubs.add(ch.push);
      retime();
      const stop = () => {
        deploySubs.delete(ch.push);
        retime();
      };
      signal.addEventListener('abort', stop, { once: true });
      try {
        yield { kind: 'snapshot', deployments: clone(deployments) };
        yield* ch.iterate();
      } finally {
        stop();
      }
    },

    async rollback(id) {
      const d = deployments.find((x) => x.id === id);
      if (!d) throw new Error(`No deployment ${id}`);
      if (!d.previousVersion) throw new Error(`${id} has no previous version to roll back to`);
      const rb = d.stages.find((s) => s.kind === 'rollback');
      if (rb?.state !== 'pending') throw new Error(`${id} is already rolling back`);
      setStage(d, { ...rb, state: 'running', startedAt: now(), progress: 0 });
    },

    async stageLogs(id, stage) {
      const d = deployments.find((x) => x.id === id);
      const s = d?.stages.find((x) => x.kind === stage);
      if (!d || !s || s.startedAt === undefined) return [];
      const lines = LOG_TEXT[stage];
      const shown =
        s.state === 'running' ? Math.max(1, Math.ceil(lines.length * s.progress)) : lines.length;
      const out: LogLine[] = lines.slice(0, shown).map((text, i) => ({
        at: (s.startedAt as number) + i * 3_000,
        level: 'info',
        text,
      }));
      if (s.state === 'failed' && s.message) {
        out.push({ at: s.finishedAt ?? now(), level: 'error', text: s.message });
      }
      return out;
    },

    async *watchPromotions(signal) {
      const ch = channel<Promotion[]>(signal);
      promoSubs.add(ch.push);
      retime();
      const stop = () => {
        promoSubs.delete(ch.push);
        retime();
      };
      signal.addEventListener('abort', stop, { once: true });
      try {
        yield promotions.map((p) => clone(p.promotion));
        yield* ch.iterate();
      } finally {
        stop();
      }
    },

    async metricHistory(metric, windowMs) {
      const r = rng((opts.seed ?? 7) + (metric === 'frequency' ? 1 : 2));
      const t1 = now();
      const out: SeriesPoint[] = [];
      let v = start(metric);
      for (let t = t1 - windowMs; t <= t1; t += MINUTE) {
        v = metricValue(metric, r, v);
        out.push({ t, v, series: series(metric) });
      }
      return out;
    },

    metricStream(metric) {
      const seed = (opts.seed ?? 7) + (metric === 'frequency' ? 11 : 12);
      return {
        async *[Symbol.asyncIterator]() {
          const r = rng(seed);
          let v = start(metric);
          // Ends when the consumer stops iterating (`return()` on the generator).
          while (true) {
            await sleep(intervalMs);
            v = metricValue(metric, r, v);
            yield [{ t: now(), v, series: series(metric) }];
          }
        },
      };
    },
  };
}
