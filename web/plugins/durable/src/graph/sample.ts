// Fixture-backed sample runs and a sample metric stream. They stand in for
// what the engine does not serve yet (attempt history, a run change stream,
// run-duration metrics), so the graph can be seen working without a worker.
// Each sample replays from its start whenever it is watched.

import type { SeriesPoint } from '@loams/charts';
import { toBase64, type Value } from '../envelope.js';
import type { Attempt, RunEvent, RunGraph, Step, StepKind, StepStatus } from './model.js';
import { delay, type RunGraphSource, type RunSummary } from './source.js';

export interface PlanStep {
  id: string;
  name: string;
  kind: Exclude<StepKind, 'workflow'>;
  /** Steps it waits for; empty means it starts with the workflow. */
  after: string[];
  /** Ticks per attempt (a timer's sleep, an await's wait). */
  ticks: number;
  /** Failed attempts before the one that succeeds. */
  fails?: number;
  /** Every attempt fails; `attempts` of them. */
  exhausted?: { attempts: number };
  error?: string;
  input?: unknown;
  output?: unknown;
}

export interface Plan {
  runId: string;
  name: string;
  input: unknown;
  output?: unknown;
  steps: PlanStep[];
}

/** Ticks between a failed attempt and the next. */
export const BACKOFF_TICKS = 1;

const json = (v: unknown): Value | undefined =>
  v === undefined ? undefined : { headers: {}, data: toBase64(JSON.stringify(v)) };

interface Timing {
  ready: number;
  end: number;
  attempts: number;
  ok: boolean;
}

function timings(plan: Plan): Map<string, Timing | undefined> {
  const out = new Map<string, Timing | undefined>();
  for (const s of plan.steps) {
    const deps = s.after.map((a) => out.get(a));
    // Downstream of a failure (or of something that never ran) never starts.
    if (deps.some((d) => !d?.ok)) {
      out.set(s.id, undefined);
      continue;
    }
    const ready = Math.max(0, ...deps.map((d) => (d as Timing).end));
    const attempts = s.exhausted ? s.exhausted.attempts : (s.fails ?? 0) + 1;
    const end = ready + attempts * s.ticks + (attempts - 1) * BACKOFF_TICKS;
    out.set(s.id, { ready, end, attempts, ok: !s.exhausted });
  }
  return out;
}

/** The last tick at which anything changes. */
export function finalTick(plan: Plan): number {
  const t = [...timings(plan).values()].filter((x): x is Timing => !!x);
  const failed = t.filter((x) => !x.ok).map((x) => x.end);
  return failed.length ? Math.min(...failed) : Math.max(0, ...t.map((x) => x.end));
}

const running = (k: StepKind): StepStatus =>
  k === 'timer' ? 'sleeping' : k === 'await' ? 'pending' : 'running';

/** The run as it stands `tick` ticks after it started at `t0`. */
export function simulate(plan: Plan, tick: number, t0: number, tickMs: number): RunGraph {
  const at = (k: number) => t0 + k * tickMs;
  const time = timings(plan);
  const stop = finalTick(plan);
  const steps: Step[] = plan.steps.map((s): Step => {
    const t = time.get(s.id);
    const base = {
      id: s.id,
      name: s.name,
      kind: s.kind,
      parent: plan.runId,
      after: s.after.length ? s.after : [plan.runId],
      ...(s.input !== undefined ? { input: json(s.input) } : {}),
    };
    // Not started, or cut short by a failure elsewhere.
    if (!t || tick < t.ready || t.ready > stop) {
      return { ...base, status: 'pending', createdAt: at(t?.ready ?? stop), attempts: [] };
    }
    const attempts: Attempt[] = [];
    let status: StepStatus = running(s.kind);
    for (let k = 0; k < t.attempts; k++) {
      const start = t.ready + k * (s.ticks + BACKOFF_TICKS);
      if (tick < start) {
        status = 'retrying';
        break;
      }
      const end = start + s.ticks;
      const last = k === t.attempts - 1;
      const fails = !t.ok || !last;
      if (tick < end) {
        attempts.push({ n: k + 1, status: running(s.kind), startedAt: at(start) });
        status = running(s.kind);
        break;
      }
      attempts.push({
        n: k + 1,
        status: fails ? 'failed' : 'succeeded',
        startedAt: at(start),
        endedAt: at(end),
        ...(fails ? { error: s.error ?? 'failed' } : {}),
      });
      status = last ? (fails ? 'failed' : 'succeeded') : 'retrying';
    }
    const done = status === 'succeeded' || status === 'failed';
    const lastAttempt = attempts[attempts.length - 1];
    return {
      ...base,
      status,
      createdAt: at(t.ready),
      startedAt: at(t.ready),
      ...(done ? { endedAt: at(t.end) } : {}),
      ...(s.kind === 'timer' || s.kind === 'await' ? { wakeAt: at(t.ready + s.ticks) } : {}),
      ...(status === 'succeeded' && s.output !== undefined ? { output: json(s.output) } : {}),
      ...(lastAttempt?.error ? { error: lastAttempt.error } : {}),
      attempts,
    };
  });
  const failed = steps.some((s) => s.status === 'failed');
  const allDone = steps.every((s) => s.status === 'succeeded');
  const settled = (failed || allDone) && tick >= stop;
  const status: StepStatus = !settled ? 'running' : failed ? 'failed' : 'succeeded';
  const root: Step = {
    id: plan.runId,
    name: plan.name,
    kind: 'workflow',
    status,
    after: [],
    createdAt: t0,
    startedAt: t0,
    ...(settled ? { endedAt: at(stop) } : {}),
    input: json(plan.input),
    ...(status === 'succeeded' && plan.output !== undefined ? { output: json(plan.output) } : {}),
    ...(failed && settled ? { error: steps.find((s) => s.status === 'failed')?.error } : {}),
    attempts: [
      {
        n: 1,
        status,
        startedAt: t0,
        ...(settled ? { endedAt: at(stop) } : {}),
      },
    ],
  };
  return {
    runId: plan.runId,
    name: plan.name,
    status,
    startedAt: t0,
    ...(settled ? { endedAt: at(stop) } : {}),
    steps: [root, ...steps],
    sample: true,
  };
}

/** The events that turn `prev` into `next`: changed steps, then the run's status. */
export function diff(prev: RunGraph, next: RunGraph): RunEvent[] {
  const before = new Map(prev.steps.map((s) => [s.id, JSON.stringify(s)]));
  const out: RunEvent[] = next.steps
    .filter((s) => before.get(s.id) !== JSON.stringify(s))
    .map((step): RunEvent => ({ type: 'step', step }));
  if (prev.status !== next.status || prev.endedAt !== next.endedAt) {
    out.push({
      type: 'run',
      status: next.status,
      ...(next.endedAt !== undefined ? { endedAt: next.endedAt } : {}),
    });
  }
  return out;
}

export const SAMPLE_PLANS: Plan[] = [
  {
    runId: 'order-7f3a',
    name: 'fulfil-order',
    input: { orderId: 'ord_7f3a', items: 3, total: 129.4 },
    output: { shipped: true, trackingId: 'trk_51c0' },
    steps: [
      {
        id: 'order-7f3a.0',
        name: 'validate-order',
        kind: 'step',
        after: [],
        ticks: 2,
        input: { orderId: 'ord_7f3a' },
        output: { valid: true },
      },
      {
        id: 'order-7f3a.1',
        name: 'reserve-inventory',
        kind: 'rpc',
        after: ['order-7f3a.0'],
        ticks: 3,
        input: { skus: ['sku_1', 'sku_2', 'sku_9'] },
        output: { reserved: 3 },
      },
      {
        id: 'order-7f3a.2',
        name: 'charge-card',
        kind: 'step',
        after: ['order-7f3a.0'],
        ticks: 2,
        fails: 2,
        error: 'payment gateway: upstream timeout after 2000 ms',
        input: { amount: 129.4, currency: 'EUR' },
        output: { chargeId: 'ch_0921' },
      },
      {
        id: 'order-7f3a.3',
        name: 'fraud-score',
        kind: 'rpc',
        after: ['order-7f3a.0'],
        ticks: 4,
        input: { orderId: 'ord_7f3a' },
        output: { score: 0.04 },
      },
      {
        id: 'order-7f3a.4',
        name: 'await manager-approval',
        kind: 'await',
        after: ['order-7f3a.1', 'order-7f3a.2', 'order-7f3a.3'],
        ticks: 3,
        output: { approvedBy: 'ops-oncall' },
      },
      {
        id: 'order-7f3a.5',
        name: 'sleep cool-off',
        kind: 'timer',
        after: ['order-7f3a.4'],
        ticks: 4,
      },
      {
        id: 'order-7f3a.6',
        name: 'ship',
        kind: 'rpc',
        after: ['order-7f3a.5'],
        ticks: 2,
        input: { carrier: 'post' },
        output: { trackingId: 'trk_51c0' },
      },
      {
        id: 'order-7f3a.7',
        name: 'send-receipt',
        kind: 'step',
        after: ['order-7f3a.5'],
        ticks: 1,
        output: { sent: true },
      },
    ],
  },
  {
    runId: 'report-2210',
    name: 'nightly-report',
    input: { day: '2026-10-10' },
    steps: [
      {
        id: 'report-2210.0',
        name: 'load-sales',
        kind: 'step',
        after: [],
        ticks: 2,
        output: { rows: 48210 },
      },
      {
        id: 'report-2210.1',
        name: 'aggregate-eu',
        kind: 'step',
        after: ['report-2210.0'],
        ticks: 3,
        output: { regions: 14 },
      },
      {
        id: 'report-2210.2',
        name: 'aggregate-us',
        kind: 'step',
        after: ['report-2210.0'],
        ticks: 2,
        exhausted: { attempts: 3 },
        error: 'division by zero in currency rollup (region us-west)',
      },
      {
        id: 'report-2210.3',
        name: 'publish',
        kind: 'step',
        after: ['report-2210.1', 'report-2210.2'],
        ticks: 1,
      },
    ],
  },
];

export interface SampleOptions {
  tickMs?: number;
  plans?: Plan[];
  now?: () => number;
}

export function mockRunSource(opts: SampleOptions = {}): RunGraphSource {
  const tickMs = opts.tickMs ?? 900;
  const plans = opts.plans ?? SAMPLE_PLANS;
  const now = opts.now ?? Date.now;
  return {
    async listRuns(): Promise<RunSummary[]> {
      const t = now();
      return plans.map((p) => ({
        runId: p.runId,
        name: p.name,
        status: simulate(p, finalTick(p), t, tickMs).status,
        startedAt: t,
        sample: true,
      }));
    },
    async *watchRun(runId, signal) {
      const plan = plans.find((p) => p.runId === runId);
      if (!plan) throw new Error(`no sample run ${runId}`);
      const t0 = now();
      const last = finalTick(plan);
      let prev = simulate(plan, 0, t0, tickMs);
      yield { type: 'snapshot', run: prev };
      for (let tick = 1; tick <= last && !signal.aborted; tick++) {
        await delay(tickMs, signal);
        if (signal.aborted) return;
        const next = simulate(plan, tick, t0, tickMs);
        for (const e of diff(prev, next)) yield e;
        prev = next;
      }
    },
    metrics(signal) {
      return mockMetricStream({ intervalMs: tickMs * 2, now, ...(signal ? { signal } : {}) });
    },
  };
}

/** Run durations per workflow as a seeded random walk: a backfill, then a point per interval. */
export async function* mockMetricStream(opts: {
  intervalMs?: number;
  signal?: AbortSignal;
  now?: () => number;
  seed?: number;
  backfill?: number;
}): AsyncGenerator<SeriesPoint[]> {
  const intervalMs = opts.intervalMs ?? 2_000;
  const now = opts.now ?? Date.now;
  let s = (opts.seed ?? 7) >>> 0;
  const rand = () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 4294967296;
  };
  const level: Record<string, number> = { 'fulfil-order': 4200, 'nightly-report': 1800 };
  const step = (t: number): SeriesPoint[] =>
    Object.keys(level).map((series) => {
      const cur = level[series] as number;
      const v = Math.max(200, Math.round(cur * (0.9 + rand() * 0.2)));
      level[series] = v;
      return { t, v, series };
    });
  const start = now();
  const back = opts.backfill ?? 20;
  const first: SeriesPoint[] = [];
  for (let i = back; i > 0; i--) first.push(...step(start - i * intervalMs));
  if (first.length) yield first;
  while (!opts.signal?.aborted) {
    await delay(intervalMs, opts.signal);
    if (opts.signal?.aborted) return;
    yield step(now());
  }
}
