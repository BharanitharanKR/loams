// The execution graph of one durable run: its steps as a DAG.
//
// A Resonate run is a root promise and the promises it spawns (runs.ts). The
// server stores promises, not steps, so some of what this model carries is not
// served yet: attempts (the SDK retries inside one task and keeps no record per
// attempt), step latencies before settlement, and a push stream of changes.
// `fromPromises` builds what the promises do say; the sample source
// (sample.ts) fills the rest from fixtures until the engine serves it.

import { decodeValue, type PromiseRecord, type Value } from '../envelope.js';
import { TAG_ORIGIN, TAG_PARENT } from '../runs.js';

export type StepStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'retrying' | 'sleeping';
/** `workflow` is the run's root; `timer` is a durable sleep; `await` waits on an outside resolve. */
export type StepKind = 'workflow' | 'step' | 'rpc' | 'timer' | 'await';

export const STEP_STATUSES: StepStatus[] = [
  'pending',
  'running',
  'retrying',
  'sleeping',
  'succeeded',
  'failed',
];

export interface Attempt {
  /** 1-based. */
  n: number;
  status: StepStatus;
  startedAt: number;
  endedAt?: number;
  error?: string;
}

export interface Step {
  id: string;
  name: string;
  kind: StepKind;
  status: StepStatus;
  /** The step that spawned this one (none for the workflow). */
  parent?: string;
  /** Steps this one ran after. Siblings with the same `after` ran in parallel. */
  after: string[];
  createdAt: number;
  startedAt?: number;
  endedAt?: number;
  /** A timer's wake time, or an await's deadline. */
  wakeAt?: number;
  input?: Value;
  output?: Value;
  error?: string;
  attempts: Attempt[];
}

export interface RunGraph {
  runId: string;
  name: string;
  status: StepStatus;
  startedAt: number;
  endedAt?: number;
  steps: Step[];
  /** True when the graph came from fixtures, not the engine. */
  sample?: boolean;
}

/** A change on a watched run. Snapshots replace; a step event upserts one step. */
export type RunEvent =
  | { type: 'snapshot'; run: RunGraph }
  | { type: 'step'; step: Step }
  | { type: 'run'; status: StepStatus; endedAt?: number };

export function isSettled(s: StepStatus): boolean {
  return s === 'succeeded' || s === 'failed';
}

export function applyEvent(run: RunGraph | undefined, e: RunEvent): RunGraph | undefined {
  if (e.type === 'snapshot') return e.run;
  if (!run) return run;
  if (e.type === 'run') {
    return {
      ...run,
      status: e.status,
      ...(e.endedAt !== undefined ? { endedAt: e.endedAt } : {}),
    };
  }
  const i = run.steps.findIndex((s) => s.id === e.step.id);
  const steps =
    i < 0 ? [...run.steps, e.step] : run.steps.map((s, j) => (j === i ? e.step : s));
  return { ...run, steps };
}

/** The edges of the DAG, as [from, to] pairs. A step with no `after` hangs off its parent. */
export function edgesOf(run: RunGraph): [string, string][] {
  const ids = new Set(run.steps.map((s) => s.id));
  const out: [string, string][] = [];
  for (const s of run.steps) {
    const from = s.after.filter((a) => ids.has(a));
    if (from.length) for (const a of from) out.push([a, s.id]);
    else if (s.parent && ids.has(s.parent)) out.push([s.parent, s.id]);
  }
  return out;
}

/** Milliseconds a step has taken so far (to `now` while unsettled). */
export function stepLatency(s: Step, now = Date.now()): number | undefined {
  if (s.startedAt === undefined) return undefined;
  return Math.max(0, (s.endedAt ?? now) - s.startedAt);
}

export function runProgress(run: RunGraph): { done: number; total: number } {
  const steps = run.steps.filter((s) => s.kind !== 'workflow');
  return { done: steps.filter((s) => isSettled(s.status)).length, total: steps.length };
}

// ---- from Resonate promises ----

/**
 * Tags the pinned SDKs put on a durable sleep's promise. The TS SDK writes
 * `resonate:timeout`, the Rust SDK `resonate:timer`; either marks a timer.
 */
const TIMER_TAGS = ['resonate:timer', 'resonate:timeout'];
/** A promise resolved from outside the run (an await on a latent promise). */
const TAG_EXTERNAL = 'resonate:external';
const TAG_TARGET = 'resonate:target';

function nameOf(p: PromiseRecord): string {
  const d = decodeValue(p.param);
  if (d.kind === 'json') {
    try {
      const v = JSON.parse(d.text);
      if (v && typeof v.func === 'string') return v.func;
    } catch {
      // not an SDK invocation; fall through to the id
    }
  }
  const tail = p.id.split(/[./]/).pop();
  return tail || p.id;
}

function kindOf(p: PromiseRecord, isRoot: boolean): StepKind {
  if (isRoot) return 'workflow';
  if (TIMER_TAGS.some((t) => p.tags[t] === 'true')) return 'timer';
  if (p.tags[TAG_EXTERNAL] === 'true') return 'await';
  if (p.tags[TAG_TARGET]) return 'rpc';
  return 'step';
}

function statusOf(p: PromiseRecord, kind: StepKind): StepStatus {
  if (p.state === 'pending') {
    if (kind === 'timer') return 'sleeping';
    if (kind === 'await') return 'pending';
    return 'running';
  }
  if (p.state === 'resolved') return 'succeeded';
  // A timer settles by timing out; that is it waking, not failing.
  if (kind === 'timer' && p.state === 'rejected_timedout') return 'succeeded';
  return 'failed';
}

function errorOf(p: PromiseRecord): string | undefined {
  if (p.state === 'rejected_canceled') return 'canceled';
  if (p.state === 'rejected_timedout') return 'timed out';
  if (p.state !== 'rejected') return undefined;
  const d = decodeValue(p.value);
  if (d.kind === 'empty') return 'rejected';
  return d.kind === 'json' ? d.text : (d.text ?? d.raw);
}

/**
 * The run graph of the promises of one run (all tagged with the same origin).
 * Ordering between siblings is inferred from time: a sibling created after
 * others settled ran after them; siblings whose lifetimes overlap ran in
 * parallel. One attempt per step: the server keeps no attempt history.
 */
export function fromPromises(runId: string, promises: PromiseRecord[]): RunGraph | undefined {
  const mine = promises.filter((p) => p.id === runId || p.tags[TAG_ORIGIN] === runId);
  const root = mine.find((p) => p.id === runId);
  if (!root) return undefined;
  const ids = new Set(mine.map((p) => p.id));
  const parentOf = (p: PromiseRecord): string | undefined => {
    if (p.id === runId) return undefined;
    const t = p.tags[TAG_PARENT];
    return t && t !== p.id && ids.has(t) ? t : runId;
  };
  const byParent = new Map<string, PromiseRecord[]>();
  for (const p of mine) {
    const parent = parentOf(p);
    if (!parent) continue;
    const list = byParent.get(parent) ?? [];
    list.push(p);
    byParent.set(parent, list);
  }
  const after = new Map<string, string[]>();
  for (const [parent, kids] of byParent) {
    kids.sort((a, b) => a.createdAt - b.createdAt || a.id.localeCompare(b.id));
    for (const k of kids) {
      const before = kids.filter(
        (o) => o !== k && o.settledAt !== undefined && o.settledAt <= k.createdAt,
      );
      // Keep only the latest wave: drop siblings another candidate already ran after.
      const covered = new Set(before.flatMap((o) => after.get(o.id) ?? []));
      const direct = before.filter((o) => !covered.has(o.id)).map((o) => o.id);
      after.set(k.id, direct.length ? direct : [parent]);
    }
  }
  const steps: Step[] = mine.map((p) => {
    const isRoot = p.id === runId;
    const kind = kindOf(p, isRoot);
    const status = statusOf(p, kind);
    const error = errorOf(p);
    const parent = parentOf(p);
    return {
      id: p.id,
      name: nameOf(p),
      kind,
      status,
      ...(parent ? { parent } : {}),
      after: after.get(p.id) ?? [],
      createdAt: p.createdAt,
      startedAt: p.createdAt,
      ...(p.settledAt !== undefined ? { endedAt: p.settledAt } : {}),
      ...(kind === 'timer' || kind === 'await' ? { wakeAt: p.timeoutAt } : {}),
      input: p.param,
      output: p.value,
      ...(error ? { error } : {}),
      attempts: [
        {
          n: 1,
          status,
          startedAt: p.createdAt,
          ...(p.settledAt !== undefined ? { endedAt: p.settledAt } : {}),
          ...(error ? { error } : {}),
        },
      ],
    };
  });
  const rootStep = steps.find((s) => s.id === runId) as Step;
  return {
    runId,
    name: rootStep.name,
    status: rootStep.status,
    startedAt: root.createdAt,
    ...(root.settledAt !== undefined ? { endedAt: root.settledAt } : {}),
    steps,
  };
}
