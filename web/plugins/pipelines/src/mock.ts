// A fixture-backed `PipelinesClient`, for the page until the flow service ships and for
// tests. Deploys are held in memory; metrics are synthetic series that emit one point per
// node per interval. Secrets are checked for presence and then dropped: the mock never
// keeps a value.

import type { Deployment, Metric, MetricPoint, PipelinesClient } from './client.js';
import { FIXTURE_CATALOG } from './fixtures.js';
import type { ConnectorDef } from './model.js';
import type { PipelineSpec } from './spec.js';
import type { Issue } from './validate.js';

export interface MockOptions {
  catalog?: readonly ConnectorDef[];
  /** Milliseconds between metric points. */
  intervalMs?: number;
  /** Milliseconds a deploy takes. */
  deployMs?: number;
  now?: () => number;
  /** 0..1, for jitter; a seeded generator in tests. */
  random?: () => number;
  /** Makes the next deploy fail with this message. */
  failDeploy?: string;
}

const sleep = (ms: number, signal: { stopped: boolean }) =>
  new Promise<void>((resolve) => {
    if (signal.stopped) return resolve();
    setTimeout(resolve, ms);
  });

/** A small, deterministic hash so each node's series has its own level. */
function seedOf(s: string): number {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619);
  return ((h >>> 0) % 1000) / 1000;
}

export interface MockPipelinesClient extends PipelinesClient {
  /** Calls by method, for tests; `deploy` records the secret refs, never the values. */
  readonly calls: { method: string; arg?: unknown }[];
}

export function createMockPipelinesClient(opts: MockOptions = {}): MockPipelinesClient {
  const catalog = opts.catalog ?? FIXTURE_CATALOG;
  const intervalMs = opts.intervalMs ?? 1000;
  const deployMs = opts.deployMs ?? 300;
  const now = opts.now ?? (() => Date.now());
  const random = opts.random ?? Math.random;
  const deployments = new Map<
    string,
    Deployment & { nodes: Set<string>; stopped: { stopped: boolean } }
  >();
  const calls: MockPipelinesClient['calls'] = [];
  let failNext = opts.failDeploy;
  let seq = 0;

  const public_ = (d: Deployment): Deployment => ({
    id: d.id,
    name: d.name,
    state: d.state,
    revision: d.revision,
    ...(d.message ? { message: d.message } : {}),
  });

  return {
    calls,
    async catalog() {
      calls.push({ method: 'catalog' });
      return catalog;
    },
    async validate(spec) {
      calls.push({ method: 'validate', arg: spec });
      const issues: Issue[] = [];
      const known = new Set(catalog.map((c) => c.id));
      for (const step of spec.steps) {
        const [prefix, ref] = step.uses.split(':', 2);
        if (prefix === 'connector' && !known.has(ref ?? '')) {
          issues.push({
            level: 'error',
            nodeId: step.id,
            message: `Server: unknown connector ${ref}.`,
          });
        }
      }
      return issues;
    },
    async deploy(spec: PipelineSpec, secrets) {
      const refs = spec.steps.flatMap((s) => Object.values(s.secrets).map((r) => r.secretRef));
      calls.push({
        method: 'deploy',
        arg: { name: spec.metadata.name, secretRefs: [...secrets.keys()] },
      });
      const missing = refs.filter((r) => !secrets.has(r));
      await new Promise((r) => setTimeout(r, deployMs));
      if (failNext) {
        const message = failNext;
        failNext = undefined;
        throw new Error(message);
      }
      // A secret ref the spec names but the caller did not send is fine on a redeploy
      // (the store has it); the mock only notes it.
      const prior = [...deployments.values()].find((d) => d.name === spec.metadata.name);
      if (prior) prior.stopped.stopped = true;
      const d = {
        id: `dep-${++seq}`,
        name: spec.metadata.name,
        state: 'running' as const,
        revision: (prior?.revision ?? 0) + 1,
        ...(missing.length
          ? { message: `${missing.length} secret(s) reused from the store.` }
          : {}),
        nodes: new Set(spec.steps.map((s) => s.id)),
        stopped: { stopped: false },
      };
      deployments.set(d.id, d);
      return public_(d);
    },
    async stop(id) {
      calls.push({ method: 'stop', arg: id });
      const d = deployments.get(id);
      if (!d) throw new Error(`No deployment ${id}.`);
      d.stopped.stopped = true;
      d.state = 'stopped';
      return public_(d);
    },
    metrics(id: string, nodeId: string, metric: Metric): AsyncIterable<MetricPoint[]> {
      const d = deployments.get(id);
      const base = seedOf(`${nodeId}/${metric}`);
      return {
        async *[Symbol.asyncIterator]() {
          if (!d?.nodes.has(nodeId)) return;
          // A short backfill, so a chart opens with a line rather than a dot.
          const t0 = now();
          let level = metric === 'throughput' ? 200 + base * 1800 : 0.2 + base * 3;
          const step = () => {
            const drift = (random() - 0.5) * (metric === 'throughput' ? 120 : 0.4);
            level = Math.max(0, level + drift);
            return Math.round(level * 100) / 100;
          };
          yield Array.from({ length: 10 }, (_, i) => ({
            t: t0 - (10 - i) * intervalMs,
            v: step(),
            series: metric,
          }));
          while (!d.stopped.stopped) {
            await sleep(intervalMs, d.stopped);
            if (d.stopped.stopped) return;
            yield [{ t: now(), v: step(), series: metric }];
          }
        },
      };
    },
  };
}
