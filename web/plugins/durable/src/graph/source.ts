// Where run graphs come from. The engine serves promises over the /durable/
// envelope but no change stream, so the live source polls the promises of one
// run (by its resonate:origin tag) and yields a snapshot whenever they change.

import type { SeriesPoint } from '@loams/charts';
import type { DurableApi, PromiseRecord } from '../envelope.js';
import { buildRuns, TAG_ORIGIN } from '../runs.js';
import { fromPromises, isSettled, type RunEvent, type StepStatus } from './model.js';

export interface RunSummary {
  runId: string;
  name: string;
  status: StepStatus;
  startedAt: number;
  sample?: boolean;
}

/** The typed client the graph tab reads through. */
export interface RunGraphSource {
  listRuns(): Promise<RunSummary[]>;
  /** Changes to one run, starting with a snapshot. Ends when the run settles or `signal` aborts. */
  watchRun(runId: string, signal: AbortSignal): AsyncIterable<RunEvent>;
  /** Run durations (ms) as they settle, series per workflow name. */
  metrics(signal?: AbortSignal): AsyncIterable<SeriesPoint[]>;
}

/** Resolves after `ms`, or early (without throwing) when `signal` aborts. */
export function delay(ms: number, signal?: AbortSignal): Promise<void> {
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

const fingerprint = (ps: PromiseRecord[]) =>
  ps
    .map((p) => `${p.id}:${p.state}:${p.settledAt ?? ''}`)
    .sort()
    .join('|');

export function promiseRunSource(api: DurableApi, pollMs = 2_000): RunGraphSource {
  const promisesOf = async (runId: string) => {
    const [root, rest] = await Promise.all([
      api.getPromise(runId),
      api.searchPromises({ tags: { [TAG_ORIGIN]: runId }, limit: 500 }),
    ]);
    return [root, ...rest.items.filter((p) => p.id !== runId)];
  };
  return {
    async listRuns() {
      const page = await api.searchPromises({ limit: 200 });
      return buildRuns(page.items).map((r) => {
        const g = fromPromises(r.promise.id, [r.promise]);
        return {
          runId: r.promise.id,
          name: g?.name ?? r.promise.id,
          status: g?.status ?? 'running',
          startedAt: r.promise.createdAt,
        };
      });
    },
    async *watchRun(runId, signal) {
      let last = '';
      while (!signal.aborted) {
        const ps = await promisesOf(runId);
        const fp = fingerprint(ps);
        const run = fromPromises(runId, ps);
        if (run && fp !== last) {
          last = fp;
          yield { type: 'snapshot', run };
        }
        if (run && isSettled(run.status)) return;
        await delay(pollMs, signal);
      }
    },
    async *metrics(signal) {
      const seen = new Set<string>();
      while (!signal?.aborted) {
        const page = await api.searchPromises({ limit: 200 });
        const points: SeriesPoint[] = [];
        for (const r of buildRuns(page.items)) {
          const p = r.promise;
          if (p.settledAt === undefined || seen.has(p.id)) continue;
          seen.add(p.id);
          const g = fromPromises(p.id, [p]);
          points.push({ t: p.settledAt, v: p.settledAt - p.createdAt, series: g?.name ?? 'run' });
        }
        if (points.length) yield points.sort((a, b) => a.t - b.t);
        await delay(pollMs * 5, signal);
      }
    },
  };
}

/** Values from several async iterables, as they arrive. */
export async function* merge<T>(...its: AsyncIterable<T>[]): AsyncGenerator<T> {
  const iters = its.map((i) => i[Symbol.asyncIterator]());
  const next = (k: number) => iters[k]!.next().then((r) => ({ k, r }));
  const pending = new Map(iters.map((_, k) => [k, next(k)]));
  try {
    while (pending.size) {
      const { k, r } = await Promise.race(pending.values());
      if (r.done) pending.delete(k);
      else {
        pending.set(k, next(k));
        yield r.value;
      }
    }
  } finally {
    await Promise.allSettled(iters.map((i) => i.return?.()));
  }
}

/** `it`, ending quietly instead of throwing (an engine that is down has no metrics). */
export async function* quiet<T>(it: AsyncIterable<T>): AsyncGenerator<T> {
  try {
    yield* it;
  } catch {
    // nothing to chart from this source
  }
}

const SAMPLE = 'sample:';

/** `primary`, plus `samples` listed after it with their ids under `sample:`. */
export function withSamples(primary: RunGraphSource, samples: RunGraphSource): RunGraphSource {
  return {
    async listRuns() {
      const [mine, theirs] = await Promise.all([
        primary.listRuns().catch(() => [] as RunSummary[]),
        samples.listRuns(),
      ]);
      return [
        ...mine,
        ...theirs.map((r) => ({ ...r, runId: SAMPLE + r.runId, sample: true })),
      ];
    },
    watchRun(runId, signal) {
      if (!runId.startsWith(SAMPLE)) return primary.watchRun(runId, signal);
      const inner = samples.watchRun(runId.slice(SAMPLE.length), signal);
      return (async function* () {
        for await (const e of inner) {
          yield e.type === 'snapshot'
            ? { ...e, run: { ...e.run, runId, sample: true } }
            : e;
        }
      })();
    },
    metrics(signal) {
      return merge(quiet(primary.metrics(signal)), samples.metrics(signal));
    },
  };
}
