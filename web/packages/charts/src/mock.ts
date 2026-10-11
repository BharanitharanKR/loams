/**
 * Fixture-backed mock metrics, for the gallery, tests and console plugins that
 * have no backend yet. Deterministic for a given seed; times come from `now()`.
 */
import type { SeriesPoint } from './types';

export interface MockMetricOptions {
  /** Series names; one point per series per tick. */
  series?: readonly string[];
  /** Milliseconds between batches. */
  intervalMs?: number;
  /** Baseline value and swing around it. */
  base?: number;
  amplitude?: number;
  seed?: number;
  /** Stop after this many batches (default: never). */
  limit?: number;
  now?: () => number;
}

/** A tiny LCG so fixtures repeat exactly. */
function lcg(seed: number) {
  let s = seed >>> 0 || 1;
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s / 4294967296;
  };
}

function sampler(options: MockMetricOptions) {
  const { series = ['value'], base = 50, amplitude = 20, seed = 7 } = options;
  const rand = lcg(seed);
  const level = new Map<string, number>();
  return (t: number): SeriesPoint[] =>
    series.map((name, i) => {
      const prev = level.get(name) ?? base + i * amplitude * 0.5;
      // Mean-reverting random walk with a slow wave, so lines look like load.
      const wave = Math.sin(t / 15_000 + i) * amplitude * 0.3;
      const next = prev + (rand() - 0.5) * amplitude * 0.4 + (base + wave - prev) * 0.1;
      const v = Math.max(0, Math.round(next * 100) / 100);
      level.set(name, v);
      return series.length > 1 ? { t, v, series: name } : { t, v };
    });
}

/** Backfill: `windowMs` of history at `stepMs`, ending at `end`. */
export function seedMetricPoints(
  windowMs: number,
  stepMs: number,
  options: MockMetricOptions & { end?: number } = {},
): SeriesPoint[] {
  const end = options.end ?? (options.now ?? Date.now)();
  const sample = sampler(options);
  const out: SeriesPoint[] = [];
  for (let t = end - windowMs; t <= end; t += stepMs) out.push(...sample(t));
  return out;
}

/**
 * An async iterable that emits a batch of points every `intervalMs`.
 *
 * `return()` (what `for await ... break` and the hooks' cleanup call) clears the
 * timer at once and settles a pending `next()`, so an unmounted chart leaves
 * nothing ticking.
 */
export function mockMetricStream(options: MockMetricOptions = {}): AsyncIterable<SeriesPoint[]> {
  const { intervalMs = 1_000, limit = Number.POSITIVE_INFINITY, now = Date.now } = options;
  return {
    [Symbol.asyncIterator]() {
      const sample = sampler(options);
      let emitted = 0;
      let done = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let settle: ((r: IteratorResult<SeriesPoint[]>) => void) | undefined;
      const finish = (): IteratorResult<SeriesPoint[]> => {
        done = true;
        if (timer !== undefined) clearTimeout(timer);
        timer = undefined;
        const r: IteratorResult<SeriesPoint[]> = { done: true, value: undefined };
        settle?.(r);
        settle = undefined;
        return r;
      };
      return {
        next() {
          if (done || emitted >= limit) return Promise.resolve(finish());
          return new Promise<IteratorResult<SeriesPoint[]>>((resolve) => {
            settle = resolve;
            timer = setTimeout(() => {
              timer = undefined;
              settle = undefined;
              emitted += 1;
              resolve({ done: false, value: sample(now()) });
            }, intervalMs);
          });
        },
        return() {
          return Promise.resolve(finish());
        },
      };
    },
  };
}

/** The same stream as rows (`{ t, v, series }`), for {@link LiveChart}'s `stream`. */
export function mockRowStream(options: MockMetricOptions = {}): AsyncIterable<Record<string, unknown>[]> {
  const points = mockMetricStream(options);
  return {
    [Symbol.asyncIterator]() {
      const it = points[Symbol.asyncIterator]();
      return {
        async next() {
          const r = await it.next();
          return r.done ? r : { done: false, value: r.value.map((p) => ({ ...p })) };
        },
        async return() {
          await it.return?.();
          return { done: true, value: undefined };
        },
      };
    },
  };
}
