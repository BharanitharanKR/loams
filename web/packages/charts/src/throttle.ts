/**
 * A leading-and-trailing throttle for `setOption`.
 *
 * The first call after a quiet spell runs at once; calls inside the interval
 * collapse into one trailing run at its end. A stream that emits every 50 ms
 * therefore repaints at most every `intervalMs`, and the last batch always
 * lands on screen.
 */
export interface Throttle {
  /** Ask for a run; runs now or at the end of the current interval. */
  schedule(): void;
  /** Run now if a trailing run is pending. */
  flush(): void;
  /** Drop any pending run. */
  cancel(): void;
}

export interface Clock {
  now(): number;
  setTimeout(fn: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
}

const realClock: Clock = {
  now: () => Date.now(),
  setTimeout: (fn, ms) => globalThis.setTimeout(fn, ms),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
};

export function createThrottle(fn: () => void, intervalMs: number, clock: Clock = realClock): Throttle {
  let last = Number.NEGATIVE_INFINITY;
  let timer: unknown;
  let pending = false;

  const run = () => {
    pending = false;
    timer = undefined;
    last = clock.now();
    fn();
  };

  return {
    schedule() {
      const elapsed = clock.now() - last;
      if (elapsed >= intervalMs && timer === undefined) {
        run();
        return;
      }
      pending = true;
      if (timer === undefined) {
        timer = clock.setTimeout(run, Math.max(0, intervalMs - elapsed));
      }
    },
    flush() {
      if (!pending) return;
      if (timer !== undefined) clock.clearTimeout(timer);
      run();
    },
    cancel() {
      if (timer !== undefined) clock.clearTimeout(timer);
      timer = undefined;
      pending = false;
    },
  };
}
