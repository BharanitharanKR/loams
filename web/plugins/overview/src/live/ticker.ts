// A cancellable interval stream: `ticker(ms, make)` yields `make(i)` every
// `ms` milliseconds until the consumer calls `return()` (a `for await` that
// breaks, or a React effect cleanup). An async generator cannot be stopped
// while it is parked on a timer, so this is a hand-written iterator whose
// `return()` clears the timer and settles the pending `next()` at once.

export function ticker<T>(ms: number, make: (i: number) => T): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      let i = 0;
      let done = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let pending: ((r: IteratorResult<T>) => void) | undefined;
      const finish = (): IteratorResult<T> => ({ done: true, value: undefined });
      return {
        next() {
          if (done) return Promise.resolve(finish());
          return new Promise<IteratorResult<T>>((resolve) => {
            pending = resolve;
            timer = setTimeout(() => {
              pending = undefined;
              timer = undefined;
              if (done) resolve(finish());
              else resolve({ done: false, value: make(i++) });
            }, ms);
          });
        },
        return() {
          done = true;
          if (timer !== undefined) clearTimeout(timer);
          timer = undefined;
          pending?.(finish());
          pending = undefined;
          return Promise.resolve(finish());
        },
      };
    },
  };
}

/**
 * `first` (a snapshot) as the stream's first batch, then everything `rest`
 * yields. `return()` is forwarded to `rest`, so stopping this stops the timer
 * underneath at once rather than after its next tick.
 */
export function startWith<T>(first: () => Promise<T>, rest: AsyncIterable<T>): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      let primed = false;
      let done = false;
      let inner: AsyncIterator<T> | undefined;
      const finish = (): IteratorResult<T> => ({ done: true, value: undefined });
      return {
        async next() {
          if (done) return finish();
          if (!primed) {
            primed = true;
            const value = await first();
            return done ? finish() : { done: false, value };
          }
          inner ??= rest[Symbol.asyncIterator]();
          return inner.next();
        },
        async return() {
          done = true;
          await inner?.return?.();
          return finish();
        },
      };
    },
  };
}
