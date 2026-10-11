import { useEffect, useRef, useState } from 'react';
import type { SeriesPoint } from './types';
import { windowPoints } from './window';

/**
 * Subscribe to a metric source and keep the last `windowMs` of points.
 *
 * `source` is a factory so the hook can restart it; it is read through a ref,
 * so an inline arrow does not resubscribe on every render. The subscription
 * restarts only when `windowMs` changes or the component remounts. Points are
 * windowed on their own newest time (see `appendWindow`).
 */
export function useMetricStream(
  source: () => AsyncIterable<SeriesPoint[]>,
  windowMs: number,
): SeriesPoint[] {
  const sourceRef = useRef(source);
  sourceRef.current = source;
  const [points, setPoints] = useState<SeriesPoint[]>([]);

  useEffect(() => {
    setPoints([]);
    const iterator = sourceRef.current()[Symbol.asyncIterator]();
    let stopped = false;
    void (async () => {
      try {
        while (!stopped) {
          const next = await iterator.next();
          if (stopped || next.done) break;
          const batch = next.value;
          setPoints((prev) => windowPoints(prev, batch, windowMs));
        }
      } catch {
        // A failed source leaves the last window on screen; the caller's source
        // is where retry policy belongs.
      }
    })();
    return () => {
      stopped = true;
      void iterator.return?.();
    };
  }, [windowMs]);

  return points;
}
