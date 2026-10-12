import { useEffect, useState } from 'react';

const QUERY = '(prefers-reduced-motion: reduce)';

function query(): MediaQueryList | undefined {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return undefined;
  return window.matchMedia(QUERY);
}

/** True while the viewer asks for reduced motion; charts then skip every animation. */
export function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(() => query()?.matches ?? false);
  useEffect(() => {
    const mql = query();
    if (!mql) return;
    const onChange = () => setReduced(mql.matches);
    onChange();
    mql.addEventListener('change', onChange);
    return () => mql.removeEventListener('change', onChange);
  }, []);
  return reduced;
}
