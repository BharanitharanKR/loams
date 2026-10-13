/** Shared helpers for the grain and soil-profile canvases. */

export function rng(seed: number) {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 4294967296;
  };
}

/** `color` at alpha `a`: a hex token as rgba(), any other CSS colour through color-mix(). */
export function rgba(color: string, a: number) {
  if (!/^#?[0-9a-f]{3}([0-9a-f]{3})?$/i.test(color)) {
    return `color-mix(in srgb, ${color || 'currentColor'} ${Math.round(a * 100)}%, transparent)`;
  }
  const h = color.replace('#', '');
  const n = Number.parseInt(
    h.length === 3
      ? h
          .split('')
          .map((c) => c + c)
          .join('')
      : h,
    16,
  );
  return `rgba(${n >> 16},${(n >> 8) & 255},${n & 255},${a})`;
}

export type Tokens = { bg: string; ink: string; accent: string; grow: string };

export function readTokens(): Tokens {
  const s = getComputedStyle(document.documentElement);
  // Without the token sheet (tests, early paint) a value is empty; rgba() then mixes currentColor.
  const v = (name: string) => s.getPropertyValue(name).trim();
  return { bg: v('--op-bg'), ink: v('--op-ink'), accent: v('--op-accent'), grow: v('--op-grow') };
}

/** Size a canvas to its box at device resolution (capped at 2x). */
export function fit(canvas: HTMLCanvasElement) {
  const r = canvas.getBoundingClientRect();
  const d = Math.min(window.devicePixelRatio || 1, 2);
  canvas.width = Math.max(1, Math.round(r.width * d));
  canvas.height = Math.max(1, Math.round(r.height * d));
  const ctx = canvas.getContext('2d');
  if (!ctx) return null;
  ctx.setTransform(d, 0, 0, d, 0, 0);
  return { ctx, w: r.width, h: r.height };
}

/** Calls `fn` when the theme class on <html> changes. */
export function onThemeChange(fn: () => void) {
  const mo = new MutationObserver(fn);
  mo.observe(document.documentElement, { attributes: true, attributeFilter: ['class', 'style'] });
  return () => mo.disconnect();
}

export function prefersReducedMotion() {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}
