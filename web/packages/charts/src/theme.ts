/**
 * Chart ink from the @loams/ui tokens.
 *
 * Surfaces, rules and text come from the `--op-*` custom properties in
 * `@loams/ui/tokens.css`, read live off <html>, so a chart follows the console's
 * `.dark` class without its own toggle. Series colours are the validated
 * categorical order (fixed order, never cycled; light and dark are each stepped
 * for their own surface, not flipped). A deployment may restyle them through
 * `--op-chart-1` .. `--op-chart-8`.
 */
import { onThemeChange, rgba } from '@loams/ui';
import { useEffect, useState } from 'react';

export type ThemeMode = 'light' | 'dark';

export interface ChartTheme {
  mode: ThemeMode;
  bg: string;
  surface: string;
  raised: string;
  rule: string;
  ruleSoft: string;
  ink: string;
  muted: string;
  faint: string;
  accent: string;
  fontSans: string;
  fontMono: string;
  /** Categorical series colours, in assignment order. */
  palette: readonly string[];
  /** Low and high ends of the single-hue sequential ramp (heatmaps). */
  sequential: readonly [string, string];
}

/** How many categorical series colours the tokens define (`--op-chart-1` .. `--op-chart-8`). */
export const PALETTE_SIZE = 8;

const FONT_SANS = 'Archivo, ui-sans-serif, system-ui, sans-serif';
const FONT_MONO = '"Martian Mono", ui-monospace, monospace';

/** Read the current chart theme off `root` (default: <html>). */
export function readChartTheme(root?: HTMLElement): ChartTheme {
  const el = root ?? (typeof document === 'undefined' ? undefined : document.documentElement);
  const mode: ThemeMode = el?.classList.contains('dark') ? 'dark' : 'light';
  const style = el && typeof getComputedStyle === 'function' ? getComputedStyle(el) : undefined;
  const v = (name: string) => style?.getPropertyValue(name).trim() ?? '';
  const palette = Array.from({ length: PALETTE_SIZE }, (_, i) => v(`--op-chart-${i + 1}`)).filter(
    Boolean,
  );
  const first = palette[0] ?? '';
  const bg = v('--op-bg');
  return {
    mode,
    bg,
    surface: v('--op-surface'),
    raised: v('--op-raised'),
    rule: v('--op-rule'),
    ruleSoft: v('--op-rule-soft'),
    ink: v('--op-ink'),
    muted: v('--op-muted'),
    faint: v('--op-faint'),
    accent: v('--op-accent'),
    fontSans: style?.getPropertyValue('--loams-font-sans').trim() || FONT_SANS,
    fontMono: style?.getPropertyValue('--loams-font-mono').trim() || FONT_MONO,
    palette,
    sequential: [first ? rgba(first, 0.12) : bg, first],
  };
}

/** The chart theme, re-read whenever <html>'s class or style changes. */
export function useChartTheme(): ChartTheme {
  const [theme, setTheme] = useState<ChartTheme>(() => readChartTheme());
  useEffect(() => {
    if (typeof document === 'undefined' || typeof MutationObserver === 'undefined') return;
    setTheme(readChartTheme());
    return onThemeChange(() => setTheme(readChartTheme()));
  }, []);
  return theme;
}
