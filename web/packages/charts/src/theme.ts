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

/** Categorical order, light surface. */
export const LIGHT_PALETTE = [
  '#2a78d6',
  '#eb6834',
  '#1baf7a',
  '#eda100',
  '#e87ba4',
  '#008300',
  '#6250d6',
  '#e34948',
] as const;

/** The same hues, stepped for the dark surface. */
export const DARK_PALETTE = [
  '#3987e5',
  '#d95926',
  '#199e70',
  '#c98500',
  '#d55181',
  '#008300',
  '#9085e9',
  '#e66767',
] as const;

/** Token values from tokens.css, used when the stylesheet is not loaded (tests, early paint). */
const FALLBACK: Record<ThemeMode, Record<string, string>> = {
  light: {
    '--op-bg': '#ffffff',
    '--op-surface': '#f9f9f7',
    '--op-raised': '#f0f0ec',
    '--op-rule': '#deded8',
    '--op-rule-soft': '#ebebe6',
    '--op-ink': '#1a1511',
    '--op-muted': '#5b5044',
    '--op-faint': '#74695b',
    '--op-accent': '#8a520a',
  },
  dark: {
    '--op-bg': '#15110d',
    '--op-surface': '#1c1713',
    '--op-raised': '#25201a',
    '--op-rule': '#372e26',
    '--op-rule-soft': '#28211b',
    '--op-ink': '#f4f5ee',
    '--op-muted': '#a69985',
    '--op-faint': '#938775',
    '--op-accent': '#e4a24a',
  },
};

const FONT_SANS = 'Archivo, ui-sans-serif, system-ui, sans-serif';
const FONT_MONO = '"Martian Mono", ui-monospace, monospace';

/** Read the current chart theme off `root` (default: <html>). */
export function readChartTheme(root?: HTMLElement): ChartTheme {
  const el = root ?? (typeof document === 'undefined' ? undefined : document.documentElement);
  const mode: ThemeMode = el?.classList.contains('dark') ? 'dark' : 'light';
  const style = el && typeof getComputedStyle === 'function' ? getComputedStyle(el) : undefined;
  const v = (name: string) => style?.getPropertyValue(name).trim() || FALLBACK[mode][name] || '';
  const base: readonly string[] = mode === 'dark' ? DARK_PALETTE : LIGHT_PALETTE;
  const palette = base.map(
    (hex, i) => style?.getPropertyValue(`--op-chart-${i + 1}`).trim() || hex,
  );
  const first = palette[0] ?? LIGHT_PALETTE[0];
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
    sequential: [first.startsWith('#') ? rgba(first, 0.12) : bg, first],
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
