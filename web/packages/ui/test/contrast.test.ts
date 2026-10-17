// @vitest-environment node
// WCAG AA for the token pairs used for text (design 59 §3, §7), read from tokens.css in both
// themes. Alpha colours are composited in sRGB over the surface they sit on, as a browser
// paints them. Also Review Focus 5: forced colours keep borders, focus and status visible.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const read = (rel: string) => readFileSync(fileURLToPath(new URL(rel, import.meta.url)), 'utf8');
const TOKENS = read('../src/tokens.css');
const STYLES = read('../src/styles.css');

type Rgba = [number, number, number, number];

/** The `--op-*` declarations of the first block that opens with `selector {`. */
function block(selector: ':root' | '.dark'): Record<string, string> {
  const start = TOKENS.indexOf(`\n${selector} {`);
  if (start < 0) throw new Error(`no ${selector} block`);
  const end = TOKENS.indexOf('\n}', start);
  const out: Record<string, string> = {};
  for (const m of TOKENS.slice(start, end).matchAll(/(--op-[a-z0-9-]+):\s*([^;]+);/g)) {
    out[m[1] as string] = (m[2] as string).trim();
  }
  return out;
}

const LIGHT = block(':root');
const DARK = { ...LIGHT, ...block('.dark') };

const gamma = (c: number) => (c <= 0.0031308 ? 12.92 * c : 1.055 * c ** (1 / 2.4) - 0.055);
const linear = (c: number) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4);
const clamp = (c: number) => Math.min(1, Math.max(0, c));

function oklch(l: number, c: number, h: number): [number, number, number] {
  const a = c * Math.cos((h * Math.PI) / 180);
  const b = c * Math.sin((h * Math.PI) / 180);
  const l_ = (l + 0.3963377774 * a + 0.2158037573 * b) ** 3;
  const m_ = (l - 0.1055613458 * a - 0.0638541728 * b) ** 3;
  const s_ = (l - 0.0894841775 * a - 1.291485548 * b) ** 3;
  return [
    4.0767416621 * l_ - 3.3077115913 * m_ + 0.2309699292 * s_,
    -1.2684380046 * l_ + 2.6097574011 * m_ - 0.3413193965 * s_,
    -0.0041960863 * l_ - 0.7034186147 * m_ + 1.707614701 * s_,
  ].map((x) => gamma(clamp(x))) as [number, number, number];
}

function parse(value: string, vars: Record<string, string>): Rgba {
  const v = value.trim();
  const ref = /^var\((--op-[a-z0-9-]+)\)$/.exec(v);
  if (ref) return parse(vars[ref[1] as string] ?? 'missing', vars);
  let m = /^#([0-9a-f]{6})$/i.exec(v);
  if (m) {
    const h = m[1] as string;
    return [0, 2, 4].map((i) => Number.parseInt(h.slice(i, i + 2), 16) / 255).concat(1) as Rgba;
  }
  m = /^rgb\((\d+)\s+(\d+)\s+(\d+)(?:\s*\/\s*([\d.]+))?\)$/.exec(v);
  if (m) return [Number(m[1]) / 255, Number(m[2]) / 255, Number(m[3]) / 255, Number(m[4] ?? 1)];
  m = /^oklch\(([\d.]+)\s+([\d.]+)\s+([\d.]+)(?:\s*\/\s*([\d.]+))?\)$/.exec(v);
  if (m) return [...oklch(Number(m[1]), Number(m[2]), Number(m[3])), Number(m[4] ?? 1)];
  throw new Error(`cannot parse colour "${value}"`);
}

/** `fg` painted over an opaque `bg`. */
function over(fg: Rgba, bg: Rgba): Rgba {
  const a = fg[3];
  return [0, 1, 2]
    .map((i) => (fg[i] as number) * a + (bg[i] as number) * (1 - a))
    .concat(1) as Rgba;
}

const luminance = (c: Rgba) =>
  0.2126 * linear(c[0]) + 0.7152 * linear(c[1]) + 0.0722 * linear(c[2]);
function ratio(a: Rgba, b: Rgba): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x) as [number, number];
  return (hi + 0.05) / (lo + 0.05);
}

const TEXT = [
  '--op-ink',
  '--op-muted',
  '--op-faint',
  '--op-accent-fg',
  '--op-grow',
  '--op-warn',
  '--op-info',
  '--op-danger',
];
const SURFACES = ['--op-bg', '--op-surface', '--op-window', '--op-raised', '--op-row-selected'];
/** A state colour on its own soft fill. */
const ON_SOFT: [string, string][] = [
  ['--op-grow', '--op-grow-soft'],
  ['--op-warn', '--op-warn-soft'],
  ['--op-info', '--op-info-soft'],
  ['--op-danger', '--op-danger-soft'],
  ['--op-accent-fg', '--op-accent-soft'],
  ['--op-ink', '--op-accent-soft'],
];
/** Text on a filled control. */
const ON_FILL: [string, string][] = [
  ['--op-accent-ink', '--op-accent'],
  ['--op-grow-on', '--op-grow-deep'],
  // The solid destructive button (Task 4) writes the canvas colour on danger.
  ['--op-bg', '--op-danger'],
];

/** Every opaque backdrop a surface can sit on: itself, or over the canvas and the surface. */
function backdrops(name: string, vars: Record<string, string>): [string, Rgba][] {
  const c = parse(vars[name] ?? 'missing', vars);
  if (c[3] === 1) return [[name, c]];
  const bg = parse(vars['--op-bg'] as string, vars);
  const surface = parse(vars['--op-surface'] as string, vars);
  return [
    [`${name} on --op-bg`, over(c, bg)],
    [`${name} on --op-surface`, over(c, surface)],
  ];
}

describe.each([
  ['light', LIGHT],
  ['dark', DARK],
] as const)('%s theme contrast', (_theme, vars) => {
  const text = (t: string) => parse(vars[t] ?? 'missing', vars);

  it.each(TEXT)('%s is AA (4.5:1) on every surface', (t) => {
    const failures = SURFACES.flatMap((s) =>
      backdrops(s, vars)
        .map(([label, bg]) => [label, ratio(over(text(t), bg), bg)] as const)
        .filter(([, r]) => r < 4.5)
        .map(([label, r]) => `${label}: ${r.toFixed(2)}`),
    );
    expect(failures).toEqual([]);
  });

  it.each(ON_SOFT)('%s is AA on %s', (t, soft) => {
    for (const [label, bg] of backdrops(soft, vars)) {
      expect(ratio(text(t), bg), label).toBeGreaterThanOrEqual(4.5);
    }
  });

  it.each(ON_FILL)('%s is AA on %s', (t, fill) => {
    expect(ratio(text(t), parse(vars[fill] as string, vars))).toBeGreaterThanOrEqual(4.5);
  });

  it('the focus ring (the accent) is 3:1 against every surface', () => {
    for (const s of ['--op-bg', '--op-surface', '--op-window']) {
      expect(ratio(text('--op-accent'), parse(vars[s] as string, vars)), s).toBeGreaterThanOrEqual(
        3,
      );
    }
  });
});

describe('design 59 values (D1073)', () => {
  it('has the radius scale', () => {
    expect(LIGHT['--op-radius']).toBe('10px');
    expect([
      LIGHT['--op-radius-xs'],
      LIGHT['--op-radius-sm'],
      LIGHT['--op-radius-md'],
      LIGHT['--op-radius-lg'],
      LIGHT['--op-radius-xl'],
      LIGHT['--op-radius-pill'],
    ]).toEqual(['4px', '6px', '8px', '14px', '22px', '999px']);
  });

  it('has the canvas, the window and the indigo accent', () => {
    expect(DARK['--op-bg']).toBe('#0a0a0a');
    expect(DARK['--op-accent']).toBe('oklch(0.571 0.21 264)');
    expect(LIGHT['--op-window']).toBe('#f4f2ee');
    expect(LIGHT['--op-bg']).toBe('#ffffff');
    expect(LIGHT['--op-accent']).toBe('oklch(0.488 0.217 264)');
  });

  it('has the composer shadow', () => {
    expect(LIGHT['--op-shadow-composer']).toMatch(/^0 12px 28px -18px /);
    expect(DARK['--op-shadow-composer']).toMatch(/^0 12px 28px -18px /);
  });

  it('uses the system font stacks (D1075)', () => {
    expect(TOKENS).toContain(
      '--loams-font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, sans-serif;',
    );
    expect(TOKENS).not.toMatch(/Archivo|Martian/);
  });
});

describe('forced colours (Review Focus 5)', () => {
  const forced = (css: string) => {
    const at = css.indexOf('@media (forced-colors: active)');
    expect(at, 'a forced-colors block').toBeGreaterThan(-1);
    return css.slice(at, css.indexOf('\n}\n', at));
  };

  it('maps hairlines and inputs to CanvasText and the accent to Highlight', () => {
    const f = forced(TOKENS);
    for (const t of ['--op-rule', '--op-rule-soft', '--op-input'])
      expect(f).toContain(`${t}: CanvasText`);
    expect(f).toContain('--op-accent: Highlight');
  });

  it('draws focus as an outline and keeps status dots visible', () => {
    const f = forced(STYLES);
    expect(f).toMatch(/:focus-visible\s*\{[^}]*outline:\s*2px solid Highlight/);
    expect(f).toMatch(/\.loams-status::before[^{]*\{[^}]*forced-color-adjust:\s*none/);
    expect(f).toMatch(/\[data-status-dot\][^{]*\{[^}]*background:\s*CanvasText/);
  });
});
