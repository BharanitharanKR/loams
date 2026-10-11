import { useChartTheme } from './theme';
import type { SeriesPoint } from './types';

const VIEW_W = 100;

export interface SparklineProps {
  points: readonly SeriesPoint[];
  ariaLabel: string;
  height?: number;
}

/** The SVG path for one series, in a 100 x `height` box with a 2px inset. */
export function sparkPath(
  points: readonly SeriesPoint[],
  height: number,
  domain: { t0: number; t1: number; v0: number; v1: number },
): string {
  const inset = 2;
  const spanT = domain.t1 - domain.t0 || 1;
  const spanV = domain.v1 - domain.v0 || 1;
  return points
    .map((p, i) => {
      const x = ((p.t - domain.t0) / spanT) * VIEW_W;
      const y = inset + (1 - (p.v - domain.v0) / spanV) * (height - inset * 2);
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(' ');
}

function summarize(points: readonly SeriesPoint[]): string {
  if (points.length === 0) return 'no data';
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  for (const p of points) {
    if (p.v < min) min = p.v;
    if (p.v > max) max = p.v;
  }
  const last = points[points.length - 1];
  const fmt = (n: number) => (Number.isInteger(n) ? String(n) : n.toFixed(2));
  return `latest ${fmt(last?.v ?? 0)}, min ${fmt(min)}, max ${fmt(max)}`;
}

/**
 * A word-sized trend line: inline SVG, no axes, no ECharts.
 *
 * Cheap enough for a table cell or a card header full of them. One series uses
 * the ochre accent (the newest thing); several take the categorical palette in
 * first-seen order. Its accessible name is `ariaLabel` plus latest/min/max, so
 * the trend is never colour or shape alone.
 */
export function Sparkline({ points, ariaLabel, height = 32 }: SparklineProps) {
  const theme = useChartTheme();
  const groups = new Map<string, SeriesPoint[]>();
  for (const p of points) {
    if (!Number.isFinite(p.t) || !Number.isFinite(p.v)) continue;
    const key = p.series ?? '';
    const list = groups.get(key);
    if (list) list.push(p);
    else groups.set(key, [p]);
  }
  const clean = [...groups.values()].flat();
  const domain = {
    t0: Math.min(...clean.map((p) => p.t)),
    t1: Math.max(...clean.map((p) => p.t)),
    v0: Math.min(...clean.map((p) => p.v)),
    v1: Math.max(...clean.map((p) => p.v)),
  };
  const single = groups.size <= 1;
  const label = single
    ? `${ariaLabel}: ${summarize(clean)}`
    : `${ariaLabel}: ${[...groups].map(([name, pts]) => `${name} ${summarize(pts)}`).join('; ')}`;

  return (
    <svg
      role="img"
      aria-label={label}
      viewBox={`0 0 ${VIEW_W} ${height}`}
      preserveAspectRatio="none"
      width="100%"
      height={height}
      style={{ display: 'block', overflow: 'visible' }}
    >
      {clean.length > 1 &&
        [...groups].map(([name, pts], i) => {
          const sorted = [...pts].sort((a, b) => a.t - b.t);
          const color = single ? theme.accent : (theme.palette[i % theme.palette.length] ?? theme.accent);
          return (
            <path
              key={name || 'series'}
              data-series={name}
              d={sparkPath(sorted, height, domain)}
              fill="none"
              stroke={color}
              strokeWidth={1.5}
              strokeLinejoin="round"
              strokeLinecap="round"
              vectorEffect="non-scaling-stroke"
            />
          );
        })}
    </svg>
  );
}
