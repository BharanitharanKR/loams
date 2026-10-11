/**
 * ChartSpec -> ECharts option, through the core compiler.
 *
 * The chart kinds themselves are `@loams-core/charts`' registry: this module
 * maps a spec's `chartType` and `encodings` onto a native widget, calls
 * `compileNativeWidget`, and then does only what the core compiler leaves to the
 * client: a time axis pinned to the live window, long-to-wide pivoting for a
 * `color`/`series` encoding, stacking, and token ink.
 *
 * The compiler is imported from its source module rather than the package root:
 * the root also exports the Cordis render service, which pulls in the Node-only
 * data and BI services. `compiler.ts` itself has no imports and no side effects
 * beyond registering the built-in kinds, and it compiles under this workspace's
 * strict settings, so every consumer of this package typechecks it as is.
 */
import {
  compileNativeWidget,
  declineChartRender,
  deepMerge,
} from '@loams-core/charts/src/compiler';
import type { ChartTheme } from './theme';
import type { ChartSpec, Row } from './types';
import { timeOf } from './window';

/** The core compiler's kinds this package drives. */
export type NativeKind =
  | 'line'
  | 'area'
  | 'bar'
  | 'pie'
  | 'scatter'
  | 'heatmap'
  | 'funnel'
  | 'waterfall';

/** A spec this package cannot draw, with a message fit to show the viewer. */
export class ChartSpecError extends Error {
  override name = 'ChartSpecError';
}

const KIND_BY_NAME: Record<string, NativeKind> = {
  line: 'line',
  'time series': 'line',
  'multi line': 'line',
  slope: 'line',
  bump: 'line',
  area: 'area',
  'stacked area': 'area',
  streamgraph: 'area',
  bar: 'bar',
  column: 'bar',
  'stacked bar': 'bar',
  'stacked column': 'bar',
  'clustered bar': 'bar',
  'clustered column': 'bar',
  'grouped bar': 'bar',
  histogram: 'bar',
  pie: 'pie',
  donut: 'pie',
  doughnut: 'pie',
  scatter: 'scatter',
  bubble: 'scatter',
  heatmap: 'heatmap',
  funnel: 'funnel',
  waterfall: 'waterfall',
};

/** Spec types that are a graph, not a chart: the core compiler routes them to @loams-core/flows. */
const GRAPH_NAMES = new Set(['network graph', 'graph', 'network', 'tree', 'flow']);

function normalizeName(chartType: string): string {
  return chartType
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, ' ')
    .replace(/\s+(chart|plot)$/, '')
    .replace(/\s+/g, ' ');
}

/** The native kind for a spec's `chartType`, or undefined if this package does not draw it. */
export function kindForChartType(chartType: string): NativeKind | undefined {
  return KIND_BY_NAME[normalizeName(chartType)];
}

/** Resolve a kind or throw a {@link ChartSpecError} saying why not. */
export function resolveKind(chartType: string): NativeKind {
  const kind = kindForChartType(chartType);
  if (kind) return kind;
  const name = normalizeName(chartType);
  if (GRAPH_NAMES.has(name)) {
    const decline = declineChartRender({ type: 'graph' });
    throw new ChartSpecError(decline?.reason ?? `"${chartType}" is a graph, not a chart.`);
  }
  throw new ChartSpecError(`Chart type "${chartType}" is not supported by @loams/charts.`);
}

type Encoding = ChartSpec['encodings'][string];
export interface FieldRef {
  field: string;
  type?: string;
}

/** Read one encoding channel, which may be a bare field name or `{ field, type }`. */
export function fieldOf(encoding: Encoding | undefined): FieldRef | undefined {
  if (typeof encoding === 'string') return encoding === '' ? undefined : { field: encoding };
  if (
    encoding &&
    typeof encoding === 'object' &&
    typeof encoding.field === 'string' &&
    encoding.field !== ''
  ) {
    return { field: encoding.field, type: encoding.type };
  }
  return undefined;
}

/** The x field of a spec, which is also its time field when the chart is live. */
export function xFieldOf(spec: ChartSpec): string | undefined {
  return fieldOf(spec.encodings?.x)?.field;
}

const TEMPORAL_TYPES = new Set(['temporal', 'time', 'datetime', 'date', 'timestamp']);

function isTemporal(x: FieldRef | undefined, rows: readonly Row[], windowed: boolean): boolean {
  if (!x) return false;
  if (x.type) return TEMPORAL_TYPES.has(x.type.toLowerCase());
  if (windowed) return true;
  const sample = rows.find((row) => row[x.field] !== undefined && row[x.field] !== null)?.[x.field];
  // Epoch-ms numbers (after 1973) and Dates read as time; small numbers and labels do not.
  return sample instanceof Date || (typeof sample === 'number' && sample > 1e11);
}

/**
 * Long rows (`{ t, v, series }`) to wide rows (`{ t, cpu: .., mem: .. }`).
 *
 * Series keep the order of `order`, extended in first-seen order, so a series
 * keeps its colour while the window slides past another one.
 */
export function pivotRows(
  rows: readonly Row[],
  xField: string,
  yField: string,
  seriesField: string,
  order: readonly string[] = [],
): { rows: Row[]; series: string[] } {
  const series = [...order];
  const known = new Set(series);
  const byX = new Map<unknown, Row>();
  for (const row of rows) {
    const rawX = row[xField];
    const key = rawX instanceof Date ? rawX.getTime() : rawX;
    const name = String(row[seriesField] ?? '');
    if (!known.has(name)) {
      known.add(name);
      series.push(name);
    }
    let wide = byX.get(key);
    if (!wide) {
      wide = { [xField]: rawX };
      byX.set(key, wide);
    }
    const value = row[yField];
    wide[name] = typeof value === 'number' ? value : value == null ? null : Number(value);
  }
  return { rows: [...byX.values()], series };
}

export interface CompileOptions {
  theme: ChartTheme;
  /** False under prefers-reduced-motion. */
  animate?: boolean;
  /** Pin a time x axis to this range (the live window). */
  window?: { start: number; end: number };
  /** Series order carried across updates, so colours follow the entity. */
  seriesOrder?: readonly string[];
}

export interface CompiledChart {
  option: Record<string, unknown>;
  kind: NativeKind;
  xField?: string;
  /** The y columns of the (possibly pivoted) dataset, in colour order. */
  yFields: string[];
  /** Series order to pass back on the next update. */
  seriesOrder: string[];
  temporal: boolean;
}

type Dict = Record<string, unknown>;

function asArray(value: unknown): Dict[] {
  if (Array.isArray(value)) return value as Dict[];
  return value && typeof value === 'object' ? [value as Dict] : [];
}

/** Compile a spec and rows to a themed ECharts option. Throws {@link ChartSpecError}. */
export function compileSpec(
  spec: ChartSpec,
  rows: readonly Row[],
  options: CompileOptions,
): CompiledChart {
  const kind = resolveKind(spec.chartType);
  const enc = spec.encodings ?? {};
  const x = fieldOf(enc.x) ?? fieldOf(enc.category) ?? fieldOf(enc.theta);
  const y = fieldOf(enc.y) ?? fieldOf(enc.value) ?? fieldOf(enc.size);
  const seriesRef = fieldOf(enc.color) ?? fieldOf(enc.series);
  const stacked = /stack|stream/i.test(spec.chartType);

  let source: Row[] = [...rows];
  let yFields = y ? [y.field] : [];
  let seriesOrder = [...(options.seriesOrder ?? [])];

  if (seriesRef && x && y && (kind === 'line' || kind === 'area' || kind === 'bar')) {
    const pivot = pivotRows(rows, x.field, y.field, seriesRef.field, seriesOrder);
    source = pivot.rows;
    yFields = pivot.series;
    seriesOrder = pivot.series;
  }

  let encode: Dict;
  switch (kind) {
    case 'pie':
    case 'funnel':
      encode = { x: x?.field, value: y?.field };
      break;
    case 'heatmap':
      encode = {
        x: x?.field,
        y: fieldOf(enc.y)?.field,
        value: (fieldOf(enc.color) ?? fieldOf(enc.value))?.field,
      };
      break;
    case 'scatter':
    case 'waterfall':
      encode = { x: x?.field, y: y?.field };
      break;
    default:
      encode = { x: x?.field, y: yFields };
  }

  let option = compileNativeWidget({ type: 'chart', chart: { kind, encode } }, source) as Dict;
  const temporal = isTemporal(x, source, options.window !== undefined);

  if (temporal && option.xAxis) {
    option = {
      ...option,
      xAxis: asArray(option.xAxis).map((axis) => ({
        ...axis,
        type: 'time',
        ...(options.window ? { min: options.window.start, max: options.window.end } : {}),
      })),
    };
    // A time axis wants numbers: normalise Dates and ISO strings in place on our copy.
    if (x) {
      source.forEach((row, i) => {
        const t = timeOf(row[x.field]);
        if (t !== undefined && t !== row[x.field]) source[i] = { ...row, [x.field]: t };
      });
      option = { ...option, dataset: { source } };
    }
  }

  const many = source.length > 24;
  option = {
    ...option,
    series: asArray(option.series).map((series, i) => {
      const next: Dict = { ...series };
      const encodeY = (series.encode as Dict | undefined)?.y;
      if (next.name === undefined && typeof encodeY === 'string') next.name = encodeY;
      if (stacked && (kind === 'bar' || kind === 'area' || kind === 'line')) {
        next.stack = 'total';
      }
      if (kind === 'line' || kind === 'area') {
        next.showSymbol = !many;
        next.symbolSize = 8;
        next.emphasis = { focus: 'series' };
        next.connectNulls = false;
      }
      if (kind === 'line' && yFields.length <= 1 && i === 0) next.sampling = 'lttb';
      return next;
    }),
  };

  if (kind === 'heatmap') {
    const valueField = encode.value as string | undefined;
    const values = valueField
      ? source.map((row) => Number(row[valueField])).filter((n) => Number.isFinite(n))
      : [];
    option = {
      ...option,
      visualMap: {
        ...(option.visualMap as Dict),
        min: values.length ? Math.min(...values) : 0,
        max: values.length ? Math.max(...values) : 1,
      },
    };
  }

  if (yFields.length > 1 && !option.legend) option = { ...option, legend: {} };

  return {
    option: applyTheme(option, kind, options.theme, options.animate ?? true),
    kind,
    xField: x?.field,
    yFields,
    seriesOrder,
    temporal,
  };
}

/** Lay token ink over a compiled option. Compiled values win over these defaults where both set a key. */
export function applyTheme(
  option: Dict,
  kind: NativeKind,
  theme: ChartTheme,
  animate: boolean,
): Dict {
  const label = { color: theme.muted, fontFamily: theme.fontMono, fontSize: 11 };
  const axis = (split: boolean): Dict => ({
    axisLine: { lineStyle: { color: theme.rule } },
    axisTick: { show: false },
    axisLabel: { ...label, hideOverlap: true },
    splitLine: { show: split, lineStyle: { color: theme.ruleSoft } },
  });
  const styleAxes = (value: unknown, split: boolean) =>
    value === undefined ? undefined : asArray(value).map((a) => deepMerge(axis(split), a));

  const series = asArray(option.series).map((s) => {
    const transparent = (s.itemStyle as Dict | undefined)?.color === 'transparent';
    if (transparent) return s;
    switch (s.type) {
      case 'line':
        return deepMerge({ lineStyle: { width: 2 } }, s);
      case 'bar':
        return deepMerge({ itemStyle: { borderRadius: [4, 4, 0, 0] }, barMaxWidth: 32 }, s);
      case 'pie':
      case 'funnel':
        return deepMerge(
          { itemStyle: { borderColor: theme.bg, borderWidth: 2 }, label: { color: theme.muted } },
          s,
        );
      case 'scatter':
        return deepMerge(
          { symbolSize: 8, itemStyle: { borderColor: theme.bg, borderWidth: 1 } },
          s,
        );
      default:
        return s;
    }
  });

  const hasLegend = option.legend !== undefined;
  const themed: Dict = {
    ...option,
    backgroundColor: 'transparent',
    color: [...theme.palette],
    textStyle: { color: theme.muted, fontFamily: theme.fontSans },
    animation: animate,
    animationDuration: animate ? 300 : 0,
    animationDurationUpdate: animate ? 300 : 0,
    grid: deepMerge(
      { left: 8, right: 16, top: hasLegend ? 32 : 12, bottom: 8, containLabel: true },
      option.grid ?? {},
    ),
    tooltip: deepMerge(
      {
        confine: true,
        backgroundColor: theme.raised,
        borderColor: theme.rule,
        textStyle: { color: theme.ink, fontFamily: theme.fontSans, fontSize: 12 },
        axisPointer: { lineStyle: { color: theme.faint } },
      },
      option.tooltip ?? {},
    ),
    series,
  };
  if (hasLegend) {
    themed.legend = deepMerge(
      {
        type: 'scroll',
        top: 0,
        icon: 'roundRect',
        itemWidth: 12,
        itemHeight: 4,
        textStyle: { color: theme.muted },
      },
      option.legend,
    );
  }
  const xAxis = styleAxes(option.xAxis, false);
  const yAxis = styleAxes(option.yAxis, kind !== 'heatmap');
  if (xAxis) themed.xAxis = xAxis;
  if (yAxis) themed.yAxis = yAxis;
  if (kind === 'heatmap') {
    themed.visualMap = {
      ...(option.visualMap as Dict),
      inRange: { color: [...theme.sequential] },
      textStyle: { color: theme.muted },
    };
  }
  return themed;
}
