// A metrics dashboard is a @loams-core/dashboards `DashboardSpec`: the same layout grid
// (12 columns, integer x/y/w/h), the same widget record and the same params, so a dashboard
// saved here opens in dashboard-ui and the other way round. A panel is a widget whose
// `chartSpec` encodes time (`t`), value (`v`) and series (`series`), and whose
// `chartProperties` names the metric it charts.
//
// Every edit here is a pure function from one spec to the next; nothing bumps `version`
// (the server does that on save, as DashboardSpecService.patch does).

import type { DashboardValidationResult } from '@loams-core/dashboards';
import {
  type ChartSpec,
  type DashboardSpec,
  DashboardSpecSchema,
  type LayoutItem,
  type Widget,
} from '@loams-core/types';
import { metricDef } from './catalog.js';

export const GRID_COLUMNS = 12;
export const MIN_W = 2;
export const MIN_H = 2;
export const DEFAULT_W = 6;
export const DEFAULT_H = 3;

export type PanelType = 'line' | 'area' | 'bar' | 'stat';

export const PANEL_TYPES: readonly { id: PanelType; label: string }[] = [
  { id: 'line', label: 'Line' },
  { id: 'area', label: 'Area' },
  { id: 'bar', label: 'Bar' },
  { id: 'stat', label: 'Stat' },
];

/** The chart-specs chart type each panel type compiles to; `stat` is drawn by the page. */
const CHART_TYPE: Record<PanelType, string> = {
  line: 'Line Chart',
  area: 'Area Chart',
  bar: 'Bar Chart',
  stat: 'Stat',
};

export interface Panel {
  id: string;
  metric: string;
  type: PanelType;
  title: string;
}

export interface TimeRange {
  id: string;
  label: string;
  ms: number;
}

export const TIME_RANGES: readonly TimeRange[] = [
  { id: '5m', label: 'Last 5 minutes', ms: 5 * 60_000 },
  { id: '15m', label: 'Last 15 minutes', ms: 15 * 60_000 },
  { id: '1h', label: 'Last hour', ms: 60 * 60_000 },
  { id: '6h', label: 'Last 6 hours', ms: 6 * 60 * 60_000 },
  { id: '24h', label: 'Last 24 hours', ms: 24 * 60 * 60_000 },
];

export interface RefreshRate {
  id: string;
  label: string;
  /** 0: no stream; the panel shows the range once and waits for Refresh. */
  ms: number;
}

export const REFRESH_RATES: readonly RefreshRate[] = [
  { id: 'off', label: 'Off', ms: 0 },
  { id: '5s', label: 'Every 5 s', ms: 5_000 },
  { id: '15s', label: 'Every 15 s', ms: 15_000 },
  { id: '1m', label: 'Every minute', ms: 60_000 },
];

export const DEFAULT_RANGE = '1h';
export const DEFAULT_REFRESH = '15s';

/** About 120 points across the range, never finer than a second. */
export function stepFor(rangeMs: number): number {
  return Math.max(1000, Math.round(rangeMs / 120 / 1000) * 1000);
}

export function chartSpecFor(metric: string, type: PanelType, title: string): ChartSpec {
  return {
    chartType: CHART_TYPE[type],
    title,
    encodings: {
      x: { field: 't', type: 'temporal' },
      y: { field: 'v', type: 'quantitative' },
      color: { field: 'series', type: 'nominal' },
    },
    chartProperties: { metric, panelType: type, ...(type === 'area' ? { stack: true } : {}) },
  };
}

export function panelWidget(id: string, metric: string, type: PanelType, title?: string): Widget {
  const label = title?.trim() || metricDef(metric)?.label || metric;
  // A chart widget must carry exactly one of chartSpec/chart (WidgetSchema); a stat tile is a
  // `kpi` widget, which the dashboards schema lets carry the same chartSpec for its metric.
  return {
    id,
    type: type === 'stat' ? 'kpi' : 'chart',
    chartSpec: chartSpecFor(metric, type, label),
  };
}

function isPanelType(v: unknown): v is PanelType {
  return v === 'line' || v === 'area' || v === 'bar' || v === 'stat';
}

/** The panel a widget describes, or undefined for a widget this page does not draw. */
export function readPanel(widget: Widget | undefined): Panel | undefined {
  const props = widget?.chartSpec?.chartProperties;
  if (!widget || !props) return undefined;
  const metric = props.metric;
  const type = props.panelType;
  if (typeof metric !== 'string' || !isPanelType(type)) return undefined;
  return { id: widget.id, metric, type, title: widget.chartSpec?.title ?? metric };
}

/** The panels in layout order (top to bottom, then left to right): the reading order. */
export function panelsOf(spec: DashboardSpec): (Panel & { layout: LayoutItem })[] {
  return [...spec.layout]
    .sort((a, b) => a.y - b.y || a.x - b.x)
    .flatMap((layout) => {
      const p = readPanel(spec.widgets[layout.id]);
      return p ? [{ ...p, layout }] : [];
    });
}

function int(v: unknown, fallback: number): number {
  return typeof v === 'number' && Number.isFinite(v) ? Math.floor(v) : fallback;
}

/** Keeps a layout item on the grid: within the 12 columns, at least MIN_W x MIN_H. */
export function clampItem(item: LayoutItem): LayoutItem {
  const w = Math.min(GRID_COLUMNS, Math.max(MIN_W, int(item.w, DEFAULT_W)));
  const h = Math.max(MIN_H, int(item.h, DEFAULT_H));
  const x = Math.min(GRID_COLUMNS - w, Math.max(0, int(item.x, 0)));
  const y = Math.max(0, int(item.y, 0));
  return { id: item.id, x, y, w, h };
}

/** A fresh widget id for a metric, unique on this dashboard. */
export function newPanelId(spec: DashboardSpec, metric: string): string {
  const base = metric.replace(/^loams_/, '').replace(/[^a-z0-9]+/gi, '-');
  let n = 1;
  while (spec.widgets[`${base}-${n}`]) n += 1;
  return `${base}-${n}`;
}

/**
 * Adds a widget and its layout row. Mirrors DashboardSpecService.addWidget: with no position
 * the panel goes at the left edge, below everything else, 6 wide and 3 tall.
 */
export function addPanel(
  spec: DashboardSpec,
  widget: Widget,
  position?: Partial<Omit<LayoutItem, 'id'>>,
): DashboardSpec {
  const maxY = spec.layout.reduce((m, it) => Math.max(m, it.y + it.h), 0);
  const item = clampItem({
    id: widget.id,
    x: int(position?.x, 0),
    y: int(position?.y, maxY),
    w: int(position?.w, DEFAULT_W),
    h: int(position?.h, DEFAULT_H),
  });
  return {
    ...spec,
    widgets: { ...spec.widgets, [widget.id]: widget },
    layout: [...spec.layout.filter((it) => it.id !== widget.id), item],
  };
}

export function removePanel(spec: DashboardSpec, id: string): DashboardSpec {
  if (!spec.widgets[id] && !spec.layout.some((it) => it.id === id)) return spec;
  const { [id]: _gone, ...widgets } = spec.widgets;
  return { ...spec, widgets, layout: spec.layout.filter((it) => it.id !== id) };
}

export function movePanel(
  spec: DashboardSpec,
  id: string,
  position: Omit<LayoutItem, 'id'>,
): DashboardSpec {
  if (!spec.layout.some((it) => it.id === id)) return spec;
  return {
    ...spec,
    layout: spec.layout.map((it) => (it.id === id ? clampItem({ id, ...position }) : it)),
  };
}

/** A keyboard step: move by dx/dy cells, grow or shrink by dw/dh cells. */
export function nudgePanel(
  spec: DashboardSpec,
  id: string,
  d: { dx?: number; dy?: number; dw?: number; dh?: number },
): DashboardSpec {
  const it = spec.layout.find((l) => l.id === id);
  if (!it) return spec;
  return movePanel(spec, id, {
    x: it.x + (d.dx ?? 0),
    y: it.y + (d.dy ?? 0),
    w: it.w + (d.dw ?? 0),
    h: it.h + (d.dh ?? 0),
  });
}

/** react-grid-layout's layout after a drag or resize, written back to the spec. */
export function applyGrid(
  spec: DashboardSpec,
  grid: readonly { i: string; x: number; y: number; w: number; h: number }[],
): DashboardSpec {
  const byId = new Map(grid.map((g) => [g.i, g]));
  let changed = false;
  const layout = spec.layout.map((it) => {
    const g = byId.get(it.id);
    if (!g) return it;
    const next = clampItem({ id: it.id, x: g.x, y: g.y, w: g.w, h: g.h });
    if (next.x !== it.x || next.y !== it.y || next.w !== it.w || next.h !== it.h) changed = true;
    return next;
  });
  return changed ? { ...spec, layout } : spec;
}

/** Changes what a panel charts, keeping its place on the grid. */
export function updatePanel(
  spec: DashboardSpec,
  id: string,
  change: Partial<Pick<Panel, 'metric' | 'type' | 'title'>>,
): DashboardSpec {
  const current = readPanel(spec.widgets[id]);
  if (!current) return spec;
  const metric = change.metric ?? current.metric;
  // A new metric with no new title takes the metric's label, not the old metric's title.
  const title =
    change.title ?? (change.metric && change.metric !== current.metric ? undefined : current.title);
  const widget = panelWidget(id, metric, change.type ?? current.type, title);
  return { ...spec, widgets: { ...spec.widgets, [id]: widget } };
}

function param(spec: DashboardSpec, name: string): unknown {
  return spec.params.find((p) => p.name === name)?.default;
}

export function setParam(spec: DashboardSpec, name: string, value: string): DashboardSpec {
  const exists = spec.params.some((p) => p.name === name);
  return {
    ...spec,
    params: exists
      ? spec.params.map((p) => (p.name === name ? { ...p, default: value } : p))
      : [...spec.params, { name, type: 'select', default: value }],
  };
}

export function rangeOf(spec: DashboardSpec): TimeRange {
  const id = param(spec, 'range');
  return (
    TIME_RANGES.find((r) => r.id === id) ??
    (TIME_RANGES.find((r) => r.id === DEFAULT_RANGE) as TimeRange)
  );
}

export function refreshOf(spec: DashboardSpec): RefreshRate {
  const id = param(spec, 'refresh');
  return (
    REFRESH_RATES.find((r) => r.id === id) ??
    (REFRESH_RATES.find((r) => r.id === DEFAULT_REFRESH) as RefreshRate)
  );
}

/** DashboardSpecService.validate, without its store: the same schema, the same messages. */
export function validateDashboard(spec: unknown): DashboardValidationResult {
  const result = DashboardSpecSchema.safeParse(spec);
  if (result.success) return { valid: true, errors: [], spec: result.data };
  const errors = result.error.issues.map(
    (issue) => `${issue.path.length > 0 ? issue.path.join('.') : '(root)'}: ${issue.message}`,
  );
  return { valid: false, errors, spec: undefined };
}

/** Whether two specs differ in anything the user can save. */
export function isDirty(a: DashboardSpec, b: DashboardSpec): boolean {
  return JSON.stringify({ ...a, version: 0 }) !== JSON.stringify({ ...b, version: 0 });
}
