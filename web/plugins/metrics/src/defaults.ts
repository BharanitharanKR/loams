// The dashboards every server starts with. A server that has saved its own copy of one
// (same id) returns that instead; these are the fixture the mock serves and the page's
// "Reset" target.

import type { DashboardSpec } from '@loams-core/types';
import { DEFAULT_RANGE, DEFAULT_REFRESH, type PanelType, panelWidget } from './spec.js';

type Row = [metric: string, type: PanelType, x: number, y: number, w: number, h: number];

function dashboard(id: string, title: string, rows: Row[]): DashboardSpec {
  const widgets: DashboardSpec['widgets'] = {};
  const layout: DashboardSpec['layout'] = [];
  rows.forEach(([metric, type, x, y, w, h], i) => {
    const wid = `${metric.replace(/^loams_/, '').replace(/_/g, '-')}-${i + 1}`;
    widgets[wid] = panelWidget(wid, metric, type);
    layout.push({ id: wid, x, y, w, h });
  });
  return {
    id,
    version: 0,
    title,
    params: [
      { name: 'range', type: 'select', default: DEFAULT_RANGE },
      { name: 'refresh', type: 'select', default: DEFAULT_REFRESH },
    ],
    layout,
    widgets,
  };
}

export const DEFAULT_DASHBOARDS: readonly DashboardSpec[] = [
  dashboard('metrics-usage', 'Usage', [
    ['loams_function_invocations_total', 'line', 0, 0, 8, 3],
    ['loams_function_duration_ms_p95', 'stat', 8, 0, 4, 3],
    ['loams_function_cpu_seconds_total', 'area', 0, 3, 6, 3],
    ['loams_function_memory_bytes', 'line', 6, 3, 6, 3],
    ['loams_function_egress_bytes_total', 'bar', 0, 6, 12, 3],
  ]),
  dashboard('metrics-postgres', 'Postgres', [
    ['loams_postgres_computes', 'area', 0, 0, 6, 3],
    ['loams_postgres_connections', 'stat', 6, 0, 6, 3],
    ['loams_postgres_wal_lag_bytes', 'line', 0, 3, 12, 3],
  ]),
  dashboard('metrics-sql', 'Loams SQL', [
    ['loams_sql_wake_latency_ms', 'line', 0, 0, 8, 3],
    ['loams_sql_active_keyspaces', 'stat', 8, 0, 4, 3],
    ['loams_sql_queries_per_second', 'area', 0, 3, 12, 3],
  ]),
  dashboard('metrics-live', 'Live', [
    ['loams_live_invocations_total', 'line', 0, 0, 6, 3],
    ['loams_live_errors_total', 'bar', 6, 0, 6, 3],
    ['loams_live_worker_restarts_total', 'stat', 0, 3, 4, 3],
  ]),
  dashboard('metrics-house', 'House', [
    ['loams_house_queries_total', 'line', 0, 0, 6, 3],
    ['loams_house_bytes_scanned_total', 'area', 6, 0, 6, 3],
  ]),
];
