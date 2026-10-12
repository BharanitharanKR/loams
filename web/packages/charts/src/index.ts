/**
 * @loams/charts: the console's one chart wrapper.
 *
 * LiveChart compiles a ChartSpec through @loams-core/charts and draws it with
 * ECharts; Sparkline is a light SVG trend; useMetricStream windows a metric
 * source. Plugins import these rather than wrapping ECharts themselves.
 */
export {
  ChartSpecError,
  compileSpec,
  kindForChartType,
  type NativeKind,
  pivotRows,
} from './compile';
export { DataTable } from './data-table';
export { LiveChart, type LiveChartProps, REPAINT_INTERVAL_MS } from './live-chart';
export { type MockMetricOptions, mockMetricStream, mockRowStream, seedMetricPoints } from './mock';
export { usePrefersReducedMotion } from './motion';
export { Sparkline, type SparklineProps } from './sparkline';
export { type ChartTheme, readChartTheme, useChartTheme } from './theme';
export type { ChartSpec, Row, SeriesPoint } from './types';
export { useMetricStream } from './use-metric-stream';
export { appendWindow, type WindowOptions, windowPoints } from './window';
