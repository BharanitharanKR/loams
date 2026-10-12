/**
 * The shared chart vocabulary.
 *
 * `ChartSpec` is the dashboard's chart description (`@loams-core/types`, the
 * schema `@loams-core/chart-specs` validates against). It is re-exported here so
 * a console plugin can type a spec without reaching into the core packages.
 */
export type { ChartSpec } from '@loams-core/types';

/** One sample of a metric: a time in epoch milliseconds, a value and an optional series name. */
export type SeriesPoint = { t: number; v: number; series?: string };

/** A row of chart data, keyed by the field names a spec's encodings use. */
export type Row = Record<string, unknown>;
