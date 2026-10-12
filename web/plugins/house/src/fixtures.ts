// The sample House's data (mock.ts): a `default.events` lake table, the catalog the
// page lists, a slice of `system.functions` for the editor's completions, and a few
// history rows. Made-up values only: no hosts, keys or endpoints.

import type { Cell, ColumnHeader, Database, HistoryEntry } from './client.js';

export const DATABASES: Database[] = [
  { name: 'default', tableCount: 2, comment: '' },
  { name: 'analytics', tableCount: 1, comment: 'Rollups' },
];

export interface SampleTable {
  database: string;
  name: string;
  class: 'LAKE' | 'REALTIME' | 'EXTERNAL';
}

export const TABLES: SampleTable[] = [
  { database: 'default', name: 'events', class: 'LAKE' },
  { database: 'default', name: 'clicks', class: 'REALTIME' },
  { database: 'analytics', name: 'daily', class: 'LAKE' },
];

export const EVENTS: { columns: ColumnHeader[]; rows: Cell[][] } = {
  columns: [
    { name: 'id', clickhouseType: 'UInt64' },
    { name: 'ts', clickhouseType: "DateTime64(3, 'UTC')" },
    { name: 'kind', clickhouseType: 'LowCardinality(String)' },
    { name: 'amount', clickhouseType: 'Nullable(Decimal(18, 2))' },
    { name: 'tags', clickhouseType: 'Array(String)' },
  ],
  rows: [
    ['1', '2026-10-01 09:00:00.000', 'signup', null, ['web']],
    ['2', '2026-10-01 09:05:12.250', 'purchase', '19.99', ['web', 'promo']],
    ['3', '2026-10-01 10:41:03.000', 'purchase', '5.00', []],
    ['4', '2026-10-02 08:12:44.900', 'refund', '-5.00', ['support']],
    ['5', '2026-10-02 13:30:00.000', 'signup', null, ['mobile']],
    ['6', '2026-10-03 17:02:19.125', 'purchase', '42.50', ['mobile']],
  ],
};

export const FUNCTIONS: { name: string; aggregate: boolean }[] = [
  { name: 'count', aggregate: true },
  { name: 'sum', aggregate: true },
  { name: 'avg', aggregate: true },
  { name: 'uniq', aggregate: true },
  { name: 'uniqExact', aggregate: true },
  { name: 'quantile', aggregate: true },
  { name: 'groupArray', aggregate: true },
  { name: 'argMax', aggregate: true },
  { name: 'toDate', aggregate: false },
  { name: 'toStartOfHour', aggregate: false },
  { name: 'toStartOfDay', aggregate: false },
  { name: 'formatReadableSize', aggregate: false },
  { name: 'arrayJoin', aggregate: false },
  { name: 'JSONExtractString', aggregate: false },
  { name: 'lower', aggregate: false },
  { name: 'now', aggregate: false },
];

export const HISTORY: HistoryEntry[] = [
  {
    queryId: 'sample-0003',
    principal: 'you',
    startedAt: '2026-10-10T16:20:00.000Z',
    durationMs: 12,
    status: 'FAILED',
    errorCode: 344,
    query: "SELECT * FROM default.events INTO OUTFILE 'events.csv'",
    readRows: 0,
    readBytes: 0,
    resultRows: 0,
  },
  {
    queryId: 'sample-0002',
    principal: 'you',
    startedAt: '2026-10-10T16:18:40.000Z',
    durationMs: 48,
    status: 'FINISHED',
    errorCode: 0,
    query:
      'SELECT kind, count() AS n, sum(amount) AS total\nFROM default.events\nGROUP BY kind\nORDER BY n DESC',
    readRows: 6,
    readBytes: 288,
    resultRows: 3,
  },
  {
    queryId: 'sample-0001',
    principal: 'a-teammate',
    startedAt: '2026-10-10T15:02:11.000Z',
    durationMs: 230,
    status: 'FINISHED',
    errorCode: 0,
    query: 'SELECT count() FROM analytics.daily',
    readRows: 31,
    readBytes: 1488,
    resultRows: 1,
  },
];

/** ClickHouse keywords the editor completes, beside `system.functions`. */
export const KEYWORDS = [
  'SELECT',
  'FROM',
  'WHERE',
  'PREWHERE',
  'GROUP BY',
  'ORDER BY',
  'LIMIT',
  'OFFSET',
  'HAVING',
  'WITH',
  'JOIN',
  'LEFT JOIN',
  'ARRAY JOIN',
  'UNION ALL',
  'FINAL',
  'SAMPLE',
  'SETTINGS',
  'FORMAT',
  'INSERT INTO',
  'CREATE TABLE',
  'DESCRIBE TABLE',
  'SHOW TABLES',
  'EXPLAIN',
];
