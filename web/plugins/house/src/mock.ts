// A sample House behind the `HouseClient` interface, answering from the fixtures in
// fixtures.ts, so the page works before `loams.house.v1` (HS1 Task 8) is served. It
// follows the contract of §49 §18.2 where the page can see it: frames in the order
// header, batch…, progress, summary; `max_rows` truncates and says so; a read-only run
// refuses writes with `house_read_only`; the deny list refuses `INTO OUTFILE`,
// `FROM INFILE` and host table functions with `344 SUPPORT_IS_DISABLED`; settings off
// the allowlist are `164 READONLY` or `115 UNKNOWN_SETTING`; every run lands in the
// history. It is not a SQL engine: statements outside the fixtures are `unimplemented`.

import {
  type Cell,
  type ColumnHeader,
  type Database,
  type ExecuteQueryRequest,
  type HistoryEntry,
  type HistoryQuery,
  type HouseClient,
  HouseError,
  type HouseFailure,
  newQueryId,
  type Page,
  type QueryFrame,
  SERVER_MAX_ROWS,
} from './client.js';
import { DATABASES, EVENTS, FUNCTIONS, HISTORY, TABLES } from './fixtures.js';
import { isAllowedSetting, isDeniedSetting, settingProblem } from './settings.js';

export interface MockOptions {
  /** Pause between batches, in ms, so streaming and Cancel are visible. 0 in tests. */
  delayMs?: number;
  /** Rows per batch frame. */
  batchRows?: number;
  /** Whether the server lists the House service. */
  available?: boolean;
  /** The principal history rows name. */
  principal?: string;
}

/** Comments and string literals blanked, so keywords inside them do not count. */
export function stripSql(sql: string): string {
  return sql
    .replace(/--[^\n]*/g, ' ')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/'(?:[^'\\]|\\.|'')*'/g, "''")
    .replace(/`[^`]*`/g, '``');
}

const WRITE =
  /^\s*(INSERT|CREATE|DROP|ALTER|TRUNCATE|RENAME|OPTIMIZE|DELETE|UPDATE|EXCHANGE|ATTACH|DETACH|SYSTEM|GRANT|REVOKE|KILL)\b/i;
const HOST_FUNCTIONS =
  /\b(file|url|s3|s3Cluster|remote|remoteSecure|hdfs|mysql|postgresql|jdbc|odbc|executable|azureBlobStorage)\s*\(/i;

function refuse(f: Omit<HouseFailure, 'metadata'> & { metadata?: Record<string, string> }): never {
  const metadata = { ...(f.metadata ?? {}) };
  if (f.clickhouseCode !== undefined) metadata.clickhouse_code = String(f.clickhouseCode);
  if (f.clickhouseName) metadata.clickhouse_name = f.clickhouseName;
  throw new HouseError({ ...f, metadata });
}

/** The classifier and deny list, as far as the sample needs them. Throws a refusal. */
export function classify(req: Pick<ExecuteQueryRequest, 'sql' | 'readOnly' | 'settings'>): void {
  const text = stripSql(req.sql);
  if (/\bINTO\s+OUTFILE\b/i.test(text) || /\bFROM\s+INFILE\b/i.test(text)) {
    const clause = /\bINTO\s+OUTFILE\b/i.test(text) ? 'INTO OUTFILE' : 'FROM INFILE';
    refuse({
      code: 'permission_denied',
      message: `Code: 344. DB::Exception: ${clause} is not supported on Loams House. (SUPPORT_IS_DISABLED)`,
      reason: 'house_disabled_function',
      clickhouseCode: 344,
      clickhouseName: 'SUPPORT_IS_DISABLED',
      metadata: { clause },
    });
  }
  const host = HOST_FUNCTIONS.exec(text);
  if (host) {
    refuse({
      code: 'permission_denied',
      message: `Code: 344. DB::Exception: Table function ${host[1]} is not supported on Loams House. (SUPPORT_IS_DISABLED)`,
      reason: 'house_disabled_function',
      clickhouseCode: 344,
      clickhouseName: 'SUPPORT_IS_DISABLED',
      metadata: { table_function: host[1] ?? '' },
    });
  }
  for (const [name, value] of Object.entries(req.settings)) {
    if (!isAllowedSetting(name)) {
      // A denied name is one the engine knows (164); anything else is unknown (115).
      const known = isDeniedSetting(name);
      refuse({
        code: 'invalid_argument',
        message: known
          ? `Code: 164. DB::Exception: Cannot modify '${name}' setting in readonly mode. (READONLY)`
          : `Code: 115. DB::Exception: Unknown setting '${name}'. (UNKNOWN_SETTING)`,
        reason: 'house_access_denied',
        clickhouseCode: known ? 164 : 115,
        clickhouseName: known ? 'READONLY' : 'UNKNOWN_SETTING',
        metadata: { setting: name },
      });
    }
    const problem = settingProblem(name, value);
    if (problem) {
      refuse({
        code: 'invalid_argument',
        message: `Code: 164. DB::Exception: ${problem} (READONLY)`,
        reason: 'house_access_denied',
        clickhouseCode: 164,
        clickhouseName: 'READONLY',
        metadata: { setting: name },
      });
    }
  }
  if (req.readOnly && WRITE.test(text)) {
    refuse({
      code: 'failed_precondition',
      message:
        'Code: 164. DB::Exception: Cannot execute query in readonly mode: the run was read-only. (READONLY)',
      reason: 'house_read_only',
      clickhouseCode: 164,
      clickhouseName: 'READONLY',
    });
  }
}

interface Answer {
  columns: ColumnHeader[];
  rows: (offset: number, count: number) => Cell[][];
  total: number;
  written?: number;
}

function answer(sql: string, database: string): Answer {
  const text = stripSql(sql).trim().replace(/;\s*$/, '');
  if (/^SELECT\s+version\s*\(\s*\)\s*$/i.test(text)) {
    return {
      columns: [{ name: 'version()', clickhouseType: 'String' }],
      rows: () => [['26.9.2.1']],
      total: 1,
    };
  }
  const numbers = /\bFROM\s+numbers(?:_mt)?\s*\(\s*(\d+)\s*\)/i.exec(text);
  if (numbers) {
    const n = Math.min(Number(numbers[1]), 10_000_000);
    return {
      columns: [
        { name: 'number', clickhouseType: 'UInt64' },
        { name: 'square', clickhouseType: 'UInt64' },
        { name: 'label', clickhouseType: 'String' },
      ],
      rows: (offset, count) =>
        Array.from({ length: Math.max(0, Math.min(count, n - offset)) }, (_, i) => {
          const v = offset + i;
          return [String(v), String(v * v), `n${v}`];
        }),
      total: n,
    };
  }
  if (/\bFROM\s+system\.functions\b/i.test(text)) {
    return {
      columns: [
        { name: 'name', clickhouseType: 'String' },
        { name: 'is_aggregate', clickhouseType: 'UInt8' },
      ],
      rows: (o, c) => FUNCTIONS.slice(o, o + c).map((f) => [f.name, f.aggregate ? 1 : 0]),
      total: FUNCTIONS.length,
    };
  }
  if (/\bFROM\s+system\.columns\b/i.test(text)) {
    // The editor's schema: only default.events has columns in the sample.
    const db = /database\s*=\s*'(\w+)'/i.exec(sql)?.[1] ?? database;
    const rows: Cell[][] =
      db === 'default' ? EVENTS.columns.map((c) => ['events', c.name, c.clickhouseType]) : [];
    return {
      columns: [
        { name: 'table', clickhouseType: 'String' },
        { name: 'name', clickhouseType: 'String' },
        { name: 'type', clickhouseType: 'String' },
      ],
      rows: (o, c) => rows.slice(o, o + c),
      total: rows.length,
    };
  }
  const from = /\bFROM\s+(?:(\w+)\.)?(\w+)\b/i.exec(text);
  if (from) {
    const db = from[1] ?? database;
    const table = from[2] ?? '';
    if (db === 'default' && table === 'events') {
      return {
        columns: EVENTS.columns,
        rows: (o, c) => EVENTS.rows.slice(o, o + c),
        total: EVENTS.rows.length,
      };
    }
    if (!TABLES.some((t) => t.database === db && t.name === table)) {
      refuse({
        code: 'not_found',
        message: `Code: 60. DB::Exception: Unknown table expression identifier '${db}.${table}'. (UNKNOWN_TABLE)`,
        reason: 'house_unknown_table',
        clickhouseCode: 60,
        clickhouseName: 'UNKNOWN_TABLE',
        metadata: { database: db, table },
      });
    }
  }
  if (WRITE.test(text)) {
    return { columns: [], rows: () => [], total: 0, written: /^\s*INSERT\b/i.test(text) ? 1 : 0 };
  }
  if (/^SELEC\b|^SELECT\s*$/i.test(text)) {
    refuse({
      code: 'invalid_argument',
      message: `Code: 62. DB::Exception: Syntax error: failed at position 1 ('${text.slice(0, 20)}'). (SYNTAX_ERROR)`,
      reason: 'house_syntax_error',
      clickhouseCode: 62,
      clickhouseName: 'SYNTAX_ERROR',
      metadata: { position: '1' },
    });
  }
  return refuse({
    code: 'unimplemented',
    message:
      'The sample House answers SELECT version(), numbers(N), system.functions, system.columns and default.events only.',
  });
}

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (ms <= 0) return resolve();
    const t = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => {
      clearTimeout(t);
      resolve();
    });
  });

const cancelled = () =>
  refuse({
    code: 'canceled',
    message: 'Code: 394. DB::Exception: Query was cancelled. (QUERY_WAS_CANCELLED)',
    reason: 'house_cancelled',
    clickhouseCode: 394,
    clickhouseName: 'QUERY_WAS_CANCELLED',
  });

function paginate<T>(items: T[], pageSize: number, token?: string): Page<T> {
  const start = token ? Number(token) || 0 : 0;
  const end = start + pageSize;
  return { items: items.slice(start, end), nextPageToken: end < items.length ? String(end) : '' };
}

export interface MockHouse extends HouseClient {
  /** Every request `executeQuery` received, for tests. */
  readonly requests: ExecuteQueryRequest[];
  readonly cancelled: string[];
}

export function createMockHouseClient(opts: MockOptions = {}): MockHouse {
  const delay = opts.delayMs ?? 40;
  const batchRows = opts.batchRows ?? 250;
  const principal = opts.principal ?? 'you';
  const history: HistoryEntry[] = HISTORY.map((h) => ({ ...h }));
  const requests: ExecuteQueryRequest[] = [];
  const cancelledIds: string[] = [];
  const live = new Map<string, AbortController>();

  const record = (e: HistoryEntry) => {
    const i = history.findIndex((h) => h.queryId === e.queryId);
    if (i >= 0) history[i] = e;
    else history.unshift(e);
  };

  async function* executeQuery(
    req: ExecuteQueryRequest,
    signal?: AbortSignal,
  ): AsyncGenerator<QueryFrame> {
    requests.push(req);
    const queryId = req.queryId || newQueryId();
    const started = Date.now();
    const kill = new AbortController();
    live.set(queryId, kill);
    const stop = () => signal?.aborted || kill.signal.aborted;
    const entry: HistoryEntry = {
      queryId,
      principal,
      startedAt: new Date(started).toISOString(),
      durationMs: 0,
      status: 'RUNNING',
      errorCode: 0,
      query: req.sql.slice(0, 4096),
      readRows: 0,
      readBytes: 0,
      resultRows: 0,
    };
    record(entry);
    try {
      classify(req);
      const a = answer(req.sql, req.database || 'default');
      const maxRows = Math.min(Math.max(1, req.maxRows || 1000), SERVER_MAX_ROWS);
      const sendRows = Math.min(a.total, maxRows);
      yield { kind: 'header', columns: a.columns };
      let sent = 0;
      while (sent < sendRows) {
        await sleep(delay, signal);
        if (stop()) cancelled();
        const rows = a.rows(sent, Math.min(batchRows, sendRows - sent));
        if (!rows.length) break;
        sent += rows.length;
        yield { kind: 'batch', rows };
        yield {
          kind: 'progress',
          progress: {
            readRows: sent,
            readBytes: sent * 24,
            totalRowsToRead: a.total,
            elapsedMs: Date.now() - started,
          },
        };
      }
      if (stop()) cancelled();
      const elapsedMs = Date.now() - started;
      Object.assign(entry, {
        status: 'FINISHED',
        durationMs: elapsedMs,
        readRows: sent,
        readBytes: sent * 24,
        resultRows: sent,
      });
      record({ ...entry });
      yield {
        kind: 'summary',
        summary: {
          resultRows: sent,
          resultBytes: sent * 24,
          readRows: sent,
          readBytes: sent * 24,
          writtenRows: a.written ?? 0,
          elapsedMs,
          truncated: a.total > sent,
          queryId,
          snapshotIds: [],
        },
      };
    } catch (e) {
      const f = e instanceof HouseError ? e.failure : undefined;
      record({
        ...entry,
        status: f?.reason === 'house_cancelled' ? 'CANCELLED' : 'FAILED',
        errorCode: Number(f?.metadata.clickhouse_code ?? 0),
        durationMs: Date.now() - started,
      });
      throw e;
    } finally {
      live.delete(queryId);
    }
  }

  return {
    requests,
    cancelled: cancelledIds,
    available: async () => opts.available ?? true,
    executeQuery,
    async cancelQuery(queryId) {
      cancelledIds.push(queryId);
      live.get(queryId)?.abort();
    },
    async explainQuery(req) {
      classify({ sql: req.sql, readOnly: true, settings: {} });
      const body = stripSql(req.sql).trim();
      if (req.kind === 'ESTIMATE')
        return { text: 'database\ttable\tparts\trows\tmarks\ndefault\tevents\t1\t6\t1' };
      if (req.kind === 'SYNTAX') return { text: body.replace(/\s+/g, ' ') };
      return {
        text: `Expression ((Project names + Projection))\n  Limit (preliminary LIMIT)\n    ReadFromStorage (${
          /\bFROM\s+([\w.]+(?:\([^)]*\))?)/i.exec(body)?.[1] ?? 'SystemOne'
        })`,
      };
    },
    async listDatabases(pageToken) {
      return paginate<Database>(DATABASES, 50, pageToken);
    },
    async listQueryHistory(q: HistoryQuery) {
      const needle = q.textContains?.toLowerCase();
      const rows = history.filter(
        (h) =>
          (q.scope === 'NAMESPACE' || h.principal === principal) &&
          (!q.status || h.status === q.status) &&
          (!needle || h.query.toLowerCase().includes(needle)),
      );
      return paginate(rows, q.pageSize ?? 50, q.pageToken);
    },
    async getQuery(queryId) {
      const h = history.find((x) => x.queryId === queryId);
      if (!h)
        refuse({ code: 'not_found', message: `No query ${queryId} in the history.`, metadata: {} });
      return { ...h };
    },
  };
}
