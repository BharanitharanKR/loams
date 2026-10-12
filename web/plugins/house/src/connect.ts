// The Analytics page's client over the console's Connect transport: `loams.house.v1`,
// served by the House front and proxied by the engine's main port (§49 §18.2). Results
// come as `JSON_ROWS` (ClickHouse `JSONCompactEachRow`), which the page decodes without
// an Arrow reader; the server's caps and refusals are the same either way.

import { create } from '@bufbuild/protobuf';
import { timestampDate } from '@bufbuild/protobuf/wkt';
import { Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';
import { errors, house, instance } from '@loams/proto';
import {
  type Cell,
  type Database,
  type ExecuteQueryRequest,
  type ExplainKind,
  type HistoryEntry,
  type HistoryQuery,
  type HouseClient,
  HouseError,
  type Page,
  type QueryFrame,
  type QueryStatus,
} from './client.js';

/** The package `GetInstance.services[]` lists when the engine proxies a House. */
export const HOUSE_PACKAGE = 'loams.house.v1';

const wireCode = (c: Code): string =>
  (Code[c] ?? 'unknown').replace(/[A-Z]/g, (m, i: number) => (i ? '_' : '') + m.toLowerCase());

/** A refused call as the page shows it: Connect code, `ErrorInfo.reason`, ClickHouse code. */
export function fromConnect(e: unknown): HouseError {
  if (e instanceof HouseError) return e;
  const err = ConnectError.from(e);
  const info = err.findDetails(errors.ErrorInfoSchema)[0];
  const metadata = { ...(info?.metadata ?? {}) };
  const ch = Number(metadata.clickhouse_code);
  return new HouseError({
    code: wireCode(err.code),
    message: err.rawMessage,
    reason: info?.reason || undefined,
    clickhouseCode: Number.isFinite(ch) && ch > 0 ? ch : undefined,
    clickhouseName: metadata.clickhouse_name || undefined,
    metadata,
  });
}

async function call<T>(f: () => Promise<T>): Promise<T> {
  try {
    return await f();
  } catch (e) {
    throw fromConnect(e);
  }
}

/** One `JSONCompactEachRow` batch: one JSON array per line. */
export function decodeRows(bytes: Uint8Array): Cell[][] {
  const text = new TextDecoder().decode(bytes);
  const rows: Cell[][] = [];
  for (const line of text.split('\n')) {
    if (line.trim() === '') continue;
    rows.push(JSON.parse(line) as Cell[]);
  }
  return rows;
}

const STATUS: Record<house.QueryStatus, QueryStatus | undefined> = {
  [house.QueryStatus.UNSPECIFIED]: undefined,
  [house.QueryStatus.RUNNING]: 'RUNNING',
  [house.QueryStatus.FINISHED]: 'FINISHED',
  [house.QueryStatus.FAILED]: 'FAILED',
  [house.QueryStatus.CANCELLED]: 'CANCELLED',
};
const STATUS_WIRE: Record<QueryStatus, house.QueryStatus> = {
  RUNNING: house.QueryStatus.RUNNING,
  FINISHED: house.QueryStatus.FINISHED,
  FAILED: house.QueryStatus.FAILED,
  CANCELLED: house.QueryStatus.CANCELLED,
};
const EXPLAIN: Record<ExplainKind, house.ExplainKind> = {
  PLAN: house.ExplainKind.PLAN,
  PIPELINE: house.ExplainKind.PIPELINE,
  SYNTAX: house.ExplainKind.SYNTAX,
  ESTIMATE: house.ExplainKind.ESTIMATE,
};

function entry(r: house.QueryRecord): HistoryEntry {
  return {
    queryId: r.queryId,
    principal: r.principal,
    startedAt: r.startedAt ? timestampDate(r.startedAt).toISOString() : '',
    durationMs: Number(r.durationMs),
    status: STATUS[r.status] ?? 'FINISHED',
    errorCode: r.errorCode,
    query: r.query,
    readRows: Number(r.readRows),
    readBytes: Number(r.readBytes),
    resultRows: Number(r.resultRows),
  };
}

function frame(f: house.ExecuteQueryResponse['frame']): QueryFrame | undefined {
  switch (f.case) {
    case 'header':
      return {
        kind: 'header',
        columns: f.value.columns.map((c) => ({ name: c.name, clickhouseType: c.clickhouseType })),
      };
    case 'batch':
      if (f.value.data.case === 'rowsJson')
        return { kind: 'batch', rows: decodeRows(f.value.data.value) };
      throw new HouseError({
        code: 'internal',
        message: 'The server answered with Arrow IPC batches; the console asked for JSON rows.',
        metadata: {},
      });
    case 'progress':
      return {
        kind: 'progress',
        progress: {
          readRows: Number(f.value.readRows),
          readBytes: Number(f.value.readBytes),
          totalRowsToRead: Number(f.value.totalRowsToRead),
          elapsedMs: Number(f.value.elapsedMs),
        },
      };
    case 'summary':
      return {
        kind: 'summary',
        summary: {
          resultRows: Number(f.value.resultRows),
          resultBytes: Number(f.value.resultBytes),
          readRows: Number(f.value.readRows),
          readBytes: Number(f.value.readBytes),
          writtenRows: Number(f.value.writtenRows),
          elapsedMs: Number(f.value.elapsedMs),
          truncated: f.value.truncated,
          queryId: f.value.queryId,
          snapshotIds: Object.entries(f.value.snapshotIds).map(([t, id]) => `${t}@${id}`),
        },
      };
    default:
      return undefined;
  }
}

/** `HouseClient` over `transport`; `available()` reads `GetInstance.services[]`. */
export function createHouseClient(transport: Transport): HouseClient {
  const svc = createClient(house.HouseService, transport);
  const inst = createClient(instance.InstanceService, transport);
  return {
    async available() {
      const info = await call(() => inst.getInstance({}));
      return info.services.some((s) => s.package === HOUSE_PACKAGE && s.available);
    },
    async *executeQuery(req: ExecuteQueryRequest, signal?: AbortSignal) {
      const request = create(house.ExecuteQueryRequestSchema, {
        sql: req.sql,
        database: req.database,
        settings: req.settings,
        params: req.params,
        // The page decodes JSON rows; Arrow needs a reader the console does not ship.
        format: house.ResultFormat.JSON_ROWS,
        maxRows: req.maxRows,
        maxBytes: req.maxBytes === undefined ? undefined : BigInt(req.maxBytes),
        timeoutMs: req.timeoutMs,
        queryId: req.queryId ?? '',
        readOnly: req.readOnly,
      });
      try {
        for await (const res of svc.executeQuery(request, { signal })) {
          const f = frame(res.frame);
          if (f) yield f;
        }
      } catch (e) {
        throw fromConnect(e);
      }
    },
    async cancelQuery(queryId) {
      await call(() => svc.cancelQuery({ queryId }));
    },
    explainQuery(req, signal) {
      return call(async () => {
        const res = await svc.explainQuery(
          { sql: req.sql, database: req.database, kind: EXPLAIN[req.kind] },
          { signal },
        );
        return { text: res.text };
      });
    },
    listDatabases(pageToken): Promise<Page<Database>> {
      return call(async () => {
        const res = await svc.listDatabases({ pageToken: pageToken ?? '' });
        return {
          items: res.databases.map((d) => ({
            name: d.name,
            tableCount: Number(d.tableCount),
            comment: d.comment,
          })),
          nextPageToken: res.nextPageToken,
        };
      });
    },
    listQueryHistory(q: HistoryQuery): Promise<Page<HistoryEntry>> {
      return call(async () => {
        const res = await svc.listQueryHistory({
          scope: q.scope === 'NAMESPACE' ? house.HistoryScope.NAMESPACE : house.HistoryScope.MINE,
          status: q.status ? STATUS_WIRE[q.status] : house.QueryStatus.UNSPECIFIED,
          textContains: q.textContains ?? '',
          pageSize: q.pageSize ?? 0,
          pageToken: q.pageToken ?? '',
        });
        return { items: res.queries.map(entry), nextPageToken: res.nextPageToken };
      });
    },
    getQuery(queryId) {
      return call(async () => {
        const res = await svc.getQuery({ queryId });
        if (!res.query)
          throw new HouseError({ code: 'not_found', message: `no query ${queryId}`, metadata: {} });
        return entry(res.query);
      });
    },
  };
}
