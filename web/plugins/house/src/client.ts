// The Analytics page's client: the shape of `loams.house.v1.HouseService` (§49 §18.2,
// HS1 Task 8). The proto is being written now and is not in `@loams/proto` yet, so this
// file defines the calls the page makes as a typed interface. `mock.ts` implements it
// from fixtures; a Connect implementation over the console's transport replaces the
// mock once `loams.house.v1` lands, decoding `ARROW_IPC` batches into `Cell` rows.
//
// TODO(HS1 Task 8): replace every request, frame, history and failure interface below
// with the generated `loams.house.v1` types from `@loams/proto`, keeping only the
// page-side helpers (`toFailure`, `reasonText`, the row caps).

/** The service name `GetInstance.services[]` lists when a House is configured. */
export const HOUSE_SERVICE = 'loams.house.v1.HouseService';

/** What the page asks for on every run (§49 §18.3). */
export const PAGE_MAX_ROWS = 1000;
/** The server's ceiling for `max_rows` (§49 §18.2). */
export const SERVER_MAX_ROWS = 100_000;
export const PAGE_TIMEOUT_MS = 30_000;

export type QueryFormat = 'ARROW_IPC' | 'JSON_ROWS';

export interface ExecuteQueryRequest {
  sql: string;
  database: string;
  settings: Record<string, string>;
  params: Record<string, string>;
  format: QueryFormat;
  maxRows: number;
  maxBytes?: number;
  timeoutMs: number;
  queryId?: string;
  readOnly: boolean;
}

export interface ColumnHeader {
  name: string;
  clickhouseType: string;
}

/** One decoded cell. Arrays, tuples and maps arrive as JSON values. */
export type Cell = string | number | boolean | null | Cell[] | { [key: string]: Cell };

export interface Progress {
  readRows: number;
  readBytes: number;
  /** 0 when the engine does not know yet. */
  totalRowsToRead: number;
  elapsedMs: number;
}

export interface Summary {
  resultRows: number;
  resultBytes: number;
  readRows: number;
  readBytes: number;
  writtenRows: number;
  elapsedMs: number;
  /** `max_rows` or `max_bytes` cut the result; the server always says so. */
  truncated: boolean;
  queryId: string;
  snapshotIds: string[];
}

/** One frame of `ExecuteQuery`'s server stream, batches already decoded to rows. */
export type QueryFrame =
  | { kind: 'header'; columns: ColumnHeader[] }
  | { kind: 'batch'; rows: Cell[][] }
  | { kind: 'progress'; progress: Progress }
  | { kind: 'summary'; summary: Summary };

export type ExplainKind = 'PLAN' | 'PIPELINE' | 'SYNTAX' | 'ESTIMATE';

export interface Database {
  name: string;
  tableCount: number;
  comment: string;
}

export type HistoryScope = 'MINE' | 'NAMESPACE';
export type QueryStatus = 'RUNNING' | 'FINISHED' | 'FAILED' | 'CANCELLED';

export interface HistoryEntry {
  queryId: string;
  principal: string;
  startedAt: string;
  durationMs: number;
  status: QueryStatus;
  /** The ClickHouse error code when the query failed, else 0. */
  errorCode: number;
  /** Truncated by the server to 4 KiB; `getQuery` returns the full text. */
  query: string;
  readRows: number;
  readBytes: number;
  resultRows: number;
}

export interface HistoryQuery {
  scope: HistoryScope;
  status?: QueryStatus;
  textContains?: string;
  pageSize?: number;
  pageToken?: string;
}

export interface Page<T> {
  items: T[];
  nextPageToken: string;
}

/** The calls of `HouseService` the page uses. */
export interface HouseClient {
  /** Whether this server lists `HOUSE_SERVICE` (from `GetInstance.services[]`). */
  available(): Promise<boolean>;
  executeQuery(req: ExecuteQueryRequest, signal?: AbortSignal): AsyncIterable<QueryFrame>;
  cancelQuery(queryId: string): Promise<void>;
  explainQuery(
    req: { sql: string; database: string; kind: ExplainKind },
    signal?: AbortSignal,
  ): Promise<{ text: string }>;
  listDatabases(pageToken?: string): Promise<Page<Database>>;
  listQueryHistory(q: HistoryQuery): Promise<Page<HistoryEntry>>;
  getQuery(queryId: string): Promise<HistoryEntry>;
}

/** The `ErrorInfo.reason`s of §49 §18.2. */
export type HouseReason =
  | 'house_syntax_error'
  | 'house_unknown_table'
  | 'house_access_denied'
  | 'house_read_only'
  | 'house_timeout'
  | 'house_memory_limit'
  | 'house_too_many_queries'
  | 'house_quota_exceeded'
  | 'house_disabled_function'
  | 'house_cancelled'
  | 'house_not_configured';

/** A refused call: Connect code, `ErrorInfo.reason` and the ClickHouse code and name. */
export interface HouseFailure {
  /** Connect's wire form, for example `invalid_argument`. */
  code: string;
  message: string;
  reason?: HouseReason | string;
  clickhouseCode?: number;
  clickhouseName?: string;
  metadata: Record<string, string>;
}

/** What the mock and, later, the Connect client throw. */
export class HouseError extends Error {
  readonly failure: HouseFailure;
  constructor(failure: HouseFailure) {
    super(failure.message);
    this.name = 'HouseError';
    this.failure = failure;
  }
}

export function toFailure(e: unknown): HouseFailure {
  if (e instanceof HouseError) return e.failure;
  const message = e instanceof Error ? e.message : String(e);
  return { code: 'unknown', message, metadata: {} };
}

/** A sentence for each reason, shown under the server's own message. */
export const REASON_TEXT: Record<HouseReason, string> = {
  house_syntax_error: 'The statement is not valid ClickHouse SQL.',
  house_unknown_table: 'A table or database in the statement does not exist.',
  house_access_denied:
    'The House refused this statement: it uses a clause, function, engine or setting that is not allowed here.',
  house_read_only: 'The statement changes data, and this run was read-only.',
  house_timeout: 'The statement ran past its time limit.',
  house_memory_limit: 'The statement needed more memory than its limit.',
  house_too_many_queries: 'Too many statements are running. Try again shortly.',
  house_quota_exceeded: "The namespace's quota is used up.",
  house_disabled_function:
    'The statement uses something the House turns off, such as INTO OUTFILE, FROM INFILE or a host table function.',
  house_cancelled: 'The statement was cancelled.',
  house_not_configured: 'This server has no House configured.',
};

export function reasonText(reason: string | undefined): string | undefined {
  return reason && reason in REASON_TEXT ? REASON_TEXT[reason as HouseReason] : undefined;
}

/** A query id the page can cancel by. */
export function newQueryId(): string {
  const c = globalThis.crypto;
  if (c?.randomUUID) return c.randomUUID();
  return `q-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}
