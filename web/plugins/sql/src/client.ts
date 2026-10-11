// The Loams SQL page's client: the shapes of loams.sqldb.v1 (§47 §11, SQ1 Task 7) as a
// typed interface. The protos have not landed in @loams/proto yet, so the page runs
// against `createMockSqlClient` (mock.ts). When `loams.sqldb.v1` is generated, a Connect
// client implements this same interface and the page does not change.

/** §47 §14: the serverless lifecycle of a database (and of each branch's compute pool). */
export type DatabaseStage =
  | 'creating'
  | 'running'
  | 'suspending'
  | 'suspended'
  | 'resuming'
  | 'branching'
  | 'restoring'
  | 'failed'
  | 'deleting';

/** §15: compute classes. */
export type ComputeClass = 'xs' | 's' | 'm' | 'l' | 'xl' | '2xl';

/** `Branch.branch_kind`: copy branches in v1; `cow` is reserved (D727). */
export type BranchKind = 'copy' | 'cow';

export interface Database {
  /** `db_` id. */
  id: string;
  namespace: string;
  name: string;
  stage: DatabaseStage;
  /** `tidb-v8.5.x`. */
  engineVersion: string;
  computeClass: ComputeClass;
  /** Idle seconds before suspend; 0 disables (§14). */
  suspendAfterSeconds: number;
  /** The default branch's id. */
  defaultBranchId: string;
  storageBytes: number;
  createTime: string;
}

export interface Branch {
  /** `br_` + 16 base32; also the TiKV keyspace name. */
  id: string;
  databaseId: string;
  name: string;
  /** Empty for the root branch. */
  parentId: string;
  branchKind: BranchKind;
  /** The TSO the branch was copied at, as a decimal string (u64). Empty for the root. */
  pointTs: string;
  stage: DatabaseStage;
  createTime: string;
}

/** `GetConnectionInfo`. The password is present only on the call that minted it. */
export interface ConnectionInfo {
  host: string;
  port: number;
  user: string;
  database: string;
  /** Shown once: later calls return an empty string. */
  password: string;
  tlsRequired: boolean;
}

export interface QueryColumn {
  name: string;
  type: string;
}

export interface QueryResult {
  columns: QueryColumn[];
  /** Cells as text; null is SQL NULL. */
  rows: (string | null)[][];
  rowsAffected: number;
  truncated: boolean;
  elapsedMs: number;
}

/** A refused call: a snake_case `ErrorInfo.reason` and a message. */
export class SqlFailure extends Error {
  constructor(
    readonly reason: string,
    message: string,
  ) {
    super(message);
    this.name = 'SqlFailure';
  }
}

export function failureMessage(e: unknown): string {
  if (e instanceof SqlFailure) return `${e.message} (${e.reason})`;
  if (e instanceof Error) return e.message;
  return String(e);
}

/** What the page asks for on every query. */
export const PAGE_MAX_ROWS = 1000;
export const PAGE_TIMEOUT_MS = 30_000;

export interface QueryInput {
  databaseId: string;
  branchId: string;
  statement: string;
  maxRows: number;
  timeoutMs: number;
}

export interface SqlClient {
  listDatabases(namespace: string): Promise<Database[]>;
  getDatabase(namespace: string, id: string): Promise<Database>;
  suspendDatabase(namespace: string, id: string, idempotencyKey: string): Promise<Database>;
  resumeDatabase(namespace: string, id: string, idempotencyKey: string): Promise<Database>;
  listBranches(namespace: string, databaseId: string): Promise<Branch[]>;
  /**
   * `GetConnectionInfo`. With `rotate`, the server mints a fresh password and returns it
   * this once (RoleService.RotateRolePassword behind it).
   */
  getConnectionInfo(
    namespace: string,
    databaseId: string,
    branchId: string,
    rotate: boolean,
  ): Promise<ConnectionInfo>;
  query(namespace: string, input: QueryInput, signal?: AbortSignal): Promise<QueryResult>;
}

/** A fresh idempotency key for a mutation. */
export function idempotencyKey(): string {
  const c = globalThis.crypto;
  if (c?.randomUUID) return c.randomUUID();
  return `k-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

/** The stages in which suspend and resume make sense. */
export const canSuspend = (s: DatabaseStage) => s === 'running';
export const canResume = (s: DatabaseStage) => s === 'suspended' || s === 'failed';
/** Stages the page polls through until they settle. */
export const isTransitional = (s: DatabaseStage) =>
  s === 'creating' ||
  s === 'suspending' ||
  s === 'resuming' ||
  s === 'branching' ||
  s === 'restoring' ||
  s === 'deleting';

/** A MySQL URI for the connection; the password stays out unless given. */
export function connectionUri(c: ConnectionInfo, password?: string): string {
  const user = encodeURIComponent(c.user);
  const auth = password ? `${user}:${encodeURIComponent(password)}` : user;
  const tls = c.tlsRequired ? '?ssl-mode=REQUIRED' : '';
  return `mysql://${auth}@${c.host}:${c.port}/${encodeURIComponent(c.database)}${tls}`;
}

/** A `mysql` CLI command; the password is prompted for, never on the command line. */
export function mysqlCommand(c: ConnectionInfo): string {
  const tls = c.tlsRequired ? ' --ssl-mode=REQUIRED' : '';
  return `mysql -h ${c.host} -P ${c.port} -u ${c.user} -p ${c.database}${tls}`;
}
