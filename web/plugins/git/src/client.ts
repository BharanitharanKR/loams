// The Git page's client: a typed view of `loams.repos.v1` (§36 §19.10, §19.13; GT1's
// public API: RepoService, WorkspaceService, ObjectService, EnvService). The package is
// not generated into @loams/proto yet, so the page talks to this interface, and
// `createGitClient` answers from the fixture-backed mock (mock.ts) until the services land.
// When they do, the Connect-backed client implements the same interface over `transport`.
//
// Nothing here carries a secret value: env keys come back as names, fingerprints and
// versions only (D844, D845). The interface has no call that returns plaintext.

import type { Transport } from '@connectrpc/connect';
import { createMockGitClient } from './mock.js';

/** Default page size for lists the server paginates. */
export const PAGE_SIZE = 100;
/** The page asks for at most this many bytes of a file (larger files show a notice). */
export const MAX_FILE_BYTES = 1024 * 1024;
/** `MAX_CHANGED_HINT` (§19.4.2): past this, the snapshot omits its changed paths. */
export const MAX_CHANGED_HINT = 10_000;

export interface Page<T> {
  items: T[];
  /** "" when there are no more. */
  nextPageToken: string;
  /** The total, when the server knows it cheaply (unfiltered listings). */
  totalSize?: number;
}

export interface Repo {
  repoId: string;
  name: string;
  defaultBranch: string;
  /** The repository's sequence number (§4.4). */
  seq: number;
  workspaceCount: number;
  hasEnv: boolean;
  updatedAt: string;
}

export interface WorkspaceSummary {
  /** `w` followed by a 26-character ULID (§19.4.1). */
  wsId: string;
  /** "" for unnamed (agent) workspaces. */
  name: string;
  owner: string;
  track: string;
  /** The latest snapshot number. */
  n: number;
  changedCount: number;
  agent: boolean;
  updatedAt: string;
}

export type ChangeKind = 'modified' | 'added' | 'deleted' | 'untracked' | 'type_changed';

export interface ChangedPath {
  path: string;
  kind: ChangeKind;
  /** The index differs from HEAD at this path. */
  staged: boolean;
}

export interface Workspace extends WorkspaceSummary {
  repoId: string;
  /** The commit last rebased onto, hex. */
  base: string;
  /** The repository seq that base was read at. */
  baseSeq: number;
  head: string;
  /** "" when detached. */
  headRef: string;
  /** The mount that wrote the latest snapshot. */
  writer: string;
  changed: ChangedPath[];
  /** More than MAX_CHANGED_HINT paths changed; the list above is empty. */
  changedOverflow: boolean;
  conflicts: string[];
  fork?: { repoId: string; wsId: string; n: number };
}

export interface Snapshot {
  n: number;
  createdAt: string;
  writer: string;
  principal: string;
  /** A named snapshot (`loams ws snapshot --name`). */
  label: string;
  head: string;
  changedCount: number;
  takeover?: { fromMount: string; fromN: number; reason: string };
}

export type EntryKind = 'dir' | 'file' | 'symlink' | 'submodule';

export interface TreeEntry {
  name: string;
  /** Full path from the root, no leading slash. */
  path: string;
  kind: EntryKind;
  /** From tree aux data (D830); undefined for directories. */
  size?: number;
  /** The overlay changes this path. */
  change?: ChangeKind;
  /** An env file (§19.13.2): rendered redacted unless the caller holds `env:read`. */
  env?: boolean;
}

export interface FileContent {
  path: string;
  size: number;
  binary: boolean;
  /** UTF-8 text, at most MAX_FILE_BYTES; for env files, the redacted render. */
  text: string;
  truncated: boolean;
}

export type PromotionStatus = 'promoted' | 'pending' | 'failed';

export interface Environment {
  name: string;
  branch: string;
  files: string[];
  /** The live set's revision (§19.13.4). */
  revision: number;
  lastPromotion?: {
    commit: string;
    seq: number;
    at: string;
    status: PromotionStatus;
    added: string[];
    changed: string[];
    removed: string[];
  };
}

/** Where one key stands in one environment. */
export type KeyEnvStatus =
  /** The branch tip pins the live version. */
  | 'live'
  /** The branch pins a version not yet promoted. */
  | 'pending'
  /** Rotated: the live version is newer than the pinned one (D843). */
  | 'rotated'
  /** Not in this environment's files. */
  | 'absent';

export interface KeyInEnv {
  status: KeyEnvStatus;
  pinnedVersion?: number;
  liveVersion?: number;
}

export type RotationStatus = 'ok' | 'due' | 'rotating' | 'failed';

export interface Rotation {
  provider: 'random' | 'loams-postgres' | 'loams-sql' | 'external' | 'webhook';
  every: string;
  grace: string;
  status: RotationStatus;
  lastRotatedAt?: string;
  nextDueAt?: string;
}

export interface EnvKey {
  key: string;
  /** The env file that defines it last (later files override earlier keys). */
  file: string;
  /** `# loams:plain`: non-secret configuration kept in git. */
  plain: boolean;
  /** The first 8 hex digits of the fingerprint; never the value. */
  fingerprint: string;
  environments: Record<string, KeyInEnv>;
  rotation?: Rotation;
}

export type AuditKind = 'read' | 'sealed' | 'promoted' | 'rotated' | 'destroyed';

export interface AuditEvent {
  id: string;
  kind: AuditKind;
  at: string;
  principal: string;
  environment: string;
  /** The workspace or deployment it happened in, when there is one. */
  subject: string;
  keys: string[];
  versions: number[];
}

export interface RepoRef {
  namespace: string;
  repoId: string;
}

export interface WsRef extends RepoRef {
  wsId: string;
}

export interface GitClient {
  listRepos(
    req: { namespace: string; pageToken?: string; pageSize?: number },
    signal?: AbortSignal,
  ): Promise<Page<Repo>>;
  /** Server-side search over names, owners and ids; paged. */
  listWorkspaces(
    req: RepoRef & { query?: string; pageToken?: string; pageSize?: number },
    signal?: AbortSignal,
  ): Promise<Page<WorkspaceSummary>>;
  getWorkspace(req: WsRef, signal?: AbortSignal): Promise<Workspace>;
  listSnapshots(
    req: WsRef & { pageToken?: string; pageSize?: number },
    signal?: AbortSignal,
  ): Promise<Page<Snapshot>>;
  /** One directory's entries at `ws@n` (latest when n is undefined). `path` "" is the root. */
  getTree(req: WsRef & { n?: number; path: string }, signal?: AbortSignal): Promise<TreeEntry[]>;
  readFile(req: WsRef & { n?: number; path: string }, signal?: AbortSignal): Promise<FileContent>;
  listEnvironments(req: RepoRef, signal?: AbortSignal): Promise<Environment[]>;
  listEnvKeys(req: RepoRef, signal?: AbortSignal): Promise<EnvKey[]>;
  listEnvAudit(
    req: RepoRef & { environment?: string; pageToken?: string; pageSize?: number },
    signal?: AbortSignal,
  ): Promise<Page<AuditEvent>>;
}

/** A refused call, decoded for display. */
export interface GitFailure {
  code: string;
  message: string;
  reason?: string;
}

export class GitError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly reason?: string,
  ) {
    super(message);
    this.name = 'GitError';
  }
}

export function toFailure(e: unknown): GitFailure {
  if (e instanceof GitError) return { code: e.code, message: e.message, reason: e.reason };
  if (e instanceof Error) return { code: 'unknown', message: e.message };
  return { code: 'unknown', message: String(e) };
}

export const isAbort = (e: unknown) => e instanceof DOMException && e.name === 'AbortError';

/**
 * The page's client. `loams.repos.v1` is not served or generated yet, so this returns the
 * mock; the transport is accepted now so the swap is local to this function.
 */
export function createGitClient(_transport?: Transport): GitClient {
  return createMockGitClient();
}

export const shortOid = (oid: string) => oid.slice(0, 12);
