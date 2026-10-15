// loams-agentd's git errors as the toolbar shows them (SD1 Task 23; Shared contracts'
// reasons; T9-1). The reason is in the Connect error's `loams.errors.v1.ErrorInfo`;
// `merge_conflict` lists its paths under `metadata["paths"]`, a JSON array. A
// git-write call the console refused for want of `agentd:git-write` (T6-3) is
// `permission_denied` with no detail.

import { Code, ConnectError } from '@connectrpc/connect';
import { ErrorInfoSchema } from '@loams/proto/errors';
import type { GitActionError } from '@loams/studio-ui';

function pathsOf(raw: string | undefined): string[] | undefined {
  if (!raw) return undefined;
  try {
    const parsed: unknown = JSON.parse(raw);
    return Array.isArray(parsed) && parsed.every((p) => typeof p === 'string') ? parsed : undefined;
  } catch {
    return undefined;
  }
}

export function gitErrorOf(e: unknown): GitActionError {
  const err = ConnectError.from(e);
  const info = err.findDetails(ErrorInfoSchema)[0];
  const reason =
    info?.reason ||
    (err.code === Code.PermissionDenied
      ? 'permission_denied'
      : err.code === Code.Unavailable
        ? 'unavailable'
        : undefined);
  const paths = pathsOf(info?.metadata.paths);
  return {
    ...(reason ? { reason } : {}),
    message: err.rawMessage || err.message,
    ...(paths ? { paths } : {}),
  };
}
