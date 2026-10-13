// The loams.admin.v1 and loams.auth.v1 values as the pages show them: proto
// enums as the words the original console used, Timestamps as ISO strings for
// @loams/ui's formatters, int64 counts as numbers, and errors as one sentence.

import type { Timestamp } from '@bufbuild/protobuf/wkt';
import { timestampDate } from '@bufbuild/protobuf/wkt';
import { Code, ConnectError } from '@connectrpc/connect';
import { admin } from '@loams/proto';
import type { AvatarKind } from '@loams/ui';

/** A Timestamp as ISO 8601, or undefined when unset ("never"). */
export function iso(ts: Timestamp | undefined): string | undefined {
  return ts ? timestampDate(ts).toISOString() : undefined;
}

/** An int64 count (a bigint in protobuf-es) as a number for display. */
export function num(n: bigint | number | undefined): number {
  return Number(n ?? 0);
}

export const ORG_ROLE: Record<number, string> = {
  [admin.OrgRole.OWNER]: 'owner',
  [admin.OrgRole.ADMIN]: 'admin',
  [admin.OrgRole.MEMBER]: 'member',
};

export const PROJECT_ROLE: Record<number, string> = {
  [admin.ProjectRole.ADMIN]: 'Admin',
  [admin.ProjectRole.DEVELOPER]: 'Developer',
  [admin.ProjectRole.VIEWER]: 'Viewer',
};

const AVATAR: Record<number, AvatarKind> = {
  [admin.SubjectKind.USER]: 'user',
  [admin.SubjectKind.TEAM]: 'team',
  [admin.SubjectKind.AGENT]: 'agent',
  [admin.SubjectKind.SERVICE_ACCOUNT]: 'service_account',
  [admin.SubjectKind.SYSTEM]: 'system',
};

/** The avatar for a subject (a user, team, agent, service account or the system). */
export function avatarKind(kind: admin.SubjectKind | undefined): AvatarKind {
  return AVATAR[kind ?? 0] ?? 'user';
}

export const FLOW_LABEL: Record<number, string> = {
  [admin.TokenFlow.FEDERATION]: 'Workload identity',
  [admin.TokenFlow.DELEGATION]: 'Delegated by a user',
  [admin.TokenFlow.VENDING]: 'Vended from a parent',
};

/** Every action a policy or a key can grant, in the contract's order. */
export const ACTIONS: { id: string; label: string }[] = [
  { id: 'query', label: 'Query (search and SQL)' },
  { id: 'collections:read', label: 'Read collections and documents' },
  { id: 'collections:write', label: 'Write documents' },
  { id: 'documents:delete', label: 'Delete documents' },
  { id: 'streams:read', label: 'Read streams' },
  { id: 'streams:produce', label: 'Produce to streams' },
  { id: 'mcp:tools', label: 'Use MCP tools' },
];

/** A failed call as one sentence for the page. */
export function errorText(error: unknown): string {
  if (error instanceof ConnectError) {
    if (error.code === Code.Unavailable || error.code === Code.Unknown) {
      return (
        error.rawMessage || 'The request failed. Check that the API is reachable and try again.'
      );
    }
    return error.rawMessage || error.message;
  }
  if (error instanceof Error && error.message) return error.message;
  return 'The request failed. Check that the API is reachable and try again.';
}

/** A slug from a display name: "QA env" → "qa-env". */
export function slugify(name: string): string {
  return name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}
