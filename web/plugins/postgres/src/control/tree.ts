import type { PgBranch } from './types.js';

export interface BranchRow {
  branch: PgBranch;
  depth: number;
  /** Ids of the branch's direct children; a branch with children cannot be deleted. */
  children: string[];
}

/**
 * Branches as a tree by `parent_id`, flattened depth-first. A branch whose parent is not
 * in the list is a root. The default branch sorts first among its siblings, then by name,
 * then id, so the order is stable.
 */
export function branchTree(branches: PgBranch[]): BranchRow[] {
  const ids = new Set(branches.map((b) => b.id));
  const kids = new Map<string, PgBranch[]>();
  const roots: PgBranch[] = [];
  for (const b of branches) {
    if (b.parentId && b.parentId !== b.id && ids.has(b.parentId)) {
      const list = kids.get(b.parentId) ?? [];
      list.push(b);
      kids.set(b.parentId, list);
    } else roots.push(b);
  }
  const cmp = (a: PgBranch, b: PgBranch) =>
    Number(b.isDefault) - Number(a.isDefault) ||
    a.name.localeCompare(b.name) ||
    a.id.localeCompare(b.id);
  const out: BranchRow[] = [];
  const seen = new Set<string>();
  const walk = (b: PgBranch, depth: number) => {
    if (seen.has(b.id)) return;
    seen.add(b.id);
    const children = (kids.get(b.id) ?? []).sort(cmp);
    out.push({ branch: b, depth, children: children.map((c) => c.id) });
    for (const c of children) walk(c, depth + 1);
  };
  for (const r of roots.sort(cmp)) walk(r, 0);
  return out;
}

/** Why the page will not offer Delete for a branch, or undefined when it may. */
export function deleteBlocker(row: BranchRow): string | undefined {
  if (row.branch.isDefault) return 'The default branch cannot be deleted.';
  if (row.children.length > 0) return 'Delete its child branches first.';
  if (row.branch.state === 'deleting') return 'Already being deleted.';
  return undefined;
}

/** A Postgres LSN, `X/Y` in hex. */
export const LSN_PATTERN = /^[0-9A-Fa-f]{1,8}\/[0-9A-Fa-f]{1,8}$/;
