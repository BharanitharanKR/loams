// The few @pierre/trees types that the vendored diffFileTree.logic.ts names (SD1 Task 20).
// Studio's trees are the VirtualTree fallback, not @pierre/trees (ruling T21-1), so the
// as-is logic file imports these structural stand-ins instead of the beta package.

/** How a file changed, in @pierre/trees' words. */
export type GitStatus = "added" | "deleted" | "modified" | "renamed" | "untracked" | "ignored";

/** One step of an incremental tree update: add or remove a path. */
export type FileTreeBatchOperation =
  | { readonly type: "add"; readonly path: string }
  | { readonly type: "remove"; readonly path: string; readonly recursive?: boolean };

/** Orders two rows of the tree. */
export type FileTreeSortComparator = (
  left: { readonly path: string; readonly depth: number },
  right: { readonly path: string; readonly depth: number },
) => number;
