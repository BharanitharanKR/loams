// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/lib/diffFileContents.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// one loader over the DiffSource seam (agentd's GetCheckoutFileDiffText at the diff's checksum) in place of T3's git and pull-request loaders and its atom commands. Loams' changes are Apache-2.0.

// Full file contents for expanding the unchanged lines around a hunk (SD1 Task 20). The
// daemon answers only for the diff the reader is looking at (its checksum); a moved diff is
// `stale`, and the viewer keeps the hunks it has.

import type { FileDiffContentsLoader } from "@pierre/diffs";

import { resolveFileDiffPath } from "./diffRendering";
import type { CheckoutDiffView, DiffSelection, DiffSource } from "./source";

/** Turns the daemon's file-text call into the full-file loader Pierre uses for hunk expansion. */
export function createDiffFileContentsLoader(
  source: DiffSource,
  selection: DiffSelection,
  diff: CheckoutDiffView,
): FileDiffContentsLoader {
  const cacheKey = `${source.key}:${diff.checksum}`;
  return async (fileDiff) => {
    const newPath = resolveFileDiffPath(fileDiff);
    const oldPath = fileDiff.prevName
      ? resolveFileDiffPath({ ...fileDiff, name: fileDiff.prevName })
      : newPath;
    const text = await source.fileText(selection, diff, newPath);
    if (text.stale) throw new Error("The diff changed; refresh to see the new lines.");
    if (text.binary) throw new Error("A binary file has no lines to show.");
    const newFile = {
      name: newPath,
      contents: text.newText ?? "",
      cacheKey: `${cacheKey}:new:${newPath}`,
    };
    if (fileDiff.type === "rename-pure") {
      return { oldFile: null, newFile };
    }
    return {
      oldFile: {
        name: oldPath,
        contents: text.oldText ?? "",
        cacheKey: `${cacheKey}:old:${oldPath}`,
      },
      newFile,
    };
  };
}
