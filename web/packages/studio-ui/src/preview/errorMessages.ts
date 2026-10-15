// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/preview/errorCodeMessages.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the import path only. Loams' changes are Apache-2.0.
import { PREVIEW_ERROR_CODE_MESSAGES } from "./constants";

/**
 * Resolve a friendly description for a Chromium / network error. Falls back
 * to the description string passed in when it isn't in our table.
 */
export function describePreviewError(description: string): string {
  const friendly = PREVIEW_ERROR_CODE_MESSAGES[description];
  if (friendly) return friendly;
  if (description.length > 0) return description;
  return "Network error";
}
