// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/lib/terminalCloseConfirm.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the desktop bridge's native confirm replaced by the question an AlertDialog asks; only a terminal whose shell is still running asks. Loams' changes are Apache-2.0.

// Closing a terminal stops its shell and drops its history (§53 §5.7), so closing one
// that is still running asks first. A terminal whose process has exited, or that the
// daemon no longer has, closes without asking.

import type { TerminalSessionStatus } from "./session";

/** Whether closing a terminal in this state needs the user's confirmation. */
export function needsCloseConfirm(status: TerminalSessionStatus): boolean {
  return status === "running" || status === "connecting";
}

/** The dialog's title and description for closing these terminals. */
export function terminalCloseQuestion(labels: readonly [string, ...string[]]): {
  title: string;
  description: string;
} {
  return labels.length === 1
    ? {
        title: `Close terminal "${labels[0]}"?`,
        description: "This stops the running process and clears its history.",
      }
    : {
        title: `Close ${labels.length} terminals?`,
        description: `This stops their running processes and clears their histories: ${labels
          .map((label) => `"${label}"`)
          .join(", ")}.`,
      };
}
