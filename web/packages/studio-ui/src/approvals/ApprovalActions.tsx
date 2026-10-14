// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/chat/ComposerPendingApprovalActions.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// agentd's three decisions (§53 §8.1: once, always for this thread, deny) as @loams/ui buttons with their keyboard hints, instead of the provider's option list and overflow menu. Loams' changes are Apache-2.0.

import { Button, Kbd } from "@loams/ui";
import { memo } from "react";

/** `ApprovalDecision` (G18): once, always for this thread, or deny. */
export type ApprovalDecision = "once" | "always" | "deny";

export const APPROVAL_OPTIONS: ReadonlyArray<{
  decision: ApprovalDecision;
  label: string;
  shortcut: string;
}> = [
  { decision: "once", label: "Allow once", shortcut: "Enter" },
  { decision: "always", label: "Always for this thread", shortcut: "Shift+Enter" },
  { decision: "deny", label: "Deny", shortcut: "Esc" },
];

export const ApprovalActions = memo(function ApprovalActions({
  isResponding,
  canRespond,
  onDecide,
}: {
  isResponding: boolean;
  canRespond: boolean;
  onDecide: (decision: ApprovalDecision) => void;
}) {
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {APPROVAL_OPTIONS.map((option) => (
        <Button
          key={option.decision}
          size="sm"
          variant={
            option.decision === "once"
              ? "primary"
              : option.decision === "deny"
                ? "danger"
                : "secondary"
          }
          disabled={isResponding || !canRespond}
          aria-keyshortcuts={option.shortcut.replace("Esc", "Escape")}
          onClick={() => onDecide(option.decision)}
        >
          {option.label}
          <Kbd className="ml-1.5">{option.shortcut}</Kbd>
        </Button>
      ))}
    </div>
  );
});
