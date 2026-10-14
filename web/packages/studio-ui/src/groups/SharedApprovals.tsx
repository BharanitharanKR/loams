// "Approve for all variants" (SD1 Task 25; design §53 §8.1): when the same tool and
// arguments wait for approval in several members, one click answers each of them once, as
// one record per member. Arguments are the daemon's redacted ones, shown as text.

import { Button } from "@loams/ui";
import { ShieldCheck } from "lucide-react";
import { useState } from "react";

import type { SharedApproval } from "./model";

export function SharedApprovals({
  shared,
  labelOf,
  onApproveAll,
}: {
  readonly shared: readonly SharedApproval[];
  /** A member's name, for the list of who asked. */
  readonly labelOf: (chatId: string) => string;
  /** Answers each approval once; rejects with the first failure. */
  readonly onApproveAll: (approvals: SharedApproval["approvals"]) => Promise<void>;
}) {
  const [pending, setPending] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  if (shared.length === 0) return null;
  return (
    <section
      aria-label="Pending in several variants"
      className="flex flex-col gap-2 border border-info bg-info-soft p-3"
    >
      <h2 className="m-0 flex items-center gap-1.5 text-[0.86rem] font-semibold text-ink">
        <ShieldCheck size={15} aria-hidden="true" className="text-info" />
        Approvals waiting in several variants
      </h2>
      <ul className="m-0 flex list-none flex-col gap-2 p-0">
        {shared.map((item) => (
          <li key={item.key} className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <span className="font-mono text-[0.8rem] font-semibold text-ink">{item.tool}</span>
            <code
              className="max-w-full truncate font-mono text-[0.76rem] text-muted"
              title={item.argsText}
            >
              {item.argsText}
            </code>
            <span className="text-[0.76rem] text-faint">
              {item.approvals.map((a) => labelOf(a.chatId)).join(", ")}
            </span>
            <Button
              size="sm"
              variant="primary"
              className="ml-auto"
              disabled={pending !== null}
              onClick={async () => {
                setPending(item.key);
                setError(null);
                try {
                  await onApproveAll(item.approvals);
                } catch (e) {
                  setError(e instanceof Error ? e.message : String(e));
                } finally {
                  setPending(null);
                }
              }}
            >
              {pending === item.key
                ? "Approving…"
                : `Approve for all variants (${item.approvals.length})`}
            </Button>
          </li>
        ))}
      </ul>
      {error && (
        <p role="alert" className="m-0 text-[0.8rem] text-danger">
          {error}
        </p>
      )}
    </section>
  );
}
