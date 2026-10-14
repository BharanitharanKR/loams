// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/chat/MessageCopyButton.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// a plain pill button with lucide's Copy and Check and a timed "Copied" state, instead of T3's MorphIcon, anchored toasts and clipboard hook. Loams' changes are Apache-2.0.

import { Check, Copy } from "lucide-react";
import { memo, useEffect, useRef, useState } from "react";

const COPIED_MS = 1200;

export const MessageCopyButton = memo(function MessageCopyButton({
  text,
  label = "Copy message",
  copy,
}: {
  text: string;
  label?: string;
  /** The clipboard writer; defaults to `navigator.clipboard`. */
  copy?: (text: string) => Promise<void>;
}) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(
    () => () => {
      if (timer.current) clearTimeout(timer.current);
    },
    [],
  );
  const write =
    copy ??
    ((value: string) =>
      typeof navigator !== "undefined" && navigator.clipboard
        ? navigator.clipboard.writeText(value)
        : Promise.reject(new Error("no clipboard")));
  return (
    <button
      type="button"
      aria-label={copied ? "Copied" : label}
      title={copied ? "Copied" : label}
      disabled={copied}
      onClick={() => {
        write(text).then(
          () => {
            setCopied(true);
            if (timer.current) clearTimeout(timer.current);
            timer.current = setTimeout(() => setCopied(false), COPIED_MS);
          },
          () => {
            // The clipboard refused: nothing changes.
          },
        );
      }}
      className="inline-flex size-6 cursor-pointer items-center justify-center rounded-pill border-0 bg-transparent text-faint hover:bg-raised hover:text-ink"
    >
      {copied ? (
        <Check className="size-3 text-grow" aria-hidden />
      ) : (
        <Copy className="size-3" aria-hidden />
      )}
    </button>
  );
});
