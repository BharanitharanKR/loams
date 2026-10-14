// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/preview/PreviewUnreachable.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// @loams/ui buttons and tokens; the "move to the environment's browser" action became "Open in browser"; PreviewFileNotShown dropped (downloads stay in the view's own policy). Loams' changes are Apache-2.0.
import { Button } from "@loams/ui";
import { useState } from "react";

import { describePreviewError } from "./errorMessages";

interface Props {
  url: string;
  /** Chromium net error code, e.g. -105, or 0 when unknown. */
  code: number;
  /** Stringified error, e.g. "ERR_NAME_NOT_RESOLVED". */
  description: string;
  onReload: () => void;
  /** Opens the address in the person's own browser. */
  onOpenInBrowser?: () => void;
}

/** Theme-aware port of Chromium's "This site can't be reached" page. */
export function PreviewUnreachable({ url, code, description, onReload, onOpenInBrowser }: Props) {
  const [showDetails, setShowDetails] = useState(false);
  const host = safeHost(url) ?? url;
  const friendly = describePreviewError(description);
  const errorLabel = description.length > 0 ? description : `ERR_${Math.abs(code) || "FAILED"}`;

  return (
    <div
      className="relative flex h-full min-h-0 w-full overflow-y-auto bg-bg"
      data-preview-unreachable
    >
      <div className="mx-auto flex w-full max-w-xl flex-1 flex-col px-8 py-12">
        <ErrorIcon className="mb-6 size-12 text-faint" />
        <h1 className="m-0 mb-3 text-xl font-semibold leading-tight text-ink">
          This site can&rsquo;t be reached
        </h1>
        <p className="m-0 text-sm leading-relaxed text-muted">
          <span className="font-semibold text-ink">{host}</span>: {friendly}.
        </p>

        {showDetails ? (
          <div className="mt-6 border border-rule bg-surface p-4 text-sm">
            <p className="m-0 mb-2 font-medium text-ink">Try:</p>
            <ul className="m-0 list-disc space-y-1 pl-5 text-muted">
              <li>Confirming the dev server is running</li>
              <li>Starting it again from the Preview tab or a terminal</li>
              <li>Checking the port it listens on</li>
            </ul>
          </div>
        ) : null}

        <div className="mt-8 text-xs uppercase tracking-wide text-faint">{errorLabel}</div>

        <div className="mt-auto flex items-center gap-2 pt-8">
          <Button size="sm" variant="quiet" onClick={() => setShowDetails((value) => !value)}>
            {showDetails ? "Hide details" : "Details"}
          </Button>
          <div className="flex-1" />
          {onOpenInBrowser ? (
            <Button size="sm" onClick={onOpenInBrowser}>
              Open in browser
            </Button>
          ) : null}
          <Button size="sm" variant="primary" onClick={onReload}>
            Reload
          </Button>
        </div>
      </div>
    </div>
  );
}

function ErrorIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 64 64"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.5"
      className={className}
      aria-hidden
    >
      <path d="M16 12 L48 12 L48 52 L16 52 Z" />
      <path d="M22 22 L42 22 M22 30 L36 30 M22 38 L40 38" strokeLinecap="round" />
      <path d="M52 8 L12 56" strokeLinecap="round" />
    </svg>
  );
}

function safeHost(url: string): string | null {
  try {
    return new URL(url).host;
  } catch {
    return null;
  }
}
