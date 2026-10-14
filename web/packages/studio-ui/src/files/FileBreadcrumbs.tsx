// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/FileBreadcrumbs.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// a folder crumb reveals that folder in the tree instead of opening a folder menu; @loams/ui tooltips and tokens; no host paths. Loams' changes are Apache-2.0.
import { cx, Tooltip } from "@loams/ui";
import { ChevronRightIcon } from "lucide-react";
import { useMemo } from "react";

import { fileBreadcrumbs } from "./filePath";

interface FileBreadcrumbsProps {
  readonly projectName: string;
  readonly relativePath: string;
  /** Shows a folder (or the project, "") in the tree. */
  readonly onRevealDirectory: (path: string) => void;
}

const CRUMB = "block max-w-40 truncate rounded-sm px-0.5";

export function FileBreadcrumbs(props: FileBreadcrumbsProps) {
  const breadcrumbs = useMemo(
    () => fileBreadcrumbs(props.projectName, props.relativePath),
    [props.projectName, props.relativePath],
  );

  return (
    <nav aria-label="File path" className="flex min-w-0 items-center text-[0.78rem]">
      {breadcrumbs.map((crumb, index) => (
        <div
          key={crumb.path || "project"}
          className="flex min-w-0 shrink-0 items-center"
          data-current-file-crumb={crumb.kind === "file"}
        >
          {index > 0 ? (
            <ChevronRightIcon aria-hidden className="mx-1 size-3.5 shrink-0 text-faint" />
          ) : null}
          {crumb.kind === "file" ? (
            <span aria-current="page" className={cx(CRUMB, "font-medium text-ink")}>
              {crumb.label}
            </span>
          ) : (
            <Tooltip content={crumb.path || props.projectName}>
              <button
                type="button"
                aria-label={`Show ${crumb.label} in the tree`}
                onClick={() => props.onRevealDirectory(crumb.path)}
                className={cx(
                  CRUMB,
                  "cursor-pointer border-0 bg-transparent font-[inherit] text-left text-muted outline-none hover:bg-raised hover:text-ink focus-visible:outline-2 focus-visible:outline-accent",
                )}
              >
                {crumb.label}
              </button>
            </Tooltip>
          )}
        </div>
      ))}
    </nav>
  );
}
