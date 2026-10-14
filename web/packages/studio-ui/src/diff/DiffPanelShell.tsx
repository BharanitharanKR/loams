// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/DiffPanelShell.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// one mode (the thread panel's tab), no Electron drag region; @loams/ui's Skeleton and tokens. Loams' changes are Apache-2.0.

// The diff panel's chrome (SD1 Task 20): a toolbar row over the body, and the loading
// state that stands in for the first diff.

import { cx, Skeleton } from "@loams/ui";
import type { ReactNode } from "react";

export function DiffPanelShell(props: {
  header: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={cx("flex h-full min-h-0 min-w-0 flex-col bg-bg", props.className)}>
      <div
        className="flex min-h-9 shrink-0 flex-wrap items-center justify-between gap-1.5 border-b border-rule-soft px-2 py-1"
        data-diff-toolbar
      >
        {props.header}
      </div>
      {props.children}
    </div>
  );
}

export function DiffFileHeaderSkeleton({
  titleWidth,
}: {
  titleWidth: "short" | "medium" | "long";
}) {
  return (
    <div className="flex h-8 items-center gap-2 px-2 pr-3">
      <Skeleton className="size-4 shrink-0" />
      <Skeleton
        className={
          titleWidth === "short"
            ? "h-3 w-2/5 max-w-52"
            : titleWidth === "medium"
              ? "h-3 w-1/2 max-w-64"
              : "h-3 w-3/5 max-w-72"
        }
      />
      <div className="ml-auto flex shrink-0 items-center gap-2">
        <Skeleton className="h-3 w-5" />
        <Skeleton className="h-3 w-5" />
      </div>
    </div>
  );
}

function DiffCodeLineSkeleton({ width }: { width: "short" | "medium" | "long" }) {
  return (
    <div className="flex items-center gap-3">
      <Skeleton className="h-2.5 w-5 shrink-0" />
      <Skeleton
        className={
          width === "short" ? "h-2.5 w-3/5" : width === "medium" ? "h-2.5 w-2/3" : "h-2.5 w-4/5"
        }
      />
    </div>
  );
}

export function DiffPanelLoadingState(props: { label: string }) {
  return (
    <div
      className="min-h-0 flex-1 overflow-hidden bg-bg"
      role="status"
      aria-live="polite"
      aria-label={props.label}
    >
      <DiffFileHeaderSkeleton titleWidth="medium" />
      <div className="space-y-2 px-3 py-2">
        <DiffCodeLineSkeleton width="medium" />
        <DiffCodeLineSkeleton width="long" />
        <DiffCodeLineSkeleton width="short" />
      </div>
      <DiffFileHeaderSkeleton titleWidth="short" />
      <DiffFileHeaderSkeleton titleWidth="long" />
      <span className="sr-only">{props.label}</span>
    </div>
  );
}
