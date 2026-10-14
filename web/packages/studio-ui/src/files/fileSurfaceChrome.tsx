// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/fileSurfaceChrome.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to @loams/ui (Button, Toggle, Tooltip, Spinner) and the Loams tokens; no editor reveal CSS. Loams' changes are Apache-2.0.
import { Button, cx, Spinner, Toggle, Tooltip } from "@loams/ui";
import type { ReactNode } from "react";

/**
 * One header row for every file surface in the side panel: crumbs on the left,
 * icon-only actions on the right.
 */
export const FILE_SURFACE_SUBHEADER_CLASS =
  "flex h-10 min-h-10 shrink-0 items-center gap-1 border-b border-rule-soft bg-bg px-2";

/**
 * An icon-only header action with its label in a tooltip. A `pressed` action is a toggle and
 * says so; a command is a plain button, because announcing it as an unpressed toggle tells a
 * screen reader it has a state it has not.
 */
export function FileSurfaceAction(props: {
  readonly label: string;
  readonly pressed?: boolean;
  readonly disabled?: boolean;
  readonly onPress: () => void;
  readonly children: ReactNode;
}) {
  const pressed = props.pressed;
  return (
    <Tooltip content={props.label}>
      {pressed === undefined ? (
        <Button
          variant="quiet"
          size="icon"
          className="shrink-0"
          disabled={props.disabled ?? false}
          onClick={props.onPress}
          aria-label={props.label}
        >
          {props.children}
        </Button>
      ) : (
        <Toggle
          size="sm"
          className="shrink-0"
          pressed={pressed}
          disabled={props.disabled ?? false}
          onPressedChange={props.onPress}
          aria-label={props.label}
        >
          {props.children}
        </Toggle>
      )}
    </Tooltip>
  );
}

export function FileSurfaceNotice(props: { readonly children: ReactNode }) {
  return (
    <div
      role="status"
      className="shrink-0 border-b border-rule-soft bg-accent-soft px-3 py-1.5 text-[0.75rem] text-ink"
    >
      {props.children}
    </div>
  );
}

export function FileSurfaceLoading(props: {
  readonly className?: string;
  readonly label?: string;
}) {
  return (
    <div
      className={cx("flex min-h-0 flex-1 items-center justify-center text-muted", props.className)}
    >
      <Spinner size="lg" label={props.label ?? "Loading file"} />
    </div>
  );
}

export function FileSurfaceFailure(props: {
  readonly message: string;
  readonly onRetry?: () => void;
}) {
  return (
    <div
      role="alert"
      className="flex min-h-0 flex-1 flex-col items-center justify-center gap-3 px-6 text-center text-[0.8rem] leading-relaxed"
    >
      <p className="text-danger">{props.message}</p>
      {props.onRetry ? (
        <Button size="sm" onClick={props.onRetry}>
          Try again
        </Button>
      ) : null}
    </div>
  );
}
