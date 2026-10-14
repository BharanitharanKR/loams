// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/preview/PreviewEmptyState.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the servers come in as props (loams-agentd's WatchPreviews) rather than from the port-scanner hook; the recent-URL list is dropped; a "start the dev server" action is added; @loams/ui's Empty and tokens. Loams' changes are Apache-2.0.
import { Button, Empty } from "@loams/ui";
import { Play, RadioTower } from "lucide-react";
import type { ReactNode } from "react";

import type { PreviewableServer } from "./discoveredServers";
import { PreviewLocalServerCard } from "./PreviewLocalServerCard";

/** A project action that starts a dev server (agentd's `RunProjectAction`). */
export interface PreviewStartAction {
  id: string;
  name: string;
}

interface Props {
  servers: ReadonlyArray<PreviewableServer>;
  onOpenServer: (server: PreviewableServer) => void;
  /** The project's actions that can start a dev server; absent or empty hides the button. */
  startActions?: ReadonlyArray<PreviewStartAction>;
  onStart?: (action: PreviewStartAction) => void;
  /** An action is starting. */
  starting?: boolean;
  /** Shown under the list, such as the web's port-forward hint. */
  footer?: ReactNode;
}

export function PreviewEmptyState({
  servers,
  onOpenServer,
  startActions,
  onStart,
  starting,
  footer,
}: Props) {
  const actions = onStart ? (startActions ?? []) : [];
  const start =
    actions.length > 0 ? (
      <div className="flex flex-wrap gap-2">
        {actions.map((action) => (
          <Button
            key={action.id}
            size="sm"
            variant={servers.length === 0 ? "primary" : "secondary"}
            disabled={starting}
            onClick={() => onStart?.(action)}
          >
            <Play aria-hidden className="size-3.5" /> {action.name}
          </Button>
        ))}
      </div>
    ) : undefined;

  if (servers.length === 0) {
    return (
      <div className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto py-4" data-preview-empty>
        <Empty title="No preview yet" seed={26} actions={start}>
          Type a localhost URL above, or run the project&rsquo;s dev server. Servers the daemon
          finds under this project show up here.
        </Empty>
        {footer}
      </div>
    );
  }

  return (
    <div className="flex min-h-0 flex-1 overflow-y-auto py-4" data-preview-servers>
      <div className="mx-auto flex w-full max-w-xl flex-col gap-3">
        <div className="flex items-center gap-2 text-sm text-muted">
          <RadioTower aria-hidden className="size-4 shrink-0" />
          <h2 className="m-0 text-sm font-medium">Local servers</h2>
        </div>
        <ul className="m-0 flex list-none flex-col gap-1.5 p-0">
          {servers.map((server) => (
            <PreviewLocalServerCard
              key={server.id}
              server={server}
              onOpen={() => onOpenServer(server)}
            />
          ))}
        </ul>
        <p className="m-0 px-1 text-xs text-muted">
          Select a running server to open it in this preview.
        </p>
        {start}
        {footer}
      </div>
    </div>
  );
}
