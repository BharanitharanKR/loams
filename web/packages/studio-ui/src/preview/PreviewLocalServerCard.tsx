// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/preview/PreviewLocalServerCard.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// a plain button row on @loams/ui tokens in place of T3's DiscoveryListRow; a lucide glyph in place of the page's favicon (no remote images, §53 D919). Loams' changes are Apache-2.0.
import { RadioTower } from "lucide-react";

import type { PreviewableServer } from "./discoveredServers";

interface Props {
  server: PreviewableServer;
  onOpen: () => void;
}

export function PreviewLocalServerCard({ server, onOpen }: Props) {
  const subtitle = describeServer(server);
  return (
    <li>
      <button
        type="button"
        onClick={onOpen}
        className="flex w-full items-center gap-3 border border-rule-soft bg-bg px-3 py-2 text-left text-ink transition-colors duration-150 ease-op hover:border-rule hover:bg-surface focus-visible:outline-2 focus-visible:outline-accent"
        data-preview-server={server.previewName}
      >
        <RadioTower aria-hidden className="size-4 shrink-0 text-accent-fg" />
        <span className="flex min-w-0 flex-1 flex-col">
          <span className="truncate text-sm font-medium">{subtitle}</span>
          <span className="truncate font-mono text-xs text-muted">{server.hostname}</span>
        </span>
        <span className="shrink-0 font-mono text-xs text-faint">:{server.port}</span>
      </button>
    </li>
  );
}

function describeServer(server: PreviewableServer): string {
  if (server.name)
    return server.projectName ? `${server.name} · ${server.projectName}` : server.name;
  return "Listening";
}
