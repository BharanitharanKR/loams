// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/preview/PreviewLocalServerCard.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the data layer and styling are ported to agentd's WatchPreviews and @loams/ui tokens (SD1 Task 26). Loams' changes are Apache-2.0.
import type { ScopedThreadRef } from "@t3tools/contracts";
import { DiscoveryListRow } from "../ui/discovery-list";

import { PreviewFaviconIcon } from "./PreviewFaviconIcon";
import type { PreviewableServer } from "./useDiscoveredLocalServers";

interface Props {
  threadRef: ScopedThreadRef;
  server: PreviewableServer;
  onOpen: () => void;
}

export function PreviewLocalServerCard({ threadRef, server, onOpen }: Props) {
  const subtitle = describeServer(server);
  return (
    <DiscoveryListRow
      onClick={onOpen}
      icon={<PreviewFaviconIcon threadRef={threadRef} url={server.requestedUrl} />}
      title={subtitle}
      description={`${server.host}:${server.port}`}
    />
  );
}

function describeServer(server: PreviewableServer): string {
  if (server.processName) return server.processName;
  return "Listening";
}
