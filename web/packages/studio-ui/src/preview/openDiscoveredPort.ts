// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/preview/openDiscoveredPort.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the data layer and styling are ported to agentd's WatchPreviews and @loams/ui tokens (SD1 Task 26). Loams' changes are Apache-2.0.
import type { DiscoveredLocalServer, ScopedThreadRef } from "@t3tools/contracts";
import {
  mapAtomCommandResult,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";

import { resolveDiscoveredServerUrl } from "~/browser/browserTargetResolver";
import type { BrowserSettingsReadError, OpenPreviewMutation } from "~/browser/openFileInPreview";
import { recordVisitForThread } from "~/browserHistoryStore";
import { useRightPanelStore } from "~/rightPanelStore";
import { openPreviewSession } from "./openPreviewSession";
import { previewRuntimeFor } from "~/browser/previewRuntime";

export async function openDiscoveredPort<E>(input: {
  readonly threadRef: ScopedThreadRef;
  readonly port: DiscoveredLocalServer;
  readonly openPreview: OpenPreviewMutation<E>;
}): Promise<AtomCommandResult<void, E | BrowserSettingsReadError>> {
  // A server tab runs on the environment, where loopback is already right.
  const resolvedUrl =
    previewRuntimeFor(input.threadRef.environmentId) === "server"
      ? input.port.url
      : resolveDiscoveredServerUrl(input.threadRef.environmentId, input.port.url);
  const result = await openPreviewSession({
    openPreview: input.openPreview,
    threadRef: input.threadRef,
    url: resolvedUrl,
  });
  return mapAtomCommandResult(result, (snapshot) => {
    recordVisitForThread(input.threadRef, input.port.url);
    useRightPanelStore.getState().openBrowser(input.threadRef, snapshot.tabId);
  });
}
