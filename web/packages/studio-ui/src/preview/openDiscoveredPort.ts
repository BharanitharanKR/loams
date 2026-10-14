// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/preview/openDiscoveredPort.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the environment's browser-target resolution became a choice between the desktop's loopback URL and the remote listener's /preview/ path (Q806); no preview session RPC, history store or right-panel store. Loams' changes are Apache-2.0.
import type { PreviewableServer } from "./discoveredServers";

/** How Studio can show a preview where it runs (§53 §5.8, §7). */
export type PreviewReach =
  /** Desktop: the loopback URL in a sandboxed `WebContentsView` placed by main (D678). */
  | { kind: "loopback" }
  /** Web: through the remote agentd's `/preview/…`, with a ticket for the frame (Q806). */
  | { kind: "remote"; open: (previewName: string) => Promise<RemotePreview> }
  /** Web without Q806: the port has to be forwarded by hand. */
  | { kind: "forward"; sshHost?: string };

/** A remote preview's frame URL and when its ticket ends. */
export interface RemotePreview {
  url: string;
  expiresAtMs: number;
}

/** Where a discovered server is opened. */
export type PreviewTarget =
  | { kind: "loopback"; url: string }
  | { kind: "remote"; url: string; expiresAtMs: number }
  | { kind: "forward"; command: string; url: string };

/** The `ssh -L` command that brings the daemon's preview proxy to this machine. */
export function forwardCommand(proxyPort: number, sshHost = "<agentd host>"): string {
  return `ssh -N -L ${proxyPort}:localhost:${proxyPort} ${sshHost}`;
}

/** Resolves `server` for `reach`. */
export async function openDiscoveredPort(input: {
  readonly server: PreviewableServer;
  readonly reach: PreviewReach;
  readonly proxyPort: number;
}): Promise<PreviewTarget> {
  const { server, reach } = input;
  switch (reach.kind) {
    case "loopback":
      return { kind: "loopback", url: server.url };
    case "remote": {
      const opened = await reach.open(server.previewName);
      return { kind: "remote", url: opened.url, expiresAtMs: opened.expiresAtMs };
    }
    case "forward":
      return {
        kind: "forward",
        url: server.url,
        command: forwardCommand(input.proxyPort, reach.sshHost),
      };
  }
}
