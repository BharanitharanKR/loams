// The desktop's agentd transport (plan SD1 Tasks 5–6, §53 §6.2): the renderer
// calls `loams-app://console/loams.agentd.v1.<Service>/<Method>`, and Electron
// main's protocol route forwards it to the daemon's loopback Connect listener,
// adding the token only main holds and refusing any method that is not on the
// renderer allowlist. The renderer never sees the token or the daemon's port.

import type { Transport } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';

/** The console's origin in Electron; Task 5's route answers `/loams.agentd.v1.` on it. */
export const DESKTOP_AGENTD_BASE_URL = 'loams-app://console/';

export interface DesktopTransportOptions {
  kind: 'desktop';
  /** Defaults to `DESKTOP_AGENTD_BASE_URL`; tests point it elsewhere. */
  baseUrl?: string;
  /** Defaults to the page's `fetch`, which Electron serves for `loams-app:`. */
  fetch?: typeof globalThis.fetch;
}

export function createDesktopTransport(options: DesktopTransportOptions): Transport {
  return createConnectTransport({
    baseUrl: options.baseUrl ?? DESKTOP_AGENTD_BASE_URL,
    useBinaryFormat: true,
    // Late-bound, so a test's stub (or Electron's) is the one called.
    fetch: options.fetch ?? ((input, init) => globalThis.fetch(input, init)),
  });
}
