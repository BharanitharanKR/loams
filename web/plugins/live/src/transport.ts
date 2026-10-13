import type { Transport } from '@connectrpc/connect';

/**
 * The one place that decides how the Live page reaches `loams.live.v1`.
 *
 * The engine serves `LiveService` and `LiveAdminService` on its main port, behind
 * its authentication (LV1 Task 12), so a browser console's transport to the engine
 * reaches both. In the desktop the protocol proxy routes the `/loams.live.v1.`
 * prefix to the engine's Live listener, which serves the same two services. Either
 * way it is the console's own Connect transport.
 */
export function liveTransport(deps: { transport: Transport }): Transport {
  return deps.transport;
}
