// loams-agentd's clients (plan SD1 Task 6, §53 §6.2, D907): `agentd` always, and
// `agentd.transport` plus one `rpc.agentd.<service>` per `loams.agentd.v1` service
// while a daemon is reachable.
//
// The platform says how to reach a daemon through `agentd.endpoint`: on the desktop,
// Electron main's protocol route and the daemon's status (@loams/platform-electron);
// in the browser nothing yet, so `agentd` reports "no host" until SD1 Task 7's host
// registry provides an endpoint. Who may inject these, and the `agentd:git-write`
// gate, are the host's (console-host `agentd.ts`, the guard and boot).

import { createClient } from '@connectrpc/connect';
import { AGENTD_SERVICES, type AgentdService, noAgentdHost } from '@loams/console-host';
import type { Context } from '@loams/cordis';

export { AGENTD_SERVICES };

/** Provides `agentd`, and the clients while the daemon is reachable. */
export function provideAgentd(ctx: Context): void {
  // Read, not injected: a platform without an agentd (the web, today) provides none.
  const endpoint = ctx.get('agentd.endpoint', true) as AgentdService | undefined;
  const hub = endpoint ?? noAgentdHost();
  ctx.provide('agentd', hub);

  let released: (() => void)[] = [];
  let current: unknown;
  const sync = () => {
    const transport = hub.reach().state === 'ready' ? hub.transport() : undefined;
    if (transport === current) return;
    for (const release of released) release();
    released = [];
    current = transport;
    if (!transport) return;
    // Re-providing restarts the plugins that inject these, on the new transport.
    released.push(ctx.provide('agentd.transport', transport) as () => void);
    for (const entry of AGENTD_SERVICES) {
      released.push(ctx.provide(entry.name, createClient(entry.service, transport)) as () => void);
    }
  };
  sync();
  ctx.effect(() => hub.subscribe(sync), 'agentd reach');
  ctx.effect(
    () => () => {
      for (const release of released) release();
      released = [];
    },
    'agentd clients',
  );
}
