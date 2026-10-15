import type { Transport } from '@connectrpc/connect';
import { TransportProvider } from '@connectrpc/connect-query';
import { Slot, SlotProvider, type SlotRegistry } from '@loams/slots';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { type ReactNode, StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { AgentdTransportProvider } from './agentd-query.js';
import type { ConsoleHandle } from './boot.js';

function Booting({ handle }: { handle: () => ConsoleHandle | undefined }) {
  const failed = handle()
    ?.plugins()
    .filter((p) => p.status === 'failed');
  return (
    <main className="loams-boot" aria-busy="true">
      <p>Starting the Loams console…</p>
      {failed && failed.length > 0 && (
        <ul>
          {failed.map((p) => (
            <li key={p.id}>
              {p.id}: {p.reason}
            </li>
          ))}
        </ul>
      )}
    </main>
  );
}

/** The console's one query cache: server state is read once and shared between pages. */
export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: { staleTime: 15_000, retry: 1, refetchOnWindowFocus: false },
    },
  });
}

/**
 * Server state for every plugin: TanStack Query, and Connect Query over the
 * console's transport (docs/design/console-ui-stack.md, decision 4). A page reads
 * an RPC with `useQuery(method, input)` from `@connectrpc/connect-query`.
 *
 * A second transport scope serves loams-agentd (SD1 Task 6): `useAgentdQuery`
 * reads the daemon over it, through the same `QueryClient`. A plugin that has the
 * `agentd` service usually scopes its own pages with `AgentdScope` instead.
 */
export function ServerStateProvider({
  client,
  transport,
  agentd,
  children,
}: {
  client: QueryClient;
  transport?: Transport;
  /** The agentd transport, when the tree should have one. */
  agentd?: Transport;
  children: ReactNode;
}) {
  let inner = agentd ? (
    <AgentdTransportProvider transport={agentd}>{children}</AgentdTransportProvider>
  ) : (
    children
  );
  if (transport) inner = <TransportProvider transport={transport}>{inner}</TransportProvider>;
  return <QueryClientProvider client={client}>{inner}</QueryClientProvider>;
}

/** Renders the `root` slot; until the shell registers it, a boot screen. */
export function renderRoot(
  element: HTMLElement,
  slots: SlotRegistry,
  handle: () => ConsoleHandle | undefined,
  transport?: Transport,
): { unmount(): void } {
  const root = createRoot(element);
  root.render(
    <StrictMode>
      <ServerStateProvider client={createQueryClient()} transport={transport}>
        <SlotProvider registry={slots}>
          <Slot name="root" props={{}} fallback={<Booting handle={handle} />} />
        </SlotProvider>
      </ServerStateProvider>
    </StrictMode>,
  );
  return root;
}
