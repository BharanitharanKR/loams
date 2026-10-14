// Server state from loams-agentd (plan SD1 Task 6; console-ui-stack decision 4).
// The console's one QueryClient serves both the Loams server and the daemon: an
// agentd page reads an RPC with `useAgentdQuery(ThreadService.method.watchThreads…)`,
// which is connect-query's `useQuery` over the agentd transport of the nearest
// `AgentdTransportProvider` (or `AgentdScope`). connect-query keys every query by
// its transport too, so a daemon's answers never mix with the server's, or with
// another host's.

import type {
  DescMessage,
  DescMethodUnary,
  MessageInitShape,
  MessageShape,
} from '@bufbuild/protobuf';
import { Code, ConnectError, type Transport } from '@connectrpc/connect';
import {
  type SkipToken,
  skipToken,
  type UseMutationOptions,
  type UseQueryOptions,
  useMutation,
  useQuery,
} from '@connectrpc/connect-query';
import { createContext, type ReactNode, useContext, useSyncExternalStore } from 'react';
import type { AgentdService } from './agentd.js';

const AgentdTransportContext = createContext<Transport | undefined>(undefined);

/** The agentd transport for the tree below; `undefined` while no daemon is reachable. */
export function AgentdTransportProvider({
  transport,
  children,
}: {
  transport: Transport | undefined;
  children: ReactNode;
}) {
  return (
    <AgentdTransportContext.Provider value={transport}>{children}</AgentdTransportContext.Provider>
  );
}

/** Follows `agentd`'s transport as the daemon comes and goes. */
export function AgentdScope({ agentd, children }: { agentd: AgentdService; children: ReactNode }) {
  const transport = useSyncExternalStore(
    (listener) => agentd.subscribe(listener),
    () => agentd.transport(),
  );
  return <AgentdTransportProvider transport={transport}>{children}</AgentdTransportProvider>;
}

export function useAgentdTransport(): Transport | undefined {
  return useContext(AgentdTransportContext);
}

const unreachable = () =>
  Promise.reject(new ConnectError('no loams-agentd is reachable', Code.Unavailable));

/** Answers every call `unavailable`: what a hook uses while there is no daemon. */
export const NO_AGENTD: Transport = { unary: unreachable, stream: unreachable };

/**
 * `useQuery` against the daemon. While no daemon is reachable the query is
 * skipped (pending, not failed), and it runs once one is.
 */
export function useAgentdQuery<
  I extends DescMessage,
  O extends DescMessage,
  SelectOutData = MessageShape<O>,
>(
  schema: DescMethodUnary<I, O>,
  input?: SkipToken | MessageInitShape<I>,
  options: Omit<UseQueryOptions<O, SelectOutData>, 'transport'> = {},
) {
  const transport = useAgentdTransport();
  return useQuery(schema, transport ? input : skipToken, {
    ...options,
    transport: transport ?? NO_AGENTD,
  });
}

/** `useMutation` against the daemon; it fails `unavailable` while there is none. */
export function useAgentdMutation<I extends DescMessage, O extends DescMessage, Ctx = unknown>(
  schema: DescMethodUnary<I, O>,
  options: Omit<UseMutationOptions<I, O, Ctx>, 'transport'> = {},
) {
  const transport = useAgentdTransport();
  return useMutation(schema, { ...options, transport: transport ?? NO_AGENTD });
}
