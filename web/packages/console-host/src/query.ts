// @loams/console-host/query: server state for plugins' React code
// (console-ui-stack decision 4): the console's query cache and the agentd hooks
// (SD1 Task 6). A separate entry, so the host's boot path loads React lazily.

export {
  AgentdScope,
  AgentdTransportProvider,
  NO_AGENTD,
  useAgentdMutation,
  useAgentdQuery,
  useAgentdTransport,
} from './agentd-query.js';
export { createQueryClient, ServerStateProvider } from './root.js';
