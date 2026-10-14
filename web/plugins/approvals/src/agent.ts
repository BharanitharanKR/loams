// The agent source of the approvals inbox (design §53 §4.5, §8.1; plan SD1 Task 13):
// the tool calls a supervised loams-agentd thread is waiting on, listed beside the
// server's approvals and labelled as the agent's. They come from agentd's
// `ApprovalService.WatchApprovals` (G17) over the console's agentd transport, and are
// answered with `ResolveApproval` (G18) by the person, never by an agent.

import { Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';
import type { AgentdService } from '@loams/console-host';
import {
  type Approval as AgentApproval,
  ApprovalService as AgentApprovalService,
  ApprovalDecision as AgentDecision,
  type WatchApprovalsResponse,
} from '@loams/proto/agentd';

export type { AgentApproval };
export { AgentDecision };

export interface AgentInboxState {
  /** Oldest first. */
  approvals: AgentApproval[];
  /** The daemon is reachable and serves approvals. */
  available: boolean;
  /** Set while the stream is down; the list is then possibly stale. */
  error?: string;
}

const EMPTY: AgentInboxState = { approvals: [], available: false };

/** The agent approvals, a `useSyncExternalStore` source. */
export class AgentInbox {
  #state: AgentInboxState = EMPTY;
  #listeners = new Set<() => void>();

  readonly getSnapshot = (): AgentInboxState => this.#state;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  /** Each `WatchApprovals` item is the whole pending list. */
  apply(response: WatchApprovalsResponse): void {
    const approvals = [...response.pending].sort((a, b) =>
      a.createdAtMs < b.createdAtMs ? -1 : a.createdAtMs > b.createdAtMs ? 1 : 0,
    );
    this.#set({ approvals, available: true });
  }

  /** The daemon has no approvals (`unimplemented`), or no daemon is reachable. */
  clear(): void {
    this.#set(EMPTY);
  }

  failed(error: unknown): void {
    this.#set({ ...this.#state, error: String(error) });
  }

  /** Drops an answered approval before the stream reports it. */
  remove(approval: AgentApproval): void {
    this.#set({
      ...this.#state,
      approvals: this.#state.approvals.filter(
        (a) => !(a.id === approval.id && a.chatId === approval.chatId),
      ),
    });
  }

  #set(state: AgentInboxState): void {
    this.#state = state;
    for (const listener of this.#listeners) listener();
  }
}

const sleep = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });

/** Watch one daemon's approvals until `signal` aborts, reopening with a backoff. */
async function watchDaemon(transport: Transport, inbox: AgentInbox, signal: AbortSignal) {
  const client = createClient(AgentApprovalService, transport);
  let delay = 500;
  while (!signal.aborted) {
    try {
      for await (const response of client.watchApprovals({}, { signal })) {
        inbox.apply(response);
        delay = 500;
      }
    } catch (error) {
      if (signal.aborted) return;
      // A daemon from before SD1 Task 13: no agent approvals, and no retry.
      if (error instanceof ConnectError && error.code === Code.Unimplemented) {
        inbox.clear();
        return;
      }
      inbox.failed(error);
    }
    await sleep(delay, signal);
    delay = Math.min(delay * 2, 15_000);
  }
}

/**
 * Follow the console's agentd: watch its approvals while a daemon is reachable, and
 * clear them when it is not. Returns the disposer.
 */
export function followAgentApprovals(agentd: AgentdService, inbox: AgentInbox): () => void {
  let current: Transport | undefined;
  let controller: AbortController | undefined;
  const sync = () => {
    const transport = agentd.reach().state === 'ready' ? agentd.transport() : undefined;
    if (transport === current) return;
    current = transport;
    controller?.abort();
    controller = undefined;
    if (!transport) {
      inbox.clear();
      return;
    }
    controller = new AbortController();
    void watchDaemon(transport, inbox, controller.signal);
  };
  sync();
  const off = agentd.subscribe(sync);
  return () => {
    off();
    controller?.abort();
  };
}

/** Answer an agent approval on the daemon behind `agentd`. */
export async function resolveAgentApproval(
  agentd: AgentdService,
  approval: AgentApproval,
  decision: AgentDecision,
): Promise<void> {
  const transport = agentd.transport();
  if (!transport) throw new Error('No loams-agentd is reachable');
  await createClient(AgentApprovalService, transport).resolveApproval({
    callId: approval.id,
    chatId: approval.chatId,
    decision,
  });
}
