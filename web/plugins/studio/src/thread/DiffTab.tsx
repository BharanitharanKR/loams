// The thread panel's Diff tab (SD1 Task 20; §53 §5.6, Shared contracts `?panel=diff`): the
// thread's checkout diff with review comments, from the daemon over the agentd transport of
// the nearest `AgentdScope`. The thread's cwd and checkout come from `WatchThreads`.

import { createClient } from '@connectrpc/connect';
import { useAgentdTransport } from '@loams/console-host/query';
import { ThreadService } from '@loams/proto/agentd';
import { DiffPanel } from '@loams/studio-ui/diff';
import { Empty, Spinner } from '@loams/ui';
import { useMemo } from 'react';
import { useWatch } from '../sidebar/data.js';
import { agentdDiffSource, type DiffThread } from './diff-source.js';

export function DiffTab({
  threadId,
  onOpenFile,
}: {
  threadId: string;
  /** Opens a file of the diff in the Files tab. */
  onOpenFile?: (path: string) => void;
}) {
  const transport = useAgentdTransport();
  const threads = useWatch(
    (t, signal) => createClient(ThreadService, t).watchThreads({}, { signal }),
    'studio-diff-threads',
  );
  const chat = threads.data?.chats.find((c) => c.id === threadId);
  const cwd = chat?.cwd ?? chat?.sourceContext?.cwd;
  const checkoutId = chat?.checkoutId ?? chat?.sourceContext?.checkoutId;
  const branch = chat?.branch ?? chat?.sourceContext?.branch;
  const thread = useMemo<DiffThread | null>(
    () =>
      cwd
        ? {
            chatId: threadId,
            cwd,
            ...(checkoutId ? { checkoutId } : {}),
            ...(branch ? { branch } : {}),
          }
        : null,
    [threadId, cwd, checkoutId, branch],
  );
  const source = useMemo(
    () => (transport && thread ? agentdDiffSource(transport, thread) : undefined),
    [transport, thread],
  );

  if (!transport) {
    return (
      <div className="p-3">
        <Empty title="No diff to show" seed={2}>
          The daemon is not reachable, so this thread's changes cannot be read.
        </Empty>
      </div>
    );
  }
  if (!source) {
    if (threads.data || threads.unsupported) {
      return (
        <div className="p-3">
          <Empty title="No checkout" seed={5}>
            This thread has no working directory yet, so it has no diff.
          </Empty>
        </div>
      );
    }
    if (threads.error) {
      return (
        <div className="p-3">
          <Empty title="The thread could not be read" seed={6}>
            {threads.error.message}
          </Empty>
        </div>
      );
    }
    return (
      <div className="flex flex-1 items-center justify-center p-6">
        <Spinner tone="muted" label="Loading the thread" />
      </div>
    );
  }
  return (
    <div className="-mx-3 flex min-h-0 flex-1 flex-col border-t border-rule-soft" data-diff-tab>
      <DiffPanel
        key={source.key}
        threadId={threadId}
        source={source}
        {...(onOpenFile ? { onOpenFile } : {})}
      />
    </div>
  );
}
