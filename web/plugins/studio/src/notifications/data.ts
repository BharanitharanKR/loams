// Studio's notifications (SD1 Task 14; §53 §6.5 G19): `NotificationService.WatchNotifications`
// over the nearest `AgentdScope` transport. The daemon first sends the last 200 it holds,
// oldest first, then each new one; a reconnect sends the buffer again, so notifications are
// kept by id. A daemon that does not serve the method yet (`unimplemented`) hides the
// surface, as the sidebar's watches do (T17-5).
//
// Unread means "arrived since this page opened Studio": the daemon keeps no read state, and
// the buffer is replayed on every connect. OS notifications are not posted from here: on
// the desktop Electron main follows the same stream for them (SD1 Task 28).

import { Code, ConnectError, createClient } from '@connectrpc/connect';
import { useAgentdTransport } from '@loams/console-host/query';
import { type Notification, NotificationKind, NotificationService } from '@loams/proto/agentd';
import { useCallback, useEffect, useState } from 'react';
import { studioHref } from '../routes.js';

/** As many as the daemon keeps. */
export const MAX_NOTIFICATIONS = 200;
const RETRY_MS = [1000, 2000, 5000, 15000, 30000];

/** One notification, as the surface shows it. */
export interface StudioNotification {
  id: string;
  kind: NotificationKind;
  title: string;
  createdAtMs: number;
  chatId?: string;
  groupId?: string;
  /** Where it leads: the thread or the run group. */
  href?: string;
}

export function toStudioNotification(n: Notification): StudioNotification {
  const href = n.chatId
    ? studioHref({ kind: 'thread', threadId: n.chatId })
    : n.groupId
      ? studioHref({ kind: 'group', groupId: n.groupId })
      : undefined;
  return {
    id: n.id,
    kind: n.kind,
    title: n.title,
    createdAtMs: Number(n.createdAtMs),
    ...(n.chatId ? { chatId: n.chatId } : {}),
    ...(n.groupId ? { groupId: n.groupId } : {}),
    ...(href ? { href } : {}),
  };
}

export const KIND_LABEL: Record<NotificationKind, string> = {
  [NotificationKind.UNSPECIFIED]: 'Notification',
  [NotificationKind.TURN_DONE]: 'Turn finished',
  [NotificationKind.APPROVAL]: 'Approval waiting',
  [NotificationKind.QUESTION]: 'Question waiting',
  [NotificationKind.ERROR]: 'Error',
  [NotificationKind.GROUP_READY]: 'Run group ready',
  [NotificationKind.CHECK_FAILED]: 'Check failed',
};

/** Newest first (the later arrival first on a tie), one per id, at most `MAX_NOTIFICATIONS`. */
export function addNotification(
  list: readonly StudioNotification[],
  n: StudioNotification,
): StudioNotification[] {
  if (list.some((x) => x.id === n.id)) return list as StudioNotification[];
  const next = [n, ...list].sort((a, b) => b.createdAtMs - a.createdAtMs);
  return next.slice(0, MAX_NOTIFICATIONS);
}

export interface NotificationsState {
  items: StudioNotification[];
  /** Arrived since the page opened and not yet looked at. */
  unread: number;
  /** The daemon does not serve notifications. */
  unsupported: boolean;
  markAllRead(): void;
}

/** Follows the daemon's notifications. */
export function useNotifications(): NotificationsState {
  const transport = useAgentdTransport();
  const [items, setItems] = useState<StudioNotification[]>([]);
  const [unsupported, setUnsupported] = useState(false);
  const [readUntil, setReadUntil] = useState(() => Date.now());

  useEffect(() => {
    if (!transport) return;
    const abort = new AbortController();
    const client = createClient(NotificationService, transport);
    let attempt = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const run = async () => {
      try {
        for await (const frame of client.watchNotifications({}, { signal: abort.signal })) {
          attempt = 0;
          if (!frame.notification) continue;
          const n = toStudioNotification(frame.notification);
          setUnsupported(false);
          setItems((list) => addNotification(list, n));
        }
      } catch (e) {
        if (abort.signal.aborted) return;
        const err = ConnectError.from(e);
        if (err.code === Code.Canceled) return;
        if (err.code === Code.Unimplemented) {
          setUnsupported(true);
          return;
        }
      }
      if (abort.signal.aborted) return;
      const wait = RETRY_MS[Math.min(attempt, RETRY_MS.length - 1)] ?? 30000;
      attempt += 1;
      timer = setTimeout(run, wait);
    };
    void run();
    return () => {
      abort.abort();
      if (timer) clearTimeout(timer);
    };
  }, [transport]);

  const markAllRead = useCallback(() => setReadUntil(Date.now()), []);
  const unread = items.filter((n) => n.createdAtMs > readUntil).length;
  return { items, unread, unsupported, markAllRead };
}
