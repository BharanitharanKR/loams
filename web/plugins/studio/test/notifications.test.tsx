// The notification surface and the thread header's usage (SD1 Task 14; §53 §6.5 G19,
// G20): the bell lists the daemon's notifications and counts the new ones, leads to their
// thread, posts OS notifications in the background as Settings › Studio asks, and hides
// itself while the daemon does not serve notifications; the header shows a thread's usage,
// with a cost only when the harness reported one.

import type { MessageInitShape } from '@bufbuild/protobuf';
import { createRouterTransport } from '@connectrpc/connect';
import { AgentdScope } from '@loams/console-host/query';
import { desktopAgentdEndpoint } from '@loams/platform-electron';
import { createMockAgentdApi } from '@loams/plugin-desktop-agentd';
import {
  NotificationKind,
  type NotificationSchema,
  NotificationService,
  ThreadService,
  type TurnUsageSchema,
} from '@loams/proto/agentd';
import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { studioAgentd } from '../src/agentd.js';
import {
  addNotification,
  toStudioNotification,
  wantsOsNotification,
} from '../src/notifications/data.js';
import { NotificationsBell } from '../src/notifications/NotificationsBell.js';
import { SETTINGS_KEY } from '../src/settings/Settings.js';
import { formatElapsed, ThreadUsage, usageParts } from '../src/thread/ThreadUsage.js';
import { StudioView } from '../src/view.js';
import { wrap } from './fakes.js';
import { live, sidebarDaemon } from './sidebar-fakes.js';

type NotificationInit = MessageInitShape<typeof NotificationSchema>;
type TurnInit = MessageInitShape<typeof TurnUsageSchema>;

/** A daemon serving notifications (a feed one can post to) and usage. */
function daemon(o: { turns?: TurnInit[]; totals?: object; notifications?: false } = {}) {
  const feed = live<NotificationInit[]>([]);
  const transport = createRouterTransport(({ service }) => {
    if (o.notifications !== false) {
      service(NotificationService, {
        async *watchNotifications(_req, ctx) {
          let sent = 0;
          for await (const list of feed.stream(ctx.signal)) {
            for (const n of list.slice(sent)) yield { notification: n };
            sent = list.length;
          }
        },
      });
    }
    service(ThreadService, {
      getThreadUsage: () => ({ turns: o.turns ?? [], totals: o.totals ?? {} }),
    });
  });
  const mock = createMockAgentdApi({ running: true, background: 'off' });
  const agentd = studioAgentd(desktopAgentdEndpoint({ agentd: mock.api }, transport), {
    pickFolder: async () => null,
  });
  const post = (n: {
    id: string;
    kind: NotificationKind;
    title: string;
    chatId?: string;
    groupId?: string;
    createdAtMs?: bigint;
  }) =>
    act(() => {
      feed.set([...feed.get(), { createdAtMs: BigInt(Date.now()), ...n }]);
    });
  return { agentd, post };
}

function showBell(d: ReturnType<typeof daemon>, notify = vi.fn(async () => {})) {
  const navigate = vi.fn();
  render(
    wrap(
      <AgentdScope agentd={d.agentd}>
        <NotificationsBell navigate={navigate} notify={notify} />
      </AgentdScope>,
    ),
  );
  return { navigate, notify };
}

let focused = true;
beforeEach(() => {
  localStorage.clear();
  focused = true;
  vi.spyOn(document, 'hasFocus').mockImplementation(() => focused);
  window.matchMedia = ((query: string) => ({
    matches: query.includes('min-width'),
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  })) as unknown as typeof window.matchMedia;
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('the notification surface', () => {
  it('notifications_list_and_count_new_ones', async () => {
    const d = daemon();
    // One from before the page opened: listed, not counted.
    d.post({
      id: 'old',
      kind: NotificationKind.TURN_DONE,
      chatId: 't-1',
      title: 'Turn finished: Old work',
      createdAtMs: BigInt(Date.now() - 60_000),
    });
    const { navigate } = showBell(d);
    await screen.findByRole('button', { name: 'Notifications' });
    d.post({ id: 'n-1', kind: NotificationKind.ERROR, chatId: 't-2', title: 'Turn failed: Build' });
    d.post({
      id: 'n-2',
      kind: NotificationKind.GROUP_READY,
      groupId: 'g-1',
      title: 'Run group ready',
    });
    const bell = await screen.findByRole('button', { name: 'Notifications, 2 unread' });
    fireEvent.click(bell);
    const list = await screen.findByRole('list', { name: 'Notifications' });
    const links = [...list.querySelectorAll('a')].map((a) => a.getAttribute('href'));
    expect(links).toEqual(['#/studio/g/g-1', '#/studio/t/t-2', '#/studio/t/t-1']);
    expect(list.textContent).toContain('Turn failed: Build');
    fireEvent.click(screen.getByRole('link', { name: /Turn failed: Build/ }));
    expect(navigate).toHaveBeenCalledWith('/studio/t/t-2');
    // Opened, so read.
    await screen.findByRole('button', { name: 'Notifications' });
  });

  it('notifications_hidden_when_unimplemented', async () => {
    const d = daemon({ notifications: false });
    showBell(d);
    // The stream fails `unimplemented`: no bell, and no error either.
    await waitFor(() => expect(screen.queryByRole('button', { name: /Notifications/ })).toBeNull());
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByRole('button', { name: /Notifications/ })).toBeNull();
  });

  it('os_notifications_in_background_as_settings_ask', async () => {
    const d = daemon();
    const { notify } = showBell(d);
    await screen.findByRole('button', { name: 'Notifications' });
    // In front: in-app only.
    d.post({ id: 'a', kind: NotificationKind.TURN_DONE, chatId: 't-1', title: 'Turn finished: A' });
    await screen.findByRole('button', { name: 'Notifications, 1 unread' });
    expect(notify).not.toHaveBeenCalled();
    // In the background: an OS notification that opens the thread.
    focused = false;
    d.post({
      id: 'b',
      kind: NotificationKind.QUESTION,
      chatId: 't-2',
      title: 'Question waiting: B',
    });
    await waitFor(() =>
      expect(notify).toHaveBeenCalledWith({
        title: 'Question waiting: B',
        body: 'Question waiting',
        route: '/studio/t/t-2',
      }),
    );
    // Settings › Studio › Notifications turned off for finished turns.
    localStorage.setItem(SETTINGS_KEY, JSON.stringify({ notifyTurnFinished: false }));
    d.post({ id: 'c', kind: NotificationKind.TURN_DONE, chatId: 't-3', title: 'Turn finished: C' });
    await screen.findByRole('button', { name: 'Notifications, 3 unread' });
    expect(notify).toHaveBeenCalledTimes(1);
  });

  it('notifications_kept_by_id_newest_first', () => {
    const at = (id: string, ms: number) =>
      toStudioNotification({
        $typeName: 'loams.agentd.v1.Notification',
        id,
        kind: NotificationKind.APPROVAL,
        title: id,
        createdAtMs: BigInt(ms),
      });
    let list = addNotification([], at('a', 1));
    list = addNotification(list, at('b', 3));
    list = addNotification(list, at('a', 1));
    list = addNotification(list, at('c', 2));
    expect(list.map((n) => n.id)).toEqual(['b', 'c', 'a']);
    expect(list[0]?.href).toBeUndefined();
    expect(
      wantsOsNotification(NotificationKind.APPROVAL, {
        notifyTurnFinished: true,
        notifyApprovalWaiting: false,
      }),
    ).toBe(false);
    expect(
      wantsOsNotification(NotificationKind.CHECK_FAILED, {
        notifyTurnFinished: true,
        notifyApprovalWaiting: false,
      }),
    ).toBe(true);
  });
});

describe("the thread header's usage", () => {
  it('thread_header_shows_usage', async () => {
    const d = daemon({
      turns: [
        { turnId: 'msg-1', inputTokens: 12_000n, outputTokens: 800n, durationMs: 65_000n },
        { turnId: 'msg-2', inputTokens: 400n, outputTokens: 200n, durationMs: 5_000n },
      ],
      totals: { inputTokens: 12_400n, outputTokens: 1_000n, durationMs: 70_000n },
    });
    render(
      wrap(
        <StudioView
          agentd={d.agentd}
          route={{ kind: 'thread', threadId: 't-1' }}
          navigate={vi.fn()}
        />,
      ),
    );
    const usage = await screen.findByLabelText('Thread usage');
    // No cost reported: none shown.
    expect(usage.textContent).toBe('12k in · 1,000 out · 1m 10s');
    const chip = usage.closest('[data-thread-usage]');
    expect(chip?.getAttribute('title')).toContain('Turn 1: 12k in · 800 out · 1m 5s');
  });

  it('usage_cost_only_when_reported', () => {
    const base = { durationMs: 3_000n };
    expect(usageParts(base)).toEqual(['3s']);
    expect(usageParts({ ...base, inputTokens: 10n, outputTokens: 2n, cacheTokens: 0n })).toEqual([
      '10 in',
      '2 out',
      '3s',
    ]);
    expect(usageParts({ ...base, cacheTokens: 5n, costUsd: 0.0125 })).toEqual([
      '5 cached',
      '$0.013',
      '3s',
    ]);
    expect(formatElapsed(3_725_000)).toBe('1h 2m');
  });

  it('usage_hidden_without_turns_or_service', async () => {
    const d = sidebarDaemon();
    render(
      wrap(
        <AgentdScope agentd={d.agentd}>
          <ThreadUsage threadId="t-login" />
        </AgentdScope>,
      ),
    );
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByLabelText('Thread usage')).toBeNull();
    cleanup();
    const empty = daemon();
    render(
      wrap(
        <AgentdScope agentd={empty.agentd}>
          <ThreadUsage threadId="t-1" />
        </AgentdScope>,
      ),
    );
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByLabelText('Thread usage')).toBeNull();
  });
});
