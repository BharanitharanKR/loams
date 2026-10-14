// One Studio page: the layout of §53 §5.1 around the route's view. Without a
// daemon every route but Settings shows how to get one (`NotConnected`).

import { AgentdScope } from '@loams/console-host/query';
import { Empty, Tabs } from '@loams/ui';
import { useEffect, useState } from 'react';
import type { StudioAgentd } from './agentd.js';
import type { StudioFactory } from './factory.js';
import type { GitActionBus } from './git/bus.js';
import { StudioGitTools } from './git/GitToolbar.js';
import { Home, ProjectPage } from './home/Home.js';
import { NotConnected } from './home/states.js';
import { StudioLayout } from './layout/StudioLayout.js';
import {
  createStudioKeys,
  type StudioKeys,
  useStudioCommand,
  useStudioKeybindings,
} from './palette/commands.js';
import { useProjects, useReach } from './projects/queries.js';
import { PANELS, type Panel, type StudioRoute, studioHref } from './routes.js';
import { SettingsPage } from './settings/Settings.js';
import { FactoryGroup } from './sidebar/Factory.js';
import { StudioSidebar } from './sidebar/Sidebar.js';
import { useNewThread } from './sidebar/useThreadActions.js';
import { DraftCard } from './thread/FactoryLinks.js';
import { FilesTab } from './thread/LazyFilesTab.js';
import { ThreadTerminalDrawer } from './thread/TerminalDrawer.js';

const PANEL_LABEL: Record<Panel, string> = {
  diff: 'Diff',
  files: 'Files',
  terminal: 'Terminal',
  preview: 'Preview',
  plan: 'Plan',
};

/** The thread's right-hand panel: its tabs are routes (`?panel=`). */
function ThreadPanel({
  route,
  navigate,
}: {
  route: Extract<StudioRoute, { kind: 'thread' }>;
  navigate: (to: string) => void;
}) {
  const panel = route.panel ?? 'diff';
  return (
    <div className="flex min-h-0 flex-1 flex-col px-3 pt-1">
      <Tabs
        label="Thread panel"
        value={panel}
        onValueChange={(p) => navigate(studioHref({ ...route, panel: p }))}
        tabs={PANELS.map((p) => ({ value: p, label: PANEL_LABEL[p] }))}
      />
      {panel === 'files' ? (
        <FilesTab threadId={route.threadId} {...(route.file ? { link: route.file } : {})} />
      ) : (
        <div className="min-h-0 flex-1 overflow-auto py-4">
          <Empty title={`No ${PANEL_LABEL[panel].toLowerCase()} yet`} seed={panel.length}>
            This thread has nothing to show here.
          </Empty>
        </div>
      )}
    </div>
  );
}

function Title({ agentd, route }: { agentd: StudioAgentd; route: StudioRoute }) {
  const projects = useProjects(agentd);
  switch (route.kind) {
    case 'home':
      return <>Home</>;
    case 'project':
      return <>{projects.data?.find((p) => p.id === route.projectId)?.name ?? 'Project'}</>;
    case 'thread':
      return (
        <>
          Thread <code>{route.threadId}</code>
        </>
      );
    case 'group':
      return (
        <>
          Run group <code>{route.groupId}</code>
        </>
      );
    case 'settings':
      return <>Settings</>;
  }
}

function Body({
  agentd,
  route,
  keys,
}: {
  agentd: StudioAgentd;
  route: StudioRoute;
  keys: StudioKeys;
}) {
  switch (route.kind) {
    case 'home':
      return (
        <>
          {route.draft && <DraftCard draft={route.draft} />}
          <Home agentd={agentd} />
        </>
      );
    case 'project':
      return <ProjectPage agentd={agentd} projectId={route.projectId} />;
    case 'thread':
      return (
        <div className="p-6">
          <Empty title="This thread is not available">
            Studio cannot open threads in this version yet. The link <code>{route.threadId}</code>{' '}
            will open here once it can.
          </Empty>
        </div>
      );
    case 'group':
      return (
        <div className="p-6">
          <Empty title="This run group is not available">
            Studio cannot compare parallel runs in this version yet.
          </Empty>
        </div>
      );
    case 'settings':
      return <SettingsPage agentd={agentd} keys={keys} />;
  }
}

/**
 * The route's commands (SD1 Task 24): a new thread, settings, history, and on a thread
 * its panels. Showing a panel that is already shown hides the panel, as upstream's
 * toggles do.
 */
function useStudioRouteCommands(
  agentd: StudioAgentd,
  keys: StudioKeys,
  route: StudioRoute,
  navigate: (to: string) => void,
  connected: boolean,
): void {
  const { commands } = keys;
  // A thread in the project in view, else the home page to pick one (SD1 Task 17).
  useStudioCommand(commands, 'chat.new', useNewThread(agentd, route, navigate));
  useStudioCommand(commands, 'settings.open', () => navigate('/studio/settings'));
  useStudioCommand(commands, 'navigation.back', () => window.history.back());
  useStudioCommand(commands, 'navigation.forward', () => window.history.forward());
  const thread = route.kind === 'thread' && connected ? route : undefined;
  const show = (panel: Panel) => () => {
    if (!thread) return;
    if ((thread.panel ?? 'diff') === panel) {
      commands.run('rightPanel.toggle');
      return;
    }
    navigate(studioHref({ ...thread, panel }));
    commands.run('rightPanel.show');
  };
  useStudioCommand(commands, 'diff.toggle', thread ? show('diff') : undefined);
  useStudioCommand(commands, 'files.toggle', thread ? show('files') : undefined);
  useStudioCommand(commands, 'preview.toggle', thread ? show('preview') : undefined);
}

function StudioPage({
  agentd,
  factory,
  route,
  navigate,
  hostParam,
  keys: given,
  openExternal,
  gitBus,
}: {
  agentd: StudioAgentd;
  /** The software factory (§53 §5.9): the desktop's bridge; none on the web until SF1. */
  factory?: StudioFactory;
  route: StudioRoute;
  navigate: (to: string) => void;
  /** Web: `?host=<id>` selects a registered agentd host, by id only (§53 §4.4). */
  hostParam?: string;
  /** Studio's commands and keybindings (SD1 Task 24); the plugin shares one with its palette. */
  keys?: StudioKeys;
  /** Opens an http(s) link outside Studio (the platform's; a terminal's links, SD1 Task 22). */
  openExternal?: (url: string) => Promise<void>;
  /** The git toolbar's link to the palette (SD1 Task 23, T24-6). */
  gitBus?: GitActionBus;
}) {
  useEffect(() => {
    if (hostParam) agentd.hosts?.select(hostParam);
  }, [agentd, hostParam]);
  const reach = useReach(agentd);
  const connected = reach.state === 'ready' || route.kind === 'settings';
  const thread = route.kind === 'thread' ? route : undefined;
  const [own] = useState(() => createStudioKeys({ edition: agentd.kind }));
  const keys = given ?? own;
  useStudioRouteCommands(agentd, keys, route, navigate, connected);
  useStudioKeybindings(keys, { threadOpen: thread !== undefined && connected });
  return (
    <StudioLayout
      title={<Title agentd={agentd} route={route} />}
      sidebar={
        <StudioSidebar agentd={agentd} route={route} navigate={navigate} keys={keys}>
          {factory && <FactoryGroup factory={factory} />}
        </StudioSidebar>
      }
      commands={keys.commands}
      tools={
        connected ? (
          <StudioGitTools
            agentd={agentd}
            route={route}
            factory={factory}
            bus={gitBus}
            openTerminal={() => keys.commands.run('terminal.toggle')}
          />
        ) : undefined
      }
      panel={thread && connected ? <ThreadPanel route={thread} navigate={navigate} /> : undefined}
      drawer={
        thread && connected ? (
          <ThreadTerminalDrawer
            threadId={thread.threadId}
            keys={keys}
            navigate={navigate}
            {...(openExternal ? { openExternal } : {})}
          />
        ) : undefined
      }
    >
      {connected ? (
        <Body agentd={agentd} route={route} keys={keys} />
      ) : (
        <div className="p-6">
          <NotConnected agentd={agentd} reach={reach} />
        </div>
      )}
    </StudioLayout>
  );
}

/** A Studio page, with connect-query reading the daemon over `agentd`'s transport. */
export function StudioView(props: Parameters<typeof StudioPage>[0]) {
  return (
    <AgentdScope agentd={props.agentd}>
      <StudioPage {...props} />
    </AgentdScope>
  );
}
