// One Studio page: the route's view in the shell's main region (design 59). The sidebar's
// thread list, the thread's panels (inspector tabs) and its terminal (a drawer tab) are the
// shell's regions now (regions.tsx). Without a daemon every route but Settings shows how to
// get one (`NotConnected`).

import type { PanelService } from '@loams/console-host';
import { AgentdScope } from '@loams/console-host/query';
import { Empty, Spinner } from '@loams/ui';
import { lazy, Suspense, useEffect, useState } from 'react';
import type { StudioAgentd } from './agentd.js';
import { Home, ProjectPage } from './home/Home.js';
import { NotConnected } from './home/states.js';
import { StudioLayout } from './layout/StudioLayout.js';
import {
  createStudioKeys,
  type StudioKeys,
  useStudioCommand,
  useStudioKeybindings,
} from './palette/commands.js';
import { PreviewTab } from './preview/PreviewTab.js';
import { useProjects, useReach } from './projects/queries.js';
import { type Panel, type StudioRoute, studioHref } from './routes.js';
import { SettingsPage } from './settings/Settings.js';
import { usePublishedSidebar } from './sidebar/data.js';
import { useNewThread } from './sidebar/useThreadActions.js';
import { DraftCard } from './thread/FactoryLinks.js';
import { DiffTab } from './thread/LazyDiffTab.js';
import { FilesTab } from './thread/LazyFilesTab.js';

// The thread page carries the markdown and highlighting stack: loaded on first use (SD1 Task 18).
const ThreadPage = lazy(() =>
  import('./thread/ThreadPage.js').then((m) => ({ default: m.ThreadPage })),
);

// The run group's view carries the pair diff's viewer: loaded on first use (SD1 Task 25).
const GroupPage = lazy(() =>
  import('./groups/GroupPage.js').then((m) => ({ default: m.GroupPage })),
);

const PANEL_LABEL: Record<Panel, string> = {
  diff: 'Diff',
  files: 'Files',
  terminal: 'Terminal',
  preview: 'Preview',
  plan: 'Plan',
};

/**
 * One of the thread's panels, the body of its inspector tab (design 59): the shell draws the
 * tab strip, and a `?panel=` link opens the matching tab.
 */
export function ThreadPanelBody({
  agentd,
  route,
  navigate,
  openExternal,
}: {
  agentd: StudioAgentd;
  route: Extract<StudioRoute, { kind: 'thread' }>;
  navigate: (to: string) => void;
  openExternal?: (url: string) => Promise<void>;
}) {
  const panel = route.panel ?? 'diff';
  // The inspector tab is edge to edge; the panel keeps the gutter its old column had.
  return (
    <div className="flex min-h-0 flex-1 flex-col px-3 pt-2" data-studio-panel={panel}>
      {panel === 'diff' ? (
        <DiffTab
          threadId={route.threadId}
          onOpenFile={(path) => navigate(studioHref({ ...route, panel: 'files', file: { path } }))}
        />
      ) : panel === 'files' ? (
        <FilesTab threadId={route.threadId} {...(route.file ? { link: route.file } : {})} />
      ) : panel === 'preview' ? (
        <PreviewTab
          threadId={route.threadId}
          {...(agentd.preview ? { preview: agentd.preview } : {})}
          {...(openExternal ? { openExternal } : {})}
        />
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

function Body({
  agentd,
  route,
  keys,
  navigate,
  openExternal,
}: {
  agentd: StudioAgentd;
  route: StudioRoute;
  keys: StudioKeys;
  navigate: (to: string) => void;
  openExternal?: ((url: string) => void) | undefined;
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
      return <ProjectPage agentd={agentd} projectId={route.projectId} navigate={navigate} />;
    case 'thread':
      return (
        <Suspense
          fallback={
            <div className="p-6">
              <Spinner label="Loading the thread" />
            </div>
          }
        >
          <ThreadPage
            threadId={route.threadId}
            navigate={navigate}
            {...(openExternal ? { openExternal } : {})}
            {...(agentd.desktop ? { openInEditor: agentd.desktop.openInEditor } : {})}
          />
        </Suspense>
      );
    case 'group':
      return (
        <Suspense
          fallback={
            <div className="p-6">
              <Spinner label="Loading the run group" />
            </div>
          }
        >
          <GroupPage groupId={route.groupId} navigate={navigate} />
        </Suspense>
      );
    case 'settings':
      return <SettingsPage agentd={agentd} keys={keys} navigate={navigate} />;
  }
}

/**
 * The route's commands (SD1 Task 24): a new thread, settings, history, and on a thread
 * its panels. Showing a panel whose inspector tab is already shown hides the inspector, as
 * upstream's toggles do.
 */
function useStudioRouteCommands(
  agentd: StudioAgentd,
  keys: StudioKeys,
  route: StudioRoute,
  navigate: (to: string) => void,
  connected: boolean,
  inspector: PanelService | undefined,
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
    const tab = `studio.${panel}`;
    const s = inspector?.state();
    if (s?.open && s.tab === tab) {
      inspector?.close();
      return;
    }
    navigate(studioHref({ ...thread, panel }));
    // The link may already name the panel (the inspector was closed since): open it here too.
    inspector?.open({ tab, props: {} });
  };
  useStudioCommand(commands, 'diff.toggle', thread ? show('diff') : undefined);
  useStudioCommand(commands, 'files.toggle', thread ? show('files') : undefined);
  useStudioCommand(commands, 'preview.toggle', thread ? show('preview') : undefined);
}

/**
 * Studio's commands and keybindings on a page of its own or in the shell's home slot
 * (regions.tsx): one place, so Mod+N and the palette's commands work on both.
 */
export function useStudioPageKeys(o: {
  agentd: StudioAgentd;
  keys: StudioKeys;
  route: StudioRoute;
  navigate: (to: string) => void;
  connected: boolean;
  inspector?: PanelService | undefined;
}): void {
  const { agentd, keys, route, navigate, connected, inspector } = o;
  useStudioRouteCommands(agentd, keys, route, navigate, connected, inspector);
  useStudioKeybindings(keys, { threadOpen: route.kind === 'thread' && connected });
}

function StudioPage({
  agentd,
  route,
  navigate,
  hostParam,
  keys: given,
  openExternal,
  inspector,
  drawer,
  setTrail,
}: {
  agentd: StudioAgentd;
  route: StudioRoute;
  navigate: (to: string) => void;
  /** Web: `?host=<id>` selects a registered agentd host, by id only (§53 §4.4). */
  hostParam?: string;
  /** Studio's commands and keybindings (SD1 Task 24); the plugin shares one with its palette. */
  keys?: StudioKeys;
  /** Opens an http(s) link outside Studio (the platform's; a terminal's links, SD1 Task 22). */
  openExternal?: (url: string) => Promise<void>;
  /** The shell's inspector: the thread's panels are its tabs (design 59). */
  inspector?: PanelService;
  /** The shell's drawer: the thread's terminal is its tab (design 59). */
  drawer?: PanelService;
  /** The shell's breadcrumbs (and tab title): the project and thread by name. */
  setTrail?: (trail: { label: string; href?: string }[] | undefined) => void;
}) {
  useEffect(() => {
    if (hostParam) agentd.hosts?.select(hostParam);
  }, [agentd, hostParam]);
  useStudioTrail(agentd, route, setTrail);
  const reach = useReach(agentd);
  const connected = reach.state === 'ready' || route.kind === 'settings';
  const thread = route.kind === 'thread' ? route : undefined;
  const [own] = useState(() => createStudioKeys({ edition: agentd.kind }));
  const keys = given ?? own;
  useStudioPageKeys({ agentd, keys, route, navigate, connected, inspector });
  // A `?panel=` link (a file link from the terminal or the diff, the palette) opens that tab,
  // again whenever the link changes (another file in the Files tab).
  const link = thread?.panel ? studioHref(thread) : undefined;
  const linkPanel = thread?.panel;
  useEffect(() => {
    if (!link || !linkPanel) return;
    if (linkPanel === 'terminal') drawer?.open({ tab: 'studio.terminal' });
    else inspector?.open({ tab: `studio.${linkPanel}`, props: {} });
  }, [link, linkPanel, inspector, drawer]);
  return (
    <StudioLayout commands={keys.commands} inspector={inspector} drawer={drawer}>
      {connected ? (
        <Body
          agentd={agentd}
          route={route}
          keys={keys}
          navigate={navigate}
          openExternal={openExternal}
        />
      ) : (
        <div className="p-6">
          <NotConnected agentd={agentd} reach={reach} />
        </div>
      )}
    </StudioLayout>
  );
}

/**
 * "Studio / loams / Fix the login redirect loop" instead of "Studio / Thread": the names
 * come from the projects query and the sidebar's published rows, once they are known.
 */
function useStudioTrail(
  agentd: StudioAgentd,
  route: StudioRoute,
  setTrail: ((trail: { label: string; href?: string }[] | undefined) => void) | undefined,
): void {
  const projects = useProjects(agentd);
  const sidebar = usePublishedSidebar(agentd);
  const thread =
    route.kind === 'thread' ? sidebar?.threads.find((t) => t.id === route.threadId) : undefined;
  const projectId = route.kind === 'project' ? route.projectId : (thread?.projectId ?? undefined);
  const project = projectId ? projects.data?.find((p) => p.id === projectId) : undefined;
  const threadTitle = thread ? thread.title || 'Untitled thread' : undefined;
  const projectName = project?.name;
  const projectHref = project ? studioHref({ kind: 'project', projectId: project.id }) : undefined;
  const kind = route.kind;
  useEffect(() => {
    if (!setTrail) return;
    const root = { label: 'Studio', href: '/studio' };
    if (kind === 'project' && projectName) setTrail([root, { label: projectName }]);
    else if (kind === 'thread' && threadTitle) {
      setTrail([
        root,
        ...(projectName && projectHref ? [{ label: projectName, href: projectHref }] : []),
        { label: threadTitle },
      ]);
    } else return;
    return () => setTrail(undefined);
  }, [setTrail, kind, projectName, projectHref, threadTitle]);
}

/** A Studio page, with connect-query reading the daemon over `agentd`'s transport. */
export function StudioView(props: Parameters<typeof StudioPage>[0]) {
  return (
    <AgentdScope agentd={props.agentd}>
      <StudioPage {...props} />
    </AgentdScope>
  );
}
