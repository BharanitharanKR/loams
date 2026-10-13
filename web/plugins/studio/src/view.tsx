// One Studio page: the layout of §53 §5.1 around the route's view. Without a
// daemon every route but Settings shows how to get one (`NotConnected`).

import { Empty, Tabs } from '@loams/ui';
import type { StudioAgentd } from './agentd.js';
import { Home, ProjectPage } from './home/Home.js';
import { NotConnected } from './home/states.js';
import { StudioLayout } from './layout/StudioLayout.js';
import { ProjectsSidebar } from './projects/ProjectsSidebar.js';
import { useProjects, useReach } from './projects/queries.js';
import { PANELS, type Panel, type StudioRoute, studioHref } from './routes.js';
import { SettingsPage } from './settings/Settings.js';

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
      <div className="min-h-0 flex-1 overflow-auto py-4">
        <Empty title={`No ${PANEL_LABEL[panel].toLowerCase()} yet`} seed={panel.length}>
          This thread has nothing to show here.
        </Empty>
      </div>
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

function Body({ agentd, route }: { agentd: StudioAgentd; route: StudioRoute }) {
  switch (route.kind) {
    case 'home':
      return <Home agentd={agentd} />;
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
      return <SettingsPage agentd={agentd} />;
  }
}

export function StudioView({
  agentd,
  route,
  navigate,
}: {
  agentd: StudioAgentd;
  route: StudioRoute;
  navigate: (to: string) => void;
}) {
  const reach = useReach(agentd);
  const connected = reach.state === 'ready' || route.kind === 'settings';
  const thread = route.kind === 'thread' ? route : undefined;
  return (
    <StudioLayout
      title={<Title agentd={agentd} route={route} />}
      sidebar={
        <ProjectsSidebar
          agentd={agentd}
          activeProject={route.kind === 'project' ? route.projectId : undefined}
        />
      }
      panel={thread && connected ? <ThreadPanel route={thread} navigate={navigate} /> : undefined}
      drawer={
        thread && connected ? (
          <div className="p-3">
            <Empty title="No terminals" seed={5}>
              Terminals for this thread's worktree open here.
            </Empty>
          </div>
        ) : undefined
      }
    >
      {connected ? (
        <Body agentd={agentd} route={route} />
      ) : (
        <div className="p-6">
          <NotConnected agentd={agentd} reach={reach} />
        </div>
      )}
    </StudioLayout>
  );
}
