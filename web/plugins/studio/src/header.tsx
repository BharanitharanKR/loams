// The header's Studio context (§53 §4.2, §4.5): the project and branch of the page,
// and on the web the agentd host. The shell shows it only in Studio mode (the
// plugin is `modes: ["studio"]`).

import type { RouterService } from '@loams/console-host';
import { AgentdScope } from '@loams/console-host/query';
import { FolderGit2, GitBranch, Server } from 'lucide-react';
import { useSyncExternalStore } from 'react';
import type { StudioAgentd } from './agentd.js';
import { useProjects } from './projects/queries.js';

const CHIP =
  'inline-flex h-7 max-w-[180px] items-center gap-1.5 rounded-pill border border-rule bg-bg px-2.5 ' +
  'text-[0.8rem] text-muted';

function Context({ agentd, router }: { agentd: StudioAgentd; router: RouterService }) {
  const location = useSyncExternalStore(router.subscribe, () => router.current());
  const projects = useProjects(agentd);
  const projectId = location.params.projectId;
  const project = projectId ? projects.data?.find((p) => p.id === projectId) : undefined;
  const host = agentd.host();
  return (
    <div className="flex items-center gap-1.5 max-[720px]:hidden" data-studio-context>
      {project && (
        <span className={CHIP} title={project.path}>
          <FolderGit2 size={13} aria-hidden="true" />
          <span className="truncate">{project.name}</span>
        </span>
      )}
      {project?.branch && (
        <span className={CHIP} title="Branch">
          <GitBranch size={13} aria-hidden="true" />
          <span className="truncate font-mono text-[0.72rem]">{project.branch}</span>
        </span>
      )}
      {agentd.kind === 'web' && (
        <span className={CHIP} title="Agentd host">
          <Server size={13} aria-hidden="true" />
          <span className="truncate">{host?.name ?? 'No host'}</span>
        </span>
      )}
    </div>
  );
}

/** The header's Studio context, reading projects over `agentd`'s transport. */
export function StudioContext(props: Parameters<typeof Context>[0]) {
  return (
    <AgentdScope agentd={props.agentd}>
      <Context {...props} />
    </AgentdScope>
  );
}
