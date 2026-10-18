// The palette's branches (design 59, D1078; CF1 Task 11): each Studio project's local
// branches, from agentd's `ListBranches`. Branches are navigation only: a row opens the
// branch's project, and the palette never switches a checkout. Like the pin rows (T10-2)
// they show once something is typed, and only then are the branches read (ruling T11-3).

import { AgentdScope, useAgentdQuery } from '@loams/console-host/query';
import { GitService } from '@loams/proto/agentd';
import type { PaletteCommand } from '@loams/slots';
import { useCallback, useEffect, useRef } from 'react';
import type { StudioAgentd } from '../agentd.js';
import { useProjects, useReach } from '../projects/queries.js';

type Publish = (commands: readonly PaletteCommand[]) => void;

function ProjectBranches({
  project,
  publish,
  navigate,
}: {
  project: { id: string; name: string; path: string };
  publish: (id: string, commands: PaletteCommand[]) => void;
  navigate: (to: string) => void;
}) {
  const branches = useAgentdQuery(GitService.method.listBranches, { repoPath: project.path });
  const names = branches.data?.branches;
  useEffect(() => {
    publish(
      project.id,
      (names ?? []).map((name) => ({
        id: `branch:${project.id}:${name}`,
        title: name,
        group: 'Branches',
        detail: project.name,
        keywords: [project.name],
        run: () => navigate(`/studio/p/${encodeURIComponent(project.id)}`),
      })),
    );
  }, [names, project.id, project.name, publish, navigate]);
  return null;
}

function Branches({
  agentd,
  publish,
  navigate,
}: {
  agentd: StudioAgentd;
  publish: Publish;
  navigate: (to: string) => void;
}) {
  const reach = useReach(agentd);
  const projects = useProjects(agentd);
  const byProject = useRef(new Map<string, PaletteCommand[]>());
  const onProject = useCallback(
    (id: string, commands: PaletteCommand[]) => {
      byProject.current.set(id, commands);
      publish([...byProject.current.values()].flat());
    },
    [publish],
  );
  if (reach.state !== 'ready') return null;
  return (
    <>
      {(projects.data ?? []).map((p) => (
        <ProjectBranches key={p.id} project={p} publish={onProject} navigate={navigate} />
      ))}
    </>
  );
}

export function BranchesSource(props: {
  query: string;
  publish: Publish;
  agentd: StudioAgentd;
  navigate: (to: string) => void;
}) {
  const typed = props.query.trim() !== '';
  const { publish } = props;
  useEffect(() => {
    if (!typed) publish([]);
  }, [typed, publish]);
  if (!typed) return null;
  return (
    <AgentdScope agentd={props.agentd}>
      <Branches agentd={props.agentd} publish={props.publish} navigate={props.navigate} />
    </AgentdScope>
  );
}
