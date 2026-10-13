// Studio's home and project pages (§53 §4.4: "home: recent threads, projects, run
// groups"). Threads and run groups arrive with SD1 Tasks 17 and 25; until then
// both say there are none.

import { Badge, Card, Empty } from '@loams/ui';
import { FolderGit2, GitBranch } from 'lucide-react';
import type { ReactNode } from 'react';
import type { StudioAgentd } from '../agentd.js';
import { useProjects } from '../projects/queries.js';
import { studioHref } from '../routes.js';

function Padded({ children }: { children: ReactNode }) {
  return <div className="p-6">{children}</div>;
}

export function Home({ agentd }: { agentd: StudioAgentd }) {
  const projects = useProjects(agentd);
  if (projects.isPending)
    return (
      <Padded>
        <Empty title="Loading projects…" />
      </Padded>
    );
  if (projects.isError) {
    return (
      <Padded>
        <Empty title="Could not list projects" grain="clay">
          {projects.error.message}
        </Empty>
      </Padded>
    );
  }
  if (projects.data.length === 0) {
    return (
      <Padded>
        <Empty title="No projects yet">
          A project is a repository on the daemon's host.{' '}
          {agentd.kind === 'desktop'
            ? 'Add a folder from this machine with Add in the sidebar.'
            : 'Clone one onto the host with Add in the sidebar.'}
        </Empty>
      </Padded>
    );
  }
  return (
    <div className="mx-auto flex max-w-[960px] flex-col gap-6 px-6 py-8">
      <header>
        <h1 className="m-0 text-[1.6rem] font-semibold text-ink tracking-[-0.01em]">
          Loams Studio
        </h1>
        <p className="m-0 mt-1 text-muted">Pick a project to start a thread in it.</p>
      </header>
      <section aria-labelledby="studio-projects" className="flex flex-col gap-3">
        <h2
          id="studio-projects"
          className="m-0 font-mono text-[0.7rem] text-faint uppercase tracking-[0.14em]"
        >
          Projects
        </h2>
        <ul className="m-0 grid list-none grid-cols-[repeat(auto-fill,minmax(240px,1fr))] gap-3 p-0">
          {projects.data.map((p) => (
            <li key={p.id}>
              <a
                href={`#${studioHref({ kind: 'project', projectId: p.id })}`}
                className="flex flex-col gap-2 border border-rule-soft bg-surface p-4 text-ink no-underline transition-colors hover:border-rule hover:bg-raised"
              >
                <span className="flex items-center gap-2 font-medium">
                  <FolderGit2
                    size={16}
                    strokeWidth={1.75}
                    aria-hidden="true"
                    className="text-accent"
                  />
                  {p.name}
                </span>
                <span className="truncate font-mono text-[0.72rem] text-faint">{p.path}</span>
                {p.branch && (
                  <span className="flex items-center gap-1.5 text-[0.8rem] text-muted">
                    <GitBranch size={13} aria-hidden="true" />
                    {p.branch}
                  </span>
                )}
              </a>
            </li>
          ))}
        </ul>
      </section>
      <Card title="Recent threads">
        <Empty title="No threads yet">Threads you start in a project show here.</Empty>
      </Card>
    </div>
  );
}

export function ProjectPage({ agentd, projectId }: { agentd: StudioAgentd; projectId: string }) {
  const projects = useProjects(agentd);
  if (projects.isPending)
    return (
      <Padded>
        <Empty title="Loading project…" />
      </Padded>
    );
  if (projects.isError) {
    return (
      <Padded>
        <Empty title="Could not list projects" grain="clay">
          {projects.error.message}
        </Empty>
      </Padded>
    );
  }
  const project = projects.data.find((p) => p.id === projectId);
  if (!project) {
    return (
      <Padded>
        <Empty title="No such project">
          The daemon has no project at <code>{projectId}</code>. It may have been removed.
        </Empty>
      </Padded>
    );
  }
  return (
    <div className="mx-auto flex max-w-[960px] flex-col gap-6 px-6 py-8">
      <header className="flex flex-col gap-1">
        <h1 className="m-0 text-[1.5rem] font-semibold text-ink">{project.name}</h1>
        <p className="m-0 flex flex-wrap items-center gap-2 text-muted">
          <code className="text-[0.78rem]">{project.path}</code>
          {project.branch && <Badge>{project.branch}</Badge>}
        </p>
      </header>
      <Empty title="No threads yet">
        A thread is one agent conversation in its own worktree of {project.name}.
      </Empty>
    </div>
  );
}
