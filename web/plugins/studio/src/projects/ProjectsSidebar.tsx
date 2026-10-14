// Studio's sidebar (§53 §5.1–§5.2): the projects, "Add project", and Settings.
// Threads and run groups join each project in SD1 Task 17; the Factory group
// (sidebar/Factory.tsx, Task 27) sits below. Add project opens the OS folder picker on the desktop (then `AddRepo`)
// and asks for a git URL on the web (`CloneRepo`).

import { Button, Dialog, Field, Input } from '@loams/ui';
import { FolderGit2, Plus, Settings } from 'lucide-react';
import { type FormEvent, type ReactNode, useState } from 'react';
import { isCloneUrl, type StudioAgentd } from '../agentd.js';
import { studioHref } from '../routes.js';
import { useAddProject, useProjects, useReach } from './queries.js';

const URL_HINT = 'An https, ssh or user@host:path git URL. The daemon clones it on its host.';

export function CloneDialog({
  open,
  onClose,
  onClone,
  pending,
  failure,
}: {
  open: boolean;
  onClose: () => void;
  onClone: (url: string) => void;
  pending: boolean;
  failure?: string;
}) {
  const [url, setUrl] = useState('');
  const [invalid, setInvalid] = useState(false);
  const submit = (e: FormEvent) => {
    e.preventDefault();
    const value = url.trim();
    if (!isCloneUrl(value)) {
      setInvalid(true);
      return;
    }
    setInvalid(false);
    onClone(value);
  };
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Add a project"
      dismissible={!pending}
      footer={
        <>
          <Button variant="quiet" onClick={onClose} disabled={pending}>
            Cancel
          </Button>
          <Button variant="primary" type="submit" form="studio-clone" disabled={pending}>
            {pending ? 'Cloning…' : 'Clone'}
          </Button>
        </>
      }
    >
      <form id="studio-clone" onSubmit={submit} noValidate>
        <Field
          label="Repository URL"
          hint={URL_HINT}
          error={invalid ? `Use ${URL_HINT.charAt(0).toLowerCase()}${URL_HINT.slice(1)}` : failure}
        >
          {(props) => (
            <Input
              {...props}
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="https://git.example.com/team/repo.git"
              autoComplete="off"
              spellCheck={false}
            />
          )}
        </Field>
      </form>
    </Dialog>
  );
}

export function ProjectsSidebar({
  agentd,
  activeProject,
  children,
}: {
  agentd: StudioAgentd;
  activeProject?: string;
  /** Groups below the projects: the Factory group (SD1 Task 27). */
  children?: ReactNode;
}) {
  const reach = useReach(agentd);
  const projects = useProjects(agentd);
  const add = useAddProject();
  const [cloning, setCloning] = useState(false);
  const ready = reach.state === 'ready';

  const onAdd = async () => {
    if (agentd.pickFolder) {
      const path = await agentd.pickFolder();
      if (path) add.mutate({ path });
      return;
    }
    add.reset();
    setCloning(true);
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex h-11 shrink-0 items-center justify-between gap-2 border-b border-rule-soft pr-2 pl-4">
        <h2 className="m-0 font-mono text-[0.68rem] text-faint uppercase tracking-[0.14em]">
          Projects
        </h2>
        <Button
          variant="quiet"
          size="sm"
          onClick={onAdd}
          disabled={!ready || add.isPending}
          aria-label="Add project"
          title="Add project"
        >
          <Plus size={14} aria-hidden="true" />
          <span>Add</span>
        </Button>
      </div>
      <nav aria-label="Projects" className="min-h-0 flex-1 overflow-auto py-2">
        {ready && projects.isSuccess && projects.data.length > 0 && (
          <ul className="m-0 list-none p-0">
            {projects.data.map((p) => {
              const current = p.id === activeProject;
              return (
                <li key={p.id}>
                  <a
                    href={`#${studioHref({ kind: 'project', projectId: p.id })}`}
                    aria-current={current ? 'page' : undefined}
                    title={p.path}
                    className={
                      'mx-2 flex items-center gap-2.5 rounded-sm px-2.5 py-1.5 text-[0.88rem] text-muted ' +
                      'no-underline hover:bg-raised hover:text-ink aria-[current=page]:bg-raised ' +
                      'aria-[current=page]:text-ink'
                    }
                  >
                    <FolderGit2
                      size={15}
                      strokeWidth={1.75}
                      aria-hidden="true"
                      className="shrink-0"
                    />
                    <span className="min-w-0 flex-1 truncate">{p.name}</span>
                    {p.branch && (
                      <span className="shrink-0 font-mono text-[0.68rem] text-faint">
                        {p.branch}
                      </span>
                    )}
                  </a>
                </li>
              );
            })}
          </ul>
        )}
        {ready && projects.isPending && (
          <p className="m-0 px-4 py-2 text-[0.84rem] text-faint">Loading…</p>
        )}
        {ready && projects.isSuccess && projects.data.length === 0 && (
          <p className="m-0 px-4 py-2 text-[0.84rem] text-faint">No projects yet.</p>
        )}
        {ready && projects.isError && (
          <p role="alert" className="m-0 px-4 py-2 text-[0.84rem] text-danger">
            Could not list projects: {projects.error.message}
          </p>
        )}
        {!ready && <p className="m-0 px-4 py-2 text-[0.84rem] text-faint">Not connected.</p>}
        {add.isError && !cloning && (
          <p role="alert" className="m-0 px-4 py-2 text-[0.84rem] text-danger">
            Could not add the project: {add.error?.message}
          </p>
        )}
      </nav>
      {children}
      <div className="shrink-0 border-t border-rule-soft p-2">
        <a
          href={`#${studioHref({ kind: 'settings' })}`}
          className="flex items-center gap-2.5 rounded-sm px-2.5 py-1.5 text-[0.86rem] text-muted no-underline hover:bg-raised hover:text-ink"
        >
          <Settings size={15} strokeWidth={1.75} aria-hidden="true" />
          Studio settings
        </a>
      </div>
      <CloneDialog
        open={cloning}
        onClose={() => setCloning(false)}
        pending={add.isPending}
        failure={add.isError ? add.error?.message : undefined}
        onClone={(url) => add.mutate({ url }, { onSuccess: () => setCloning(false) })}
      />
    </div>
  );
}
