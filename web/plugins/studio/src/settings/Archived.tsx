// Archived threads (design 59, D1079): out of the sidebar, in Studio's settings. Each one
// opens from here, and Unarchive puts it back in its project in the sidebar.

import { Button, Card } from '@loams/ui';
import type { StudioAgentd } from '../agentd.js';
import { useProjects, useReach } from '../projects/queries.js';
import { useSidebarData } from '../sidebar/data.js';
import { useThreadActions } from '../sidebar/useThreadActions.js';

export function ArchivedThreads({
  agentd,
  navigate,
}: {
  agentd: StudioAgentd;
  navigate: (to: string) => void;
}) {
  const reach = useReach(agentd);
  const ready = reach.state === 'ready';
  const projects = useProjects(agentd);
  const [data, applyPins] = useSidebarData(agentd, ready ? projects.data : undefined);
  const actions = useThreadActions({ navigate, threads: data.threads, applyPins });
  if (!ready) return null;
  const archived = data.threads.filter((t) => t.archived);
  return (
    <Card title="Archived threads">
      {data.state === 'loading' ? (
        <p className="m-0 text-body text-faint">Loading threads…</p>
      ) : archived.length === 0 ? (
        <p className="m-0 text-body text-muted">
          No archived threads. Archive a thread from its menu in the sidebar.
        </p>
      ) : (
        <ul aria-label="Archived threads" className="m-0 grid list-none gap-1 p-0">
          {archived.map((t) => (
            <li key={t.id} className="flex min-w-0 items-center gap-2">
              <a
                href={`#/studio/t/${encodeURIComponent(t.id)}`}
                className="min-w-0 flex-1 truncate text-body text-ink"
                title={t.title}
              >
                {t.title}
              </a>
              <Button
                size="sm"
                disabled={actions.pending}
                onClick={() => void actions.setArchived(t.id, false)}
                aria-label={`Unarchive ${t.title}`}
              >
                Unarchive
              </Button>
            </li>
          ))}
        </ul>
      )}
      {actions.error && (
        <p role="alert" className="m-0 mt-2 text-body text-danger">
          {actions.error}
        </p>
      )}
    </Card>
  );
}
