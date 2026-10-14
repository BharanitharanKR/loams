import type { LoamsDesktopApi } from '@loams/desktop/contracts';
import { Button, Stat, Stats } from '@loams/ui';
import { MessageSquarePlus } from 'lucide-react';
import { draftFromPlaneIssue, threadDraftHref } from '../drafts.js';
import { QueryCard, useQuery } from '../model.js';
import { DataTable, label, mono, num, rel } from './common.js';
import type { PlaneIssue } from './dto.js';

export function PlanePanel({
  desktop,
  navigate,
}: {
  desktop: LoamsDesktopApi;
  navigate: (to: string) => void;
}) {
  const stats = useQuery<Record<string, number>>(desktop, 'plane', 'stats');
  const issues = useQuery<PlaneIssue[]>(desktop, 'plane', 'issues', { limit: 20 });
  return (
    <div className="flex flex-col gap-4">
      <QueryCard app="plane" title="Project stats" query={stats}>
        {(s) => {
          const entries = Object.entries(s);
          return entries.length === 0 ? (
            <p className="p-4 text-sm text-muted">No stats yet.</p>
          ) : (
            <Stats>
              {entries.map(([k, v]) => (
                <Stat key={k} label={label(k)} value={num(v)} />
              ))}
            </Stats>
          );
        }}
      </QueryCard>
      <QueryCard app="plane" title="Issues" query={issues}>
        {(rows) => (
          <DataTable
            caption="Issues"
            rows={rows}
            rowKey={(r, i) => r.id ?? String(i)}
            empty="No issues"
            columns={[
              { key: 'id', header: 'Id', cell: (r) => mono(r.id) },
              { key: 'title', header: 'Title', cell: (r) => r.title },
              { key: 'prio', header: 'Priority', cell: (r) => r.priority },
              { key: 'upd', header: 'Updated', cell: (r) => rel(r.updatedAt) },
              {
                key: 'thread',
                header: <span className="sr-only">Studio</span>,
                cell: (r) => (
                  // "Start thread from issue" (§53 §5.9): a thread draft in Studio.
                  <Button
                    size="sm"
                    variant="quiet"
                    aria-label={`Start thread from ${r.id ?? r.title ?? 'issue'}`}
                    title="Start a Studio thread from this issue"
                    onClick={() => navigate(threadDraftHref(draftFromPlaneIssue(r)))}
                  >
                    <MessageSquarePlus aria-hidden="true" size={14} /> Start thread
                  </Button>
                ),
              },
            ]}
          />
        )}
      </QueryCard>
    </div>
  );
}
