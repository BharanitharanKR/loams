import type { LoamsDesktopApi } from '@loams/desktop/contracts';
import { Button, Select, Stat, Stats } from '@loams/ui';
import { Wrench } from 'lucide-react';
import { useState } from 'react';
import { draftFromGlitchtipIssue, type GlitchtipEvent, threadDraftHref } from '../drafts.js';
import { QueryCard, useQuery } from '../model.js';
import { DataTable, mono, rel } from './common.js';
import type { GlitchtipIssue, GlitchtipOrg } from './dto.js';

/**
 * "Fix this error" (§53 §5.9): reads the issue's latest event for its stack trace and
 * release, then opens a thread draft in Studio. An event that cannot be read still
 * starts the draft, which then says so.
 */
function FixButton({
  desktop,
  issue,
  navigate,
}: {
  desktop: LoamsDesktopApi;
  issue: GlitchtipIssue;
  navigate: (to: string) => void;
}) {
  const [busy, setBusy] = useState(false);
  const fix = async () => {
    setBusy(true);
    let event: GlitchtipEvent | undefined;
    try {
      if (issue.id) {
        const r = await desktop.factory.query<GlitchtipEvent>({
          app: 'glitchtip',
          op: 'latestEvent',
          params: { issueId: issue.id },
        });
        if (r.ok) event = r.value;
      }
    } catch {
      // The draft says the stack trace could not be read.
    }
    setBusy(false);
    navigate(threadDraftHref(draftFromGlitchtipIssue(issue, event)));
  };
  return (
    <Button
      size="sm"
      variant="quiet"
      disabled={busy}
      aria-label={`Fix this error: ${issue.id ?? issue.title ?? 'issue'}`}
      title="Start a Studio thread to fix this error"
      onClick={() => void fix()}
    >
      <Wrench aria-hidden="true" size={14} /> {busy ? 'Reading…' : 'Fix this error'}
    </Button>
  );
}

export function GlitchtipPanel({
  desktop,
  navigate,
}: {
  desktop: LoamsDesktopApi;
  navigate: (to: string) => void;
}) {
  const orgs = useQuery<GlitchtipOrg[]>(desktop, 'glitchtip', 'organizations');
  const [picked, setPicked] = useState<string>();
  const list = orgs[0].state === 'ready' ? orgs[0].data : [];
  const orgSlug = picked ?? list[0]?.slug;
  const issues = useQuery<GlitchtipIssue[]>(
    desktop,
    'glitchtip',
    'issues',
    { orgSlug, limit: 20 },
    Boolean(orgSlug),
  );
  const loaded = issues[0].state === 'ready' ? issues[0].data : undefined;
  return (
    <div className="flex flex-col gap-4">
      <Stats>
        <Stat label="Organisations" value={orgs[0].state === 'ready' ? list.length : '…'} />
        <Stat label="Unresolved errors" value={loaded ? loaded.length : '…'} />
      </Stats>
      <QueryCard
        app="glitchtip"
        title="Unresolved issues"
        query={
          orgs[0].state === 'ready' && !orgSlug ? [{ state: 'ready', data: [] }, orgs[1]] : issues
        }
        actions={
          list.length > 0 && (
            <Select
              aria-label="Organisation"
              value={orgSlug ?? ''}
              onChange={(e) => setPicked(e.target.value)}
            >
              {list.map((o) => (
                <option key={o.slug} value={o.slug}>
                  {o.name ?? o.slug}
                </option>
              ))}
            </Select>
          )
        }
      >
        {(rows) => (
          <DataTable
            caption="Unresolved issues"
            rows={rows}
            rowKey={(r, i) => r.id ?? String(i)}
            empty="No unresolved issues"
            columns={[
              { key: 'id', header: 'Id', cell: (r) => mono(r.id) },
              { key: 'title', header: 'Issue', cell: (r) => r.title },
              { key: 'level', header: 'Level', cell: (r) => r.level },
              { key: 'count', header: 'Events', numeric: true, cell: (r) => r.count },
              { key: 'seen', header: 'Last seen', cell: (r) => rel(r.lastSeen) },
              {
                key: 'fix',
                header: <span className="sr-only">Studio</span>,
                cell: (r) => <FixButton desktop={desktop} issue={r} navigate={navigate} />,
              },
            ]}
          />
        )}
      </QueryCard>
    </div>
  );
}
