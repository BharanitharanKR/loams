// One workspace (§19.4): its base commit and the repository seq it was read at, its
// HEAD and writer, the overlay of changed paths (the snapshot's `changed` hint, or a note
// when it overflowed MAX_CHANGED_HINT), the snapshots timeline (latest first, with named
// snapshots, takeovers and the fork origin), and the code browser, pinned to a snapshot
// when one is chosen from the timeline.

import { Badge, Button, formatRelative, Notice, StatusTag } from '@loams/ui';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { CHANGE_TEXT, CodeBrowser } from './CodeBrowser.js';
import {
  type ChangedPath,
  type GitClient,
  type GitFailure,
  isAbort,
  MAX_CHANGED_HINT,
  type Snapshot,
  shortOid,
  toFailure,
  type Workspace,
} from './client.js';
import { Tabs } from './Tabs.js';

type Tab = 'changes' | 'snapshots' | 'files';

export function WorkspaceDetail(p: {
  client: GitClient;
  namespace: string;
  repoId: string;
  wsId: string;
}) {
  const { client, namespace, repoId, wsId } = p;
  const [ws, setWs] = useState<Workspace>();
  const [error, setError] = useState<GitFailure>();
  const [tab, setTab] = useState<Tab>('changes');
  const [pinned, setPinned] = useState<number>();
  const ref = useMemo(() => ({ namespace, repoId, wsId }), [namespace, repoId, wsId]);

  useEffect(() => {
    const ac = new AbortController();
    setWs(undefined);
    setError(undefined);
    setPinned(undefined);
    client.getWorkspace(ref, ac.signal).then(setWs, (e) => {
      if (!isAbort(e)) setError(toFailure(e));
    });
    return () => ac.abort();
  }, [client, ref]);

  if (error) {
    return (
      <Notice tone="danger" title="Could not open the workspace">
        {error.message}
      </Notice>
    );
  }
  if (!ws) {
    return (
      <p className="m-0 text-sm text-muted" role="status">
        Loading workspace…
      </p>
    );
  }

  return (
    <article className="flex min-w-0 flex-col gap-3" aria-labelledby="git-ws-title">
      <header className="flex flex-col gap-1">
        <h2 id="git-ws-title" className="m-0 flex flex-wrap items-center gap-2 text-base">
          <span className="font-mono">{ws.name || ws.wsId}</span>
          {ws.agent && <Badge>agent</Badge>}
          {ws.conflicts.length > 0 && <StatusTag status="failed">conflicts</StatusTag>}
        </h2>
        <dl className="m-0 grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-0.5 text-sm">
          <dt className="text-muted">Workspace</dt>
          <dd className="m-0 font-mono">{ws.wsId}</dd>
          <dt className="text-muted">Owner</dt>
          <dd className="m-0 font-mono">{ws.owner}</dd>
          <dt className="text-muted">Tracks</dt>
          <dd className="m-0 font-mono">{ws.track}</dd>
          <dt className="text-muted">Base commit</dt>
          <dd className="m-0 font-mono" title={ws.base}>
            {shortOid(ws.base)}{' '}
            <span className="text-muted">at seq {ws.baseSeq.toLocaleString('en-US')}</span>
          </dd>
          <dt className="text-muted">HEAD</dt>
          <dd className="m-0 font-mono" title={ws.head}>
            {shortOid(ws.head)} {ws.headRef ? `(${ws.headRef})` : '(detached)'}
          </dd>
          <dt className="text-muted">Latest snapshot</dt>
          <dd className="m-0 font-mono">
            @{ws.n} · writer {ws.writer} · {formatRelative(ws.updatedAt)}
          </dd>
          {ws.fork && (
            <>
              <dt className="text-muted">Forked from</dt>
              <dd className="m-0 font-mono">
                {ws.fork.wsId}@{ws.fork.n}
              </dd>
            </>
          )}
        </dl>
      </header>
      {ws.conflicts.length > 0 && (
        <Notice tone="warn" title="Unresolved conflicts">
          {ws.conflicts.join(', ')}
        </Notice>
      )}
      <Tabs<Tab>
        label="Workspace views"
        value={tab}
        onChange={setTab}
        tabs={[
          {
            key: 'changes',
            label: `Changes (${ws.changedOverflow ? `>${MAX_CHANGED_HINT.toLocaleString('en-US')}` : ws.changed.length})`,
          },
          { key: 'snapshots', label: 'Snapshots' },
          { key: 'files', label: pinned !== undefined ? `Files @${pinned}` : 'Files' },
        ]}
      >
        {tab === 'changes' && <ChangedPaths ws={ws} />}
        {tab === 'snapshots' && (
          <SnapshotTimeline
            client={client}
            wsRef={ref}
            pinned={pinned}
            onBrowse={(n) => {
              setPinned(n);
              setTab('files');
            }}
          />
        )}
        {tab === 'files' && (
          <div className="flex flex-col gap-2">
            {pinned !== undefined && (
              <p className="m-0 flex items-center gap-2 text-sm text-muted">
                Browsing snapshot @{pinned}.
                <Button size="sm" variant="quiet" onClick={() => setPinned(undefined)}>
                  Browse latest
                </Button>
              </p>
            )}
            <CodeBrowser client={client} ws={ref} n={pinned} />
          </div>
        )}
      </Tabs>
    </article>
  );
}

function ChangedPaths({ ws }: { ws: Workspace }) {
  if (ws.changedOverflow) {
    return (
      <Notice title="Too many changed paths to list">
        More than {MAX_CHANGED_HINT.toLocaleString('en-US')} paths differ from the base, so the
        snapshot omits the list. Browse the files to see the overlay.
      </Notice>
    );
  }
  if (ws.changed.length === 0) {
    return <p className="m-0 text-sm text-muted">No changes: the workspace matches its base.</p>;
  }
  const sorted = [...ws.changed].sort((a, b) => a.path.localeCompare(b.path));
  return (
    <table className="w-full border-collapse text-sm">
      <caption className="sr-only">Changed paths in the overlay</caption>
      <thead>
        <tr className="text-left text-muted">
          <th scope="col" className="py-1 font-normal">
            Path
          </th>
          <th scope="col" className="py-1 font-normal">
            Change
          </th>
          <th scope="col" className="py-1 font-normal">
            Staged
          </th>
        </tr>
      </thead>
      <tbody>
        {sorted.map((c: ChangedPath) => (
          <tr key={c.path} className="border-0 border-t border-solid border-rule">
            <td className="py-1 font-mono">{c.path}</td>
            <td className="py-1">
              <Badge>{CHANGE_TEXT[c.kind]}</Badge>
            </td>
            <td className="py-1">{c.staged ? 'yes' : 'no'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function SnapshotTimeline(p: {
  client: GitClient;
  wsRef: { namespace: string; repoId: string; wsId: string };
  pinned?: number;
  onBrowse: (n: number) => void;
}) {
  const { client, wsRef } = p;
  const [snaps, setSnaps] = useState<Snapshot[]>([]);
  const [next, setNext] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<GitFailure>();
  // One request at a time: a new page or another workspace aborts the one in flight, so
  // an older page of the previous workspace never lands in this timeline.
  const inflight = useRef<AbortController | null>(null);

  const load = useCallback(
    async (token: string) => {
      inflight.current?.abort();
      const ac = new AbortController();
      inflight.current = ac;
      setLoading(true);
      setError(undefined);
      try {
        const res = await client.listSnapshots(
          { ...wsRef, pageToken: token, pageSize: 50 },
          ac.signal,
        );
        setSnaps((prev) => (token ? [...prev, ...res.items] : res.items));
        setNext(res.nextPageToken);
      } catch (e) {
        if (isAbort(e)) return;
        setError(toFailure(e));
      } finally {
        if (inflight.current === ac) {
          inflight.current = null;
          setLoading(false);
        }
      }
    },
    [client, wsRef],
  );

  useEffect(() => {
    setSnaps([]);
    setNext('');
    void load('');
    return () => inflight.current?.abort();
  }, [load]);

  return (
    <section className="flex flex-col gap-2" aria-label="Snapshots">
      {error && (
        <Notice tone="danger" title="Could not list snapshots">
          {error.message}
        </Notice>
      )}
      <ol className="m-0 flex list-none flex-col gap-0 border-0 border-l-2 border-solid border-rule p-0 pl-4">
        {snaps.map((s) => (
          <li key={s.n} className="relative flex flex-col gap-0.5 py-2">
            <span
              aria-hidden="true"
              className={`absolute top-3 -left-[21px] h-2.5 w-2.5 rounded-full ${
                s.label ? 'bg-accent' : 'bg-rule'
              }`}
            />
            <span className="flex flex-wrap items-center gap-2 text-sm">
              <span className="font-mono text-ink">@{s.n}</span>
              {s.label && <Badge>{s.label}</Badge>}
              {s.takeover && (
                <Badge
                  title={`Taken over from ${s.takeover.fromMount} at @${s.takeover.fromN}: ${s.takeover.reason}`}
                >
                  takeover
                </Badge>
              )}
              {p.pinned === s.n && <StatusTag status="progress">browsing</StatusTag>}
              <time dateTime={s.createdAt} className="text-xs text-muted">
                {formatRelative(s.createdAt)}
              </time>
            </span>
            <span className="text-xs text-muted">
              HEAD {shortOid(s.head)} · {s.changedCount.toLocaleString('en-US')} changed · by{' '}
              {s.principal} on {s.writer}
            </span>
            <span>
              <Button
                size="sm"
                variant="quiet"
                aria-label={`Browse files at snapshot ${s.n}`}
                onClick={() => p.onBrowse(s.n)}
              >
                Browse files
              </Button>
            </span>
          </li>
        ))}
      </ol>
      {loading && (
        <p className="m-0 text-sm text-muted" role="status">
          Loading snapshots…
        </p>
      )}
      {next && !loading && (
        <Button size="sm" variant="quiet" onClick={() => void load(next)}>
          Older snapshots
        </Button>
      )}
    </section>
  );
}
