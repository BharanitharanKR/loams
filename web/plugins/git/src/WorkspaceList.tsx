// The workspaces of a repository (WorkspaceService.ListWorkspaces). A repository can hold
// millions (§19.11), so the page never loads them all: it searches on the server
// (debounced), pages through the results as the person scrolls, and renders only the rows
// in view (VirtualList). A search page can come back short or empty with a continuation
// token: the server scanned its window and found fewer matches. The list keeps paging
// until the token runs out.

import { Badge, Button, Field, formatNumber, formatRelative, Input, Notice } from '@loams/ui';
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  type GitClient,
  type GitFailure,
  isAbort,
  PAGE_SIZE,
  toFailure,
  type WorkspaceSummary,
} from './client.js';
import { VirtualList } from './VirtualList.js';

export const SEARCH_DEBOUNCE_MS = 250;
export const ROW_HEIGHT = 44;
/** Search pages fetched without a person asking, per query, before "Keep searching". */
export const AUTO_SCAN_PAGES = 3;

export function WorkspaceList(p: {
  client: GitClient;
  namespace: string;
  repoId: string;
  selected?: string;
  onSelect: (ws: WorkspaceSummary) => void;
  height?: number;
  debounceMs?: number;
}) {
  const { client, namespace, repoId } = p;
  const debounce = p.debounceMs ?? SEARCH_DEBOUNCE_MS;
  const [draft, setDraft] = useState('');
  const [query, setQuery] = useState('');
  const [items, setItems] = useState<WorkspaceSummary[]>([]);
  const [total, setTotal] = useState<number>();
  const [next, setNext] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<GitFailure>();
  // The in-flight request; a new query or repository aborts it.
  const inflight = useRef<AbortController | null>(null);
  // Continuations fetched on their own for the current query (each one is a server scan).
  const [autoPages, setAutoPages] = useState(0);

  useEffect(() => {
    const t = setTimeout(() => setQuery(draft.trim()), debounce);
    return () => clearTimeout(t);
  }, [draft, debounce]);

  const fetchPage = useCallback(
    async (token: string) => {
      inflight.current?.abort();
      const ac = new AbortController();
      inflight.current = ac;
      setLoading(true);
      setError(undefined);
      try {
        const res = await client.listWorkspaces(
          { namespace, repoId, query, pageToken: token, pageSize: PAGE_SIZE },
          ac.signal,
        );
        setItems((prev) => (token ? [...prev, ...res.items] : res.items));
        setTotal(res.totalSize);
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
    [client, namespace, repoId, query],
  );

  useEffect(() => {
    setItems([]);
    setNext('');
    setTotal(undefined);
    setAutoPages(0);
    void fetchPage('');
    return () => inflight.current?.abort();
  }, [fetchPage]);

  // A sparse search can return a short page; keep scanning while it shows too few rows.
  const height = p.height ?? 480;
  const shown = Math.ceil(height / ROW_HEIGHT);
  // Bounded: after AUTO_SCAN_PAGES the person decides whether to keep scanning.
  const scanMore = Boolean(query && next && !error && items.length < shown);
  const scanPaused = scanMore && autoPages >= AUTO_SCAN_PAGES;
  useEffect(() => {
    if (!scanMore || loading || inflight.current || autoPages >= AUTO_SCAN_PAGES) return;
    setAutoPages((n) => n + 1);
    void fetchPage(next);
  }, [scanMore, loading, autoPages, next, fetchPage]);

  const onEndReached = useCallback(() => {
    if (next && !inflight.current) void fetchPage(next);
  }, [next, fetchPage]);

  const status = loading
    ? 'Searching…'
    : query
      ? `${formatNumber(items.length)} match${items.length === 1 ? '' : 'es'}${next ? ' so far' : ''}`
      : total !== undefined
        ? `${formatNumber(total)} workspaces`
        : '';

  return (
    <section className="flex flex-col gap-2" aria-label="Workspaces">
      <search>
        <form onSubmit={(e) => e.preventDefault()}>
          <Field
            label="Search workspaces"
            hint="Name, owner or workspace id. Searched on the server."
          >
            {(f) => (
              <Input
                {...f}
                type="search"
                value={draft}
                autoComplete="off"
                spellCheck={false}
                onChange={(e) => setDraft(e.target.value)}
              />
            )}
          </Field>
        </form>
      </search>
      <p className="m-0 text-xs text-muted" role="status" aria-live="polite">
        {status}
      </p>
      {error && (
        <Notice tone="danger" title="Could not list workspaces">
          {error.message}
        </Notice>
      )}
      {scanPaused && !loading && (
        <Button size="sm" variant="quiet" onClick={() => setAutoPages(0)}>
          Keep searching
        </Button>
      )}
      {!loading && !error && items.length === 0 && !next ? (
        <p className="m-0 text-sm text-muted">
          {query ? `No workspaces match “${query}”.` : 'No workspaces in this repository yet.'}
        </p>
      ) : (
        <VirtualList
          items={items}
          total={query ? undefined : total}
          rowHeight={ROW_HEIGHT}
          height={height}
          label="Workspaces"
          getKey={(w) => w.wsId}
          selectedKey={p.selected}
          onSelect={p.onSelect}
          onEndReached={onEndReached}
          renderRow={(w) => <WorkspaceRow ws={w} />}
        />
      )}
    </section>
  );
}

function WorkspaceRow({ ws }: { ws: WorkspaceSummary }) {
  return (
    <span className="flex min-w-0 flex-1 flex-col">
      <span className="flex min-w-0 items-center gap-2">
        <span className={/* mono-ok: id */ 'truncate font-mono text-sm text-ink'}>
          {ws.name || ws.wsId}
        </span>
        {ws.agent && <Badge>agent</Badge>}
      </span>
      <span className="truncate text-xs text-muted">
        {ws.owner} · @{ws.n} · {ws.changedCount.toLocaleString('en-US')} changed ·{' '}
        {formatRelative(ws.updatedAt)}
      </span>
    </span>
  );
}
