// The repositories in a namespace (RepoService.ListRepos), paged.

import { Badge, Button, formatNumber, Notice } from '@loams/ui';
import { useCallback, useEffect, useState } from 'react';
import { type GitClient, type GitFailure, isAbort, type Repo, toFailure } from './client.js';

export function RepoList(p: {
  client: GitClient;
  namespace: string;
  selected?: string;
  onSelect: (r: Repo) => void;
}) {
  const { client, namespace, onSelect } = p;
  const [repos, setRepos] = useState<Repo[]>([]);
  const [next, setNext] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<GitFailure>();

  const load = useCallback(
    async (token: string, signal?: AbortSignal) => {
      setLoading(true);
      setError(undefined);
      try {
        const res = await client.listRepos({ namespace, pageToken: token }, signal);
        setRepos((prev) => (token ? [...prev, ...res.items] : res.items));
        setNext(res.nextPageToken);
      } catch (e) {
        if (isAbort(e)) return;
        setError(toFailure(e));
      } finally {
        if (!signal?.aborted) setLoading(false);
      }
    },
    [client, namespace],
  );

  useEffect(() => {
    const ac = new AbortController();
    setRepos([]);
    void load('', ac.signal);
    return () => ac.abort();
  }, [load]);

  // Select the first repository once, so the page opens on something.
  useEffect(() => {
    if (!p.selected && repos[0]) onSelect(repos[0]);
  }, [p.selected, repos, onSelect]);

  return (
    <nav className="flex flex-col gap-2" aria-label="Repositories">
      <h2 className="m-0 text-base">Repositories</h2>
      {error && (
        <Notice tone="danger" title="Could not list repositories">
          {error.message}
        </Notice>
      )}
      {!loading && !error && repos.length === 0 && (
        <p className="m-0 text-sm text-muted">No repositories in {namespace} yet.</p>
      )}
      <ul className="m-0 flex list-none flex-col gap-1 p-0">
        {repos.map((r) => {
          const on = r.repoId === p.selected;
          return (
            <li key={r.repoId}>
              <button
                type="button"
                aria-current={on ? 'true' : undefined}
                className={`flex w-full cursor-pointer flex-col items-start rounded-md border border-solid px-2 py-1 text-left font-sans ${
                  on ? 'border-accent bg-accent-soft' : 'border-transparent bg-transparent'
                }`}
                onClick={() => onSelect(r)}
              >
                <span className="flex items-center gap-2 font-mono text-sm text-ink">
                  {r.name}
                  {r.hasEnv && <Badge title="This repository has an env/ folder">env</Badge>}
                </span>
                <span className="text-xs text-muted">
                  {formatNumber(r.workspaceCount)} workspaces · seq {r.seq.toLocaleString('en-US')}
                </span>
              </button>
            </li>
          );
        })}
      </ul>
      {loading && (
        <p className="m-0 text-sm text-muted" role="status">
          Loading…
        </p>
      )}
      {next && !loading && (
        <Button size="sm" variant="quiet" onClick={() => void load(next)}>
          Load more
        </Button>
      )}
    </nav>
  );
}
