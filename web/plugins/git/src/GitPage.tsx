// `/git`: Loams Git (§36 §19). Repositories on the left; for the chosen repository, a
// Workspaces tab (a searchable, virtualised list of possibly millions of workspaces and
// the chosen workspace's detail) and an Env secrets tab (§19.13).
//
// The page runs against the GitClient interface (client.ts). Until `loams.repos.v1` is
// served, that client is the fixture-backed mock, and the page says so.

import { NamespacePicker, PageHead } from '@loams/desktop-ui';
import { Badge, Notice } from '@loams/ui';
import { useCallback, useState } from 'react';
import type { GitClient, Repo, WorkspaceSummary } from './client.js';
import { EnvView } from './EnvView.js';
import { RepoList } from './RepoList.js';
import { Tabs } from './Tabs.js';
import { WorkspaceDetail } from './WorkspaceDetail.js';
import { WorkspaceList } from './WorkspaceList.js';

const NAMESPACES_KEY = 'loams.git.namespaces';

type RepoTab = 'workspaces' | 'env';

export interface GitPageProps {
  client: GitClient;
  /** The client answers from fixtures; the page shows a notice. */
  preview?: boolean;
  namespace?: string;
  /** Height of the workspace list viewport, in pixels. */
  listHeight?: number;
  /** Search debounce, in milliseconds (tests pass 0). */
  debounceMs?: number;
}

export function GitPage(p: GitPageProps) {
  const [ns, setNs] = useState(p.namespace ?? 'default');
  const [repo, setRepo] = useState<Repo>();
  const [tab, setTab] = useState<RepoTab>('workspaces');
  const [ws, setWs] = useState<WorkspaceSummary>();

  // A different namespace drops the repository and workspace chosen in the last one.
  const selectNs = useCallback((next: string) => {
    setNs(next);
    setRepo(undefined);
    setWs(undefined);
  }, []);

  const selectRepo = useCallback((r: Repo) => {
    setRepo(r);
    setWs(undefined);
    setTab('workspaces');
  }, []);

  return (
    <div className="lc-page">
      <PageHead
        title="Git"
        subtitle="Repositories, virtual workspaces and env secrets on the bucket."
        badge={
          p.preview ? <Badge title="Answers from built-in fixtures">preview</Badge> : undefined
        }
      />
      {p.preview && (
        <Notice title="Preview data">
          This server does not serve loams.repos.v1 yet. The page shows built-in example data.
        </Notice>
      )}
      <NamespacePicker ns={ns} onOpen={selectNs} storageKey={NAMESPACES_KEY} idPrefix="git" />
      <div className="grid items-start gap-6 xl:grid-cols-[220px_minmax(0,1fr)]">
        <RepoList client={p.client} namespace={ns} selected={repo?.repoId} onSelect={selectRepo} />
        {repo ? (
          <section className="flex min-w-0 flex-col gap-3" aria-label={`Repository ${repo.name}`}>
            <h2 className="m-0 text-base">
              <span className={/* mono-ok: id */ 'font-mono'}>{repo.name}</span>
              <span className="ml-2 text-sm font-normal text-muted">in {ns}</span>
            </h2>
            <Tabs<RepoTab>
              label={`${repo.name} views`}
              value={tab}
              onChange={setTab}
              tabs={[
                { key: 'workspaces', label: 'Workspaces' },
                { key: 'env', label: 'Env secrets' },
              ]}
            >
              {tab === 'workspaces' ? (
                <div className="grid min-w-0 items-start gap-4 2xl:grid-cols-[360px_minmax(0,1fr)]">
                  <WorkspaceList
                    key={repo.repoId}
                    client={p.client}
                    namespace={ns}
                    repoId={repo.repoId}
                    selected={ws?.wsId}
                    onSelect={setWs}
                    height={p.listHeight}
                    debounceMs={p.debounceMs}
                  />
                  {ws ? (
                    <WorkspaceDetail
                      client={p.client}
                      namespace={ns}
                      repoId={repo.repoId}
                      wsId={ws.wsId}
                    />
                  ) : (
                    <p className="m-0 text-sm text-muted">
                      Choose a workspace to see its base, changes, snapshots and files.
                    </p>
                  )}
                </div>
              ) : (
                <EnvView client={p.client} namespace={ns} repoId={repo.repoId} />
              )}
            </Tabs>
          </section>
        ) : (
          <p className="m-0 text-sm text-muted">Choose a repository.</p>
        )}
      </div>
    </div>
  );
}
