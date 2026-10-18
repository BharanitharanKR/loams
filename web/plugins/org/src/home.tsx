import { createClient } from '@connectrpc/connect';
import { useQuery, useTransport } from '@connectrpc/connect-query';
import {
  AuditTable,
  LoadError,
  Loading,
  num,
  ORG_ROLE,
  Page,
  PageHead,
  RouteLink,
  useAccount,
  useTrail,
} from '@loams/admin-ui';
import type { RouterService, SessionService } from '@loams/console-host';
import { admin } from '@loams/proto';
import { Badge, Card, Empty, formatBytes, formatNumber, Stat, Stats } from '@loams/ui';
import { useQueries } from '@tanstack/react-query';
import { Bot, Lock } from 'lucide-react';
import { type CSSProperties, useMemo } from 'react';

/** The org's front page: every project, its environments, and what just happened. */
export function Home({
  router,
  session,
  auditPath,
}: {
  router: RouterService;
  session: SessionService;
  auditPath: string;
}) {
  const account = useAccount(session);
  const orgName = account?.org?.name ?? 'Organization';
  useTrail(router, [{ label: orgName }]);
  const transport = useTransport();
  const clients = useMemo(
    () => ({
      projects: createClient(admin.ProjectService, transport),
      agents: createClient(admin.AgentService, transport),
    }),
    [transport],
  );
  const projects = useQuery(admin.ProjectService.method.listProjects, {});
  const list = projects.data?.projects ?? [];
  // Every project's environments and agents, for the totals and the project cards.
  const envs = useQueries({
    queries: list.map((p) => ({
      queryKey: ['org-home', 'environments', p.slug],
      queryFn: () => clients.projects.listEnvironments({ project: p.slug }),
    })),
  });
  const agents = useQueries({
    queries: list.map((p) => ({
      queryKey: ['org-home', 'agents', p.slug],
      queryFn: () => clients.agents.listAgents({ project: p.slug }),
    })),
  });
  const audit = useQuery(admin.AuditService.method.listAuditEvents, { pageSize: 8 });

  // Ready once the project list is in and every project's query answered: an org with
  // no projects is ready at once (B7: the tiles stayed at "…" forever).
  const envsReady = projects.data !== undefined && envs.every((q) => q.data);
  const envError = envs.find((q) => q.error)?.error ?? projects.error;
  const agentsReady = projects.data !== undefined && agents.every((q) => q.data);
  const all = envs.flatMap((q) => q.data?.environments ?? []);
  const sum = (f: (e: admin.Environment) => bigint | undefined) =>
    all.reduce((n, e) => n + num(f(e)), 0);
  // The engine does not measure environments yet (their stats are all zero), and data
  // written straight to a namespace is in no environment: a 0 would read as "empty", so
  // the tiles show a dash until some environment reports a number.
  const reported = all.some(
    (e) =>
      num(e.stats?.documents) > 0 ||
      num(e.stats?.storageBytes) > 0 ||
      num(e.stats?.queries24h) > 0,
  );
  const stat = (text: string) => (envsReady ? (reported ? text : <NotReported />) : '…');
  const allAgents = agents.flatMap((q) => q.data?.agents ?? []);
  const liveTokens = allAgents.reduce((n, a) => n + a.activeTokens, 0);

  return (
    <Page>
      <PageHead title={orgName} eyebrow="Organization">
        {projects.data ? list.length : '…'} projects, {envsReady ? all.length : '…'} environments.
        Each environment is one namespace in your bucket.
      </PageHead>

      <Stats>
        <Stat
          label="Documents"
          value={stat(formatNumber(sum((e) => e.stats?.documents)))}
          detail={list.length > 0 ? 'Across every environment' : 'No project environments yet'}
        />
        <Stat
          label="Storage"
          value={stat(formatBytes(sum((e) => e.stats?.storageBytes)))}
          detail="In object storage"
        />
        <Stat
          label="Queries, 24 h"
          value={stat(formatNumber(sum((e) => e.stats?.queries24h)))}
        />
        <Stat
          label="Agent tokens live"
          value={agentsReady ? liveTokens : '…'}
          detail={
            agentsReady
              ? `${allAgents.filter((a) => a.status === admin.AgentStatus.ACTIVE).length} active agents`
              : undefined
          }
        />
      </Stats>

      <h2 className="section-title">Projects</h2>
      {envError && (
        <LoadError
          error={envError}
          retry={() => {
            void projects.refetch();
            for (const q of envs) void q.refetch();
          }}
        />
      )}
      {projects.isPending ? (
        <Loading />
      ) : projects.data && list.length === 0 ? (
        <Empty title="No projects yet">
          A project groups environments, each one namespace in your bucket, with their agents and
          keys. Data written straight to a namespace (with the SDK, the CLI or a wire protocol) does
          not need one: open it in the <RouteLink to="/data">Data explorer</RouteLink> or under{' '}
          <RouteLink to="/namespaces">Namespaces</RouteLink>.
        </Empty>
      ) : (
        <div className="project-grid">
          {list.map((p, i) => (
            <RouteLink
              key={p.slug}
              to={`/projects/${p.slug}`}
              className="project-card"
              style={{ '--i': i } as CSSProperties}
            >
              <span className="project-card-head">
                <strong>{p.name}</strong>
                <span className="project-agents">
                  <Bot size={14} aria-hidden="true" /> {p.agentCount}
                </span>
              </span>
              <span className="project-desc">{p.description}</span>
              <span className="project-envs">
                {(envs[i]?.data?.environments ?? []).map((e) => (
                  <span key={e.slug} className="env-chip">
                    <span className="env-dot" data-slug={e.slug} aria-hidden="true" />
                    {e.name}
                    {e.protected && <Lock size={11} aria-label="protected" />}
                    <span className="env-chip-n">{formatNumber(num(e.stats?.documents))}</span>
                  </span>
                ))}
              </span>
            </RouteLink>
          ))}
        </div>
      )}

      <div style={{ marginTop: 28 }}>
        <Card
          title="Recent activity"
          actions={
            <RouteLink to={auditPath} className="card-link">
              Audit log
            </RouteLink>
          }
          flush
        >
          {audit.isPending ? (
            <Loading />
          ) : audit.error ? (
            <div className="pad">
              <LoadError error={audit.error} retry={() => void audit.refetch()} />
            </div>
          ) : (
            <AuditTable events={audit.data?.events ?? []} />
          )}
        </Card>
      </div>
      {account?.user && (
        <p className="footnote">
          Signed in as {account.user.name} <Badge>{ORG_ROLE[account.role] ?? 'member'}</Badge>
        </p>
      )}
    </Page>
  );
}

const NOT_REPORTED =
  'Not reported: this server does not measure environments yet. Each namespace shows its own size under Namespaces.';

/** A tile's value the server does not report: a dash, with why on hover. */
function NotReported() {
  return (
    <span title={NOT_REPORTED} aria-label={`Not reported. ${NOT_REPORTED}`}>
      —
    </span>
  );
}
