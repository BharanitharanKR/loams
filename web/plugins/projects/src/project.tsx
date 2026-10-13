import { useMutation, useQuery } from '@connectrpc/connect-query';
import {
  AuditTable,
  errorText,
  Form,
  iso,
  LoadError,
  Loading,
  num,
  Page,
  PageHead,
  RouteLink,
  slugify,
  useAccount,
  useNotify,
  useTrail,
} from '@loams/admin-ui';
import type { RouterService, SessionService } from '@loams/console-host';
import { admin } from '@loams/proto';
import {
  Avatar,
  Badge,
  Button,
  Card,
  Checkbox,
  DataTable,
  Dialog,
  Field,
  formatBytes,
  formatNumber,
  formatRelative,
  Input,
  Primary,
  StatusTag,
} from '@loams/ui';
import { Lock, Plus } from 'lucide-react';
import { type FormEvent, useState } from 'react';

type Environment = admin.Environment;

export function ProjectPage({
  project,
  router,
  session,
  auditPath,
}: {
  project: string;
  router: RouterService;
  session: SessionService;
  auditPath: string;
}) {
  const orgName = useAccount(session)?.org?.name ?? 'Organization';
  const projects = useQuery(admin.ProjectService.method.listProjects, {});
  const p = projects.data?.projects.find((x) => x.slug === project);
  useTrail(router, [{ label: orgName, href: '/' }, { label: p?.name ?? project }]);
  const envs = useQuery(admin.ProjectService.method.listEnvironments, { project });
  const agents = useQuery(admin.AgentService.method.listAgents, { project });
  const audit = useQuery(admin.AuditService.method.listAuditEvents, { project, pageSize: 6 });
  const notify = useNotify();
  const [creating, setCreating] = useState(false);

  if (projects.isPending) return <Loading />;
  if (!p) {
    return (
      <Page>
        <LoadError
          error={projects.error ?? `There is no project ${project} in this org.`}
          retry={projects.error ? () => void projects.refetch() : undefined}
        />
      </Page>
    );
  }

  return (
    <Page>
      <PageHead
        title={p.name}
        eyebrow="Project"
        actions={
          <Button variant="primary" onClick={() => setCreating(true)}>
            <Plus size={15} aria-hidden="true" /> New environment
          </Button>
        }
      >
        {p.description}
      </PageHead>

      <Card title="Environments" flush>
        {envs.isPending ? (
          <Loading />
        ) : envs.error ? (
          <div className="pad">
            <LoadError error={envs.error} retry={() => void envs.refetch()} />
          </div>
        ) : (
          <DataTable<Environment>
            rows={envs.data?.environments ?? []}
            rowKey={(e) => e.slug}
            onRowClick={(e) => router.navigate(`/projects/${project}/environments/${e.slug}`)}
            columns={[
              {
                key: 'name',
                header: 'Environment',
                cell: (e) => (
                  <RouteLink
                    to={`/projects/${project}/environments/${e.slug}`}
                    className="row-link"
                  >
                    <span className="env-dot" data-slug={e.slug} aria-hidden="true" />
                    <Primary
                      title={
                        <>
                          {e.name} {e.protected && <Lock size={12} aria-label="protected" />}
                        </>
                      }
                      detail={e.region}
                    />
                  </RouteLink>
                ),
              },
              { key: 'ns', header: 'Namespace', cell: (e) => <Badge>{e.namespace}</Badge> },
              {
                key: 'cols',
                header: 'Collections',
                numeric: true,
                sortValue: (e) => num(e.stats?.collections),
                cell: (e) => num(e.stats?.collections),
              },
              {
                key: 'docs',
                header: 'Documents',
                numeric: true,
                sortValue: (e) => num(e.stats?.documents),
                cell: (e) => formatNumber(num(e.stats?.documents)),
              },
              {
                key: 'size',
                header: 'Storage',
                numeric: true,
                sortValue: (e) => num(e.stats?.storageBytes),
                cell: (e) => formatBytes(num(e.stats?.storageBytes)),
              },
              {
                key: 'q',
                header: 'Queries, 24 h',
                numeric: true,
                sortValue: (e) => num(e.stats?.queries24h),
                cell: (e) => formatNumber(num(e.stats?.queries24h)),
              },
            ]}
          />
        )}
      </Card>

      <div className="grid-2" style={{ marginTop: 16 }}>
        <Card
          title="Agents"
          actions={
            <RouteLink to={`/projects/${project}/agents`} className="card-link">
              All agents
            </RouteLink>
          }
          flush
        >
          {agents.error ? (
            <div className="pad">
              <LoadError error={agents.error} retry={() => void agents.refetch()} />
            </div>
          ) : (
            <ul className="agent-list">
              {(agents.data?.agents ?? []).map((a) => {
                const active = iso(a.lastActiveAt);
                return (
                  <li key={a.id}>
                    <RouteLink to={`/projects/${project}/agents/${a.id}`}>
                      <Avatar name={a.name} kind="agent" size={28} />
                      <Primary
                        title={a.name}
                        detail={active ? `active ${formatRelative(active)}` : 'never active'}
                      />
                      {a.status === admin.AgentStatus.SUSPENDED ? (
                        <StatusTag status="failed">Suspended</StatusTag>
                      ) : (
                        <Badge>{a.activeTokens} live</Badge>
                      )}
                    </RouteLink>
                  </li>
                );
              })}
            </ul>
          )}
        </Card>
        <Card
          title="Recent activity"
          actions={
            <RouteLink to={auditPath} className="card-link">
              Audit log
            </RouteLink>
          }
          flush
        >
          {audit.error ? (
            <div className="pad">
              <LoadError error={audit.error} retry={() => void audit.refetch()} />
            </div>
          ) : (
            <AuditTable events={audit.data?.events ?? []} showProject={false} />
          )}
        </Card>
      </div>

      <NewEnvironment
        project={project}
        open={creating}
        onClose={() => setCreating(false)}
        onCreated={(e) => {
          setCreating(false);
          notify(`Created ${e.name}, namespace ${e.namespace}`);
          void envs.refetch();
        }}
      />
    </Page>
  );
}

function NewEnvironment({
  project,
  open,
  onClose,
  onCreated,
}: {
  project: string;
  open: boolean;
  onClose: () => void;
  onCreated: (e: Environment) => void;
}) {
  const [name, setName] = useState('');
  const [protect, setProtect] = useState(false);
  const [error, setError] = useState<string>();
  const create = useMutation(admin.ProjectService.method.createEnvironment);
  const slug = slugify(name);

  const submit = async (ev: FormEvent) => {
    ev.preventDefault();
    if (!slug) return setError('Give the environment a name.');
    try {
      const r = await create.mutateAsync({ project, name: name.trim(), slug, protected: protect });
      if (r.environment) onCreated(r.environment);
    } catch (e) {
      setError(errorText(e));
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="New environment"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="new-env" disabled={create.isPending}>
            Create environment
          </Button>
        </>
      }
    >
      <Form id="new-env" onSubmit={submit}>
        <Field
          label="Name"
          hint={slug ? `Namespace ${project}-${slug}` : 'For example Preview or QA'}
          error={error}
        >
          {(p) => <Input {...p} value={name} onChange={(e) => setName(e.target.value)} autoFocus />}
        </Field>
        <Checkbox
          label="Protected: destructive actions need the project admin role"
          checked={protect}
          onChange={(e) => setProtect(e.target.checked)}
        />
      </Form>
    </Dialog>
  );
}
