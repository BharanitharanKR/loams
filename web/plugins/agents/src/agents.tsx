import { useMutation, useQuery } from '@connectrpc/connect-query';
import {
  ACTIONS,
  errorText,
  Form,
  iso,
  LoadError,
  Loading,
  Page,
  PageHead,
  RouteLink,
  useAccount,
  useNotify,
  useTrail,
} from '@loams/admin-ui';
import type { RouterService, SessionService } from '@loams/console-host';
import { admin } from '@loams/proto';
import {
  Avatar,
  Button,
  Card,
  Checkbox,
  DataTable,
  Dialog,
  Empty,
  Field,
  formatDuration,
  formatRelative,
  Input,
  Primary,
  SelectMenu,
  StatusTag,
  Textarea,
} from '@loams/ui';
import { Plus } from 'lucide-react';
import { type FormEvent, useState } from 'react';

type Agent = admin.Agent;

export function AgentStatusTag({ status }: { status: admin.AgentStatus }) {
  return status === admin.AgentStatus.SUSPENDED ? (
    <StatusTag status="failed">Suspended</StatusTag>
  ) : (
    <StatusTag status="done">Active</StatusTag>
  );
}

export function AgentsPage({
  project,
  router,
  session,
}: {
  project: string;
  router: RouterService;
  session: SessionService;
}) {
  const orgName = useAccount(session)?.org?.name ?? 'Organization';
  const projects = useQuery(admin.ProjectService.method.listProjects, {});
  const p = projects.data?.projects.find((x) => x.slug === project);
  useTrail(router, [
    { label: orgName, href: '/' },
    { label: p?.name ?? project, href: `/projects/${project}` },
    { label: 'Agents' },
  ]);
  const agents = useQuery(admin.AgentService.method.listAgents, { project });
  const notify = useNotify();
  const [creating, setCreating] = useState(false);

  return (
    <Page>
      <PageHead
        eyebrow={p?.name}
        title="Agents"
        actions={
          <Button variant="primary" onClick={() => setCreating(true)}>
            <Plus size={15} aria-hidden="true" /> Register agent
          </Button>
        }
      >
        Agents never hold long-lived secrets. Each one gets short-lived tokens, scoped by its
        policy, through workload identity, a user's delegation, or a parent token.
      </PageHead>
      <Card flush>
        {agents.error ? (
          <div className="pad">
            <LoadError error={agents.error} retry={() => void agents.refetch()} />
          </div>
        ) : !agents.data ? (
          <Loading />
        ) : (
          <DataTable<Agent>
            rows={agents.data.agents}
            rowKey={(a) => a.id}
            onRowClick={(a) => router.navigate(`/projects/${project}/agents/${a.id}`)}
            empty={
              <div className="pad">
                <Empty
                  title="No agents yet"
                  actions={
                    <Button variant="primary" size="sm" onClick={() => setCreating(true)}>
                      Register agent
                    </Button>
                  }
                >
                  Register an agent to give it a policy and let it exchange its identity for tokens.
                </Empty>
              </div>
            }
            columns={[
              {
                key: 'name',
                header: 'Agent',
                sortValue: (a) => a.name,
                cell: (a) => (
                  <RouteLink to={`/projects/${project}/agents/${a.id}`} className="row-link">
                    <Avatar name={a.name} kind="agent" size={28} />
                    <Primary title={a.name} detail={a.description} />
                  </RouteLink>
                ),
              },
              {
                key: 'status',
                header: 'Status',
                sortValue: (a) => a.status,
                cell: (a) => <AgentStatusTag status={a.status} />,
              },
              {
                key: 'envs',
                header: 'Environments',
                cell: (a) => (
                  <span className="chips">
                    {a.policy.map((r) => (
                      <span key={r.environment} className="env-chip">
                        <span className="env-dot" data-slug={r.environment} aria-hidden="true" />
                        {r.environment}
                      </span>
                    ))}
                  </span>
                ),
              },
              {
                key: 'ttl',
                header: 'Token TTL',
                sortValue: (a) => a.maxTokenTtlS,
                cell: (a) => formatDuration(a.maxTokenTtlS),
              },
              {
                key: 'tokens',
                header: 'Live tokens',
                numeric: true,
                sortValue: (a) => a.activeTokens,
                cell: (a) => a.activeTokens,
              },
              {
                key: 'seen',
                header: 'Last active',
                sortValue: (a) => iso(a.lastActiveAt) ?? '',
                cell: (a) => {
                  const at = iso(a.lastActiveAt);
                  return at ? formatRelative(at) : <span className="muted">never</span>;
                },
              },
            ]}
          />
        )}
      </Card>
      <RegisterAgent
        project={project}
        session={session}
        open={creating}
        onClose={() => setCreating(false)}
        onCreated={(a) => {
          setCreating(false);
          notify(`Registered ${a.name}`);
          void agents.refetch();
        }}
      />
    </Page>
  );
}

const TTLS = [
  { value: '300', label: '5 minutes' },
  { value: '900', label: '15 minutes' },
  { value: '1800', label: '30 minutes' },
  { value: '3600', label: '1 hour' },
];
/** The agent's owner when no team is picked: the signed-in person. */
const ME = 'me';

function RegisterAgent({
  project,
  session,
  open,
  onClose,
  onCreated,
}: {
  project: string;
  session: SessionService;
  open: boolean;
  onClose: () => void;
  onCreated: (a: Agent) => void;
}) {
  const account = useAccount(session);
  const envs = useQuery(admin.ProjectService.method.listEnvironments, { project });
  const teams = useQuery(admin.OrgService.method.listTeams, {});
  const create = useMutation(admin.AgentService.method.createAgent);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [owner, setOwner] = useState(ME);
  const [envChoice, setEnv] = useState<string>();
  const loadedEnvs = envs.data?.environments ?? [];
  // Default to an environment that exists, preferring an unprotected one.
  const env = envChoice ?? (loadedEnvs.find((e) => !e.protected) ?? loadedEnvs[0])?.slug ?? '';
  const [actions, setActions] = useState<string[]>(['query']);
  const [ttl, setTtl] = useState('900');
  const [error, setError] = useState<string>();

  const submit = async (ev: FormEvent) => {
    ev.preventDefault();
    if (!/^[a-z0-9][a-z0-9-]{1,62}$/.test(name))
      return setError('Use lowercase letters, digits and dashes, like docs-qa.');
    if (!actions.length) return setError('Allow at least one action.');
    if (!env) return setError('Pick an environment.');
    const team = teams.data?.teams.find((t) => t.id === owner);
    try {
      const r = await create.mutateAsync({
        project,
        name,
        description,
        owner: team
          ? { kind: admin.SubjectKind.TEAM, id: team.id }
          : { kind: admin.SubjectKind.USER, id: account?.user?.id ?? '' },
        policy: [{ environment: env, actions, collections: [] }],
        maxTokenTtlS: Number(ttl),
        limits: { requestsPerS: 20, concurrentQueries: 4 },
      });
      if (r.agent) onCreated(r.agent);
    } catch (e) {
      setError(errorText(e));
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Register agent"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="new-agent" disabled={create.isPending}>
            Register agent
          </Button>
        </>
      }
    >
      <Form id="new-agent" onSubmit={submit}>
        <Field label="Name" error={error}>
          {(p) => (
            <Input
              {...p}
              value={name}
              onChange={(e) => setName(e.target.value.toLowerCase())}
              placeholder="docs-qa"
              autoFocus
            />
          )}
        </Field>
        <Field label="What it does">
          {(p) => (
            <Textarea
              {...p}
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={2}
            />
          )}
        </Field>
        <Field label="Owner" hint="Accountable for the agent and notified when it is denied.">
          {() => (
            <SelectMenu
              label="Owner"
              value={owner}
              onValueChange={setOwner}
              options={[
                { value: ME, label: `Me (${account?.user?.name ?? 'you'})` },
                ...(teams.data?.teams ?? []).map((t) => ({
                  value: t.id,
                  label: `Team: ${t.name}`,
                })),
              ]}
            />
          )}
        </Field>
        <Field label="Environment" hint="Start in development; widen the policy later.">
          {() => (
            <SelectMenu
              label="Environment"
              value={env || null}
              onValueChange={setEnv}
              options={loadedEnvs.map((e) => ({
                value: e.slug,
                label: `${e.name}${e.protected ? ' (protected)' : ''}`,
              }))}
            />
          )}
        </Field>
        <fieldset className="checks">
          <legend>Actions</legend>
          {ACTIONS.map((a) => (
            <Checkbox
              key={a.id}
              label={a.label}
              checked={actions.includes(a.id)}
              onChange={(e) =>
                setActions((s) => (e.target.checked ? [...s, a.id] : s.filter((x) => x !== a.id)))
              }
            />
          ))}
        </fieldset>
        <Field
          label="Token lifetime"
          hint="The longest any of its tokens may live. Shorter is safer."
        >
          {() => (
            <SelectMenu label="Token lifetime" value={ttl} onValueChange={setTtl} options={TTLS} />
          )}
        </Field>
      </Form>
    </Dialog>
  );
}
