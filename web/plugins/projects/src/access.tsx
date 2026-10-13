import { useMutation, useQuery } from '@connectrpc/connect-query';
import {
  avatarKind,
  errorText,
  Form,
  iso,
  LoadError,
  Loading,
  Page,
  PageHead,
  PROJECT_ROLE,
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
  DataTable,
  Dialog,
  Empty,
  Field,
  formatDate,
  formatRelative,
  Primary,
  SelectMenu,
} from '@loams/ui';
import { Plus } from 'lucide-react';
import { type FormEvent, useState } from 'react';

export function AccessPage({
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
    { label: 'Access' },
  ]);
  const grants = useQuery(admin.ProjectService.method.listAccess, { project });
  const sas = useQuery(admin.AgentService.method.listServiceAccounts, { project });
  const [granting, setGranting] = useState(false);

  return (
    <Page>
      <PageHead
        eyebrow={p?.name}
        title="Access"
        actions={
          <Button variant="primary" onClick={() => setGranting(true)}>
            <Plus size={15} aria-hidden="true" /> Grant access
          </Button>
        }
      >
        Roles apply to every environment in the project. Protected environments also need the admin
        role for destructive actions.
      </PageHead>
      <Card title="People and teams" flush>
        {grants.error ? (
          <div className="pad">
            <LoadError error={grants.error} retry={() => void grants.refetch()} />
          </div>
        ) : !grants.data ? (
          <Loading />
        ) : (
          <DataTable<admin.Grant>
            rows={grants.data.grants}
            rowKey={(g) => g.id}
            columns={[
              {
                key: 'who',
                header: 'Who',
                sortValue: (g) => g.subject?.name ?? '',
                cell: (g) => (
                  <span className="who">
                    <Avatar
                      name={g.subject?.name ?? ''}
                      kind={avatarKind(g.subject?.kind)}
                      size={26}
                    />
                    <Primary
                      title={g.subject?.name}
                      detail={g.subject?.kind === admin.SubjectKind.TEAM ? 'Team' : 'Member'}
                    />
                  </span>
                ),
              },
              {
                key: 'role',
                header: 'Role',
                sortValue: (g) => g.role,
                cell: (g) => <Badge>{PROJECT_ROLE[g.role] ?? 'Viewer'}</Badge>,
              },
              {
                key: 'since',
                header: 'Since',
                sortValue: (g) => iso(g.createdAt) ?? '',
                cell: (g) => {
                  const at = iso(g.createdAt);
                  return at ? formatDate(at) : '';
                },
              },
            ]}
          />
        )}
      </Card>
      <Card title="Service accounts" flush style={{ marginTop: 16 }}>
        <DataTable<admin.ServiceAccount>
          rows={sas.data?.serviceAccounts ?? []}
          rowKey={(s) => s.id}
          empty={
            <div className="pad">
              <Empty title="No service accounts" grain="silt" seed={5}>
                For CI jobs and batch workloads that are not agents. They can federate, or hold API
                keys.
              </Empty>
            </div>
          }
          columns={[
            {
              key: 'name',
              header: 'Service account',
              sortValue: (s) => s.name,
              cell: (s) => (
                <span className="who">
                  <Avatar name={s.name} kind="service_account" size={26} />
                  <Primary title={s.name} detail={s.description} />
                </span>
              ),
            },
            {
              key: 'keys',
              header: 'Keys',
              numeric: true,
              sortValue: (s) => s.keyCount,
              cell: (s) => s.keyCount,
            },
            {
              key: 'seen',
              header: 'Last active',
              cell: (s) => {
                const at = iso(s.lastActiveAt);
                return at ? formatRelative(at) : <span className="muted">never</span>;
              },
            },
          ]}
        />
      </Card>
      <GrantDialog
        project={project}
        open={granting}
        onClose={() => setGranting(false)}
        onDone={() => void grants.refetch()}
      />
    </Page>
  );
}

const ROLES = [
  { value: 'admin', label: 'Admin', role: admin.ProjectRole.ADMIN },
  { value: 'developer', label: 'Developer', role: admin.ProjectRole.DEVELOPER },
  { value: 'viewer', label: 'Viewer', role: admin.ProjectRole.VIEWER },
] as const;

type RoleValue = (typeof ROLES)[number]['value'];

function GrantDialog({
  project,
  open,
  onClose,
  onDone,
}: {
  project: string;
  open: boolean;
  onClose: () => void;
  onDone: () => void;
}) {
  const teams = useQuery(admin.OrgService.method.listTeams, {});
  const members = useQuery(admin.OrgService.method.listMembers, {});
  const grant = useMutation(admin.ProjectService.method.grantAccess);
  const notify = useNotify();
  const [subject, setSubject] = useState('');
  const [role, setRole] = useState<RoleValue>('developer');
  const [error, setError] = useState<string>();
  const submit = async (ev: FormEvent) => {
    ev.preventDefault();
    const [kind, id] = subject.split(':');
    if (!id || (kind !== 'team' && kind !== 'user')) return setError('Pick a team or a member.');
    const chosen = ROLES.find((r) => r.value === role) ?? ROLES[1];
    try {
      await grant.mutateAsync({
        project,
        subject: {
          kind: kind === 'team' ? admin.SubjectKind.TEAM : admin.SubjectKind.USER,
          id,
        },
        role: chosen.role,
      });
      notify(`Granted ${chosen.label.toLowerCase()} on ${project}`);
      onDone();
      onClose();
    } catch (e) {
      setError(errorText(e));
    }
  };
  const options = [
    ...(teams.data?.teams ?? []).map((t) => ({ value: `team:${t.id}`, label: `Team: ${t.name}` })),
    ...(members.data?.members ?? []).map((m) => ({
      value: `user:${m.user?.id ?? ''}`,
      label: m.user?.name ?? '',
    })),
  ];
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Grant access"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="grant" disabled={grant.isPending}>
            Grant access
          </Button>
        </>
      }
    >
      <Form id="grant" onSubmit={submit}>
        <Field label="Team or member" error={error}>
          {() => (
            <SelectMenu
              label="Team or member"
              value={subject || null}
              onValueChange={setSubject}
              options={options}
            />
          )}
        </Field>
        <Field
          label="Role"
          hint="Admin manages access and protected environments; viewer only reads."
        >
          {() => (
            <SelectMenu
              label="Role"
              value={role}
              onValueChange={setRole}
              options={ROLES.map((r) => ({ value: r.value, label: r.label }))}
            />
          )}
        </Field>
      </Form>
    </Dialog>
  );
}
