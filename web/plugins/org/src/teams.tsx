import { useMutation, useQuery } from '@connectrpc/connect-query';
import {
  avatarKind,
  errorText,
  Form,
  LoadError,
  Loading,
  Page,
  PageHead,
  PROJECT_ROLE,
  RouteLink,
  useAccount,
  useNotify,
  useTrail,
} from '@loams/admin-ui';
import type { RouterService, SessionService } from '@loams/console-host';
import { admin } from '@loams/proto';
import { Avatar, Badge, Button, Card, DataTable, Dialog, Field, Input, Primary } from '@loams/ui';
import { Plus } from 'lucide-react';
import { useState } from 'react';

export function TeamsPage({ router, session }: { router: RouterService; session: SessionService }) {
  const orgName = useAccount(session)?.org?.name ?? 'Organization';
  useTrail(router, [{ label: orgName, href: '/' }, { label: 'Teams' }]);
  const teams = useQuery(admin.OrgService.method.listTeams, {});
  const create = useMutation(admin.OrgService.method.createTeam);
  const notify = useNotify();
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState('');
  const [group, setGroup] = useState('');
  return (
    <Page>
      <PageHead
        eyebrow="Organization"
        title="Teams"
        actions={
          <Button variant="primary" onClick={() => setCreating(true)}>
            <Plus size={15} aria-hidden="true" /> New team
          </Button>
        }
      >
        Teams receive project roles. A team linked to an SSO group gains and loses members as the
        group does.
      </PageHead>
      <Card flush>
        {teams.error ? (
          <div className="pad">
            <LoadError error={teams.error} retry={() => void teams.refetch()} />
          </div>
        ) : !teams.data ? (
          <Loading />
        ) : (
          <DataTable<admin.Team>
            rows={teams.data.teams}
            rowKey={(t) => t.id}
            onRowClick={(t) => router.navigate(`/teams/${t.id}`)}
            columns={[
              {
                key: 'name',
                header: 'Team',
                sortValue: (t) => t.name,
                cell: (t) => (
                  <RouteLink to={`/teams/${t.id}`} className="row-link">
                    <Avatar name={t.name} kind="team" size={28} />
                    <Primary title={t.name} detail={t.description} />
                  </RouteLink>
                ),
              },
              {
                key: 'members',
                header: 'Members',
                numeric: true,
                sortValue: (t) => t.memberCount,
                cell: (t) => t.memberCount,
              },
              {
                key: 'sso',
                header: 'SSO group',
                cell: (t) =>
                  t.idpGroup ? (
                    <Badge>{t.idpGroup}</Badge>
                  ) : (
                    <span className="muted">not linked</span>
                  ),
              },
            ]}
          />
        )}
      </Card>
      <Dialog
        open={creating}
        onClose={() => setCreating(false)}
        title="New team"
        footer={
          <>
            <Button onClick={() => setCreating(false)}>Cancel</Button>
            <Button
              variant="primary"
              disabled={create.isPending}
              onClick={async () => {
                if (!name.trim()) return;
                setCreating(false);
                try {
                  await create.mutateAsync({ name: name.trim(), idpGroup: group });
                  void teams.refetch();
                  notify(`Created ${name.trim()}`);
                } catch (e) {
                  notify(errorText(e), 'danger');
                }
              }}
            >
              Create team
            </Button>
          </>
        }
      >
        <Form>
          <Field label="Name">
            {(p) => (
              <Input {...p} value={name} onChange={(e) => setName(e.target.value)} autoFocus />
            )}
          </Field>
          <Field label="SSO group" hint="Optional: the OIDC groups claim value to sync from.">
            {(p) => (
              <Input
                {...p}
                value={group}
                onChange={(e) => setGroup(e.target.value)}
                placeholder="acme-search"
              />
            )}
          </Field>
        </Form>
      </Dialog>
    </Page>
  );
}

export function TeamPage({
  team,
  router,
  session,
}: {
  team: string;
  router: RouterService;
  session: SessionService;
}) {
  const orgName = useAccount(session)?.org?.name ?? 'Organization';
  const t = useQuery(admin.OrgService.method.getTeam, { team });
  useTrail(router, [
    { label: orgName, href: '/' },
    { label: 'Teams', href: '/teams' },
    { label: t.data?.team?.name ?? '…' },
  ]);
  if (t.error) {
    return (
      <Page>
        <LoadError error={t.error} retry={() => void t.refetch()} />
      </Page>
    );
  }
  if (!t.data) return <Loading />;
  const { team: info, members, projects } = t.data;
  return (
    <Page>
      <PageHead
        eyebrow={
          <RouteLink to="/teams" className="eyebrow-link">
            Teams
          </RouteLink>
        }
        title={info?.name}
      >
        {info?.description} {info?.idpGroup && <Badge>SSO group {info.idpGroup}</Badge>}
      </PageHead>
      <div className="grid-2">
        <Card title="Members" flush>
          <DataTable<admin.User>
            rows={members}
            rowKey={(u) => u.id}
            columns={[
              {
                key: 'who',
                header: 'Member',
                sortValue: (u) => u.name,
                cell: (u) => (
                  <span className="who">
                    <Avatar name={u.name} kind={avatarKind(admin.SubjectKind.USER)} size={26} />
                    <Primary title={u.name} detail={u.email} />
                  </span>
                ),
              },
            ]}
          />
        </Card>
        <Card title="Projects" flush>
          <DataTable<admin.ProjectGrantRef>
            rows={projects}
            rowKey={(g) => g.project}
            empty={<p className="pad muted">This team has no project roles yet.</p>}
            columns={[
              {
                key: 'p',
                header: 'Project',
                sortValue: (g) => g.project,
                cell: (g) => <RouteLink to={`/projects/${g.project}`}>{g.project}</RouteLink>,
              },
              {
                key: 'r',
                header: 'Role',
                cell: (g) => <Badge>{PROJECT_ROLE[g.role] ?? 'Viewer'}</Badge>,
              },
            ]}
          />
        </Card>
      </div>
    </Page>
  );
}
