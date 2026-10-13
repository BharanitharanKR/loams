import { useMutation, useQuery } from '@connectrpc/connect-query';
import {
  errorText,
  Form,
  iso,
  LoadError,
  Loading,
  ORG_ROLE,
  Page,
  PageHead,
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
  Field,
  formatRelative,
  Input,
  Primary,
  SelectMenu,
  StatusTag,
} from '@loams/ui';
import { Plus } from 'lucide-react';
import { useState } from 'react';

const seen = (m: admin.Member) => iso(m.user?.lastSeenAt);

export function MembersPage({
  router,
  session,
}: {
  router: RouterService;
  session: SessionService;
}) {
  const orgName = useAccount(session)?.org?.name ?? 'Organization';
  useTrail(router, [{ label: orgName, href: '/' }, { label: 'Members' }]);
  const members = useQuery(admin.OrgService.method.listMembers, {});
  const invitations = useQuery(admin.OrgService.method.listInvitations, {});
  const invite = useMutation(admin.OrgService.method.createInvitation);
  const notify = useNotify();
  const [inviting, setInviting] = useState(false);
  const [email, setEmail] = useState('');
  const [role, setRole] = useState<'member' | 'admin'>('member');
  const pending = invitations.data?.invitations ?? [];
  return (
    <Page>
      <PageHead
        eyebrow="Organization"
        title="Members"
        actions={
          <Button variant="primary" onClick={() => setInviting(true)}>
            <Plus size={15} aria-hidden="true" /> Invite
          </Button>
        }
      />
      <Card flush>
        {members.error ? (
          <div className="pad">
            <LoadError error={members.error} retry={() => void members.refetch()} />
          </div>
        ) : !members.data ? (
          <Loading />
        ) : (
          <DataTable<admin.Member>
            rows={members.data.members}
            rowKey={(m) => m.user?.id ?? ''}
            columns={[
              {
                key: 'who',
                header: 'Member',
                sortValue: (m) => m.user?.name ?? '',
                cell: (m) => (
                  <span className="who">
                    <Avatar name={m.user?.name ?? ''} size={28} />
                    <Primary title={m.user?.name} detail={m.user?.email} />
                  </span>
                ),
              },
              {
                key: 'role',
                header: 'Role',
                sortValue: (m) => m.role,
                cell: (m) => <Badge>{ORG_ROLE[m.role] ?? 'member'}</Badge>,
              },
              {
                key: 'auth',
                header: 'Sign-in',
                cell: (m) => (
                  <span className="chips">
                    {m.user?.sso ? <Badge>SSO</Badge> : <Badge>Password</Badge>}
                    {m.user?.twoFactor ? (
                      <StatusTag status="done">2FA</StatusTag>
                    ) : (
                      <StatusTag status="progress">No 2FA</StatusTag>
                    )}
                  </span>
                ),
              },
              {
                key: 'teams',
                header: 'Teams',
                numeric: true,
                sortValue: (m) => m.teams.length,
                cell: (m) => m.teams.length,
              },
              {
                key: 'seen',
                header: 'Last seen',
                sortValue: (m) => seen(m) ?? '',
                cell: (m) => {
                  const at = seen(m);
                  return at ? formatRelative(at) : <span className="muted">never</span>;
                },
              },
            ]}
          />
        )}
      </Card>
      {pending.length > 0 && (
        <Card title="Pending invitations" flush style={{ marginTop: 16 }}>
          <DataTable<admin.Invitation>
            rows={pending}
            rowKey={(i) => i.id}
            columns={[
              { key: 'email', header: 'Email', sortValue: (i) => i.email, cell: (i) => i.email },
              {
                key: 'role',
                header: 'Role',
                cell: (i) => <Badge>{ORG_ROLE[i.role] ?? 'member'}</Badge>,
              },
              { key: 'by', header: 'Invited by', cell: (i) => i.invitedBy?.name },
              {
                key: 'exp',
                header: 'Expires',
                cell: (i) => {
                  const at = iso(i.expiresAt);
                  return at ? formatRelative(at) : <span className="muted">never</span>;
                },
              },
            ]}
          />
        </Card>
      )}
      <Dialog
        open={inviting}
        onClose={() => setInviting(false)}
        title="Invite someone"
        footer={
          <>
            <Button onClick={() => setInviting(false)}>Cancel</Button>
            <Button
              variant="primary"
              disabled={invite.isPending}
              onClick={async () => {
                if (!email.includes('@')) return;
                setInviting(false);
                try {
                  await invite.mutateAsync({
                    email,
                    role: role === 'admin' ? admin.OrgRole.ADMIN : admin.OrgRole.MEMBER,
                  });
                  void invitations.refetch();
                  notify(`Invited ${email}`);
                } catch (e) {
                  notify(errorText(e), 'danger');
                }
              }}
            >
              Send invitation
            </Button>
          </>
        }
      >
        <Form>
          <Field label="Email">
            {(p) => (
              <Input
                {...p}
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                autoFocus
              />
            )}
          </Field>
          <Field label="Role">
            {() => (
              <SelectMenu
                label="Role"
                value={role}
                onValueChange={setRole}
                options={[
                  { value: 'member', label: 'Member' },
                  { value: 'admin', label: 'Admin' },
                ]}
              />
            )}
          </Field>
        </Form>
      </Dialog>
    </Page>
  );
}
