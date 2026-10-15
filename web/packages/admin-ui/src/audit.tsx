import { admin } from '@loams/proto';
import { Avatar, Badge, DataTable, formatRelative, StatusTag } from '@loams/ui';
import { avatarKind, iso } from './model.js';

type Event = admin.AuditEvent;

const when = (e: Event) => iso(e.at) ?? '';

/** The audit log as a table: who, acting for whom, did what, to what, where. */
export function AuditTable({
  events,
  showProject = true,
}: {
  events: Event[];
  showProject?: boolean;
}) {
  return (
    <DataTable<Event>
      rows={events}
      rowKey={(e) => e.id}
      caption="Audit events"
      empty={<p className="pad muted">No events yet.</p>}
      columns={[
        {
          key: 'at',
          header: 'When',
          width: '120px',
          sortValue: when,
          cell: (e) => (
            <time
              dateTime={when(e)}
              title={new Date(when(e)).toLocaleString()}
              className="muted nowrap"
            >
              {formatRelative(when(e))}
            </time>
          ),
        },
        {
          key: 'actor',
          header: 'Actor',
          sortValue: (e) => e.actor?.name ?? '',
          cell: (e) => (
            <span className="who">
              <Avatar name={e.actor?.name ?? ''} kind={avatarKind(e.actor?.kind)} size={22} />
              <span>
                {e.actor?.name}
                {e.actingFor?.name && <span className="muted"> for {e.actingFor.name}</span>}
              </span>
            </span>
          ),
        },
        {
          key: 'action',
          header: 'Action',
          sortValue: (e) => e.action,
          cell: (e) => <code className="action">{e.action}</code>,
        },
        {
          key: 'target',
          header: 'Target',
          cell: (e) => (
            <span>
              {e.target?.name}{' '}
              <span className="muted">{(e.target?.kind ?? '').replace('_', ' ')}</span>
            </span>
          ),
        },
        ...(showProject
          ? [
              {
                key: 'where',
                header: 'Where',
                cell: (e: Event) =>
                  e.project ? (
                    <Badge>
                      {e.project}
                      {e.environment ? `/${e.environment}` : ''}
                    </Badge>
                  ) : (
                    <span className="muted">org</span>
                  ),
              },
            ]
          : []),
        {
          key: 'outcome',
          header: 'Outcome',
          cell: (e) =>
            e.outcome === admin.AuditOutcome.DENIED ? (
              <StatusTag status="failed">Denied</StatusTag>
            ) : (
              <StatusTag status="done">OK</StatusTag>
            ),
        },
      ]}
    />
  );
}
