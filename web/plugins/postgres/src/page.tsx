import type { LoamsDesktopApi } from '@loams/desktop/contracts';
import { ConnectPanel, PageHead, SqlConsole, StackCard, Tabs, useStack } from '@loams/desktop-ui';
import { Badge, Notice } from '@loams/ui';
import { Branches } from './branches.js';
import { ControlPlane } from './control/ControlPlane.js';
import type { PgControlClient } from './control/types.js';

/**
 * `/postgres`: the local Neon stack, and the Loams control plane (loams.postgres.v1:
 * projects, branches, roles, databases, operations) when `control` is given.
 */
export function PostgresPage({
  desktop,
  control,
}: {
  desktop: LoamsDesktopApi;
  control?: PgControlClient;
}) {
  const state = useStack(desktop.stacks, 'postgres');
  const running = state?.phase === 'running';
  const controlTab = control && {
    id: 'control',
    label: 'Control plane',
    content: <ControlPlane client={control} copy={desktop.shell.clipboardWrite} />,
  };
  return (
    <div className="lc-page flex flex-col gap-4">
      <PageHead
        title="Postgres"
        badge={<Badge>Local stack</Badge>}
        subtitle={
          <>
            A Loams control plane will manage this on remote servers.
            <span className="block text-xs text-faint">
              Branches via the local Neon stack; the Loams control plane takes over on servers that
              offer loams.postgres.v1.
            </span>
          </>
        }
      />
      <StackCard desktop={desktop} id="postgres" />
      {running ? (
        <Tabs
          tabs={[
            { id: 'branches', label: 'Branches', content: <Branches pg={desktop.pg} /> },
            {
              id: 'connect',
              label: 'Connect',
              content: (
                <ConnectPanel
                  dialect="postgres"
                  connection={desktop.pg.connection}
                  revealPassword={desktop.pg.revealPassword}
                  copy={desktop.shell.clipboardWrite}
                />
              ),
            },
            {
              id: 'sql',
              label: 'SQL',
              content: <SqlConsole dialect="postgres" run={desktop.pg.query} />,
            },
            ...(controlTab ? [controlTab] : []),
          ]}
        />
      ) : (
        <>
          {state && state.phase !== 'unavailable' && (
            <Notice title="Start the stack to browse branches and run SQL">
              Branches, connection details and the SQL console appear once the local stack is
              running.
            </Notice>
          )}
          {controlTab && <Tabs tabs={[controlTab]} />}
        </>
      )}
    </div>
  );
}
