// Starting records for the mock control plane: one project in the `default` namespace with
// a small branch tree, its roles and databases, and a finished operation. No passwords:
// the mock makes one only when a role is created or reset, and keeps none.

import type { PgBranch, PgDatabase, PgOperation, PgProject, PgRole } from './types.js';

export interface MockState {
  projects: PgProject[];
  branches: PgBranch[];
  roles: PgRole[];
  databases: PgDatabase[];
  operations: PgOperation[];
}

const T0 = '2026-10-01T09:00:00Z';
const T1 = '2026-10-03T14:30:00Z';
const T2 = '2026-10-06T08:15:00Z';

const wal = (lsn: string) => ({
  commitLsn: lsn,
  flushLsn: lsn,
  remoteConsistentLsn: lsn,
  backupLsn: lsn,
});

const br = (b: Partial<PgBranch> & Pick<PgBranch, 'id' | 'name'>): PgBranch => ({
  projectId: 'prj-shop',
  parentId: '',
  parentLsn: '',
  isDefault: false,
  protected: false,
  state: 'ready',
  wal: wal('0/3A1C2F0'),
  logicalSizeBytes: 48_000_000,
  createTime: T0,
  updateTime: T0,
  version: 1,
  ...b,
});

export function fixtureState(): MockState {
  const branches = [
    br({ id: 'br-main', name: 'main', isDefault: true, protected: true, version: 3 }),
    br({
      id: 'br-dev',
      name: 'dev',
      parentId: 'br-main',
      parentLsn: '0/2F00A10',
      parentTime: T1,
      createTime: T1,
      updateTime: T1,
      wal: wal('0/3100B88'),
    }),
    br({
      id: 'br-feature',
      name: 'feature-checkout',
      parentId: 'br-dev',
      parentLsn: '0/30FF020',
      parentTime: T2,
      createTime: T2,
      updateTime: T2,
      wal: wal('0/3101C40'),
    }),
    br({
      id: 'br-preview',
      name: 'preview-42',
      parentId: 'br-main',
      parentLsn: '0/3A00000',
      parentTime: T2,
      createTime: T2,
      updateTime: T2,
      expireTime: '2026-10-13T08:15:00Z',
    }),
  ];
  const roles: PgRole[] = [];
  const databases: PgDatabase[] = [];
  for (const b of branches) {
    roles.push({ name: 'loams_admin', branchId: b.id, system: true, createTime: b.createTime });
    roles.push({ name: 'app', branchId: b.id, system: false, createTime: b.createTime });
    databases.push({
      name: 'postgres',
      branchId: b.id,
      ownerRole: 'loams_admin',
      createTime: b.createTime,
    });
    databases.push({ name: 'shop', branchId: b.id, ownerRole: 'app', createTime: b.createTime });
  }
  return {
    projects: [
      {
        id: 'prj-shop',
        namespace: 'default',
        name: 'shop',
        pgVersion: 17,
        region: 'local',
        historyRetentionSeconds: 7 * 24 * 3600,
        defaultBranchId: 'br-main',
        walPool: 'default',
        state: 'ready',
        createTime: T0,
        updateTime: T2,
        version: 4,
      },
    ],
    branches,
    roles,
    databases,
    operations: [
      {
        id: 'op-seed-1',
        kind: 'postgres.branch.create',
        namespace: 'default',
        target: { project: 'prj-shop', branch: 'br-preview' },
        state: 'succeeded',
        progress: { done: 2, total: 2, unit: 'steps', phase: 'done' },
        createdAt: T2,
        updatedAt: T2,
      },
    ],
  };
}
