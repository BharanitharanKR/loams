// A fixture-backed SqlClient (client.ts) so the page works before loams.sqldb.v1 lands.
// It keeps state in memory: suspend and resume walk through their transitional stages,
// and a rotated password is returned once. Hosts and passwords are placeholders, never
// real endpoints or secrets.

import {
  type Branch,
  type ConnectionInfo,
  type Database,
  type DatabaseStage,
  type QueryInput,
  type QueryResult,
  type SqlClient,
  SqlFailure,
} from './client.js';

export interface MockFixtures {
  databases: Database[];
  branches: Branch[];
}

export function defaultFixtures(namespace = 'default'): MockFixtures {
  const t = '2026-10-01T09:00:00Z';
  return {
    databases: [
      {
        id: 'db_shop000000000001',
        namespace,
        name: 'shop',
        stage: 'running',
        engineVersion: 'tidb-v8.5.8',
        computeClass: 's',
        suspendAfterSeconds: 300,
        defaultBranchId: 'br_shopmain00000000',
        storageBytes: 48_234_496,
        createTime: t,
      },
      {
        id: 'db_analytics0000001',
        namespace,
        name: 'analytics',
        stage: 'suspended',
        engineVersion: 'tidb-v8.5.8',
        computeClass: 'm',
        suspendAfterSeconds: 300,
        defaultBranchId: 'br_anlmain000000000',
        storageBytes: 1_288_490_188,
        createTime: t,
      },
    ],
    branches: [
      {
        id: 'br_shopmain00000000',
        databaseId: 'db_shop000000000001',
        name: 'main',
        parentId: '',
        branchKind: 'copy',
        pointTs: '',
        stage: 'running',
        createTime: t,
      },
      {
        id: 'br_shopdev000000000',
        databaseId: 'db_shop000000000001',
        name: 'dev',
        parentId: 'br_shopmain00000000',
        branchKind: 'copy',
        pointTs: '462118237114122241',
        stage: 'suspended',
        createTime: '2026-10-05T12:30:00Z',
      },
      {
        id: 'br_anlmain000000000',
        databaseId: 'db_analytics0000001',
        name: 'main',
        parentId: '',
        branchKind: 'copy',
        pointTs: '',
        stage: 'suspended',
        createTime: t,
      },
    ],
  };
}

export interface MockOptions {
  fixtures?: MockFixtures;
  /** Milliseconds a transitional stage lasts; 0 settles on the next read. */
  transitionMs?: number;
  now?: () => number;
}

const SAMPLE: QueryResult = {
  columns: [
    { name: 'id', type: 'BIGINT' },
    { name: 'name', type: 'VARCHAR' },
    { name: 'price', type: 'DECIMAL' },
    { name: 'discontinued_at', type: 'DATETIME' },
  ],
  rows: [
    ['1', 'Tea', '3.50', null],
    ['2', 'Coffee', '4.00', null],
    ['3', 'Cocoa', '3.75', '2026-09-30 00:00:00'],
  ],
  rowsAffected: 0,
  truncated: false,
  elapsedMs: 4,
};

/** A deterministic placeholder password; it only lives in memory. */
function mockPassword(n: number): string {
  return `mock-pw-${n.toString(36).padStart(4, '0')}-not-a-secret`;
}

export function createMockSqlClient(opts: MockOptions = {}): SqlClient {
  const fx = opts.fixtures ?? defaultFixtures();
  const dbs = new Map(fx.databases.map((d) => [d.id, { ...d }]));
  const branches = fx.branches.map((b) => ({ ...b }));
  const transitionMs = opts.transitionMs ?? 1500;
  const now = opts.now ?? (() => Date.now());
  const pending = new Map<string, { until: number; to: DatabaseStage }>();
  const minted = new Set<string>();
  let rotations = 0;

  const settle = (db: Database) => {
    const p = pending.get(db.id);
    if (p && now() >= p.until) {
      db.stage = p.to;
      pending.delete(db.id);
      for (const b of branches)
        if (b.databaseId === db.id && b.id === db.defaultBranchId) b.stage = p.to;
    }
    return db;
  };

  const find = (namespace: string, id: string) => {
    const db = dbs.get(id);
    if (!db || db.namespace !== namespace)
      throw new SqlFailure('database_not_found', `No database ${id} in ${namespace}.`);
    return settle(db);
  };

  const move = (db: Database, via: DatabaseStage, to: DatabaseStage) => {
    db.stage = via;
    pending.set(db.id, { until: now() + transitionMs, to });
    return { ...db };
  };

  return {
    async listDatabases(namespace) {
      return [...dbs.values()]
        .filter((d) => d.namespace === namespace)
        .map((d) => ({ ...settle(d) }));
    },
    async getDatabase(namespace, id) {
      return { ...find(namespace, id) };
    },
    async suspendDatabase(namespace, id) {
      const db = find(namespace, id);
      if (db.stage === 'suspended' || db.stage === 'suspending') return { ...db };
      if (db.stage !== 'running')
        throw new SqlFailure('database_not_running', `${db.name} is ${db.stage}.`);
      return move(db, 'suspending', 'suspended');
    },
    async resumeDatabase(namespace, id) {
      const db = find(namespace, id);
      if (db.stage === 'running' || db.stage === 'resuming') return { ...db };
      if (db.stage !== 'suspended' && db.stage !== 'failed')
        throw new SqlFailure('database_busy', `${db.name} is ${db.stage}.`);
      return move(db, 'resuming', 'running');
    },
    async listBranches(namespace, databaseId) {
      find(namespace, databaseId);
      return branches.filter((b) => b.databaseId === databaseId).map((b) => ({ ...b }));
    },
    async getConnectionInfo(namespace, databaseId, branchId, rotate): Promise<ConnectionInfo> {
      const db = find(namespace, databaseId);
      const br = branches.find((b) => b.id === branchId && b.databaseId === databaseId);
      if (!br) throw new SqlFailure('branch_not_found', `No branch ${branchId}.`);
      const key = `${databaseId}/${branchId}`;
      let password = '';
      if (rotate || !minted.has(key)) {
        rotations += 1;
        password = mockPassword(rotations);
        minted.add(key);
      }
      return {
        host: `${br.name}.${db.name}.sql.example.invalid`,
        port: 4000,
        user: `${db.name}_owner.${br.name}`,
        database: db.name,
        password,
        tlsRequired: true,
      };
    },
    async query(namespace, input: QueryInput, signal) {
      const db = find(namespace, input.databaseId);
      if (signal?.aborted) throw new SqlFailure('cancelled', 'The query was cancelled.');
      if (db.stage !== 'running')
        throw new SqlFailure('database_suspended', `${db.name} is ${db.stage}; resume it first.`);
      const s = input.statement.trim();
      if (!s) throw new SqlFailure('empty_statement', 'Write a statement to run.');
      if (/^(insert|update|delete|replace)\b/i.test(s))
        return { columns: [], rows: [], rowsAffected: 1, truncated: false, elapsedMs: 3 };
      if (/^(create|drop|alter|truncate)\b/i.test(s))
        return { columns: [], rows: [], rowsAffected: 0, truncated: false, elapsedMs: 12 };
      if (/^select\s+1\s*;?$/i.test(s))
        return {
          columns: [{ name: '1', type: 'BIGINT' }],
          rows: [['1']],
          rowsAffected: 0,
          truncated: false,
          elapsedMs: 1,
        };
      if (/^(select|show|describe|desc|explain|with)\b/i.test(s)) {
        const rows = SAMPLE.rows.slice(0, input.maxRows);
        return { ...SAMPLE, rows, truncated: rows.length < SAMPLE.rows.length };
      }
      throw new SqlFailure(
        'syntax_error',
        `You have an error in your SQL syntax near '${s.slice(0, 20)}'.`,
      );
    },
  };
}
