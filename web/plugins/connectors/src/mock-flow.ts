// A fixture-backed, in-memory `FlowClient` so the instance pages work until `FlowService` lands.
// It keeps secret *names* only: the values passed to `createInstance` are dropped on receipt.

import {
  type CreateInstanceRequest,
  type FlowClient,
  type FlowResult,
  type InstanceDetail,
  type InstanceSummary,
  NAME_RE,
} from './flow-client.js';

const T0 = Date.UTC(2026, 9, 10, 12, 0, 0);
const iso = (minsAgo: number) => new Date(T0 - minsAgo * 60_000).toISOString();
const series = (base: number) =>
  Array.from({ length: 12 }, (_, i) => ({
    at: iso((11 - i) * 5),
    eventsPerSec: Math.round(base * (0.8 + ((i * 7) % 5) / 10)),
    bytesPerSec: Math.round(base * 512 * (0.8 + ((i * 3) % 5) / 10)),
  }));

export function fixtureInstances(): InstanceDetail[] {
  return [
    {
      name: 'orders-cdc',
      connector: 'postgresql',
      connectorMajor: 1,
      direction: 'source',
      runtime: 'native',
      state: 'running',
      eventsPerSec: 1240,
      lagSeconds: 2,
      version: 3,
      fabric: { kind: 'topic', topic: 'shop.public.orders', partitions: 4 },
      delivery: 'effectively_once',
      config: { host: 'orders-db', database: 'shop', password: '${secret:password}' },
      secrets: ['password'],
      checkpoints: [
        {
          split: 'slot:loams_orders',
          position: 'lsn 0/3A2F1C8',
          epoch: 7,
          committedAt: iso(0),
          lagSeconds: 2,
        },
      ],
      throughput: series(1240),
      errors: [],
      dlqCount: 0,
    },
    {
      name: 'clicks-to-house',
      connector: 'clickhouse',
      connectorMajor: 1,
      direction: 'sink',
      runtime: 'native',
      state: 'paused',
      eventsPerSec: 0,
      lagSeconds: 840,
      version: 5,
      fabric: { kind: 'topic', topic: 'web.clicks', partitions: 2 },
      delivery: 'effectively_once',
      config: { url: 'house', table: 'clicks' },
      secrets: [],
      checkpoints: [
        {
          split: 'partition-0',
          position: 'offset 918223',
          epoch: 2,
          committedAt: iso(14),
          lagSeconds: 840,
        },
        {
          split: 'partition-1',
          position: 'offset 917004',
          epoch: 2,
          committedAt: iso(14),
          lagSeconds: 820,
        },
      ],
      throughput: series(300).map((p, i) =>
        i > 8 ? { ...p, eventsPerSec: 0, bytesPerSec: 0 } : p,
      ),
      errors: [
        {
          at: iso(15),
          class: 'schema_incompatible',
          message: 'Column "ts" changed from DateTime64 to String; instance paused.',
          split: 'partition-0',
        },
      ],
      dlqCount: 0,
      lastError: {
        at: iso(15),
        class: 'schema_incompatible',
        message: 'Column "ts" changed from DateTime64 to String; instance paused.',
        split: 'partition-0',
      },
    },
    {
      name: 'erp-oracle-cdc',
      connector: 'debezium-oracle',
      connectorMajor: 1,
      direction: 'source',
      runtime: 'fallback',
      state: 'failed',
      eventsPerSec: 0,
      lagSeconds: 7200,
      version: 1,
      fabric: { kind: 'topic', topic: 'erp.orders' },
      delivery: 'at_least_once',
      config: { host: 'erp-db', user: 'loams', password: '${secret:password}' },
      secrets: ['password'],
      checkpoints: [
        {
          split: 'logminer',
          position: 'scn 88123001',
          epoch: 1,
          committedAt: iso(120),
          lagSeconds: 7200,
        },
      ],
      throughput: series(80).map((p) => ({ ...p, eventsPerSec: 0, bytesPerSec: 0 })),
      errors: [
        {
          at: iso(118),
          class: 'position_lost',
          message: 'SCN 88123001 is older than the oldest archived log.',
          hint: 'Re-snapshot the instance to continue.',
        },
      ],
      dlqCount: 12,
      lastError: {
        at: iso(118),
        class: 'position_lost',
        message: 'SCN 88123001 is older than the oldest archived log.',
        hint: 'Re-snapshot the instance to continue.',
      },
    },
  ];
}

const summarize = (d: InstanceDetail): InstanceSummary => ({
  name: d.name,
  connector: d.connector,
  direction: d.direction,
  runtime: d.runtime,
  state: d.state,
  eventsPerSec: d.eventsPerSec,
  lagSeconds: d.lagSeconds,
  version: d.version,
});

const notFound = (name: string) =>
  ({ ok: false, code: 'not_found', message: `No instance "${name}".` }) as const;

export interface MockFlowClient extends FlowClient {
  /** For tests: the stored instances (no secret values are ever kept). */
  readonly store: Map<string, InstanceDetail>;
}

export function createMockFlowClient(seed: InstanceDetail[] = fixtureInstances()): MockFlowClient {
  const store = new Map(seed.map((d) => [d.name, structuredClone(d)]));
  const transition = (
    name: string,
    from: InstanceDetail['state'][],
    to: InstanceDetail['state'],
  ): FlowResult<InstanceSummary> => {
    const d = store.get(name);
    if (!d) return notFound(name);
    if (!from.includes(d.state)) {
      return { ok: false, code: 'failed_precondition', message: `"${name}" is ${d.state}.` };
    }
    d.state = to;
    d.version += 1;
    if (to === 'paused') d.eventsPerSec = 0;
    return { ok: true, value: summarize(d) };
  };
  return {
    fixture: true,
    store,
    listInstances: async () => [...store.values()].map(summarize),
    getInstance: async (name) => {
      const d = store.get(name);
      return d ? { ok: true, value: structuredClone(d) } : notFound(name);
    },
    createInstance: async (req: CreateInstanceRequest) => {
      if (!NAME_RE.test(req.name)) {
        return { ok: false, code: 'invalid_argument', message: 'Invalid instance name.' };
      }
      if (store.has(req.name)) {
        return { ok: false, code: 'already_exists', message: `"${req.name}" already exists.` };
      }
      const d: InstanceDetail = {
        name: req.name,
        connector: req.connector,
        connectorMajor: 1,
        direction: req.direction,
        runtime: req.runtime,
        state: req.enabled ? 'starting' : 'stopped',
        eventsPerSec: 0,
        lagSeconds: 0,
        version: 1,
        fabric: req.fabric,
        delivery: 'at_least_once',
        config: structuredClone(req.config),
        // Names only; the values in `req.secrets` go no further than this line.
        secrets: Object.keys(req.secrets).sort(),
        checkpoints: [],
        throughput: [],
        errors: [],
        dlqCount: 0,
      };
      store.set(d.name, d);
      return { ok: true, value: summarize(d) };
    },
    pause: async (name) => transition(name, ['running', 'starting'], 'paused'),
    resume: async (name) => transition(name, ['paused', 'stopped', 'failed'], 'starting'),
  };
}
