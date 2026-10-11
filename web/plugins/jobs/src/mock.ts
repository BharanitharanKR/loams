// A fixture-backed JobsClient. The page runs on it until loams.jobs.v1 is generated into
// @loams/proto; then a transport-backed client replaces it behind the same interface.
// The data is in memory and per client: retrying or discarding a dead letter changes what
// the next call returns, so the page behaves as it will against the service.

import {
  type DeadLetter,
  type FunctionUsage,
  type JobDetail,
  type JobQuery,
  type JobState,
  type JobSummary,
  type JobsClient,
  JobsClientError,
  type Page,
  type QueueSummary,
  type ScheduleSummary,
  type UsageWindow,
} from './client.js';

export interface JobsFixtures {
  queues: QueueSummary[];
  jobs: JobDetail[];
  schedules: ScheduleSummary[];
  deadLetters: DeadLetter[];
  usage: Record<UsageWindow, FunctionUsage[]>;
}

const counts = (c: Partial<Record<JobState, number>>): Record<JobState, number> => ({
  waiting: 0,
  prioritized: 0,
  delayed: 0,
  active: 0,
  waiting_children: 0,
  completed: 0,
  failed: 0,
  dead_lettered: 0,
  canceled: 0,
  ...c,
});

const at = (minutesAgo: number): string =>
  new Date(Date.UTC(2026, 9, 10, 12, 0, 0) - minutesAgo * 60_000).toISOString();

function job(
  j: Partial<JobDetail> & Pick<JobDetail, 'id' | 'queue' | 'name' | 'state'>,
): JobDetail {
  return {
    attempt: 0,
    maxAttempts: 3,
    stalled: 0,
    mode: 'queue',
    createdAt: at(30),
    updatedAt: at(1),
    payload: { contentType: 'application/json', sizeBytes: 2, text: '{}' },
    headers: {},
    attempts: [],
    logs: [],
    ...j,
  };
}

/** The sample namespace's data. Ids are ULID-shaped; nothing here is a real host or key. */
export function sampleFixtures(): JobsFixtures {
  return {
    queues: [
      {
        name: 'emails',
        paused: false,
        shards: 1,
        deadLetterQueue: 'emails.dlq',
        counts: counts({
          waiting: 42,
          prioritized: 3,
          delayed: 7,
          active: 8,
          completed: 12_480,
          failed: 11,
        }),
        rates: { enqueued: 14.2, leased: 13.8, completed: 13.1, failed: 0.2 },
        oldestWaitingSeconds: 38,
        concurrency: 16,
      },
      {
        name: 'reports',
        paused: true,
        shards: 2,
        counts: counts({ waiting: 120, active: 0, completed: 940, failed: 2 }),
        rates: { enqueued: 0.6, leased: 0, completed: 0, failed: 0 },
        oldestWaitingSeconds: 5_400,
      },
      {
        name: 'emails.dlq',
        paused: false,
        shards: 1,
        counts: counts({ dead_lettered: 2 }),
        rates: { enqueued: 0, leased: 0, completed: 0, failed: 0 },
        oldestWaitingSeconds: 0,
      },
    ],
    jobs: [
      job({
        id: '01JA2Z7Q8M4V9K3T6R1N5B0C7D',
        queue: 'emails',
        name: 'send_welcome',
        state: 'active',
        attempt: 1,
        mode: 'durable',
        payload: {
          contentType: 'application/json',
          sizeBytes: 74,
          text: '{"template":"welcome","user_id":"u_123","locale":"en-GB","attachments":[]}',
        },
        headers: { traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' },
        attempts: [{ n: 1, worker: 'worker-a:4121:x9', startedAt: at(1), outcome: 'running' }],
        logs: [
          { at: at(1), text: 'rendering template welcome' },
          { at: at(0.5), text: 'step render settled; sending' },
        ],
        progress: 50,
      }),
      job({
        id: '01JA2Z6H1F0E8D7C6B5A4Z3Y2X',
        queue: 'emails',
        name: 'send_receipt',
        state: 'delayed',
        attempt: 2,
        updatedAt: at(3),
        error: { code: 'smtp_unavailable', message: 'the mail relay refused the connection' },
        attempts: [
          {
            n: 1,
            worker: 'worker-b:812:k2',
            startedAt: at(5),
            endedAt: at(4.9),
            outcome: 'retry',
            error: { code: 'smtp_unavailable', message: 'the mail relay refused the connection' },
          },
          {
            n: 2,
            worker: 'worker-a:4121:x9',
            startedAt: at(3.2),
            endedAt: at(3),
            outcome: 'retry',
            error: { code: 'smtp_unavailable', message: 'the mail relay refused the connection' },
          },
        ],
        logs: [
          { at: at(5), text: 'connecting to relay' },
          { at: at(4.9), text: 'retry in 60 s (exponential backoff)' },
          { at: at(3), text: 'retry in 120 s (exponential backoff)' },
        ],
      }),
      job({
        id: '01JA2Z5B9C8D7E6F5G4H3J2K1M',
        queue: 'emails',
        name: 'send_digest',
        state: 'completed',
        attempt: 1,
        createdAt: at(20),
        updatedAt: at(19),
        attempts: [
          { n: 1, worker: 'worker-b:812:k2', startedAt: at(19.5), endedAt: at(19), outcome: 'ok' },
        ],
      }),
      job({
        id: '01JA2Z4N7P6Q5R4S3T2V1W0X9Y',
        queue: 'reports',
        name: 'monthly_rollup',
        state: 'waiting',
        priority: 5,
        jobKey: 'rollup-2026-09',
        payload: {
          contentType: 'application/x-msgpack',
          sizeBytes: 48_213,
          blobRef: 'blob:01JA2Z4N7P6Q5R4S3T2V1W0X9Y/payload',
        },
      }),
      job({
        id: '01JA2Z3A2B3C4D5E6F7G8H9J0K',
        queue: 'reports',
        name: 'export_csv',
        state: 'failed',
        attempt: 3,
        error: { code: 'timeout', message: 'the lease passed its hard timeout of 300 s' },
        attempts: [1, 2, 3].map((n) => ({
          n,
          worker: 'worker-c:77:p0',
          startedAt: at(40 - n * 6),
          endedAt: at(35 - n * 6),
          outcome: n === 3 ? ('fail' as const) : ('retry' as const),
          error: { code: 'timeout', message: 'the lease passed its hard timeout of 300 s' },
        })),
      }),
    ],
    schedules: [
      {
        id: 'nightly-digest',
        cron: '0 2 * * *',
        timezone: 'UTC',
        queue: 'emails',
        task: 'send_digest',
        paused: false,
        nextRunAt: at(-840),
        lastRunAt: at(600),
      },
      {
        id: 'monthly-rollup',
        cron: '0 4 1 * *',
        timezone: 'Europe/London',
        queue: 'reports',
        task: 'monthly_rollup',
        paused: true,
      },
    ],
    deadLetters: [
      {
        id: '01JA2Y9Z8X7W6V5T4S3R2Q1P0N',
        queue: 'emails.dlq',
        originalQueue: 'emails',
        name: 'send_receipt',
        attempts: 5,
        failedAt: at(90),
        error: { code: 'invalid_recipient', message: 'the address has no MX record' },
      },
      {
        id: '01JA2Y8M7L6K5J4H3G2F1E0D9C',
        queue: 'emails.dlq',
        originalQueue: 'emails',
        name: 'send_welcome',
        attempts: 5,
        failedAt: at(240),
        error: { code: 'template_error', message: 'missing variable first_name' },
      },
    ],
    usage: {
      '1h': [
        usage('send-email', 't0', 'workerd', 1_820, 410, 96, 52_000_000, 4),
        usage('render-report', 't1', 'wasm', 64, 380, 312, 210_000_000, 0),
        usage('resize-image', 't0', 'workerd', 940, 122, 88, 74_000_000, 12),
      ],
      '24h': [
        usage('send-email', 't0', 'workerd', 41_300, 9_620, 2_210, 61_000_000, 57),
        usage('render-report', 't1', 'wasm', 1_420, 8_870, 7_340, 240_000_000, 3),
        usage('resize-image', 't0', 'workerd', 22_800, 2_940, 2_100, 81_000_000, 140),
      ],
      '7d': [
        usage('send-email', 't0', 'workerd', 288_000, 67_100, 15_400, 66_000_000, 390),
        usage('render-report', 't1', 'wasm', 9_910, 61_900, 51_200, 252_000_000, 21),
        usage('resize-image', 't0', 'workerd', 160_000, 20_600, 14_700, 88_000_000, 960),
      ],
    },
  };
}

function usage(
  fn: string,
  tier: string,
  runner: string,
  invocations: number,
  wallSeconds: number,
  cpuSeconds: number,
  residentBytes: number,
  errors: number,
): FunctionUsage {
  return {
    function: fn,
    tier,
    runner,
    invocations,
    wallSeconds,
    cpuSeconds,
    residentBytes,
    errors,
  };
}

const empty = (): JobsFixtures => ({
  queues: [],
  jobs: [],
  schedules: [],
  deadLetters: [],
  usage: { '1h': [], '24h': [], '7d': [] },
});

export const MOCK_PAGE_SIZE = 50;

const summary = ({
  payload: _p,
  headers: _h,
  attempts: _a,
  logs: _l,
  progress: _g,
  ...rest
}: JobDetail): JobSummary => rest;

/**
 * A JobsClient over fixtures, keyed by namespace. Namespaces not in `data` are empty.
 * The default has the sample data in `default`.
 */
export function createMockJobsClient(
  data: Record<string, JobsFixtures> = { default: sampleFixtures() },
): JobsClient {
  const ns = (n: string): JobsFixtures => {
    data[n] ??= empty();
    return data[n];
  };
  const clone = <T>(v: T): T => structuredClone(v);
  // Removes the dead letter from its namespace and returns it, or refuses an unknown one.
  const takeDead = (n: string, queue: string, id: string): DeadLetter => {
    const list = ns(n).deadLetters;
    const i = list.findIndex((d) => d.queue === queue && d.id === id);
    const d = list[i];
    if (!d) throw new JobsClientError('not_found', `no dead letter ${id} in ${queue}`);
    list.splice(i, 1);
    return d;
  };
  const bump = (n: string, queue: string, state: JobState, by: number) => {
    const q = ns(n).queues.find((x) => x.name === queue);
    if (q) q.counts[state] = Math.max(0, q.counts[state] + by);
  };

  return {
    async listQueues(n) {
      return clone(ns(n).queues);
    },
    async listJobs(n, query: JobQuery): Promise<Page<JobSummary>> {
      const all = ns(n)
        .jobs.filter(
          (j) =>
            (!query.queue || j.queue === query.queue) && (!query.state || j.state === query.state),
        )
        .sort((a, b) => b.createdAt.localeCompare(a.createdAt) || b.id.localeCompare(a.id));
      const start = query.pageToken ? Number(query.pageToken) : 0;
      const items = all.slice(start, start + MOCK_PAGE_SIZE).map(summary);
      const next = start + MOCK_PAGE_SIZE;
      return { items: clone(items), nextPageToken: next < all.length ? String(next) : undefined };
    },
    async getJob(n, queue, id) {
      const j = ns(n).jobs.find((x) => x.queue === queue && x.id === id);
      if (!j) throw new JobsClientError('not_found', `no job ${id} in ${queue}`);
      return clone(j);
    },
    async listSchedules(n) {
      return clone(ns(n).schedules);
    },
    async listDeadLetters(n) {
      return clone(ns(n).deadLetters);
    },
    async retryDeadLetter(n, queue, id) {
      const d = takeDead(n, queue, id);
      bump(n, d.queue, 'dead_lettered', -1);
      bump(n, d.originalQueue, 'waiting', 1);
      ns(n).jobs.push(
        job({
          id: d.id,
          queue: d.originalQueue,
          name: d.name,
          state: 'waiting',
          attempt: 0,
          maxAttempts: d.attempts,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          headers: { 'loams-redriven-from': d.queue },
        }),
      );
    },
    async discardDeadLetter(n, queue, id) {
      takeDead(n, queue, id);
      bump(n, queue, 'dead_lettered', -1);
    },
    async functionUsage(n, window) {
      return clone(ns(n).usage[window]);
    },
  };
}
