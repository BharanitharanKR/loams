// The Jobs page's client contract. `loams.jobs.v1` (§26 §5.5, JB1 Task 1) has no generated
// TypeScript in @loams/proto yet, so the page talks to this interface. Each method names the
// RPC it maps onto once the service lands; until then `createMockJobsClient` (mock.ts) backs
// the page with fixtures. Names follow the JB1 shared contract: JobState is §26 §6.4's, the
// per-function families are JB1-Q1's `loams_function_*` (labels function, tier, runner).

/** §26 §6.4, the wire enum `JOB_STATE_*` without its prefix. */
export type JobState =
  | 'waiting'
  | 'prioritized'
  | 'delayed'
  | 'active'
  | 'waiting_children'
  | 'completed'
  | 'failed'
  | 'dead_lettered'
  | 'canceled';

export const JOB_STATES: readonly JobState[] = [
  'waiting',
  'prioritized',
  'delayed',
  'active',
  'waiting_children',
  'completed',
  'failed',
  'dead_lettered',
  'canceled',
];

/** A queue with its counts (`QueueOp::Counts`) and rates (`QueueOp::Metrics`). */
export interface QueueSummary {
  name: string;
  paused: boolean;
  shards: number;
  /** The dead-letter queue this queue names, if any (§26 §6.10). */
  deadLetterQueue?: string;
  counts: Record<JobState, number>;
  /** Jobs per second over the last minute. */
  rates: { enqueued: number; leased: number; completed: number; failed: number };
  /** `loams_jobs_oldest_waiting_seconds`. */
  oldestWaitingSeconds: number;
  /** Global concurrency cap, when set. */
  concurrency?: number;
}

export interface JobError {
  code: string;
  message: string;
}

export interface JobSummary {
  /** Server-assigned ULID. */
  id: string;
  queue: string;
  /** Task name (Celery `name`, BullMQ job name). */
  name: string;
  state: JobState;
  /** Attempts made so far, 1-based once leased. */
  attempt: number;
  maxAttempts: number;
  stalled: number;
  priority?: number;
  jobKey?: string;
  mode: 'queue' | 'durable';
  createdAt: string;
  updatedAt: string;
  error?: JobError;
}

export interface Payload {
  contentType: string;
  sizeBytes: number;
  /** The inline bytes decoded as text, when the payload is inline (<= 16 KiB) and textual. */
  text?: string;
  /** Set when the payload is in the object store (read with `GetBlob`). */
  blobRef?: string;
}

export type AttemptOutcome = 'running' | 'ok' | 'retry' | 'fail' | 'stalled' | 'released';

export interface Attempt {
  n: number;
  worker: string;
  startedAt: string;
  endedAt?: string;
  outcome: AttemptOutcome;
  error?: JobError;
}

export interface LogLine {
  at: string;
  text: string;
}

export interface JobDetail extends JobSummary {
  payload: Payload;
  headers: Record<string, string>;
  attempts: Attempt[];
  /** `JobOp::Logs`, capped per job (§26 §6.11). */
  logs: LogLine[];
  progress?: number;
}

export interface ScheduleSummary {
  id: string;
  cron: string;
  timezone: string;
  queue: string;
  task: string;
  paused: boolean;
  nextRunAt?: string;
  lastRunAt?: string;
}

export interface DeadLetter {
  id: string;
  /** The dead-letter queue it sits in. */
  queue: string;
  /** The queue it failed on (from its headers, §26 §6.10). */
  originalQueue: string;
  name: string;
  attempts: number;
  failedAt: string;
  error: JobError;
}

export type UsageWindow = '1h' | '24h' | '7d';
export const USAGE_WINDOWS: readonly UsageWindow[] = ['1h', '24h', '7d'];

/** One function's `loams_function_*` series over a window (JB1-Q1). */
export interface FunctionUsage {
  function: string;
  tier: string;
  runner: string;
  invocations: number;
  wallSeconds: number;
  cpuSeconds: number;
  /** Peak `loams_function_resident_bytes` in the window. */
  residentBytes: number;
  errors: number;
}

export interface JobQuery {
  queue?: string;
  state?: JobState;
  pageToken?: string;
}

export interface Page<T> {
  items: T[];
  nextPageToken?: string;
}

/** What the Jobs page needs. Every call is scoped to a namespace. */
export interface JobsClient {
  /** `QueueAdmin` Counts and Metrics for every queue. */
  listQueues(namespace: string): Promise<QueueSummary[]>;
  /** `Query`, newest first. */
  listJobs(namespace: string, query: JobQuery): Promise<Page<JobSummary>>;
  /** `GetJob` and `JobAdmin` Logs. */
  getJob(namespace: string, queue: string, id: string): Promise<JobDetail>;
  /** `ListSchedules`. */
  listSchedules(namespace: string): Promise<ScheduleSummary[]>;
  /** `Query` with state dead_lettered. */
  listDeadLetters(namespace: string): Promise<DeadLetter[]>;
  /** `JobAdmin` Retry: back to the original queue, waiting. */
  retryDeadLetter(namespace: string, queue: string, id: string): Promise<void>;
  /** `JobAdmin` Remove. */
  discardDeadLetter(namespace: string, queue: string, id: string): Promise<void>;
  /** The `loams_function_*` families, summed over the window. */
  functionUsage(namespace: string, window: UsageWindow): Promise<FunctionUsage[]>;
}

/** A failed call. `code` is Connect's wire name once the real client is in place. */
export class JobsClientError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'JobsClientError';
  }
}
