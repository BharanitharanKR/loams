// Test helpers: a mock client with a call log, and a client whose calls fail.

import { type JobsClient, JobsClientError } from '../src/client.js';
import { createMockJobsClient, type JobsFixtures, sampleFixtures } from '../src/mock.js';

export interface Recorded {
  method: keyof JobsClient;
  args: unknown[];
}

/** The sample mock, wrapped so each call is recorded. */
export function recordingClient(data?: Record<string, JobsFixtures>) {
  const inner = createMockJobsClient(data ?? { default: sampleFixtures() });
  const calls: Recorded[] = [];
  const client = Object.fromEntries(
    (Object.keys(inner) as (keyof JobsClient)[]).map((m) => [
      m,
      (...args: unknown[]) => {
        calls.push({ method: m, args });
        return (inner[m] as (...a: unknown[]) => Promise<unknown>)(...args);
      },
    ]),
  ) as unknown as JobsClient;
  return { client, calls };
}

/** A client where `failing` methods reject with `unavailable`; the rest use the sample. */
export function failingClient(failing: (keyof JobsClient)[]): JobsClient {
  const inner = createMockJobsClient();
  return Object.fromEntries(
    (Object.keys(inner) as (keyof JobsClient)[]).map((m) => [
      m,
      failing.includes(m)
        ? () => Promise.reject(new JobsClientError('unavailable', 'the jobs service is down'))
        : inner[m],
    ]),
  ) as unknown as JobsClient;
}
