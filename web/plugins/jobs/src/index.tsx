// @loams/plugin-jobs: the Jobs page (§26, JB1). Queues, jobs, schedules, dead letters and
// per-function usage. loams.jobs.v1 is not generated into @loams/proto yet, so the page
// runs on the fixture-backed mock client and says so; a transport-backed JobsClient
// replaces it behind the same interface when the service lands.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { JobsPage } from './JobsPage.js';
import { createMockJobsClient } from './mock.js';

export type * from './client.js';
export { JOB_STATES, JobsClientError, USAGE_WINDOWS } from './client.js';
export { JOBS_PACKAGE, JobDetailView, JobsPage } from './JobsPage.js';
export { createMockJobsClient, type JobsFixtures, sampleFixtures } from './mock.js';

const plugin: PluginModule = {
  name: 'jobs',
  inject: ['desktop', 'router', 'slots'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const client = createMockJobsClient();
    ctx.effect(() =>
      router.page({ id: 'jobs', path: '/jobs', title: 'Jobs', plugin: 'jobs' }, () => (
        <JobsPage client={client} preview />
      )),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'jobs',
          order: 85,
          meta: { id: 'jobs', label: 'Jobs', href: '/jobs', icon: 'jobs', group: 'Compute' },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
