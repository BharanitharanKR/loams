import { type Status, StatusTag } from '@loams/ui';
import type { DatabaseStage } from './client.js';

/** How the page names a stage: `resuming` reads as "Waking" (the gate wakes the pool, §14). */
export const STAGE_LABEL: Record<DatabaseStage, string> = {
  creating: 'Creating',
  running: 'Running',
  suspending: 'Suspending',
  suspended: 'Suspended',
  resuming: 'Waking',
  branching: 'Branching',
  restoring: 'Restoring',
  failed: 'Failed',
  deleting: 'Deleting',
};

const STAGE_STATUS: Record<DatabaseStage, Status> = {
  creating: 'progress',
  running: 'done',
  suspending: 'progress',
  suspended: 'neutral',
  resuming: 'progress',
  branching: 'progress',
  restoring: 'progress',
  failed: 'failed',
  deleting: 'progress',
};

export function Stage({ stage }: { stage: DatabaseStage }) {
  return <StatusTag status={STAGE_STATUS[stage]}>{STAGE_LABEL[stage]}</StatusTag>;
}
