// Studio's projects through connect-query over the agentd transport (SD1 Task 6;
// console-ui-stack decision 4): `ProjectService.ListRepos`, `AddRepo` and
// `CloneRepo`, in the console's one query cache. The transport comes from the
// nearest `AgentdScope` (view.tsx, index.tsx).

import { createConnectQueryKey, skipToken } from '@connectrpc/connect-query';
import { useAgentdMutation, useAgentdQuery } from '@loams/console-host/query';
import { ProjectService } from '@loams/proto/agentd';
import { useQueryClient } from '@tanstack/react-query';
import { useState, useSyncExternalStore } from 'react';
import { isCloneUrl, type Project, projectOf, type Reach, type StudioAgentd } from '../agentd.js';

export function useReach(agentd: StudioAgentd): Reach {
  return useSyncExternalStore(
    (l) => agentd.subscribe(l),
    () => agentd.reach(),
  );
}

const toProjects = (r: { repos: Parameters<typeof projectOf>[0][] }): Project[] =>
  r.repos.map(projectOf);

export function useProjects(agentd: StudioAgentd) {
  const reach = useReach(agentd);
  return useAgentdQuery(ProjectService.method.listRepos, reach.state === 'ready' ? {} : skipToken, {
    select: toProjects,
  });
}

/** Adds a project (a folder on the daemon's host, or a clone URL) and refreshes the list. */
export function useAddProject() {
  const client = useQueryClient();
  const onSuccess = () =>
    client.invalidateQueries({
      queryKey: createConnectQueryKey({
        schema: ProjectService.method.listRepos,
        cardinality: 'finite',
      }),
    });
  const add = useAgentdMutation(ProjectService.method.addRepo, { onSuccess });
  const clone = useAgentdMutation(ProjectService.method.cloneRepo, { onSuccess });
  const [refused, setRefused] = useState<Error | null>(null);
  const last = add.isIdle ? clone : add;
  const error = refused ?? last.error;
  return {
    isPending: add.isPending || clone.isPending,
    isError: error !== null,
    error,
    reset() {
      setRefused(null);
      add.reset();
      clone.reset();
    },
    mutate(input: { path: string } | { url: string }, opts?: { onSuccess?: () => void }) {
      setRefused(null);
      add.reset();
      clone.reset();
      if ('path' in input) {
        add.mutate({ path: input.path }, { onSuccess: () => opts?.onSuccess?.() });
      } else if (!isCloneUrl(input.url)) {
        // Refused here too (the daemon refuses it again), so it never leaves the page.
        setRefused(new Error('Expected an https, ssh or user@host:path git URL'));
      } else {
        clone.mutate({ url: input.url }, { onSuccess: () => opts?.onSuccess?.() });
      }
    },
  };
}
