// Studio's server state for projects, through the console's one query cache
// (console-ui-stack decision 4). The Connect client of SD1 Task 6 swaps these
// query functions for connect-query's; the keys and the components stay.

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useSyncExternalStore } from 'react';
import type { Project, Reach, StudioAgentd } from '../agentd.js';

export function useReach(agentd: StudioAgentd): Reach {
  return useSyncExternalStore(
    (l) => agentd.subscribe(l),
    () => agentd.reach(),
  );
}

const projectsKey = (agentd: StudioAgentd) => ['studio', 'projects', agentd.cacheKey()] as const;

export function useProjects(agentd: StudioAgentd) {
  const reach = useReach(agentd);
  return useQuery({
    queryKey: projectsKey(agentd),
    queryFn: () => agentd.listProjects(),
    enabled: reach.state === 'ready',
  });
}

/** Adds a project (a folder or a clone URL) and refreshes the list. */
export function useAddProject(agentd: StudioAgentd) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (input: { path: string } | { url: string }): Promise<Project> => {
      if ('path' in input) {
        if (!agentd.addProject) throw new Error('This host cannot add a local folder');
        return agentd.addProject(input.path);
      }
      if (!agentd.cloneProject) throw new Error('This host cannot clone a repository');
      return agentd.cloneProject(input.url);
    },
    onSuccess: () => client.invalidateQueries({ queryKey: projectsKey(agentd) }),
  });
}
