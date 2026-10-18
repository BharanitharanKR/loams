// The palette's projects (design 59, D1078; CF1 Task 11): every project of the org,
// from the same `ListProjects` the top bar's project pill reads. Like the pin rows (T10-2)
// they show once something is typed (ruling T11-3).

import { useQuery } from '@connectrpc/connect-query';
import type { RouterService } from '@loams/console-host';
import { admin } from '@loams/proto';
import type { PaletteCommand } from '@loams/slots';
import { useEffect } from 'react';

export function ProjectsSource({
  query,
  publish,
  router,
}: {
  query: string;
  publish: (commands: readonly PaletteCommand[]) => void;
  router: Pick<RouterService, 'navigate'>;
}) {
  const projects = useQuery(admin.ProjectService.method.listProjects, {});
  const list = projects.data?.projects;
  const typed = query.trim() !== '';
  useEffect(() => {
    publish(
      (typed ? (list ?? []) : []).map((p) => ({
        id: `project:${p.slug}`,
        title: `Go to project ${p.name}`,
        group: 'Projects',
        keywords: [p.slug],
        run: () => router.navigate(`/projects/${encodeURIComponent(p.slug)}`),
      })),
    );
  }, [list, publish, router, typed]);
  return null;
}
