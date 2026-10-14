// The Files tab, loaded when it is first shown (SD1 Task 21): the viewer brings
// @pierre/diffs and Shiki's highlighter, which Studio's first screen does not need.

import { Spinner } from '@loams/ui';
import { type ComponentProps, lazy, Suspense } from 'react';

const Tab = lazy(() => import('./FilesTab.js').then((m) => ({ default: m.FilesTab })));

export function FilesTab(props: ComponentProps<typeof Tab>) {
  return (
    <Suspense
      fallback={
        <div className="flex flex-1 items-center justify-center p-6">
          <Spinner tone="muted" label="Loading files" />
        </div>
      }
    >
      <Tab {...props} />
    </Suspense>
  );
}
