// The Diff tab, loaded when it is first shown (SD1 Task 20, as Task 21's Files tab): the
// viewer brings @pierre/diffs and Shiki's highlighter, which Studio's first screen does not
// need.

import { Spinner } from '@loams/ui';
import { type ComponentProps, lazy, Suspense } from 'react';

const Tab = lazy(() => import('./DiffTab.js').then((m) => ({ default: m.DiffTab })));

export function DiffTab(props: ComponentProps<typeof Tab>) {
  return (
    <Suspense
      fallback={
        <div className="flex flex-1 items-center justify-center p-6">
          <Spinner tone="muted" label="Loading the diff" />
        </div>
      }
    >
      <Tab {...props} />
    </Suspense>
  );
}
