// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/collapsible.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to Loams tokens, rounded-none and rounded-pill, and cx, with Base UI underneath; no cva, lucide or Effect. Loams' changes are Apache-2.0.

// A disclosure: a trigger that shows and hides a panel (tool-call rows, settings sections).
// Base UI gives the button its aria-expanded and aria-controls.

import { Collapsible as BaseCollapsible } from '@base-ui/react/collapsible';
import type { ComponentProps } from 'react';
import { cx } from './cx';

export function Collapsible(props: ComponentProps<typeof BaseCollapsible.Root>) {
  return <BaseCollapsible.Root data-slot="collapsible" {...props} />;
}

export function CollapsibleTrigger({
  className,
  ...props
}: Omit<ComponentProps<typeof BaseCollapsible.Trigger>, 'className'> & { className?: string }) {
  return (
    <BaseCollapsible.Trigger
      className={cx(
        'cursor-pointer border-0 bg-transparent p-0 text-left font-[inherit] text-inherit ' +
          'focus-visible:outline-2 focus-visible:outline-accent',
        className,
      )}
      data-slot="collapsible-trigger"
      {...props}
    />
  );
}

export function CollapsiblePanel({
  className,
  animate = true,
  ...props
}: Omit<ComponentProps<typeof BaseCollapsible.Panel>, 'className'> & {
  className?: string;
  /** Height travel on open and close; reduced motion always skips it. */
  animate?: boolean;
}) {
  return (
    <BaseCollapsible.Panel
      className={cx(
        'overflow-hidden',
        animate &&
          'h-(--collapsible-panel-height) transition-[height] duration-200 ease-op motion-reduce:transition-none ' +
            'data-ending-style:h-0 data-starting-style:h-0',
        className,
      )}
      data-slot="collapsible-panel"
      {...props}
    />
  );
}
