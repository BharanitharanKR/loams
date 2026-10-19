// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/empty.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx; no cva; the parts build the Loams Empty, which keeps its props and paints no grain (Q929). Loams' changes are Apache-2.0.

import type { ComponentProps, ReactNode } from 'react';
import { cx } from './cx';
import type { GrainKind } from './grain';

export function EmptyHeader({ className, ...props }: ComponentProps<'div'>) {
  return (
    <div
      data-slot="empty-header"
      className={cx('flex max-w-sm flex-col items-center gap-2 text-center', className)}
      {...props}
    />
  );
}

export function EmptyTitle({ className, ...props }: ComponentProps<'div'>) {
  return (
    <div
      data-slot="empty-title"
      className={cx('text-emphasis font-semibold text-ink', className)}
      {...props}
    />
  );
}

export function EmptyDescription({ className, ...props }: ComponentProps<'div'>) {
  return (
    <div
      data-slot="empty-description"
      className={cx(
        'text-body text-muted [&_a]:text-accent-fg [&_a]:underline [&_a]:underline-offset-4',
        className,
      )}
      {...props}
    />
  );
}

export function EmptyContent({ className, ...props }: ComponentProps<'div'>) {
  return (
    <div
      data-slot="empty-content"
      className={cx(
        'flex w-full min-w-0 max-w-sm flex-col items-center gap-3 text-balance text-body',
        className,
      )}
      {...props}
    />
  );
}

export function EmptyMedia({ className, ...props }: ComponentProps<'div'>) {
  return (
    <div
      data-slot="empty-media"
      className={cx(
        'mb-2 grid size-9 shrink-0 place-items-center rounded-md bg-raised text-muted [&_svg]:pointer-events-none [&_svg]:size-4.5 [&_svg]:shrink-0',
        className,
      )}
      {...props}
    />
  );
}

/**
 * An empty state says what goes here and how to add it. `grain` and `seed` are kept
 * for callers and unused: the shell has no grain (Q929).
 */
export function Empty({
  title,
  children,
  actions,
}: {
  title: string;
  children?: ReactNode;
  actions?: ReactNode;
  grain?: Exclude<GrainKind, 'bloom'>;
  seed?: number;
}) {
  return (
    <div className="loams-empty" data-slot="empty">
      <EmptyHeader>
        <EmptyTitle>{title}</EmptyTitle>
        {children && <EmptyDescription className="loams-empty-body">{children}</EmptyDescription>}
      </EmptyHeader>
      {actions && (
        <EmptyContent className="loams-empty-actions flex-row justify-center">
          {actions}
        </EmptyContent>
      )}
    </div>
  );
}
