// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/alert.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; upstream's error variant is danger; the glass and sidebar surfaces are dropped. Loams' changes are Apache-2.0.

import {
  Children,
  type ComponentProps,
  isValidElement,
  type ReactElement,
  type ReactNode,
} from 'react';
import { cx } from './cx';

const VARIANT = {
  default: 'border-rule bg-surface text-ink [&_svg]:text-muted',
  info: 'border-transparent bg-info-soft text-ink [&_[data-slot=alert-title]]:text-info [&_svg]:text-info',
  success:
    'border-transparent bg-grow-soft text-ink [&_[data-slot=alert-title]]:text-grow [&_svg]:text-grow',
  warning:
    'border-transparent bg-warn-soft text-ink [&_[data-slot=alert-title]]:text-warn [&_svg]:text-warn',
  danger:
    'border-transparent bg-danger-soft text-ink [&_[data-slot=alert-title]]:text-danger [&_svg]:text-danger',
} as const;

export type AlertVariant = keyof typeof VARIANT;

function alertChildSlot(child: ReactElement): string | undefined {
  const propsSlot = (child.props as Record<string, string | undefined>)['data-slot'];
  if (propsSlot) return propsSlot;
  const type = child.type as { displayName?: string; name?: string };
  switch (type.displayName ?? type.name) {
    case 'AlertAction':
      return 'alert-action';
    case 'AlertTitle':
      return 'alert-title';
    case 'AlertDescription':
      return 'alert-description';
    default:
      return undefined;
  }
}

/**
 * A message that stays on the page. Children sort themselves: an icon first, then
 * AlertTitle and AlertDescription, then AlertAction at the end.
 */
export function Alert({
  className,
  variant = 'default',
  controlAlignment = 'center',
  children,
  ...props
}: ComponentProps<'div'> & {
  variant?: AlertVariant;
  controlAlignment?: 'center' | 'first-line';
}) {
  const icon: ReactNode[] = [];
  const content: ReactNode[] = [];
  const action: ReactNode[] = [];

  Children.forEach(children, (child) => {
    if (!isValidElement(child)) {
      content.push(child);
      return;
    }
    const slot = alertChildSlot(child);
    if (slot === 'alert-action') action.push(child);
    else if (slot === 'alert-title' || slot === 'alert-description') content.push(child);
    else icon.push(child);
  });

  return (
    <div
      className={cx(
        'relative w-full rounded-lg border px-3.5 py-3 text-body',
        VARIANT[variant],
        className,
      )}
      data-slot="alert"
      data-variant={variant}
      role="alert"
      {...props}
    >
      <div
        className={cx(
          'flex gap-2',
          controlAlignment === 'first-line' ? 'items-start' : 'items-center',
          controlAlignment === 'first-line' && action.length > 0 && 'min-h-6 pt-0.5',
        )}
      >
        {icon.length > 0 && (
          <div
            className={cx(
              'flex shrink-0 items-center justify-center',
              controlAlignment === 'first-line'
                ? 'h-lh w-4 [&>svg]:size-4'
                : 'size-4 [&>svg]:size-full',
            )}
          >
            {icon}
          </div>
        )}
        {content.length > 0 && (
          <div className="flex min-w-0 flex-1 flex-col gap-0.5">{content}</div>
        )}
        {action.length > 0 && (
          <div
            className={cx(
              'flex shrink-0 items-center',
              controlAlignment === 'first-line' ? 'h-lh self-start' : 'self-center',
            )}
          >
            {action}
          </div>
        )}
      </div>
    </div>
  );
}

export function AlertTitle({ className, ...props }: ComponentProps<'div'>) {
  return <div className={cx('font-medium', className)} data-slot="alert-title" {...props} />;
}

export function AlertDescription({ className, ...props }: ComponentProps<'div'>) {
  return (
    <div
      className={cx('flex flex-col gap-2.5 text-muted', className)}
      data-slot="alert-description"
      {...props}
    />
  );
}

export function AlertAction({ className, ...props }: ComponentProps<'div'>) {
  return <div className={cx('flex gap-1', className)} data-slot="alert-action" {...props} />;
}

AlertTitle.displayName = 'AlertTitle';
AlertDescription.displayName = 'AlertDescription';
AlertAction.displayName = 'AlertAction';
