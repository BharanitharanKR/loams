// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/badge.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; the default is the old Loams badge (neutral). Loams' changes are Apache-2.0.

import { mergeProps } from '@base-ui/react/merge-props';
import { useRender } from '@base-ui/react/use-render';
import { cx } from './cx';

const VARIANT = {
  neutral: 'bg-raised text-muted',
  accent: 'bg-accent-soft text-accent-fg',
  success: 'bg-grow-soft text-grow',
  warning: 'bg-warn-soft text-warn',
  danger: 'bg-danger-soft text-danger',
  info: 'bg-info-soft text-info',
  outline: 'border-rule bg-transparent text-ink',
} as const;

const SIZE = {
  sm: 'h-4 min-w-4 px-1 text-micro leading-none',
  md: 'h-5 min-w-5 px-1.5 text-micro',
  lg: 'h-6 min-w-6 px-2 text-small',
} as const;

export type BadgeVariant = keyof typeof VARIANT;
export type BadgeProps = useRender.ComponentProps<'span'> & {
  variant?: BadgeVariant;
  size?: keyof typeof SIZE;
};

/** A short label: a scope, a version, a count or a state. */
export function Badge({
  className,
  variant = 'neutral',
  size = 'md',
  render,
  ...props
}: BadgeProps) {
  const defaults = {
    className: cx(
      'loams-badge relative inline-flex shrink-0 items-center justify-center gap-1 whitespace-nowrap',
      'rounded-xs border border-transparent font-medium [&_svg]:size-3 [&_svg]:shrink-0',
      VARIANT[variant],
      SIZE[size],
      className,
    ),
    'data-slot': 'badge',
  };
  return useRender({
    defaultTagName: 'span',
    props: mergeProps<'span'>(defaults, props),
    render,
  });
}
