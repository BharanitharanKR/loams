// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/collapsible-section-header.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect. Loams' changes are Apache-2.0.

import type { ComponentProps, ReactNode } from 'react';
import { cx } from './cx';
import { ChevronDown } from './select';

const tones = {
  muted: { label: 'text-faint', line: 'bg-rule' },
  info: { label: 'text-info', line: 'bg-info-soft' },
  emphasized: { label: 'text-muted', line: 'bg-rule' },
  accent: { label: 'text-accent-fg', line: 'bg-accent-soft' },
} as const;

/** A section heading that folds its section: a label, a hairline and a chevron. */
export function CollapsibleSectionHeader({
  children,
  expanded,
  tone = 'muted',
  variant = 'default',
  accessory,
  ...buttonProps
}: Omit<ComponentProps<'button'>, 'className' | 'style' | 'aria-expanded'> & {
  expanded: boolean;
  tone?: keyof typeof tones;
  variant?: 'default' | 'panel';
  accessory?: ReactNode;
}) {
  return (
    <button
      {...buttonProps}
      type="button"
      aria-expanded={expanded}
      className={cx(
        'flex h-8 w-full cursor-pointer items-center gap-2 rounded-md border-0 bg-transparent text-left text-small font-medium',
        'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent',
        variant === 'panel' ? 'ps-2 pe-2.5' : 'px-2',
        tones[tone].label,
      )}
    >
      <span className="shrink-0">{children}</span>
      <span aria-hidden className={cx('h-px min-w-2 flex-1', tones[tone].line)} />
      {accessory}
      <ChevronDown
        className={cx(
          'size-3 shrink-0 transition-transform duration-(--op-duration) motion-reduce:transition-none',
          !expanded && 'rotate-180',
        )}
      />
    </button>
  );
}

export function SectionHeaderStatus({ children }: { children: ReactNode }) {
  return <span className="shrink-0 text-micro leading-none text-danger">{children}</span>;
}
