// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/scroll-area.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to Loams tokens, rounded-none and rounded-pill, and cx, with Base UI underneath; no cva, lucide or Effect. Loams' changes are Apache-2.0.

// A scrolling region with thin overlay scrollbars that appear on hover and while scrolling,
// and an optional fade at the edges that have more content.

import { ScrollArea as BaseScrollArea } from '@base-ui/react/scroll-area';
import type { ReactNode } from 'react';
import { cx } from './cx';

export function ScrollArea({
  children,
  className,
  label,
  fade = false,
  fadePadding = true,
  hideScrollbars = false,
  viewportTabIndex,
}: {
  children: ReactNode;
  className?: string;
  /** A name makes the viewport a keyboard-reachable region. */
  label?: string;
  /** Fades the edges that have more content to scroll to. */
  fade?: boolean;
  /**
   * Keeps focused and highlighted items clear of the fade. Off for lists whose rows take
   * focus on click, where the scroll padding would nudge the list.
   */
  fadePadding?: boolean;
  hideScrollbars?: boolean;
  /** Overrides the viewport's tab stop when focusable rows already give scroll access. */
  viewportTabIndex?: number;
}) {
  const tabIndex = viewportTabIndex ?? (label ? 0 : undefined);
  return (
    <BaseScrollArea.Root
      className={cx('relative size-full min-h-0 overflow-hidden', className)}
      data-slot="scroll-area"
    >
      <BaseScrollArea.Viewport
        {...(label ? { role: 'region', 'aria-label': label } : {})}
        {...(tabIndex === undefined ? {} : { tabIndex })}
        className={cx(
          'h-full max-h-[inherit] overflow-auto overscroll-contain outline-none ' +
            'focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent',
          fade &&
            '[--fade-size:1.5rem] focus-visible:mask-none ' +
              'mask-t-from-[calc(100%-min(var(--fade-size),var(--scroll-area-overflow-y-start)))] ' +
              'mask-b-from-[calc(100%-min(var(--fade-size),var(--scroll-area-overflow-y-end)))]',
          fade && fadePadding && 'scroll-p-[var(--fade-size)]',
          fade && !fadePadding && 'has-focus-visible:mask-none',
          hideScrollbars && '[scrollbar-width:none] [&::-webkit-scrollbar]:hidden',
        )}
        data-slot="scroll-area-viewport"
      >
        {children}
      </BaseScrollArea.Viewport>
      {!hideScrollbars && (
        <>
          <ScrollBar orientation="vertical" />
          <ScrollBar orientation="horizontal" />
          <BaseScrollArea.Corner />
        </>
      )}
    </BaseScrollArea.Root>
  );
}

function ScrollBar({ orientation }: { orientation: 'vertical' | 'horizontal' }) {
  return (
    <BaseScrollArea.Scrollbar
      orientation={orientation}
      className={
        'flex opacity-0 transition-opacity delay-300 data-hovering:opacity-100 data-scrolling:opacity-100 ' +
        'data-hovering:delay-0 data-scrolling:delay-0 ' +
        'data-[orientation=horizontal]:mx-1 data-[orientation=horizontal]:h-1.5 data-[orientation=horizontal]:flex-col ' +
        'data-[orientation=vertical]:my-1 data-[orientation=vertical]:w-1.5'
      }
      data-slot="scroll-area-scrollbar"
    >
      <BaseScrollArea.Thumb className="relative flex-1 rounded-pill bg-rule hover:bg-faint" />
    </BaseScrollArea.Scrollbar>
  );
}
