// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/sheet.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; the root is SheetRoot and the Loams Sheet wrapper keeps its props; the sides are the Loams widths, and the slide snaps unless motion is on (D1076). Loams' changes are Apache-2.0.

import { Dialog as SheetPrimitive } from '@base-ui/react/dialog';
import type * as React from 'react';
import type { CSSProperties, ReactNode } from 'react';
import { Button } from './button';
import { cx, type WithClass } from './cx';
import { CloseGlyph } from './dialog';
import { DIALOG_BACKDROP_CLASS } from './dialog-styles';

export const SheetRoot = SheetPrimitive.Root;

export const SheetPortal = SheetPrimitive.Portal;

/** Where a sheet comes from, and how wide or tall it is. */
const SHEET_SIDE = {
  left: 'h-full w-[min(88vw,360px)] border-e data-starting-style:-translate-x-full data-ending-style:-translate-x-full',
  right:
    'h-full w-[min(88vw,420px)] border-s data-starting-style:translate-x-full data-ending-style:translate-x-full',
  bottom:
    'row-start-2 max-h-[85dvh] w-full border-t data-starting-style:translate-y-full data-ending-style:translate-y-full',
  top: 'max-h-[85dvh] w-full border-b data-starting-style:-translate-y-full data-ending-style:-translate-y-full',
} as const;

export type SheetSide = keyof typeof SHEET_SIDE;

export function SheetBackdrop({ className, ...props }: WithClass<SheetPrimitive.Backdrop.Props>) {
  return (
    <SheetPrimitive.Backdrop
      className={cx(DIALOG_BACKDROP_CLASS, className)}
      data-slot="sheet-backdrop"
      {...props}
    />
  );
}

export function SheetViewport({
  className,
  side,
  variant = 'default',
  ...props
}: WithClass<SheetPrimitive.Viewport.Props> & {
  side?: SheetSide;
  variant?: 'default' | 'inset';
}) {
  return (
    <SheetPrimitive.Viewport
      className={cx(
        'fixed inset-0 z-[71] grid',
        side === 'bottom' && 'grid grid-rows-[1fr_auto] pt-12',
        side === 'top' && 'grid grid-rows-[auto_1fr] pb-12',
        side === 'left' && 'flex justify-start',
        side === 'right' && 'flex justify-end',
        variant === 'inset' && 'sm:p-4',
        className,
      )}
      data-slot="sheet-viewport"
      {...props}
    />
  );
}

export function SheetPopup({
  className,
  children,
  showCloseButton = true,
  keepMounted = false,
  transitionDurationMs,
  side = 'right',
  variant = 'default',
  style,
  ...props
}: WithClass<SheetPrimitive.Popup.Props> & {
  showCloseButton?: boolean;
  keepMounted?: boolean;
  transitionDurationMs?: number;
  side?: SheetSide;
  variant?: 'default' | 'inset';
}) {
  const transitionStyle =
    transitionDurationMs === undefined
      ? undefined
      : ({ transitionDuration: `${transitionDurationMs}ms` } satisfies CSSProperties);
  const instant = transitionDurationMs === 0;

  return (
    <SheetPortal keepMounted={keepMounted}>
      <SheetBackdrop
        className={
          instant
            ? 'transition-none! data-ending-style:opacity-100! data-starting-style:opacity-100!'
            : undefined
        }
        style={transitionStyle}
      />
      <SheetViewport side={side} variant={variant}>
        <SheetPrimitive.Popup
          className={cx(
            'relative flex max-h-full min-h-0 min-w-0 flex-col border-rule bg-surface text-ink shadow-modal outline-none ' +
              'transition-[opacity,translate] duration-(--op-panel-duration) ease-panel motion-reduce:transition-none ' +
              '[-webkit-app-region:no-drag]',
            SHEET_SIDE[side],
            variant === 'inset' && 'sm:rounded-xl sm:border',
            instant &&
              'transition-none! data-ending-style:translate-x-0! data-starting-style:translate-x-0! data-ending-style:translate-y-0! data-starting-style:translate-y-0! data-ending-style:opacity-100! data-starting-style:opacity-100!',
            className,
          )}
          data-side={side}
          data-slot="sheet-popup"
          style={{ ...transitionStyle, ...style }}
          {...props}
        >
          {children}
          {showCloseButton && (
            <SheetPrimitive.Close
              aria-label="Close"
              className="absolute end-2 top-2"
              render={<Button size="icon-sm" variant="quiet" />}
            >
              <CloseGlyph />
            </SheetPrimitive.Close>
          )}
        </SheetPrimitive.Popup>
      </SheetViewport>
    </SheetPortal>
  );
}

export function SheetHeader({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      className={cx(
        'flex flex-col gap-2 p-5 in-[[data-slot=sheet-popup]:has([data-slot=sheet-panel])]:pb-3 max-sm:pb-4',
        className,
      )}
      data-slot="sheet-header"
      {...props}
    />
  );
}

export function SheetTitle({ className, ...props }: WithClass<SheetPrimitive.Title.Props>) {
  return (
    <SheetPrimitive.Title
      className={cx('m-0 text-section font-semibold leading-none', className)}
      data-slot="sheet-title"
      {...props}
    />
  );
}

export function SheetDescription({
  className,
  ...props
}: WithClass<SheetPrimitive.Description.Props>) {
  return (
    <SheetPrimitive.Description
      className={cx('m-0 text-body text-muted', className)}
      data-slot="sheet-description"
      {...props}
    />
  );
}

/* The Loams wrapper ------------------------------------------------------------------- */

/**
 * A modal panel from one edge: the sidebar or the side panel on a narrow window, a
 * bottom sheet on a phone. Focus is trapped, Escape and the scrim close it.
 */
export function Sheet({
  open,
  onClose,
  title,
  side = 'right',
  children,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  side?: SheetSide;
  children: ReactNode;
}) {
  return (
    <SheetRoot open={open} onOpenChange={(next) => !next && onClose()}>
      <SheetPopup side={side} showCloseButton={false}>
        <div className="flex h-11 shrink-0 items-center justify-between gap-3 border-b border-rule-soft px-3">
          <SheetPrimitive.Title
            className="m-0 truncate text-body font-medium"
            data-slot="sheet-title"
          >
            {title}
          </SheetPrimitive.Title>
          <SheetPrimitive.Close
            render={<Button variant="quiet" size="icon-sm" />}
            aria-label="Close"
          >
            <CloseGlyph />
          </SheetPrimitive.Close>
        </div>
        <div className="min-h-0 flex-1 overflow-auto">{children}</div>
      </SheetPopup>
    </SheetRoot>
  );
}
