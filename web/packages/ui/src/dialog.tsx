// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/dialog.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; the root is DialogRoot and the Loams Dialog wrapper keeps its props; the media variant is dropped. Loams' changes are Apache-2.0.

import { Dialog as DialogPrimitive } from '@base-ui/react/dialog';
import type * as React from 'react';
import type { ReactNode } from 'react';
import { Button } from './button';
import { cx, type WithClass } from './cx';
import {
  DIALOG_BACKDROP_CLASS,
  DIALOG_MOBILE_SHEET_CLASS,
  DIALOG_POPUP_CLASS,
  DIALOG_VIEWPORT_CLASS,
} from './dialog-styles';
import { ScrollArea } from './scroll-area';

export const DialogRoot = DialogPrimitive.Root;

export const DialogPortal = DialogPrimitive.Portal;

export function DialogTrigger(props: DialogPrimitive.Trigger.Props) {
  return <DialogPrimitive.Trigger data-slot="dialog-trigger" {...props} />;
}

export function DialogClose(props: DialogPrimitive.Close.Props) {
  return <DialogPrimitive.Close data-slot="dialog-close" {...props} />;
}

export function DialogBackdrop({ className, ...props }: WithClass<DialogPrimitive.Backdrop.Props>) {
  return (
    <DialogPrimitive.Backdrop
      forceRender
      className={cx(DIALOG_BACKDROP_CLASS, className)}
      data-slot="dialog-backdrop"
      {...props}
    />
  );
}

export function DialogViewport({ className, ...props }: WithClass<DialogPrimitive.Viewport.Props>) {
  return (
    <DialogPrimitive.Viewport
      className={cx(DIALOG_VIEWPORT_CLASS, className)}
      data-slot="dialog-viewport"
      {...props}
    />
  );
}

/** The close glyph (16×16, in place of lucide's XIcon). */
export const CloseGlyph = () => (
  <svg
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth={2}
    strokeLinecap="round"
    aria-hidden="true"
    className="size-4"
  >
    <path d="M4 4l8 8M12 4l-8 8" />
  </svg>
);

export function DialogPopup({
  className,
  children,
  showCloseButton = true,
  bottomStickOnMobile = true,
  ...props
}: WithClass<DialogPrimitive.Popup.Props> & {
  showCloseButton?: boolean;
  bottomStickOnMobile?: boolean;
}) {
  return (
    <DialogPortal>
      <DialogBackdrop />
      <DialogViewport
        className={cx(bottomStickOnMobile && 'max-sm:grid-rows-[1fr_auto] max-sm:p-0 max-sm:pt-12')}
      >
        <DialogPrimitive.Popup
          className={cx(
            DIALOG_POPUP_CLASS,
            'row-start-2 max-h-full max-w-lg',
            bottomStickOnMobile && DIALOG_MOBILE_SHEET_CLASS,
            className,
          )}
          data-slot="dialog-popup"
          {...props}
        >
          {children}
          {showCloseButton && (
            <DialogPrimitive.Close
              aria-label="Close"
              className="absolute end-2 top-2"
              render={<Button size="icon-sm" variant="quiet" />}
            >
              <CloseGlyph />
            </DialogPrimitive.Close>
          )}
        </DialogPrimitive.Popup>
      </DialogViewport>
    </DialogPortal>
  );
}

export function DialogHeader({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      className={cx(
        'flex flex-col gap-2 p-6 in-[[data-slot=dialog-popup]:has([data-slot=dialog-panel])]:pb-3 max-sm:pb-4',
        className,
      )}
      data-slot="dialog-header"
      {...props}
    />
  );
}

export function DialogFooter({
  className,
  variant = 'default',
  ...props
}: React.ComponentProps<'div'> & {
  variant?: 'default' | 'bare';
}) {
  return (
    <div
      className={cx(
        'flex flex-col-reverse gap-2 px-6 sm:flex-row sm:justify-end sm:rounded-b-[calc(var(--op-radius-xl)-1px)]',
        variant === 'default' && 'border-t border-rule-soft bg-raised py-3',
        variant === 'bare' && 'py-4',
        className,
      )}
      data-slot="dialog-footer"
      {...props}
    />
  );
}

export function DialogTitle({ className, ...props }: WithClass<DialogPrimitive.Title.Props>) {
  return (
    <DialogPrimitive.Title
      className={cx('m-0 wrap-anywhere text-section font-semibold leading-none', className)}
      data-slot="dialog-title"
      {...props}
    />
  );
}

export function DialogDescription({
  className,
  ...props
}: WithClass<DialogPrimitive.Description.Props>) {
  return (
    <DialogPrimitive.Description
      className={cx('m-0 text-body text-muted', className)}
      data-slot="dialog-description"
      {...props}
    />
  );
}

export function DialogPanel({
  className,
  scrollFade = true,
  ...props
}: React.ComponentProps<'div'> & { scrollFade?: boolean }) {
  return (
    <ScrollArea fade={scrollFade}>
      <div
        className={cx(
          'space-y-4 p-6 in-[[data-slot=dialog-popup]:has([data-slot=dialog-header])]:pt-1 in-[[data-slot=dialog-popup]:has([data-slot=dialog-footer]:not(.border-t))]:pb-1',
          className,
        )}
        data-slot="dialog-panel"
        {...props}
      />
    </ScrollArea>
  );
}

/* The Loams wrapper ------------------------------------------------------------------- */

/**
 * A modal. Focus moves in and is trapped, Escape and a click on the scrim close it,
 * and the page behind is inert. `dismissible={false}` holds it open while work is pending.
 */
export function Dialog({
  open,
  onClose,
  title,
  children,
  footer,
  dismissible = true,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  /** False keeps Escape, the scrim and the close button from dismissing it. */
  dismissible?: boolean;
}) {
  return (
    <DialogRoot
      open={open}
      onOpenChange={(next) => {
        if (!next && dismissible) onClose();
      }}
      disablePointerDismissal={!dismissible}
    >
      <DialogPopup showCloseButton={false} className="loams-dialog">
        <div className="loams-dialog-head">
          {/* Base UI renders the title as an <h2>. */}
          <DialogTitle>{title}</DialogTitle>
          <DialogClose
            render={<Button variant="quiet" size="icon-sm" />}
            aria-label="Close"
            disabled={!dismissible}
          >
            <CloseGlyph />
          </DialogClose>
        </div>
        <div className="loams-dialog-body">{children}</div>
        {footer && <div className="loams-dialog-foot">{footer}</div>}
      </DialogPopup>
    </DialogRoot>
  );
}
