// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/alert-dialog.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; the root is AlertDialogRoot and the Loams AlertDialog wrapper keeps its props. Loams' changes are Apache-2.0.

import { AlertDialog as AlertDialogPrimitive } from '@base-ui/react/alert-dialog';
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

export const AlertDialogRoot = AlertDialogPrimitive.Root;

export const AlertDialogPortal = AlertDialogPrimitive.Portal;

export function AlertDialogBackdrop({
  className,
  ...props
}: WithClass<AlertDialogPrimitive.Backdrop.Props>) {
  return (
    <AlertDialogPrimitive.Backdrop
      forceRender
      className={cx(DIALOG_BACKDROP_CLASS, className)}
      data-slot="alert-dialog-backdrop"
      {...props}
    />
  );
}

export function AlertDialogViewport({
  className,
  ...props
}: WithClass<AlertDialogPrimitive.Viewport.Props>) {
  return (
    <AlertDialogPrimitive.Viewport
      className={cx(DIALOG_VIEWPORT_CLASS, className)}
      data-slot="alert-dialog-viewport"
      {...props}
    />
  );
}

export function AlertDialogPopup({
  className,
  bottomStickOnMobile = true,
  portalContainer,
  ...props
}: WithClass<AlertDialogPrimitive.Popup.Props> & {
  bottomStickOnMobile?: boolean;
  portalContainer?: AlertDialogPrimitive.Portal.Props['container'];
}) {
  return (
    <AlertDialogPortal container={portalContainer}>
      <AlertDialogBackdrop />
      <AlertDialogViewport
        className={cx(bottomStickOnMobile && 'max-sm:grid-rows-[1fr_auto] max-sm:p-0 max-sm:pt-12')}
      >
        <AlertDialogPrimitive.Popup
          className={cx(
            DIALOG_POPUP_CLASS,
            'row-start-2 max-h-full max-w-lg',
            bottomStickOnMobile && DIALOG_MOBILE_SHEET_CLASS,
            className,
          )}
          data-slot="alert-dialog-popup"
          {...props}
        />
      </AlertDialogViewport>
    </AlertDialogPortal>
  );
}

export function AlertDialogHeader({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      className={cx('flex min-h-0 flex-col gap-2 px-5 pt-5 pb-4 text-left', className)}
      data-slot="alert-dialog-header"
      {...props}
    />
  );
}

export function AlertDialogFooter({
  className,
  variant = 'default',
  ...props
}: React.ComponentProps<'div'> & {
  variant?: 'default' | 'bare';
}) {
  return (
    <div
      className={cx(
        'flex flex-col-reverse gap-2 px-5 sm:flex-row sm:justify-end sm:rounded-b-[calc(var(--op-radius-xl)-1px)]',
        variant === 'default' && 'border-t border-rule-soft bg-raised py-3',
        variant === 'bare' && 'pb-5',
        className,
      )}
      data-slot="alert-dialog-footer"
      {...props}
    />
  );
}

export function AlertDialogTitle({
  className,
  ...props
}: WithClass<AlertDialogPrimitive.Title.Props>) {
  return (
    <AlertDialogPrimitive.Title
      className={cx(
        'm-0 wrap-anywhere shrink-0 text-section font-semibold leading-snug',
        className,
      )}
      data-slot="alert-dialog-title"
      {...props}
    />
  );
}

export function AlertDialogDescription({
  className,
  ...props
}: WithClass<AlertDialogPrimitive.Description.Props>) {
  return (
    <AlertDialogPrimitive.Description
      className={cx(
        'm-0 wrap-anywhere max-h-[min(24rem,60dvh)] min-h-0 select-text overflow-y-auto overscroll-contain text-body text-muted',
        className,
      )}
      data-slot="alert-dialog-description"
      {...props}
    />
  );
}

export function AlertDialogClose(props: AlertDialogPrimitive.Close.Props) {
  return <AlertDialogPrimitive.Close data-slot="alert-dialog-close" {...props} />;
}

/* The Loams wrapper ------------------------------------------------------------------- */

/**
 * A confirmation that interrupts: Base UI's alert dialog (role `alertdialog`), so a click
 * on the scrim does not dismiss it and focus starts inside. Escape and Cancel call
 * `onClose`; the confirm button calls `onConfirm`. While `pending`, nothing dismisses it.
 */
export function AlertDialog({
  open,
  onClose,
  title,
  children,
  confirm,
  cancel = 'Cancel',
  onConfirm,
  tone,
  pending = false,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  /** The consequence, read as the dialog's description. */
  children?: ReactNode;
  /** The confirm button's label: the action, never "OK". */
  confirm: ReactNode;
  cancel?: ReactNode;
  onConfirm: () => void;
  tone?: 'danger';
  pending?: boolean;
}) {
  return (
    <AlertDialogRoot
      open={open}
      onOpenChange={(next) => {
        if (!next && !pending) onClose();
      }}
    >
      <AlertDialogPopup>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          {children && <AlertDialogDescription>{children}</AlertDialogDescription>}
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogClose render={<Button variant="quiet" />} disabled={pending}>
            {cancel}
          </AlertDialogClose>
          <Button
            variant={tone === 'danger' ? 'danger-solid' : 'primary'}
            onClick={onConfirm}
            disabled={pending}
            aria-busy={pending || undefined}
          >
            {confirm}
          </Button>
        </AlertDialogFooter>
      </AlertDialogPopup>
    </AlertDialogRoot>
  );
}
