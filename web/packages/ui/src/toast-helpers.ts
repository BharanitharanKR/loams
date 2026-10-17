// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/toastHelpers.ts at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx; renamed stackedToast over the Loams ToastData; the thread fields removed. Loams' changes are Apache-2.0.

// `stackedToast` (upstream's `stackedThreadToast`) builds a toast whose actions sit on their
// own row at the end; `hiddenToastActionProps` removes an action on `update`.

import type { ToastManagerAddOptions } from '@base-ui/react/toast';
import type { ComponentPropsWithoutRef, ReactNode } from 'react';

import type { ToastData } from './toast';

export type StackedToastOptions = {
  type: 'error' | 'warning' | 'success' | 'info' | 'loading';
  title: ReactNode;
  description?: ReactNode;
  timeout?: number;
  priority?: 'low' | 'high';
  actionProps?: ComponentPropsWithoutRef<'button'>;
  actionVariant?: ToastData['actionVariant'];
  data?: Omit<ToastData, 'actionLayout'>;
};

/**
 * Defined `actionProps` that hide a previous toast CTA on `toastManager.update`.
 * Passing `actionProps: undefined` is a no-op because updates omit undefined keys.
 */
export const hiddenToastActionProps = {
  children: null,
} as const satisfies Pick<ComponentPropsWithoutRef<'button'>, 'children'>;

export function stackedToast(options: StackedToastOptions): ToastManagerAddOptions<ToastData> {
  const { type, title, description, timeout, priority, actionProps, actionVariant, data } = options;

  const mergedData: ToastData = {
    ...(data !== undefined ? data : {}),
    actionLayout: 'stacked-end',
  };
  if (actionVariant !== undefined) {
    mergedData.actionVariant = actionVariant;
  }

  const payload: ToastManagerAddOptions<ToastData> = {
    type,
    title,
    data: mergedData,
  };

  if (description !== undefined) {
    payload.description = description;
  }
  if (timeout !== undefined) {
    payload.timeout = timeout;
  }
  if (priority !== undefined) {
    payload.priority = priority;
  }
  if (actionProps !== undefined) {
    payload.actionProps = actionProps;
  }

  return payload;
}
