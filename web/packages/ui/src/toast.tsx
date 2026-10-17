// Toasts on Base UI. Moved out of overlay.tsx unchanged in CF1 Task 5; Task 6 replaces
// them with T3 Code's toast.

import { Toast as BaseToast } from '@base-ui/react/toast';
import { cx } from './cx';

/** Holds the toasts; the shell mounts it once, with a `<Toaster />` inside. */
export const ToastProvider = BaseToast.Provider;

/** `add({ title, description, type: 'danger' })` shows a toast. */
export const useToast = BaseToast.useToastManager;

/** Where toasts appear: the bottom right, newest on top. */
export function Toaster() {
  const { toasts } = BaseToast.useToastManager();
  return (
    <BaseToast.Portal>
      <BaseToast.Viewport className="fixed right-4 bottom-4 z-[80] grid w-[min(360px,calc(100vw-32px))] gap-2">
        {toasts.map((t) => (
          <BaseToast.Root
            key={t.id}
            toast={t}
            className={cx(
              'border border-rule border-l-[3px] bg-bg px-3.5 py-2.5 text-[0.86rem] shadow-pop',
              t.type === 'danger' ? 'border-l-danger' : 'border-l-grow',
            )}
          >
            <BaseToast.Content className="flex items-start gap-3">
              <div className="grid min-w-0 flex-1 gap-0.5">
                <BaseToast.Title className="m-0 font-[540]" />
                <BaseToast.Description className="m-0 text-muted" />
              </div>
              <BaseToast.Close
                className="cursor-pointer text-muted hover:text-ink"
                aria-label="Dismiss"
              >
                ×
              </BaseToast.Close>
            </BaseToast.Content>
          </BaseToast.Root>
        ))}
      </BaseToast.Viewport>
    </BaseToast.Portal>
  );
}
