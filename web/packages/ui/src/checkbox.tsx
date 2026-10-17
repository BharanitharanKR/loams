// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/checkbox.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; exported as CheckboxControl (Checkbox is field.tsx's labelled checkbox). Loams' changes are Apache-2.0.

import { Checkbox as CheckboxPrimitive } from '@base-ui/react/checkbox';
import { cx, type WithClass } from './cx';

/** A checkbox on Base UI. Name it with aria-label or a wrapping <label>. */
export function CheckboxControl({ className, ...props }: WithClass<CheckboxPrimitive.Root.Props>) {
  return (
    <CheckboxPrimitive.Root
      className={cx(
        'relative inline-flex size-4 shrink-0 items-center justify-center rounded-xs border border-input bg-bg outline-none',
        'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent',
        'aria-invalid:border-danger data-disabled:opacity-60',
        className,
      )}
      data-slot="checkbox"
      {...props}
    >
      {/* The glyphs name their colour so rows that mute stray icons leave the indicator alone. */}
      <CheckboxPrimitive.Indicator
        className="absolute -inset-px flex items-center justify-center rounded-xs text-accent-ink data-unchecked:hidden data-checked:bg-accent data-indeterminate:text-ink"
        data-slot="checkbox-indicator"
        render={(props, state) => (
          <span {...props}>
            {state.indeterminate ? (
              <svg
                className="size-3 text-current"
                fill="none"
                height="24"
                stroke="currentColor"
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth="3"
                viewBox="0 0 24 24"
                width="24"
                aria-hidden="true"
              >
                <path d="M5.252 12h13.496" />
              </svg>
            ) : (
              <svg
                className="size-3 text-current"
                fill="none"
                height="24"
                stroke="currentColor"
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth="3"
                viewBox="0 0 24 24"
                width="24"
                aria-hidden="true"
              >
                <path d="M5.252 12.7 10.2 18.63 18.748 5.37" />
              </svg>
            )}
          </span>
        )}
      />
    </CheckboxPrimitive.Root>
  );
}
