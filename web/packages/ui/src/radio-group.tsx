// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/radio-group.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect. Loams' changes are Apache-2.0.

import { Radio as RadioPrimitive } from '@base-ui/react/radio';
import { RadioGroup as RadioGroupPrimitive } from '@base-ui/react/radio-group';
import { cx, type WithClass } from './cx';

/** One choice from a few. The arrow keys move the choice. */
export function RadioGroup({ className, ...props }: WithClass<RadioGroupPrimitive.Props>) {
  return (
    <RadioGroupPrimitive
      className={cx('flex flex-col gap-3', className)}
      data-slot="radio-group"
      {...props}
    />
  );
}

export function Radio({ className, ...props }: WithClass<RadioPrimitive.Root.Props>) {
  return (
    <RadioPrimitive.Root
      className={cx(
        'relative inline-flex size-4 shrink-0 items-center justify-center rounded-pill border border-input bg-bg outline-none',
        'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent',
        'aria-invalid:border-danger data-disabled:opacity-60',
        className,
      )}
      data-slot="radio"
      {...props}
    >
      <RadioPrimitive.Indicator
        className="absolute -inset-px flex items-center justify-center rounded-pill before:size-1.5 before:rounded-pill before:bg-accent-ink data-unchecked:hidden data-checked:bg-accent"
        data-slot="radio-indicator"
      />
    </RadioPrimitive.Root>
  );
}

export { Radio as RadioGroupItem };
