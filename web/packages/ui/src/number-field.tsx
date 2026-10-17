// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/number-field.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; lucide's minus and plus are inline SVGs. Loams' changes are Apache-2.0.

import { NumberField as NumberFieldPrimitive } from '@base-ui/react/number-field';
import * as React from 'react';
import { cx, type WithClass } from './cx';

const Glyph = ({ d }: { d: string }) => (
  <svg
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth={2}
    strokeLinecap="round"
    aria-hidden="true"
    className="size-4"
  >
    <path d={d} />
  </svg>
);

/** A number with steppers. Wrap NumberFieldInput and the steppers in NumberFieldGroup. */
export function NumberField({
  id,
  className,
  size = 'default',
  ...props
}: WithClass<NumberFieldPrimitive.Root.Props> & {
  size?: 'sm' | 'default' | 'lg';
}): React.ReactElement {
  const generatedId = React.useId();
  const fieldId = id ?? generatedId;
  return (
    <NumberFieldPrimitive.Root
      className={cx('flex w-full flex-col items-start gap-2', className)}
      data-size={size}
      data-slot="number-field"
      id={fieldId}
      {...props}
    />
  );
}

export function NumberFieldGroup({
  className,
  ...props
}: WithClass<NumberFieldPrimitive.Group.Props>): React.ReactElement {
  return (
    <NumberFieldPrimitive.Group
      className={cx(
        'relative flex w-full justify-between rounded-md border border-input bg-bg text-body text-ink',
        'has-focus-visible:outline-2 has-focus-visible:outline-offset-2 has-focus-visible:outline-accent',
        'has-aria-invalid:border-danger data-disabled:pointer-events-none data-disabled:opacity-60',
        '[&_svg]:pointer-events-none [&_svg]:shrink-0',
        className,
      )}
      data-slot="number-field-group"
      {...props}
    />
  );
}

const STEPPER =
  'relative flex shrink-0 cursor-pointer items-center justify-center border-0 bg-transparent px-2.5 text-muted ' +
  'transition-colors duration-(--op-duration) hover:bg-raised hover:text-ink motion-reduce:transition-none';

export function NumberFieldDecrement({
  className,
  ...props
}: WithClass<NumberFieldPrimitive.Decrement.Props>): React.ReactElement {
  return (
    <NumberFieldPrimitive.Decrement
      className={cx(STEPPER, 'rounded-s-md', className)}
      data-slot="number-field-decrement"
      {...props}
    >
      <Glyph d="M3 8h10" />
    </NumberFieldPrimitive.Decrement>
  );
}

export function NumberFieldIncrement({
  className,
  ...props
}: WithClass<NumberFieldPrimitive.Increment.Props>): React.ReactElement {
  return (
    <NumberFieldPrimitive.Increment
      className={cx(STEPPER, 'rounded-e-md', className)}
      data-slot="number-field-increment"
      {...props}
    >
      <Glyph d="M8 3v10M3 8h10" />
    </NumberFieldPrimitive.Increment>
  );
}

export function NumberFieldInput({
  className,
  ...props
}: WithClass<NumberFieldPrimitive.Input.Props>): React.ReactElement {
  return (
    <NumberFieldPrimitive.Input
      className={cx(
        'h-9 w-full min-w-0 grow bg-transparent px-3 text-center tabular-nums leading-9 outline-none',
        'in-data-[size=sm]:h-7 in-data-[size=sm]:px-2.5 in-data-[size=sm]:leading-7',
        'in-data-[size=lg]:h-10 in-data-[size=lg]:leading-10',
        className,
      )}
      data-slot="number-field-input"
      {...props}
    />
  );
}
