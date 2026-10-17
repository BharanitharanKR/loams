// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/textarea.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; exported as TextareaControl (Textarea is field.tsx's). Loams' changes are Apache-2.0.

import { Field as FieldPrimitive } from '@base-ui/react/field';
import { mergeProps } from '@base-ui/react/merge-props';
import type * as React from 'react';
import { cx } from './cx';
import { CONTROL_BOX } from './input';

export type TextareaControlProps = React.ComponentProps<'textarea'> & {
  size?: 'sm' | 'default' | 'lg' | number;
  unstyled?: boolean;
};

/** A textarea that grows with its content, in the outlined box. */
export function TextareaControl({
  className,
  size = 'default',
  unstyled = false,
  ...props
}: TextareaControlProps) {
  return (
    <span
      className={cx(!unstyled && CONTROL_BOX, className) || undefined}
      data-size={size}
      data-slot="textarea-control"
    >
      <FieldPrimitive.Control
        render={(defaultProps) => (
          <textarea
            className={cx(
              'field-sizing-content min-h-18 max-h-64 w-full rounded-[inherit] border-0 bg-transparent px-3 py-2 font-[inherit] text-inherit outline-none wrap-anywhere placeholder:text-faint',
              size === 'sm' ? 'min-h-16 px-2.5 py-1.5 text-small' : 'text-[length:inherit]',
              size === 'lg' && 'min-h-20 py-2.5',
            )}
            data-slot="textarea"
            {...mergeProps(defaultProps, props)}
          />
        )}
      />
    </span>
  );
}
