// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/separator.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect. Loams' changes are Apache-2.0.

import { Separator as SeparatorPrimitive } from '@base-ui/react/separator';
import { cx, type WithClass } from './cx';

export function Separator({
  className,
  orientation = 'horizontal',
  ...props
}: WithClass<SeparatorPrimitive.Props>) {
  return (
    <SeparatorPrimitive
      className={cx(
        "shrink-0 bg-rule data-[orientation=horizontal]:h-px data-[orientation=horizontal]:w-full data-[orientation=vertical]:w-px data-[orientation=vertical]:not-[[class^='h-']]:not-[[class*='_h-']]:self-stretch",
        className,
      )}
      data-slot="separator"
      orientation={orientation}
      {...props}
    />
  );
}
