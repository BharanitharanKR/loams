// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/skeleton.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to Loams tokens, rounded-none and rounded-pill, and cx, with Base UI underneath; no cva, lucide or Effect. Loams' changes are Apache-2.0.

import type { ComponentProps } from 'react';
import { cx } from './cx';

// A skeleton's width and height are its content, so callers size it through className.
// Its shape is not: a block (hard corners) or a pill.
const SHAPES = { block: 'rounded-md', pill: 'rounded-pill' } as const;

/** A placeholder while content loads. Hidden from assistive technology; pair it with a status. */
export function Skeleton({
  className,
  shape = 'block',
  ...props
}: ComponentProps<'div'> & { shape?: keyof typeof SHAPES }) {
  return (
    <div
      aria-hidden="true"
      className={cx('bg-raised motion-safe:animate-pulse', SHAPES[shape], className)}
      data-slot="skeleton"
      {...props}
    />
  );
}
