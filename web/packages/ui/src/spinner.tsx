// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/spinner.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to Loams tokens, rounded-none and rounded-pill, and cx, with Base UI underneath; no cva, lucide or Effect. Loams' changes are Apache-2.0.

// A busy indicator: a labelled status with a turning arc in the current colour. Reduced
// motion keeps the arc still. lucide's LoaderCircle is replaced with the same arc inline,
// so @loams/ui takes no icon dependency.

import { cx } from './cx';

const SIZES = { xs: 'size-3', sm: 'size-3.5', md: 'size-4', lg: 'size-5' } as const;

export function Spinner({
  size = 'md',
  tone = 'current',
  label = 'Loading',
  className,
}: {
  size?: keyof typeof SIZES;
  /** `muted` for a spinner beside secondary text; `current` follows the parent's colour. */
  tone?: 'current' | 'muted';
  /** What is loading, for assistive technology. */
  label?: string;
  className?: string;
}) {
  return (
    <svg
      role="status"
      aria-label={label}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      className={cx(
        'inline-block shrink-0 motion-safe:animate-spin',
        SIZES[size],
        tone === 'muted' && 'text-muted',
        className,
      )}
      data-slot="spinner"
    >
      <path d="M21 12a9 9 0 1 1-6.219-8.56" />
    </svg>
  );
}
