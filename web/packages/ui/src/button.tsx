// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/button.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// the cva table became the Loams .loams-btn classes (painted in styles.css on the design 59 tokens), so buttonClass keeps working outside React; Loams variant and size names kept; InlineButton dropped. Loams' changes are Apache-2.0.

import { mergeProps } from '@base-ui/react/merge-props';
import { useRender } from '@base-ui/react/use-render';
import { cx } from './cx';

export type ButtonVariant = 'primary' | 'secondary' | 'quiet' | 'danger' | 'danger-solid' | 'link';
export type ButtonSize = 'xs' | 'sm' | 'md' | 'lg' | 'icon' | 'icon-sm';

/** The class list for a button, for links styled as buttons and callers outside React. */
export function buttonClass({
  variant = 'secondary',
  size = 'md',
  className,
}: {
  variant?: ButtonVariant;
  size?: ButtonSize;
  className?: string;
} = {}): string {
  return cx('loams-btn', `loams-btn-${variant}`, size !== 'md' && `loams-btn-${size}`, className);
}

export type ButtonProps = useRender.ComponentProps<'button'> & {
  variant?: ButtonVariant;
  size?: ButtonSize;
};

/** The one action control. `type` defaults to "button"; `render` swaps the element. */
export function Button({ variant, size, className, render, ...props }: ButtonProps) {
  const defaults = {
    className: buttonClass({ variant, size, className }),
    'data-slot': 'button',
    'data-variant': variant ?? 'secondary',
    type: render ? undefined : ('button' as const),
  };
  return useRender({
    defaultTagName: 'button',
    props: mergeProps<'button'>(defaults, props),
    render,
  });
}
