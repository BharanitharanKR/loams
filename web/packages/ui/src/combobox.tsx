// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/combobox.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; the root is ComboboxRoot (Combobox is the Loams wrapper); the virtualised list is dropped (long lists use VirtualList). Loams' changes are Apache-2.0.

import { Combobox as ComboboxPrimitive } from '@base-ui/react/combobox';
import * as React from 'react';
import { cx, type WithClass } from './cx';
import { InputControl } from './input';
import { ScrollArea } from './scroll-area';
import { GROUP_LABEL, POPUP_ITEM, POPUP_SURFACE } from './select';

const ComboboxContext = React.createContext<{
  chipsRef: React.RefObject<Element | null> | null;
  multiple: boolean;
}>({
  chipsRef: null,
  multiple: false,
});

/** Inline 16×16 glyphs in place of lucide's. */
export const ChevronsUpDown = () => (
  <svg
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth={2}
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    <path d="M5 6l3-3 3 3M5 10l3 3 3-3" />
  </svg>
);
export const Cross = () => (
  <svg
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth={2}
    strokeLinecap="round"
    aria-hidden="true"
  >
    <path d="M4 4l8 8M12 4l-8 8" />
  </svg>
);
const Search = () => (
  <svg
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth={2}
    strokeLinecap="round"
    aria-hidden="true"
    className="pointer-events-none absolute top-1.5 left-0 size-4 shrink-0 text-faint"
  >
    <path d="M7 12A5 5 0 1 0 7 2a5 5 0 0 0 0 10zM14 14l-3.5-3.5" />
  </svg>
);

/** The small square button inside an input (open the list, clear it). */
export const INPUT_BUTTON =
  'absolute top-1/2 inline-flex size-7 -translate-y-1/2 shrink-0 cursor-pointer items-center justify-center ' +
  'rounded-sm border-0 bg-transparent text-muted outline-none hover:text-ink ' +
  'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent ' +
  'has-[+[data-slot$=-clear]]:hidden [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0';

/** The popup's positioner layer: above dialogs (71), below toasts (80). */
export const POPUP_LAYER = 'z-[75] select-none';

export function ComboboxRoot<Value, Multiple extends boolean | undefined = false>(
  props: ComboboxPrimitive.Root.Props<Value, Multiple>,
) {
  const chipsRef = React.useRef<Element | null>(null);
  const value = React.useMemo(() => ({ chipsRef, multiple: !!props.multiple }), [props.multiple]);
  return (
    <ComboboxContext value={value}>
      <ComboboxPrimitive.Root {...props} />
    </ComboboxContext>
  );
}

export function ComboboxInput({
  className,
  showTrigger = true,
  showClear = false,
  startAddon,
  size,
  unstyled = false,
  ...props
}: Omit<WithClass<ComboboxPrimitive.Input.Props>, 'size'> & {
  showTrigger?: boolean;
  showClear?: boolean;
  startAddon?: React.ReactNode;
  size?: 'sm' | 'default' | 'lg' | number;
  unstyled?: boolean;
  ref?: React.Ref<HTMLInputElement>;
}) {
  const sizeValue = size ?? 'default';

  return (
    <div className="relative w-full text-ink has-disabled:opacity-60 not-has-[>*.w-full]:w-fit">
      {startAddon && (
        <div
          className="pointer-events-none absolute inset-y-0 start-px z-10 flex items-center ps-3 text-muted [&_svg]:size-4"
          data-slot="combobox-start-addon"
        >
          {startAddon}
        </div>
      )}
      <ComboboxPrimitive.Input
        className={cx(
          !!startAddon && '*:data-[slot=combobox-input]:ps-8',
          'has-[+[data-slot=combobox-trigger],+[data-slot=combobox-clear]]:*:data-[slot=combobox-input]:pe-8',
          className,
        )}
        data-slot="combobox-input"
        render={
          <InputControl
            className={cx(
              'has-disabled:opacity-100',
              unstyled && 'rounded-none bg-transparent text-body',
            )}
            nativeInput
            size={sizeValue}
            unstyled={unstyled}
          />
        }
        {...props}
      />
      {showTrigger && (
        <ComboboxTrigger className={cx(INPUT_BUTTON, 'end-1')}>
          <ComboboxPrimitive.Icon data-slot="combobox-icon">
            <ChevronsUpDown />
          </ComboboxPrimitive.Icon>
        </ComboboxTrigger>
      )}
      {showClear && (
        <ComboboxClear className={cx(INPUT_BUTTON, 'end-1')} aria-label="Clear">
          <Cross />
        </ComboboxClear>
      )}
    </div>
  );
}

/** A search box at the top of a popup, for a combobox opened from a button. */
export function ComboboxSearchInput(props: React.ComponentProps<typeof ComboboxInput>) {
  return (
    <div className="min-w-0 shrink-0 px-3 pt-2.5">
      <div className="relative border-b border-rule pb-1.5 transition-colors duration-(--op-duration) focus-within:border-accent motion-reduce:transition-none">
        <Search />
        <ComboboxInput
          {...props}
          className="[&_input]:h-6.5 [&_input]:ps-5 [&_input]:leading-6.5"
          showTrigger={false}
          size="sm"
          unstyled
        />
      </div>
    </div>
  );
}

export function ComboboxTrigger({
  className,
  children,
  ...props
}: WithClass<ComboboxPrimitive.Trigger.Props>) {
  return (
    <ComboboxPrimitive.Trigger className={className} data-slot="combobox-trigger" {...props}>
      {children}
    </ComboboxPrimitive.Trigger>
  );
}

export function ComboboxPopup({
  className,
  children,
  side = 'bottom',
  sideOffset = 4,
  alignOffset,
  align = 'start',
  anchor: anchorProp,
  ...props
}: WithClass<ComboboxPrimitive.Popup.Props> & {
  align?: ComboboxPrimitive.Positioner.Props['align'];
  sideOffset?: ComboboxPrimitive.Positioner.Props['sideOffset'];
  alignOffset?: ComboboxPrimitive.Positioner.Props['alignOffset'];
  side?: ComboboxPrimitive.Positioner.Props['side'];
  anchor?: ComboboxPrimitive.Positioner.Props['anchor'];
}) {
  const { chipsRef, multiple } = React.use(ComboboxContext);
  // Chips are not vendored, so the chips ref anchors only once something attaches it.
  const anchor = anchorProp ?? (multiple && chipsRef?.current ? chipsRef : undefined);

  return (
    <ComboboxPrimitive.Portal>
      <ComboboxPrimitive.Positioner
        align={align}
        alignOffset={alignOffset}
        anchor={anchor}
        className={POPUP_LAYER}
        data-slot="combobox-positioner"
        side={side}
        sideOffset={sideOffset}
      >
        <span
          className={cx(
            'relative flex max-h-full min-w-(--anchor-width) max-w-(--available-width) origin-(--transform-origin)',
            POPUP_SURFACE,
            className,
          )}
        >
          <ComboboxPrimitive.Popup
            className="flex min-w-0 max-h-[min(var(--available-height),23rem)] flex-1 flex-col overflow-hidden text-ink"
            data-slot="combobox-popup"
            {...props}
          >
            {children}
          </ComboboxPrimitive.Popup>
        </span>
      </ComboboxPrimitive.Positioner>
    </ComboboxPrimitive.Portal>
  );
}

export function ComboboxItem({
  className,
  children,
  hideIndicator: _hideIndicator = false,
  ...props
}: WithClass<ComboboxPrimitive.Item.Props> & { hideIndicator?: boolean }) {
  return (
    <ComboboxPrimitive.Item
      className={cx(
        POPUP_ITEM,
        'in-data-[side=none]:min-w-[calc(var(--anchor-width)+1.25rem)] not-data-disabled:hover:bg-raised',
        className,
      )}
      data-slot="combobox-item"
      {...props}
    >
      <div
        className="flex min-w-0 flex-1 items-center gap-2 [&_svg]:text-muted"
        data-slot="combobox-item-content"
      >
        {children}
      </div>
    </ComboboxPrimitive.Item>
  );
}

export function ComboboxEmpty({ className, ...props }: WithClass<ComboboxPrimitive.Empty.Props>) {
  return (
    <ComboboxPrimitive.Empty
      className={cx('not-empty:p-2 text-center text-body text-muted', className)}
      data-slot="combobox-empty"
      {...props}
    />
  );
}

export function ComboboxList({ className, ...props }: WithClass<ComboboxPrimitive.List.Props>) {
  return (
    <ScrollArea fade>
      <ComboboxPrimitive.List
        className={cx('not-empty:scroll-py-1 not-empty:px-1 not-empty:py-1', className)}
        data-slot="combobox-list"
        {...props}
      />
    </ScrollArea>
  );
}

export function ComboboxClear({ className, ...props }: WithClass<ComboboxPrimitive.Clear.Props>) {
  return <ComboboxPrimitive.Clear className={className} data-slot="combobox-clear" {...props} />;
}

export function ComboboxStatus({ className, ...props }: WithClass<ComboboxPrimitive.Status.Props>) {
  return (
    <ComboboxPrimitive.Status
      className={cx(GROUP_LABEL, 'px-3 py-2 empty:m-0 empty:p-0', className)}
      data-slot="combobox-status"
      {...props}
    />
  );
}

export const useComboboxFilter = ComboboxPrimitive.useFilter;

/* The Loams wrapper ------------------------------------------------------------------- */

/** A text input that filters a list of string options; Enter or a click picks one. */
export function Combobox({
  value,
  onValueChange,
  options,
  label,
  placeholder,
  empty = 'No matches.',
}: {
  value: string | null;
  onValueChange: (value: string | null) => void;
  options: string[];
  label: string;
  placeholder?: string;
  empty?: React.ReactNode;
}) {
  const id = React.useId();
  return (
    <ComboboxRoot items={options} value={value} onValueChange={(v) => onValueChange(v)}>
      <label htmlFor={id} className="sr-only">
        {label}
      </label>
      <ComboboxInput id={id} placeholder={placeholder} showTrigger={false} />
      <ComboboxPopup>
        <ComboboxEmpty>{empty}</ComboboxEmpty>
        <ComboboxList>
          {(item: string) => (
            <ComboboxItem key={item} value={item}>
              {item}
            </ComboboxItem>
          )}
        </ComboboxList>
      </ComboboxPopup>
    </ComboboxRoot>
  );
}
