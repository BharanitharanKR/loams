// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/autocomplete.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; the virtualised list and its heading are dropped (long lists use VirtualList). Loams' changes are Apache-2.0.

import { Autocomplete as AutocompletePrimitive } from '@base-ui/react/autocomplete';
import type * as React from 'react';
import { ChevronsUpDown, Cross, INPUT_BUTTON, POPUP_LAYER } from './combobox';
import { cx, type WithClass } from './cx';
import { InputControl } from './input';
import { ScrollArea } from './scroll-area';
import { GROUP_LABEL, POPUP_ITEM, POPUP_SURFACE } from './select';

export const Autocomplete = AutocompletePrimitive.Root;

export function AutocompleteInput({
  className,
  showTrigger = false,
  showClear = false,
  startAddon,
  size,
  ...props
}: Omit<WithClass<AutocompletePrimitive.Input.Props>, 'size'> & {
  showTrigger?: boolean;
  showClear?: boolean;
  startAddon?: React.ReactNode;
  size?: 'sm' | 'default' | 'lg' | number;
  ref?: React.Ref<HTMLInputElement>;
}) {
  const sizeValue = size ?? 'default';

  return (
    <div className="relative w-full text-ink has-disabled:opacity-60 not-has-[>*.w-full]:w-fit">
      {startAddon && (
        <div
          className="pointer-events-none absolute inset-y-0 start-px z-10 flex items-center ps-3 text-muted has-[button]:pointer-events-auto [&_svg]:size-4"
          data-slot="autocomplete-start-addon"
        >
          {startAddon}
        </div>
      )}
      <AutocompletePrimitive.Input
        className={cx(
          !!startAddon && '*:data-[slot=autocomplete-input]:ps-8',
          'has-[+[data-slot=autocomplete-trigger],+[data-slot=autocomplete-clear]]:*:data-[slot=autocomplete-input]:pe-8',
          className,
        )}
        data-slot="autocomplete-input"
        render={<InputControl nativeInput size={sizeValue} />}
        {...props}
      />
      {showTrigger && (
        <AutocompleteTrigger className={cx(INPUT_BUTTON, 'end-1')}>
          <AutocompletePrimitive.Icon data-slot="autocomplete-icon">
            <ChevronsUpDown />
          </AutocompletePrimitive.Icon>
        </AutocompleteTrigger>
      )}
      {showClear && <AutocompleteClear />}
    </div>
  );
}

export function AutocompletePopup({
  className,
  children,
  side = 'bottom',
  sideOffset = 4,
  alignOffset,
  align = 'start',
  anchor,
  ...props
}: WithClass<AutocompletePrimitive.Popup.Props> & {
  align?: AutocompletePrimitive.Positioner.Props['align'];
  sideOffset?: AutocompletePrimitive.Positioner.Props['sideOffset'];
  alignOffset?: AutocompletePrimitive.Positioner.Props['alignOffset'];
  side?: AutocompletePrimitive.Positioner.Props['side'];
  anchor?: AutocompletePrimitive.Positioner.Props['anchor'];
}) {
  return (
    <AutocompletePrimitive.Portal>
      <AutocompletePrimitive.Positioner
        align={align}
        alignOffset={alignOffset}
        anchor={anchor}
        className={POPUP_LAYER}
        data-slot="autocomplete-positioner"
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
          <AutocompletePrimitive.Popup
            className="flex max-h-[min(var(--available-height),23rem)] flex-1 flex-col text-ink"
            data-slot="autocomplete-popup"
            {...props}
          >
            {children}
          </AutocompletePrimitive.Popup>
        </span>
      </AutocompletePrimitive.Positioner>
    </AutocompletePrimitive.Portal>
  );
}

export function AutocompleteItem({
  className,
  children,
  ...props
}: WithClass<AutocompletePrimitive.Item.Props>) {
  return (
    <AutocompletePrimitive.Item
      className={cx(POPUP_ITEM, 'select-none hover:bg-raised [&_svg]:text-muted', className)}
      data-slot="autocomplete-item"
      {...props}
    >
      {children}
    </AutocompletePrimitive.Item>
  );
}

export function AutocompleteGroup({
  className,
  ...props
}: WithClass<AutocompletePrimitive.Group.Props>) {
  return (
    <AutocompletePrimitive.Group
      className={cx('[[role=group]+&]:mt-1.5', className)}
      data-slot="autocomplete-group"
      {...props}
    />
  );
}

export function AutocompleteGroupLabel({
  className,
  ...props
}: WithClass<AutocompletePrimitive.GroupLabel.Props>) {
  return (
    <AutocompletePrimitive.GroupLabel
      className={cx(GROUP_LABEL, className)}
      data-slot="autocomplete-group-label"
      {...props}
    />
  );
}

export function AutocompleteEmpty({
  className,
  ...props
}: WithClass<AutocompletePrimitive.Empty.Props>) {
  return (
    <AutocompletePrimitive.Empty
      className={cx('not-empty:p-2 text-center text-body text-muted', className)}
      data-slot="autocomplete-empty"
      {...props}
    />
  );
}

export function AutocompleteList({
  className,
  ...props
}: WithClass<AutocompletePrimitive.List.Props>) {
  return (
    <ScrollArea fade>
      <AutocompletePrimitive.List
        className={cx('not-empty:scroll-py-1 not-empty:p-1', className)}
        data-slot="autocomplete-list"
        {...props}
      />
    </ScrollArea>
  );
}

export function AutocompleteClear({
  className,
  ...props
}: WithClass<AutocompletePrimitive.Clear.Props>) {
  return (
    <AutocompletePrimitive.Clear
      className={cx(INPUT_BUTTON, 'end-1', className)}
      data-slot="autocomplete-clear"
      aria-label="Clear"
      {...props}
    >
      <Cross />
    </AutocompletePrimitive.Clear>
  );
}

export function AutocompleteCollection({
  ...props
}: WithClass<AutocompletePrimitive.Collection.Props>) {
  return <AutocompletePrimitive.Collection data-slot="autocomplete-collection" {...props} />;
}

export function AutocompleteTrigger({
  className,
  children,
  ...props
}: WithClass<AutocompletePrimitive.Trigger.Props>) {
  return (
    <AutocompletePrimitive.Trigger
      className={className}
      data-slot="autocomplete-trigger"
      {...props}
    >
      {children}
    </AutocompletePrimitive.Trigger>
  );
}
