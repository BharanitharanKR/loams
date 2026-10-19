// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/menu.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; the root is MenuRoot, upstream's MenuItem and MenuSeparator are MenuPart and MenuPartSeparator, and the Loams Menu, MenuItem, MenuLabel and MenuSeparator wrappers keep their props; the "ghost" item variant and the DropdownMenu aliases are dropped; shortcuts are not monospace (D1075). Loams' changes are Apache-2.0.

import { Menu as MenuPrimitive } from '@base-ui/react/menu';
import type * as React from 'react';
import type { ReactElement, ReactNode } from 'react';
import { cx, type WithClass } from './cx';
import { OVERLAY_LAYER, OVERLAY_SURFACE } from './popover';

const Check = () => (
  <svg
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth={2}
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
    className="size-3.5"
  >
    <path d="M3 8.5l3.2 3.5L13 4.5" />
  </svg>
);

const ChevronRight = () => (
  <svg
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth={2}
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
    className="-me-0.5 ms-auto size-4 text-muted"
  >
    <path d="M6 4l4 4-4 4" />
  </svg>
);

/** What every menu row shares; each part adds its own layout. */
const ROW_BASE =
  'min-h-7 cursor-default select-none items-center rounded-sm py-1 text-body text-ink outline-none ' +
  'data-highlighted:bg-raised data-disabled:pointer-events-none data-disabled:opacity-60 ' +
  '[&_svg]:pointer-events-none [&_svg]:shrink-0';
/** One row in a menu. */
const ROW = `flex gap-2 px-2 ${ROW_BASE}`;

export const MenuRoot = MenuPrimitive.Root;

export function MenuTrigger({ className, children, ...props }: MenuPrimitive.Trigger.Props) {
  return (
    <MenuPrimitive.Trigger className={className} data-slot="menu-trigger" {...props}>
      {children}
    </MenuPrimitive.Trigger>
  );
}

export function MenuPopup({
  children,
  className,
  sideOffset = 4,
  align = 'center',
  alignOffset,
  side = 'bottom',
  anchor,
  keepMounted = false,
  ...props
}: WithClass<MenuPrimitive.Popup.Props> & {
  align?: MenuPrimitive.Positioner.Props['align'];
  sideOffset?: MenuPrimitive.Positioner.Props['sideOffset'];
  alignOffset?: MenuPrimitive.Positioner.Props['alignOffset'];
  side?: MenuPrimitive.Positioner.Props['side'];
  anchor?: MenuPrimitive.Positioner.Props['anchor'];
  keepMounted?: boolean;
}) {
  return (
    <MenuPrimitive.Portal keepMounted={keepMounted}>
      <MenuPrimitive.Positioner
        align={align}
        alignOffset={alignOffset}
        anchor={anchor}
        className={OVERLAY_LAYER}
        data-slot="menu-positioner"
        side={side}
        sideOffset={sideOffset}
      >
        <MenuPrimitive.Popup
          className={cx(
            'relative flex origin-(--transform-origin) rounded-panel text-body focus:outline-none',
            // Portaled menus can overlap an Electron window drag region (the top bar).
            // Drag hit-testing ignores z-index, so OVERLAY_SURFACE opts out of it.
            OVERLAY_SURFACE,
            'min-w-[min(10rem,calc(100vw-2rem))] max-w-[calc(100vw-2rem)]',
            className,
          )}
          data-slot="menu-popup"
          {...props}
        >
          <div className="max-h-(--available-height) w-full overflow-y-auto p-1">{children}</div>
        </MenuPrimitive.Popup>
      </MenuPrimitive.Positioner>
    </MenuPrimitive.Portal>
  );
}

export function MenuGroup(props: MenuPrimitive.Group.Props) {
  return <MenuPrimitive.Group data-slot="menu-group" {...props} />;
}

/** Upstream's MenuItem: a row that runs an action (the Loams `MenuItem` wraps it). */
export function MenuPart({
  className,
  inset,
  density = 'default',
  variant = 'default',
  ...props
}: WithClass<MenuPrimitive.Item.Props> & {
  inset?: boolean;
  density?: 'default' | 'touch';
  variant?: 'default' | 'danger';
}) {
  return (
    <MenuPrimitive.Item
      className={cx(
        ROW,
        'data-inset:ps-8 data-[variant=danger]:text-danger [&>svg:not([class*=size-])]:size-4 [&>svg:not([class*=text-])]:text-muted data-[variant=danger]:[&>svg:not([class*=text-])]:text-current',
        density === 'touch' && 'min-h-10',
        className,
      )}
      data-density={density}
      data-inset={inset}
      data-slot="menu-item"
      data-variant={variant}
      {...props}
    />
  );
}

export function MenuItemLabel({ className, ...props }: React.ComponentProps<'span'>) {
  return (
    <span data-slot="menu-item-label" className={cx('min-w-0 truncate', className)} {...props} />
  );
}

export function MenuCheckboxItem({
  className,
  children,
  checked,
  variant = 'default',
  ...props
}: WithClass<MenuPrimitive.CheckboxItem.Props> & {
  variant?: 'default' | 'switch';
}) {
  return (
    <MenuPrimitive.CheckboxItem
      checked={checked}
      className={cx(
        'grid ps-2',
        ROW_BASE,
        'in-data-[side=none]:min-w-[calc(var(--anchor-width)+1.25rem)]',
        variant === 'switch'
          ? 'grid-cols-[1fr_auto] gap-4 pe-1.5'
          : 'grid-cols-[1rem_minmax(0,1fr)] gap-2 pe-4',
        className,
      )}
      data-slot="menu-checkbox-item"
      {...props}
    >
      {variant === 'switch' ? (
        <>
          <span className="col-start-1">{children}</span>
          <MenuPrimitive.CheckboxItemIndicator
            className="inline-flex h-[calc(var(--thumb-size)+2px)] w-[calc(var(--thumb-size)*2-2px)] shrink-0 items-center rounded-pill p-px outline-none transition-colors duration-(--op-duration) [--thumb-size:--spacing(3)] motion-reduce:transition-none data-checked:bg-accent data-unchecked:bg-input data-disabled:opacity-60"
            keepMounted
          >
            <span className="pointer-events-none block aspect-square h-full origin-left rounded-pill bg-bg transition-transform duration-(--op-duration) in-[[data-slot=menu-checkbox-item][data-checked]]:translate-x-[calc(var(--thumb-size)-4px)] motion-reduce:transition-none" />
          </MenuPrimitive.CheckboxItemIndicator>
        </>
      ) : (
        <>
          <MenuPrimitive.CheckboxItemIndicator className="col-start-1 flex">
            <Check />
          </MenuPrimitive.CheckboxItemIndicator>
          <span className="col-start-2">{children}</span>
        </>
      )}
    </MenuPrimitive.CheckboxItem>
  );
}

export function MenuRadioGroup(props: MenuPrimitive.RadioGroup.Props) {
  return <MenuPrimitive.RadioGroup data-slot="menu-radio-group" {...props} />;
}

export function MenuRadioItem({
  className,
  children,
  hideIndicator: _hideIndicator = false,
  ...props
}: WithClass<MenuPrimitive.RadioItem.Props> & {
  hideIndicator?: boolean;
}) {
  return (
    <MenuPrimitive.RadioItem
      className={cx(
        ROW,
        'in-data-[side=none]:min-w-[calc(var(--anchor-width)+1.25rem)] data-checked:bg-row-selected',
        className,
      )}
      data-slot="menu-radio-item"
      {...props}
    >
      <span className="min-w-0 flex-1">{children}</span>
    </MenuPrimitive.RadioItem>
  );
}

export function MenuRadioItemIndicator({
  className,
  children,
  ...props
}: WithClass<MenuPrimitive.RadioItemIndicator.Props>) {
  return (
    <MenuPrimitive.RadioItemIndicator
      aria-hidden
      className={cx('flex shrink-0', className)}
      data-slot="menu-radio-item-indicator"
      {...props}
    >
      {children ?? <Check />}
    </MenuPrimitive.RadioItemIndicator>
  );
}

export function MenuGroupLabel({
  className,
  inset,
  ...props
}: WithClass<MenuPrimitive.GroupLabel.Props> & {
  inset?: boolean;
}) {
  return (
    <MenuPrimitive.GroupLabel
      className={cx('px-2 py-1.5 text-small font-medium text-muted data-inset:ps-8', className)}
      data-inset={inset}
      data-slot="menu-label"
      {...props}
    />
  );
}

/** Upstream's MenuSeparator (the Loams `MenuSeparator` wraps it). */
export function MenuPartSeparator({
  className,
  ...props
}: WithClass<MenuPrimitive.Separator.Props>) {
  return (
    <MenuPrimitive.Separator
      className={cx('mx-2 my-1 h-px bg-rule-soft', className)}
      data-slot="menu-separator"
      {...props}
    />
  );
}

export function MenuShortcut({ className, ...props }: React.ComponentProps<'kbd'>) {
  return (
    <kbd
      className={cx('ms-auto font-sans text-small text-faint', className)}
      data-slot="menu-shortcut"
      {...props}
    />
  );
}

export function MenuSub(props: MenuPrimitive.SubmenuRoot.Props) {
  return <MenuPrimitive.SubmenuRoot data-slot="menu-sub" {...props} />;
}

export function MenuSubTrigger({
  className,
  inset,
  density = 'default',
  children,
  ...props
}: WithClass<MenuPrimitive.SubmenuTrigger.Props> & {
  inset?: boolean;
  density?: 'default' | 'touch';
}) {
  return (
    <MenuPrimitive.SubmenuTrigger
      className={cx(
        ROW,
        'data-inset:ps-8 data-popup-open:bg-raised',
        density === 'touch' && 'min-h-10',
        className,
      )}
      data-density={density}
      data-inset={inset}
      data-slot="menu-sub-trigger"
      {...props}
    >
      {children}
      <ChevronRight />
    </MenuPrimitive.SubmenuTrigger>
  );
}

export function MenuSubPopup({
  className,
  sideOffset = 0,
  alignOffset,
  align = 'start',
  ...props
}: WithClass<MenuPrimitive.Popup.Props> & {
  align?: MenuPrimitive.Positioner.Props['align'];
  sideOffset?: MenuPrimitive.Positioner.Props['sideOffset'];
  alignOffset?: MenuPrimitive.Positioner.Props['alignOffset'];
}) {
  const defaultAlignOffset = align !== 'center' ? -5 : undefined;

  return (
    <MenuPopup
      align={align}
      alignOffset={alignOffset ?? defaultAlignOffset}
      className={className}
      data-slot="menu-sub-content"
      side="inline-end"
      sideOffset={sideOffset}
      {...props}
    />
  );
}

/* The Loams wrappers ------------------------------------------------------------------ */

/** A menu of actions on a trigger. `trigger` is the element that opens it (a Button). */
export function Menu({
  trigger,
  children,
  align = 'end',
  label,
}: {
  trigger: ReactElement;
  children: ReactNode;
  align?: 'start' | 'center' | 'end';
  /** An accessible name for the menu when the items do not make it clear. */
  label?: string;
}) {
  return (
    <MenuRoot>
      <MenuTrigger render={trigger} />
      <MenuPopup align={align} sideOffset={6} aria-label={label}>
        {children}
      </MenuPopup>
    </MenuRoot>
  );
}

export function MenuItem({
  onSelect,
  children,
  disabled,
  tone,
}: {
  onSelect?: () => void;
  children: ReactNode;
  disabled?: boolean;
  tone?: 'danger';
}) {
  return (
    <MenuPart onClick={onSelect} disabled={disabled} variant={tone ?? 'default'}>
      {children}
    </MenuPart>
  );
}

/** A non-interactive heading or block at the top of a menu (who is signed in, say). */
export function MenuLabel({ children }: { children: ReactNode }) {
  return <div className="grid gap-0.5 px-2 py-1.5 text-small text-muted">{children}</div>;
}

export function MenuSeparator() {
  return <MenuPartSeparator />;
}
