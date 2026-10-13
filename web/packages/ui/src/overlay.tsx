// Headless primitives from Base UI, wrapped once and styled with Tailwind token
// utilities (docs/design/console-ui-stack.md, decision 2). Plugins import these
// from @loams/ui and never @base-ui/react directly, so focus handling, keyboard
// behaviour and the look stay the same on every page.

import { Combobox as BaseCombobox } from '@base-ui/react/combobox';
import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { Menu as BaseMenu } from '@base-ui/react/menu';
import { Popover as BasePopover } from '@base-ui/react/popover';
import { Select as BaseSelect } from '@base-ui/react/select';
import { Tabs as BaseTabs } from '@base-ui/react/tabs';
import { Toast as BaseToast } from '@base-ui/react/toast';
import { Tooltip as BaseTooltip } from '@base-ui/react/tooltip';
import { type ReactElement, type ReactNode, useId } from 'react';
import { Button } from './button';
import { cx } from './cx';

/** The surface every pop-up shares: hard corners, a rule, the pop shadow. */
const POPUP =
  'z-50 min-w-44 border border-rule bg-bg text-ink shadow-pop outline-none ' +
  'origin-(--transform-origin) transition-[opacity,scale] duration-150 ease-op ' +
  'data-starting-style:opacity-0 data-starting-style:scale-[0.98] ' +
  'data-ending-style:opacity-0 data-ending-style:scale-[0.98]';
/** A row in a menu, select or combobox list. */
const ITEM =
  'flex cursor-default select-none items-center gap-2 px-3 py-2 text-[0.86rem] outline-none ' +
  'data-highlighted:bg-raised data-disabled:opacity-50';

/* Dialog ---------------------------------------------------------------------------- */

/**
 * A modal. Focus moves in and is trapped, Escape and a click on the scrim close it,
 * and the page behind is inert. `dismissible={false}` holds it open while work is pending.
 */
export function Dialog({
  open,
  onClose,
  title,
  children,
  footer,
  dismissible = true,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  /** False keeps Escape, the scrim and the close button from dismissing it. */
  dismissible?: boolean;
}) {
  return (
    <BaseDialog.Root
      open={open}
      onOpenChange={(next) => {
        if (!next && dismissible) onClose();
      }}
      disablePointerDismissal={!dismissible}
    >
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="loams-dialog-scrim" />
        <BaseDialog.Popup className="loams-dialog">
          <div className="loams-dialog-head">
            {/* Base UI renders the title as an <h2>. */}
            <BaseDialog.Title>{title}</BaseDialog.Title>
            <BaseDialog.Close
              render={<Button variant="quiet" size="icon" />}
              aria-label="Close"
              disabled={!dismissible}
            >
              ×
            </BaseDialog.Close>
          </div>
          <div className="loams-dialog-body">{children}</div>
          {footer && <div className="loams-dialog-foot">{footer}</div>}
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}

/* Menu ------------------------------------------------------------------------------ */

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
    <BaseMenu.Root>
      <BaseMenu.Trigger render={trigger} />
      <BaseMenu.Portal>
        <BaseMenu.Positioner align={align} sideOffset={6}>
          <BaseMenu.Popup className={cx(POPUP, 'py-1')} aria-label={label}>
            {children}
          </BaseMenu.Popup>
        </BaseMenu.Positioner>
      </BaseMenu.Portal>
    </BaseMenu.Root>
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
    <BaseMenu.Item
      className={cx(ITEM, tone === 'danger' && 'text-danger')}
      onClick={onSelect}
      disabled={disabled}
    >
      {children}
    </BaseMenu.Item>
  );
}

/** A non-interactive heading or block at the top of a menu (who is signed in, say). */
export function MenuLabel({ children }: { children: ReactNode }) {
  return <div className="grid gap-0.5 px-3 py-2 text-[0.8rem] text-muted">{children}</div>;
}

export function MenuSeparator() {
  return <BaseMenu.Separator className="my-1 h-px bg-rule-soft" />;
}

/* Popover --------------------------------------------------------------------------- */

/** Non-modal content anchored to a trigger: details, a small form, a picker. */
export function Popover({
  trigger,
  title,
  children,
  align = 'start',
  open,
  onOpenChange,
}: {
  trigger: ReactElement;
  title?: ReactNode;
  children: ReactNode;
  align?: 'start' | 'center' | 'end';
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  return (
    <BasePopover.Root open={open} onOpenChange={onOpenChange ? (o) => onOpenChange(o) : undefined}>
      <BasePopover.Trigger render={trigger} />
      <BasePopover.Portal>
        <BasePopover.Positioner align={align} sideOffset={6}>
          <BasePopover.Popup className={cx(POPUP, 'grid gap-2 p-3')}>
            {title && (
              <BasePopover.Title className="m-0 text-[0.9rem] font-[540]">
                {title}
              </BasePopover.Title>
            )}
            {children}
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}

/* Select ---------------------------------------------------------------------------- */

export interface Option<V extends string = string> {
  value: V;
  label: ReactNode;
  disabled?: boolean;
}

/**
 * A select on Base UI: a styled list that matches the menus, for choices that need
 * more than plain text. The native `Select` (field.tsx) stays for plain forms.
 */
export function SelectMenu<V extends string>({
  value,
  onValueChange,
  options,
  label,
  placeholder = 'Choose…',
  className,
}: {
  value: V | null;
  onValueChange: (value: V) => void;
  options: Option<V>[];
  /** The visible or accessible label. */
  label: string;
  placeholder?: string;
  className?: string;
}) {
  return (
    <BaseSelect.Root
      value={value}
      onValueChange={(v) => v !== null && onValueChange(v as V)}
      items={options.map((o) => ({ value: o.value, label: o.label }))}
    >
      <BaseSelect.Trigger
        className={cx('loams-input inline-flex items-center justify-between gap-2', className)}
        aria-label={label}
      >
        <BaseSelect.Value placeholder={placeholder} />
        <BaseSelect.Icon className="text-faint" aria-hidden="true">
          ▾
        </BaseSelect.Icon>
      </BaseSelect.Trigger>
      <BaseSelect.Portal>
        <BaseSelect.Positioner sideOffset={4} alignItemWithTrigger={false}>
          <BaseSelect.Popup className={cx(POPUP, 'min-w-(--anchor-width) py-1')}>
            <BaseSelect.List>
              {options.map((o) => (
                <BaseSelect.Item
                  key={o.value}
                  value={o.value}
                  disabled={o.disabled}
                  className={ITEM}
                >
                  <BaseSelect.ItemIndicator className="w-3 text-accent">✓</BaseSelect.ItemIndicator>
                  <BaseSelect.ItemText>{o.label}</BaseSelect.ItemText>
                </BaseSelect.Item>
              ))}
            </BaseSelect.List>
          </BaseSelect.Popup>
        </BaseSelect.Positioner>
      </BaseSelect.Portal>
    </BaseSelect.Root>
  );
}

/* Combobox -------------------------------------------------------------------------- */

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
  empty?: ReactNode;
}) {
  const id = useId();
  return (
    <BaseCombobox.Root items={options} value={value} onValueChange={(v) => onValueChange(v)}>
      <label htmlFor={id} className="sr-only">
        {label}
      </label>
      <BaseCombobox.Input id={id} placeholder={placeholder} className="loams-input" />
      <BaseCombobox.Portal>
        <BaseCombobox.Positioner sideOffset={4}>
          <BaseCombobox.Popup className={cx(POPUP, 'min-w-(--anchor-width) py-1')}>
            <BaseCombobox.Empty className="px-3 py-2 text-[0.84rem] text-muted empty:hidden">
              {empty}
            </BaseCombobox.Empty>
            <BaseCombobox.List className="max-h-72 overflow-y-auto">
              {(item: string) => (
                <BaseCombobox.Item key={item} value={item} className={ITEM}>
                  {item}
                </BaseCombobox.Item>
              )}
            </BaseCombobox.List>
          </BaseCombobox.Popup>
        </BaseCombobox.Positioner>
      </BaseCombobox.Portal>
    </BaseCombobox.Root>
  );
}

/* Tabs ------------------------------------------------------------------------------ */

export interface Tab<V extends string = string> {
  value: V;
  label: ReactNode;
  panel?: ReactNode;
}

/**
 * Tabs with arrow-key movement between them. Give each tab a `panel`, or leave the
 * panels out and render the active view yourself (for tabs that are routes).
 */
export function Tabs<V extends string>({
  value,
  onValueChange,
  tabs,
  label,
}: {
  value: V;
  onValueChange: (value: V) => void;
  tabs: Tab<V>[];
  label: string;
}) {
  return (
    <BaseTabs.Root value={value} onValueChange={(v) => onValueChange(v as V)}>
      <BaseTabs.List className="relative flex gap-1 border-b border-rule" aria-label={label}>
        {tabs.map((t) => (
          <BaseTabs.Tab
            key={t.value}
            value={t.value}
            className={
              'cursor-pointer border-0 border-b-2 border-transparent bg-transparent px-3 py-2 ' +
              'text-[0.9rem] text-muted hover:text-ink data-active:border-accent data-active:text-ink'
            }
          >
            {t.label}
          </BaseTabs.Tab>
        ))}
      </BaseTabs.List>
      {tabs.map(
        (t) =>
          t.panel !== undefined && (
            <BaseTabs.Panel key={t.value} value={t.value} className="pt-4 outline-none">
              {t.panel}
            </BaseTabs.Panel>
          ),
      )}
    </BaseTabs.Root>
  );
}

/* Tooltip --------------------------------------------------------------------------- */

/** A short label on hover or focus. Never the only place a control's name lives. */
export function Tooltip({ content, children }: { content: ReactNode; children: ReactElement }) {
  return (
    <BaseTooltip.Root>
      <BaseTooltip.Trigger render={children} />
      <BaseTooltip.Portal>
        <BaseTooltip.Positioner sideOffset={6}>
          <BaseTooltip.Popup className={cx(POPUP, 'min-w-0 px-2 py-1 text-[0.78rem] text-ink')}>
            {content}
          </BaseTooltip.Popup>
        </BaseTooltip.Positioner>
      </BaseTooltip.Portal>
    </BaseTooltip.Root>
  );
}

/** Shares hover delays between tooltips; the shell mounts it once. */
export const TooltipProvider = BaseTooltip.Provider;

/* Toast ----------------------------------------------------------------------------- */

/** Holds the toasts; the shell mounts it once, with a `<Toaster />` inside. */
export const ToastProvider = BaseToast.Provider;

/** `add({ title, description, type: 'danger' })` shows a toast. */
export const useToast = BaseToast.useToastManager;

/** Where toasts appear: the bottom right, newest on top. */
export function Toaster() {
  const { toasts } = BaseToast.useToastManager();
  return (
    <BaseToast.Portal>
      <BaseToast.Viewport className="fixed right-4 bottom-4 z-[80] grid w-[min(360px,calc(100vw-32px))] gap-2">
        {toasts.map((t) => (
          <BaseToast.Root
            key={t.id}
            toast={t}
            className={cx(
              'border border-rule border-l-[3px] bg-bg px-3.5 py-2.5 text-[0.86rem] shadow-pop',
              t.type === 'danger' ? 'border-l-danger' : 'border-l-grow',
            )}
          >
            <BaseToast.Content className="flex items-start gap-3">
              <div className="grid min-w-0 flex-1 gap-0.5">
                <BaseToast.Title className="m-0 font-[540]" />
                <BaseToast.Description className="m-0 text-muted" />
              </div>
              <BaseToast.Close
                className="cursor-pointer text-muted hover:text-ink"
                aria-label="Dismiss"
              >
                ×
              </BaseToast.Close>
            </BaseToast.Content>
          </BaseToast.Root>
        ))}
      </BaseToast.Viewport>
    </BaseToast.Portal>
  );
}
