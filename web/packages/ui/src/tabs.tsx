// Tabs on Base UI, styled with the token utilities (docs/design/console-ui-stack.md,
// decision 2). Moved out of overlay.tsx unchanged in CF1 Task 5; not vendored (T3 Code
// has no tabs primitive).

import { Tabs as BaseTabs } from '@base-ui/react/tabs';
import type { ReactNode } from 'react';

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
