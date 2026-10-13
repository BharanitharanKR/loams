// A WAI-ARIA tablist: ArrowLeft/ArrowRight/Home/End move between tabs (automatic
// activation), only the selected tab is in the tab order, and each tab controls its panel.

import { type KeyboardEvent, type ReactNode, useId, useRef } from 'react';

export interface Tab<K extends string> {
  key: K;
  label: ReactNode;
}

export function Tabs<K extends string>(p: {
  label: string;
  tabs: Tab<K>[];
  value: K;
  onChange: (k: K) => void;
  children: ReactNode;
}) {
  const base = useId();
  const refs = useRef<(HTMLButtonElement | null)[]>([]);
  const index = Math.max(
    0,
    p.tabs.findIndex((t) => t.key === p.value),
  );
  const go = (i: number) => {
    const n = (i + p.tabs.length) % p.tabs.length;
    p.onChange(p.tabs[n]!.key);
    refs.current[n]?.focus();
  };
  const onKeyDown = (e: KeyboardEvent) => {
    const moves: Record<string, number> = {
      ArrowRight: index + 1,
      ArrowLeft: index - 1,
      Home: 0,
      End: p.tabs.length - 1,
    };
    const to = moves[e.key];
    if (to !== undefined) {
      e.preventDefault();
      go(to);
    }
  };
  return (
    <div className="flex min-w-0 flex-col gap-3">
      <div
        role="tablist"
        aria-label={p.label}
        className="flex gap-1 border-0 border-b border-solid border-rule"
        onKeyDown={onKeyDown}
      >
        {p.tabs.map((t, i) => {
          const on = t.key === p.value;
          return (
            <button
              key={t.key}
              ref={(el) => {
                refs.current[i] = el;
              }}
              type="button"
              role="tab"
              id={`${base}-tab-${t.key}`}
              aria-selected={on}
              aria-controls={`${base}-panel`}
              tabIndex={on ? 0 : -1}
              className={`cursor-pointer border-0 border-b-2 border-solid bg-transparent px-3 py-1.5 font-sans text-sm ${
                on ? 'border-accent text-ink' : 'border-transparent text-muted'
              }`}
              onClick={() => p.onChange(t.key)}
            >
              {t.label}
            </button>
          );
        })}
      </div>
      <div
        role="tabpanel"
        id={`${base}-panel`}
        aria-labelledby={`${base}-tab-${p.value}`}
        className="min-w-0"
      >
        {p.children}
      </div>
    </div>
  );
}
