// The SQL editor (§49 §18.3): a ClickHouse-dialect textarea with completions from the
// ClickHouse keywords and the function list fetched once from `system.functions`.
// Keyboard: Ctrl or Cmd + Enter runs; while the completion list is open, Up and Down
// move, Enter or Tab accept, Escape closes it.

import { type KeyboardEvent, useId, useMemo, useRef, useState } from 'react';

export interface EditorProps {
  value: string;
  onChange: (sql: string) => void;
  onRun: () => void;
  /** Words to complete: keywords and function names. */
  words: string[];
  disabled?: boolean;
  describedBy?: string;
  invalid?: boolean;
}

const MAX_SUGGESTIONS = 8;

/** The identifier ending at `caret`, and where it starts. */
export function wordAt(text: string, caret: number): { start: number; word: string } {
  let start = caret;
  while (start > 0 && /[A-Za-z0-9_]/.test(text[start - 1] ?? '')) start--;
  return { start, word: text.slice(start, caret) };
}

export function suggest(words: string[], prefix: string): string[] {
  if (prefix.length < 2) return [];
  const p = prefix.toLowerCase();
  const seen = new Set<string>();
  const out: string[] = [];
  for (const w of words) {
    const k = w.toLowerCase();
    if (k.startsWith(p) && k !== p && !seen.has(k)) {
      seen.add(k);
      out.push(w);
      if (out.length === MAX_SUGGESTIONS) break;
    }
  }
  return out;
}

export function Editor(p: EditorProps) {
  const ref = useRef<HTMLTextAreaElement>(null);
  const id = useId();
  const listId = `${id}-completions`;
  const [caret, setCaret] = useState(0);
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);

  const { start, word } = wordAt(p.value, caret);
  const options = useMemo(() => (open ? suggest(p.words, word) : []), [open, p.words, word]);
  const shown = open && options.length > 0;

  const accept = (w: string) => {
    const el = ref.current;
    const next = p.value.slice(0, start) + w + p.value.slice(caret);
    p.onChange(next);
    setOpen(false);
    const at = start + w.length;
    requestAnimationFrame(() => {
      el?.focus();
      el?.setSelectionRange(at, at);
      setCaret(at);
    });
  };

  const onKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      setOpen(false);
      if (!p.disabled && p.value.trim()) p.onRun();
      return;
    }
    if (!shown) {
      if (e.key === ' ' && e.ctrlKey) {
        e.preventDefault();
        setOpen(true);
        setActive(0);
      }
      return;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActive((a) => (a + 1) % options.length);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActive((a) => (a - 1 + options.length) % options.length);
    } else if (e.key === 'Enter' || e.key === 'Tab') {
      e.preventDefault();
      const w = options[Math.min(active, options.length - 1)];
      if (w) accept(w);
    } else if (e.key === 'Escape') {
      e.preventDefault();
      setOpen(false);
    }
  };

  return (
    <div className="relative">
      <textarea
        ref={ref}
        aria-label="SQL"
        role="combobox"
        aria-autocomplete="list"
        aria-expanded={shown}
        aria-controls={listId}
        aria-activedescendant={shown ? `${listId}-${active}` : undefined}
        aria-describedby={p.describedBy}
        aria-invalid={p.invalid || undefined}
        className={
          /* mono-ok: sql */ 'loams-input box-border w-full resize-y font-mono text-[13px]'
        }
        rows={8}
        spellCheck={false}
        value={p.value}
        onChange={(e) => {
          p.onChange(e.target.value);
          setCaret(e.target.selectionStart);
          setOpen(true);
          setActive(0);
        }}
        onSelect={(e) => setCaret(e.currentTarget.selectionStart)}
        onBlur={() => setOpen(false)}
        onKeyDown={onKey}
      />
      <div
        id={listId}
        role="listbox"
        aria-label="Completions"
        hidden={!shown}
        className={
          /* mono-ok: sql */ 'absolute left-2 z-10 m-0 max-h-60 overflow-auto rounded-md border border-solid border-rule bg-surface p-1 font-mono text-[13px] shadow'
        }
      >
        {options.map((w, i) => (
          <div
            key={w}
            id={`${listId}-${i}`}
            role="option"
            // Focus stays in the textarea (aria-activedescendant); -1 keeps options out of
            // the tab order.
            tabIndex={-1}
            aria-selected={i === active}
            className={`cursor-pointer rounded px-2 py-1 ${i === active ? 'bg-accent-soft' : ''}`}
            // mousedown, not click: the textarea's blur would close the list first.
            onMouseDown={(e) => {
              e.preventDefault();
              accept(w);
            }}
          >
            {w}
          </div>
        ))}
      </div>
    </div>
  );
}
