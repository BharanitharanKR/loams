// The results grid (§49 §18.3): ClickHouse type names in the headers, rows appended as
// `batch` frames arrive, and only the rows in view drawn (a fixed row height and a
// scroll window), so a 100 000-row result costs no more to draw than a page of it.
// The table keeps `aria-rowcount` and each row's `aria-rowindex`, so assistive
// technology knows the whole size while only part of it is in the DOM.

import { useId, useState } from 'react';
import type { Cell, ColumnHeader } from './client.js';

export const ROW_HEIGHT = 28;
export const VIEW_ROWS = 15;
const OVERSCAN = 10;

export function cellText(v: Cell): string {
  if (v === null) return 'NULL';
  if (typeof v === 'string') return v;
  return JSON.stringify(v);
}

/** The slice of rows to draw for a scroll offset. */
export function windowFor(scrollTop: number, total: number): { first: number; last: number } {
  const first = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  const last = Math.min(total, Math.ceil(scrollTop / ROW_HEIGHT) + VIEW_ROWS + OVERSCAN);
  return { first, last };
}

export function ResultsGrid({ columns, rows }: { columns: ColumnHeader[]; rows: Cell[][] }) {
  const [top, setTop] = useState(0);
  const caption = useId();
  const { first, last } = windowFor(top, rows.length);
  const before = first * ROW_HEIGHT;
  const after = Math.max(0, (rows.length - last) * ROW_HEIGHT);

  if (!columns.length) return null;
  return (
    <section
      className="overflow-auto rounded-md border border-solid border-rule"
      style={{ maxHeight: ROW_HEIGHT * (VIEW_ROWS + 1) + 2 }}
      onScroll={(e) => setTop(e.currentTarget.scrollTop)}
      // Focusable so the keyboard can scroll it.
      // biome-ignore lint/a11y/noNoninteractiveTabindex: a scroll region must take focus
      tabIndex={0}
      aria-labelledby={caption}
    >
      {/* Spacers outside the table stand in for the rows not drawn, so the table holds
          only real rows and the sticky header stays inside the drawn slice. */}
      <div aria-hidden="true" style={{ height: before }} />
      <table
        className="w-full border-collapse font-mono text-[13px]"
        aria-rowcount={rows.length + 1}
        aria-colcount={columns.length}
      >
        <caption id={caption} className="sr-only">
          Result rows
        </caption>
        <thead className="sticky top-0 bg-surface">
          <tr aria-rowindex={1}>
            {columns.map((c) => (
              <th
                key={c.name}
                scope="col"
                className="whitespace-nowrap border-0 border-b border-solid border-rule px-2 py-1 text-left font-sans"
              >
                {c.name}
                <span className="ml-1 font-normal text-faint text-xs">{c.clickhouseType}</span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(first, last).map((row, j) => {
            const i = first + j;
            return (
              <tr key={i} aria-rowindex={i + 2} style={{ height: ROW_HEIGHT }}>
                {columns.map((c, k) => {
                  const v = row[k] ?? null;
                  const text = cellText(v);
                  return (
                    <td
                      key={c.name}
                      className={`max-w-[420px] truncate whitespace-nowrap border-0 border-b border-solid border-rule px-2 ${
                        v === null ? 'text-faint' : ''
                      }`}
                      title={`${c.clickhouseType}: ${text}`}
                    >
                      {text}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
      <div aria-hidden="true" style={{ height: after }} />
    </section>
  );
}
