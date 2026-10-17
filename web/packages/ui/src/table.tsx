// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/table.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx; the parts are exported as TableRoot and friends, and the Loams column Table (Loams' own) follows unchanged. Loams' changes are Apache-2.0.

import type { ComponentProps, ReactNode } from 'react';
import { cx } from './cx';

/* The T3 Code parts: a semantic table you lay out yourself. ------------------------- */

export function TableRoot({ className, ...props }: ComponentProps<'table'>) {
  return (
    <div data-slot="table-container" className="relative w-full overflow-x-auto">
      <table
        data-slot="table"
        className={cx('w-full caption-bottom border-collapse text-small', className)}
        {...props}
      />
    </div>
  );
}

export function TableHeader({ className, ...props }: ComponentProps<'thead'>) {
  return (
    <thead
      data-slot="table-header"
      className={cx('[&_tr]:border-b [&_tr]:border-rule', className)}
      {...props}
    />
  );
}

export function TableBody({ className, ...props }: ComponentProps<'tbody'>) {
  return (
    <tbody
      data-slot="table-body"
      className={cx('[&_tr:last-child]:border-0', className)}
      {...props}
    />
  );
}

export function TableRow({ className, ...props }: ComponentProps<'tr'>) {
  return (
    <tr
      data-slot="table-row"
      className={cx(
        'border-b border-rule transition-colors duration-(--op-duration) hover:bg-row-hover has-aria-expanded:bg-row-hover data-[state=selected]:bg-row-selected motion-reduce:transition-none',
        className,
      )}
      {...props}
    />
  );
}

export function TableHead({ className, ...props }: ComponentProps<'th'>) {
  return (
    <th
      data-slot="table-head"
      className={cx(
        'h-9 px-2 text-left align-middle font-medium whitespace-nowrap text-muted [&:has([role=checkbox])]:pr-0',
        className,
      )}
      {...props}
    />
  );
}

export function TableCell({ className, ...props }: ComponentProps<'td'>) {
  return (
    <td
      data-slot="table-cell"
      className={cx(
        'p-2 align-middle whitespace-nowrap text-ink [&:has([role=checkbox])]:pr-0',
        className,
      )}
      {...props}
    />
  );
}

/* The Loams column table. -------------------------------------------------------------- */

export type Column<T> = {
  key: string;
  header: ReactNode;
  cell: (row: T) => ReactNode;
  /** Right-aligned, tabular numbers. */
  numeric?: boolean;
  width?: string;
};

/**
 * A plain data table. `empty` renders in place of the body when there are no
 * rows. `onRowClick` is a pointer shortcut only: put a link or button to the
 * same place in a cell, so keyboard and screen-reader users can reach it.
 * Clicks on links and buttons inside the row are left to them.
 */
export function Table<T>({
  columns,
  rows,
  rowKey,
  empty,
  caption,
  onRowClick,
}: {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  empty?: ReactNode;
  caption?: string;
  onRowClick?: (row: T) => void;
}) {
  if (rows.length === 0 && empty) return <>{empty}</>;
  return (
    <div className="loams-table-wrap">
      <table className="loams-table">
        {caption && <caption className="sr-only">{caption}</caption>}
        <thead>
          <tr>
            {columns.map((c) => (
              <th
                key={c.key}
                className={c.numeric ? 'loams-num' : undefined}
                style={{ width: c.width }}
              >
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={rowKey(row)}
              data-href={onRowClick ? '' : undefined}
              onClick={
                onRowClick
                  ? (e) => {
                      if (
                        (e.target as HTMLElement).closest(
                          'a, button, input, select, textarea, label',
                        )
                      )
                        return;
                      onRowClick(row);
                    }
                  : undefined
              }
            >
              {columns.map((c) => (
                <td key={c.key} className={c.numeric ? 'loams-num' : undefined}>
                  {c.cell(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** A table cell with a strong first line and a muted second. */
export function Primary({ title, detail }: { title: ReactNode; detail?: ReactNode }) {
  return (
    <span className="loams-table-primary">
      <strong>{title}</strong>
      {detail && <span>{detail}</span>}
    </span>
  );
}
