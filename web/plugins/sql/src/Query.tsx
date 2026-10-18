// The SQL editor and its results: the shared query workbench in the MySQL dialect, with the
// branch's tables and columns (information_schema) for completion and the examples.
// Ctrl/Cmd+Enter runs; Ctrl/Cmd+Shift+Enter runs the selection.

import { type QueryOutcome, type SqlSchema, type SqlTable, SqlWorkbench } from '@loams/ui';
import { useCallback, useEffect, useState } from 'react';
import { failureMessage, PAGE_MAX_ROWS, PAGE_TIMEOUT_MS, type SqlClient } from './client.js';

export interface QueryProps {
  client: SqlClient;
  namespace: string;
  databaseId: string;
  branchId: string;
  /** False while the database is not running: the editor stays, Run is disabled. */
  running: boolean;
}

const SCHEMA_SQL =
  'SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.columns ' +
  'WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, ORDINAL_POSITION';

export function Query({ client, namespace, databaseId, branchId, running }: QueryProps) {
  const [schema, setSchema] = useState<SqlSchema>({ tables: [] });

  // The branch's tables, read once it runs; completion still has keywords without them.
  useEffect(() => {
    if (!running) return;
    const ac = new AbortController();
    client
      .query(
        namespace,
        {
          databaseId,
          branchId,
          statement: SCHEMA_SQL,
          maxRows: 10_000,
          timeoutMs: PAGE_TIMEOUT_MS,
        },
        ac.signal,
      )
      .then(
        (r) => {
          const byName = new Map<string, SqlTable>();
          for (const [table, column, type] of r.rows) {
            if (!table || !column) continue;
            let t = byName.get(table);
            if (!t) {
              t = { name: table, kind: 'table', columns: [] };
              byName.set(table, t);
            }
            t.columns.push({ name: column, type: type ?? undefined });
          }
          if (!ac.signal.aborted) setSchema({ tables: [...byName.values()] });
        },
        () => undefined,
      );
    return () => ac.abort();
  }, [client, namespace, databaseId, branchId, running]);

  const run = useCallback(
    async (statement: string, signal: AbortSignal): Promise<QueryOutcome> => {
      const r = await client.query(
        namespace,
        { databaseId, branchId, statement, maxRows: PAGE_MAX_ROWS, timeoutMs: PAGE_TIMEOUT_MS },
        signal,
      );
      return {
        columns: r.columns,
        rows: r.rows,
        rowsAffected: r.rowsAffected,
        truncated: r.truncated,
        elapsedMs: r.elapsedMs,
      };
    },
    [client, namespace, databaseId, branchId],
  );

  return (
    <section aria-labelledby="sql-query-title" className="flex flex-col gap-3">
      <h3 id="sql-query-title" className="text-sm font-semibold">
        SQL
      </h3>
      <SqlWorkbench
        dialect="mysql"
        schema={schema}
        run={run}
        label="Statement"
        historyScope={`sqldb:${namespace}:${databaseId}`}
        resetKey={`${databaseId}/${branchId}`}
        disabled={!running}
        disabledReason="Resume the database to run SQL."
        errorText={failureMessage}
      />
    </section>
  );
}
