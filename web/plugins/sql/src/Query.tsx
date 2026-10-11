// The SQL editor and its results grid. Ctrl/Cmd+Enter runs; Escape cancels a run.

import { Button, Notice } from '@loams/ui';
import { useEffect, useRef, useState } from 'react';
import {
  failureMessage,
  PAGE_MAX_ROWS,
  PAGE_TIMEOUT_MS,
  type QueryResult,
  type SqlClient,
} from './client.js';

export interface QueryProps {
  client: SqlClient;
  namespace: string;
  databaseId: string;
  branchId: string;
  /** False while the database is not running: the editor stays, Run is disabled. */
  running: boolean;
}

type Outcome =
  | { state: 'idle' }
  | { state: 'running' }
  | { state: 'error'; message: string }
  | { state: 'done'; result: QueryResult };

export function Query({ client, namespace, databaseId, branchId, running }: QueryProps) {
  const [statement, setStatement] = useState('SELECT 1;');
  const [outcome, setOutcome] = useState<Outcome>({ state: 'idle' });
  const abort = useRef<AbortController | null>(null);

  // A new branch is a new target: drop the last result and any run in flight.
  // biome-ignore lint/correctness/useExhaustiveDependencies: reset on target change
  useEffect(() => {
    abort.current?.abort();
    abort.current = null;
    setOutcome({ state: 'idle' });
  }, [databaseId, branchId]);

  useEffect(() => () => abort.current?.abort(), []);

  const run = async () => {
    if (!running || outcome.state === 'running' || !statement.trim()) return;
    const ac = new AbortController();
    abort.current = ac;
    setOutcome({ state: 'running' });
    try {
      const result = await client.query(
        namespace,
        { databaseId, branchId, statement, maxRows: PAGE_MAX_ROWS, timeoutMs: PAGE_TIMEOUT_MS },
        ac.signal,
      );
      if (abort.current === ac) setOutcome({ state: 'done', result });
    } catch (e) {
      if (abort.current === ac) setOutcome({ state: 'error', message: failureMessage(e) });
    } finally {
      if (abort.current === ac) abort.current = null;
    }
  };

  const cancel = () => {
    abort.current?.abort();
    abort.current = null;
    setOutcome({ state: 'idle' });
  };

  return (
    <section aria-labelledby="sql-query-title" className="flex flex-col gap-3">
      <h3 id="sql-query-title" className="text-sm font-semibold">
        SQL
      </h3>
      <label htmlFor="sql-statement" className="sr-only">
        Statement
      </label>
      <textarea
        id="sql-statement"
        className="loams-input font-mono text-sm min-h-32"
        spellCheck={false}
        value={statement}
        aria-describedby="sql-statement-hint"
        onChange={(e) => setStatement(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
            e.preventDefault();
            void run();
          } else if (e.key === 'Escape' && outcome.state === 'running') {
            e.preventDefault();
            cancel();
          }
        }}
      />
      <div className="flex items-center gap-2">
        <Button
          variant="primary"
          disabled={!running || outcome.state === 'running' || !statement.trim()}
          onClick={() => void run()}
        >
          Run
        </Button>
        {outcome.state === 'running' && <Button onClick={cancel}>Cancel</Button>}
        <span id="sql-statement-hint" className="text-faint text-xs">
          {running
            ? `Ctrl+Enter runs. At most ${PAGE_MAX_ROWS} rows, ${PAGE_TIMEOUT_MS / 1000} s.`
            : 'Resume the database to run SQL.'}
        </span>
      </div>
      <div aria-live="polite">
        {outcome.state === 'running' && (
          <p className="text-muted" role="status">
            Running…
          </p>
        )}
        {outcome.state === 'error' && (
          <Notice tone="danger" title="The statement failed">
            {outcome.message}
          </Notice>
        )}
        {outcome.state === 'done' && <ResultGrid result={outcome.result} />}
      </div>
    </section>
  );
}

export function ResultGrid({ result }: { result: QueryResult }) {
  const summary =
    result.columns.length === 0
      ? `${result.rowsAffected} row${result.rowsAffected === 1 ? '' : 's'} affected`
      : `${result.rows.length} row${result.rows.length === 1 ? '' : 's'}${result.truncated ? ' (truncated)' : ''}`;
  return (
    <div className="flex flex-col gap-2">
      <p className="text-muted text-xs" role="status">
        {summary} · {result.elapsedMs} ms
      </p>
      {result.columns.length > 0 && (
        <div className="loams-table-wrap" role="region" aria-label="Results" tabIndex={0}>
          <table className="loams-table">
            <caption className="sr-only">Query results</caption>
            <thead>
              <tr>
                {result.columns.map((c, i) => (
                  // biome-ignore lint/suspicious/noArrayIndexKey: columns can repeat names
                  <th key={i} scope="col" title={c.type}>
                    {c.name}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, r) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: rows have no identity
                <tr key={r}>
                  {row.map((cell, i) => (
                    // biome-ignore lint/suspicious/noArrayIndexKey: positional cells
                    <td key={i} className="font-mono">
                      {cell === null ? <span className="text-faint">NULL</span> : cell}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
