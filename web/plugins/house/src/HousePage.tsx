// `/analytics`: the Analytics (House) page (§49 §18.3, D778). It asks whether the
// server lists `loams.house.v1.HouseService`, then shows the SQL editor, the settings
// panel, the streaming results grid and the server's query history.
//
// Runs are read-only by default (`read_only = true`, `max_rows = 1 000`). With "Allow
// writes" on, a statement the server refuses with `house_read_only` asks for
// confirmation and runs again with `read_only = false`; with it off, the refusal is
// shown. Refusals from the classifier and deny list (INTO OUTFILE, FROM INFILE, host
// table functions, settings off the allowlist) are shown with their ClickHouse code.

import { PageHead } from '@loams/desktop-ui';
import { Badge, Button, Checkbox, Dialog, Empty, Notice, Select } from '@loams/ui';
import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import {
  type Cell,
  type ColumnHeader,
  type Database,
  type HouseClient,
  HouseError,
  type HouseFailure,
  newQueryId,
  PAGE_MAX_ROWS,
  PAGE_TIMEOUT_MS,
  type Progress,
  SERVER_MAX_ROWS,
  type Summary,
  toFailure,
} from './client.js';
import { Editor } from './Editor.js';
import { FailureView } from './FailureView.js';
import { KEYWORDS } from './fixtures.js';
import { HistoryPanel } from './HistoryPanel.js';
import { ResultsGrid } from './ResultsGrid.js';
import { SettingsPanel } from './SettingsPanel.js';
import { type SettingRow, settingsMap } from './settings.js';

export const PACKAGE = 'loams.house.v1';
const FIRST_SQL = 'SELECT kind, count() AS n\nFROM default.events\nGROUP BY kind\nORDER BY n DESC';
const fmt = new Intl.NumberFormat('en');

export interface HousePageProps {
  client: HouseClient;
  /** The client answers from built-in sample data, not a server. */
  sample?: boolean;
}

type Detected = 'loading' | 'available' | 'absent' | { error: string };

export function HousePage({ client, sample }: HousePageProps) {
  const [detected, setDetected] = useState<Detected>('loading');
  const [tick, setTick] = useState(0);

  // biome-ignore lint/correctness/useExhaustiveDependencies: tick retries on purpose
  useEffect(() => {
    let live = true;
    setDetected('loading');
    client.available().then(
      (ok) => live && setDetected(ok ? 'available' : 'absent'),
      (e) => live && setDetected({ error: toFailure(e).message }),
    );
    return () => {
      live = false;
    };
  }, [client, tick]);

  const head = <PageHead title="Analytics" badge={<Badge>{PACKAGE}</Badge>} />;
  if (detected === 'loading') {
    return (
      <div className="lc-page">
        {head}
        <p className="text-muted" role="status">
          Asking the server about Loams House…
        </p>
      </div>
    );
  }
  if (typeof detected === 'object') {
    return (
      <div className="lc-page">
        {head}
        <Notice tone="danger" title="Could not reach the server">
          {detected.error}
        </Notice>
        <Button onClick={() => setTick((t) => t + 1)}>Try again</Button>
      </div>
    );
  }
  if (detected === 'absent') {
    return (
      <div className="lc-page">
        {head}
        <Empty title="Analytics is not configured on this server" seed={33}>
          Loams House answers ClickHouse SQL over {PACKAGE}. This server does not list it. Configure
          a House endpoint on the server, or switch to one that has it.
        </Empty>
      </div>
    );
  }
  return <Workspace client={client} sample={sample} />;
}

interface RunState {
  columns: ColumnHeader[];
  progress?: Progress;
  summary?: Summary;
  queryId: string;
  sql: string;
  maxRows: number;
}

function Workspace({ client, sample }: { client: HouseClient; sample?: boolean }) {
  const ids = useId();
  const failureId = `${ids}-failure`;
  const [databases, setDatabases] = useState<Database[]>([]);
  const [database, setDatabase] = useState('default');
  const [sql, setSql] = useState(FIRST_SQL);
  const [settings, setSettings] = useState<SettingRow[]>([]);
  const [allowWrites, setAllowWrites] = useState(false);
  const [busy, setBusy] = useState(false);
  const [run, setRun] = useState<RunState>();
  const rowsRef = useRef<Cell[][]>([]);
  const [, setVersion] = useState(0);
  const [failure, setFailure] = useState<HouseFailure>();
  const [notice, setNotice] = useState<string>();
  const [plan, setPlan] = useState<string>();
  const [confirm, setConfirm] = useState<{ sql: string }>();
  const [historyTick, setHistoryTick] = useState(0);
  const [functions, setFunctions] = useState<string[]>([]);
  const abort = useRef<AbortController | undefined>(undefined);
  const queryId = useRef<string | undefined>(undefined);

  useEffect(() => () => abort.current?.abort(), []);

  useEffect(() => {
    let live = true;
    const ac = new AbortController();
    client.listDatabases().then(
      (p) => {
        if (!live) return;
        setDatabases(p.items);
        if (p.items.length && !p.items.some((d) => d.name === 'default'))
          setDatabase(p.items[0]?.name ?? 'default');
      },
      () => undefined,
    );
    // The function list, fetched once through ExecuteQuery (§49 §18.3).
    (async () => {
      const names: string[] = [];
      try {
        for await (const f of client.executeQuery(
          {
            sql: 'SELECT name, is_aggregate FROM system.functions ORDER BY name',
            database: 'system',
            settings: {},
            params: {},
            format: 'ARROW_IPC',
            maxRows: SERVER_MAX_ROWS,
            timeoutMs: PAGE_TIMEOUT_MS,
            readOnly: true,
          },
          ac.signal,
        )) {
          if (f.kind === 'batch')
            for (const r of f.rows) if (typeof r[0] === 'string') names.push(r[0]);
        }
      } catch {
        // completions are a convenience: keywords alone still work
      }
      if (live) setFunctions(names);
    })();
    return () => {
      live = false;
      // Leaving the page stops the function-list stream rather than reading it out.
      ac.abort();
    };
  }, [client]);

  const words = useMemo(() => [...KEYWORDS, ...functions], [functions]);

  const execute = useCallback(
    async (text: string, readOnly: boolean, maxRows = PAGE_MAX_ROWS) => {
      const s = settingsMap(settings);
      if (!s.ok) {
        setFailure({
          code: 'invalid_argument',
          message: s.problem,
          reason: 'house_access_denied',
          metadata: {},
        });
        return;
      }
      abort.current?.abort();
      const c = new AbortController();
      abort.current = c;
      const id = newQueryId();
      queryId.current = id;
      rowsRef.current = [];
      setVersion((v) => v + 1);
      setBusy(true);
      setFailure(undefined);
      setNotice(undefined);
      setPlan(undefined);
      setRun({ columns: [], queryId: id, sql: text, maxRows });
      try {
        for await (const f of client.executeQuery(
          {
            sql: text,
            database,
            settings: s.settings,
            params: {},
            format: 'ARROW_IPC',
            maxRows,
            timeoutMs: PAGE_TIMEOUT_MS,
            queryId: id,
            readOnly,
          },
          c.signal,
        )) {
          if (c.signal.aborted) break;
          if (f.kind === 'header') setRun((r) => r && { ...r, columns: f.columns });
          else if (f.kind === 'batch') {
            rowsRef.current.push(...f.rows);
            setVersion((v) => v + 1);
          } else if (f.kind === 'progress') setRun((r) => r && { ...r, progress: f.progress });
          else setRun((r) => r && { ...r, summary: f.summary });
        }
        if (c.signal.aborted && abort.current === c)
          setNotice('Cancelled. The House stops the statement.');
      } catch (e) {
        const f = toFailure(e);
        // A run that a newer one replaced reports nothing.
        if (abort.current !== c) return;
        if (f.reason === 'house_cancelled' || c.signal.aborted) {
          setNotice('Cancelled. The House stops the statement.');
        } else if (
          e instanceof HouseError &&
          f.reason === 'house_read_only' &&
          readOnly &&
          allowWrites
        ) {
          // The server classified it as a write: ask, then run it writable.
          setRun(undefined);
          setConfirm({ sql: text });
        } else {
          setRun(undefined);
          setFailure(f);
        }
      } finally {
        if (abort.current === c) {
          setBusy(false);
          abort.current = undefined;
        }
        setHistoryTick((t) => t + 1);
      }
    },
    [client, database, settings, allowWrites],
  );

  const cancel = () => {
    const id = queryId.current;
    abort.current?.abort();
    if (id) void client.cancelQuery(id).catch(() => undefined);
  };

  const explain = async () => {
    setFailure(undefined);
    setNotice(undefined);
    try {
      const res = await client.explainQuery({ sql, database, kind: 'PLAN' });
      setRun(undefined);
      setPlan(res.text);
    } catch (e) {
      setFailure(toFailure(e));
    }
  };

  const rows = rowsRef.current;
  const summary = run?.summary;
  const progress = run?.progress;
  const pct =
    progress && progress.totalRowsToRead > 0
      ? Math.min(100, Math.round((progress.readRows / progress.totalRowsToRead) * 100))
      : undefined;

  return (
    <div className="lc-page">
      <PageHead
        title="Analytics"
        badge={<Badge>{PACKAGE}</Badge>}
        subtitle="ClickHouse SQL over Loams House: lake and realtime tables, read-only by default."
        actions={
          databases.length > 0 && (
            <label className="flex items-center gap-2 text-sm" htmlFor={`${ids}-db`}>
              <span className="text-muted">Database</span>
              <Select
                id={`${ids}-db`}
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
              >
                {databases.map((d) => (
                  <option key={d.name} value={d.name}>
                    {d.name}
                  </option>
                ))}
              </Select>
            </label>
          )
        }
      />
      {sample && (
        <Notice tone="warn" title="Sample data">
          This page runs against a built-in sample House until the server serves {PACKAGE}. Nothing
          you run here reaches a server.
        </Notice>
      )}
      <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_300px]">
        <div className="flex min-w-0 flex-col gap-4">
          <section aria-label="Editor" className="flex flex-col gap-3">
            <Editor
              value={sql}
              onChange={setSql}
              onRun={() => void execute(sql, true)}
              words={words}
              disabled={busy}
              describedBy={failure ? failureId : undefined}
              invalid={!!failure}
            />
            <div className="flex flex-wrap items-center gap-2">
              <Button
                variant="primary"
                onClick={() => void execute(sql, true)}
                disabled={busy || !sql.trim()}
              >
                Run
              </Button>
              <Button onClick={() => void explain()} disabled={busy || !sql.trim()}>
                Explain
              </Button>
              {busy && (
                <Button variant="quiet" onClick={cancel}>
                  Cancel
                </Button>
              )}
              <Checkbox
                label="Allow writes"
                checked={allowWrites}
                onChange={(e) => setAllowWrites(e.target.checked)}
              />
              <span className="text-xs text-muted">
                Ctrl or Cmd + Enter runs. Ctrl + Space completes.
              </span>
            </div>
          </section>

          {failure && <FailureView id={failureId} failure={failure} writesAllowed={allowWrites} />}
          {notice && <Notice title={notice} />}

          {busy && (
            <div className="flex items-center gap-3 text-sm text-muted" role="status">
              <span>Running… {progress ? `${fmt.format(progress.readRows)} rows read` : ''}</span>
              {pct !== undefined && (
                <progress
                  max={100}
                  value={pct}
                  aria-label="Rows read of the estimate"
                  className="w-40"
                />
              )}
            </div>
          )}

          {plan !== undefined && (
            <section aria-label="Plan" className="flex flex-col gap-2">
              <h3 className="m-0 text-base">Plan</h3>
              <pre className="m-0 overflow-auto rounded-md bg-surface p-3 font-mono text-xs">
                {plan}
              </pre>
            </section>
          )}

          {run && (run.columns.length > 0 || summary) && (
            <section aria-label="Results" className="flex flex-col gap-3">
              <p className="m-0 text-sm text-muted" role="status">
                {fmt.format(rows.length)} {rows.length === 1 ? 'row' : 'rows'}
                {summary &&
                  ` in ${fmt.format(summary.elapsedMs)} ms · ${fmt.format(summary.readRows)} rows read`}
                {summary &&
                  summary.writtenRows > 0 &&
                  ` · ${fmt.format(summary.writtenRows)} written`}
              </p>
              {summary?.truncated && (
                <div
                  className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-solid border-rule bg-accent-soft px-3 py-2"
                  role="status"
                  data-testid="truncated-banner"
                >
                  <span>
                    Showing the first {fmt.format(rows.length)} rows. The result has more.
                  </span>
                  {run.maxRows < SERVER_MAX_ROWS && (
                    <Button
                      size="sm"
                      disabled={busy}
                      onClick={() => void execute(run.sql, true, SERVER_MAX_ROWS)}
                    >
                      Stream up to {fmt.format(SERVER_MAX_ROWS)} rows
                    </Button>
                  )}
                </div>
              )}
              <ResultsGrid columns={run.columns} rows={rows} />
            </section>
          )}
        </div>

        <aside className="flex flex-col gap-6" aria-label="Settings and history">
          <SettingsPanel rows={settings} onChange={setSettings} />
          <HistoryPanel client={client} refresh={historyTick} onOpen={setSql} />
        </aside>
      </div>

      <Dialog
        open={!!confirm}
        onClose={() => setConfirm(undefined)}
        title="Run a statement that changes data?"
        footer={
          <>
            <Button onClick={() => setConfirm(undefined)}>Cancel</Button>
            <Button
              variant="danger"
              onClick={() => {
                const text = confirm?.sql;
                setConfirm(undefined);
                if (text) void execute(text, false);
              }}
            >
              Run
            </Button>
          </>
        }
      >
        <p className="m-0 text-sm">
          The House says this statement writes. It runs once more, with writes allowed, on{' '}
          <span className="font-mono">{database}</span>.
        </p>
        <pre className="m-0 mt-2 max-h-48 overflow-auto rounded-md bg-surface p-2 font-mono text-xs">
          {confirm?.sql}
        </pre>
      </Dialog>
    </div>
  );
}
