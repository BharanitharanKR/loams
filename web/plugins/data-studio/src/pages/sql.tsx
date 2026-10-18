import {
  type QueryOutcome,
  SchemaPanel,
  type SqlEditorHandle,
  type SqlFunction,
  type SqlSchema,
  type SqlTable,
  SqlWorkbench,
} from '@loams/ui';
import { useCallback, useEffect, useRef, useState } from 'react';
import type { DataClient } from '../client.js';
import { PageHead, Tabs } from '../shared.js';

/** The namespace's tables and functions, read with SQL from information_schema. */
export async function loadEngineSchema(client: DataClient, ns: string): Promise<SqlSchema> {
  const cols = await client.sql(
    ns,
    'SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns ' +
      "WHERE table_schema <> 'information_schema' ORDER BY table_schema, table_name, ordinal_position",
  );
  const tables = new Map<string, SqlTable>();
  for (const r of cols.rows) {
    const [schema, table, column, type] = r.map((v) => (typeof v === 'string' ? v : String(v)));
    if (!schema || !table || !column) continue;
    const key = `${schema}.${table}`;
    let t = tables.get(key);
    if (!t) {
      t = {
        name: table,
        schema,
        kind: schema === 'collections' ? 'collection' : 'table',
        columns: [],
      };
      tables.set(key, t);
    }
    t.columns.push({ name: column, type });
  }
  let functions: SqlFunction[] = [];
  try {
    const fns = await client.sql(
      ns,
      'SELECT DISTINCT routine_name, function_type FROM information_schema.routines',
    );
    functions = fns.rows.flatMap((r) => {
      const [name, kind] = r;
      if (typeof name !== 'string') return [];
      const k = String(kind).toLowerCase();
      return [
        {
          name,
          kind: k === 'aggregate' ? 'aggregate' : k === 'window' ? 'window' : 'scalar',
        } satisfies SqlFunction,
      ];
    });
  } catch {
    // function names are a convenience
  }
  return { tables: [...tables.values()], functions, defaultSchema: 'collections' };
}

/** `/data/:ns/sql`: a read-only SQL editor over the namespace. */
export function SqlPage({
  client,
  ns,
  navigate,
}: {
  client: DataClient;
  ns: string;
  navigate: (to: string) => void;
}) {
  const [schema, setSchema] = useState<SqlSchema>({ tables: [] });
  const [schemaState, setSchemaState] = useState<{ loading: boolean; error?: string }>({
    loading: true,
  });
  const editor = useRef<SqlEditorHandle>(null);

  useEffect(() => {
    let live = true;
    setSchemaState({ loading: true });
    loadEngineSchema(client, ns).then(
      (s) => {
        if (!live) return;
        setSchema(s);
        setSchemaState({ loading: false });
      },
      (e) =>
        live &&
        setSchemaState({
          loading: false,
          error: `Could not read the schema: ${String(e?.message ?? e)}`,
        }),
    );
    return () => {
      live = false;
    };
  }, [client, ns]);

  const run = useCallback(
    async (text: string): Promise<QueryOutcome> => {
      const r = await client.sql(ns, text);
      return { columns: r.columns, rows: r.rows, truncated: r.truncated };
    },
    [client, ns],
  );

  return (
    <div className="lc-page ds-page">
      <PageHead
        crumbs={
          <>
            <a href="#/data">Data</a> / <a href={`#/data/${encodeURIComponent(ns)}`}>{ns}</a>
          </>
        }
        title="SQL"
        subtitle={`Read-only SQL (DataFusion) over ${ns}: collections are tables, and vector_search, text_search, rrf and the graph functions are table functions.`}
      />
      <Tabs ns={ns} active="sql" navigate={navigate} />
      <SqlWorkbench
        dialect="datafusion"
        schema={schema}
        run={run}
        historyScope={`engine:${ns}`}
        label="SQL"
        resetKey={ns}
        handle={editor}
        aside={
          <SchemaPanel
            schema={schema}
            loading={schemaState.loading}
            error={schemaState.error}
            onInsert={(name) => editor.current?.insert(name)}
          />
        }
      />
    </div>
  );
}
