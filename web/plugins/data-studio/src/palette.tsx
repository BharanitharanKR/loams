// The palette's tables (design 59, D1078; CF1 Task 11): the open environment's
// collections, which the namespace's SQL serves as tables, from the Data explorer's own
// `ListCollections`. A row opens the collection; once something is typed, each table
// also offers Search and the SQL editor (ruling T11-2).

import type { PaletteCommand } from '@loams/slots';
import { useEffect, useState } from 'react';
import type { DataClient } from './client.js';

/** The statement the SQL editor opens with for a table (`/data/:ns/sql?table=`). */
export function tableStatement(table: string): string {
  return `SELECT * FROM "${table.replaceAll('"', '""')}" LIMIT 100`;
}

/** The open environment's tables, by name, for the palette (D1078). Loaded once per opening. */
export function TablesSource({
  query,
  publish,
  client,
  namespace,
  navigate,
}: {
  query: string;
  publish: (commands: readonly PaletteCommand[]) => void;
  client: Pick<DataClient, 'listCollections'>;
  namespace: string | undefined;
  navigate: (to: string) => void;
}) {
  const [names, setNames] = useState<string[]>([]);
  useEffect(() => {
    if (!namespace) return;
    let live = true;
    client
      .listCollections(namespace)
      .then((cs) => live && setNames(cs.map((c) => c.name)))
      .catch(() => live && setNames([]));
    return () => {
      live = false;
    };
  }, [client, namespace]);
  const typed = query.trim() !== '';
  useEffect(() => {
    if (!namespace) return publish([]);
    const ns = encodeURIComponent(namespace);
    publish(
      names.flatMap((name) => {
        const at = `/data/${ns}/${encodeURIComponent(name)}`;
        const keywords = [`${namespace}.${name}`];
        const open: PaletteCommand = {
          id: `table:${namespace}.${name}`,
          title: name,
          group: 'Tables',
          detail: namespace,
          keywords,
          run: () => navigate(at),
        };
        if (!typed) return [open];
        return [
          open,
          {
            id: `table-search:${namespace}.${name}`,
            title: `Search ${name}`,
            group: 'Tables',
            detail: namespace,
            keywords,
            secondary: true,
            run: () => navigate(`${at}/search`),
          },
          {
            id: `table-sql:${namespace}.${name}`,
            title: `Query ${name} in SQL`,
            group: 'Tables',
            detail: namespace,
            keywords,
            secondary: true,
            run: () => navigate(`/data/${ns}/sql?table=${encodeURIComponent(name)}`),
          },
        ];
      }),
    );
  }, [names, namespace, navigate, publish, typed]);
  return null;
}
