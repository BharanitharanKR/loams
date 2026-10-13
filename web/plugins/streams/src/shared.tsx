// Small pieces every Streams page uses: an async loader, error text, the
// page header and the namespace picker (free text plus a recent list, as in
// Data Studio).

import {
  type Loaded,
  rememberNamespace as rememberShared,
  NamespacePicker as SharedPicker,
  useLoad as useSharedLoad,
} from '@loams/desktop-ui';
import { isNotServed, Notice } from '@loams/ui';
import { ApiError } from './client.js';

/**
 * The message a load carries when the server has no Streams API at all (a server
 * that is not an engine, such as the desktop's Demo server): the page then says
 * so calmly instead of showing an error.
 */
export const NOT_SERVED = 'This server does not serve streams and links.';

export function errorText(e: unknown): string {
  if (
    e instanceof ApiError &&
    (isNotServed({ code: e.code }) || (e.status === 404 && e.code === 'error'))
  )
    return NOT_SERVED;
  return e instanceof Error ? e.message : String(e);
}

export type { Loaded } from '@loams/desktop-ui';
export { PageHead } from '@loams/desktop-ui';

/** Runs `load` on mount and whenever `deps` change; a reload keeps the old data in view. */
export function useLoad<T>(load: () => Promise<T>, deps: unknown[]): [Loaded<T>, () => void] {
  return useSharedLoad(load, deps, { keepOnReload: true, errorText });
}

export function ErrorNotice({ title, message }: { title: string; message: string }) {
  return (
    <Notice tone="danger" title={title}>
      {message}
    </Notice>
  );
}

const RECENT_KEY = 'loams.streams.namespaces';

export function rememberNamespace(ns: string): void {
  rememberShared(RECENT_KEY, ns);
}

export function NamespacePicker({ ns, onOpen }: { ns: string; onOpen: (ns: string) => void }) {
  return <SharedPicker ns={ns} onOpen={onOpen} storageKey={RECENT_KEY} idPrefix="sp" />;
}
