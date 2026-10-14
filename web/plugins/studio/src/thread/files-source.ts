// A thread's checkout as @loams/studio-ui's `FilesSource` (SD1 Task 21): agentd's
// `loams.agentd.v1.FileService` over the agentd transport (SD1 Task 6), the same on the
// desktop and the web. Every call names its checkout by `WorkspaceTarget` (the thread's
// chat id), which the daemon resolves and jails. Content search is not here: it is the
// `ContentSearchSource` seam, `pendingContentSearch()` until SD1 Task 11's
// `SearchWorkspaceContent` lands.

import { Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';
import {
  FileService,
  WorkspaceEntryKind,
  WorkspaceFileChangeKind,
  WorkspaceReadOnlyReason,
  type WorkspaceTarget,
  WorkspaceTextEncoding,
} from '@loams/proto/agentd';
import type {
  EntryKind,
  FileChangeKind,
  FileChanges,
  FilesSource,
  ReadOnlyReason,
} from '@loams/studio-ui/files';

type Target = Pick<WorkspaceTarget, 'chatId' | 'spaceId' | 'checkoutPath'>;

const ENTRY_KIND: Record<WorkspaceEntryKind, EntryKind> = {
  [WorkspaceEntryKind.UNSPECIFIED]: 'file',
  [WorkspaceEntryKind.FILE]: 'file',
  [WorkspaceEntryKind.DIRECTORY]: 'directory',
  [WorkspaceEntryKind.SYMLINK]: 'symlink',
};

const READ_ONLY: Partial<Record<WorkspaceReadOnlyReason, ReadOnlyReason>> = {
  [WorkspaceReadOnlyReason.BINARY]: 'binary',
  [WorkspaceReadOnlyReason.UNSUPPORTED_ENCODING]: 'unsupported-encoding',
  [WorkspaceReadOnlyReason.MIXED_LINE_ENDINGS]: 'mixed-line-endings',
  [WorkspaceReadOnlyReason.SYMLINK]: 'symlink',
  [WorkspaceReadOnlyReason.TOO_LARGE]: 'too-large',
  [WorkspaceReadOnlyReason.PERMISSION_DENIED]: 'permission-denied',
  [WorkspaceReadOnlyReason.NOT_REGULAR_FILE]: 'not-regular-file',
  [WorkspaceReadOnlyReason.OUTSIDE_WORKSPACE]: 'outside-workspace',
};

const CHANGE_KIND: Partial<Record<WorkspaceFileChangeKind, FileChangeKind>> = {
  [WorkspaceFileChangeKind.CREATED]: 'created',
  [WorkspaceFileChangeKind.MODIFIED]: 'modified',
  [WorkspaceFileChangeKind.REMOVED]: 'removed',
  [WorkspaceFileChangeKind.RENAMED]: 'renamed',
};

/** An image is read in chunks; past this many bytes the viewer gives up. */
const MAX_IMAGE_BYTES = 16 * 1024 * 1024;

const transportIds = new WeakMap<Transport, number>();
let nextTransportId = 1;

/** One id per transport object, so two hosts' checkouts never share a cache entry. */
function transportId(transport: Transport): number {
  let id = transportIds.get(transport);
  if (id === undefined) {
    id = nextTransportId++;
    transportIds.set(transport, id);
  }
  return id;
}

function base64Bytes(data: string): Uint8Array {
  const binary = atob(data);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

const sleep = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener('abort', () => {
      clearTimeout(timer);
      resolve();
    });
  });

export function agentdFilesSource(transport: Transport, target: Target): FilesSource {
  const client = createClient(FileService, transport);
  const where = target.chatId
    ? `chat:${target.chatId}`
    : target.spaceId
      ? `space:${target.spaceId}`
      : `path:${target.checkoutPath ?? ''}`;
  // The checkout a listing or read last named; an image read must name it.
  let checkoutId: string | undefined;

  const opts = (signal?: AbortSignal) => (signal ? { signal } : {});

  const listDirectory: FilesSource['listDirectory'] = async (directory, signal) => {
    const { page } = await client.listDirectory({ target, directory }, opts(signal));
    if (page?.checkoutId) checkoutId = page.checkoutId;
    return {
      directory,
      truncated: page?.truncated ?? false,
      entries: (page?.entries ?? []).map((entry) => ({
        path: entry.path,
        name: entry.name,
        kind: ENTRY_KIND[entry.kind] ?? 'file',
        ignored: entry.ignored,
        ...(entry.size !== undefined ? { size: Number(entry.size) } : {}),
      })),
    };
  };

  return {
    key: `agentd:${transportId(transport)}:${where}`,
    listDirectory,

    async readFile(path, signal) {
      const { file } = await client.readFile({ target, path }, opts(signal));
      if (!file) throw new Error('The daemon returned no file.');
      if (file.checkoutId) checkoutId = file.checkoutId;
      const binary = file.encoding === WorkspaceTextEncoding.BINARY;
      const reason = file.readOnlyReason !== undefined ? READ_ONLY[file.readOnlyReason] : undefined;
      const readOnlyReason: ReadOnlyReason | undefined =
        file.text === undefined ? (reason ?? (binary ? 'binary' : undefined)) : undefined;
      return {
        path: file.path,
        text: file.text ?? null,
        size: Number(file.size),
        truncated: file.truncated,
        ...(file.contentHash ? { contentHash: file.contentHash } : {}),
        ...(readOnlyReason ? { readOnlyReason } : {}),
      };
    },

    async readImage(path, signal) {
      if (!checkoutId) await listDirectory('', signal);
      const parts: Uint8Array[] = [];
      let offset = 0n;
      let total = 0;
      let hash: string | undefined;
      let mimeType = '';
      for (;;) {
        const { chunk } = await client.readImage(
          {
            target,
            path,
            expectedCheckoutId: checkoutId ?? '',
            offset,
            ...(hash ? { expectedContentHash: hash } : {}),
          },
          opts(signal),
        );
        if (!chunk) throw new Error('The daemon returned no image.');
        hash = chunk.contentHash;
        mimeType = chunk.mimeType;
        const bytes = base64Bytes(chunk.data);
        parts.push(bytes);
        total += bytes.length;
        if (total > MAX_IMAGE_BYTES) throw new Error('The image is too large to show.');
        if (chunk.done || chunk.nextOffset <= offset) break;
        offset = chunk.nextOffset;
      }
      const bytes = new Uint8Array(total);
      let at = 0;
      for (const part of parts) {
        bytes.set(part, at);
        at += part.length;
      }
      return { mimeType, bytes };
    },

    async searchFiles(query, limit, signal) {
      const { matches } = await client.search({ target, query, limit }, opts(signal));
      return matches.map((match) => ({
        path: match.path,
        name: match.name,
        kind: ENTRY_KIND[match.kind] ?? 'file',
        score: Number(match.score),
      }));
    },

    watch(listener) {
      const controller = new AbortController();
      const { signal } = controller;
      void (async () => {
        let delay = 1000;
        let reconnect = false;
        while (!signal.aborted) {
          try {
            for await (const { changes } of client.watchFiles({ target }, { signal })) {
              delay = 1000;
              if (reconnect) {
                // Changes may have been missed while the stream was down.
                reconnect = false;
                listener({ resyncRequired: true, changes: [] });
              }
              if (!changes) continue;
              const batch: FileChanges = {
                resyncRequired: changes.resyncRequired,
                changes: changes.changes.flatMap((change) => {
                  const kind = CHANGE_KIND[change.kind];
                  if (!kind) return [];
                  return [
                    {
                      kind,
                      path: change.path,
                      ...(change.oldPath ? { oldPath: change.oldPath } : {}),
                    },
                  ];
                }),
              };
              listener(batch);
            }
          } catch (error) {
            if (signal.aborted) return;
            const code = error instanceof ConnectError ? error.code : undefined;
            if (code === Code.Unimplemented || code === Code.PermissionDenied) return;
          }
          if (signal.aborted) return;
          reconnect = true;
          await sleep(delay, signal);
          delay = Math.min(delay * 2, 30_000);
        }
      })();
      return () => controller.abort();
    },
  };
}
