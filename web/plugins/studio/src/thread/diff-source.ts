// A thread's checkout as @loams/studio-ui's `DiffSource` (SD1 Task 20): agentd's
// `DiffService`, `ReviewService` and `GitService` over the agentd transport (SD1 Task 6),
// the same on the desktop and the web. The thread's cwd and checkout come from its `Chat`.
// Discard, DiscardPaths and PostReviewToChangeRequest are git writes (T9-8, T11-4): the
// console's guard lets them through for Studio, which holds `agentd:git-write`; the daemon
// refuses a stale checksum with `stale_diff`, which becomes `StaleDiffError`.

import { Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';
import {
  ChangeRequestState,
  type CheckoutDiff,
  DiffService,
  DiffSide,
  GitService,
  type ReviewComment as ProtoReviewComment,
  ReviewEvent as ProtoReviewEvent,
  ReviewService,
} from '@loams/proto/agentd';
import { ErrorInfoSchema } from '@loams/proto/errors';
import {
  type ChangeRequestInfo,
  type CheckoutDiffView,
  type DiffSelection,
  type DiffSource,
  type ReviewComment,
  StaleDiffError,
} from '@loams/studio-ui/diff';

/** What the diff panel needs to know about the thread. */
export interface DiffThread {
  readonly chatId: string;
  readonly cwd: string;
  readonly checkoutId?: string;
  readonly branch?: string;
}

const RETRY_MS = [1000, 2000, 5000, 15000, 30000];

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

/** `stale_diff` from the daemon (ErrorInfo reason, or the ndjson text's prefix). */
export function isStaleDiff(error: unknown): boolean {
  if (!(error instanceof ConnectError)) return false;
  if (error.code !== Code.FailedPrecondition) return false;
  const info = error.findDetails(ErrorInfoSchema)[0];
  return info?.reason === 'stale_diff' || error.rawMessage.startsWith('stale_diff');
}

async function guarded<T>(call: () => Promise<T>): Promise<T> {
  try {
    return await call();
  } catch (error) {
    if (isStaleDiff(error)) throw new StaleDiffError();
    throw error;
  }
}

function view(diff: CheckoutDiff | undefined): CheckoutDiffView {
  if (!diff) throw new Error('The daemon returned no diff.');
  return {
    checkoutId: diff.checkoutId,
    cwd: diff.cwd,
    patch: diff.patch,
    files: diff.files.map((file) => ({
      path: file.path,
      ...(file.oldPath ? { oldPath: file.oldPath } : {}),
      status: file.status,
      additions: file.additions,
      deletions: file.deletions,
      binary: file.binary,
    })),
    additions: diff.additions,
    deletions: diff.deletions,
    truncated: diff.truncated,
    checksum: diff.checksum,
    updatedAtMs: Number(diff.updatedAtMs),
  };
}

function comment(c: ProtoReviewComment): ReviewComment {
  return {
    id: c.id,
    checkoutId: c.checkoutId,
    path: c.path,
    side: c.side === DiffSide.OLD ? 'old' : 'new',
    line: c.line,
    ...(c.endLine !== undefined ? { endLine: c.endLine } : {}),
    body: c.body,
    diffChecksum: c.diffChecksum,
    createdAtMs: Number(c.createdAtMs),
    resolved: c.resolved,
    outdated: c.outdated,
  };
}

/** `GetCheckoutDiff`'s mode and refs for a selection. */
export function diffRequest(thread: DiffThread, selection: DiffSelection) {
  switch (selection.kind) {
    case 'branch':
      return {
        cwd: thread.cwd,
        mode: 'branch',
        ...(selection.baseRef ? { baseRef: selection.baseRef } : {}),
      };
    case 'commit':
      return {
        cwd: thread.cwd,
        mode: 'commit',
        ...(selection.commitSha ? { commitSha: selection.commitSha } : {}),
      };
    case 'turn':
      return { cwd: thread.cwd, mode: 'turn', chatId: thread.chatId };
    default:
      return { cwd: thread.cwd, mode: '' };
  }
}

/**
 * Follows a server stream until the returned function is called: a stream that ends or
 * fails is reopened after a backoff; `unimplemented` and `permission_denied` stop it.
 */
function follow<T>(
  open: (signal: AbortSignal) => AsyncIterable<T>,
  onFrame: (frame: T) => void,
  onError?: (error: Error) => void,
  onReopen?: () => void,
): () => void {
  const controller = new AbortController();
  const { signal } = controller;
  void (async () => {
    let attempt = 0;
    while (!signal.aborted) {
      try {
        for await (const frame of open(signal)) {
          attempt = 0;
          onFrame(frame);
        }
      } catch (error) {
        if (signal.aborted) return;
        const code = error instanceof ConnectError ? error.code : undefined;
        if (code === Code.Unimplemented || code === Code.PermissionDenied) {
          onError?.(error as Error);
          return;
        }
        if (attempt >= 1) onError?.(error instanceof Error ? error : new Error(String(error)));
      }
      if (signal.aborted) return;
      const delay = RETRY_MS[Math.min(attempt, RETRY_MS.length - 1)] ?? 30000;
      attempt++;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, delay);
        signal.addEventListener('abort', () => {
          clearTimeout(timer);
          resolve();
        });
      });
      onReopen?.();
    }
  })();
  return () => controller.abort();
}

export function agentdDiffSource(transport: Transport, thread: DiffThread): DiffSource {
  const diff = createClient(DiffService, transport);
  const review = createClient(ReviewService, transport);
  const git = createClient(GitService, transport);
  const opts = (signal?: AbortSignal) => (signal ? { signal } : {});

  return {
    key: `agentd:${transportId(transport)}:chat:${thread.chatId}:${thread.cwd}`,

    watchWorkingDiff(listener, onError) {
      let checkoutId = thread.checkoutId;
      let seen = false;
      const emit = (next: CheckoutDiffView) => {
        seen = true;
        checkoutId = next.checkoutId;
        listener(next);
      };
      // The daemon's diff sync tracks only some checkouts; ask once, then follow it.
      const controller = new AbortController();
      diff
        .getCheckoutDiff({ cwd: thread.cwd, mode: '' }, { signal: controller.signal })
        .then((reply) => {
          if (!controller.signal.aborted) emit(view(reply.diff));
        })
        .catch((error: unknown) => {
          if (!controller.signal.aborted && !seen) {
            onError(error instanceof Error ? error : new Error(String(error)));
          }
        });
      const stop = follow(
        (signal) => diff.watchCheckoutDiffs({}, { signal }),
        ({ diffs }) => {
          const mine = diffs.find(
            (d) => (checkoutId && d.checkoutId === checkoutId) || d.cwd === thread.cwd,
          );
          if (mine) emit(view(mine));
        },
      );
      return () => {
        controller.abort();
        stop();
      };
    },

    async getDiff(selection, signal) {
      const reply = await diff.getCheckoutDiff(diffRequest(thread, selection), opts(signal));
      return view(reply.diff);
    },

    async fileText(selection, current, path, signal) {
      const reply = await diff.getCheckoutFileDiffText(
        {
          ...diffRequest(thread, selection),
          checkoutId: current.checkoutId,
          path,
          diffChecksum: current.checksum,
        },
        opts(signal),
      );
      const text = reply.text;
      return {
        oldText: text?.oldText ?? null,
        newText: text?.newText ?? null,
        binary: text?.binary ?? false,
        truncated: text?.truncated ?? false,
        stale: text?.stale ?? true,
      };
    },

    async listBranches(signal) {
      const reply = await git.listBranches({ repoPath: thread.cwd }, opts(signal));
      return reply.branches.filter((branch) => branch !== thread.branch);
    },

    async listCommits(signal) {
      const reply = await git.listGitHistory({ cwd: thread.cwd, limit: 30n }, opts(signal));
      return (reply.page?.commits ?? []).map((c) => ({ sha: c.sha, subject: c.subject }));
    },

    watchComments(checkoutId, listener, onError) {
      const byId = new Map<string, ReviewComment>();
      const publish = () =>
        listener([...byId.values()].sort((a, b) => a.createdAtMs - b.createdAtMs));
      return follow(
        (signal) => review.watchReviewComments({ checkoutId }, { signal }),
        ({ frame }) => {
          if (frame.case === 'reset') {
            byId.clear();
            for (const c of frame.value.comments) byId.set(c.id, comment(c));
          } else if (frame.case === 'delta') {
            for (const c of frame.value.upsert) byId.set(c.id, comment(c));
            for (const id of frame.value.remove) byId.delete(id);
          } else return;
          publish();
        },
        onError,
      );
    },

    async upsertComment(draft) {
      const reply = await review.upsertReviewComment({
        comment: {
          id: draft.id,
          checkoutId: draft.checkoutId,
          path: draft.path,
          side: draft.side === 'old' ? DiffSide.OLD : DiffSide.NEW,
          line: draft.line,
          ...(draft.endLine !== undefined ? { endLine: draft.endLine } : {}),
          body: draft.body,
          diffChecksum: draft.diffChecksum,
          createdAtMs: BigInt(draft.createdAtMs ?? Date.now()),
          resolved: draft.resolved ?? false,
        },
      });
      return reply.id;
    },

    async deleteComment(checkoutId, id) {
      await review.deleteReviewComment({ checkoutId, id });
    },

    async resolveComment(checkoutId, id, resolved) {
      await review.resolveReviewComment({ checkoutId, id, resolved });
    },

    async sendReviewToThread(commentIds) {
      await review.sendReviewToThread({ chatId: thread.chatId, commentIds: [...commentIds] });
    },

    async postReviewToChangeRequest(checkoutId, commentIds, event) {
      const reply = await review.postReviewToChangeRequest({
        checkoutId,
        commentIds: [...commentIds],
        event:
          event === 'request-changes' ? ProtoReviewEvent.REQUEST_CHANGES : ProtoReviewEvent.COMMENT,
      });
      return reply.url;
    },

    watchChangeRequest(listener) {
      return follow(
        (signal) =>
          git.watchChangeRequest(
            { cwd: thread.cwd, ...(thread.branch ? { branch: thread.branch } : {}) },
            { signal },
          ),
        ({ status }) => {
          const cr = status?.changeRequest;
          const info: ChangeRequestInfo | null =
            cr && cr.state === ChangeRequestState.OPEN
              ? { number: Number(cr.number), title: cr.title, url: cr.url, provider: cr.provider }
              : null;
          listener(info);
        },
      );
    },

    discardAll(checkoutId, expectedChecksum) {
      return guarded(async () => {
        await diff.discard({ chatId: thread.chatId, checkoutId, expectedChecksum });
      });
    },

    discardPaths(checkoutId, paths, expectedChecksum) {
      return guarded(async () => {
        await diff.discardPaths({ checkoutId, paths: [...paths], expectedChecksum });
      });
    },
  };
}
