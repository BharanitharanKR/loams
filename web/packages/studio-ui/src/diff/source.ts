// What the diff panel needs from a daemon (SD1 Task 20): a thread's checkout diffs, a
// file's two sides, its review comments and the review actions, and the guarded discards.
// The panel names no transport: @loams/plugin-studio implements this over agentd's
// `DiffService`, `ReviewService` and `GitService` (thread/diff-source.ts), and tests
// implement it in memory. Patches, file text and comment bodies are data, rendered as
// text (§53 D919).

/** Which diff the panel shows (Shared contracts: `GetCheckoutDiff` modes). */
export type DiffSourceKind = "working" | "branch" | "commit" | "turn";

export type DiffSelection =
  | { readonly kind: "working" }
  | { readonly kind: "branch"; readonly baseRef: string | null }
  | { readonly kind: "commit"; readonly commitSha: string | null }
  | { readonly kind: "turn" };

/** One changed file as the daemon counts it. */
export interface DiffFileStat {
  readonly path: string;
  readonly oldPath?: string;
  readonly status: string;
  readonly additions: number;
  readonly deletions: number;
  readonly binary: boolean;
}

/** A checkout's diff: a unified patch, its files and the checksum that guards writes. */
export interface CheckoutDiffView {
  readonly checkoutId: string;
  readonly cwd: string;
  readonly patch: string;
  readonly files: readonly DiffFileStat[];
  readonly additions: number;
  readonly deletions: number;
  /** The daemon cut the patch at its size cap. */
  readonly truncated: boolean;
  readonly checksum: string;
  readonly updatedAtMs: number;
}

/** A file's two sides, for expanding the unchanged lines around a hunk. */
export interface DiffFileText {
  readonly oldText: string | null;
  readonly newText: string | null;
  readonly binary: boolean;
  readonly truncated: boolean;
  /** The diff moved since the checksum asked for. */
  readonly stale: boolean;
}

/** The side of the diff a comment is anchored to. */
export type ReviewSide = "old" | "new";

/** A review comment as the daemon stores it (`loams.agentd.v1.ReviewComment`). */
export interface ReviewComment {
  readonly id: string;
  readonly checkoutId: string;
  readonly path: string;
  readonly side: ReviewSide;
  readonly line: number;
  readonly endLine?: number;
  readonly body: string;
  /** The diff the comment was written against. */
  readonly diffChecksum: string;
  readonly createdAtMs: number;
  readonly resolved: boolean;
  /** Computed by the daemon: the anchored lines are no longer in the current diff. */
  readonly outdated: boolean;
}

/** What the panel sends to store a comment; the daemon fills in the rest. */
export type ReviewCommentDraft = Omit<ReviewComment, "outdated" | "createdAtMs" | "resolved"> & {
  readonly createdAtMs?: number;
  readonly resolved?: boolean;
};

/** The open pull request for the thread's branch, when there is one. */
export interface ChangeRequestInfo {
  readonly number: number;
  readonly title: string;
  readonly url: string;
  readonly provider: string;
}

export interface HistoryCommit {
  readonly sha: string;
  readonly subject: string;
}

export type ReviewEvent = "comment" | "request-changes";

export interface DiffSource {
  /**
   * Names the daemon and thread, for query keys. It is the panel's identity: a new source
   * object with the same key (the thread's branch renamed, say) keeps the panel's diff,
   * comments and open draft; only `watchChangeRequest` follows the new object.
   */
  readonly key: string;
  /** The working tree's diff now and on every change; returns the unsubscribe. */
  watchWorkingDiff(
    listener: (diff: CheckoutDiffView) => void,
    onError: (error: Error) => void,
  ): () => void;
  /** One diff: the working tree, a branch against a base, a commit or the latest turn. */
  getDiff(selection: DiffSelection, signal?: AbortSignal): Promise<CheckoutDiffView>;
  /** A file's two sides at a diff's checksum. */
  fileText(
    selection: DiffSelection,
    diff: CheckoutDiffView,
    path: string,
    signal?: AbortSignal,
  ): Promise<DiffFileText>;
  /** Branches to compare against. */
  listBranches(signal?: AbortSignal): Promise<string[]>;
  /** Recent commits to show. */
  listCommits(signal?: AbortSignal): Promise<HistoryCommit[]>;

  /** The checkout's comments: the whole list now and after each change. */
  watchComments(
    checkoutId: string,
    listener: (comments: readonly ReviewComment[]) => void,
    onError: (error: Error) => void,
  ): () => void;
  /** Creates (an empty `id`: the daemon assigns one) or edits a comment; answers its id. */
  upsertComment(comment: ReviewCommentDraft): Promise<string>;
  deleteComment(checkoutId: string, id: string): Promise<void>;
  resolveComment(checkoutId: string, id: string, resolved: boolean): Promise<void>;
  /** Queues one run on the thread that asks the agent to address these comments. */
  sendReviewToThread(commentIds: readonly string[]): Promise<void>;
  /** Posts the comments as a review on the open pull request; returns its URL. */
  postReviewToChangeRequest(
    checkoutId: string,
    commentIds: readonly string[],
    event: ReviewEvent,
  ): Promise<string>;
  /** The open pull request for the thread's branch, or null; returns the unsubscribe. */
  watchChangeRequest(listener: (changeRequest: ChangeRequestInfo | null) => void): () => void;

  /** Restores the whole checkout; refused when `expectedChecksum` is stale. */
  discardAll(checkoutId: string, expectedChecksum: string): Promise<void>;
  /** Restores some paths; refused when `expectedChecksum` is stale. */
  discardPaths(
    checkoutId: string,
    paths: readonly string[],
    expectedChecksum: string,
  ): Promise<void>;
}

/** A refusal because the diff moved since the reader saw it (`stale_diff`). */
export class StaleDiffError extends Error {
  constructor(message = "The diff changed since you last saw it. Review it again, then retry.") {
    super(message);
    this.name = "StaleDiffError";
  }
}
