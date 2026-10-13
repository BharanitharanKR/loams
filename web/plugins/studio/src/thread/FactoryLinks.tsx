// A thread's links to factory objects (§53 §5.9, D916; SD1 Task 27):
//
//   - `DraftCard`: the thread a factory app handed over ("Start thread from issue"
//     from Plane, "Fix this error" from GlitchTip), shown on Studio's home until the
//     composer (Task 19) takes it as its first message.
//   - `BranchPr`: the Forgejo pull request of a thread's branch, for the thread's
//     toolbar (Task 23 places it there).
//
// Everything shown is text from another system: rendered as text, links http(s) only
// (§53 D919).

import { Card } from '@loams/ui';
import { useQuery } from '@tanstack/react-query';
import { ExternalLink, GitBranch, GitPullRequest } from 'lucide-react';
import { useId } from 'react';
import { httpUrl, type StudioFactory } from '../factory.js';
import type { ThreadDraft } from '../routes.js';

const SOURCE: Record<ThreadDraft['from'], string> = { plane: 'Plane', glitchtip: 'GlitchTip' };

export function DraftCard({ draft }: { draft: ThreadDraft }) {
  const id = useId();
  const link = httpUrl(draft.link);
  return (
    <section aria-labelledby={id} className="mx-auto w-full max-w-[960px] px-6 pt-8">
      <Card
        title={<span id={id}>New thread from {SOURCE[draft.from]}</span>}
        className="border-accent"
      >
        <div className="flex flex-col gap-3">
          {draft.title && <strong className="text-ink">{draft.title}</strong>}
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[0.82rem] text-muted">
            {draft.branch && (
              <span className="flex items-center gap-1.5">
                <GitBranch size={13} aria-hidden="true" />
                <code>{draft.branch}</code>
              </span>
            )}
            {link && (
              <a
                href={link}
                target="_blank"
                rel="noopener noreferrer"
                className="flex min-w-0 items-center gap-1.5"
              >
                <ExternalLink size={13} aria-hidden="true" className="shrink-0" />
                <span className="truncate">{link}</span>
              </a>
            )}
          </div>
          <pre className="m-0 max-h-72 overflow-auto whitespace-pre-wrap border border-rule-soft bg-raised p-3 font-mono text-[0.78rem] text-ink">
            {draft.prompt}
          </pre>
          <p className="m-0 text-[0.84rem] text-muted">
            Studio cannot start threads in this version yet. This draft opens in a new thread&apos;s
            composer once it can; pick a project to work in.
          </p>
        </div>
      </Card>
    </section>
  );
}

/** The open Forgejo PR whose head is `branch` in `repo` (`owner/name`); nothing if none. */
export function BranchPr({
  factory,
  repo,
  branch,
}: {
  factory: StudioFactory;
  repo: string;
  branch: string;
}) {
  const [owner, name] = repo.split('/');
  const pulls = factory.pulls;
  const pr = useQuery({
    queryKey: ['studio', 'factory', 'pulls', repo],
    queryFn: () => (pulls && owner && name ? pulls(owner, name) : Promise.resolve([])),
    enabled: Boolean(pulls && owner && name),
    select: (rows) => rows.find((p) => p.head === branch),
  });
  const url = httpUrl(pr.data?.url);
  if (!pr.data || !url) return null;
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      title={pr.data.title}
      className="inline-flex shrink-0 items-center gap-1.5 rounded-pill border border-rule-soft px-2.5 py-1 text-[0.8rem] text-muted no-underline hover:bg-raised hover:text-ink"
    >
      <GitPullRequest size={13} aria-hidden="true" />
      PR #{pr.data.id}
    </a>
  );
}
