import { type ReactNode, useEffect, useState } from 'react';
import { Button } from './button';
import { cx } from './cx';

export function Stats({ children }: { children: ReactNode }) {
  return <div className="loams-stats">{children}</div>;
}

export function Stat({
  label,
  value,
  detail,
}: {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
}) {
  return (
    <div className="loams-stat">
      <span>{label}</span>
      <strong>{value}</strong>
      {detail && <small>{detail}</small>}
    </div>
  );
}

export type NoticeTone = 'info' | 'warn' | 'danger' | 'success';

export function Notice({
  tone = 'info',
  title,
  children,
}: {
  tone?: NoticeTone;
  title: ReactNode;
  children?: ReactNode;
}) {
  return (
    <div
      className={cx('loams-notice', tone !== 'info' && `loams-notice-${tone}`)}
      role={tone === 'danger' ? 'alert' : 'status'}
    >
      <div>
        <strong>{title}</strong>
        {children && <div className="loams-notice-body">{children}</div>}
      </div>
    </div>
  );
}

/** The sentence every fixture-backed page leads with. */
export const SAMPLE_DATA_TEXT = 'this page is not connected to your Loams server yet.';

/**
 * The one notice every page that renders demonstration data shows. Pass `children` for what is
 * specific to the page (what changes only the sample); the lead sentence stays the same.
 */
export function SampleDataNotice({ children }: { children?: ReactNode }) {
  return (
    <div className="loams-sample-data" role="status" data-sample-data="">
      <strong>Sample data</strong>
      <span>
        {' '}
        — {SAMPLE_DATA_TEXT}
        {children && <> {children}</>}
      </span>
    </div>
  );
}

/** The compact form, for a heading or a single section. */
export function SampleDataBadge({ title }: { title?: string }) {
  return (
    <span
      className="loams-sample-data-badge"
      data-sample-data=""
      title={title ?? `Sample data: ${SAMPLE_DATA_TEXT}`}
    >
      Sample data
    </span>
  );
}

/** A command or a value to copy. */
export function Snippet({
  children,
  prompt = true,
  copy = true,
}: {
  children: string;
  prompt?: boolean;
  copy?: boolean;
}) {
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => setCopied(false), 1600);
    return () => clearTimeout(t);
  }, [copied]);
  return (
    <div className="loams-snippet">
      <code>
        {prompt && <span className="loams-snippet-prompt">$ </span>}
        {children}
      </code>
      {copy && (
        <Button
          variant="quiet"
          size="sm"
          onClick={() => navigator.clipboard?.writeText(children).then(() => setCopied(true))}
        >
          {copied ? 'Copied' : 'Copy'}
        </Button>
      )}
    </div>
  );
}

/** A usage bar; ochre above 80 percent, oxide at the limit. */
export function Meter({ value, max, label }: { value: number; max: number; label: string }) {
  const ratio = max > 0 ? Math.min(1, value / max) : 0;
  const level = ratio >= 1 ? 'full' : ratio >= 0.8 ? 'high' : undefined;
  return (
    <div className="loams-meter" data-level={level}>
      <meter className="sr-only" value={value} min={0} max={max} aria-label={label} />
      <span aria-hidden="true" style={{ width: `${ratio * 100}%` }} />
    </div>
  );
}

export type AvatarKind = 'user' | 'team' | 'agent' | 'service_account' | 'system';

/** Initials in a disc for people, a square for agents, teams and service accounts. */
export function Avatar({
  name,
  kind = 'user',
  size = 24,
}: {
  name: string;
  kind?: AvatarKind;
  size?: number;
}) {
  const initials = name
    .split(/[\s\-_]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((w) => w[0]?.toUpperCase())
    .join('');
  return (
    <span
      className="loams-avatar"
      data-kind={kind}
      style={{ width: size, height: size }}
      aria-hidden="true"
    >
      {initials}
    </span>
  );
}
