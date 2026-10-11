import { Empty } from './empty';

/**
 * A backend that answers "not configured" or "not implemented": in a preview that is a
 * feature still to come, not a fault. Covers an `ErrorInfo.reason` such as
 * `house_not_configured` or `not_implemented`, and Connect's `unimplemented` code.
 */
export function isNotServed(failure: { code?: string; reason?: string } | undefined): boolean {
  if (!failure) return false;
  const reason = failure.reason ?? '';
  return (
    reason === 'not_configured' ||
    reason.endsWith('_not_configured') ||
    reason === 'not_implemented' ||
    failure.code === 'unimplemented'
  );
}

/** What a page shows instead of an error when its backend is not served yet. */
export function ComingSoon({ feature, seed = 21 }: { feature: string; seed?: number }) {
  return (
    <Empty title={`${feature} — coming soon in a later preview`} seed={seed}>
      This engine does not run {feature} yet. Nothing is wrong with your setup.
    </Empty>
  );
}
