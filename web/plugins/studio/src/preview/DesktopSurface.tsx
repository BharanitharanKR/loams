// The desktop's preview surface (SD1 Task 26, D678): this element is a placeholder, and
// Electron main places the preview's WebContentsView over the rect it reports. The view
// hides on unmount and while a popover or dialog is open, which it would cover.

import { Notice } from '@loams/ui';
import { useEffect, useRef, useState } from 'react';
import type { DesktopPreviewApi, DesktopPreviewState } from './access.js';

/** A popover or modal that would be drawn under the native view. */
const OVERLAY = '[data-overlay-open], dialog[open], [role="dialog"], [role="menu"]';

type Rect = { x: number; y: number; width: number; height: number };
const same = (a: Rect | undefined, b: Rect) =>
  !!a && a.x === b.x && a.y === b.y && a.width === b.width && a.height === b.height;

export function DesktopSurface({
  view,
  url,
  onState,
}: {
  view: DesktopPreviewApi;
  url: string;
  onState: (state: DesktopPreviewState) => void;
}) {
  const slot = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string>();

  useEffect(() => view.onState(onState), [view, onState]);

  useEffect(() => {
    const el = slot.current;
    if (!el) return;
    let frame = 0;
    let shown: Rect | undefined;
    let hidden = false;
    let alive = true;
    const hide = () => {
      if (hidden) return;
      hidden = true;
      shown = undefined;
      void view.hide().catch(() => undefined);
    };
    const report = () => {
      frame = 0;
      if (!alive) return;
      if (document.querySelector(OVERLAY)) return hide();
      const r = el.getBoundingClientRect();
      const rect = { x: r.x, y: r.y, width: r.width, height: r.height };
      if (same(shown, rect)) return;
      shown = rect;
      hidden = false;
      view
        .show(url, rect)
        .then((res) => {
          if (!res.ok) shown = undefined;
          if (alive) setError(res.ok ? undefined : res.message);
        })
        .catch((e) => {
          shown = undefined;
          if (alive) setError(e instanceof Error ? e.message : String(e));
        });
    };
    const schedule = () => {
      if (!frame) frame = requestAnimationFrame(report);
    };
    const ro = new ResizeObserver(schedule);
    ro.observe(el);
    const mo = new MutationObserver(schedule);
    mo.observe(document.body, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ['open', 'data-overlay-open'],
    });
    window.addEventListener('resize', schedule);
    window.addEventListener('scroll', schedule, true);
    schedule();
    return () => {
      alive = false;
      if (frame) cancelAnimationFrame(frame);
      ro.disconnect();
      mo.disconnect();
      window.removeEventListener('resize', schedule);
      window.removeEventListener('scroll', schedule, true);
      void view.hide().catch(() => undefined);
    };
  }, [view, url]);

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      {error && <Notice tone="danger" title={error} />}
      <div
        ref={slot}
        data-preview-surface="desktop"
        title="Preview"
        className="min-h-40 flex-1 border border-rule-soft bg-surface"
      />
    </div>
  );
}
