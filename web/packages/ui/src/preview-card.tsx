// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/preview-card.tsx at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the design 59 tokens and cx, with Base UI underneath; no cva, lucide or Effect; the popup is the shared overlay surface, between dialogs (71) and toasts (80). Loams' changes are Apache-2.0.

import { PreviewCard as PreviewCardPrimitive } from '@base-ui/react/preview-card';
import { cx, type WithClass } from './cx';
import { OVERLAY_LAYER, OVERLAY_SURFACE } from './popover';

export const PreviewCard = PreviewCardPrimitive.Root;

export function PreviewCardTrigger(props: PreviewCardPrimitive.Trigger.Props) {
  return <PreviewCardPrimitive.Trigger data-slot="preview-card-trigger" {...props} />;
}

export function PreviewCardPopup({
  className,
  align = 'start',
  side = 'top',
  sideOffset = 6,
  ...props
}: WithClass<PreviewCardPrimitive.Popup.Props> & {
  align?: PreviewCardPrimitive.Positioner.Props['align'];
  side?: PreviewCardPrimitive.Positioner.Props['side'];
  sideOffset?: PreviewCardPrimitive.Positioner.Props['sideOffset'];
}) {
  return (
    <PreviewCardPrimitive.Portal>
      <PreviewCardPrimitive.Positioner
        align={align}
        className={cx(OVERLAY_LAYER, 'max-w-(--available-width)')}
        data-slot="preview-card-positioner"
        side={side}
        sideOffset={sideOffset}
      >
        <PreviewCardPrimitive.Popup
          className={cx(
            'origin-(--transform-origin) rounded-panel p-3 text-body',
            OVERLAY_SURFACE,
            className,
          )}
          data-slot="preview-card-popup"
          {...props}
        />
      </PreviewCardPrimitive.Positioner>
    </PreviewCardPrimitive.Portal>
  );
}
