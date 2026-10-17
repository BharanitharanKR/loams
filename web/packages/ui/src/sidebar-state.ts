// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ui/sidebarState.ts at commit 33aa4614, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// this header only. Loams' changes are Apache-2.0.
export type ResponsiveSidebarState = "expanded" | "collapsed";

export function resolveSidebarState(input: {
  isMobile: boolean;
  open: boolean;
  openMobile: boolean;
}): ResponsiveSidebarState {
  return (input.isMobile ? input.openMobile : input.open) ? "expanded" : "collapsed";
}
