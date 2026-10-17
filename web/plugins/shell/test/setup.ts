// jsdom has no Element.getAnimations or ResizeObserver; Base UI's scroll area (the
// sidebar's content) and the Logo's grain canvas use them. The node-environment tests
// (viewport.test.ts) have no DOM at all.
if (typeof Element !== 'undefined') {
  Element.prototype.getAnimations ??= () => [];
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
}
