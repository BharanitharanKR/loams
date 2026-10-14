// @loams/studio-ui/preview: Studio's Preview tab pieces (design §53 §5.8; SD1 Task 26),
// adapted from T3 Code's preview chrome (inventory (n)). The surface itself, a desktop
// WebContentsView or a sandboxed frame on the remote listener, is @loams/plugin-studio's.
export { PREVIEW_ERROR_CODE_MESSAGES } from "./constants";
export {
  type DiscoveredServer,
  isLoopbackHost,
  mergeServers,
  type PreviewableServer,
  parsePreviewUrl,
  previewNameOf,
} from "./discoveredServers";
export { describePreviewError } from "./errorMessages";
export {
  forwardCommand,
  openDiscoveredPort,
  type PreviewReach,
  type PreviewTarget,
  type RemotePreview,
} from "./openDiscoveredPort";
export { PreviewChromeRow } from "./PreviewChromeRow";
export { PreviewEmptyState, type PreviewStartAction } from "./PreviewEmptyState";
export { PreviewLocalServerCard } from "./PreviewLocalServerCard";
export { PreviewUnreachable } from "./PreviewUnreachable";
