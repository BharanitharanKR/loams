// The composer (SD1 Task 19; inventory (c)): T3 Code's Tiptap editor and its
// Markdown document model, the `@`, `/` and `$` triggers and their menu, attachments,
// per-thread drafts and the send projection. The container that talks to the daemon
// is @loams/plugin-studio's thread/Composer.tsx.

export {
  ACCEPTED_IMAGE_TYPES,
  ATTACHMENT_ACCEPT,
  type AttachmentCheck,
  type AttachmentUploader,
  bytesToBase64,
  checkAttachment,
  isHeic,
  MAX_ATTACHMENT_BYTES,
  MAX_ATTACHMENTS,
  newUploadId,
  UPLOAD_CHUNK_BYTES,
  uploadAttachment,
} from "./attachments";
export {
  type ComposerDraft,
  type ComposerDraftState,
  composerDraftStore,
  createComposerDraftStore,
  DRAFTS_STORAGE_KEY,
  type DraftAttachment,
  type DraftPatch,
  type DraftStorage,
  EMPTY_DRAFT,
  readStoredDrafts,
  resetComposerDraftStoreForTests,
  useComposerDraft,
} from "./draftStore";
export {
  type ComposerChipActions,
  type ComposerContextChip,
  type ComposerDraftContextRecords,
  type ComposerPromptEditorHandle,
  type ComposerPromptEditorProps,
  ComposerPromptEditorTiptap,
} from "./Editor";
export {
  type ComposerTrigger,
  collapseExpandedComposerCursor,
  detectComposerTrigger,
  expandCollapsedComposerCursor,
  isComposerSendKey,
  replaceTextRange,
} from "./logic";
export {
  type ComposerFileMatch,
  type ComposerMenuItem,
  type ComposerSkillInfo,
  type ComposerSlashCommandInfo,
  composerMenuReplacement,
  fileMenuItems,
  isMenuTrigger,
  searchSkillMenu,
  searchSlashMenu,
} from "./menu";
export { hasSendableContent, promptForSend, terminalContextRecord } from "./send";
export { serializeComposerFileLink } from "./shared/composerTrigger";
export type { ServerProviderSkill } from "./shared/contracts";
