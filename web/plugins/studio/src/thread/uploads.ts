// The composer's uploads (SD1 Task 19; §53 §5.6): images go to the daemon through
// `AttachmentService.UploadChunk` and `UploadCommit` (@loams/studio-ui's chunked
// `uploadAttachment`); the committed path joins the draft, and the run names it.
// What the composer refuses (HEIC, other types, too large, too many) never leaves the
// page, and says why.

import { createClient, type Transport } from '@connectrpc/connect';
import { AttachmentService } from '@loams/proto/agentd';
import {
  type AttachmentUploader,
  checkAttachment,
  composerDraftStore,
  type DraftAttachment,
  MAX_ATTACHMENTS,
  newUploadId,
  uploadAttachment,
} from '@loams/studio-ui';
import { useCallback, useState } from 'react';

/** `AttachmentUploader` over a daemon's AttachmentService. */
export function agentdUploader(transport: Transport): AttachmentUploader {
  const client = createClient(AttachmentService, transport);
  return {
    async chunk(uploadId, data, seq) {
      await client.uploadChunk({ uploadId, data, seq: BigInt(seq) });
    },
    async commit(uploadId, fileName) {
      const { path } = await client.uploadCommit({ uploadId, fileName });
      return path;
    },
  };
}

/** A file on its way to the daemon. */
export interface PendingUpload {
  id: string;
  name: string;
  sent: number;
  total: number;
}

/**
 * The composer's attachment intake for a thread: refuses what it cannot send, uploads
 * the rest one by one and adds each committed file to the thread's draft.
 */
export function useUploads(threadId: string, uploader: AttachmentUploader | undefined) {
  const [pending, setPending] = useState<PendingUpload[]>([]);
  const [refusals, setRefusals] = useState<string[]>([]);

  const add = useCallback(
    async (files: readonly File[]) => {
      const store = composerDraftStore();
      const reasons: string[] = [];
      const accepted: File[] = [];
      const already = (store.getState().drafts[threadId]?.attachments.length ?? 0) + pending.length;
      for (const file of files) {
        const check = checkAttachment(file);
        if (!check.ok) reasons.push(check.reason);
        else if (already + accepted.length >= MAX_ATTACHMENTS)
          reasons.push(
            `${file.name} was not added: at most ${MAX_ATTACHMENTS} images per message.`,
          );
        else accepted.push(file);
      }
      setRefusals(reasons);
      if (!uploader) {
        if (accepted.length) setRefusals([...reasons, 'Studio is not connected to loams-agentd.']);
        return;
      }
      for (const file of accepted) {
        const id = newUploadId();
        setPending((list) => [...list, { id, name: file.name, sent: 0, total: file.size }]);
        try {
          const path = await uploadAttachment(file, uploader, {
            uploadId: id,
            onProgress: (sent, total) =>
              setPending((list) => list.map((p) => (p.id === id ? { ...p, sent, total } : p))),
          });
          const attachment: DraftAttachment = {
            id,
            name: file.name,
            mimeType: file.type,
            size: file.size,
            path,
          };
          const current = store.getState().drafts[threadId]?.attachments ?? [];
          store.getState().update(threadId, { attachments: [...current, attachment] });
        } catch (error) {
          setRefusals((list) => [
            ...list,
            `${file.name} could not be uploaded: ${error instanceof Error ? error.message : String(error)}`,
          ]);
        } finally {
          setPending((list) => list.filter((p) => p.id !== id));
        }
      }
    },
    [pending.length, threadId, uploader],
  );

  const remove = useCallback(
    (attachmentId: string) => {
      const store = composerDraftStore();
      const current = store.getState().drafts[threadId]?.attachments ?? [];
      store
        .getState()
        .update(threadId, { attachments: current.filter((a) => a.id !== attachmentId) });
    },
    [threadId],
  );

  return {
    pending,
    refusals,
    add,
    remove,
    dismissRefusals: useCallback(() => setRefusals([]), []),
  };
}
