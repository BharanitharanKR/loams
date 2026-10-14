// Composer attachments (SD1 Task 19; §53 §5.6): which images the composer takes and
// how it sends them to the daemon. Browsers cannot stream a request body over
// Connect, so an upload is a series of `UploadChunk` calls (base64, numbered) and an
// `UploadCommit`, which answers the file's path on the daemon's host; the run then
// names that path (`RunRequest.attachments`).
//
// HEIC is refused with a message: T3 Code converts it with heic-to, which is LGPL-3.0
// and never added (Global Constraints, Licences).

/** The image types a harness can read from an attachment path. */
export const ACCEPTED_IMAGE_TYPES = ["image/png", "image/jpeg", "image/gif", "image/webp"] as const;

/** For `<input accept>`. */
export const ATTACHMENT_ACCEPT = ACCEPTED_IMAGE_TYPES.join(",");

/** Per file; the daemon's own cap is 32 MiB. */
export const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;

/** Per message. */
export const MAX_ATTACHMENTS = 8;

/**
 * Raw bytes per chunk: a multiple of 3, so every chunk's base64 has no padding and the
 * daemon can join them before decoding (about 60 KB of base64, the size the daemon's
 * uploads were made for).
 */
export const UPLOAD_CHUNK_BYTES = 45 * 1024;

export type AttachmentCheck = { ok: true } | { ok: false; reason: string };

const HEIC = /\.(heic|heif)$/i;

export function isHeic(file: { name: string; type: string }): boolean {
  return /^image\/hei[cf](-sequence)?$/i.test(file.type) || HEIC.test(file.name);
}

/** Whether the composer takes a file, and why not. */
export function checkAttachment(file: {
  name: string;
  type: string;
  size: number;
}): AttachmentCheck {
  if (isHeic(file)) {
    return {
      ok: false,
      reason: `${file.name} is a HEIC image, which Studio cannot send. Export it as PNG or JPEG and attach that instead.`,
    };
  }
  if (!(ACCEPTED_IMAGE_TYPES as readonly string[]).includes(file.type)) {
    return {
      ok: false,
      reason: `${file.name} is not a PNG, JPEG, GIF or WebP image.`,
    };
  }
  if (file.size > MAX_ATTACHMENT_BYTES) {
    return {
      ok: false,
      reason: `${file.name} is larger than ${MAX_ATTACHMENT_BYTES / (1024 * 1024)} MB.`,
    };
  }
  return { ok: true };
}

/** The daemon's upload calls (`AttachmentService`). */
export interface AttachmentUploader {
  chunk(uploadId: string, data: string, seq: number): Promise<void>;
  commit(uploadId: string, fileName: string): Promise<string>;
}

/** An upload id the daemon accepts: `[A-Za-z0-9_-]{1,64}`. */
export function newUploadId(): string {
  const random =
    typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
      ? crypto.randomUUID()
      : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return `studio-${random}`.replace(/[^A-Za-z0-9_-]/g, "").slice(0, 64);
}

/** Base64 of a byte array, without building one argument per byte. */
export function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const step = 0x2000;
  for (let i = 0; i < bytes.length; i += step) {
    binary += String.fromCharCode(...bytes.subarray(i, i + step));
  }
  return btoa(binary);
}

async function readBytes(blob: Blob): Promise<Uint8Array> {
  if (typeof blob.arrayBuffer === "function") return new Uint8Array(await blob.arrayBuffer());
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(new Uint8Array(reader.result as ArrayBuffer));
    reader.onerror = () => reject(reader.error ?? new Error("Could not read the file."));
    reader.readAsArrayBuffer(blob);
  });
}

/**
 * Uploads a file in chunks and commits it; answers its path on the daemon's host.
 * An empty file still sends one (empty) chunk, so the commit finds the upload.
 */
export async function uploadAttachment(
  file: Blob & { name: string },
  uploader: AttachmentUploader,
  options: {
    uploadId?: string;
    signal?: AbortSignal;
    onProgress?: (sentBytes: number, totalBytes: number) => void;
  } = {},
): Promise<string> {
  const uploadId = options.uploadId ?? newUploadId();
  const total = file.size;
  let seq = 0;
  for (let offset = 0; offset < total || seq === 0; offset += UPLOAD_CHUNK_BYTES) {
    options.signal?.throwIfAborted();
    const bytes = await readBytes(file.slice(offset, Math.min(total, offset + UPLOAD_CHUNK_BYTES)));
    await uploader.chunk(uploadId, bytesToBase64(bytes), seq);
    seq += 1;
    options.onProgress?.(Math.min(total, offset + UPLOAD_CHUNK_BYTES), total);
    if (total === 0) break;
  }
  options.signal?.throwIfAborted();
  return uploader.commit(uploadId, file.name);
}
