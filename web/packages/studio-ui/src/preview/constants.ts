// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/preview/previewConstants.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the error labels kept; the recent-URL limit dropped (Studio keeps no URL history) and the preview proxy's own failures added. Loams' changes are Apache-2.0.

/**
 * Common Chromium error codes mapped to a short human label. Used by the
 * unreachable view to drop the raw `ERR_*` code in favour of friendlier copy.
 */
export const PREVIEW_ERROR_CODE_MESSAGES: Readonly<Record<string, string>> = Object.freeze({
  ERR_NAME_NOT_RESOLVED: "DNS address could not be found",
  ERR_NAME_RESOLUTION_FAILED: "DNS address could not be found",
  ERR_CONNECTION_REFUSED: "Connection refused",
  ERR_CONNECTION_RESET: "Connection was reset",
  ERR_CONNECTION_CLOSED: "Connection was closed",
  ERR_CONNECTION_TIMED_OUT: "Connection timed out",
  ERR_INTERNET_DISCONNECTED: "No internet connection",
  ERR_TIMED_OUT: "Connection timed out",
  ERR_CERT_AUTHORITY_INVALID: "Certificate authority is not trusted",
  ERR_CERT_COMMON_NAME_INVALID: "Certificate hostname mismatch",
  ERR_CERT_DATE_INVALID: "Certificate is expired or not yet valid",
  ERR_TOO_MANY_REDIRECTS: "Too many redirects",
  // loams-agentd's preview proxy answers these itself.
  ERR_PREVIEW_NOT_RUNNING: "The dev server is not running",
  ERR_PREVIEW_EXPIRED: "The preview link has expired",
});
