// A transport failure in plain words (UX walkthrough, 0.1.0): a page that could not reach
// its server showed "HTTP 502", "Bad Gateway" or "Failed to fetch". Any other message is
// the server's own sentence and comes back as it is.

const GATEWAY: Record<string, number> = {
  'bad gateway': 502,
  'service unavailable': 503,
  'gateway timeout': 504,
};

const UNREACHABLE =
  /^(?:failed to fetch|load failed|networkerror when attempting to fetch resource\.?)$/i;

/** `message`, or a plain sentence when it is only a gateway status or a network failure. */
export function plainError(message: string): string {
  const m = message.trim();
  if (UNREACHABLE.test(m)) {
    return 'The server could not be reached. Check that it is running and that its address is right, then try again.';
  }
  const status =
    /^(?:HTTP\s+)?(50[234])(?:\s[A-Za-z ]*)?$/i.exec(m)?.[1] ?? GATEWAY[m.toLowerCase()];
  if (status) {
    return `The server did not answer (HTTP ${status}). Check that the engine is running, then try again.`;
  }
  return message;
}
