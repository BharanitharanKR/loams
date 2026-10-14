// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/chat/HighlightedTokens.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the token-span renderer and `keyedLines`, fed by an effect that keeps the plain text until Studio's highlighter answers (no Suspense, no embedded scripts, no word grouping). Loams' changes are Apache-2.0.

import { type CSSProperties, useEffect, useMemo, useState } from "react";
import { LRUCache } from "../lib/lruCache";
import { highlightToTokens, type TokenLines } from "./highlight";

interface SyntaxToken {
  readonly content: string;
  readonly offset: number;
  readonly color?: string;
  readonly fontStyle?: number;
}

const MAX_CACHE_ENTRIES = 300;
const MAX_CACHE_BYTES = 8 * 1024 * 1024;
const tokenCache = new LRUCache<TokenLines>(MAX_CACHE_ENTRIES, MAX_CACHE_BYTES);

function syntaxTokenStyle(token: SyntaxToken): CSSProperties {
  const fontStyle = token.fontStyle ?? 0;
  return {
    ...(token.color ? { color: token.color } : {}),
    ...(fontStyle & 1 ? { fontStyle: "italic" } : {}),
    ...(fontStyle & 2 ? { fontWeight: 700 } : {}),
    ...(fontStyle & 4 ? { textDecoration: "underline" } : {}),
  };
}

/**
 * Keys each line by its start offset, which stays unique when several lines
 * are empty, and pairs it with the line ending `code` has after it. Shiki drops
 * `\n` and `\r\n` from tokens alike, so CRLF has to come back from the source.
 */
export function keyedLines<Token extends SyntaxToken>(
  code: string,
  lines: ReadonlyArray<ReadonlyArray<Token>>,
) {
  const endings = code.match(/\r?\n/gu) ?? [];
  let lineStart = 0;
  return lines.map((tokens, index) => {
    const text = tokens.map((token) => token.content).join("");
    const key = `${lineStart}:${text}`;
    const ending = endings[index] ?? "";
    lineStart += text.length + ending.length;
    return { key, tokens, ending };
  });
}

/** The tokens of `code`, or null while they load (or when highlighting is unavailable). */
export function useHighlightedTokens(code: string, language: string): TokenLines | null {
  const cacheKey = `${language}\u0000${code}`;
  const [state, setState] = useState<{ key: string; lines: TokenLines | null }>(() => ({
    key: cacheKey,
    lines: tokenCache.get(cacheKey),
  }));
  useEffect(() => {
    const cached = tokenCache.get(cacheKey);
    if (cached) {
      setState({ key: cacheKey, lines: cached });
      return;
    }
    let live = true;
    highlightToTokens(code, language).then(
      (lines) => {
        tokenCache.set(cacheKey, lines, code.length * 4);
        if (live) setState({ key: cacheKey, lines });
      },
      () => {
        // Shiki could not start: the plain text stays.
      },
    );
    return () => {
      live = false;
    };
  }, [cacheKey, code, language]);
  return state.key === cacheKey ? state.lines : null;
}

/**
 * Colours `code` inside the caller's `<code>` without changing its text, so selection
 * and copy match the plain version. Until the tokens arrive the text is plain. Every
 * token is a React text node: nothing here is parsed as HTML (§53 D919).
 */
export function HighlightedTokens({ code, language }: { code: string; language: string }) {
  const tokens = useHighlightedTokens(code, language);
  const lines = useMemo(() => (tokens ? keyedLines(code, tokens) : null), [code, tokens]);
  if (!lines) return <>{code}</>;
  return lines.map(({ key, tokens: lineTokens, ending }) => (
    <span key={key} data-line="">
      {lineTokens.map((token) => (
        <span key={`${token.offset}:${token.content}`} style={syntaxTokenStyle(token)}>
          {token.content}
        </span>
      ))}
      {ending}
    </span>
  ));
}
