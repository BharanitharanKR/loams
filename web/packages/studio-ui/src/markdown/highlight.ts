// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/lib/syntaxHighlighting.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// Shiki's own core with the Oniguruma engine instead of @pierre/diffs' shared highlighter, one CSS-variable theme mapped onto @loams/ui tokens instead of two colour themes, and tokens instead of HTML. Loams' changes are Apache-2.0.

// Code highlighting for Studio's markdown (SD1 Task 18; §53 D919). Shiki tokenises; the
// caller renders each token as a React text span (HighlightedTokens.tsx), so no HTML
// string from Shiki ever reaches the DOM. The theme is CSS variables whose fallbacks are
// @loams/ui tokens, so code follows the light and dark token sets like everything else
// (console-ui-stack ruling 1). Grammars load on first use; an unknown language is text.

import type { HighlighterCore, ThemedToken } from "shiki/core";

/** The variables Shiki's CSS-variables theme reads, mapped onto @loams/ui tokens. */
export const CODE_TOKEN_VARIABLES: Readonly<Record<string, string>> = {
  foreground: "var(--op-ink)",
  background: "transparent",
  "token-constant": "var(--op-info)",
  "token-string": "var(--op-grow)",
  "token-comment": "var(--op-faint)",
  "token-keyword": "var(--op-accent)",
  "token-parameter": "var(--op-ink)",
  "token-function": "var(--op-info)",
  "token-string-expression": "var(--op-grow)",
  "token-punctuation": "var(--op-muted)",
  "token-link": "var(--op-accent)",
};

const THEME_NAME = "loams-tokens";

/**
 * The grammars Studio loads, each on first use. A short list on purpose: Shiki bundles
 * about 300 grammars, and every one would be a chunk in the console and the desktop
 * app. A fence in any other language renders as plain text.
 */
const LANGUAGE_LOADERS: Readonly<
  Record<string, () => Promise<{ default: Parameters<HighlighterCore["loadLanguage"]>[0] }>>
> = {
  c: () => import("shiki/langs/c.mjs"),
  cpp: () => import("shiki/langs/cpp.mjs"),
  csharp: () => import("shiki/langs/csharp.mjs"),
  css: () => import("shiki/langs/css.mjs"),
  diff: () => import("shiki/langs/diff.mjs"),
  docker: () => import("shiki/langs/docker.mjs"),
  go: () => import("shiki/langs/go.mjs"),
  graphql: () => import("shiki/langs/graphql.mjs"),
  html: () => import("shiki/langs/html.mjs"),
  ini: () => import("shiki/langs/ini.mjs"),
  java: () => import("shiki/langs/java.mjs"),
  javascript: () => import("shiki/langs/javascript.mjs"),
  json: () => import("shiki/langs/json.mjs"),
  jsonc: () => import("shiki/langs/jsonc.mjs"),
  jsx: () => import("shiki/langs/jsx.mjs"),
  kotlin: () => import("shiki/langs/kotlin.mjs"),
  lua: () => import("shiki/langs/lua.mjs"),
  make: () => import("shiki/langs/make.mjs"),
  nix: () => import("shiki/langs/nix.mjs"),
  php: () => import("shiki/langs/php.mjs"),
  powershell: () => import("shiki/langs/powershell.mjs"),
  proto: () => import("shiki/langs/proto.mjs"),
  python: () => import("shiki/langs/python.mjs"),
  ruby: () => import("shiki/langs/ruby.mjs"),
  rust: () => import("shiki/langs/rust.mjs"),
  scss: () => import("shiki/langs/scss.mjs"),
  shellscript: () => import("shiki/langs/shellscript.mjs"),
  sql: () => import("shiki/langs/sql.mjs"),
  swift: () => import("shiki/langs/swift.mjs"),
  toml: () => import("shiki/langs/toml.mjs"),
  tsx: () => import("shiki/langs/tsx.mjs"),
  typescript: () => import("shiki/langs/typescript.mjs"),
  xml: () => import("shiki/langs/xml.mjs"),
  yaml: () => import("shiki/langs/yaml.mjs"),
  zig: () => import("shiki/langs/zig.mjs"),
};

const LANGUAGE_ALIASES: Readonly<Record<string, string>> = {
  bash: "shellscript",
  "c++": "cpp",
  cjs: "javascript",
  console: "shellscript",
  cs: "csharp",
  cts: "typescript",
  dockerfile: "docker",
  golang: "go",
  gql: "graphql",
  h: "c",
  hpp: "cpp",
  js: "javascript",
  kt: "kotlin",
  makefile: "make",
  mjs: "javascript",
  mts: "typescript",
  patch: "diff",
  protobuf: "proto",
  ps1: "powershell",
  pwsh: "powershell",
  py: "python",
  rb: "ruby",
  rs: "rust",
  sh: "shellscript",
  shell: "shellscript",
  ts: "typescript",
  yml: "yaml",
  zsh: "shellscript",
};

/** One line of tokens, as Shiki returns them. */
export type TokenLines = ThemedToken[][];

let highlighterPromise: Promise<HighlighterCore> | null = null;
const languagePromises = new Map<string, Promise<string>>();

async function createHighlighter(): Promise<HighlighterCore> {
  const [{ createHighlighterCore, createCssVariablesTheme }, { createOnigurumaEngine }] =
    await Promise.all([import("shiki/core"), import("shiki/engine/oniguruma")]);
  return createHighlighterCore({
    themes: [
      createCssVariablesTheme({
        name: THEME_NAME,
        variablePrefix: "--loams-code-",
        variableDefaults: { ...CODE_TOKEN_VARIABLES },
        fontStyle: true,
      }),
    ],
    langs: [],
    // Always Oniguruma: the JS regex engine can backtrack catastrophically (upstream's note).
    engine: createOnigurumaEngine(import("shiki/wasm")),
  });
}

function getHighlighter(): Promise<HighlighterCore> {
  highlighterPromise ??= createHighlighter().catch((error) => {
    highlighterPromise = null;
    throw error;
  });
  return highlighterPromise;
}

/**
 * Loads `language`'s grammar into the shared highlighter. Resolves to the language
 * id to tokenise with: `language` itself, or `"text"` when Shiki has no grammar for it.
 */
async function loadLanguage(highlighter: HighlighterCore, language: string): Promise<string> {
  const cached = languagePromises.get(language);
  if (cached) return cached;
  const promise = (async () => {
    const id = LANGUAGE_ALIASES[language] ?? language;
    const load = LANGUAGE_LOADERS[id];
    if (!load) return "text";
    try {
      await highlighter.loadLanguage((await load()).default);
      return id;
    } catch {
      return "text";
    }
  })();
  languagePromises.set(language, promise);
  return promise;
}

/**
 * Tokenises `code` as `language`. Rejects only when Shiki itself cannot start; the
 * caller keeps the plain text then.
 */
export async function highlightToTokens(code: string, language: string): Promise<TokenLines> {
  const highlighter = await getHighlighter();
  const lang = await loadLanguage(highlighter, language.toLowerCase());
  return highlighter.codeToTokens(code, { lang, theme: THEME_NAME }).tokens;
}
