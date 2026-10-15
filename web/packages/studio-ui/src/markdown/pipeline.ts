// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// packages/shared/src/markdownPipeline.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the remark and rehype plugin lists without rehype-raw (§53 D919; raw HTML becomes text), a sanitize schema that keeps only http(s) and relative links and blob: images, and the code-meta and inline-code taggers; no Windows-path, Codex-directive or GitHub-alert plugins. Loams' changes are Apache-2.0.

// Studio's markdown pipeline (SD1 Task 18, inventory (b)). Model output is untrusted
// (§53 D919):
// - **No raw HTML.** There is no rehype-raw: HTML in the source stays a `raw` node,
//   which `rehypeRawAsText` turns into a text node before the sanitizer runs (it would
//   drop it otherwise), so `<script>` shows as the text `<script>`.
// - **rehype-sanitize** runs last, as upstream's does, as a second fence: only the
//   default schema's elements survive, links keep http(s) or relative targets, and an
//   image keeps its `src` only when it is a `blob:` URL (a local attachment).
// - The components (Markdown.tsx) apply the same rules again when they render.

import rehypeSanitize, { defaultSchema } from "rehype-sanitize";
import remarkBreaks from "remark-breaks";
import remarkGfm from "remark-gfm";
import type { PluggableList } from "unified";

type MarkdownAstNode = {
  type?: string;
  meta?: unknown;
  data?: {
    hProperties?: Record<string, unknown>;
  };
  children?: MarkdownAstNode[];
};

/** Keeps a fence's meta (`ts title="a.ts"`) as `data-code-meta`, for the block's title. */
function remarkPreserveCodeMeta() {
  return (tree: MarkdownAstNode) => {
    const visit = (node: MarkdownAstNode) => {
      if (node.type === "code" && typeof node.meta === "string" && node.meta.trim().length > 0) {
        node.data = {
          ...node.data,
          hProperties: {
            ...node.data?.hProperties,
            dataCodeMeta: node.meta.trim(),
          },
        };
      }
      node.children?.forEach(visit);
    };

    visit(tree);
  };
}

/**
 * Tags inline code while it can still be told apart from fenced code, so the
 * renderer styles `code` spans and blocks differently. Code inside links stays
 * untagged.
 */
function remarkTagInlineCode() {
  return (tree: MarkdownAstNode) => {
    const visit = (node: MarkdownAstNode, insideLink: boolean) => {
      if (node.type === "inlineCode" && !insideLink) {
        node.data = {
          ...node.data,
          hProperties: {
            ...node.data?.hProperties,
            dataInlineCode: "",
          },
        };
      }
      const childInsideLink = insideLink || node.type === "link" || node.type === "linkReference";
      for (const child of node.children ?? []) visit(child, childInsideLink);
    };

    visit(tree, false);
  };
}

type HastNode = { type?: string; value?: string; children?: HastNode[] };

/** Raw HTML (never parsed: no rehype-raw) becomes the text it was written as. */
function rehypeRawAsText() {
  return (tree: HastNode) => {
    const visit = (node: HastNode) => {
      for (const child of node.children ?? []) {
        if (child.type === "raw") child.type = "text";
        else visit(child);
      }
    };
    visit(tree);
  };
}

/** The sanitize schema: GitHub's default, narrowed to Studio's link and image rules. */
export const CHAT_MARKDOWN_SANITIZE_SCHEMA = {
  ...defaultSchema,
  attributes: {
    ...defaultSchema.attributes,
    "*": (defaultSchema.attributes?.["*"] ?? []).filter((attribute) => attribute !== "title"),
    code: [...(defaultSchema.attributes?.code ?? []), "dataCodeMeta", "dataInlineCode"],
  },
  protocols: {
    ...defaultSchema.protocols,
    href: ["http", "https"],
    src: ["blob"],
    cite: ["http", "https"],
  },
} satisfies Parameters<typeof rehypeSanitize>[0];

export const CHAT_MARKDOWN_REMARK_PLUGINS: PluggableList = [
  remarkGfm,
  remarkPreserveCodeMeta,
  remarkTagInlineCode,
];

export const CHAT_MARKDOWN_REMARK_PLUGINS_WITH_BREAKS: PluggableList = [
  remarkGfm,
  remarkBreaks,
  remarkPreserveCodeMeta,
  remarkTagInlineCode,
];

/** No rehype-raw (D919): raw HTML never becomes elements. */
export const CHAT_MARKDOWN_REHYPE_PLUGINS: PluggableList = [
  rehypeRawAsText,
  [rehypeSanitize, CHAT_MARKDOWN_SANITIZE_SCHEMA],
];

export function shouldPreserveAssistantLineBreaks(text: string): boolean {
  return /^★ Insight(?:\s|─)/mu.test(text);
}
