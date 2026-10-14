// Where a terminal link goes (SD1 Task 22, §53 §5.7). An http(s) link opens outside
// Studio; anything else links.ts finds is a file path, opened in the thread's file
// viewer: relative to the terminal's working directory when it lies inside it,
// absolute otherwise, with a `:line[:column]` suffix (a compiler's) split off.

export interface FileLinkTarget {
  /** Relative to the terminal's cwd when inside it, else absolute (or `~/…`). */
  path: string;
  line?: number;
  column?: number;
}

function normalizePosix(path: string): string {
  const absolute = path.startsWith("/");
  const out: string[] = [];
  for (const part of path.split("/")) {
    if (part === "" || part === ".") continue;
    if (part === "..") {
      if (out.length > 0 && out[out.length - 1] !== "..") out.pop();
      else if (!absolute) out.push("..");
      continue;
    }
    out.push(part);
  }
  return (absolute ? "/" : "") + out.join("/");
}

const WINDOWS_ABSOLUTE = /^(?:[A-Za-z]:[\\/]|\\\\)/;

/** The viewer's target for a path link found in a terminal running in `cwd`. */
export function fileLinkTarget(text: string, cwd: string): FileLinkTarget {
  const position = /^(.*?)(?::(\d+))?(?::(\d+))?$/.exec(text);
  const raw = position?.[1] || text;
  const line = position?.[2] ? Number(position[2]) : undefined;
  const column = position?.[3] ? Number(position[3]) : undefined;
  const at = {
    ...(line !== undefined ? { line } : {}),
    ...(column !== undefined ? { column } : {}),
  };
  if (raw.startsWith("~/") || WINDOWS_ABSOLUTE.test(raw)) return { path: raw, ...at };
  const base = normalizePosix(cwd);
  const full = raw.startsWith("/") ? normalizePosix(raw) : normalizePosix(`${base}/${raw}`);
  const inside =
    base === "/" ? full.slice(1) : full.startsWith(`${base}/`) ? full.slice(base.length + 1) : null;
  return { path: inside ?? full, ...at };
}
