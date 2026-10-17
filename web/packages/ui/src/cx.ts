/** Joins class names, skipping empty ones. */
export function cx(...names: (string | false | null | undefined)[]): string {
  return names.filter(Boolean).join(' ');
}

/** Base UI props with a plain-string className, so `cx` can join it (no state functions). */
export type WithClass<P> = Omit<P, 'className'> & { className?: string };
