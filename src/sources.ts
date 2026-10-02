/** A virtual data source: deterministic in time. */
export type Source = (unixTimeMs: number) => number;

export const sources: Record<string, Source> = {
  sin: (t) => Math.sin(t / 10000),
  cos: (t) => Math.cos(t / 10000),
};

/**
 * The one place a source name becomes a function. Object.hasOwn, not `in`
 * or indexing: those also see Object.prototype, so "toString" or
 * "__proto__" would pass as a source name and install a prototype member as
 * the sampling function.
 */
export function lookupSource(name: string): Source | undefined {
  return Object.hasOwn(sources, name) ? sources[name] : undefined;
}
