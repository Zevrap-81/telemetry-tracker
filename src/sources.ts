/** A virtual data source: deterministic in time. */
export type Source = (unixTimeMs: number) => number;

export const sources: Record<string, Source> = {
  sin: (t) => Math.sin(t / 10000),
  cos: (t) => Math.cos(t / 10000),
};
